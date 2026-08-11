using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace Draghi.Pipelining;

/// <summary>
/// Source-driven pipelined request/response coordinator. Processes each item from the source
/// through the policy's lifecycle (execute/activate/complete/recover). The source's enumerator owns
/// cancellation and completion; <see cref="CompleteAsync"/> drives shutdown by signalling it.
/// </summary>
/// <remarks>
/// Concurrent <see cref="CompleteAsync"/> calls are supported; the first call initiates shutdown
/// and all callers observe the same run-completion task. <see cref="GetEnumerator"/> is a diagnostic,
/// best-effort snapshot and is not safe to use concurrently from multiple callers. <see cref="Depth"/>
/// is a lock-free read safe from any thread, but may be stale on return. Internally, execution,
/// completion advancement, and recovery may run concurrently.
/// </remarks>
public sealed partial class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    TPolicy _policy;
    TSource _source;
    TEnumerator _enumerator;

    // Completed initially and between runs; a faulted run remains observable until reinitialization.
    Task _executionTask = Task.CompletedTask;
    // Shutdown handoff: null, caller in Complete, caller done, or the executor's waiter.
    object? _shutdownSlot;
    DepthState _depthState;
    Exception? _completionException;

    // Executor-owned tail, held outside the store until trailing work permits commit or substitution.
    T _pendingTail = default!;
    bool _hasPendingTail;
    ValueTask _pendingTailTask;
    // Enumeration snapshot only; ActivationGate owns versioned handoff identity.
    T _executingItem = default!;
    // Seqlock generation for non-atomically-writable value types; the executor is the sole writer.
    uint _executingItemGeneration;

    /// <summary>
    /// The item holding the executor's single-pump slot, or default. Set before execution and held
    /// through trailing work; recovery replaces it with the substitute.
    /// </summary>
    /// <remarks>
    /// Policies may use this as the current executor identity without separate bookkeeping.
    /// </remarks>
    public T ExecutingItem => ReadSlot(ref _executingItem, ref _executingItemGeneration);

    // Most recently activated item. Its reference remains through inter-item gaps.
    T _activatedItem = default!;
    // Seqlock generation for non-atomically-writable value types.
    uint _activatedItemGeneration;

    /// <summary>
    /// The most recently activated item, or default before the first activation.
    /// </summary>
    /// <remarks>
    /// The prior identity remains visible between completion and the next activation; no activated work
    /// occurs during that gap.
    /// </remarks>
    public T ActivatedItem => ReadSlot(ref _activatedItem, ref _activatedItemGeneration);
    // Enumeration visibility while the executor holds an item outside the store.
    bool _executingItemVisible;

    // Cross-thread components; each type documents its own ownership and ordering protocol.
    InFlightStore<T> _inFlight;
    ItemTenure _itemTenure;
    ActivationGate<T> _activationGate;

    T _inFlightRecoveryItem = default!; // The item being recovered, for the bailout/completion paths to access.
    long _inFlightRecoverySequence;          // The recovery position's claim ordinal - the turn identity its completion releases.
    bool _pendingTailActivated;        // The tail was activated on the executor strand and owns its turn directly.
    long _pendingTailGeneration;              // Provisional-turn generation, or zero when activation was not handed off.
    TaskCompletionSource? _advanceDrainWaiter; // Set while teardown waits for residents, executor publication, and advancement ownership to quiesce.

    // Fixed completion trampoline; only the current head carries its registration.
    readonly Action _onAdvanceCallback;

    internal Pipeline(TPolicy policy, TSource source)
    {
        // Delegate references bind to `this` and don't change.
        _onAdvanceCallback = OnAdvanceCallback;
        // Explicit store construction: the count word is biased (+1) and a default-initialized
        // struct would read count -1 (see InFlightStore's ctor).
        _inFlight = new();
        _activationGate = new();
        Initialize(policy, source);
    }

    /// Per-run init: sets policy + source, resets transient executor state, creates a fresh enumerator
    /// and execution task. Called from the constructor and for instance reuse. Throws if the
    /// previous run hasn't fully completed (first call uses a completed sentinel).
    [MemberNotNull(nameof(_policy), nameof(_source))]
    internal void Initialize(TPolicy policy, TSource source)
    {
        if (!_executionTask.IsCompleted)
            throw new InvalidOperationException("Cannot re-initialize a pipeline whose previous run hasn't fully completed. Await CompleteAsync's returned task first.");

        _policy = policy;
        _source = source;
        _shutdownSlot = null;

        // Depth is counted at DISPATCH (the executor's single-consumer pull), not at enqueue, so the
        // source needs no depth-increment hook. Pipeline doesn't pass a CT here: source owns
        // its own cancellation lifecycle (caller-configured at source construction).
        _enumerator = _source.CreateEnumerator();
        _executionTask = ExecuteSource();

        // Remaining transient fields were initialized or cleared by the preceding run.
    }

#if DEBUG
    // Verifies that trailing recovery remains executor-strand serialized; not a lock.
    int _recoverySwapGuard;
#endif


    /// <summary>Current in-flight count: items dispatched by the executor but not yet completed.
    /// Excludes the source backlog (enqueued but not yet dispatched); a queue-backed pipeline exposes
    /// that as <see cref="UnboundedPipeline{T,TPolicy}.Backlog"/>, and <c>Depth + Backlog</c> is the total
    /// outstanding. Lock-free read, may be stale by the time the caller observes it. Use
    /// <see cref="WaitForEmptyAsync"/> to await empty (both halves zero).</summary>
    public int Depth => _depthState.Depth;

    /// <summary>Completion of the current run: completes when the run has fully torn down, faults
    /// when the run breaks, the same task <see cref="CompleteAsync"/> returns. Unlike CompleteAsync
    /// this does not initiate completion, so an embedder can observe a live pipeline. Between runs
    /// this reads as a completed task; a faulted run keeps its fault here until the next run
    /// starts.</summary>
    public Task Completion => _executionTask;

    // Covers the dequeue-to-depth-increment gap for external empty observations.
    bool _pulling;
    internal bool IsPulling => Volatile.Read(ref _pulling);

    /// <summary>
    /// Waits for the pipeline to be empty: both in-flight (<see cref="Depth"/>) and backlog
    /// (enqueued but not yet dispatched) at zero. Does not prevent new items from being yielded by
    /// the source.
    /// </summary>
    /// <remarks>
    /// The caller supplies backlog because <see cref="Depth"/> counts only dispatched items. This
    /// observes momentary emptiness, not full executor quiescence; use <see cref="CompleteAsync"/> for that.
    /// </remarks>
    internal ValueTask WaitForEmptyAsync(int backlog, CancellationToken cancellationToken = default)
        => _depthState.WaitForEmptyAsync(backlog, cancellationToken);

    /// Rechecks an armed empty wait against a fresh backlog snapshot.
    internal void RecheckEmpty(int backlog) => _depthState.RecheckEmpty(backlog);

    /// <summary>
    /// Initiates pipeline shutdown. First-writer wins: subsequent calls return the same execution task.
    /// The returned task completes when the executor loop has fully drained and exited.
    /// </summary>
    /// <param name="exception">
    /// Optional exception delivered to any items still in flight when shutdown drains them
    /// (via <see cref="IPipelinePolicy{T}.CompleteItem"/>'s exception parameter). Note: this exception is
    /// not propagated through the returned task. It only flows to items.
    /// </param>
    /// <remarks>
    /// Awaiting the returned task gives "fully quiet" semantics: all items are completed, the executor
    /// has exited, and the advancer chain (including in-flight recovery continuations) has unwound -
    /// drain coordinates with the advancer via an idle TCS so any remaining continuation work
    /// is observed before drain returns.
    /// </remarks>
    public ValueTask CompleteAsync(Exception? exception = null)
    {
        // First claim initiates shutdown; later calls return the same execution task.
        if (Interlocked.CompareExchange(ref _shutdownSlot, Pipeline.ShutdownInFlight, null) != null)
            return new(_executionTask);

        _completionException = exception;
        // Signals source completion while leaving the enumerator available for draining.
        _enumerator.Complete();

        // Signal a terminal cleanup that arrived while Complete was in flight.
        var swapped = Interlocked.Exchange(ref _shutdownSlot, Pipeline.ShutdownDone);
        if (swapped is TaskCompletionSource waiter)
            waiter.TrySetResult();
        return new(_executionTask);
    }

    /// <summary>Returns an enumerator over all items currently in the pipeline, from oldest to newest.</summary>
    /// <remarks>
    /// Best-effort under concurrent mutation. Both the execution queue and the in-flight queue may
    /// be mutated by the execution loop or the advancer, pausing enqueues alone is not sufficient.
    /// For reference types, null checks filter out cleared queue slots.
    /// For value types the enumerator may yield default(T) values from slots that were concurrently dequeued,
    /// or torn values for types that are not atomically writable.
    /// Use a reference type for T if you need more reliable enumeration.
    /// </remarks>
    public Enumerator GetEnumerator() => new(this);

    async Task ExecuteSource()
    {
        // Hoisted locals are cleared before suspension and termination to avoid retaining items.
        T item;
        PipelineItemResult itemResult;
        // Preserve the executor failure across teardown.
        ExceptionDispatchInfo? fault = null;
        try
        {
            // The source continues yielding admitted items after shutdown until drained.
            while (true)
            {
                // Commit the pending tail before a possible source suspension so its task is observed.
                var commitWork = CommitPendingTail();
                if (commitWork is not null)
                {
                    // Avoid retaining the prior item across cold trailing recovery.
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        item = default!;
                    itemResult = default;
                    await commitWork.ConfigureAwait(false);
                }

                // _pulling closes the interval after removal from backlog and before depth increment.
                Volatile.Write(ref _pulling, true);
                if (!_enumerator.TryGetNext(out item!))
                {
                    Volatile.Write(ref _pulling, false);
                    // Clear hoisted locals only on the suspension path.
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        item = default!;
                    itemResult = default;
                    // Drop a resolved handoff identity before the executor parks.
                    _activationGate.ClearConsumedHandoffItem();

                    // A genuinely armed source wait proves backlog empty; combine it with depth zero.
                    var wait = _enumerator.WaitForNextAsync();
                    var waitAwaiter = wait.GetAwaiter();
                    if (!waitAwaiter.IsCompleted && _depthState.Depth is 0)
                        _depthState.OnDepthReachedZero();
                    if (!await wait)
                        break;
                    continue;
                }

                // Publish depth before clearing _pulling so one of backlog/depth/pulling always owns the item.
                _depthState.IncrementDepth();
                Volatile.Write(ref _pulling, false);

                // Publish enumeration identity before activation.
                SetExecutingItem(item);
                Volatile.Write(ref _executingItemVisible, true);

                var activated = false;
                // Generation identifies this item's provisional turn through commit and retirement.
                long executionGeneration = 0;
                // With no resident or handoff, the executor is the sole activation decider.
                if (_inFlight.Count is 0 && !_activationGate.HasHandoff)
                {
                    // The turn remains authoritative against concurrent provisional ownership.
                    var generation = _activationGate.NextGeneration();
                    if (_activationGate.TryClaimProvisionalTurn(generation))
                    {
                        executionGeneration = generation;
                        ActivateHeadItem(item, preferAsync: false);
                        activated = true;
                    }
                    else
                    {
                        executionGeneration = _activationGate.PublishHandoff(item);
                    }
                }
                else
                {
                    // Publish before the count read; reclaim uses the same claim-first order as the
                    // empty-edge pass. A lost reclaim leaves activation to that pass or a later stop.
                    executionGeneration = _activationGate.PublishHandoff(item);
                    Interlocked.MemoryBarrier();
                    if (_inFlight.Count is 0 && _activationGate.TryReclaimHandoff(executionGeneration))
                    {
                        ActivateHeadItem(item, preferAsync: false);
                        activated = true;
                    }
                }

                try
                {
                    itemResult = await _policy.ExecuteItemAsync(item, pipelineTaskRecovery: false, _enumerator.CompletionToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var owned = ClearExecutingItem(activated);
                    var failedTurn = GetActivationOwner(activated, owned, executionGeneration);
                    await RecoverItem(item, activated, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), failedTurn, _enumerator.CompletionToken).ConfigureAwait(false);
                    continue;
                }

                // Direct retirement is valid only when both tasks succeeded and no earlier resident exists.
                if (_inFlight.Count is 0
                    && itemResult.PipelineTask.IsCompletedSuccessfully && itemResult.TrailingExecutionTask.IsCompletedSuccessfully)
                {
                    if (ClearExecutingItem(activated))
                    {
                        // Reclaim proves both the task token and activation turn are ours.
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                        RetireItem(item, null, ownedTurn: GetActivationOwner(activated, ownsTurn: true, executionGeneration));
                    }
                    else
                    {
                        // A lost reclaim routes through the store; the advance license orders activation
                        // before retirement.
                        CommitInFlightItem(item, activateAtCommit: false, ownsTurn: false, turnGeneration: executionGeneration, itemResult.PipelineTask);
                    }
                }
                else if (itemResult.PipelineTask.IsCompleted && !itemResult.PipelineTask.IsCompletedSuccessfully)
                {
                    // Recovery receives unresolved trailing work so the substitute can sequence shared output.
                    var ownedFault = ClearExecutingItem(activated);
                    var faultedTurn = GetActivationOwner(activated, ownedFault, executionGeneration);
                    var outstandingTrailing = itemResult.TrailingExecutionTask;
                    try
                    {
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        await RecoverItem(item, activated, new PipelineItemFailureContext(PipelineItemFailureKind.ExecutionPipelineTask, ex, outstandingTrailing), faultedTurn, _enumerator.CompletionToken).ConfigureAwait(false);
                        continue;
                    }
                }
                else
                {
                    // Completion gates on both tasks. Unpublish enumeration before exposing the tail
                    // slot so a concurrent snapshot cannot yield the item twice.
                    Volatile.Write(ref _executingItemVisible, false);
                    _pendingTail = item;
                    _pendingTailTask = itemResult.PipelineTask;
                    _pendingTailActivated = activated;
                    _pendingTailGeneration = executionGeneration;
                    Volatile.Write(ref _hasPendingTail, true);
                }

                if (itemResult.TrailingExecutionTask != default)
                {
                    try
                    {
                        await itemResult.TrailingExecutionTask.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (_hasPendingTail)
                        {
                            await RecoverTrailingFailure(item, activated, executionGeneration, ex, _enumerator.CompletionToken).ConfigureAwait(false);
                        }
                    }
                }
            }

            // Release hoisted item state before termination.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                item = default!;
            itemResult = default;
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _enumerator.CompletionToken
            && ex.CancellationToken.IsCancellationRequested)
        {
            // Only cancellation from the fired enumeration token is a clean source shutdown.
        }
        catch (Exception ex)
        {
            // Capture before teardown so a later disposal failure cannot mask the root cause.
            fault = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            // Every exit drains and disposes; teardown failures are combined with the root failure.
            try
            {
                await DrainOnCompletionAsync();

                // If CompleteAsync is still inside enumerator.Complete, install its signal before disposal.
                var prev = Interlocked.CompareExchange(ref _shutdownSlot, Pipeline.ShutdownDone, null);
                if (prev is not null && !ReferenceEquals(prev, Pipeline.ShutdownDone))
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    if (ReferenceEquals(Interlocked.CompareExchange(ref _shutdownSlot, tcs, Pipeline.ShutdownInFlight), Pipeline.ShutdownInFlight))
                        await tcs.Task.ConfigureAwait(false);
                }
                await _enumerator.DisposeAsync().ConfigureAwait(false);

                // Release per-run references after full quiescence.
                _completionException = null;
                _policy = default!;
                _source = default!;
                _enumerator = default;
                _pendingTail = default!;
                _pendingTailTask = default;
                // Clear public slots for reference and value-type pipelines.
                SetExecutingItem(default!);
                SetActivatedItem(default!);
                _inFlight.Reset();
                _itemTenure.Reset();
                _activationGate.Reset();
                _inFlightRecoveryItem = default!;
                _advanceDrainWaiter = null;
            }
            catch (Exception teardownEx)
            {
                fault = fault is null
                    ? ExceptionDispatchInfo.Capture(teardownEx)
                    : ExceptionDispatchInfo.Capture(new AggregateException(fault.SourceException, teardownEx));
            }
        }

        // Fault the execution task after teardown while preserving the original stack.
        fault?.Throw();
    }

    /// Waits for both store residents and the non-resident executor item to drain.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        while (_inFlight.Count > 0 || Volatile.Read(ref _executingItemVisible) || _inFlight.HasAdvanceOwner)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // Full-fence arm before recheck pairs with the advancer's decrement-then-signal.
            Interlocked.Exchange(ref _advanceDrainWaiter, tcs);
            // Re-check post-publish in case the last advancer exited while we were setting up.
            if (_inFlight.Count == 0 && !Volatile.Read(ref _executingItemVisible) && !_inFlight.HasAdvanceOwner)
            {
                Volatile.Write(ref _advanceDrainWaiter, null);
                break;
            }
            await tcs.Task.ConfigureAwait(false);
            Volatile.Write(ref _advanceDrainWaiter, null);
        }
    }

    /// Registers the fixed callback once per task tenure. Arm first because completed tasks invoke inline.
    void RegisterAdvanceCallback(ValueTask task, long seq)
    {
        _itemTenure.ArmCompletionCallback(seq);
        task.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onAdvanceCallback);
    }

    /// Marks callback delivery, then acquires or deposits work on the advance license.
    void OnAdvanceCallback()
    {
        // Publish delivery before the license operation so a serving holder may safely claim the head.
        _itemTenure.MarkCompletionCallbackDelivered();
        if (!_inFlight.TryAcquireAdvanceOrRequest())
        {
            return;
        }
        Advance();
    }

    /// Retires completed FIFO heads and installs activation plus the callback at the next live head.
    /// The advance license permits one chain at a time.
    void Advance(bool emptyReached = false)
    {
        var holdsLicense = true;
        while (true)
        {
            // The license excludes rival task consumers. completionCallbackPending additionally prevents
            // retirement while the completer is still dispatching the registered callback.
            if (_inFlight.TryClaimCompletedHead(ref _itemTenure, out var claimedItem, out var claimedTask, out var claimedSequence, out var completionCallbackPending))
            {
                // Consume before decrementing count so a successor cannot start against resources
                // still owned by this task tenure. The task token is not touched again.
                Exception? taskException = null;
                try
                {
                    claimedTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    taskException = ex;
                }

                if (taskException is not null)
                {
                    if (!RecoverInFlightItem(claimedItem, taskException, claimedSequence, out var recEmpty))
                        // Recovery substitute took over this position; its continuation resumes the advance
                        // (ResumeAdvanceAfterRecovery) after retiring the substitute. The count credit stays.
                        return;
                    emptyReached |= recEmpty; // recovered inline (no substitute / sync-complete)
                }
                else
                {
                    // Retirement effects (Drive): complete BEFORE decrement so the depth-0 ActivatedItem
                    // clear runs while the store count still shuts the executor's Count==0 inline gate.
                    emptyReached |= RetireItemDeferred(claimedItem, null, ownedTurn: claimedSequence);
                }

                if (AdvanceDecrement(ref emptyReached, out holdsLicense))
                    continue; // a successor may exist: the top of the loop peeks and drives it uniformly.
                break; // idle edge: AdvanceDecrement already resolved the license (held or released).
            }

            // Callback dispatch still owns the tenure. Exit normally; its acquire-or-deposit will
            // redrive after publishing delivery.
            if (completionCallbackPending)
            {
                break;
            }

            // No completed head to claim.
            if (_inFlight.TryPeekHead(out var peekedItem, out var peekedTask))
            {
                // License-covered status read: claims are the only token consumers and we hold the
                // license, so this can never land on a consumed token.
                if (peekedTask.IsCompleted)
                    continue; // completed since the claim miss: claim it inline.
                // A resident head excludes count-zero turn assigners. If no turn exists, move the
                // activation frontier here; otherwise its existing callback owns the redrive.
                if (!_activationGate.HasTurn)
                {
                    var headSequence = _itemTenure.HeadSequence;
                    _activationGate.AssignTurnAtStop(headSequence);
                    ActivateHeadItem(peekedItem, preferAsync: true);
                    // The turn assignment makes this the sole callback registration for the tenure.
                    RegisterAdvanceCallback(peekedTask, headSequence);
                }
                break;
            }

            // A positive count with no visible head means a commit is between count increment and
            // publication. Re-peek while licensed because even an SPSC peek mutates consumer state;
            // after release the committer's acquire-or-deposit owns the redrive.
            if (_inFlight.Count > 0)
            {
                if (_inFlight.TryPeekHead(out _, out _))
                {
                    continue; // still licensed throughout - the publish landed, re-probe from the top.
                }
                if (_inFlight.ReleaseAdvance())
                    continue; // a deposit landed during this hold: kept the license, re-probe.
                holdsLicense = false;
                break; // still invisible after both checks: the committer's own arm covers it.
            }

            // count==0 idle probe (a fire landed on an already-drained store): run the empty-edge pass - a
            // deferral may be parked with no other driver.
            ResolveEmptyEdgeHandoff();
            break;
        }

        // Stops that did not release internally leave the license to this common exit.
        if (AdvanceExit(emptyReached, releaseLicense: holdsLicense))
            Advance(emptyReached);
    }

    /// Decrements resident count and resolves the idle edge. A zero-count publication is the last
    /// run-state mutation before the exit signals used by teardown.
    bool AdvanceDecrement(ref bool emptyReached, out bool holdsLicense)
    {
        // Track ownership explicitly because a served deposit keeps the license at count zero.
        holdsLicense = true;
        // Above one, a plain decrement cannot reach the edge. At the edge one CAS combines count
        // decrement with release-or-serve so no deposit is lost.
        bool idle, serve = false;
        if (_inFlight.Count > 1)
            idle = _inFlight.DecrementCount();
        else
            idle = _inFlight.DecrementCountAtEdge(out serve);
        if (!idle)
            return true; // a successor exists (count under-promises the store, never over-promises).
        // At zero, acquire or deposit before resolving the handoff. The license orders any resulting
        // activation before another advance can retire the claimed item.
        emptyReached = true;
        if (!serve && !_inFlight.TryAcquireAdvanceOrRequest())
        {
            // Deposited on a live holder: its eventual empty-edge pass covers the handoff.
            holdsLicense = false;
            return false;
        }
        ResolveEmptyEdgeHandoff();
        if (serve)
            return true; // the edge CAS consumed a deposit: licensed re-probe from the top.
        // Release-or-serve covers deposits that arrived during the handoff.
        if (_inFlight.ReleaseAdvance())
            return true;
        holdsLicense = false;
        return false;
    }

    /// Publishes depth and signals teardown after advancement is quiescent. Returns true when release
    /// serves a deposited pass and the caller must continue advancing.
    bool AdvanceExit(bool emptyReached, bool releaseLicense = false)
    {
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        var serve = releaseLicense && _inFlight.ReleaseAdvance();
        if (!serve)
            SignalDrainWakeupIfWaiting();
        return serve;
    }

    /// Resolves the activation handoff at an empty edge. The edge lock serializes its claim with
    /// executor-side provisional turns; the generation-pinned peek and take bind activation to the exact
    /// placement observed. The policy call runs outside the edge lock but under the advance license,
    /// preserving activation-before-retirement.
    void ResolveEmptyEdgeHandoff()
    {
        if (!_activationGate.HasHandoff)
        {
            return;
        }
        T captured = default!;
        var handoffLock = _activationGate.EdgeLock;
        handoffLock.Enter();
        // Claim only a free turn, then take the exact observed generation. A recycled handoff
        // fails the pinned take and releases the provisional turn.
        var claimed = false;
        var turnGeneration = 0L; // only ever read below once claimed is true, which proves it was assigned
        if (_inFlight.Count == 0 && _activationGate.TryPeekHandoff(out turnGeneration, out captured)
            && _activationGate.TryClaimProvisionalTurn(turnGeneration))
        {
            claimed = _activationGate.TryTakeHandoff(turnGeneration);
            if (!claimed)
            {
                _activationGate.Release(-turnGeneration);
                captured = default!;
            }
        }
        handoffLock.Exit();
        if (!claimed)
        {
            return;
        }
        ActivateHeadItem(captured, preferAsync: true);
        // Recovery may inherit this generation only after the original activation call returns.
        _activationGate.MarkHandoffResolved(turnGeneration);
    }

    /// Resume the advance from a recovery continuation that has just retired its substitute
    /// (CompleteRecoveryItem ran the effects). The recovery held this position's count credit, so
    /// resume at DecrementCount (AdvanceDecrement) and continue retiring or exit.
    void ResumeAdvanceAfterRecovery(bool emptyReached = false)
    {
        // The recovery episode held the advance license across the substitute (the license is
        // episode-affine, not thread-affine); resume under it and release through the normal exits.
        if (AdvanceDecrement(ref emptyReached, out var holdsLicense))
            Advance(emptyReached);
        else if (AdvanceExit(emptyReached, releaseLicense: holdsLicense))
            Advance(emptyReached);
    }
}

public static class Pipeline
{
    // Identity-only shutdown states shared by every closed pipeline type.
    internal static readonly object ShutdownInFlight = new();
    internal static readonly object ShutdownDone = new();

    /// Marker carrying a committed recovery item's own fault through the in-flight store's task path.
    /// Recovery identity travels in the task rather than pipeline state: the guard wrapper (see
    /// RecoverCommittedPendingTailAsync) rethrows the recovery's late fault as this type, and
    /// RecoverInFlightItem recognizes it and completes the item directly with the inner exception. A
    /// recovery's own failure is never re-recovered, and policies are never consulted about items they
    /// returned as recoveries.
    ///
    /// Exception-as-marker rather than a status enum on every store entry: the fault path is already
    /// exceptional and the drain already catches the task's exception, so the marker only retypes an
    /// in-flight throw. Revisit if store entries ever need more kinds than this one.
    internal sealed class RecoveryItemFaultException(Exception innerException)
        : Exception(null, innerException)
    {
        public new Exception InnerException => base.InnerException!;
    }

    /// <summary>Construct a source-driven pipeline against a caller-supplied source.</summary>
    /// <remarks>
    /// No CancellationToken parameter: the caller-supplied source owns its own cancellation
    /// lifecycle (either at source construction or by integrating their CT into the source's
    /// CreateEnumerator implementation). Threading another CT through here would be redundant.
    /// <para>
    /// Instance reuse: pass a previously-completed <paramref name="instance"/> to rebind it
    /// against the new <paramref name="policy"/> + <paramref name="source"/> and restart it.
    /// The shell allocation (in-flight queue, delegates, depth machinery) gets reused while
    /// the per-run state (enumerator, execution task) gets fresh.
    /// </para>
    /// <para>
    /// The natural call-site pattern when caller caches a nullable pipeline field:
    /// <code>_cachedPipeline = Pipeline.Create(policy, source, _cachedPipeline);</code>
    /// First call: <paramref name="instance"/> is null → fresh allocation.
    /// Subsequent calls: <paramref name="instance"/> is non-null → shell reuse.
    /// </para>
    /// <para>
    /// Throws if <paramref name="instance"/> is non-null but its previous run hasn't fully
    /// completed (caller must await the previous CompleteAsync's returned task first).
    /// </para>
    /// </remarks>
    public static Pipeline<T, TPolicy, TSource, TEnumerator> Create<T, TPolicy, TSource, TEnumerator>(TPolicy policy, TSource source, Pipeline<T, TPolicy, TSource, TEnumerator>? instance = null)
        where TPolicy : IPipelinePolicy<T>
        where TSource : IPipelineSource<T, TEnumerator>
        where TEnumerator : struct, IPipelineEnumerator<T>
    {
        if (instance is null)
            return new(policy, source);
        instance.Initialize(policy, source);
        return instance;
    }

    /// <summary>Construct a queue-backed pipeline. Returns a <see cref="UnboundedPipeline{T,TPolicy}"/> that
    /// exposes <see cref="UnboundedPipeline{T,TPolicy}.Enqueue"/> directly.</summary>
    /// <remarks>
    /// The cancellation token is passed to the internally-created source. Pipeline itself stays
    /// CT-free, and the source owns the cancellation lifecycle.
    /// </remarks>
    public static UnboundedPipeline<T, TPolicy> Create<T, TPolicy>(TPolicy policy, bool runContinuationsAsynchronously = true, PipelineScheduler? scheduler = null, CancellationToken cancellationToken = default)
        where TPolicy : IPipelinePolicy<T>
    {
        var source = UnboundedQueueSource<T>.Create(runContinuationsAsynchronously, scheduler, cancellationToken);
        var pipeline = new Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator>(policy, source);
        return new UnboundedPipeline<T, TPolicy>(pipeline, source);
    }
}
