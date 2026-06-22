using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// <summary>
/// Source-driven pipelined request/response coordinator. Processes each item from the source
/// through the policy's lifecycle (execute/activate/complete/recover). The source's enumerator owns
/// cancellation and completion; <see cref="CompleteAsync"/> drives shutdown by signalling it.
/// </summary>
/// <remarks>
/// <see cref="CompleteAsync"/> and <see cref="GetEnumerator"/> are not thread-safe - serialize
/// external callers. <see cref="Depth"/> is a lock-free read safe from any thread, but like a
/// concurrent collection's count it is a snapshot that may be stale on return. Internally the class
/// IS concurrent: the execution loop runs on its own scheduler thread, the advancer fires on
/// threadpool continuations from waiter-task completions, and recovery spans async boundaries.
/// </remarks>
public sealed class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    TPolicy _policy;
    TSource _source;
    TEnumerator _enumerator;

    // Field-initialized to a completed sentinel so the first Initialize from the constructor sees a
    // "completed previous run" and proceeds. On run completion ExecuteSource swaps it back to
    // Task.CompletedTask, releasing the prior ExecuteSource task box.
    Task _executionTask = Task.CompletedTask;
    // Shutdown coordination over a single slot, claimable by either an external CompleteAsync
    // caller or by the executor's terminal cleanup (self-shutdown via external CT cancellation).
    // Slot values:
    //   null                  = None: no shutdown in progress.
    //   _shutdownInFlight     = a CompleteAsync caller is mid-call to _enumerator.Complete.
    //   _shutdownDone         = caller (or executor on the no-caller path) finished. The
    //                            terminal cleanup may dispose.
    //   <a TaskCompletionSource> = executor's lazily-installed TCS that the caller must signal.
    // Transitions use paired Interlocked CompareExchange / Exchange (full fences), so the
    // publish-and-signal handoff has no StoreLoad reorder hazard. Allocates one TCS per shutdown
    // when a caller is in flight, none when the executor wins the CAS.
    TaskCompletionSource? _shutdownSlot;
    // Identity-only sentinels. Never signaled; their Task is never observed. The Exchange-result
    // check uses ReferenceEquals to distinguish them from a real executor-published TCS.
    static readonly TaskCompletionSource _shutdownInFlight = new();
    static readonly TaskCompletionSource _shutdownDone = new();
    Pipeline.DepthState _depthState;
    Exception? _completionException;

    // Executor-owned, only touched by the execution loop.
    // Pending tail, the most recent waiter, held outside the queue so the executor can swap it
    // for a recovery item if the trailing task fails. Committed to the waiter queue on the next
    // loop iteration.
    T _tailWaiter = default!;
    bool _hasTailWaiter;
    ValueTask _tailWaiterTask;
    // Published by the execution loop for deferred activation. The bool flag is the handshake:
    // set (true) by the executor, claimed (false) by the advancer via Interlocked.Exchange.
    // The item field is written before the flag (ordered by the Exchange barrier).
    T _executingItem = default!;
    bool _executingItemActivationPending;
    // Seqlock generation for the public ExecutingItem getter under value-type T (see PublishSlot /
    // ReadSlot). Unused for reference T (single-word atomic). _executingItem has a sole writer (the
    // executor strand; recovery is awaited inline, the advancer only reads it under _activationLock),
    // so the seqlock's single-writer assumption holds.
    uint _executingItemGen;

    /// <summary>
    /// The item currently holding the executor's single-pump slot, or default if none. The
    /// pipeline assigns this before <see cref="IPipelinePolicy{T}.ExecuteItemAsync"/> is called
    /// and keeps it through the trailing task's tail (a tail-waiter item retains the slot until
    /// the next iteration's <see cref="CommitTailWaiter"/>); recovery republishes to the
    /// substitute when <see cref="IPipelinePolicy{T}.TryRecoverItemFailure"/> returns true, and
    /// leaves the slot cleared when it returns false.
    /// </summary>
    /// <remarks>
    /// The single-pump invariant (only one item holds this slot at a time, ever) is what makes the
    /// pipeline an item-sequencer: policies use this identity as the single source of truth so they
    /// can omit their own bookkeeping or locking primitive.
    /// </remarks>
    public T ExecutingItem => ReadSlot(ref _executingItem, ref _executingItemGen);

    /// The most recently activated item (set whenever the pipeline calls
    /// <see cref="IPipelinePolicy{T}.ActivateHeadItem"/>). The Activate-before-Complete in-order
    /// invariant means this identifies the item currently in its post-activation phase. Held across
    /// the brief window between completion of the prior item and activation of the next, never reset
    /// to default while the pipeline is live, so a completion-thread callback racing the activation
    /// thread always sees a valid reference rather than a transient null.
    T _activatedItem = default!;
    // Seqlock generation for the public ActivatedItem getter under value-type T. Unused for reference
    // T. The writers of _activatedItem (ActivateHeadItem, the depth-0 clear, the post-drain reset)
    // run on different strands but never overlap for a live slot, serialized by the in-order
    // retirement discipline + Complete-before-DecrementCount.
    uint _activatedItemGen;

    /// <summary>
    /// The most recently activated item, or default if no activation has occurred yet. Mirror of
    /// <see cref="ExecutingItem"/> for the post-activation phase: ActivatedItem identifies whichever
    /// item is currently in its post-activation phase under the in-order Activate-before-Complete
    /// discipline. Policies use this identity to omit their own current-item bookkeeping (read-channel
    /// or response-writer ownership) - the executor's in-order activation IS that bookkeeping.
    /// </summary>
    /// <remarks>
    /// Holds the prior activation's reference across the brief gap between Complete(prev) and
    /// Activate(next): a stale reference there is safe because in-flight processing only happens during
    /// an activated item's phase, when the read sees a usable identity.
    /// </remarks>
    public T ActivatedItem => ReadSlot(ref _activatedItem, ref _activatedItemGen);
    // Visibility-only flag for GetEnumerator. Set on dispatch in both branches (waiters=0 and
    // waiters>0) so heartbeat-style consumers can act on the in-flight item before it transitions
    // to a tail-waiter slot. Cleared on every transition that gives the enumerator another channel
    // to see the item (sync completion, tail-waiter committal, recovery). Kept separate from
    // _executingItemActivationPending so the advancer C-path's Exchange semantics on that flag stay unchanged.
    bool _hasInFlightItem;
    // The chain-arm-to-escalator activation handoff. Set (true) by the slot drainer when its
    // chain obligation meets an in-flight first escalation (the head is being relocated to the
    // queue and cannot be named); claimed (false) via Interlocked.Exchange by whichever side
    // gets there first: the drainer's one-shot re-peek or the escalating commit's compensation
    // check. Self-clearing per dance; no per-run reset needed.
    bool _pendingHeadActivation;

    // Cross-thread atomics, touched by the executor, advancer, enqueuer, and completion callbacks.
    // The store has a single inline slot (zero alloc) for the common one-pending-waiter case. The
    // SPSC queue inside it is lazy-allocated only on true overlap (a second waiter arrives while
    // the first is still pending). See WaiterStore<T>.
    WaiterStore<T> _waiters;
    // Completions-since-pass-start dirty flag. Callbacks that bail set it (plain write, published by
    // their TryAcquire's full fence). Drain passes consume it with an Interlocked.Exchange at pass
    // start, and the post-release recheck closes the lost-wake window - the clear/fence/recheck gate.
    bool _drainSignal;

    Latch _advancing; // Held while a thread is currently the advancer. See Latch.cs for semantics.
    T _waiterRecoveryItem = default!; // The item being recovered, for the bailout/completion paths to access.
    TaskCompletionSource? _drainWakeupTcs; // Set by DrainOnCompletionAsync while waiting for the advancer chain to release and _waiters to empty. Signaled by callbacks at end-of-cycle so drain can re-check both conditions.

    // Activation.
    readonly Action _onWaiterTaskCompletedAction;

    internal Pipeline(TPolicy policy, TSource source)
    {
        // Delegate references bind to `this` and don't change.
        _onWaiterTaskCompletedAction = OnWaiterTaskCompleted;
        Initialize(policy, source);
    }

    /// Per-run init: sets policy + source, resets transient executor state, creates a fresh enumerator
    /// and execution task. Called from the constructor and by callers for flyweight reuse. Throws if the
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
        // source no longer needs a depth-increment hook. Pipeline doesn't pass a CT here: source owns
        // its own cancellation lifecycle (caller-configured at source construction).
        _enumerator = _source.GetAsyncEnumerator();
        _executionTask = ExecuteSource();

        // Other per-run fields are left at default: field-initialized on first run, or zeroed by the
        // previous run's ExecuteSource-exit cleanup.
    }

    // Off the hot path (advancer/recovery handoff), so OS handoff on contention is preferable.
    // Lazy-allocated alongside _waiters at first escalation. Stays null while no advancer can
    // exist (advancers require waiters), and the few lock sites that may run pre-escalation
    // null-guard.
    Lock? _activationLock;


    /// <summary>Current count of items in the pipeline (queued + in flight + waiting). Lock-free read,
    /// may be stale by the time the caller observes it. Use <see cref="WaitForEmptyAsync"/> to await depth zero.</summary>
    public int Depth => _depthState.Depth;

    /// <summary>
    /// Waits for the pipeline depth to reach zero (all items have received
    /// <see cref="IPipelinePolicy{T}.CompleteItem"/>). Does not prevent new items from being yielded
    /// by the source.
    /// </summary>
    /// <remarks>
    /// This is a pipeline-state query, not an executor-quiescence guarantee. The signal fires from
    /// inside <see cref="IPipelinePolicy{T}.CompleteItem"/>, so the executor may still be inside its
    /// inner drain loop and threadpool-resident advancer continuations may still be unwinding when
    /// this returns. For strict "fully quiet" GC-collectability semantics use
    /// <see cref="CompleteAsync"/>.
    /// </remarks>
    internal ValueTask WaitForEmptyAsync(CancellationToken cancellationToken = default)
        => _depthState.GetIdleTask(cancellationToken);

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
        // First-claim wins (CAS null -> InFlight). Subsequent callers (or a late call after the
        // executor self-shut-down via external CT cancellation, which transitions the slot to Done
        // from the terminal cleanup) just return the executor task.
        if (Interlocked.CompareExchange(ref _shutdownSlot, _shutdownInFlight, null) != null)
            return new(_executionTask);

        _completionException = exception;
        // Complete() signals the enumeration to wind down: cancels CompletionToken (firing policy
        // CT for in-flight ExecuteItemAsync calls) and signals the source's wake/completion
        // mechanism via the registered callback. The enumerator stays usable for drain reads.
        // DisposeAsync runs at the end of the executor's main loop as the terminal cleanup.
        _enumerator.Complete();

        // Transition InFlight -> Done. The Exchange picks up whatever was in the slot - either the
        // InFlight sentinel we put there (executor hasn't shown up) or a TCS the executor
        // published while we were inside _enumerator.Complete. In the latter case we signal it so
        // the executor unblocks. The Exchange's full fence pairs with the executor's CASes (see
        // _shutdownSlot comment) - one of the two sides always observes the other's transition.
        var swapped = Interlocked.Exchange(ref _shutdownSlot, _shutdownDone);
        if (swapped is not null && !ReferenceEquals(swapped, _shutdownInFlight))
            swapped.TrySetResult();
        return new(_executionTask);
    }

    /// <summary>Returns an enumerator over all items currently in the pipeline, from oldest to newest.</summary>
    /// <remarks>
    /// Best-effort under concurrent mutation. Both the execution queue and the waiters queue may
    /// be mutated by the execution loop or the advancer, pausing enqueues alone is not sufficient.
    /// For reference types, null checks filter out cleared queue slots.
    /// For value types the enumerator may yield default(T) values from slots that were concurrently dequeued,
    /// or torn values for types that are not atomically writable.
    /// Use a reference type for T if you need more reliable enumeration.
    /// </remarks>
    public Enumerator GetEnumerator() => new(this);

    async Task ExecuteSource()
    {
        // Promoted out of the loop so the post-loop clear below can null them out.
        T item;
        PipelineItemResult itemResult;
        // Root cause of an executor break, captured (stack preserved) so the teardown drain/dispose
        // in the finally can't mask it, then rethrown after teardown so it faults _executionTask.
        ExceptionDispatchInfo? fault = null;
        try
        {
            // The CTS-cancelled check belongs to the source's MoveNextAsync, not here: even after
            // CompleteAsync fires, items admitted before Complete() must still be processed. The
            // source returns false once its queue is empty and its wake signal is completed.
            while (true)
            {
                // Commit the previous iteration's pending tail to the waiter queue BEFORE waiting
                // on MoveNextAsync. If the source has nothing more to yield, the tail would
                // otherwise sit in _tailWaiter forever with no UnsafeOnCompleted callback wired,
                // and a producer that completes its pipeline task would have nobody listening.
                // CommitTailWaiter is sync in all paths except trailing recovery, where it
                // returns a Task to await.
                var commitWork = CommitTailWaiter();
                if (commitWork is not null)
                {
                    // Cold suspension (trailing recovery): clear the per-item locals first, same
                    // retention rationale as the wait-path clear below.
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        item = default!;
                    itemResult = default;
                    await commitWork.ConfigureAwait(false);
                }

                // The pull seam: a backlogged item is taken synchronously by TryGetNext (lock-free
                // on the queue source, no ValueTask machinery, no Current store). A miss goes to
                // WaitForNextAsync, whose awaitable re-checks under the source's wake lock and
                // either resolves synchronously (retry/completed) or suspends on the bare-delegate
                // wait protocol - the wake invokes the executor's continuation directly, with no
                // value-task-source dispatch in between.
                if (!_enumerator.TryGetNext(out item!))
                {
                    // About to (possibly) suspend. item/itemResult are hoisted into the executor's
                    // state-machine box (they live across the in-iteration awaits), so without this
                    // clear the box keeps the last-processed item and its tasks GC-rooted across
                    // the whole idle period (the post-loop clear only runs on termination).
                    // Clearing only on the miss path keeps the backlogged loop free of these dead
                    // stores. Both are reassigned before any use, and the recovery `continue`
                    // paths route back through here too.
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        item = default!;
                    itemResult = default;
                    if (!await _enumerator.WaitForNextAsync())
                        break;
                    continue;
                }

                // Count depth at DISPATCH: a source-yielded item just became in-flight. This is the
                // single-consumer chokepoint (the executor), so the increment is single-writer - no
                // producer race, no enqueue-side serialization. Recovery substitutes do NOT increment
                // here (they republish SetExecutingItem for the failed item's already-counted slot).
                _depthState.IncrementDepth();

                // Publish _executingItem and the visibility flag before either activation path.
                // _hasInFlightItem signals GetEnumerator that the in-flight item is yieldable during
                // the dispatch window where it isn't tracked anywhere else.
                SetExecutingItem(item);
                Volatile.Write(ref _hasInFlightItem, true);

                var activated = false;
                if (_waiters.Count is 0)
                {
                    ActivateHeadItem(item, preferAsync: false);
                    activated = true;
                }
                else
                {
                    // Release-only publish: subsequent readers seeing _executingItemActivationPending=true also see
                    // the _executingItem write. The full fence isn't needed here, we use the ordering
                    // not the Exchange's return value. CommitTailWaiter and the advancer C-path still
                    // use Exchange because they DO need the return value (test-and-set claim).
                    Volatile.Write(ref _executingItemActivationPending, true);
                }

                try
                {
                    itemResult = await _policy.ExecuteItemAsync(item, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ClearExecutingItem(activated);
                    await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), _enumerator.CompletionToken).ConfigureAwait(false);
                    continue;
                }

                // Sync shortcut: only taken when both tasks are already observed successful at dispatch
                // time. Items with a non-default trailing task fall through to the tail-waiter path until
                // their trailing is also sync-complete (default(ValueTask) is success, so items without a
                // trailing keep the fast path). This is how the framework guarantees CompleteWaiter doesn't
                // fire before trailing is observed.
                //
                // Gated on _waiters.Count is 0 to keep retirement strictly in-order: the store plus the
                // head-gated drain is a reorder buffer (entries complete out of order, buffer, and retire
                // in program order from the head). This inline shortcut bypasses the ROB, so it's only
                // safe when the ROB is empty (Count is 0 = this item is the head). With an earlier entry
                // buffered, retiring inline would jump ahead of it, so we fall through to route through
                // the ROB instead. The shortcut is a perf optimization, not a correctness path - an
                // ordered transport never produces a later item sync-completing before an earlier one.
                if (_waiters.Count is 0
                    && itemResult.PipelineTask.IsCompletedSuccessfully && itemResult.TrailingExecutionTask.IsCompletedSuccessfully)
                {
                    itemResult.PipelineTask.GetAwaiter().GetResult();
                    ClearExecutingItem(activated);
                    CompleteWaiter(item, null);
                }
                else if (itemResult.PipelineTask.IsCompleted && !itemResult.PipelineTask.IsCompletedSuccessfully)
                {
                    // Pipeline task faulted synchronously. Recovery path. The failed item's
                    // TRAILING task may still be in-flight (the per-iteration await at the
                    // loop's bottom is skipped via `continue` below); pass it through the
                    // failure context so the substitute can sequence against it before
                    // touching the shared output - else recovery writes race the still-
                    // running trailing flush on the writer. Pass the ValueTask directly (no
                    // AsTask conversion): the recovery is the sole awaiter, single-consume
                    // is preserved by construction.
                    ClearExecutingItem(activated);
                    var outstandingTrailing = itemResult.TrailingExecutionTask;
                    try
                    {
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex, outstandingTrailing), _enumerator.CompletionToken).ConfigureAwait(false);
                        continue;
                    }
                }
                else
                {
                    // Tail waiter path. Either the pipeline task is pending, or it's sync-complete but
                    // trailing is pending/faulted - either way completion gates on both. The framework's
                    // trailing-await below stalls the executor before the next CommitTailWaiter (which
                    // fires CompleteWaiter), so trailing is structurally observed before completion.
                    //
                    // Clear _hasInFlightItem BEFORE publishing _hasTailWaiter so a concurrent enumerator
                    // never sees both true at once - the reverse order would let a heartbeat-style
                    // enumerator yield the same item twice (phase 0 and phase 4) and fire the consumer's
                    // per-tick callback twice. _executingItem stays populated for the advancer C-path
                    // when waiters>0; CommitTailWaiter clears it under the lock.
                    Volatile.Write(ref _hasInFlightItem, false);
                    _tailWaiter = item;
                    _tailWaiterTask = itemResult.PipelineTask;
                    Volatile.Write(ref _hasTailWaiter, true);
                }

                if (itemResult.TrailingExecutionTask != default)
                {
                    try
                    {
                        await itemResult.TrailingExecutionTask.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (_hasTailWaiter)
                        {
                            await RecoverTrailingFailure(item, activated, ex, _enumerator.CompletionToken).ConfigureAwait(false);
                        }
                    }
                }
            }

            // Clear the locals so the executor's async state machine box does not
            // retain the last-processed item and its tasks across termination.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                item = default!;
            itemResult = default;
            // No post-loop CommitTailWaiter needed: the top-of-loop commit already ran before the
            // MoveNextAsync that returned false.
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _enumerator.CompletionToken)
        {
            // Sanctioned shutdown: a source may signal completion either by returning false from
            // WaitForNextAsync or, the idiomatic IAsyncEnumerator way, by throwing OCE carrying the
            // enumerator's own CompletionToken. Identity-matched (not just IsCancellationRequested) so a
            // foreign or untokened OCE is NOT swallowed - it falls through to the capture below and faults.
        }
        catch (Exception ex)
        {
            // Any other throw from a source/policy/loop seam (TryGetNext, WaitForNextAsync, commit, a
            // recovery that itself escaped) breaks the pipeline. Capture the root cause now, before the
            // teardown drain/dispose runs, so a teardown fault can't mask which seam actually broke.
            fault = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            // Teardown runs on EVERY exit - clean, sanctioned-shutdown, or fault - so a faulted loop
            // still drains in-flight items (decoupled from the source, they retire normally) and
            // disposes the enumerator, instead of leaking the run. Guarded so a teardown throw is folded
            // into the captured fault rather than replacing it.
            try
            {
                await DrainOnCompletionAsync();

                // Close the shutdown gate before disposing the enumerator. CAS null -> Done:
                //   - Wins (slot was null): no caller in flight; dispose immediately.
                //   - Loses with Done: caller already finished; dispose immediately.
                //   - Loses with InFlight: caller is mid-_enumerator.Complete; install a TCS via a second
                //     CAS over the InFlight sentinel. If that CAS wins, await the TCS - the caller's
                //     Exchange to Done picks our TCS up and signals it. If that CAS loses, the caller
                //     raced to Done between our two CASes, so skip the await.
                var prev = Interlocked.CompareExchange(ref _shutdownSlot, _shutdownDone, null);
                if (prev is not null && !ReferenceEquals(prev, _shutdownDone))
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    if (ReferenceEquals(Interlocked.CompareExchange(ref _shutdownSlot, tcs, _shutdownInFlight), _shutdownInFlight))
                        await tcs.Task.ConfigureAwait(false);
                }
                await _enumerator.DisposeAsync().ConfigureAwait(false);

                // ExecuteSource has fully completed: all items drained, enumerator disposed, advancer
                // chain quiesced. Default the per-run reference-holding fields so a cached-but-idle
                // Pipeline shell doesn't hold them across an idle period. Trimmed to the safe minimum:
                // only reference-typed and reference-containing fields that could be promoted to gen1/2.
                // _shutdownSlot is already at Done from the gate-close above, so a racing CompleteAsync
                // first-call's CAS null -> InFlight fails and they return the execution task without
                // touching the defaulted fields.
                _completionException = null;
                _policy = default!;
                _source = default!;
                _enumerator = default;
                _tailWaiter = default!;
                _tailWaiterTask = default;
                // Public surface: cleared unconditionally so value-type T pipelines don't expose a
                // stale slot value post-shutdown.
                SetExecutingItem(default!);
                SetActivatedItem(default!);
                _waiters.Reset();
                _waiterRecoveryItem = default!;
                _drainWakeupTcs = null;
                // Reset the sentinel ONLY on a clean exit. On a fault, leave _executionTask pointing at
                // this faulting task so CompleteAsync (which returns it) surfaces the fault; rethrow below.
                // A settled async-Task box releases its state machine, so the faulted task roots no
                // per-run state (only a collectable Pipeline self-cycle) - no separate task needed.
                if (fault is null)
                    _executionTask = Task.CompletedTask;
            }
            catch (Exception teardownEx)
            {
                fault = fault is null
                    ? ExceptionDispatchInfo.Capture(teardownEx)
                    : ExceptionDispatchInfo.Capture(new AggregateException(fault.SourceException, teardownEx));
            }
        }

        // Rethrow the captured root (or root + teardown aggregate) with its original stack, faulting
        // _executionTask. No-op on the clean / sanctioned-shutdown paths.
        fault?.Throw();
    }

    /// Drains remaining items after the execution loop exits. Waits for the advancer chain to
    /// quiesce via a TCS that DrainReadyWaiters / BailoutRecoveryOnShutdown signal on release of
    /// _advancing. Recovery continuations always complete their own items (via CompleteRecoveryWaiter
    /// or BailoutRecoveryOnShutdown), so drain doesn't compete, it just waits for the chain to
    /// finish. After advancer-idle, drains _waiters.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        // Wait for both the advancer chain to be idle (no callback mid-flight) and _waiters to be empty
        // (every committed item's waiter task completed and its callback drained it). Each callback
        // signals _drainWakeupTcs on release, waking this loop to re-check. The count condition makes
        // drain wait for in-flight pipeline tasks to finish naturally - dequeueing items ourselves would
        // race the body's still-running pipeline task and tear down state it depends on.
        while (_advancing.IsHeld || _waiters.Count > 0)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // Full fence, NOT Volatile.Write: this is the arm side of a Dekker-shaped arm-then-check
            // against the callbacks' release-then-signal. A release-only publish lets the re-check's
            // loads hoist ABOVE the store - the check reads a stale IsHeld/Count, the releasing
            // callback reads the not-yet-visible null tcs, and the wakeup is lost with both sides
            // convinced the other had it. The signal side is already fenced by the latch release's
            // Interlocked.Exchange, so this is the matching half.
            Interlocked.Exchange(ref _drainWakeupTcs, tcs);
            // Re-check post-publish in case the last callback released while we were setting up.
            if (!_advancing.IsHeld && _waiters.Count == 0)
            {
                Volatile.Write(ref _drainWakeupTcs, null);
                break;
            }
            await tcs.Task.ConfigureAwait(false);
            Volatile.Write(ref _drainWakeupTcs, null);
        }
    }

    /// Handles execution-phase or pipeline-task failures, including recovery.
    /// Recovery items get the full async treatment since they're taking the place of the original item.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverItem(T item, PipelineItemFailureContext context, CancellationToken cancellationToken)
    {
        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            CompleteWaiter(item, context.Exception);
            return;
        }

        // Recovery item takes over. Republish _executingItem / _hasInFlightItem (the failed item's
        // ClearExecutingItem cleared them), then mirror the main loop's activation gating:
        // inline-activate only when no prior pipeline task is in flight, otherwise publish for
        // deferred activation. Activating eagerly while a prior waiter still owns the read channel
        // overwrites its activation state and races the in-flight decoder consumer.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _hasInFlightItem, true);
        var recoveryActivated = false;
        if (_waiters.Count is 0)
        {
            ActivateHeadItem(recoveryItem, preferAsync: false);
            recoveryActivated = true;
        }
        else
        {
            Volatile.Write(ref _executingItemActivationPending, true);
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
                await result.TrailingExecutionTask.ConfigureAwait(false);

            if (result.PipelineTask.IsCompleted)
            {
                ClearExecutingItem(recoveryActivated);
                result.PipelineTask.GetAwaiter().GetResult();
                CompleteWaiter(recoveryItem, null);
            }
            else
            {
                // Tail-waiter transition. Mirror the main loop's _hasInFlightItem clear so the
                // enumerator doesn't double-yield the item across the dispatch window. The task
                // is guarded: the substitute re-enters the normal tail lifecycle, and its own
                // late fault must complete it directly rather than re-enter recovery.
                Volatile.Write(ref _hasInFlightItem, false);
                _tailWaiter = recoveryItem;
                _tailWaiterTask = GuardRecoveryTask(result.PipelineTask);
                Volatile.Write(ref _hasTailWaiter, true);
            }
        }
        catch (Exception recoveryEx)
        {
            // No in-flight tail-waiter to observe here: the only _tailWaiter publish in the try
            // is in the trailing pending branch, which doesn't throw.
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
        }
    }

    /// Handles trailing execution task failures, including tail waiter recovery.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverTrailingFailure(T item, bool activated, Exception ex, CancellationToken cancellationToken)
    {
        // Preserve the pipeline task: the framework re-uses the materialized Task locally below
        // (no-recovery branch's CommitWaiter), so we want a stable handle that outlives this
        // method's locals. Wrap it back into a ValueTask for the context's per-construction-
        // site carrier - the recovery awaits the Task-backed ValueTask idempotently, the
        // framework's CommitWaiter takes the Task directly.
        var pipelineTask = _tailWaiterTask.Preserve();
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.TrailingExecutionTask, ex, pipelineTask);
        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            // Pipeline task may still be pending, enqueue as a waiter rather than completing prematurely.
            // Items can handle their own interdependency between the two tasks if needed.
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            CommitWaiter(item, activated, pipelineTask);
            return;
        }

        // Swap the tail: replace _executingItem with the recovery item under the activation lock
        // so the advancer's count==0 claim can't observe partial state and double-activate.
        // On inline activation also clear the published slots so a later advancer claim cannot
        // re-read recovery and activate it twice.
        // If _activationLock is still null we never escalated, so no advancer can be running and
        // the lock body runs uncontended without it.
        bool recoveryActivated;
        if (_activationLock is { } activationLock)
        {
            lock (activationLock)
            {
                SetExecutingItem(recoveryItem);
                recoveryActivated = !Interlocked.Exchange(ref _executingItemActivationPending, true);
                if (recoveryActivated)
                {
                    ActivateHeadItem(recoveryItem, preferAsync: false);
                    // Inside _activationLock. Plain write suffices: all readers of _executingItemActivationPending
                    // are Interlocked.Exchange (acquire-semantics, sees latest committed value), and
                    // the lock exit's release fence orders this write w.r.t. anyone acquiring next.
                    _executingItemActivationPending = false;
                    // _executingItem stays populated: the recovery's ExecuteItemAsync below needs it
                    // for write-phase gating (same C-path pattern).
                }
            }
        }
        else
        {
            SetExecutingItem(recoveryItem);
            recoveryActivated = !Interlocked.Exchange(ref _executingItemActivationPending, true);
            if (recoveryActivated)
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                // No-lock path: a null _activationLock means no escalation ever happened, so no
                // advancer exists to read this flag. Single writer, plain write suffices.
                _executingItemActivationPending = false;
            }
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
            {
                try
                {
                    await result.TrailingExecutionTask.ConfigureAwait(false);
                }
                catch (Exception trailingEx)
                {
                    _hasTailWaiter = false;
                    _tailWaiterTask = default;
                    ClearExecutingItem(recoveryActivated);
                    CompleteWaiter(recoveryItem, trailingEx);
                    return;
                }
            }

            if (result.PipelineTask.IsCompleted)
            {
                _hasTailWaiter = false;
                _tailWaiterTask = default;
                ClearExecutingItem(recoveryActivated);
                Exception? pipelineEx = null;
                try
                {
                    result.PipelineTask.GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    pipelineEx = e;
                }
                CompleteWaiter(recoveryItem, pipelineEx);
                return;
            }

            // Guarded for the same reason as RecoverItem's tail transition: the substitute's own
            // late fault completes it directly, never re-enters recovery.
            _tailWaiter = recoveryItem;
            _tailWaiterTask = GuardRecoveryTask(result.PipelineTask);
        }
        catch (Exception recoveryEx)
        {
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
        }
    }

    /// Commits the pending tail (if any) to the waiter queue. Called by the executor at the
    /// start of each iteration and before going idle. Returns null when the commit completed
    /// synchronously (no tail, EnqueueWaiter, or sync-complete pipeline task), or a Task the
    /// caller must await for the trailing-recovery path. Returning Task? instead of ValueTask
    /// lets the caller skip the await ceremony entirely on the common sync paths via a null check.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Task? CommitTailWaiter()
    {
        if (!_hasTailWaiter)
            return null;

        var item = _tailWaiter;
        var task = _tailWaiterTask;
        _hasTailWaiter = false;
        _tailWaiterTask = default;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _tailWaiter = default!;

        // Check whether the waiter path already activated this item via _executingItem.
        var alreadyActivated = !Interlocked.Exchange(ref _executingItemActivationPending, false);
        // Only clear _executingItem when our Exchange won. If the advancer won, it reads
        // _executingItem under its lock for the C-path activation, clearing here would NRE it.
        if (!alreadyActivated)
            SetExecutingItem(default!);

        // Fence-acquire so CompleteWaiter below cannot fire CompleteItem before the advancer's
        // in-progress ActivateHeadItem finishes. Same pattern as ClearExecutingItem's deferred branch.
        // alreadyActivated also covers the "never published" case (inline activation took the
        // count==0 branch and never set _executingItemActivationPending). When _activationLock is still null no
        // advancer has ever run, so the fence has nothing to synchronize with and the skip is safe.
        if (alreadyActivated && _activationLock is { } activationLock)
            lock (activationLock) { }

        // Post-fence clear for the alreadyActivated case: the advancer (if it ever had the
        // publish) finished its ActivateHeadItem behind the fence above, and any future C-path
        // Exchange win requires a fresh publish that rewrites _executingItem first - nothing
        // reads the current value anymore. Without this the inline-activated case (no publish,
        // so the Exchange above "loses" vacuously and the !alreadyActivated clear is skipped)
        // strands the committed item in _executingItem across the whole idle period.
        if (alreadyActivated)
            SetExecutingItem(default!);

        if (task.IsCompleted)
        {
            if (task.IsCompletedSuccessfully)
            {
                task.GetAwaiter().GetResult();
                CompleteWaiter(item, null);
                return null;
            }

            return RecoverCommittedTailWaiterAsync(item, task).AsTask();
        }

        CommitWaiter(item, activated: alreadyActivated, task);
        return null;
    }

    // Awaited inline by the executor (via CommitTailWaiter) so the recovery happens on the
    // executor's logical thread. This preserves the SPSC contract on _waiters (executor is sole
    // producer), keeps pipeline ordering correct (recovery enqueues before subsequent items are
    // committed), and avoids the dual-producer hazard that a fire-and-forget continuation pattern
    // would create.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverCommittedTailWaiterAsync(T item, ValueTask task)
    {
        Exception exception;
        try
        {
            task.GetAwaiter().GetResult();
            exception = null!; // Unreachable.
        }
        catch (Exception ex)
        {
            exception = ex;
        }

        // A recovery item's own guarded tail faulting between SetTail and this commit (the
        // recovery substitutes re-enter the normal tail lifecycle; see GuardRecoveryTask).
        // Complete directly with the real fault - never re-recovered, never consulted.
        if (exception is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            CompleteWaiter(item, recoveryFault.InnerException);
            return;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, exception);
        if (!_policy.TryRecoverItemFailure(context, item, _enumerator.CompletionToken, out var recoveryItem))
        {
            CompleteWaiter(item, exception);
            return;
        }

        // Republish and gate activation on the count, mirroring RecoverItem: activating
        // unconditionally while prior waiters are in flight would put a second active reader on the
        // wire.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _hasInFlightItem, true);
        var recoveryActivated = false;
        if (_waiters.Count is 0)
        {
            ActivateHeadItem(recoveryItem, preferAsync: false);
            recoveryActivated = true;
        }
        else
        {
            Volatile.Write(ref _executingItemActivationPending, true);
        }

        PipelineItemResult result;
        try
        {
            result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
            return;
        }

        if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
        {
            try
            {
                await result.TrailingExecutionTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ClearExecutingItem(recoveryActivated);
                CompleteWaiter(recoveryItem, ex);
                return;
            }
        }

        var pipelineTask = result.PipelineTask;
        if (pipelineTask.IsCompleted)
        {
            ClearExecutingItem(recoveryActivated);
            Exception? pipelineException = null;
            try
            {
                pipelineTask.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                pipelineException = ex;
            }
            CompleteWaiter(recoveryItem, pipelineException);
        }
        else if (_enumerator.CompletionToken.IsCancellationRequested)
        {
            // Pipeline shutdown while recovery's async work was in flight. EnqueueWaiter at this
            // point would leak: OnWaiterTaskCompleted bails on wake completion, and DrainOnCompletionAsync
            // has likely already passed. Complete directly so depth tracking and CompleteItem still fire.
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, _completionException);
        }
        else
        {
            // The recovery enters the store as an ordinary waiter; its identity travels in the TASK
            // (the guard wrapper rethrows late faults as RecoveryItemFaultException, see the marker
            // type), not in pipeline state - no fields, no value-T-unsound item comparisons.
            //
            // Consume the publish with CommitTailWaiter's Exchange discipline before the commit (this
            // runs inside the executor's commit await, so the tail slot is not an option). A lost
            // Exchange means the advancer's C-path is mid-activation under its lock; the empty lock
            // acquire synchronizes-with its release so the commit below cannot race ActivateHeadItem.
            var alreadyActivated = !Interlocked.Exchange(ref _executingItemActivationPending, false);
            if (!alreadyActivated)
                SetExecutingItem(default!);
            if (alreadyActivated && !recoveryActivated && _activationLock is { } activationLock)
                lock (activationLock) { }
            // Post-fence clear, mirroring CommitTailWaiter: the inline-activated case never
            // published, so the Exchange "loses" vacuously and the clear above is skipped -
            // without this the recovery item strands in _executingItem across the idle period.
            if (alreadyActivated)
                SetExecutingItem(default!);
            Volatile.Write(ref _hasInFlightItem, false);
            CommitWaiter(recoveryItem, activated: alreadyActivated, GuardRecoveryTask(pipelineTask));
        }
    }

    /// Wraps a recovery item's pending pipeline task before it enters the normal tail/waiter
    /// lifecycle: any late fault is rethrown as the framework's marker so CommitTailWaiter's
    /// faulted-at-commit branch and RecoverWaiter complete the recovery directly instead of
    /// consulting the policy - a recovery's own failure is never re-recovered. Allocates only
    /// on the failure path (which already allocates recovery machinery).
    static async ValueTask GuardRecoveryTask(ValueTask pipelineTask)
    {
        try
        {
            await pipelineTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new Pipeline.RecoveryItemFaultException(ex);
        }
    }

    void CompleteWaiter(T item, Exception? exception)
    {
        if (CompleteWaiterDeferred(item, exception))
            _depthState.OnDepthReachedZero();
    }

    /// Same as <see cref="CompleteWaiter"/> but skips the <see cref="DepthState.OnDepthReachedZero"/>
    /// signal so the caller can defer it (returns true iff depth reached 0). Drain paths holding the
    /// advancer latch or _activationLock use this: firing OnDepthReachedZero there would resume external
    /// WaitForEmptyAsync awaiters while internal sync is still held. Defer until the drainer releases.
    bool CompleteWaiterDeferred(T item, Exception? exception)
    {
        var depth = _depthState.DecrementDepth();
        // A negative depth means a double-decrement, which would feed garbage to the policy and let
        // the zero-signal comparison miss forever (a stranded WaitForEmptyAsync). The comparison stays
        // `is 0` deliberately: <= would mask the same corruption by double-signaling.
        Debug.Assert(depth >= 0, "Pipeline depth under-ran: double completion for a single enqueue.");
        // Release the ActivatedItem slot only at the in-order retirement terminal: when this completion
        // drains the pipeline to empty (depth 0). CompleteItem is the single in-order retirement seam,
        // and the slot retires off it like depth and the policy's per-item resources. Correct and
        // uniform across reference and value T via two properties:
        //   - No stomp. Completion is strictly head-ordered (the store drains head-first, and the sync
        //     fast-path is gated on _waiters.Count is 0), so any completion that is NOT the slot's owner
        //     is an earlier item completing while the newest-activated owner is still live. Clearing
        //     only at depth 0 means it never nulls the live owner's slot.
        //   - Never null while live. The field's contract (see the ActivatedItem remarks) is that a
        //     stale non-null reference between completion and the next activation is safe and preferred
        //     over a transient null: the sole reader (PgDecoder.CurrentExecutionControl) reads the slot
        //     only during an active read, never in that gap, so a transient null while live is the real
        //     NRE hazard. depth-0 clears only when nothing is live, so it never produces that null.
        //     (Identity or a per-item generation would clear promptly but reintroduce null-while-live.)
        //     Struct-T safe by construction - a plain counter, no reference identity to box.
        if (depth is 0)
        {
            SetActivatedItem(default!);
        }
        _policy.CompleteItem(item, depth, exception);
        return depth is 0;
    }

    /// Commits a waiter to the store and coordinates activation with the execution loop. The
    /// store routes to the inline slot when empty (zero alloc), otherwise it escalates to the
    /// SPSC queue (allocates queue + lock on first overlap). All call sites run on the executor's
    /// logical thread, preserving the SPSC single-producer contract. The wired completion
    /// callback dispatches on _waiters.IsEscalated at fire time (slot occupant -> DrainSlotInline,
    /// queued or post-escalation moved-to-head -> DrainReadyWaiters).
    void CommitWaiter(T item, bool activated, ValueTask waiterTask)
    {
        // Allocate the activation lock on first commit (slot or queue): the count==0 handoff
        // serializes against the executor's deferred-publish + ClearExecutingItem fence-acquire,
        // and deferred-publish kicks in for any subsequent iter once Count > 0. One small Lock
        // allocation per Pipeline lifetime, amortized across all subsequent commits and runs.
        _activationLock ??= new Lock();

        var count = _waiters.TryEscalateOrEnqueue(item, waiterTask, out _, out var slotWasMoved);
        var wasEmpty = count is 1;

        // The atomic _executingItemActivationPending claim already happened in CommitTailWaiter /
        // ClearExecutingItem. Just clear for hygiene and inline-activate if we're the head.
        if (!activated)
        {
            _executingItemActivationPending = false;
            SetExecutingItem(default!);

            if (wasEmpty && !waiterTask.IsCompleted)
            {
                ActivateHeadItem(item, preferAsync: true);
            }
        }

        if (waiterTask.IsCompleted)
            _onWaiterTaskCompletedAction();
        else
            waiterTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onWaiterTaskCompletedAction);

        // Compensation check for the slot drainer's chain arm: if our escalation's move raced
        // the drainer's head peek, the drainer published its activation obligation instead of
        // waiting out our two enqueue writes. We own the moved pair (TakeMovedSlotPair), so
        // claim and activate it here (IsCompleted skip as everywhere: a completed moved head
        // is drained, not activated - the nudge below picks it up). Exactly-once via the flag
        // Exchange against the drainer's one-shot re-peek.
        if (slotWasMoved)
        {
            _waiters.TakeMovedSlotPair(out var movedItem, out var movedTask);
            if (Interlocked.Exchange(ref _pendingHeadActivation, false) && !movedTask.IsCompleted)
                ActivateHeadItem(movedItem, preferAsync: true);
        }

        // During first escalation a slot callback can fire after the queue is published but
        // before the slot contents are moved into it, bumping completedCount without finding
        // anything to drain. Without this nudge the slot item would wait for the next callback
        // fire, unbounded when the next item is a long-lived, long-running exclusive operation.
        // Losing the acquire deposits the obligation on the holder (including a stale-verdict
        // reclaim hold).
        if (slotWasMoved && Volatile.Read(ref _drainSignal) && _advancing.TryAcquireOrFlagPending())
            DrainReadyWaiters();
    }

    /// Called when a committed waiter's task completes. Dispatches to the drain matching the
    /// store's tier at fire time: slot occupant (pre-escalation) -> DrainSlotInline; queued
    /// entry, or a slot occupant whose contents were moved to queue head by a later
    /// escalation, -> DrainReadyWaiters.
    void OnWaiterTaskCompleted()
    {
        // Plain store: the acquire RMW below is the full fence that publishes it. The flag
        // keeps the materializing-token role (gates the nudge and the reclaim); the lost-wake
        // hole the flag alone could not close now lives in the latch word.
        _drainSignal = true;

        // Try to become the advancer, only one thread processes completions at a time.
        // Process even during shutdown: drain's wait-for-advancer-idle is what coordinates with
        // us, and bailing out here would let drain "complete" the item via DrainOnCompletionAsync's
        // queue sweep while the body's pipeline task is still running, stranding the item in a
        // half-completed state (activated slot cleared but its body still reading).
        if (!_advancing.TryAcquireOrFlagPending())
        {
            // Obligation deposited in the latch word; the holder's release serves it.
            return;
        }

        if (_waiters.IsEscalated)
            DrainReadyWaiters();
        else
            DrainSlotInline();
    }

    /// Slot-mode drain. Advancer latch is held by the caller. CAS-claims the slot and processes
    /// the (item, task). A concurrent escalation that won the CAS leaves nothing here to drain
    /// and we re-route to the queue drain.
    void DrainSlotInline()
    {
        while (true)
        {
        if (!_waiters.TryClaimSlotForDrain(out var item, out var task))
        {
            // Escalation got there first, and head of queue is our item now.
            if (_waiters.IsEscalated)
            {
                DrainReadyWaiters();
                return;
            }
            // Empty, a still-running occupant (tri-state peek bail), or claimed elsewhere.
            // (No signal bookkeeping: a stale dirty flag costs one spurious reclaim check.)
            var failServeDeposit = _advancing.ReleaseAndCheckPending();
            SignalDrainWakeupIfWaiting();
            // A deposit during our hold transfers the obligation here; re-enter the claim
            // loop to serve it. Losing the re-acquire re-deposits on the winner.
            if (failServeDeposit && _advancing.TryAcquireOrFlagPending())
            {
                continue;
            }
            return;
        }

        // Pass start: consume the signal (full fence orders the clear before this pass's
        // reads). Completions that fire during the pass re-set it and the post-release
        // recheck below catches them - the clear/fence/recheck gate.
        Interlocked.Exchange(ref _drainSignal, false);

        // Consume the waiter task BEFORE DecrementCount publishes the freed position to the
        // executor's Count==0 inline-activation gate. The consume is the release point for
        // per-item resources tied to the task (pooled/shared IValueTaskSource implementations
        // reset in GetResult), and a successor dispatched the instant the count drops must
        // find them released. The queue drain already orders this way (consume at the head,
        // DecrementCount after processing): without it a successor could reuse a per-item shared
        // resource before its release, surfacing an already-started fault.
        Exception? taskException = null;
        try
        {
            task.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            taskException = ex;
        }

        // Fault decision BEFORE the decrement, mirroring the queue drain. Decrementing first would
        // publish the freed position mid-recovery: the executor's Count==0 inline-activation gate
        // could dispatch a successor against the recovery's own activation (a second active reader),
        // and the recovery rejoin's AdvanceAndDrain would decrement the SAME position again (a double
        // decrement, which the -1 skew arm then misattributes to a counted successor).
        bool emptyReached = false;
        if (taskException is not null && !RecoverWaiter(item, taskException, out emptyReached))
        {
            // Recovery item taking over; the count still carries this position. The advancer
            // flag stays held, and the recovery continuation decrements exactly once and
            // re-runs the activation partition via AdvanceAndDrainRecovery's slot-mode rejoin.
            return;
        }

        // Complete BEFORE Decrement. CompleteWaiterDeferred's _activatedItem clear is depth-gated
        // (it only fires when this completion drains the pipeline to empty), so an intermediate
        // completion cannot stomp a live successor in the first place. The depth-0 boundary case
        // still needs this ordering: were the store count decremented first, the inline-activation
        // gate (Count==0) could fire on another path - executor's deferred-publish branch, a
        // successor commit's wasEmpty inline-activate - admitting a brand-new item and writing
        // _activatedItem = NEW between our DecrementDepth-to-0 and the gated clear, which would then
        // null NEW. NEW's body's first decoder read would see _activatedItem=null and NRE in
        // CurrentExecutionControl. Running the complete (hence the clear) while the store count still
        // carries this position keeps that gate shut until after the clear. Mirrors the recovery-fault
        // path's Complete-before-Decrement ordering.
        if (taskException is null)
            emptyReached = CompleteWaiterDeferred(item, null);

        var count = _waiters.DecrementCount();
        // -1 floor, not 0: a claimable-but-uncounted commit (fields/flag precede the increment)
        // consumed here sends the count transiently negative, bounded at -1 by the single producer.
        Debug.Assert(count >= -1);
        // No count==0 assertion: TryClaimSlotForDrain already emptied the slot, so a successor's
        // commit can CAS into it (or first-escalate) before our decrement lands - the store's
        // _hasSlot CAS is the ownership contract, not the count. The returned count is the
        // activation-responsibility partition below, same as DrainReadyWaiters' drain loop.

        ActivateNextAfterSlotAdvance(count);

        // Capture the deposit consumed by the release: a contended acquirer (a callback, the nudge)
        // that bailed against this hold flagged the latch word. The obligation must be SERVED, not
        // delegated to the flag - the slot drain's own claim cleared _drainSignal at consume time, so
        // a deposit whose companion flag the claim ate would vanish if the reclaim below read only the
        // flag (stranding the queue head). OR-ing the deposit into the reclaim gate carries the
        // obligation past the flag's self-consumption.
        var serveDeposit = _advancing.ReleaseAndCheckPending();
        // Signal AFTER advancer release: prevents a WaitForEmptyAsync awaiter from resuming and
        // committing a new slot waiter whose callback would then race the still-held advancer
        // (the callback's failed acquire deposits, costing the holder a serve pass).
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalDrainWakeupIfWaiting();

        // Serve a consumed deposit unconditionally. ReleaseAndCheckPending cleared the pending bit,
        // so the obligation is ours and must not be gated on cancellation. During shutdown the drain
        // never dequeues, so a dropped wake strands the waiter and hangs completion on Count > 0.
        if (serveDeposit)
        {
            if (_advancing.TryAcquireOrFlagPending())
            {
                if (_waiters.IsEscalated)
                {
                    DrainReadyWaiters();
                    return;
                }
                continue;
            }
            return;
        }

        // Dirty-flag reclaim: a successor's waiter that completed while we held the advancer set the
        // signal from its callback, and without reclaim its activation is lost. A set signal in
        // non-escalated slot mode means that waiter is the current slot occupant with a completed
        // task, so looping back to the claim is safe. Gated off during shutdown, where the drain
        // sweep owns completions and the deposit-serve above already covers any real bail.
        if (!_enumerator.CompletionToken.IsCancellationRequested
            && Volatile.Read(ref _drainSignal)
            && _advancing.TryAcquireOrFlagPending())
        {
            if (_waiters.IsEscalated)
            {
                DrainReadyWaiters();
                return;
            }
            continue;
        }
        return;
        }
    }

    /// The slot-mode activation partition on a DecrementCount result, shared by DrainSlotInline's
    /// advance path and the recovery rejoin (AdvanceAndDrain's slot branch). Caller holds the
    /// advancer latch.
    void ActivateNextAfterSlotAdvance(int count)
    {
        if (count > 0)
        {
            // Slot-mode D-path, mirroring DrainReadyWaiters' count > 0 arm: the successor's
            // commit-increment preceded our decrement, so that commit observed the claimed-but-
            // uncounted position (count >= 2, wasEmpty false) and skipped self-activation - the
            // decrement's return value designates us its activator. Without this arm both sides skip
            // and the activation decision is lost (a hang with a null pending-activation control).
            if (_waiters.TryPeekSlotForActivation(out var next, out var nextTask))
            {
                // Mirror CommitWaiter's guard: a completed-at-commit waiter is never
                // activated - its callback already fired (and bailed against our held
                // latch), so the post-release reclaim below drains it.
                if (!nextTask.IsCompleted)
                    ActivateHeadItem(next, preferAsync: true);
            }
            else
            {
                // A first escalation is (or was) relocating the head to the queue; hand the
                // obligation to the escalating commit instead of waiting out its move.
                // Publish the obligation, then re-peek ONCE: the flag ops are full fences,
                // so an invisible head means the escalator's enqueue is not yet ordered
                // before our peek - its compensation check (CommitWaiter, slotWasMoved) is
                // still ahead and will claim the flag. A visible head means we claim our
                // own flag back; whichever Exchange wins owns the activation, exactly once.
                // No thread ever waits on another thread's progress.
                Interlocked.Exchange(ref _pendingHeadActivation, true);
                if (_waiters.TryPeek(out var head)
                    && Interlocked.Exchange(ref _pendingHeadActivation, false)
                    && !head.WaiterTask.IsCompleted)
                {
                    ActivateHeadItem(head.Waiter, preferAsync: true);
                }
            }
        }
        else
        {
            // Same lock-guarded claim+activate as DrainReadyWaiters' count==0 branch: the executor may
            // have deferred-published a next item against our Count==1 read. If so, claim and activate
            // under the lock so its ClearExecutingItem fence-acquire sees us done. Consume the publish
            // ONLY at Count == 0: a successor that raised the count since our decrement observed count 1
            // (wasEmpty) and self-activated, and its own drain continues the chain. Consuming here
            // without activating would orphan the published item (slot mode has no recurring D-path to
            // recover it). The Count-then-Exchange TOCTOU is benign: a commit's Exchange precedes its
            // count increment, so whichever Exchange wins owns that item's activation exactly once.
            lock (_activationLock!)
            {
                // <= 0, not == 0: a negative count means the in-flight commit's entry was already
                // completed-and-consumed (skew bound -1), so the deferred publish is the only live
                // responsibility and this claim is correct. The committer's own late increment returns
                // <= 0 (wasEmpty false) and its IsCompleted guard skips, so without this arm both sides
                // skip and the publish strands.
                if (_waiters.Count <= 0 && Interlocked.Exchange(ref _executingItemActivationPending, false))
                {
                    var executing = _executingItem;
                    // _executingItem stays populated: the main loop is concurrently about to call
                    // _policy.ExecuteItemAsync(executing), which needs ExecutingItem for write-phase
                    // gating. The next iteration overwrites or clears it naturally.
                    ActivateHeadItem(executing, preferAsync: true);
                }
            }
        }
    }

    /// The advancer loop: processes completed waiters from the head of the queue in order.
    /// Only one thread runs this at a time, ensuring head-first processing and ordered completion.
    /// Reachable only after a waiter was committed and escalated to the queue (slot-mode drains
    /// route through DrainSlotInline), so _activationLock is non-null and _waiters.Queue
    /// is non-null on every code path here.
    void DrainReadyWaiters()
    {
        var emptyReached = false;
        while (true)
        {
            // Sub-pass start: consume the signal (full fence orders the clear before the pass's queue
            // reads). Completions firing mid-pass re-set it and the do-while's post-release recheck
            // catches them - the clear/fence/recheck gate.
            Interlocked.Exchange(ref _drainSignal, false);
            var drainedAny = false;

            while (_waiters.TryPeek(out var item) && item.WaiterTask.IsCompleted)
            {
                _waiters.TryDequeue(out _);
                drainedAny = true;

                // Process the completed waiter. The consume here already precedes this
                // path's DecrementCount below - the queue drain has always had the
                // consume-before-republish ordering (see DrainSlotInline).
                var waiter = item.Waiter;
                Exception? taskException = null;
                try
                {
                    item.WaiterTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    taskException = ex;
                }

                bool advance;
                if (taskException is null)
                {
                    // Accumulate the idle signal across the do-while: depth can reach 0 mid-loop
                    // (last drained item), but the OR captures it for after the final release.
                    emptyReached |= CompleteWaiterDeferred(waiter, null);
                    advance = true;
                }
                else
                {
                    advance = RecoverWaiter(waiter, taskException, out var recoveryEmpty);
                    emptyReached |= recoveryEmpty;
                }

                if (!advance)
                {
                    // Recovery item is occupying this pipeline position. The advancer flag stays
                    // held. The recovery continuation will complete the item, advance via
                    // AdvanceAndDrainRecovery, and release the flag (which signals any waiting drain).
                    // The recovery continuation also fires the idle signal if applicable, so it is
                    // safe to forget our local emptyReached here (cannot be true: depth hasn't yet
                    // hit zero or we wouldn't be doing recovery).
                    return;
                }

                // Advance: decrement queue count and activate next.
                var count = _waiters.DecrementCount();
                // -1 floor, not 0: consuming a visible-but-uncounted entry (the committer is between
                // its enqueue and its increment, its task completed at commit) sends the count
                // transiently negative, bounded at -1 by the single producer.
                Debug.Assert(count >= -1);

                // <= 0, not == 0: the C-path must also own the skewed -1 case. The consumed entry was
                // the in-flight commit (already completed, so its wasEmpty/IsCompleted guards skip),
                // leaving the deferred publish as the only live responsibility. Routing -1 to the
                // D-path arm instead activates default(T) through the unchecked peek (an NRE).
                if (count <= 0)
                {
                    // Last waiter drained. Hold the lock around claim+activate so the executor's
                    // ClearExecutingItem fence-acquire blocks until ActivateHeadItem finishes.
                    // Wrapping just the ActivateHeadItem call would leave a TOCTOU where the
                    // executor observes the Exchange and acquires its fence-lock uncontested.
                    lock (_activationLock!)
                    {
                        // Count gates the Exchange: only CONSUME the publish when the queue is actually
                        // drained (Count <= 0). A successor committing between our Decrement and here
                        // leaves Count > 0, and the publish is the item we won the Exchange for, whose
                        // own commit sees alreadyActivated and does not self-activate. LEAVE the publish
                        // at Count > 0 so the executor's CommitTailWaiter Exchange reclaims and activates
                        // it; consuming-then-clearing here would strand the activation (neither side
                        // activates). The Exchange stays inside the lock so the executor's fence-acquire
                        // blocks until ActivateHeadItem finishes. The skewed -1 is a C-path state too.
                        if (_waiters.Count <= 0 && Interlocked.Exchange(ref _executingItemActivationPending, false))
                        {
                            var executing = _executingItem;
                            ActivateHeadItem(executing, preferAsync: true);
                        }
                    }
                }
                else
                {
                    // More waiters remain, activate the next one (different item from _executingItem
                    // so no executor-completion race, no activation lock needed). count > 0 implies a
                    // peekable head (the count under-promises the queue, never over-promises), so the
                    // failed peek is a canary, not a case.
                    if (_waiters.TryPeek(out var nextItem))
                        ActivateHeadItem(nextItem.Waiter, preferAsync: true);
                    else
                        Debug.Assert(false, "count > 0 with no peekable queue head.");
                }
            }

            // SIGNAL CONSERVATION, count-gated: a pass that consumed the signal and dequeued nothing
            // did not satisfy it - the signaled work is still materializing (the escalation's slot-to-
            // queue move, or a commit between its enqueue and its increment) and was truthfully
            // invisible to our peeks. Destroying the token here would let every later rescue (the
            // one-shot nudge, the reclaim) read FALSE and decline, stranding completion. Restore before
            // the release: the release's full fence publishes it, and the materializer's own tail check
            // finds the token. The count gate kills the dangling restore - with no entry resident there
            // is no materializer left to owe a token to, and an unconditional restore fed a
            // reclaim-retry livelock.
            if (!drainedAny && _waiters.Count > 0)
            {
                _drainSignal = true;
            }

            // The release Exchange consumes a deposited obligation atomically with releasing: a
            // contended acquirer (callback, nudge, a sibling reclaim) that lost against our hold
            // flagged the latch word instead of spending a one-shot wake against a recheck that
            // already ran - the two-location strand the plain latch + flag protocol could not close.
            // Deposit set means we are obligated: re-acquire and run another pass. Losing the
            // re-acquire re-deposits on the winner, whose own release then serves - obligation
            // transfer, no kernel wait.
            if (_advancing.ReleaseAndCheckPending())
            {
                if (_advancing.TryAcquireOrFlagPending())
                    continue;
                break;
            }

            // No deposit consumed: re-check for late signals. The sandwich argument (release
            // fence before, TryReclaim's acquire fence after) licenses a plain read here;
            // Volatile.Read defeats JIT hoisting.
            var recheck = !_enumerator.CompletionToken.IsCancellationRequested
                && Volatile.Read(ref _drainSignal);
            if (!recheck || !TryReclaimAdvancerForWork())
                break;
        }

        // Signal AFTER advancer release for the same reason as DrainSlotInline (external awaiter
        // must not resume while internal sync is still held).
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalDrainWakeupIfWaiting();

        // Re-acquire the advancer latch and check for a ready waiter. Returns true with the
        // latch held if the pass should continue, false otherwise. TryPeek MUST run inside the
        // latch's protection so the SPSC single-consumer contract on _waiters holds against a
        // racing OnWaiterTaskCompleted caller.
        bool TryReclaimAdvancerForWork()
        {
            // The transient hold had a strand: a callback's one-shot wake bailed against the hold
            // while the miss path below released with no post-release rendezvous - signal set, latch
            // free, completed entry resident, nobody left. The pending word closes it: a bail against
            // this hold deposits in the latch word, and the miss release reads the deposit atomically
            // with releasing and serves.
            if (!_advancing.TryAcquireOrFlagPending())
            {
                // Obligation deposited on the winner; its release serves.
                return false;
            }

            if (_waiters.TryPeek(out var pending) && pending.WaiterTask.IsCompleted)
            {
                return true;
            }

            if (_advancing.ReleaseAndCheckPending())
            {
                // A bail landed against our transient hold after the peek's verdict went
                // stale. Re-acquire and continue the pass; losing re-deposits on the winner.
                if (_advancing.TryAcquireOrFlagPending())
                    return true;
            }
            return false;
        }
    }

    /// Returns true if the advancer should continue (recovery completed or no recovery),
    /// false if recovery is occupying this pipeline position (advancer must stop, recovery will resume knowing it held the advancer flag).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal up to the drain caller
    /// on the sync return-true paths so it fires after the advancer release. The async return-false
    /// path goes through the continuation chain which still uses CompleteRecoveryWaiter inline
    /// (residual race window, but DrainReadyWaiters' do-while reclaim downstream catches any
    /// stranded queue counts; slot-mode stranding from recovery is the case handled here).
    [MethodImpl(MethodImplOptions.NoInlining)]
    // The caller consumed the waiter task (the consume-before-republish ordering, see
    // DrainSlotInline) and hands over the extracted exception.
    bool RecoverWaiter(T failedItem, Exception ex, out bool emptyReached)
    {
        emptyReached = false;

        // A committed executor-side recovery faulting late, identified by the guard wrapper's
        // marker exception (see RecoverCommittedTailWaiterAsync): completed directly with the
        // real fault - recovery is the cleanup of last resort, its own failure means the wire
        // is gone and there is nothing a policy could substitute. This is what entitles
        // policies to assert they are never consulted about their own recovery items.
        if (ex is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            emptyReached = CompleteWaiterDeferred(failedItem, recoveryFault.InnerException);
            return true;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTaskWaiter, ex);
        if (!_policy.TryRecoverItemFailure(context, failedItem, _enumerator.CompletionToken, out var recoveryItem))
        {
            emptyReached = CompleteWaiterDeferred(failedItem, ex);
            return true;
        }

        // Recovery item takes over, activate it at the current pipeline position.
        ActivateHeadItem(recoveryItem);

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            // waiterExecution: this is the waiter-drain recovery (RecoverWaiter off the advancer chain
            // via OnWaiterTaskCompleted). It can run on a non-pump thread, concurrently with the
            // executor's own next dispatch, so a policy must not share single-pump per-dispatch state.
            executeTask = _policy.ExecuteItemAsync(recoveryItem, waiterExecution: true, _enumerator.CompletionToken);
        }
        catch (Exception recoveryEx)
        {
            emptyReached = CompleteWaiterDeferred(recoveryItem, recoveryEx);
            return true;
        }

        // Publish the recovery item so the bailout path can complete it on shutdown.
        // The recovery continuation always completes this item itself (via CompleteRecoveryWaiter
        // on the normal path or BailoutRecoveryOnShutdown on shutdown), so no atomic claim race
        // with drain is needed, drain just waits for the advancer chain to quiesce.
        _waiterRecoveryItem = recoveryItem;

        if (executeTask.IsCompletedSuccessfully)
        {
            return RecoverWaiterResult(recoveryItem, executeTask.Result, out emptyReached);
        }

        // Execute is async, hook continuation. Advancer stops, continuation owns the flag.
        executeTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            // Bailout: wake signal completed while recovery's executeTask was in flight.
            if (_enumerator.CompletionToken.IsCancellationRequested)
            {
                try { executeTask.GetAwaiter().GetResult(); }
                catch { /* shutdown in progress, exception observed and discarded */ }
                BailoutRecoveryOnShutdown();
                return;
            }

            PipelineItemResult result;
            try
            {
                result = executeTask.GetAwaiter().GetResult();
            }
            catch (Exception recoveryEx)
            {
                CompleteRecoveryWaiter(recoveryItem, recoveryEx);
                AdvanceAndDrainRecovery();
                return;
            }

            if (!RecoverWaiterResult(recoveryItem, result, out _))
                return; // pipeline task pending, continuation will resume

            AdvanceAndDrainRecovery();
        });
        return false;
    }

    /// Handles a completed recovery execution result. Returns true if the advancer should continue,
    /// false if the recovery's pipeline task is pending (occupying this position).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal on the sync return-true
    /// paths. Async return-false paths run through the continuation chain (documented gap).
    bool RecoverWaiterResult(T recoveryItem, PipelineItemResult result, out bool emptyReached)
    {
        emptyReached = false;
        // Await trailing execution, must observe the result.
        if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
        {
            if (result.TrailingExecutionTask.IsCompleted)
            {
                try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                catch (Exception ex)
                {
                    emptyReached = CompleteRecoveryWaiterDeferred(recoveryItem, ex);
                    return true;
                }
            }
            else
            {
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    Exception? trailingEx = null;
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch (Exception ex) { trailingEx = ex; }

                    // Bailout: wake signal completed while recovery's trailing task was in flight.
                    if (_enumerator.CompletionToken.IsCancellationRequested)
                    {
                        BailoutRecoveryOnShutdown();
                        return;
                    }

                    if (trailingEx is not null)
                    {
                        CompleteRecoveryWaiter(recoveryItem, trailingEx);
                        AdvanceAndDrainRecovery();
                        return;
                    }

                    if (!RecoverWaiterPipelineTask(recoveryItem, pipelineTask, out _))
                        return; // pipeline task pending, lock still held

                    AdvanceAndDrainRecovery();
                });
                return false;
            }
        }

        return RecoverWaiterPipelineTask(recoveryItem, result.PipelineTask, out emptyReached);
    }

    /// Handles the recovery item's pipeline task. Returns true if done, false if pending.
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal to the caller on the
    /// sync return-true path. The async return-false path goes through CompleteRecoveryWaiter
    /// inline inside the continuation (documented gap, has the same race window but the
    /// downstream AdvanceAndDrainRecovery -> DrainReadyWaiters' reclaim catches stranded queue
    /// counts).
    bool RecoverWaiterPipelineTask(T recoveryItem, ValueTask pipelineTask, out bool emptyReached)
    {
        emptyReached = false;
        if (pipelineTask.IsCompleted)
        {
            Exception? pipelineException = null;
            try
            {
                pipelineTask.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                pipelineException = ex;
            }
            emptyReached = CompleteRecoveryWaiterDeferred(recoveryItem, pipelineException);
            return true;
        }

        // Pipeline task pending, hook continuation. Advancer stays held.
        pipelineTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            Exception? pipelineException = null;
            try
            {
                pipelineTask.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                pipelineException = ex;
            }
            if (_enumerator.CompletionToken.IsCancellationRequested)
            {
                // Bailout: wake signal completed while recovery's pipeline task was in flight.
                BailoutRecoveryOnShutdown();
                return;
            }

            CompleteRecoveryWaiter(recoveryItem, pipelineException);
            AdvanceAndDrainRecovery();
        });
        return false;
    }

    /// Shared shutdown bailout for recovery continuations. Completes the recovery item with the
    /// shutdown exception, releases _advancing, and signals any drain waiting for the advancer
    /// chain to quiesce. The continuation always owns completion (no Exchange race with drain)
    /// because drain only waits for advancer-idle and does not compete for the recovery item.
    void BailoutRecoveryOnShutdown()
    {
        var recoveryItem = _waiterRecoveryItem;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        CompleteWaiter(recoveryItem, _completionException);
        // The recovery park holds its position's count credit (the fault decision precedes the
        // decrement in both drains). Every park resolution settles it exactly once: the sync advance
        // paths decrement in their drain loop, the normal continuation decrements via AdvanceAndDrain,
        // and this bailout settles it here - without it the credit strands and DrainOnCompletionAsync's
        // count wait never falls to zero.
        var count = _waiters.DecrementCount();
        Debug.Assert(count >= -1);
        // Shutdown bailout: a deposit consumed here is deliberately dropped - completion
        // ownership has transferred to DrainOnCompletionAsync's queue sweep, which waits on
        // advancer-idle (signaled below) and sweeps without consulting the signal flag.
        _advancing.ReleaseAndCheckPending();
        SignalDrainWakeupIfWaiting();
    }

    /// Called from recovery continuations to continue advancer activity after the recovery item
    /// completion. Delegates to AdvanceAndDrain whose loop exit signals the advancer-idle TCS.
    void AdvanceAndDrainRecovery() => AdvanceAndDrain();

    /// Signals any drain that's waiting for the advancer chain to quiesce. Null check is the
    /// common case (no one waiting), only allocates a Volatile.Read + null compare.
    /// Ordering: no hardware fence is required, cache coherence delivers the write eventually
    /// on any target. The only real requirement is to stop the JIT from hoisting or caching the
    /// load across calls (a relaxed atomic load would express this exactly). .NET lacks a
    /// relaxed-read primitive, so we use Volatile.Read and pay an unnecessary acquire fence
    /// on ARM as the cost of saying "actually emit the load."
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SignalDrainWakeupIfWaiting()
        => Volatile.Read(ref _drainWakeupTcs)?.TrySetResult();

    /// Completes the recovery item on the normal (non-shutdown) recovery path. The continuation
    /// owns completion uncontested. Drain (DrainOnCompletionAsync) only waits for the advancer
    /// chain to quiesce via AdvanceAndDrainRecovery's loop-exit signal.
    void CompleteRecoveryWaiter(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        CompleteWaiter(recoveryItem, exception);
    }

    /// <see cref="CompleteRecoveryWaiter"/> variant that returns the deferred-empty signal so the
    /// caller can fire <see cref="DepthState.OnDepthReachedZero"/> after releasing the advancer.
    /// Used by the sync recovery chain (<see cref="RecoverWaiter"/>, <see cref="RecoverWaiterResult"/>,
    /// <see cref="RecoverWaiterPipelineTask"/>) to avoid the same stranding window the slot drain fix
    /// closed: signalling OnDepthReachedZero while the drain caller still holds the advancer would
    /// let a WaitForEmptyAsync awaiter resume and commit a follow-up slot waiter whose callback
    /// would bail on TryAcquire.
    bool CompleteRecoveryWaiterDeferred(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        return CompleteWaiterDeferred(recoveryItem, exception);
    }

    /// Decrements waiter count, activates the next item, and resumes draining.
    /// Must only be called when the advancer flag is held.
    void AdvanceAndDrain()
    {
        var count = _waiters.DecrementCount();
        Debug.Assert(count >= -1); // -1 floor, see DrainReadyWaiters.

        // Slot-mode rejoin (a slot drain parked for recovery; the count still carried the recovered
        // position - see DrainSlotInline's recovery return): the successor, if any, lives in the SLOT,
        // not the queue. The queue partition below would peek nothing and lose its activation (its
        // count > 0 arm asserts on the empty queue). Run the slot partition instead and re-enter the
        // slot claim loop, which serves deposits and releases the advancer.
        if (!_waiters.IsEscalated)
        {
            ActivateNextAfterSlotAdvance(count);
            DrainSlotInline();
            return;
        }

        // <= 0 partition + checked peek, mirroring DrainReadyWaiters (this advance is the
        // recovery path's drain step and shares the count-skew exposure).
        if (count <= 0)
        {
            // Same lock-guarded claim+activate as DrainReadyWaiters: lock wraps the Exchange so
            // the executor's fence-acquire blocks until ActivateHeadItem finishes.
            // Advancer chain reachability implies _activationLock is non-null.
            lock (_activationLock!)
            {
                // Count gates the Exchange - LEAVE the publish at Count > 0 for the executor's commit
                // to reclaim and activate, never consume-and-clear it (see DrainReadyWaiters' C-path).
                if (_waiters.Count <= 0 && Interlocked.Exchange(ref _executingItemActivationPending, false))
                {
                    var executing = _executingItem;
                    ActivateHeadItem(executing);
                }
            }
        }
        else if (_waiters.TryPeek(out var nextItem))
        {
            ActivateHeadItem(nextItem.Waiter);
        }
        else
        {
            Debug.Assert(false, "count > 0 with no peekable queue head.");
        }

        // Continue draining ready items, we already hold the advancer flag.
        DrainReadyWaiters();
    }

    // Whether a store to a slot field of type T is atomic against a concurrent getter read, deciding
    // whether the seqlock below is needed. Mirrors ConcurrentDictionary's IsValueWriteAtomic
    // (ECMA-335 I.12.6.6): references and IntPtr are atomic, primitives up to 4 bytes always, 8-byte
    // primitives only on 64-bit, and EVERY custom struct is tearable. That last point is load-bearing:
    // the JIT may field-decompose a struct copy regardless of size, so a `sizeof <= nativeword` test is
    // NOT a safe substitute. Cached per instantiation - true for reference T (the seqlock vanishes),
    // false for the struct pipelines that need it.
    static readonly bool _writeAtomic = IsWriteAtomic();

    static bool IsWriteAtomic()
    {
        if (!typeof(T).IsValueType || typeof(T) == typeof(IntPtr) || typeof(T) == typeof(UIntPtr))
            return true;

        switch (Type.GetTypeCode(typeof(T)))
        {
            case TypeCode.Boolean:
            case TypeCode.Byte:
            case TypeCode.Char:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.SByte:
            case TypeCode.Single:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                return true;
            case TypeCode.Int64:
            case TypeCode.Double:
            case TypeCode.UInt64:
                return IntPtr.Size == 8;
            default:
                return false;
        }
    }

    // Publish a value to one of the public single-pump slots (_executingItem / _activatedItem).
    // When the store is atomic (_writeAtomic), a cross-thread getter read can never tear, so a plain
    // store suffices and the generation work is skipped. The !typeof(T).IsValueType guard JIT-folds, so
    // reference T short-circuits with zero cost.
    //
    // Otherwise T is a multi-word struct and a concurrent getter could observe a field-mismatched
    // ("torn") value. The seqlock generation closes that: bump it odd before the store and even after,
    // so ReadSlot detects any overlap and retries. A torn copy is harmless - reference fields are
    // atomically copied, so a torn struct still has every reference valid (just mismatched), never a
    // garbage pointer, and ReadSlot discards it on the generation recheck. Interlocked.Increment is a
    // full fence, so the store stays sandwiched in the odd/even bracket.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void PublishSlot(ref T slot, ref uint gen, T value)
    {
        if (!typeof(T).IsValueType || _writeAtomic)
        {
            slot = value;
        }
        else
        {
            Interlocked.Increment(ref gen); // odd: store in progress
            slot = value;
            Interlocked.Increment(ref gen); // even: store complete
        }
    }

    // Tear-free read of a public single-pump slot. Reference T: a plain atomic load. Value T: the
    // seqlock read - sample an even generation, copy the struct, re-sample; if the generation moved
    // the copy straddled a write (possibly torn, always GC-safe) so retry. Writes are item-paced
    // (wire-I/O-gated) so the loop effectively never spins, but correctness does not depend on that:
    // a snapshot is only returned when no write overlapped it. Stale-but-consistent is fine (the
    // slot's contract permits a stale reference); only tearing is excluded.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static T ReadSlot(ref T slot, ref uint gen)
    {
        if (!typeof(T).IsValueType || _writeAtomic)
            return slot;

        var spin = new SpinWait();
        while (true)
        {
            var g1 = Volatile.Read(ref gen);
            if ((g1 & 1) == 0) // even: no write in progress at the sample point
            {
                var value = slot; // acquire on g1 orders this read after the generation sample
                Interlocked.MemoryBarrier(); // the copy must complete before the re-sample (LoadLoad)
                if (Volatile.Read(ref gen) == g1)
                    return value;
            }
            spin.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SetExecutingItem(T value) => PublishSlot(ref _executingItem, ref _executingItemGen, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SetActivatedItem(T value) => PublishSlot(ref _activatedItem, ref _activatedItemGen, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivateHeadItem(T item, bool preferAsync = true)
    {
        // Update the publicly-observed ActivatedItem slot before dispatching to the policy:
        // a same-thread inline activation sees the new value via the policy call's own
        // sequencing, and a TP-dispatched activation publishes via the dispatch fence.
        SetActivatedItem(item);
        _policy.ActivateHeadItem(item, preferAsync);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ClearExecutingItem(bool wasActivated)
    {
        // Single writer (the executor), so no RMW needed; the Volatile.Write carries the clear to
        // the enumerator's cross-thread read. Cleared in both paths so the enumerator stops yielding
        // the item once it's done (or transitioning to another channel like a tail-waiter).
        Volatile.Write(ref _hasInFlightItem, false);

        if (!wasActivated)
        {
            if (Interlocked.Exchange(ref _executingItemActivationPending, false))
            {
                // Won the race-back: advancer won't activate. Item is done (callers invoke us after
                // pipelineTask.IsCompleted), so activation is optional, skip it.
                SetExecutingItem(default!);
            }
            else
            {
                // Advancer claimed first and is in ActivateHeadItem under the lock. Empty
                // acquire/release synchronizes-with that lock release, closing the
                // activation-after-completion race before the caller proceeds to CompleteWaiter.
                // wasActivated=false means the deferred-publish branch ran, which only triggers
                // when _waiterQueueCount > 0, so _activationLock is non-null here.
                lock (_activationLock!) { }
            }
        }
        else
        {
            // Inline-activated path: no advancer is tracking, so we can drop the item reference
            // along with the visibility flag.
            SetExecutingItem(default!);
        }
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    static void ThrowCompleted() => throw new InvalidOperationException("The pipeline has been completed.");


    public struct Enumerator
    {
        readonly Pipeline<T, TPolicy, TSource, TEnumerator> _pipeline;
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>.Enumerator _waitersEnumerator;
        // 0: executing item, 1: init queue waiters, 2: enumerate queue waiters, 3: slot waiter, 4: tail waiter, 5: done
        int _phase;

        internal Enumerator(Pipeline<T, TPolicy, TSource, TEnumerator> pipeline)
        {
            _pipeline = pipeline;
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            switch (_phase)
            {
                case 0:
                    _phase = 1;
                    // Visibility-only window: the in-flight item is held on _executingItem before
                    // being committed elsewhere. Without yielding it here, heartbeat-style
                    // consumers can't see the item during dispatch (waiting-body abort propagation
                    // needs this). Volatile.Read pairs with the executor's Volatile.Write on
                    // _hasInFlightItem.
                    if (Volatile.Read(ref _pipeline._hasInFlightItem) && _pipeline._executingItem is { } inFlight)
                    {
                        Current = inFlight;
                        return true;
                    }
                    goto case 1;
                case 1:
                    // Null queue means the pipeline never escalated. Skip to the slot phase.
                    var queue = _pipeline._waiters.Queue;
                    if (queue is null)
                    {
                        _phase = 3;
                        goto case 3;
                    }
                    _waitersEnumerator = new(queue);
                    _phase = 2;
                    goto case 2;
                case 2:
                    while (_waitersEnumerator.MoveNext())
                    {
                        if (_waitersEnumerator.Current.Waiter is { } waiter)
                        {
                            Current = waiter;
                            return true;
                        }
                    }
                    _phase = 3;
                    goto case 3;
                case 3:
                    _phase = 4;
                    if (_pipeline._waiters.TrySnapshotSlot(out var slotItem) && slotItem is { } slot)
                    {
                        Current = slot;
                        return true;
                    }
                    goto case 4;
                case 4:
                    _phase = 5;
                    // Volatile.Read pairs with the executor's Volatile.Write on _hasTailWaiter:
                    // if observed true, the prior _tailWaiter / _tailWaiterTask writes are visible.
                    // Consistent for any T that fits in a native word (refs, primitives, small
                    // structs). Larger structs can tear their own write regardless of fences.
                    if (Volatile.Read(ref _pipeline._hasTailWaiter) && _pipeline._tailWaiter is { } tail)
                    {
                        Current = tail;
                        return true;
                    }
                    return false;
                default:
                    return false;
            }
        }
    }

}

public static class Pipeline
{
    /// Marker carrying a committed recovery item's own fault through the waiter store's task plumbing.
    /// Recovery identity travels in the task rather than pipeline state: the guard wrapper (see
    /// RecoverCommittedTailWaiterAsync) rethrows the recovery's late fault as this type, and
    /// RecoverWaiter recognizes it and completes the item directly with the inner exception. A
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
    /// GetAsyncEnumerator implementation). Threading another CT through here would be redundant.
    /// <para>
    /// Flyweight reuse: pass a previously-completed <paramref name="instance"/> to rebind it
    /// against the new <paramref name="policy"/> + <paramref name="source"/> and restart it.
    /// The shell allocation (waiter queue, delegates, depth/latch machinery) gets reused while
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

    /// <summary>Construct a queue-backed pipeline. Returns a <see cref="QueuedPipeline{T,TPolicy}"/> that
    /// exposes <see cref="QueuedPipeline{T,TPolicy}.Enqueue"/> directly.</summary>
    /// <remarks>
    /// The cancellation token is passed to the internally-created source. Pipeline itself stays
    /// CT-free, and the source owns the cancellation lifecycle.
    /// </remarks>
    public static QueuedPipeline<T, TPolicy> Create<T, TPolicy>(TPolicy policy, bool runContinuationsAsynchronously = true, PipelineScheduler? scheduler = null, CancellationToken cancellationToken = default)
        where TPolicy : IPipelinePolicy<T>
    {
        var source = UnboundedQueueSource<T>.Create(runContinuationsAsynchronously, scheduler, cancellationToken);
        var pipeline = new Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator>(policy, source);
        return new QueuedPipeline<T, TPolicy>(pipeline, source);
    }

    /// Split-counter depth + drain-waiter (WaitForEmptyAsync) protocol. Depth is the difference of two
    /// monotonic totals rather than one shared word:
    ///   _enqueued - producer-owned. The SPSC contract serializes enqueues, so the increment is a plain
    ///     read + Volatile.Write (release), no RMW, on its own padded cache line (no producer/completer
    ///     line ping-pong).
    ///   _completed - completer-side Interlocked.Increment. The RMW stays because executor-inline
    ///     completions and advancer drains overlap, but the producer never touches this line.
    /// Counters are uint, wrap-safe while live depth stays below 2^31 (guarded in IncrementDepth via a
    /// producer-cached completed snapshot, so the guard adds no cross-line read per enqueue).
    ///
    /// Zero-crossing (drain): completers bump _completed (full-fence RMW), compute depth against their
    /// own bump, and on zero check the _drainTcs slot (the load cannot hoist above the RMW) and
    /// Exchange-take + fire. The armer publishes the TCS with a CAS - the publish IS the arm, and the
    /// full-fence RMW orders the subsequent depth re-check (a release publish would let the re-check's
    /// loads reorder above it on x64, a lost wake).
    ///
    /// Firing on a momentary zero while a producer races a new item in is the documented
    /// WaitForEmptyAsync semantics (idle convergence, not exactly-once). TrySetResult and the
    /// Exchange-clear keep completer/armer races idempotent, and at most one armed cycle is outstanding
    /// (single-caller API), so a clear+fire always targets the cycle that armed it. The momentary-zero
    /// license covers zeros at or after the arm: the armer's re-check fire qualifies by construction, a
    /// completer's fire is deferred and revalidates depth first (see OnDepthReachedZero).
    internal struct DepthState
    {
        // Ref-free explicit-layout blob so the producer counter's isolation survives the
        // runtime's auto layout of this ref-containing struct: wherever the blob lands,
        // Value sits >= 128 bytes (one Apple Silicon line) from the fields on either side.
        [StructLayout(LayoutKind.Explicit, Size = 256)]
        struct PaddedProducerCounter
        {
            [FieldOffset(128)] public uint Value;
            // Producer-owned stale snapshot of _completed for the overflow guard. Same line as
            // Value on purpose: only the producer reads/writes it.
            [FieldOffset(132)] public uint CompletedCache;
        }

        PaddedProducerCounter _enqueued;
        uint _completed;
        // The slot doubles as the arm signal: non-null means a drain waiter is armed. The
        // publish CAS is the full-fence RMW the Dekker pair needs, so no separate flag word.
        TaskCompletionSource? _drainTcs;

        /// <summary>Current depth. Lock-free.</summary>
        public int Depth
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Read completed BEFORE dispatched: an item is dispatch-counted (here, _enqueued) by
                // the executor strictly BEFORE it can complete (you cannot complete what was never
                // dispatched), so this order can never observe comp > disp (negative depth). The bump
                // and the eventual decrement order on the same logical item.
                var comp = Volatile.Read(ref _completed);
                var enq = Volatile.Read(ref _enqueued.Value);
                return (int)(enq - comp);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementDepth()
        {
            // Executor-owned (called at dispatch, the single-consumer pull) so the counter stays
            // single-writer; plain read + release store. Depth now means in-flight (dispatched -
            // completed), not enqueued - completed.
            var next = _enqueued.Value + 1;
            // Wrap guard against the producer-local stale snapshot: apparent live depth past
            // int.MaxValue forces a snapshot refresh (the only time the producer touches the
            // completer's line) and throws if the depth is genuinely at the limit.
            if (next - _enqueued.CompletedCache > (uint)int.MaxValue)
                RefreshCacheOrThrow(next);
            Volatile.Write(ref _enqueued.Value, next);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void RefreshCacheOrThrow(uint next)
        {
            var comp = Volatile.Read(ref _completed);
            if (next - comp > (uint)int.MaxValue)
                throw new InvalidOperationException("Pipeline depth overflow.");
            _enqueued.CompletedCache = comp;
        }

        /// <summary>Records a completion. Returns the new depth. Caller MUST invoke
        /// <see cref="OnDepthReachedZero"/> when the result is 0 to signal any pending drain waiter.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int DecrementDepth()
        {
            // Full-fence RMW; the enqueued read (and any subsequent _drainTcs read in
            // OnDepthReachedZero) is ordered after it. Depth is computed against OUR bump:
            // when completers race the final item, exactly the last bumper observes zero.
            var comp = Interlocked.Increment(ref _completed);
            var enq = Volatile.Read(ref _enqueued.Value);
            return (int)(enq - comp);
        }

        /// <summary>
        /// Returns a task that completes when depth reaches 0 (momentarily; see remarks on the
        /// containing type). Publish-arm-recheck; single-caller API.
        /// </summary>
        public ValueTask GetIdleTask(CancellationToken cancellationToken)
        {
            if (Depth is 0)
                return ValueTask.CompletedTask;

            var newTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // The publish IS the arm: the CAS is the full-fence RMW, so the re-check below is ordered
            // after it. A completer that hit zero before observing the publish skipped its fire, and
            // the re-check catches that case and self-signals.
            var tcs = Interlocked.CompareExchange(ref _drainTcs, newTcs, null) ?? newTcs;
            if (Depth is 0)
                SignalDrainWaiter();

            return new(tcs.Task.WaitAsync(cancellationToken));
        }

        /// <summary>
        /// Called by a completer after <see cref="DecrementDepth"/> returns 0. Disarms and
        /// signals the pending drain waiter, if any - revalidating depth first, because the
        /// zero verdict is bump-instant and this call may have been DEFERRED (the drain paths
        /// signal after releasing the advancer): a producer and a NEW WaitForEmptyAsync arm
        /// can land in the deferral window, and a blind fire would wake that caller on a zero
        /// predating its arm (its entry check saw depth > 0).
        /// </summary>
        public void OnDepthReachedZero()
        {
            // The caller's bump was a full fence, so this load is ordered after it; a publish
            // that preceded the bump is visible here. A publish that FOLLOWS the bump may be
            // missed - that side's re-check covers it (see GetIdleTask).
            if (Volatile.Read(ref _drainTcs) is null)
                return;

            while (true)
            {
                var tcs = Interlocked.Exchange(ref _drainTcs, null);
                if (tcs is null)
                    return;

                // Fire only against a still-zero depth: the armer's momentary-zero contract
                // covers zeros that occur AT or AFTER its arm; a stale pre-arm zero does not.
                if (Depth is 0)
                {
                    tcs.TrySetResult();
                    return;
                }

                // Stale zero against a newer arm: hand the tcs back. The CAS can only fail
                // against a caller that cancelled its wait (the WaitAsync wrapper frees the
                // single-caller API early) and re-armed - the held tcs has no waiter and is
                // dropped; the new arm did its own publish-recheck.
                if (Interlocked.CompareExchange(ref _drainTcs, tcs, null) is not null)
                    return;

                // The put-back is itself an arm: a completer that hit zero while we held the
                // tcs read the slot as empty and skipped its fire. Re-check depth after the
                // put-back (same publish-then-recheck discipline as GetIdleTask) and loop
                // back to re-take if a fresh zero materialized.
                if (Depth is not 0)
                    return;
            }
        }

        void SignalDrainWaiter()
        {
            // Exchange-take makes the disarm exactly-once per armed cycle; the winner fires.
            // TrySetResult keeps the armer-vs-completer self-signal race idempotent.
            var tcs = Interlocked.Exchange(ref _drainTcs, null);
            tcs?.TrySetResult();
        }
    }
}
