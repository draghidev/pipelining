using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// <summary>
/// Source-driven pipelined request/response coordinator. Consumes items from an
/// <see cref="IPipelineSource{T,TEnumerator}"/> via <c>await foreach</c> and processes each
/// through the policy's lifecycle (execute/activate/complete/recover). Cancellation and
/// completion are owned by the source's enumerator (see <see cref="IPipelineEnumerator{T}"/>).
/// <see cref="CompleteAsync"/> drives shutdown by signalling that enumerator.
/// </summary>
/// <remarks>
/// <para>
/// The class is not thread-safe. <see cref="CompleteAsync"/>, <see cref="GetEnumerator"/>, and
/// the <see cref="Depth"/> getter must be invoked from a single caller at a time. Concurrent
/// calls produce undefined results (stale signals, missed completions). Callers needing
/// multi-threaded access must serialize externally.
/// </para>
/// <para>
/// Internally the class IS concurrent: the execution loop runs on its own scheduler thread, the
/// advancer can fire on threadpool continuations driven by waiter-task completions, and the
/// recovery paths span async boundaries. The "not thread-safe" contract applies specifically to
/// the public API surface, where the cost of broadly thread-safe semantics would not be justified
/// for the target use case (connection-bound network protocol clients with a single producer).
/// Adding piecemeal thread safety to individual methods is worse than committing to either pure
/// stance, so we don't.
/// </para>
/// </remarks>
public sealed class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    TPolicy _policy;
    TSource _source;
    TEnumerator _enumerator;

    // Execution. Field-initialized to a completed sentinel so the first call to Initialize from
    // the constructor sees a "completed previous run" and proceeds. On run completion, ExecuteSource
    // swaps the field back to Task.CompletedTask, releasing the prior ExecuteSource task box. The
    // cached-but-idle Pipeline shell's footprint then shrinks to just its own fields.
    Task _executionTask = Task.CompletedTask;
    // Pipeline state.
    bool _completing; // First-writer guard for CompleteAsync.
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
    bool _hasExecutingItem;

    // Cross-thread atomics, touched by the executor, advancer, enqueuer, and completion callbacks.
    // The store has a single inline slot (zero alloc) for the common one-pending-waiter case. The
    // SPSC queue inside it is lazy-allocated only on true overlap (a second waiter arrives while
    // the first is still pending). See WaiterStore<T>.
    WaiterStore<T> _waiters;
    int _waiterCompletedCount; // Number of completed waiter tasks not yet processed by the advancer.
    Latch _advancing; // Held while a thread is currently the advancer. See Latch.cs for semantics.
    T _waiterRecoveryItem = default!; // The item being recovered, for the bailout/completion paths to access.
    TaskCompletionSource? _advancerIdleTcs; // Set by DrainOnCompletionAsync while waiting for the advancer chain to fully quiesce, cleared after the wait completes.

    // Activation.
    readonly Action _onWaiterTaskCompletedAction;
    readonly Action _onCommittedTaskCompletedAction;
    readonly Action _incrementDepthAction;

    internal Pipeline(TPolicy policy, TSource source)
    {
        // Delegate references bind to `this` and don't change.
        _onWaiterTaskCompletedAction = OnWaiterTaskCompleted;
        _onCommittedTaskCompletedAction = OnCommittedTaskCompleted;
        _incrementDepthAction = IncrementDepth;
        Initialize(policy, source);
    }

    /// Per-run init: sets policy + source, resets transient executor state, creates a fresh enumerator
    /// and execution task. Called from the constructor and by callers for flyweight reuse. Throws if the
    /// previous run hasn't fully completed (first call uses a completed sentinel).
    [System.Diagnostics.CodeAnalysis.MemberNotNull(nameof(_policy), nameof(_source))]
    internal void Initialize(TPolicy policy, TSource source)
    {
        if (!_executionTask.IsCompleted)
            throw new InvalidOperationException("Cannot (re)initialize a pipeline whose previous run hasn't fully completed. Await CompleteAsync's returned task first.");

        _policy = policy;
        _source = source;
        _completing = false;

        // GetAsyncEnumerator takes the depth-increment callback inline. Source binds it before
        // any item can be admitted, so the very first admission lands in _depthState. Pipeline
        // doesn't pass a CT here: source owns its own cancellation lifecycle (caller-configured
        // at source construction).
        _enumerator = _source.GetAsyncEnumerator(_incrementDepthAction);
        _executionTask = ExecuteSource();

        // Other per-run fields are left at default: field-initialized on first run, or zeroed by the
        // previous run's ExecuteSource-exit cleanup.
    }

    void IncrementDepth() => _depthState.IncrementDepth();

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
    /// not propagated through the returned task. It only flows to items. Exceptions thrown by
    /// <see cref="IPipelinePolicy{T}.OnExecutionIdleAsync"/> during shutdown DO fault the returned task.
    /// </param>
    /// <remarks>
    /// Awaiting the returned task gives "fully quiet" semantics: all items are completed, the executor
    /// has exited, and the advancer chain (including in-flight recovery continuations) has unwound -
    /// drain coordinates with the advancer via an idle TCS so any remaining continuation work
    /// is observed before drain returns.
    /// </remarks>
    public ValueTask CompleteAsync(Exception? exception = null)
    {
        if (Interlocked.Exchange(ref _completing, true))
            return new(_executionTask);

        _completionException = exception;
        // Complete() signals the enumeration to wind down: cancels CompletionToken (firing policy
        // CT for in-flight ExecuteItemAsync calls) and signals the source's wake/completion
        // mechanism via the registered callback. The enumerator stays usable for drain reads.
        // DisposeAsync runs at the end of the executor's main loop as the terminal cleanup.
        _enumerator.Complete();

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
        try
        {
            // The CTS-cancelled check belongs to the source's MoveNextAsync, not here: even after
            // CompleteAsync fires, items admitted before Complete() must still be processed. The
            // source returns false once its queue is empty and its wake signal is completed.
            while (true)
            {
                // Commit the previous iteration's pending tail to the waiter queue BEFORE parking
                // on MoveNextAsync. If the source has nothing more to yield, the tail would
                // otherwise sit in _tailWaiter forever with no UnsafeOnCompleted callback wired,
                // and a producer that completes its pipeline task would have nobody listening.
                // CommitTailWaiter is sync in all paths except trailing recovery, where it
                // returns a Task to await.
                var commitWork = CommitTailWaiter();
                if (commitWork is not null)
                    await commitWork.ConfigureAwait(false);

                if (!await _enumerator.MoveNextAsync().ConfigureAwait(false))
                    break;

                item = _enumerator.Current;

                var activated = false;
                if (_waiters.Count is 0)
                {
                    ActivateHeadItem(item, preferAsync: false);
                    activated = true;
                }
                else
                {
                    _executingItem = item;
                    // Release-only publish: subsequent readers seeing _hasExecutingItem=true also see
                    // the _executingItem write. The full fence isn't needed here, we use the ordering
                    // not the Exchange's return value. CommitTailWaiter and the advancer C-path still
                    // use Exchange because they DO need the return value (test-and-set claim).
                    Volatile.Write(ref _hasExecutingItem, true);
                }

                try
                {
                    itemResult = await _policy.ExecuteItemAsync(item, _enumerator.CompletionToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ClearExecutingItem(activated);
                    await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), activated, _enumerator.CompletionToken).ConfigureAwait(false);
                    continue;
                }

                // Sync shortcut: only taken when both tasks are already observed successful at dispatch
                // time. Items with a non-default trailing task fall through to the tail-waiter path until
                // their trailing is also sync-complete (default(ValueTask) is success, so items without a
                // trailing keep the fast path). This is how the framework guarantees CompleteWaiter doesn't
                // fire before trailing is observed.
                if (itemResult.PipelineTask.IsCompletedSuccessfully && itemResult.TrailingExecutionTask.IsCompletedSuccessfully)
                {
                    itemResult.PipelineTask.GetAwaiter().GetResult();
                    ClearExecutingItem(activated);
                    CompleteWaiter(item, null);
                }
                else if (itemResult.PipelineTask.IsCompleted && !itemResult.PipelineTask.IsCompletedSuccessfully)
                {
                    // Pipeline task faulted synchronously. Recovery path. Trailing fate is
                    // handled separately below.
                    ClearExecutingItem(activated);
                    try
                    {
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex), activated, _enumerator.CompletionToken).ConfigureAwait(false);
                        continue;
                    }
                }
                else
                {
                    // Tail waiter path. Either pipeline task is pending, or it's sync-complete
                    // but trailing is pending/faulted. Either way, completion must be gated on
                    // both. The framework's trailing-await below stalls the executor before
                    // next iteration's CommitTailWaiter, which is what fires CompleteWaiter for
                    // this item. So trailing is structurally observed before completion.
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
        catch (OperationCanceledException) when (_enumerator.CompletionToken.IsCancellationRequested)
        {
            // Source's MoveNextAsync threw OCE on shutdown, expected.
        }

        await DrainOnCompletionAsync();
        await _enumerator.DisposeAsync().ConfigureAwait(false);

        // ExecuteSource has fully completed: all items drained, enumerator disposed, advancer
        // chain quiesced. Default the per-run reference-holding fields so a cached-but-idle
        // Pipeline shell doesn't hold them across an idle period. Trimmed to the safe minimum:
        // only reference-typed and reference-containing fields that could be promoted to gen1/2.
        // _completing is set to true so any racing CompleteAsync first-call returns the (about-
        // to-be-swapped-to-CompletedTask) execution task without touching the defaulted fields.
        _completing = true;
        _completionException = null;
        _policy = default!;
        _source = default!;
        _enumerator = default;
        _tailWaiter = default!;
        _tailWaiterTask = default;
        _executingItem = default!;
        _waiters.Reset();
        _waiterRecoveryItem = default!;
        _advancerIdleTcs = null;
        _executionTask = Task.CompletedTask;
    }

    /// Drains remaining items after the execution loop exits. Waits for the advancer chain to
    /// quiesce via a TCS that DrainReadyWaiters / BailoutRecoveryOnShutdown signal on release of
    /// _advancing. Recovery continuations always complete their own items (via CompleteRecoveryWaiter
    /// or BailoutRecoveryOnShutdown), so drain doesn't compete, it just waits for the chain to
    /// finish. After advancer-idle, drains _waiters.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        while (_advancing.IsHeld)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _advancerIdleTcs, tcs);
            // Re-check post-publish: advancer may have released before seeing our TCS.
            if (!_advancing.IsHeld)
            {
                Volatile.Write(ref _advancerIdleTcs, null);
                break;
            }
            await tcs.Task.ConfigureAwait(false);
            Volatile.Write(ref _advancerIdleTcs, null);
            // Loop: spurious wake if advancer released then re-claimed before our check.
        }

        var exception = _completionException;
        // Drain slot first (FIFO) then queue. Both are no-ops if the corresponding tier is empty.
        if (_waiters.TryClaimSlotForDrain(out var slotItem, out _))
        {
            _waiters.DecrementCount();
            CompleteWaiter(slotItem, exception);
        }
        while (_waiters.TryDequeue(out var item))
        {
            _waiters.DecrementCount();
            CompleteWaiter(item.Waiter, exception);
        }
    }

    /// Handles execution-phase or pipeline-task failures, including recovery.
    /// Recovery items get the full async treatment since they're taking the place of the original item.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverItem(T item, PipelineItemFailureContext context, bool activated, CancellationToken cancellationToken)
    {
        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            CompleteWaiter(item, context.Exception);
            return;
        }

        // Recovery item takes over, activate and execute with full async support.
        ActivateHeadItem(recoveryItem, preferAsync: false);
        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
                await result.TrailingExecutionTask.ConfigureAwait(false);

            if (result.PipelineTask.IsCompleted)
            {
                ClearExecutingItem(activated);
                result.PipelineTask.GetAwaiter().GetResult();
                CompleteWaiter(recoveryItem, null);
            }
            else
            {
                _tailWaiter = recoveryItem;
                _tailWaiterTask = result.PipelineTask;
                Volatile.Write(ref _hasTailWaiter, true);
            }
        }
        catch (Exception recoveryEx)
        {
            // No in-flight tail-waiter to observe here: the only _tailWaiter publish in the try
            // is in the trailing pending branch, which doesn't throw.
            ClearExecutingItem(activated);
            CompleteWaiter(recoveryItem, recoveryEx);
        }
    }

    /// Handles trailing execution task failures, including tail waiter recovery.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverTrailingFailure(T item, bool activated, Exception ex, CancellationToken cancellationToken)
    {
        var pipelineValueTask = _tailWaiterTask;
        // Convert to Task so both the policy and the pipeline can safely observe it (ValueTask may be single consume).
        var pipelineTask = pipelineValueTask.AsTask();
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.TrailingExecutionTask, ex, pipelineTask);
        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            // Pipeline task may still be pending, enqueue as a waiter rather than completing prematurely.
            // Items can handle their own interdependency between the two tasks if needed.
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            CommitWaiter(item, activated, new(pipelineTask));
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
                _executingItem = recoveryItem;
                recoveryActivated = !Interlocked.Exchange(ref _hasExecutingItem, true);
                if (recoveryActivated)
                {
                    ActivateHeadItem(recoveryItem, preferAsync: false);
                    // Inside _activationLock. Plain write suffices: all readers of _hasExecutingItem
                    // are Interlocked.Exchange (acquire-semantics, sees latest committed value), and
                    // the lock exit's release fence orders this write w.r.t. anyone acquiring next.
                    _hasExecutingItem = false;
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        _executingItem = default!;
                }
            }
        }
        else
        {
            _executingItem = recoveryItem;
            recoveryActivated = !Interlocked.Exchange(ref _hasExecutingItem, true);
            if (recoveryActivated)
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                // No-lock path: a null _activationLock means no escalation ever happened, so no
                // advancer exists to read this flag. Single writer, plain write suffices.
                _hasExecutingItem = false;
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
            }
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, _enumerator.CompletionToken).ConfigureAwait(false);

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

            _tailWaiter = recoveryItem;
            _tailWaiterTask = result.PipelineTask;
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
        var alreadyActivated = !Interlocked.Exchange(ref _hasExecutingItem, false);
        // Only clear _executingItem when our Exchange won. If the advancer won, it reads
        // _executingItem under its lock for the C-path activation, clearing here would NRE it.
        if (!alreadyActivated && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _executingItem = default!;

        // Fence-acquire so CompleteWaiter below cannot fire CompleteItem before the advancer's
        // in-progress ActivateHeadItem finishes. Same pattern as ClearExecutingItem's deferred branch.
        // alreadyActivated also covers the "never published" case (inline activation took the
        // count==0 branch and never set _hasExecutingItem). When _activationLock is still null no
        // advancer has ever run, so the fence has nothing to synchronize with and the skip is safe.
        if (alreadyActivated && _activationLock is { } activationLock)
            lock (activationLock) { }

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

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, exception);
        if (!_policy.TryRecoverItemFailure(context, item, _enumerator.CompletionToken, out var recoveryItem))
        {
            CompleteWaiter(item, exception);
            return;
        }

        ActivateHeadItem(recoveryItem, preferAsync: false);

        PipelineItemResult result;
        try
        {
            result = await _policy.ExecuteItemAsync(recoveryItem, _enumerator.CompletionToken).ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
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
                CompleteWaiter(recoveryItem, ex);
                return;
            }
        }

        var pipelineTask = result.PipelineTask;
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
            CompleteWaiter(recoveryItem, pipelineException);
        }
        else if (_enumerator.CompletionToken.IsCancellationRequested)
        {
            // Pipeline shutdown while recovery's async work was in flight. EnqueueWaiter at this
            // point would leak: OnWaiterTaskCompleted bails on wake completion, and DrainOnCompletionAsync
            // has likely already passed. Complete directly so depth tracking and CompleteItem still fire.
            CompleteWaiter(recoveryItem, _completionException);
        }
        else
        {
            CommitWaiter(recoveryItem, activated: true, pipelineTask);
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
        _policy.CompleteItem(item, depth, exception);
        return depth is 0;
    }

    /// Commits a waiter to the store and coordinates activation with the execution loop. The
    /// store routes to the inline slot when empty (zero alloc), otherwise it escalates to the
    /// SPSC queue (allocates queue + lock on first overlap). The wired completion callback is
    /// chosen accordingly: the slot path uses _onCommittedTaskCompletedAction (which morphs into
    /// the advancer-wake path after a later escalation, since the slot contents get moved to
    /// queue head and the callback's drain still finds them as the head item). The queue path
    /// uses _onWaiterTaskCompletedAction directly. All call sites run on the executor's logical
    /// thread, preserving the SPSC single-producer contract.
    void CommitWaiter(T item, bool activated, ValueTask waiterTask)
    {
        // Allocate the activation lock on first commit (slot or queue): the count==0 handoff
        // serializes against the executor's deferred-publish + ClearExecutingItem fence-acquire,
        // and deferred-publish kicks in for any subsequent iter once Count > 0. One small Lock
        // allocation per Pipeline lifetime, amortized across all subsequent commits and runs.
        _activationLock ??= new Lock();

        var count = _waiters.TryEscalateOrEnqueue(item, waiterTask, out var isSlot, out var slotWasMoved);
        var wasEmpty = count is 1;

        // The atomic _hasExecutingItem claim already happened in CommitTailWaiter /
        // ClearExecutingItem. Just clear for hygiene and inline-activate if we're the head.
        if (!activated)
        {
            _hasExecutingItem = false;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _executingItem = default!;

            if (wasEmpty && !waiterTask.IsCompleted)
            {
                ActivateHeadItem(item, preferAsync: true);
            }
        }

        var callback = isSlot ? _onCommittedTaskCompletedAction : _onWaiterTaskCompletedAction;
        if (waiterTask.IsCompleted)
            callback();
        else
            waiterTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(callback);

        // During first escalation a slot callback can fire after the queue is published but
        // before the slot contents are moved into it, bumping completedCount without finding
        // anything to drain. Without this nudge the slot item would wait for the next callback
        // fire, unbounded when the next item is a long-lived flow (exclusive scope, COPY, LISTEN).
        if (slotWasMoved && _waiterCompletedCount > 0 && _advancing.TryAcquire())
            DrainReadyWaiters();
    }

    /// Called when a waiter's pipeline task completes. Signals readiness and tries to become the advancer.
    void OnWaiterTaskCompleted()
    {
        Interlocked.Increment(ref _waiterCompletedCount);

        // Try to become the advancer, only one thread processes completions at a time.
        // Don't acquire if completed, CompleteAsync will drain remaining items.
        if (_enumerator.CompletionToken.IsCancellationRequested || !_advancing.TryAcquire())
            return;

        DrainReadyWaiters();
    }

    /// Wired on the slot-tier UnsafeOnCompleted. The same callback covers both pre- and
    /// post-escalation: if the store has not escalated when this fires, the slot drain happens
    /// inline. If escalation moved the slot contents to queue head before this fired, the
    /// callback functions as OnWaiterTaskCompleted and the standard advancer drains the queue.
    void OnCommittedTaskCompleted()
    {
        Interlocked.Increment(ref _waiterCompletedCount);

        if (_enumerator.CompletionToken.IsCancellationRequested || !_advancing.TryAcquire())
            return;

        if (_waiters.IsEscalated)
        {
            // The slot's (item, task) was moved to queue head by EscalateAndEnqueue, and head's
            // task is the one that just completed. Standard drain picks it up.
            DrainReadyWaiters();
            return;
        }

        DrainSlotInline();
    }

    /// Slot-mode drain. Advancer latch is held by the caller. CAS-claims the slot and processes
    /// the (item, task). A concurrent escalation that won the CAS leaves nothing here to drain
    /// and we re-route to the queue drain.
    void DrainSlotInline()
    {
        if (!_waiters.TryClaimSlotForDrain(out var item, out var task))
        {
            // Escalation got there first, and head of queue is our item now.
            if (_waiters.IsEscalated)
            {
                DrainReadyWaiters();
                return;
            }
            // Slot already drained elsewhere. Shouldn't happen given single-callback wiring.
            Interlocked.Decrement(ref _waiterCompletedCount);
            _advancing.Release();
            SignalAdvancerIdleIfWaiting();
            return;
        }

        Interlocked.Decrement(ref _waiterCompletedCount);
        var count = _waiters.DecrementCount();
        Debug.Assert(count == 0);

        bool advance;
        bool emptyReached;
        if (task.IsCompletedSuccessfully)
        {
            task.GetAwaiter().GetResult();
            emptyReached = CompleteWaiterDeferred(item, null);
            advance = true;
        }
        else
        {
            advance = RecoverWaiter(item, task, out emptyReached);
        }

        if (!advance)
        {
            // Recovery item taking over. Advancer flag stays held, recovery continuation releases.
            return;
        }

        // count is 0. Same lock-guarded claim+activate as DrainReadyWaiters' count==0 branch:
        // the executor may have deferred-published a next item against our Count==1 read. If so,
        // claim and activate under the lock so its ClearExecutingItem fence-acquire sees us done.
        lock (_activationLock!)
        {
            if (Interlocked.Exchange(ref _hasExecutingItem, false))
            {
                if (_waiters.Count is 0)
                {
                    var executing = _executingItem;
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        _executingItem = default!;
                    ActivateHeadItem(executing, preferAsync: true);
                }
                else if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                {
                    _executingItem = default!;
                }
            }
        }

        _advancing.Release();
        // Signal AFTER advancer release: prevents a WaitForEmptyAsync awaiter from resuming and
        // committing a new slot waiter whose callback would then race the still-held advancer
        // (TryAcquire fails, callback bails, count stranded - exactly the slot-drain stranding
        // case the queue path's do-while reclaim recovers from but DrainSlotInline can't).
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalAdvancerIdleIfWaiting();
    }

    /// The advancer loop: processes completed waiters from the head of the queue in order.
    /// Only one thread runs this at a time, ensuring head-first processing and ordered completion.
    /// Reachable only after a waiter was committed and escalated to the queue (slot-mode drains
    /// run inline in OnCommittedTaskCompleted), so _activationLock is non-null and _waiters.Queue
    /// is non-null on every code path here.
    void DrainReadyWaiters()
    {
        var emptyReached = false;
        do
        {
            while (_waiters.TryPeek(out var item) && item.WaiterTask.IsCompleted)
            {
                _waiters.TryDequeue(out _);
                Interlocked.Decrement(ref _waiterCompletedCount);

                // Process the completed waiter.
                var waiter = item.Waiter;
                bool advance;
                if (item.WaiterTask.IsCompletedSuccessfully)
                {
                    item.WaiterTask.GetAwaiter().GetResult();
                    // Accumulate the idle signal across the do-while: depth can reach 0 mid-loop
                    // (last drained item), but the OR captures it for after the final release.
                    emptyReached |= CompleteWaiterDeferred(waiter, null);
                    advance = true;
                }
                else
                {
                    advance = RecoverWaiter(waiter, item.WaiterTask, out var recoveryEmpty);
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
                Debug.Assert(count >= 0);

                if (count is 0)
                {
                    // Last waiter drained. Hold the lock around claim+activate so the executor's
                    // ClearExecutingItem fence-acquire blocks until ActivateHeadItem finishes.
                    // Wrapping just the ActivateHeadItem call would leave a TOCTOU where the
                    // executor observes the Exchange and acquires its fence-lock uncontested.
                    lock (_activationLock!)
                    {
                        if (Interlocked.Exchange(ref _hasExecutingItem, false))
                        {
                            // Re-check count under the lock. Executor iterations between our
                            // Decrement and here can enqueue waiters and re-publish _executingItem.
                            // The latest publish is no longer the head, so activating here would
                            // double-activate (now via C-path, again via D-path). Skip when count > 0.
                            // Plain read: the Exchange above is a full fence (acquire on the read),
                            // so this read sees the latest committed value without its own LDAR.
                            if (_waiters.Count is 0)
                            {
                                var executing = _executingItem;
                                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                                    _executingItem = default!;
                                ActivateHeadItem(executing, preferAsync: true);
                            }
                            else if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                            {
                                _executingItem = default!;
                            }
                        }
                    }
                }
                else
                {
                    // More waiters remain, activate the next one (different item from _executingItem
                    // so no executor-completion race, no activation lock needed).
                    _waiters.TryPeek(out var nextItem);
                    ActivateHeadItem(nextItem.Waiter, preferAsync: true);
                }
            }

            // Release latch, then re-check for late arrivals. Plain read of _waiterCompletedCount
            // is safe: sandwiched between this release and TryReclaimAdvancerForWork's acquire,
            // both full barriers via Latch's underlying Interlocked.Exchange.
            _advancing.Release();
        } while (!_enumerator.CompletionToken.IsCancellationRequested && _waiterCompletedCount > 0 && TryReclaimAdvancerForWork());

        // Signal AFTER advancer release for the same reason as DrainSlotInline (external awaiter
        // must not resume while internal sync is still held).
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalAdvancerIdleIfWaiting();

        // Re-acquire the advancer latch and check for a ready waiter. Returns true with the latch
        // held if there's work, false (latch released) otherwise. TryPeek MUST run inside the
        // latch's protection so the SPSC single-consumer contract on _waiters holds against a
        // racing OnWaiterTaskCompleted caller.
        bool TryReclaimAdvancerForWork()
        {
            if (!_advancing.TryAcquire())
                return false;

            if (_waiters.TryPeek(out var pending) && pending.WaiterTask.IsCompleted)
                return true;

            _advancing.Release();
            return false;
        }
    }

    /// Returns true if the advancer should continue (recovery completed or no recovery),
    /// false if recovery is occupying this pipeline position (advancer must stop, recovery will resume knowing it held the advancer flag).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal up to the drain caller
    /// on the sync return-true paths so it fires after _advancing.Release(). The async return-false
    /// path goes through the continuation chain which still uses CompleteRecoveryWaiter inline
    /// (residual race window, but DrainReadyWaiters' do-while reclaim downstream catches any
    /// stranded queue counts; slot-mode stranding from recovery is the case handled here).
    [MethodImpl(MethodImplOptions.NoInlining)]
    bool RecoverWaiter(T failedItem, ValueTask waiterTask, out bool emptyReached)
    {
        Debug.Assert(waiterTask.IsCompleted && !waiterTask.IsCompletedSuccessfully);

        emptyReached = false;

        Exception ex;
        try
        {
            waiterTask.GetAwaiter().GetResult();
            ex = null!; // Unreachable, task was faulted.
        }
        catch (Exception e)
        {
            ex = e;
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
            executeTask = _policy.ExecuteItemAsync(recoveryItem, _enumerator.CompletionToken);
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
        _advancing.Release();
        SignalAdvancerIdleIfWaiting();
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
    void SignalAdvancerIdleIfWaiting()
        => Volatile.Read(ref _advancerIdleTcs)?.TrySetResult();

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
        Debug.Assert(count >= 0);

        if (count is 0)
        {
            // Same lock-guarded claim+activate as DrainReadyWaiters: lock wraps the Exchange so
            // the executor's fence-acquire blocks until ActivateHeadItem finishes.
            // Advancer chain reachability implies _activationLock is non-null.
            lock (_activationLock!)
            {
                if (Interlocked.Exchange(ref _hasExecutingItem, false))
                {
                    // Re-check count under the lock (see DrainReadyWaiters count==0 branch).
                    // Plain read: the Exchange above provides the acquire fence.
                    if (_waiters.Count is 0)
                    {
                        var executing = _executingItem;
                        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                            _executingItem = default!;
                        ActivateHeadItem(executing);
                    }
                    else if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    {
                        _executingItem = default!;
                    }
                }
            }
        }
        else
        {
            _waiters.TryPeek(out var nextItem);
            ActivateHeadItem(nextItem.Waiter);
        }

        // Continue draining ready items, we already hold the advancer flag.
        DrainReadyWaiters();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivateHeadItem(T item, bool preferAsync = true) => _policy.ActivateHeadItem(item, preferAsync);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ClearExecutingItem(bool wasActivated)
    {
        if (!wasActivated)
        {
            if (Interlocked.Exchange(ref _hasExecutingItem, false))
            {
                // Won the race-back: advancer won't activate. Item is done (callers invoke us after
                // pipelineTask.IsCompleted), so activation is optional, skip it.
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
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
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    static void ThrowCompleted() => throw new InvalidOperationException("The pipeline has been completed.");


    public struct Enumerator
    {
        readonly Pipeline<T, TPolicy, TSource, TEnumerator> _pipeline;
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>.Enumerator _waitersEnumerator;
        // 0: init queue waiters, 1: enumerate queue waiters, 2: slot waiter, 3: tail waiter, 4: done
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
                    // Null queue means the pipeline never escalated. Skip to the slot phase.
                    var queue = _pipeline._waiters.Queue;
                    if (queue is null)
                    {
                        _phase = 2;
                        goto case 2;
                    }
                    _waitersEnumerator = new(queue);
                    _phase = 1;
                    goto case 1;
                case 1:
                    while (_waitersEnumerator.MoveNext())
                    {
                        if (_waitersEnumerator.Current.Waiter is { } waiter)
                        {
                            Current = waiter;
                            return true;
                        }
                    }
                    _phase = 2;
                    goto case 2;
                case 2:
                    _phase = 3;
                    if (_pipeline._waiters.TrySnapshotSlot(out var slotItem) && slotItem is { } slot)
                    {
                        Current = slot;
                        return true;
                    }
                    goto case 3;
                case 3:
                    _phase = 4;
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
        var source = UnboundedQueueSource<T>.Create(runContinuationsAsynchronously, scheduler ?? policy.ExecutionScheduler, cancellationToken);
        var pipeline = new Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator>(policy, source);
        return new QueuedPipeline<T, TPolicy>(pipeline, source);
    }

    /// <summary>
    /// Packed state word coordinating pipeline depth and the drain-waiter (WaitForIdleAsync) protocol.
    /// Layout:
    ///   bits 0-31 : depth (int), total of queued + waiting items
    ///   bit  32   : DrainBit, set when a drain waiter is registered in <see cref="_drainTcs"/>
    ///   bits 33-63: reserved for future flags
    /// </summary>
    /// <remarks>
    /// Depth changes use <see cref="Interlocked.Increment(ref int)"/> and <see cref="Interlocked.Decrement(ref int)"/>
    /// on the first 32 bits via <see cref="Unsafe.As{TFrom, TTo}(ref TFrom)"/> (endianness-aware).
    /// This keeps the hot path (every Enqueue and CompleteWaiter) at native 32-bit Interlocked cost
    /// while the drain side can CAS the full word atomically.
    /// <para>
    /// Order on the consumer is <em>clear-bit-before-Exchange-TCS</em>. A publisher racing in after
    /// the clear will re-run its publish path and either reuse the still-present <see cref="_drainTcs"/>
    /// (we care about idle state convergence, not exactly-once semantics, TrySetResult handles the at most once publication).
    /// </para>
    /// </remarks>
    internal struct DepthState
    {
        ulong _value;
        TaskCompletionSource? _drainTcs;

        const ulong DrainBit = 1UL << 32;

        /// Returns a ref to the low-32-bits half of <see cref="_value"/>, regardless of endianness.
        /// On little-endian the low half sits at byte offset 0, on big-endian at offset 4.
        /// Split as if/else (not ternary inside Add) so the JIT constant-folds
        /// <see cref="BitConverter.IsLittleEndian"/> and elides the Add(0) on LE.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ref int DepthRef(ref ulong value)
        {
            if (BitConverter.IsLittleEndian)
                return ref Unsafe.As<ulong, int>(ref value);
            return ref Unsafe.Add(ref Unsafe.As<ulong, int>(ref value), 1);
        }

        /// <summary>Current depth. Lock-free, <see cref="Volatile.Read(ref readonly int)"/> semantics.</summary>
        public int Depth
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref DepthRef(ref _value));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementDepth()
        {
            // Single-producer pre-check, race-safe: only the producer increments, decrements
            // only lower the value. Observing MaxValue means a further Increment would wrap to
            // MinValue and silently break the drain protocol (negative depth never reaches 0).
            // Hitting this implies an unbounded producer. The queue's GC heap would exhaust
            // long before this in any realistic workload.
            if (Depth == int.MaxValue)
                ThrowOverflow();
            Interlocked.Increment(ref DepthRef(ref _value));
        }

        [DoesNotReturn]
        static void ThrowOverflow() => throw new InvalidOperationException("Pipeline depth overflow.");

        /// <summary>Decrements depth. Returns the new value. Caller MUST invoke
        /// <see cref="OnDepthReachedZero"/> when the result is 0 to signal any pending drain waiter.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int DecrementDepth() => Interlocked.Decrement(ref DepthRef(ref _value));

        /// <summary>
        /// Returns a task that completes when depth reaches 0. Lock-free publish + backstop.
        /// Drain protocol:
        /// </summary>
        /// <list type="bullet">
        /// <item><see cref="GetIdleTask"/> short-circuits on depth==0. Otherwise it publishes
        /// <see cref="_drainTcs"/> via <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>
        /// (concurrent publishers share), then CAS-loops to set <see cref="DrainBit"/>. Observing
        /// depth==0 during the loop means a transition happened during publish, so it self-signals
        /// and returns.</item>
        /// <item><see cref="OnDepthReachedZero"/> CAS-loops to clear <see cref="DrainBit"/>. The clearer
        /// takes <see cref="_drainTcs"/> via <see cref="Interlocked.Exchange{T}(ref T, T)"/> and signals it.</item>
        /// </list>
        public ValueTask GetIdleTask(CancellationToken cancellationToken)
        {
            if (Depth is 0)
                return ValueTask.CompletedTask;

            var newTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tcs = Interlocked.CompareExchange(ref _drainTcs, newTcs, null) ?? newTcs;

            while (true)
            {
                var state = Volatile.Read(ref _value);
                if ((int)state is 0)
                {
                    // Depth reached 0 during our publish window. Self-signal and clear our
                    // published slot so the next caller doesn't reuse the completed TCS.
                    // OnDepthReachedZero won't clear it because DrainBit was never set on this path.
                    // Plain write is safe, the API is single-caller.
                    tcs.TrySetResult();
                    _drainTcs = null;
                    break;
                }
                if ((state & DrainBit) != 0)
                    break; // already published by us or another publisher.
                if (Interlocked.CompareExchange(ref _value, state | DrainBit, state) == state)
                    break;
            }

            return new(tcs.Task.WaitAsync(cancellationToken));
        }

        /// <summary>
        /// Called by the consumer after <see cref="DecrementDepth"/> returns 0. Clears
        /// <see cref="DrainBit"/> and signals the pending drain waiter, if any.
        /// </summary>
        public void OnDepthReachedZero()
        {
            while (true)
            {
                var state = Volatile.Read(ref _value);
                if ((state & DrainBit) == 0)
                    return; // no waiter, or already consumed by a racing decrementer.
                if (Interlocked.CompareExchange(ref _value, state & ~DrainBit, state) == state)
                {
                    var tcs = Interlocked.Exchange(ref _drainTcs, null);
                    tcs?.TrySetResult();
                    return;
                }
            }
        }
    }
}
