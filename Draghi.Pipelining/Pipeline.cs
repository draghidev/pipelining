using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// <summary>
/// Single-producer, single-consumer pipelined request/response coordinator. The class is not
/// thread-safe. All public instance methods (<see cref="Enqueue"/>, <see cref="CompleteAsync"/>,
/// <see cref="WaitForIdleAsync"/>, <see cref="GetEnumerator"/>, the <see cref="Depth"/> getter,
/// <see cref="CompletionToken"/>) must be invoked from a single caller at a time. Concurrent
/// calls produce undefined results (stale signals, missed completions). Callers needing
/// multi-threaded access must serialize externally.
/// </summary>
/// <remarks>
/// Internally the class IS concurrent: the execution loop runs on its own scheduler thread, the
/// advancer can fire on threadpool continuations driven by waiter-task completions, and the
/// recovery paths span async boundaries. The "not thread-safe" contract applies specifically to
/// the public API surface, where the cost of broadly thread-safe semantics would not be justified
/// for the target use case (connection-bound network protocol clients with a single producer).
/// Adding piecemeal thread safety to individual methods is worse than committing to either pure
/// stance, so we don't.
/// </remarks>
public sealed class Pipeline<T, TPolicy>
    where TPolicy : IPipelinePolicy<T>
{
    TPolicy _policy;

    // Execution.
    readonly SingleProducerSingleConsumerQueue<Element> _queue;
    bool _notEmpty; // Set by Enqueue before the queue write, checked by the executor under the wake lock.
    readonly Pipeline.WakeSignal _wakeSignal;
    readonly Task _executionTask;
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
    readonly SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)> _waiters;
    int _waiterQueueCount; // Authoritative count. Interlocked by executor and advancer.
    int _waiterCompletedCount; // Number of completed waiter tasks not yet processed by the advancer.
    bool _advancing; // True if a thread is currently the advancer.
    bool _waiterInRecovery; // True when recovery holds _advancing across an async boundary.
    T _waiterRecoveryItem = default!; // The item being recovered, so DrainOnCompletion can complete it.
    readonly Lock _waiterRecoveryLock = new(); // Protects AdvanceAndDrain in recovery from racing with CompleteAsync's drain.

    // Activation.
    readonly Action _onWaiterTaskCompletedAction;

    internal Pipeline(TPolicy policy)
    {
        _policy = policy;
        _queue = new();
        _waiters = new();
        _wakeSignal = new(policy.RunEnqueueAsynchronously, policy.ExecutionScheduler ?? PipelineScheduler.ThreadPool);
        _onWaiterTaskCompletedAction = OnWaiterTaskCompleted;
        _executionTask = ExecuteQueue();
    }

    // _activationLock: Lock because it fires off the hot path (advancer/recovery handoff) and
    // the OS handoff is preferable if it ever contends.
    readonly Lock _activationLock = new();


    /// Cancellation token that fires when the pipeline is completing. Can be used by the protocol
    /// to create a linked token or to abort IO operations directly.
    public CancellationToken CompletionToken => _wakeSignal.CompletionToken;

    public int Depth => _depthState.Depth;

    /// Returns an action to invoke outside any held lock to signal the execution loop to continue.
    public Pipeline.EnqueueResult Enqueue(T item)
    {
        if (_wakeSignal.IsCompleted)
            ThrowCompleted();

        // IncrementDepth BEFORE _queue.Enqueue, so depth is a strict upper bound. If the increment
        // came after the queue publish, the executor could dequeue and decrement before this thread
        // got to increment, producing a negative remainingDepth in CompleteItem and skipping
        // OnDepthReachedZero's depth==0 firing.
        _depthState.IncrementDepth();
        // Plain write ordered before the enqueue by the SPSC queue's internal volatile write of _last.
        // The executor's wake-lock acquire at the top of the loop is the cross-thread fence
        // that makes this visible; no fence needed on this side. Keeps Enqueue hot.
        _notEmpty = true;
        _queue.Enqueue(new(item));

        return new(_wakeSignal);
    }

    /// Waits for the pipeline to become idle (all items completed, depth reaches zero).
    /// Does not prevent new items from being enqueued, the caller is responsible for that.
    /// <remarks>
    /// Also waits for any in-flight advancer / recovery continuation to unwind past the
    /// CompleteWaiter that fired the depth-zero signal. Without that, the advancer's lambda
    /// frames (still on the stack returning out of AdvanceAndDrainRecovery and friends) hold
    /// the just-completed item in their captures, racing any caller that asserts on item
    /// release immediately after WaitForIdleAsync returns.
    /// </remarks>
    public ValueTask WaitForIdleAsync(CancellationToken cancellationToken = default)
    {
        var idle = _depthState.GetIdleTask(cancellationToken);
        // Skip the advancer wait once the wake signal is completed: DrainOnCompletionAsync
        // claims _advancing and never releases it, so the spin would hang under shutdown.
        if (idle.IsCompletedSuccessfully && (_wakeSignal.IsCompleted || !Volatile.Read(ref _advancing)))
            return ValueTask.CompletedTask;
        return WaitForIdleSlowAsync(idle);
    }

    async ValueTask WaitForIdleSlowAsync(ValueTask idle)
    {
        await idle.ConfigureAwait(false);
        while (!_wakeSignal.IsCompleted && Volatile.Read(ref _advancing))
            await Task.Yield();
    }

    public ValueTask CompleteAsync(Exception? exception = null)
    {
        if (Interlocked.Exchange(ref _completing, true))
            return new(_executionTask);

        _completionException = exception;
        _wakeSignal.Complete();

        // The executor drains remaining items on exit. Await it for full completion.
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

    async Task ExecuteQueue()
    {
        var needsYieldAfterFirst = true;
        // Promoted out of the inner loop so the pre-idle clear below can null them out.
        // Roslyn won't properly clear them when leaving scope via a goto otherwise.
        Element element;
        T item;
        PipelineItemResult itemResult;
        while (!_wakeSignal.IsCompleted)
        {
            _wakeSignal.AcquireWakeLock();
            _notEmpty = false;
            if (_queue.IsEmpty && !_notEmpty)
            {
                // Lock held through OnCompleted which releases it after storing the continuation.
                if (!await _wakeSignal.WaitUnsynchronized())
                    break;
            }
            else
            {
                _wakeSignal.ReleaseWakeLock();
            }

            while (!_wakeSignal.IsCompleted && _queue.TryDequeue(out element))
            {
                // Commit the previous iteration's pending tail to the waiter queue. CommitTailWaiter
                // is sync in all paths except trailing recovery, where it returns a Task to await.
                var commitWork = CommitTailWaiter();
                if (commitWork is not null)
                    await commitWork.ConfigureAwait(false);

                item = element.Value;

                var activated = false;
                if (Volatile.Read(ref _waiterQueueCount) is 0)
                {
                    ActivateHeadItem(item, preferAsync: false);
                    activated = true;
                }
                else
                {
                    _executingItem = item;
                    Interlocked.Exchange(ref _hasExecutingItem, true);
                }

                try
                {
                    itemResult = await _policy.ExecuteItemAsync(item, _wakeSignal.CompletionToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ClearExecutingItem(activated);
                    await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), activated, _wakeSignal.CompletionToken).ConfigureAwait(false);
                    goto afterItem;
                }

                // Handle pipeline task completion.
                if (itemResult.PipelineTask.IsCompletedSuccessfully)
                {
                    itemResult.PipelineTask.GetAwaiter().GetResult();
                    ClearExecutingItem(activated);
                    CompleteWaiter(item, null);
                }
                else if (itemResult.PipelineTask.IsCompleted)
                {
                    // Pipeline task faulted synchronously.
                    ClearExecutingItem(activated);
                    try
                    {
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex), activated, _wakeSignal.CompletionToken).ConfigureAwait(false);
                        goto afterItem;
                    }

                    CompleteWaiter(item, null);
                }
                else
                {
                    // Pipeline task pending, store as pending tail for the next iteration.
                    _tailWaiter = item;
                    _hasTailWaiter = true;
                    _tailWaiterTask = itemResult.PipelineTask;
                }

                // Await trailing execution task if present.
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
                            await RecoverTrailingFailure(item, activated, ex, _wakeSignal.CompletionToken).ConfigureAwait(false);
                        }
                    }
                }

                afterItem:
                if (needsYieldAfterFirst)
                {
                    needsYieldAfterFirst = false;
                    if (_queue.IsEmpty)
                        break;

                    var yieldTask = _policy.YieldAfterFirstItem();
                    if (yieldTask != default)
                        await yieldTask.ConfigureAwait(false);
                }
            }

            // Clear the locals so the executor's async state machine box does not
            // retain the last-processed item and its tasks across the park.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                item = default!;
                element = default;
            }
            itemResult = default;

            // Commit any remaining tail before going idle. If the tail needs recovery, the
            // executor awaits the inline recovery flow here. OnExecutionIdleAsync below
            // observes a fully-drained tail state, idle truly means idle.
            var preIdleCommit = CommitTailWaiter();
            if (preIdleCommit is not null)
                await preIdleCommit.ConfigureAwait(false);

            // No queue empty check before idle: the wake signal is the floor for every race
            // between drain-completion and OnExecutionIdleAsync actually parking, so any
            // pre-idle flag/queue probe is pure overhead that the signal would catch anyway.
            // Callers requiring strict ordering with idle handoff must synchronize externally.
            try
            {
                var idleTask = _policy.OnExecutionIdleAsync(_wakeSignal.CompletionToken);
                if (idleTask != default)
                    await idleTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _ = CompleteAsync(ex);
                // Drain, then throw so _executionTask faults and callers observe the failure.
                await DrainOnCompletionAsync();
                throw;
            }

            needsYieldAfterFirst = true;
        }

        await DrainOnCompletionAsync();
    }

    /// Drains remaining items after the execution loop exits. Coordinates with any in-flight
    /// recovery continuation via _waiterRecoveryLock: whichever side observes _waiterInRecovery
    /// first under the lock completes the recovery item. _advancing is released by the
    /// continuation in its bailout path, so the wait below always terminates.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        lock (_waiterRecoveryLock)
        {
            if (_waiterInRecovery)
            {
                _waiterInRecovery = false;
                CompleteWaiter(_waiterRecoveryItem, _completionException);
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _waiterRecoveryItem = default!;
            }
        }

        // Wait for any in-flight advancer / recovery continuation to release the flag.
        while (Interlocked.Exchange(ref _advancing, true))
            await Task.Yield();

        var exception = _completionException;
        while (_waiters.TryDequeue(out var item))
            CompleteWaiter(item.Waiter, exception);
        while (_queue.TryDequeue(out var item))
            CompleteWaiter(item.Value, exception);
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
            var result = await _policy.ExecuteItemAsync(recoveryItem, _wakeSignal.CompletionToken).ConfigureAwait(false);

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
                _hasTailWaiter = true;
                _tailWaiterTask = result.PipelineTask;
            }
        }
        catch (Exception recoveryEx)
        {
            // Reaching this catch means an await in the try block threw before recovery's tail
            // was published (the only code that writes _tailWaiter*/_hasTailWaiter is in the
            // pipeline-task-pending branch at the end of the try, which doesn't throw). So there's
            // no in-flight tail-waiter task to observe here; CommitTailWaiter cleared the slots
            // before this iteration began.
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
            EnqueueWaiter(item, activated, new(pipelineTask));
            return;
        }

        // Swap the tail: replace _executingItem with the recovery item.
        // If null was returned, the waiter path already claimed it, activate recovery too.
        // Under the activation lock: serializes with the advancer's count==0 claim path so the
        // advancer can't observe the partial state (_executingItem=recovery, _hasExecutingItem still
        // reflecting the original item) and double-activate recovery.
        // When we activate inline, also clear _hasExecutingItem and _executingItem so a later
        // advancer claim (e.g., from a waiter completing during recovery's ExecuteItemAsync await)
        // cannot read our published recovery and activate it a second time.
        bool recoveryActivated;
        lock (_activationLock)
        {
            _executingItem = recoveryItem;
            recoveryActivated = !Interlocked.Exchange(ref _hasExecutingItem, true);
            if (recoveryActivated)
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                Interlocked.Exchange(ref _hasExecutingItem, false);
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
            }
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, _wakeSignal.CompletionToken).ConfigureAwait(false);

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
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _executingItem = default!;

        // If the advancer claimed first, it is now inside lock(_activationLock) ActivateHeadItem(item).
        // Fence-acquire so CompleteWaiter / RecoverCommittedTailWaiter below cannot fire CompleteItem
        // before that activation finishes. Same pattern as ClearExecutingItem's deferred branch.
        if (alreadyActivated)
            lock (_activationLock) { }

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

        EnqueueWaiter(item, activated: alreadyActivated, task);
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

        // Pipeline task failed, attempt recovery.
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, exception);
        if (!_policy.TryRecoverItemFailure(context, item, _wakeSignal.CompletionToken, out var recoveryItem))
        {
            CompleteWaiter(item, exception);
            return;
        }

        ActivateHeadItem(recoveryItem, preferAsync: false);

        PipelineItemResult result;
        try
        {
            result = await _policy.ExecuteItemAsync(recoveryItem, _wakeSignal.CompletionToken).ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
            CompleteWaiter(recoveryItem, recoveryEx);
            return;
        }

        // Observe trailing execution task.
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
        else if (_wakeSignal.IsCompleted)
        {
            // Pipeline shutdown while recovery's async work was in flight. EnqueueWaiter at this
            // point would leak: OnWaiterTaskCompleted bails on wake completion, and DrainOnCompletionAsync
            // has likely already passed. Complete directly so depth tracking and CompleteItem still fire.
            CompleteWaiter(recoveryItem, _completionException);
        }
        else
        {
            // Recovery's pipeline task is pending, enqueue it.
            EnqueueWaiter(recoveryItem, activated: true, pipelineTask);
        }
    }

    void CompleteWaiter(T item, Exception? exception)
    {
        var depth = _depthState.DecrementDepth();
        _policy.CompleteItem(item, depth, exception);

        if (depth is 0)
            _depthState.OnDepthReachedZero();
    }

    /// Enqueues as a waiter and coordinates activation with the execution loop.
    /// Producer-side is restricted to the executor's logical thread (CommitTailWaiter,
    /// RecoverTrailingFailure no-recovery branch, RecoverCommittedTailWaiterAsync's pending-pipeline
    /// branch). The latter runs under the executor's await chain so it's still executor-thread.
    /// This preserves the SPSC contract on _waiters without a lock.
    void EnqueueWaiter(T item, bool activated, ValueTask waiterTask)
    {
        _waiters.Enqueue((item, waiterTask));
        var wasEmpty = Interlocked.Increment(ref _waiterQueueCount) is 1;

        // Coordinate with the execution loop's deferred activation.
        if (!activated)
        {
            // If we were the first waiter, all previous waiters may have drained while we were inside Execute.
            // Atomically claim _executingItem: if non-null, the advancer didn't activate us.
            if (wasEmpty && Interlocked.Exchange(ref _hasExecutingItem, false))
            {
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
                ActivateHeadItem(item);
            }
            else
            {
                _hasExecutingItem = false;
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
            }
        }

        // Activation may have caused synchronous completion.
        if (wasEmpty && waiterTask.IsCompleted)
            OnWaiterTaskCompleted();
        else
            waiterTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onWaiterTaskCompletedAction);
    }

    /// Called when a waiter's pipeline task completes. Signals readiness and tries to become the advancer.
    void OnWaiterTaskCompleted()
    {
        Interlocked.Increment(ref _waiterCompletedCount);

        // Try to become the advancer, only one thread processes completions at a time.
        // Don't acquire if completed, CompleteAsync will drain remaining items.
        if (_wakeSignal.IsCompleted || Interlocked.Exchange(ref _advancing, true))
            return;

        DrainReadyWaiters();
    }

    /// The advancer loop: processes completed waiters from the head of the queue in order.
    /// Only one thread runs this at a time, ensuring head-first processing and ordered completion.
    void DrainReadyWaiters()
    {
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
                    CompleteWaiter(waiter, null);
                    advance = true;
                }
                else
                {
                    advance = RecoverWaiter(waiter, item.WaiterTask);
                }

                if (!advance)
                {
                    // Recovery item is occupying this pipeline position.
                    // The advancer flag stays held; the recovery continuation will
                    // complete the item, advance, and release the flag.
                    return;
                }

                // Advance: decrement queue count and activate next.
                var count = Interlocked.Decrement(ref _waiterQueueCount);
                Debug.Assert(count >= 0);

                if (count is 0)
                {
                    // Last waiter drained, activate the pending tail or executing item, if any.
                    // Hold the activation lock around the entire claim+activate sequence so the
                    // executor's ClearExecutingItem fence-acquire blocks until ActivateHeadItem
                    // finishes. Wrapping just the ActivateHeadItem call would leave a TOCTOU:
                    // the executor could observe the Exchange's effect and acquire its fence-lock
                    // uncontested before the advancer entered this lock.
                    lock (_activationLock)
                    {
                        if (Interlocked.Exchange(ref _hasExecutingItem, false))
                        {
                            var executing = _executingItem;
                            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                                _executingItem = default!;
                            ActivateHeadItem(executing);
                        }
                    }
                }
                else
                {
                    // More waiters remain, activate the next one (different item from _executingItem
                    // so no executor-completion race, no activation lock needed).
                    _waiters.TryPeek(out var nextItem);
                    ActivateHeadItem(nextItem.Waiter);
                }
            }

            // Release the advancer flag (full barrier) and double-check for late arrivals.
            // Plain read of _waiterCompletedCount is safe here: sandwiched between the Exchange above
            // (release) and the Exchange below (re-acquire), both full barriers on all architectures.
            // This avoids a Volatile.Read which has higher cost on arm64 (ldapr).
            Interlocked.Exchange(ref _advancing, false);
        } while (!_wakeSignal.IsCompleted
                 && _waiterCompletedCount > 0
                 && _waiters.TryPeek(out var pending) && pending.WaiterTask.IsCompleted
                 && !Interlocked.Exchange(ref _advancing, true));
    }

    /// Returns true if the advancer should continue (recovery completed or no recovery),
    /// false if recovery is occupying this pipeline position (advancer must stop, recovery will resume knowing it held the advancer flag).
    [MethodImpl(MethodImplOptions.NoInlining)]
    bool RecoverWaiter(T failedItem, ValueTask waiterTask)
    {
        Debug.Assert(waiterTask.IsCompleted && !waiterTask.IsCompletedSuccessfully);

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
        if (!_policy.TryRecoverItemFailure(context, failedItem, _wakeSignal.CompletionToken, out var recoveryItem))
        {
            CompleteWaiter(failedItem, ex);
            return true;
        }

        // Recovery item takes over, activate it at the current pipeline position.
        ActivateHeadItem(recoveryItem);

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            executeTask = _policy.ExecuteItemAsync(recoveryItem, _wakeSignal.CompletionToken);
        }
        catch (Exception recoveryEx)
        {
            CompleteWaiter(recoveryItem, recoveryEx);
            return true;
        }

        // Unified _waiterInRecovery state for both sync and async paths so CompleteRecoveryWaiter
        // can use the same atomic-claim discipline. In sync path drain can't race (advancer flag
        // held throughout DrainReadyWaiters), so the flag is safely true→false within that call.
        // Item-before-flag with Volatile.Write release on the flag so a racing DrainOnCompletion
        // (whose lock acquire serves as the matching acquire fence) cannot observe the flag=true
        // before the item write. Two plain writes here would let ARM64 store-store reorder them.
        _waiterRecoveryItem = recoveryItem;
        Volatile.Write(ref _waiterInRecovery, true);

        if (executeTask.IsCompletedSuccessfully)
        {
            return RecoverWaiterResult(recoveryItem, executeTask.Result);
        }

        // Execute is async, hook continuation. Advancer stops, continuation owns the flag.
        executeTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            // Bailout: wake signal completed while recovery's executeTask was in flight.
            if (_wakeSignal.IsCompleted)
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

            if (!RecoverWaiterResult(recoveryItem, result))
                return; // pipeline task pending, continuation will resume

            AdvanceAndDrainRecovery();
        });
        return false;
    }

    /// Handles a completed recovery execution result. Returns true if the advancer should continue,
    /// false if the recovery's pipeline task is pending (occupying this position).
    bool RecoverWaiterResult(T recoveryItem, PipelineItemResult result)
    {
        // Await trailing execution, must observe the result.
        if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
        {
            if (result.TrailingExecutionTask.IsCompleted)
            {
                try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                catch (Exception ex)
                {
                    CompleteRecoveryWaiter(recoveryItem, ex);
                    return true;
                }
            }
            else
            {
                // Trailing task is async, hook continuation.
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    Exception? trailingEx = null;
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch (Exception ex) { trailingEx = ex; }

                    // Bailout: wake signal completed while recovery's trailing task was in flight.
                    if (_wakeSignal.IsCompleted)
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

                    if (!RecoverWaiterPipelineTask(recoveryItem, pipelineTask))
                        return; // pipeline task pending, lock still held

                    AdvanceAndDrainRecovery();
                });
                return false;
            }
        }

        return RecoverWaiterPipelineTask(recoveryItem, result.PipelineTask);
    }

    /// Handles the recovery item's pipeline task. Returns true if done, false if pending.
    bool RecoverWaiterPipelineTask(T recoveryItem, ValueTask pipelineTask)
    {
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
            CompleteRecoveryWaiter(recoveryItem, pipelineException);
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
            if (_wakeSignal.IsCompleted)
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

    /// Shared shutdown bailout for recovery continuations. When the wake signal completes mid-recovery,
    /// the continuation coordinates with DrainOnCompletionAsync via _waiterRecoveryLock so the recovery
    /// item is completed exactly once, then releases _advancing so the drain's spin terminates.
    [MethodImpl(MethodImplOptions.NoInlining)]
    void BailoutRecoveryOnShutdown()
    {
        lock (_waiterRecoveryLock)
        {
            if (_waiterInRecovery)
            {
                _waiterInRecovery = false;
                CompleteWaiter(_waiterRecoveryItem, _completionException);
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _waiterRecoveryItem = default!;
            }
        }
        Interlocked.Exchange(ref _advancing, false);
    }

    /// Called from recovery continuations. Tries to take the recovery lock, if it can't
    /// CompleteAsync is already draining and recovery just bails.
    void AdvanceAndDrainRecovery()
    {
        var entered = false;
        try
        {
            entered = _waiterRecoveryLock.TryEnter();
            if (!entered)
            {
                // Plain read is ordered after TryEnter's barrier.
                if (!_wakeSignal.IsCompleted)
                    throw new InvalidOperationException("Concurrent waiter recoveries.");
                _waiterInRecovery = false;
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _waiterRecoveryItem = default!;
                // Release _advancing so DrainOnCompletionAsync's spin terminates.
                Interlocked.Exchange(ref _advancing, false);
                return;
            }
            _waiterInRecovery = false;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _waiterRecoveryItem = default!;
            AdvanceAndDrain();
        }
        finally
        {
            if (entered)
                _waiterRecoveryLock.Exit();
        }
    }

    /// Atomically transitions recovery ownership and completes the recovery item, if we still own it.
    /// Coordinates with DrainOnCompletion to ensure exactly-once CompleteItem invocation when
    /// CompleteAsync races a recovery continuation's success path. Without this, the continuation
    /// and DrainOnCompletion could both call CompleteItem on the same item.
    [MethodImpl(MethodImplOptions.NoInlining)]
    void CompleteRecoveryWaiter(T recoveryItem, Exception? exception)
    {
        bool shouldComplete;
        lock (_waiterRecoveryLock)
        {
            shouldComplete = _waiterInRecovery;
            if (shouldComplete)
            {
                _waiterInRecovery = false;
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _waiterRecoveryItem = default!;
            }
        }
        if (shouldComplete)
            CompleteWaiter(recoveryItem, exception);
    }

    /// Decrements waiter count, activates the next item, and resumes draining.
    /// Must only be called when the advancer flag is held.
    void AdvanceAndDrain()
    {
        var count = Interlocked.Decrement(ref _waiterQueueCount);
        Debug.Assert(count >= 0);

        if (count is 0)
        {
            // Same lock-guarded claim+activate as DrainReadyWaiters: lock wraps the Exchange so
            // the executor's fence-acquire blocks until ActivateHeadItem finishes.
            lock (_activationLock)
            {
                if (Interlocked.Exchange(ref _hasExecutingItem, false))
                {
                    var executing = _executingItem;
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        _executingItem = default!;
                    ActivateHeadItem(executing);
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
                // We won the race-back: no concurrent Activate to wait for.
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
            }
            else
            {
                // Advancer claimed first and is calling ActivateHeadItem under the lock.
                // Wait for it to finish before the caller proceeds to CompleteWaiter, closing the
                // activation-after-completion race. _executingItem was already cleared by the advancer.
                // Empty body is intentional, the acquire/release pair is used purely as a
                // memory fence to synchronize-with the advancer's lock release.
                lock (_activationLock) { }
            }
        }
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    static void ThrowCompleted() => throw new InvalidOperationException("The pipeline has been completed.");


    public struct Enumerator
    {
        readonly Pipeline<T, TPolicy> _pipeline;
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>.Enumerator _waitersEnumerator;
        SingleProducerSingleConsumerQueue<Element>.Enumerator _queueEnumerator;
        // 0: init waiters, 1: enumerate waiters, 2: tail waiter, 3: init queue, 4: enumerate queue, 5: done
        int _phase;

        internal Enumerator(Pipeline<T, TPolicy> pipeline)
        {
            _pipeline = pipeline;
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            switch (_phase)
            {
                case 0:
                    _waitersEnumerator = new(_pipeline._waiters);
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
                    // Match cases 1/4 null-filter: under concurrent enumeration the executor's
                    // publish of _hasTailWaiter=true could be observed (on weak-memory architectures)
                    // before its publish of _tailWaiter, yielding a stale default(T). The
                    // `is { }` pattern filters the ref-T null case in line with the documented
                    // best-effort contract; value-T may still yield default per the same docs.
                    if (_pipeline._hasTailWaiter && _pipeline._tailWaiter is { } tail)
                    {
                        Current = tail;
                        return true;
                    }
                    goto case 3;
                case 3:
                    _queueEnumerator = new(_pipeline._queue);
                    _phase = 4;
                    goto case 4;
                case 4:
                    while (_queueEnumerator.MoveNext())
                    {
                        if (_queueEnumerator.Current.Value is { } queued)
                        {
                            Current = queued;
                            return true;
                        }
                    }
                    _phase = 5;
                    return false;
                default:
                    return false;
            }
        }
    }

    // Used as the array element type to remove array variance checks.
    readonly struct Element(T value)
    {
        public T Value { get; } = value;
    }
}

public static class Pipeline
{
    public static Pipeline<T, TPolicy> Create<T, TPolicy>(TPolicy policy)
        where TPolicy : IPipelinePolicy<T>
        => new(policy);

    /// <summary>
    /// Represents a deferred enqueue completion returned by <see cref="Pipeline{T,TPolicy}.Enqueue"/>.
    /// The item is already in the queue; calling <see cref="Execute"/> may signal the execution loop to process it.
    /// This two-step design exists because the signal may synchronously run the execution loop on the caller's
    /// thread (when <see cref="IPipelinePolicy{T}.RunEnqueueAsynchronously"/> is false),
    /// so it must be invoked outside any held lock.
    /// </summary>
    public readonly struct EnqueueResult
    {
        readonly WakeSignal? _signal;
        internal EnqueueResult(WakeSignal? signal) => _signal = signal;

        /// <summary>Signals the execution loop, which may run the executor inline on the calling thread.</summary>
        public void Execute() => _signal?.Signal();
    }

    /// Single-consumer single-producer wake signal for suspending and waking execution.
    /// Doubles as its own awaitable and awaiter (GetAwaiter returns this).
    /// The wake lock is held from WaitUnsynchronized through OnCompleted, which stores
    /// the continuation and releases the lock. Signal re-acquires the lock to claim the continuation.
    /// A spinlock is used instead of Lock because the critical section is a few field reads/writes
    /// with at most two threads contending (execution loop and Signal caller).
    /// No code under the lock re-enters or is user code that may, so reentrancy support is not needed either.
    internal sealed class WakeSignal : IThreadPoolWorkItem
    {
        readonly bool _runContinuationsAsynchronously;
        readonly CancellationTokenSource _cts = new();
        Action? _continuation;
        bool _pending;
        int _wakeLock;

        public WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler executionScheduler)
        {
            _runContinuationsAsynchronously = runContinuationsAsynchronously;
            Scheduler = executionScheduler;
        }

        public PipelineScheduler Scheduler { get; }
        public CancellationToken CompletionToken => _cts.Token;
        public bool IsCompleted => _cts.IsCancellationRequested;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquireWakeLock()
        {
            if (Interlocked.Exchange(ref _wakeLock, 1) != 0)
                AcquireWakeLockSlow();

            [MethodImpl(MethodImplOptions.NoInlining)]
            void AcquireWakeLockSlow()
            {
                var spinner = new SpinWait();
                while (Interlocked.Exchange(ref _wakeLock, 1) != 0)
                    spinner.SpinOnce();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseWakeLock() => Volatile.Write(ref _wakeLock, 0);

        /// Prepares the signal for a new wait. Must be called under the wake lock.
        /// Returns an awaiter that holds the lock through OnCompleted.
        public Awaiter WaitUnsynchronized()
        {
            Debug.Assert(!_pending, "Concurrent wait calls.");
            _pending = true;
            return new(this);
        }

        public void Signal() => SignalCore(_runContinuationsAsynchronously);

        void SignalCore(bool runContinuationsAsynchronously)
        {
            AcquireWakeLock();
            try
            {
                if (!_pending)
                    return;
                _pending = false;
            }
            finally
            {
                ReleaseWakeLock();
            }

            if (runContinuationsAsynchronously)
            {
                Scheduler.SubmitDetached(this, preferLocal: true);
            }
            else
                _continuation!();
        }

        void IThreadPoolWorkItem.Execute() => _continuation!();

        /// Marks the source as completed, wakes any pending wait.
        public void Complete()
        {
            _cts.Cancel();
            SignalCore(runContinuationsAsynchronously: true);
        }

        internal readonly struct Awaiter(WakeSignal signal) : ICriticalNotifyCompletion
        {
            public Awaiter GetAwaiter() => this;

            // Always false so OnCompleted always runs and releases the lock.
            public bool IsCompleted => false;

            public bool GetResult() => !signal.IsCompleted;

            public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

            public void UnsafeOnCompleted(Action continuation)
            {
                if (!ReferenceEquals(signal._continuation, continuation))
                    signal._continuation = continuation;
                signal.ReleaseWakeLock();

                // If completed while we were setting up the wait, wake ourselves.
                if (signal.IsCompleted)
                    signal.SignalCore(runContinuationsAsynchronously: true);
            }
        }
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
            // Hitting this implies an unbounded producer; the queue's GC heap would exhaust
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
