using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

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
    int _pipelineDepth; // Authoritative total of all queued and waiting items.
    Exception? _completionException;
    TaskCompletionSource? _drainTcs;

    // Executor-owned, only touched by the execution loop. Padded to isolate from cross-thread atomics.
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

    PipelineScheduler ExecutionScheduler => _wakeSignal.Scheduler;

    internal Pipeline(TPolicy policy)
    {
        _policy = policy;
        _queue = new();
        _waiters = new();
        _wakeSignal = new(policy.RunEnqueueAsynchronously, policy.ExecutionScheduler ?? PipelineScheduler.ThreadPool);
        _onWaiterTaskCompletedAction = OnWaiterTaskCompleted;
        _executionTask = ExecuteQueue();
    }

    int _drainLock; // Spinlock for drain TCS and completion state.

    /// Cancellation token that fires when the pipeline is completing. Can be used by the protocol
    /// to create a linked token or to abort IO operations directly.
    public CancellationToken CompletionToken => _wakeSignal.CompletionToken;

    public int Depth => Volatile.Read(ref _pipelineDepth);

    /// Returns an action to invoke outside any held lock to signal the execution loop to continue.
    public Pipeline.EnqueueResult Enqueue(T item)
    {
        if (_wakeSignal.IsCompleted)
            ThrowCompleted();

        // Plain write ordered before the enqueue by the SPSC queue's internal volatile write of _last.
        // The executor's wake-lock acquire at the top of the loop is the cross-thread fence
        // that makes this visible; no fence needed on this side. Keeps Enqueue hot.
        _notEmpty = true;
        _queue.Enqueue(new(item));
        Interlocked.Increment(ref _pipelineDepth);

        return new(_wakeSignal);
    }

    /// Waits for the pipeline to become idle (all items completed, depth reaches zero).
    /// Does not prevent new items from being enqueued, the caller is responsible for that.
    public ValueTask WaitForIdleAsync(CancellationToken cancellationToken = default)
    {
        AcquireDrainLock();
        try
        {
            if (_pipelineDepth is 0)
                return ValueTask.CompletedTask;

            _drainTcs ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            return new(_drainTcs.Task.WaitAsync(cancellationToken));
        }
        finally
        {
            ReleaseDrainLock();
        }
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

            while (!_wakeSignal.IsCompleted && _queue.TryDequeue(out var element))
            {
                // Commit the previous iteration's pending tail to the waiter queue.
                CommitTailWaiter();

                var item = element.Value;

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

                PipelineItemResult itemResult;
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

            // Commit any remaining tail before going idle.
            CommitTailWaiter();

            // No in-flight check before idle: the wake signal is the floor for every race
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

    /// Drains remaining items after the execution loop exits. Acquires the advancer to prevent
    /// racing with completion callbacks. Recovery is locked out via _recoveryLock if active.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        if (_waiterInRecovery)
        {
            // Recovery holds _advancing, take the lock and drain under it so recovery's
            // AdvanceAndDrainRecovery can't race with us on the queue.
            // Also complete the in-flight recovery item since it's not in any queue.
            lock (_waiterRecoveryLock)
            {
                CompleteWaiter(_waiterRecoveryItem, _completionException);
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _waiterRecoveryItem = default!;
                DoDrain();
            }
        }
        else
        {
            while (Interlocked.Exchange(ref _advancing, true))
                await Task.Yield();
            DoDrain();
        }

        void DoDrain()
        {
            var exception = _completionException;
            while (_waiters.TryDequeue(out var item))
                CompleteWaiter(item.Waiter, exception);
            while (_queue.TryDequeue(out var item))
                CompleteWaiter(item.Value, exception);
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
            var pipelineTask = _tailWaiterTask;
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            ClearExecutingItem(activated);

            // Observe the recovery's pipeline task to prevent unobserved task exceptions.
            // The item itself will be completed with an exception as we don't do recovery of recovery.
            if (!pipelineTask.IsCompleted)
            {
                pipelineTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    try { pipelineTask.GetAwaiter().GetResult(); }
                    catch { /* Recovery already failed, pipeline task result is discarded. */ }
                });
            }
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
            EnqueueWaiter(item, activated, new(pipelineTask));
            return;
        }

        // Swap the tail: replace _executingItem with the recovery item.
        // If null was returned, the waiter path already claimed it, activate recovery too.
        _executingItem = recoveryItem;
        var recoveryActivated = !Interlocked.Exchange(ref _hasExecutingItem, true);
        if (recoveryActivated)
            ActivateHeadItem(recoveryItem, preferAsync: false);

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
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
        }
    }

    /// Commits the pending tail (if any) to the waiter queue. Called by the executor at the
    /// start of each iteration and before going idle.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void CommitTailWaiter()
    {
        if (!_hasTailWaiter)
            return;

        var item = _tailWaiter;
        var task = _tailWaiterTask;
        _hasTailWaiter = false;
        _tailWaiterTask = default;

        // Check whether the waiter path already activated this item via _executingItem.
        var alreadyActivated = !Interlocked.Exchange(ref _hasExecutingItem, false);
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _executingItem = default!;

        if (task.IsCompleted)
        {
            if (task.IsCompletedSuccessfully)
            {
                task.GetAwaiter().GetResult();
                CompleteWaiter(item, null);
                return;
            }

            RecoverCommittedTailWaiter(item, task);
            return;
        }

        EnqueueWaiter(item, activated: alreadyActivated, task);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void RecoverCommittedTailWaiter(T item, ValueTask task)
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

        // Recovery item takes over, activate and execute.
        // This runs on the executor so we can't await, but the recovery may need async work.
        // Use the same continuation-based pattern as the advancer recovery.
        ActivateHeadItem(recoveryItem, preferAsync: false);

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            executeTask = _policy.ExecuteItemAsync(recoveryItem, _wakeSignal.CompletionToken);
        }
        catch (Exception recoveryEx)
        {
            CompleteWaiter(recoveryItem, recoveryEx);
            return;
        }

        if (executeTask.IsCompletedSuccessfully)
        {
            RecoverCommittedTailWaiterResult(recoveryItem, executeTask.Result);
            return;
        }

        // Execute is async, hook continuation.
        executeTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            PipelineItemResult result;
            try
            {
                result = executeTask.GetAwaiter().GetResult();
            }
            catch (Exception recoveryEx)
            {
                CompleteWaiter(recoveryItem, recoveryEx);
                return;
            }
            RecoverCommittedTailWaiterResult(recoveryItem, result);
        });
    }

    void RecoverCommittedTailWaiterResult(T recoveryItem, PipelineItemResult result)
    {
        // Observe trailing execution task.
        if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
        {
            if (result.TrailingExecutionTask.IsCompleted)
            {
                try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                catch (Exception ex)
                {
                    CompleteWaiter(recoveryItem, ex);
                    return;
                }
            }
            else
            {
                // Trailing task is async, hook continuation.
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch (Exception ex)
                    {
                        CompleteWaiter(recoveryItem, ex);
                        return;
                    }
                    RecoverCommittedTailWaiterPipelineTask(recoveryItem, pipelineTask);
                });
                return;
            }
        }

        RecoverCommittedTailWaiterPipelineTask(recoveryItem, result.PipelineTask);
    }

    void RecoverCommittedTailWaiterPipelineTask(T recoveryItem, ValueTask pipelineTask)
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
            CompleteWaiter(recoveryItem, pipelineException);
        }
        else
        {
            // Recovery's pipeline task is pending, enqueue it.
            EnqueueWaiter(recoveryItem, activated: true, pipelineTask);
        }
    }

    void CompleteWaiter(T item, Exception? exception)
    {
        var depth = Interlocked.Decrement(ref _pipelineDepth);
        _policy.CompleteItem(item, depth, exception);

        if (depth is 0 && _drainTcs is not null)
        {
            AcquireDrainLock();
            try
            {
                if (_drainTcs is not null)
                {
                    _drainTcs.SetResult();
                    _drainTcs = null;
                }
            }
            finally
            {
                ReleaseDrainLock();
            }
        }
    }

    /// Enqueues as a waiter and coordinates activation with the execution loop.
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
                    if (Interlocked.Exchange(ref _hasExecutingItem, false))
                    {
                        var executing = _executingItem;
                        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                            _executingItem = default!;
                        ActivateHeadItem(executing);
                    }
                }
                else
                {
                    // More waiters remain, activate the next one.
                    _waiters.TryPeek(out var nextItem);
                    ActivateHeadItem(nextItem.Waiter);
                }
            }

            // Release the advancer flag (full barrier) and double-check for late arrivals.
            // Plain read of _readyCount is safe here: sandwiched between the Exchange above
            // (release) and the Exchange below (re-acquire), both full barriers on all architectures.
            // This avoids a Volatile.Read which has higher cost on arm64 (ldapr).
            Interlocked.Exchange(ref _advancing, false);
        } while (!_wakeSignal.IsCompleted
                 && _waiterCompletedCount > 0
                 && _waiters.TryPeek(out var pending) && pending.WaiterTask.IsCompleted
                 && !Interlocked.Exchange(ref _advancing, true));
    }

    /// Returns true if the advancer should continue advancing, false if a recovery item
    /// with a pending pipeline task is now occupying this position.
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

        if (executeTask.IsCompletedSuccessfully)
        {
            return RecoverWaiterResult(recoveryItem, executeTask.Result);
        }

        // Execute is async, hook continuation. Advancer stops, continuation owns the flag.
        _waiterInRecovery = true;
        _waiterRecoveryItem = recoveryItem;
        executeTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            // If completed, DrainOnCompletion already completed the recovery item.
            if (_wakeSignal.IsCompleted)
            {
                _waiterInRecovery = false;
                return;
            }

            PipelineItemResult result;
            try
            {
                result = executeTask.GetAwaiter().GetResult();
            }
            catch (Exception recoveryEx)
            {
                CompleteWaiter(recoveryItem, recoveryEx);
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
                    CompleteWaiter(recoveryItem, ex);
                    return true;
                }
            }
            else
            {
                // Trailing task is async, hook continuation.
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch (Exception ex)
                    {
                        if (_wakeSignal.IsCompleted)
                        {
                            _waiterInRecovery = false;
                        }
                        else
                        {
                            CompleteWaiter(recoveryItem, ex);
                            AdvanceAndDrainRecovery();
                        }
                        return;
                    }

                    // If completed, DrainOnCompletion already completed the recovery item.
                    if (_wakeSignal.IsCompleted)
                    {
                        _waiterInRecovery = false;
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
            CompleteWaiter(recoveryItem, pipelineException);
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
                // DrainOnCompletion already completed the recovery item.
                _waiterInRecovery = false;
            }
            else
            {
                CompleteWaiter(recoveryItem, pipelineException);
                AdvanceAndDrainRecovery();
            }
        });
        return false;
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
                return;
            }
            _waiterInRecovery = false;
            AdvanceAndDrain();
        }
        finally
        {
            if (entered)
                _waiterRecoveryLock.Exit();
        }
    }

    /// Decrements waiter count, activates the next item, and resumes draining.
    /// Must only be called when the advancer flag is held.
    void AdvanceAndDrain()
    {
        var count = Interlocked.Decrement(ref _waiterQueueCount);
        Debug.Assert(count >= 0);

        if (count is 0)
        {
            if (Interlocked.Exchange(ref _hasExecutingItem, false))
                ActivateHeadItem(_executingItem);
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
            Interlocked.Exchange(ref _hasExecutingItem, false);
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _executingItem = default!;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void AcquireDrainLock()
    {
        if (Interlocked.Exchange(ref _drainLock, 1) != 0)
            AcquireDrainLockSlow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void AcquireDrainLockSlow()
    {
        var spinner = new SpinWait();
        while (Interlocked.Exchange(ref _drainLock, 1) != 0)
            spinner.SpinOnce();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ReleaseDrainLock() => Volatile.Write(ref _drainLock, 0);

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
                    if (_pipeline._hasTailWaiter)
                    {
                        Current = _pipeline._tailWaiter;
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
}
