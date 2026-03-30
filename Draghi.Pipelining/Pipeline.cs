using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

public sealed class Pipeline<T, TPolicy>
    where T : class
    where TPolicy : IPipelinePolicy<T>
{
    TPolicy _policy;

    // Execution.
    readonly PipelineExecutionMode _mode;
    readonly SingleProducerSingleConsumerQueue<Element> _queue;
    readonly Pipeline.WakeSignal _wakeSignal;
    readonly Task _executionTask;
    // Pipeline state.
    int _pipelineDepth; // Authoritative total of all queued and waiting items.
    bool _completed;
    Exception? _completionException;
    readonly CancellationTokenSource _cts;
    readonly CancellationToken _completionToken;
    TaskCompletionSource? _drainTcs;

    // Waiter tracking — items that could not be completed synchronously wait for their turn in the pipeline.
    readonly SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)> _waiters;
    int _waiterQueueCount; // Authoritative count.
    // Published by the execution loop for deferred activation via Interlocked.Exchange.
    // Also used as the activation handshake for the pending tail.
    T? _executingItem;
    // Pending tail — the most recent waiter, held outside the queue so the executor can swap it
    // for a recovery item if the trailing task fails. Committed to the waiter queue on the next
    // loop iteration. Executor-owned; fields rather than locals because RecoverTailWaiter is
    // async and async methods cannot capture locals by ref.
    T? _tailWaiter;
    ValueTask _tailWaiterTask;

    // Waiter completion — serialized via the advancer pattern to ensure head-first processing.
    int _waiterCompletedCount; // Number of completed waiter tasks not yet processed by the advancer.
    bool _advancing; // True if a thread is currently the advancer.

    // Activation.
    readonly Action _onWaiterTaskCompletedAction;

    PipelineScheduler ExecutionScheduler => _wakeSignal.Scheduler;

    internal Pipeline(TPolicy policy)
    {
        _policy = policy;
        _queue = new();
        _waiters = new();
        _mode = policy.ExecutionMode;
        _wakeSignal = new(_mode is PipelineExecutionMode.Async, policy.ExecutionScheduler ?? PipelineScheduler.ThreadPool);
        _onWaiterTaskCompletedAction = OnWaiterTaskCompleted;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(policy.CancellationToken);
        _completionToken = _cts.Token;
        _executionTask = ExecuteQueue();
    }

    readonly Lock _drainLock = new(); // Protects drain TCS and completion state.


    public int Depth => Volatile.Read(ref _pipelineDepth);

    /// Returns an action to invoke outside any held lock to signal the execution loop to continue.
    public Pipeline.EnqueueResult Enqueue(T item)
    {
        if (_completed)
            ThrowCompleted();

        _queue.Enqueue(new(item));
        Interlocked.Increment(ref _pipelineDepth);

        return new(_wakeSignal);
    }

    /// Waits for the pipeline to become idle (all items completed, depth reaches zero).
    /// Does not prevent new items from being enqueued — the caller is responsible for that.
    public ValueTask WaitForIdleAsync(CancellationToken cancellationToken = default)
    {
        lock (_drainLock)
        {
            if (_pipelineDepth is 0)
                return ValueTask.CompletedTask;

            _drainTcs ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            return new(_drainTcs.Task.WaitAsync(cancellationToken));
        }
    }

    public async ValueTask CompleteAsync(Exception? exception = null)
    {
        lock (_drainLock)
        {
            if (_completed)
                return;
            _completed = true;
            _completionException = exception;
            _cts.Cancel();
            _wakeSignal.Complete();
        }

        await _executionTask.ConfigureAwait(false);

        while (_waiters.TryDequeue(out var item))
            CompleteWaiter(item.Waiter, exception);
        while (_queue.TryDequeue(out var item))
            CompleteWaiter(item.Value, exception);
    }

    /// <summary>Returns an enumerator for items currently in the pipeline and queue, from oldest to newest.</summary>
    /// <remarks>Best-effort under concurrent mutation.</remarks>
    public Enumerator GetEnumerator() => new(this);

    async Task ExecuteQueue()
    {
        var needsYield = _mode is PipelineExecutionMode.SyncFirst;
        while (true)
        {
            if (_completionToken.IsCancellationRequested)
                return;

            _wakeSignal.AcquireWakeLock();
            if (_queue.IsEmpty)
            {
                needsYield = _mode is PipelineExecutionMode.SyncFirst;
                if (!await _wakeSignal.WaitUnsynchronized())
                    return;
            }
            else if (needsYield)
            {
                needsYield = false;
                _wakeSignal.ReleaseWakeLock();
                await LocalContinueOnAsync();
            }
            else
            {
                _wakeSignal.ReleaseWakeLock();
            }

            while (!_completionToken.IsCancellationRequested && _queue.TryDequeue(out var element))
            {
                // Commit the previous iteration's pending tail to the waiter queue.
                CommitTailWaiter();

                var item = element.Value;

                var activated = false;
                if (Volatile.Read(ref _waiterQueueCount) is 0)
                {
                    ActivateHeadItem(item, schedule: false);
                    activated = true;
                }
                else
                    Interlocked.Exchange(ref _executingItem, item);

                PipelineItemResult itemResult;
                try
                {
                    // Elide the await when the result is immediately available — avoids awaiter
                    // overhead for ValueTasks not backed by a Task or IValueTaskSource.
                    var executeTask = _policy.ExecuteItemAsync(item, _completionToken);
                    itemResult = executeTask.IsCompletedSuccessfully
                        ? executeTask.Result
                        : await executeTask.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ClearExecutingItem(activated);
                    await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), activated, _completionToken).ConfigureAwait(false);
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
                        await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex), activated, _completionToken).ConfigureAwait(false);
                        goto afterItem;
                    }

                    CompleteWaiter(item, null);
                }
                else
                {
                    // Pipeline task pending — store as pending tail for the next iteration.
                    _tailWaiter = item;
                    _tailWaiterTask = itemResult.PipelineTask;
                }

                // Await trailing execution task if not yet complete.
                if (!itemResult.TrailingExecutionTask.IsCompletedSuccessfully)
                {
                    try
                    {
                        await itemResult.TrailingExecutionTask.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (_tailWaiter is not null)
                        {
                            await RecoverTrailingFailure(item, activated, ex, _completionToken).ConfigureAwait(false);
                        }
                    }
                }

                afterItem:
                // Scheduling: yield off the caller's stack after the first iteration in SyncFirst mode.
                if (needsYield)
                {
                    if (_queue.IsEmpty)
                        break;

                    needsYield = false;
                    await LocalContinueOnAsync();
                }
            }

            // Commit any remaining tail before going idle.
            CommitTailWaiter();

            // Skip the idle callback if we're completing with an error — the callback may not be able to handle a broken state.
            if (_completionException is null)
            {
                // Elide the await when completed synchronously — still call GetResult to
                // complete the IValueTaskSource contract.
                var idleTask = _policy.OnExecutionIdleAsync(_completionToken);
                if (!idleTask.IsCompletedSuccessfully)
                    await idleTask.ConfigureAwait(false);
                else
                    idleTask.GetAwaiter().GetResult();
            }
        }

    }

    /// Handles execution-phase or pipeline-task failures, including recovery.
    /// Recovery items get the full async treatment since they're taking the place of the original item.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverItem(T item, PipelineItemFailureContext context, bool activated, CancellationToken cancellationToken)
    {
        var recoveryItem = _policy.TryRecoverItemFailure(context, item, cancellationToken);
        if (recoveryItem is null)
        {
            CompleteWaiter(item, context.Exception);
            return;
        }

        // Recovery item takes over — activate and execute with full async support.
        ActivateHeadItem(recoveryItem, schedule: false);
        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, _completionToken).ConfigureAwait(false);

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
            }

            if (!result.TrailingExecutionTask.IsCompleted)
                await result.TrailingExecutionTask.ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
            var pipelineTask = _tailWaiterTask;
            _tailWaiter = null;
            _tailWaiterTask = default;
            ClearExecutingItem(activated);

            // Observe the recovery's pipeline task to prevent unobserved task exceptions.
            // The item itself will be completed with an exception as we don't do recovery of recovery.
            if (!pipelineTask.IsCompleted)
            {
                pipelineTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    try { pipelineTask.GetAwaiter().GetResult(); }
                    catch { /* Recovery already failed — pipeline task result is discarded. */ }
                });
            }
            CompleteWaiter(recoveryItem, recoveryEx);
        }
    }

    /// Handles trailing execution task failures, including tail waiter recovery.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverTrailingFailure(T item, bool activated, Exception ex, CancellationToken cancellationToken)
    {
        var pipelineTask = _tailWaiterTask;
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.TrailingExecutionTask, ex, pipelineTask);
        var recoveryItem = _policy.TryRecoverItemFailure(context, item, cancellationToken);

        if (recoveryItem is null)
        {
            // Pipeline task may still be pending — enqueue as a waiter rather than completing prematurely.
            // Items can handle their own interdependency between the two tasks if needed.
            _tailWaiter = null;
            _tailWaiterTask = default;
            EnqueueWaiter(item, activated, pipelineTask);
            return;
        }

        // Swap the tail: exchange _executingItem with the recovery item.
        // If old value is null, the waiter path already activated the original — activate recovery too.
        var previous = Interlocked.Exchange(ref _executingItem, recoveryItem);
        var recoveryActivated = previous is null;
        if (recoveryActivated)
            ActivateHeadItem(recoveryItem, schedule: false);

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, _completionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
            {
                try
                {
                    await result.TrailingExecutionTask.ConfigureAwait(false);
                }
                catch
                {
                    // Trailing failure is secondary.
                }
            }

            if (result.PipelineTask.IsCompleted)
            {
                _tailWaiter = null;
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
            _tailWaiter = null;
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
        var item = _tailWaiter;
        if (item is null)
            return;

        var task = _tailWaiterTask;
        _tailWaiter = null;
        _tailWaiterTask = default;

        // Check whether the waiter path already activated this item via _executingItem.
        var alreadyActivated = Interlocked.Exchange(ref _executingItem, null) is null;

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

        // Pipeline task failed — attempt recovery.
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, exception);
        var recoveryItem = _policy.TryRecoverItemFailure(context, item, _completionToken);
        if (recoveryItem is null)
        {
            CompleteWaiter(item, exception);
            return;
        }

        // Recovery item takes over — activate and execute.
        // This runs on the executor so we can't await, but the recovery may need async work.
        // Use the same continuation-based pattern as the advancer recovery.
        ActivateHeadItem(recoveryItem, schedule: false);

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            executeTask = _policy.ExecuteItemAsync(recoveryItem, _completionToken);
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

        // Execute is async — hook continuation.
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
                // Trailing task is async — hook continuation.
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch { /* Trailing failure is secondary */ }
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
            // Recovery's pipeline task is pending — enqueue it.
            EnqueueWaiter(recoveryItem, activated: true, pipelineTask);
        }
    }

    void CompleteWaiter(T item, Exception? exception)
    {
        var depth = Interlocked.Decrement(ref _pipelineDepth);
        _policy.CompleteItem(item, depth, exception);

        if (depth is 0 && _drainTcs is not null)
        {
            lock (_drainLock)
            {
                if (_drainTcs is not null)
                {
                    _drainTcs.SetResult();
                    _drainTcs = null;
                }
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
            if (wasEmpty && Interlocked.Exchange(ref _executingItem, null) is not null)
            {
                ActivateHeadItem(item);
            }
            else
            {
                // Plain write: _executingItem is only read when _waiterQueueCount reaches zero,
                // which requires as many OnWaiterTaskReady calls (full barriers) as the current
                // count — all of which happen after this write.
                _executingItem = null;
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

        // Try to become the advancer — only one thread processes completions at a time.
        if (Interlocked.Exchange(ref _advancing, true))
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
                    // Last waiter drained — activate the pending tail or executing item, if any.
                    if (Interlocked.Exchange(ref _executingItem, null) is { } executing)
                        ActivateHeadItem(executing);
                }
                else
                {
                    // More waiters remain — activate the next one.
                    _waiters.TryPeek(out var nextItem);
                    ActivateHeadItem(nextItem.Waiter);
                }
            }

            // Release the advancer flag (full barrier) and double-check for late arrivals.
            // Plain read of _readyCount is safe here: sandwiched between the Exchange above
            // (release) and the Exchange below (re-acquire), both full barriers on all architectures.
            // This avoids a Volatile.Read which has higher cost on arm64 (ldapr).
            Interlocked.Exchange(ref _advancing, false);
        } while (_waiterCompletedCount > 0
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
            ex = null!; // Unreachable — task was faulted.
        }
        catch (Exception e)
        {
            ex = e;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTaskWaiter, ex);
        var recoveryItem = _policy.TryRecoverItemFailure(context, failedItem, _completionToken);

        if (recoveryItem is null)
        {
            CompleteWaiter(failedItem, ex);
            return true;
        }

        // Recovery item takes over — activate it at the current pipeline position.
        ActivateHeadItem(recoveryItem);

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            executeTask = _policy.ExecuteItemAsync(recoveryItem, _completionToken);
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

        // Execute is async — hook continuation. Advancer stops; continuation owns the flag.
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
                AdvanceAndDrain();
                return;
            }

            if (!RecoverWaiterResult(recoveryItem, result))
                return; // pipeline task pending, continuation will resume

            AdvanceAndDrain();
        });
        return false;
    }

    /// Handles a completed recovery execution result. Returns true if the advancer should continue,
    /// false if the recovery's pipeline task is pending (occupying this position).
    bool RecoverWaiterResult(T recoveryItem, PipelineItemResult result)
    {
        // Await trailing execution — must observe the result.
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
                // Trailing task is async — hook continuation.
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch { /* Trailing failure is secondary */ }

                    if (!RecoverWaiterPipelineTask(recoveryItem, pipelineTask))
                        return; // pipeline task pending

                    AdvanceAndDrain();
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

        // Pipeline task pending — hook continuation. Advancer stays held.
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
            CompleteWaiter(recoveryItem, pipelineException);
            AdvanceAndDrain();
        });
        return false;
    }

    /// Decrements waiter count, activates the next item, and resumes draining.
    /// Must only be called when the advancer flag is held.
    void AdvanceAndDrain()
    {
        var count = Interlocked.Decrement(ref _waiterQueueCount);
        Debug.Assert(count >= 0);

        if (count is 0)
        {
            if (Interlocked.Exchange(ref _executingItem, null) is { } executing)
                ActivateHeadItem(executing);
        }
        else
        {
            _waiters.TryPeek(out var nextItem);
            ActivateHeadItem(nextItem.Waiter);
        }

        // Continue draining ready items — we already hold the advancer flag.
        DrainReadyWaiters();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivateHeadItem(T item, bool schedule = true) => _policy.ActivateHeadItem(item, schedule);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ClearExecutingItem(bool wasActivated)
    {
        if (!wasActivated)
            Interlocked.Exchange(ref _executingItem, null);
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    static void ThrowCompleted() => throw new InvalidOperationException("The pipeline has been completed.");

    ContinueOnAwaitable LocalContinueOnAsync() => ExecutionScheduler.ContinueOnAsync(forceYielding: true, preferLocal: true);

    public struct Enumerator
    {
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>.Enumerator _waitersEnumerator;
        SingleProducerSingleConsumerQueue<Element>.Enumerator _queueEnumerator;
        bool _enumeratingQueue;

        internal Enumerator(Pipeline<T, TPolicy> pipeline)
        {
            _waitersEnumerator = new(pipeline._waiters);
            _queueEnumerator = new(pipeline._queue);
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            if (!_enumeratingQueue)
            {
                while (_waitersEnumerator.MoveNext())
                {
                    // Attempt to filter out nonsensical observations due to concurrent mutations of segments.
                    // This can cause us to observe default element values during dequeue slot clearing.
                    if (_waitersEnumerator.Current.Waiter is { } item)
                    {
                        Current = item;
                        return true;
                    }
                }
                _enumeratingQueue = true;
            }

            while (_queueEnumerator.MoveNext())
            {
                // Attempt to filter out nonsensical observations due to concurrent mutations of segments.
                // This can cause us to observe default element values during dequeue slot clearing.
                if (_queueEnumerator.Current.Value is { } item)
                {
                    Current = item;
                    return true;
                }
            }

            return false;
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
        where T : class
        where TPolicy : IPipelinePolicy<T>
        => new(policy);

    /// <summary>
    /// Represents a deferred enqueue completion returned by <see cref="Pipeline{T,TPolicy}.Enqueue"/>.
    /// The item is already in the queue; calling <see cref="Execute"/> may signal the execution loop to process it.
    /// This two-step design exists because the signal may synchronously run the execution loop on the caller's
    /// thread (in <see cref="PipelineExecutionMode.Sync"/> or <see cref="PipelineExecutionMode.SyncFirst"/> mode),
    /// so it must be invoked outside any held lock.
    /// </summary>
    public readonly struct EnqueueResult
    {
        readonly WakeSignal? _signal;
        internal EnqueueResult(WakeSignal? signal) => _signal = signal;

        /// <summary>Finalizes the enqueue, which may signal and run the execution loop on the calling thread.</summary>
        public void Execute() => _signal?.Signal();
    }

    /// Single-consumer single-producer wake signal for suspending and waking execution.
    /// Doubles as its own awaitable and awaiter (GetAwaiter returns this).
    /// The wake lock is held from WaitUnsynchronized through OnCompleted, which stores
    /// the continuation and releases the lock. Signal re-acquires the lock to claim the continuation.
    /// A spinlock is used instead of Lock because the critical section is a few field reads/writes
    /// with at most two threads contending (execution loop and Signal caller).
    /// No code under the lock re-enters or is user code that may, so reentrancy support is not needed either.
    internal sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler executionScheduler) : ICriticalNotifyCompletion, IThreadPoolWorkItem
    {
        readonly bool _runContinuationsAsynchronously = runContinuationsAsynchronously;
        public PipelineScheduler Scheduler { get; } = executionScheduler;
        Action? _continuation;
        bool _pending;
        bool _completed;
        int _wakeLock;

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

        /// Returns this signal as an awaitable. Must be called under the queue lock —
        /// the lock is released by OnCompleted.
        public WakeSignal WaitUnsynchronized()
        {
            Debug.Assert(!_pending, "Concurrent wait calls.");
            _pending = true;
            return this;
        }

        public WakeSignal GetAwaiter() => this;

        public bool IsCompleted => !_pending;

        public bool GetResult() => !_completed;

        public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

        public void UnsafeOnCompleted(Action continuation)
        {
            if (!ReferenceEquals(_continuation, continuation))
                _continuation = continuation;
            ReleaseWakeLock();
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
            {
                _continuation!();
            }
        }

        void IThreadPoolWorkItem.Execute()
        {
            _continuation!();
        }

        /// Marks the signal as completed, cancels any pending wait.
        public void Complete()
        {
            _completed = true;
            SignalCore(runContinuationsAsynchronously: true);
        }
    }
}
