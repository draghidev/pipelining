using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// <summary>
/// Single-producer, single-consumer pipelined request/response coordinator. The class is not
/// thread-safe. All public instance methods (<see cref="Enqueue"/>, <see cref="CompleteAsync"/>,
/// <see cref="GetEnumerator"/>, the <see cref="Depth"/> getter,
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
    T _waiterRecoveryItem = default!; // The item being recovered, for the bailout/completion paths to access.
    TaskCompletionSource? _advancerIdleTcs; // Set by DrainOnCompletionAsync while waiting for the advancer chain to fully quiesce; cleared after the wait completes.

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

    // Off the hot path (advancer/recovery handoff), so OS handoff on contention is preferable.
    readonly Lock _activationLock = new();


    /// <summary>
    /// Cancellation token fired by <see cref="CompleteAsync"/>. Can be linked or passed to IO operations
    /// to abort them on shutdown. The same token is delivered to <see cref="IPipelinePolicy{T}.ExecuteItemAsync"/>
    /// and <see cref="IPipelinePolicy{T}.OnExecutionIdleAsync"/>; policies should observe it to allow shutdown to proceed.
    /// </summary>
    public CancellationToken CompletionToken => _wakeSignal.CompletionToken;

    /// <summary>Current count of items in the pipeline (queued + in flight + waiting). Lock-free read,
    /// may be stale by the time the caller observes it. Use <see cref="WaitForEmptyAsync"/> to await depth zero.</summary>
    public int Depth => _depthState.Depth;

    /// <summary>
    /// Enqueues an item for processing. Returns an <see cref="Pipeline.EnqueueResult"/> whose
    /// <see cref="Pipeline.EnqueueResult.Execute"/> must be invoked (outside any held lock) to signal
    /// the execution loop. Discarding the result strands the item until the next enqueue/wake.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the pipeline has already been completed via <see cref="CompleteAsync"/>.</exception>
    public Pipeline.EnqueueResult Enqueue(T item)
    {
        if (_wakeSignal.IsCompleted)
            ThrowCompleted();

        // IncrementDepth BEFORE _queue.Enqueue keeps depth a strict upper bound. Otherwise the
        // executor could dequeue and decrement first, producing a negative remainingDepth in
        // CompleteItem and skipping OnDepthReachedZero's fire.
        _depthState.IncrementDepth();
        // Plain write, ordered by the SPSC queue's volatile write of _last. The executor's
        // wake-lock acquire is the cross-thread fence on the reading side.
        _notEmpty = true;
        _queue.Enqueue(new(item));

        return new(_wakeSignal);
    }

    /// <summary>
    /// Waits for the pipeline depth to reach zero (all items have received
    /// <see cref="IPipelinePolicy{T}.CompleteItem"/>). Does not prevent new items from being enqueued;
    /// the caller is responsible for that.
    /// </summary>
    /// <remarks>
    /// This is a pipeline-state query, not an executor-quiescence guarantee. The signal fires from
    /// inside <see cref="IPipelinePolicy{T}.CompleteItem"/>, so the executor may still be inside its
    /// inner drain loop and threadpool-resident advancer continuations may still be unwinding when
    /// this returns. Callers needing executor-side quiescence (e.g. asserting on per-batch policy
    /// callbacks) should observe <see cref="IPipelinePolicy{T}.OnExecutionIdleAsync"/> via the policy
    /// itself. For strict "fully quiet" GC-collectability semantics use <see cref="CompleteAsync"/>.
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
    /// not propagated through the returned task; it only flows to items. Exceptions thrown by
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
                    // Pending: store as tail for the next iteration.
                    _tailWaiter = item;
                    _hasTailWaiter = true;
                    _tailWaiterTask = itemResult.PipelineTask;
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

            // No pre-idle queue probe: the wake signal is the floor for every drain/park race,
            // so any extra check would be redundant work that the signal catches anyway.
            // Callers needing strict ordering with idle handoff must synchronize externally.
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

    /// Drains remaining items after the execution loop exits. Waits for the advancer chain to
    /// quiesce via a TCS that DrainReadyWaiters / BailoutRecoveryOnShutdown signal on release of
    /// _advancing. Recovery continuations always complete their own items (via CompleteRecoveryWaiter
    /// or BailoutRecoveryOnShutdown), so drain doesn't compete - it just waits for the chain to
    /// finish. After advancer-idle, drains _waiters and _queue.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        while (Volatile.Read(ref _advancing))
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _advancerIdleTcs, tcs);
            // Re-check post-publish: advancer may have released before seeing our TCS.
            if (!Volatile.Read(ref _advancing))
            {
                Volatile.Write(ref _advancerIdleTcs, null);
                break;
            }
            await tcs.Task.ConfigureAwait(false);
            Volatile.Write(ref _advancerIdleTcs, null);
            // Loop: spurious wake if advancer released then re-claimed before our check.
        }

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
            EnqueueWaiter(item, activated, new(pipelineTask));
            return;
        }

        // Swap the tail: replace _executingItem with the recovery item under the activation lock
        // so the advancer's count==0 claim can't observe partial state and double-activate.
        // On inline activation also clear the published slots so a later advancer claim cannot
        // re-read recovery and activate it twice.
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
        // Only clear _executingItem when our Exchange won. If the advancer won, it reads
        // _executingItem under its lock for the C-path activation, clearing here would NRE it.
        if (!alreadyActivated && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _executingItem = default!;

        // Fence-acquire so CompleteWaiter below cannot fire CompleteItem before the advancer's
        // in-progress ActivateHeadItem finishes. Same pattern as ClearExecutingItem's deferred branch.
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

    /// Enqueues as a waiter and coordinates activation with the execution loop. All call sites
    /// run on the executor's logical thread (direct or via the executor's await chain), preserving
    /// the SPSC single-producer contract on _waiters without a lock.
    void EnqueueWaiter(T item, bool activated, ValueTask waiterTask)
    {
        _waiters.Enqueue((item, waiterTask));
        var wasEmpty = Interlocked.Increment(ref _waiterQueueCount) is 1;

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
                    // Recovery item is occupying this pipeline position. The advancer flag stays
                    // held; the recovery continuation will complete the item, advance via
                    // AdvanceAndDrainRecovery, and release the flag (which signals any waiting drain).
                    return;
                }

                // Advance: decrement queue count and activate next.
                var count = Interlocked.Decrement(ref _waiterQueueCount);
                Debug.Assert(count >= 0);

                if (count is 0)
                {
                    // Last waiter drained. Hold the lock around claim+activate so the executor's
                    // ClearExecutingItem fence-acquire blocks until ActivateHeadItem finishes.
                    // Wrapping just the ActivateHeadItem call would leave a TOCTOU where the
                    // executor observes the Exchange and acquires its fence-lock uncontested.
                    lock (_activationLock)
                    {
                        if (Interlocked.Exchange(ref _hasExecutingItem, false))
                        {
                            // Re-check count under the lock. Executor iterations between our
                            // Decrement and here can enqueue waiters and re-publish _executingItem;
                            // the latest publish is no longer the head, so activating here would
                            // double-activate (now via C-path, again via D-path). Skip when count > 0.
                            if (Volatile.Read(ref _waiterQueueCount) is 0)
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

            // Release flag, then re-check for late arrivals. Plain read of _waiterCompletedCount
            // is safe: sandwiched between this Exchange (release) and TryReclaimAdvancerForWork's
            // Exchange (re-acquire), both full barriers. Avoids ldapr cost of Volatile.Read on arm64.
            Interlocked.Exchange(ref _advancing, false);
        } while (!_wakeSignal.IsCompleted && _waiterCompletedCount > 0 && TryReclaimAdvancerForWork());

        SignalAdvancerIdleIfWaiting();

        // Re-acquire the advancer flag and check for a ready waiter. Returns true with the flag held
        // if there's work, false (flag released) otherwise. TryPeek MUST run inside the _advancing
        // protection so the SPSC single-consumer contract on _waiters holds against a racing
        // OnWaiterTaskCompleted caller.
        bool TryReclaimAdvancerForWork()
        {
            if (Interlocked.Exchange(ref _advancing, true))
                return false;

            if (_waiters.TryPeek(out var pending) && pending.WaiterTask.IsCompleted)
                return true;

            Interlocked.Exchange(ref _advancing, false);
            return false;
        }
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

        // Publish the recovery item so the bailout path can complete it on shutdown.
        // The recovery continuation always completes this item itself (via CompleteRecoveryWaiter
        // on the normal path or BailoutRecoveryOnShutdown on shutdown), so no atomic claim race
        // with drain is needed; drain just waits for the advancer chain to quiesce.
        _waiterRecoveryItem = recoveryItem;

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
        Interlocked.Exchange(ref _advancing, false);
        SignalAdvancerIdleIfWaiting();
    }

    /// Called from recovery continuations to continue advancer activity after the recovery item
    /// completion. Delegates to AdvanceAndDrain whose loop exit signals the advancer-idle TCS.
    void AdvanceAndDrainRecovery() => AdvanceAndDrain();

    /// Signals any drain that's waiting for the advancer chain to quiesce. Null check is the
    /// common case (no one waiting), only allocates a Volatile.Read + null compare.
    /// Ordering: no hardware fence is required - cache coherence delivers the write eventually
    /// on any target. The only real requirement is to stop the JIT from hoisting or caching the
    /// load across calls (a relaxed atomic load would express this exactly). .NET lacks a
    /// relaxed-read primitive, so we use Volatile.Read and pay an unnecessary acquire fence
    /// on ARM as the cost of saying "actually emit the load."
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SignalAdvancerIdleIfWaiting()
        => Volatile.Read(ref _advancerIdleTcs)?.TrySetResult();

    /// Completes the recovery item on the normal (non-shutdown) recovery path. The continuation
    /// owns completion uncontested; drain (DrainOnCompletionAsync) only waits for the advancer
    /// chain to quiesce via AdvanceAndDrainRecovery's loop-exit signal.
    void CompleteRecoveryWaiter(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
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
                    // Re-check count under the lock (see DrainReadyWaiters count==0 branch).
                    // Skipping when count > 0 prevents double-activating a re-published _executingItem.
                    if (Volatile.Read(ref _waiterQueueCount) is 0)
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
                // pipelineTask.IsCompleted), so activation is optional - skip it.
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    _executingItem = default!;
            }
            else
            {
                // Advancer claimed first and is in ActivateHeadItem under the lock. Empty
                // acquire/release synchronizes-with that lock release, closing the
                // activation-after-completion race before the caller proceeds to CompleteWaiter.
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
    internal sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler executionScheduler)
        : IThreadPoolWorkItem
    {
        readonly CancellationTokenSource _cts = new();
        Action? _continuation;
        bool _pending;
        int _wakeLock;

        public PipelineScheduler Scheduler { get; } = executionScheduler;
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

        public void Signal() => SignalCore(runContinuationsAsynchronously);

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
