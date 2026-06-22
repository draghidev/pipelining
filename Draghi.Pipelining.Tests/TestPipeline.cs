namespace Draghi.Pipelining.Tests;

sealed class TestPipelineItem
{
    // Bumped from 2s to 10s for parallel-test resilience: under method-level parallelism a busy
    // machine can saturate the TP enough that legitimate progress slips past 2s. The async waits
    // below are true-async (TCS, no TP-thread block) so the timeout never traps a healthy run.
    static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    static string BuildTimeoutMessage(string description)
    {
        ThreadPool.GetAvailableThreads(out var availWorker, out var availIO);
        ThreadPool.GetMaxThreads(out var maxWorker, out var maxIO);
        ThreadPool.GetMinThreads(out var minWorker, out var minIO);
        return $"{description} ({WaitTimeout.TotalSeconds}s timeout) | ThreadPool: " +
            $"pending={ThreadPool.PendingWorkItemCount}, threads={ThreadPool.ThreadCount}, " +
            $"completed={ThreadPool.CompletedWorkItemCount}, " +
            $"avail={availWorker}/{availIO}, min={minWorker}/{minIO}, max={maxWorker}/{maxIO} | " +
            $"GC: gen0={GC.CollectionCount(0)}, gen1={GC.CollectionCount(1)}, gen2={GC.CollectionCount(2)}, " +
            $"heap={GC.GetTotalMemory(forceFullCollection: false)}";
    }

    public string? Name { get; init; }
    // TCS-backed signals (was ManualResetEventSlim). Async waits now await the Task with timeout
    // - no Task.Run + MRES.Wait + TP-blocked-worker hop. The sync waits use Task.Wait(timeout)
    // on the caller's thread; the stress runners' TryWait* return bool the same way MRES did.
    readonly TaskCompletionSource _completedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _activatedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _executedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _pipelineTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource<PipelineItemResult> _executeTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _trailingTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Exception? Exception { get; private set; }
    public bool IsCompleted { get; private set; }
    public bool IsExecuted => _executedTcs.Task.IsCompletedSuccessfully;

    // Non-asserting waits for stress runners that own their timeout/diagnosis/suspend handling.
    public bool TryWaitForExecuted(TimeSpan timeout) => _executedTcs.Task.Wait(timeout);
    public bool TryWaitForCompleted(TimeSpan timeout) => _completedTcs.Task.Wait(timeout);

    // Async TryWait variants: avoid the Task.Run + TP-blocked-worker hop that the sync forms
    // need at the await sites. Used by stress runners under heavy TP load.
    public async Task<bool> TryWaitForExecutedAsync(TimeSpan timeout)
    {
        try { await _executedTcs.Task.WaitAsync(timeout).ConfigureAwait(false); return true; }
        catch (TimeoutException) { return false; }
    }

    public async Task<bool> TryWaitForCompletedAsync(TimeSpan timeout)
    {
        try { await _completedTcs.Task.WaitAsync(timeout).ConfigureAwait(false); return true; }
        catch (TimeoutException) { return false; }
    }
    public Exception? ThrowOnExecute { get; init; }
    public Exception? PipelineTaskException { get; init; }
    public Exception? TrailingTaskException { get; init; }
    public bool CompleteAsync { get; init; }
    public bool ExecuteAsync { get; init; }
    public bool HasTrailingTask { get; init; }
    // Custom trailing-task backing source (takes precedence over the TCS-backed trailing task).
    // Lets tests observe the executor's trailing await registration - the only deterministic
    // suspension between the tail transition and the next iteration's CommitTailWaiter.
    public System.Threading.Tasks.Sources.IValueTaskSource? TrailingTaskSource { get; init; }
    public Action? OnComplete { get; init; }

    // The shutdown-settle registration (TestPipelinePolicy.ExecuteItemAsync) parks this item in
    // the enumerator CTS's callback list; released on completion so the CTS doesn't root every
    // completed item for the pipeline's lifetime (the Idle_* retention guards watch for that).
    CancellationTokenRegistration _shutdownRegistration;
    public void AttachShutdownRegistration(CancellationTokenRegistration registration)
        => _shutdownRegistration = registration;

    public void Complete(Exception? exception)
    {
        _shutdownRegistration.Dispose();
        Exception = exception;
        IsCompleted = true;
        OnComplete?.Invoke();
        _completedTcs.TrySetResult();
    }

    public void SignalExecuted() => _executedTcs.TrySetResult();

    public void WaitForExecuted()
    {
        if (!_executedTcs.Task.Wait(WaitTimeout))
            Assert.Fail(BuildTimeoutMessage("Timed out waiting for item execution."));
    }

    public async Task WaitForExecutedAsync()
    {
        try { await _executedTcs.Task.WaitAsync(WaitTimeout).ConfigureAwait(false); }
        catch (TimeoutException) { Assert.Fail(BuildTimeoutMessage("Timed out waiting for item execution.")); }
    }

    int _activationCount;
    public int ActivationCount => Volatile.Read(ref _activationCount);

    public void Activate()
    {
        Interlocked.Increment(ref _activationCount);
        _activatedTcs.TrySetResult();
    }
    public void WaitForActivation()
    {
        if (!_activatedTcs.Task.Wait(WaitTimeout))
            Assert.Fail(BuildTimeoutMessage("Timed out waiting for item activation."));
    }

    public async Task WaitForActivationAsync()
    {
        try { await _activatedTcs.Task.WaitAsync(WaitTimeout).ConfigureAwait(false); }
        catch (TimeoutException) { Assert.Fail(BuildTimeoutMessage("Timed out waiting for item activation.")); }
    }

    public void WaitForComplete()
    {
        if (!_completedTcs.Task.Wait(WaitTimeout))
            Assert.Fail(BuildTimeoutMessage("Timed out waiting for item completion."));
    }

    public async Task WaitForCompleteAsync()
    {
        try { await _completedTcs.Task.WaitAsync(WaitTimeout).ConfigureAwait(false); }
        catch (TimeoutException) { Assert.Fail(BuildTimeoutMessage("Timed out waiting for item completion.")); }
    }

    // TrySet: an explicit completion can race the shutdown-token settlement (see
    // TryCancelPipelineTask) - first writer wins, matching the escalation design where an
    // item's natural completion and its abort escalation are inherently racing settlers.
    public void CompletePipelineTask()
    {
        if (PipelineTaskException is not null)
            _pipelineTaskTcs.TrySetException(PipelineTaskException);
        else
            _pipelineTaskTcs.TrySetResult();
    }

    /// Shutdown-escalation settle (see TestPipelinePolicy.ExecuteItemAsync). Races benignly
    /// with an explicit CompletePipelineTask; TrySet semantics, first writer wins.
    public void TryCancelPipelineTask(CancellationToken token)
        => _pipelineTaskTcs.TrySetException(new OperationCanceledException(token));

    public void CompleteExecuteTask()
    {
        _executeTaskTcs.SetResult(new PipelineItemResult(GetPipelineTask()));
    }

    public void CompleteTrailingTask()
    {
        if (TrailingTaskException is not null)
            _trailingTaskTcs.SetException(TrailingTaskException);
        else
            _trailingTaskTcs.SetResult();
    }

    public ValueTask GetPipelineTask()
    {
        if (!CompleteAsync)
            return PipelineTaskException is { } ex ? new(Task.FromException(ex)) : default;
        return new(_pipelineTaskTcs.Task);
    }

    public ValueTask<PipelineItemResult> GetExecuteTask()
    {
        if (!ExecuteAsync)
        {
            var trailingTask = TrailingTaskSource is { } source
                ? new ValueTask(source, 0)
                : TrailingTaskException is not null || HasTrailingTask
                    ? new ValueTask(_trailingTaskTcs.Task)
                    : default;
            return new(new PipelineItemResult(trailingTask, GetPipelineTask()));
        }
        return new(_executeTaskTcs.Task);
    }

    public ValueTask GetTrailingTask()
    {
        if (TrailingTaskException is null && !HasTrailingTask)
            return default;
        return new(_trailingTaskTcs.Task);
    }
}

struct TestPipelinePolicy : IPipelinePolicy<TestPipelineItem>
{
    readonly bool _runEnqueueAsynchronously;
    readonly Func<PipelineItemFailureContext, TestPipelineItem?>? _recoveryFactory;

    // Executor-idle observation no longer lives on the policy (the OnExecutionIdleAsync hook was
    // removed in the source-driven refactor). Tests that need an "executor at rest" barrier wire it
    // through TestObservableQueueSource's onIdle lambda via ObservablePipeline.Create.
    public TestPipelinePolicy(bool runEnqueueAsynchronously = true, Func<PipelineItemFailureContext, TestPipelineItem?>? recoveryFactory = null)
    {
        _runEnqueueAsynchronously = runEnqueueAsynchronously;
        _recoveryFactory = recoveryFactory;
    }

    public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool waiterExecution, CancellationToken cancellationToken)
    {
        if (item.ThrowOnExecute is { } ex)
            throw ex;

        // The item-side escalation half of the shutdown design: the pipeline only drains
        // gracefully, and items are responsible for settling their own waiting sources when
        // the shutdown signal fires (the pipeline's signal to items is this token). Without
        // it a pending item legitimately blocks CompleteAsync forever.
        if (item.CompleteAsync && cancellationToken.CanBeCanceled)
            item.AttachShutdownRegistration(cancellationToken.UnsafeRegister(
                static (state, token) => ((TestPipelineItem)state!).TryCancelPipelineTask(token), item));

        var task = item.GetExecuteTask();
        item.SignalExecuted();
        return task;
    }

    public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

    public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
    {
        // Depth-accounting sentinel: a negative remainingDepth means a double-decrement
        // (a recovery double-completion door, or depth/count token disagreement). Asserting
        // here turns every test plus the stress runners into a detector for the whole
        // accounting family - the policy seam is the contract surface, so this is where
        // "never report a negative depth" is pinned.
        if (remainingDepth < 0)
            Assert.Fail($"CompleteItem observed negative remainingDepth {remainingDepth} for item {item.Name ?? "?"}.");
        item.Complete(exception);
    }

    public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
    {
        recoveryItem = _recoveryFactory?.Invoke(context);
        return recoveryItem is not null;
    }

    public bool RunEnqueueAsynchronously => _runEnqueueAsynchronously;
}
