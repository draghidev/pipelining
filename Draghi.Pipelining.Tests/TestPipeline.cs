namespace Draghi.Pipelining.Tests;

sealed class TestPipelineItem
{
    static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(2);

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
    readonly ManualResetEventSlim _completed = new(false);
    readonly ManualResetEventSlim _activated = new(false);
    readonly ManualResetEventSlim _executed = new(false);
    readonly TaskCompletionSource _pipelineTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource<PipelineItemResult> _executeTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _trailingTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Exception? Exception { get; private set; }
    public bool IsCompleted { get; private set; }
    public bool IsExecuted => _executed.IsSet;

    // Non-asserting waits for stress runners that own their timeout/diagnosis/suspend handling.
    public bool TryWaitForExecuted(TimeSpan timeout) => _executed.Wait(timeout);
    public bool TryWaitForCompleted(TimeSpan timeout) => _completed.Wait(timeout);
    public Exception? ThrowOnExecute { get; init; }
    public Exception? PipelineTaskException { get; init; }
    public Exception? TrailingTaskException { get; init; }
    public bool CompleteAsync { get; init; }
    public bool ExecuteAsync { get; init; }
    public bool HasTrailingTask { get; init; }
    public Action? OnComplete { get; init; }

    public void Complete(Exception? exception)
    {
        Exception = exception;
        IsCompleted = true;
        OnComplete?.Invoke();
        _completed.Set();
    }

    public void SignalExecuted() => _executed.Set();

    public void WaitForExecuted()
    {
        if (!_executed.Wait(WaitTimeout))
        {
            Assert.Fail(BuildTimeoutMessage("Timed out waiting for item execution."));
        }
    }

    public Task WaitForExecutedAsync()
    {
        if (_executed.IsSet)
            return Task.CompletedTask;
        return Task.Run(() =>
        {
            if (!_executed.Wait(WaitTimeout))
            {
                Assert.Fail(BuildTimeoutMessage("Timed out waiting for item execution."));
            }
        });
    }

    int _activationCount;
    public int ActivationCount => Volatile.Read(ref _activationCount);

    public void Activate()
    {
        Interlocked.Increment(ref _activationCount);
        _activated.Set();
    }
    public void WaitForActivation()
    {
        if (!_activated.Wait(WaitTimeout))
        {
            Assert.Fail(BuildTimeoutMessage("Timed out waiting for item activation."));
        }
    }

    public Task WaitForActivationAsync()
    {
        if (_activated.IsSet)
            return Task.CompletedTask;
        return Task.Run(() =>
        {
            if (!_activated.Wait(WaitTimeout))
            {
                Assert.Fail(BuildTimeoutMessage("Timed out waiting for item activation."));
            }
        });
    }

    public void WaitForComplete()
    {
        if (!_completed.Wait(WaitTimeout))
        {
            Assert.Fail(BuildTimeoutMessage("Timed out waiting for item completion."));
        }
    }

    public Task WaitForCompleteAsync()
    {
        if (_completed.IsSet)
            return Task.CompletedTask;
        return Task.Run(() =>
        {
            if (!_completed.Wait(WaitTimeout))
            {
                Assert.Fail(BuildTimeoutMessage("Timed out waiting for item completion."));
            }
        });
    }

    // TrySet: an explicit completion can race the shutdown-token settlement (see
    // TryCancelPipelineTask) - first writer wins, matching the escalation design where a
    // flow's natural completion and its abort escalation are inherently racing settlers.
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
            var trailingTask = TrailingTaskException is not null || HasTrailingTask
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

    public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
    {
        if (item.ThrowOnExecute is { } ex)
            throw ex;

        // The flow-side escalation half of the shutdown design: the pipeline only drains
        // gracefully, and items are responsible for settling their own waiting sources when
        // the shutdown signal fires (the real protocol does this via the heartbeat abort
        // walk; the pipeline's signal to items is this token). Without it a pending item
        // legitimately blocks CompleteAsync forever.
        if (item.CompleteAsync && cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(
                static (state, token) => ((TestPipelineItem)state!).TryCancelPipelineTask(token), item);

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
