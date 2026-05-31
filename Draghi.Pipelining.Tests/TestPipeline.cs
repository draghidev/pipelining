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

    public void CompletePipelineTask()
    {
        if (PipelineTaskException is not null)
            _pipelineTaskTcs.SetException(PipelineTaskException);
        else
            _pipelineTaskTcs.SetResult();
    }

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
    readonly TaskCompletionSource? _idleTcs;

    public TestPipelinePolicy(bool runEnqueueAsynchronously = true, Func<PipelineItemFailureContext, TestPipelineItem?>? recoveryFactory = null, TaskCompletionSource? idleTcs = null)
    {
        _runEnqueueAsynchronously = runEnqueueAsynchronously;
        _recoveryFactory = recoveryFactory;
        _idleTcs = idleTcs;
    }

    public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
    {
        if (item.ThrowOnExecute is { } ex)
            throw ex;

        var task = item.GetExecuteTask();
        item.SignalExecuted();
        return task;
    }

    public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

    public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
    {
        item.Complete(exception);
    }

    public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
    {
        recoveryItem = _recoveryFactory?.Invoke(context);
        return recoveryItem is not null;
    }

    public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken)
    {
        _idleTcs?.TrySetResult();
        return ValueTask.CompletedTask;
    }

    public bool RunEnqueueAsynchronously => _runEnqueueAsynchronously;

    public ValueTask YieldAfterFirstItem() => default;
}
