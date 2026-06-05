using System.Diagnostics.CodeAnalysis;

namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineBehavioralTests
{
    [TestMethod]
    [Ignore("YieldAfterFirstItem hook removed from IPipelinePolicy; restore via custom IPipelineSource later.")]
    public async Task YieldAfterFirstItem_CalledOncePerBatch_WhenMoreItemsQueued()
    {
        var counter = new Counter();
        var pipeline = Pipeline.Create<TestPipelineItem, YieldCountingPolicy>(new(counter));

        // Hold the executor inside the first ExecuteItemAsync so we can enqueue more items
        // before the first one completes and the queue gets re-checked.
        var first = new TestPipelineItem { ExecuteAsync = true };
        pipeline.Enqueue(first).Execute();
        await first.WaitForExecutedAsync();

        var second = new TestPipelineItem();
        var third = new TestPipelineItem();
        pipeline.Enqueue(second).Execute();
        pipeline.Enqueue(third).Execute();

        // Release the first item, executor finishes it, sees queue is non-empty,
        // and must call YieldAfterFirstItem before processing the next.
        first.CompleteExecuteTask();

        await first.WaitForCompleteAsync();
        await second.WaitForCompleteAsync();
        await third.WaitForCompleteAsync();

        Assert.AreEqual(1, counter.Value, "YieldAfterFirstItem should fire exactly once per batch.");
    }

    [TestMethod]
    [Ignore("YieldAfterFirstItem hook removed from IPipelinePolicy; restore via custom IPipelineSource later.")]
    public async Task YieldAfterFirstItem_NotCalled_WhenQueueEmptyAfterFirstItem()
    {
        var counter = new Counter();
        var pipeline = Pipeline.Create<TestPipelineItem, YieldCountingPolicy>(new(counter));

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        // Give the executor a chance to settle into the idle path.
        await pipeline.WaitForEmptyAsync();
        await Task.Delay(50);

        Assert.AreEqual(0, counter.Value, "YieldAfterFirstItem should not fire when the queue is empty after the first item.");
    }

    [TestMethod]
    [Ignore("YieldAfterFirstItem hook removed from IPipelinePolicy; restore via custom IPipelineSource later.")]
    public async Task YieldAfterFirstItem_CalledAgainOnNewBatch()
    {
        var counter = new Counter();
        // The re-arm contract is per genuine executor park, not per depth-zero. WaitForEmptyAsync
        // returns at depth-zero (fired from inside CompleteItem while executor may still be mid-batch),
        // which can race the second batch's enqueue into the same inner-while iteration. Sync on the
        // policy's OnExecutionIdleAsync hook instead - testing a policy contract via the policy hook.
        var idleSem = new SemaphoreSlim(0);
        var pipeline = Pipeline.Create<TestPipelineItem, YieldCountingPolicy>(new(counter, () => idleSem.Release()));

        // First batch.
        var firstA = new TestPipelineItem { ExecuteAsync = true };
        pipeline.Enqueue(firstA).Execute();
        await firstA.WaitForExecutedAsync();
        var secondA = new TestPipelineItem();
        pipeline.Enqueue(secondA).Execute();
        firstA.CompleteExecuteTask();
        await firstA.WaitForCompleteAsync();
        await secondA.WaitForCompleteAsync();

        await idleSem.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(1, counter.Value, "After first batch, yield count should be 1.");

        // Second batch. Executor is genuinely parked now, needsYieldAfterFirst is reset.
        var firstB = new TestPipelineItem { ExecuteAsync = true };
        pipeline.Enqueue(firstB).Execute();
        await firstB.WaitForExecutedAsync();
        var secondB = new TestPipelineItem();
        pipeline.Enqueue(secondB).Execute();
        firstB.CompleteExecuteTask();
        await firstB.WaitForCompleteAsync();
        await secondB.WaitForCompleteAsync();

        await idleSem.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(2, counter.Value, "Each idle→active batch transition should re-arm YieldAfterFirstItem.");
    }

    [TestMethod]
    public async Task ExecutionScheduler_CustomScheduler_ReceivesSubmissions()
    {
        var scheduler = new RecordingScheduler();
        var pipeline = Pipeline.Create<TestPipelineItem, CustomSchedulerPolicy>(new(scheduler));

        // Enqueue an item. The wake signal will dispatch through the custom scheduler
        // when running continuations asynchronously.
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        Assert.IsTrue(scheduler.SubmitCount > 0, "Custom ExecutionScheduler should receive at least one submission.");
    }

    [TestMethod]
    public async Task ActivateHeadItem_PreferAsyncParameter_BothValuesAcrossPaths()
    {
        var preferAsyncValues = new List<bool>();
        var pipeline = Pipeline.Create<TestPipelineItem, PreferAsyncCapturingPolicy>(new(preferAsyncValues));

        // Both items async so both become waiters. First activates inline (preferAsync=false).
        // Second is published as deferred-activation _executingItem and only activated by
        // AdvanceAndDrain when first's pipeline task completes (preferAsync=true).
        var first = new TestPipelineItem { CompleteAsync = true };
        var second = new TestPipelineItem { CompleteAsync = true };

        pipeline.Enqueue(first).Execute();
        pipeline.Enqueue(second).Execute();

        await first.WaitForExecutedAsync();
        await second.WaitForExecutedAsync();
        await first.WaitForActivationAsync();

        first.CompletePipelineTask();

        // Wait for the advancer to activate second.
        await second.WaitForActivationAsync();

        // Drain.
        second.CompletePipelineTask();
        await first.WaitForCompleteAsync();
        await second.WaitForCompleteAsync();

        Assert.IsTrue(preferAsyncValues.Contains(false), "First item's executor-side activation should pass preferAsync=false.");
        Assert.IsTrue(preferAsyncValues.Contains(true), "Second item's advancer-side activation should pass preferAsync=true.");
    }

    [TestMethod]
    [Ignore("OnExecutionIdleAsync hook removed from IPipelinePolicy; restore via custom IPipelineSource later.")]
    public async Task OnExecutionIdleAsync_Throws_PropagatesViaCompleteAsync()
    {
        var idleException = new InvalidOperationException("idle handler exploded");
        var pipeline = Pipeline.Create<TestPipelineItem, ThrowingIdlePolicy>(new(idleException));

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        // After the item completes, the executor calls OnExecutionIdleAsync which throws.
        // The executor self-completes the pipeline with that exception. CompleteAsync's task should fault.
        var thrown = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () => await pipeline.CompleteAsync());
        Assert.AreSame(idleException, thrown);
    }

    [TestMethod]
    public async Task CompletionToken_FiresInExecuteItemAsync_WhenCompleteAsyncCalled()
    {
        var tokenCapture = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TokenCapturingPolicy>(new(tokenCapture));

        // ExecuteAsync = true holds the executor inside ExecuteItemAsync until we release it.
        var item = new TestPipelineItem { ExecuteAsync = true };
        pipeline.Enqueue(item).Execute();

        // Get the token the policy received.
        var token = await tokenCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsFalse(token.IsCancellationRequested, "Token should not be cancelled before CompleteAsync.");

        // Begin completion. This calls _wakeSignal.Complete() which cancels the wake-signal CTS,
        // which is the same token threaded into ExecuteItemAsync.
        var completing = pipeline.CompleteAsync();

        // The token should fire promptly. Use a short polling loop to avoid timing races.
        var deadline = Environment.TickCount64 + 5000;
        while (!token.IsCancellationRequested && Environment.TickCount64 < deadline)
            await Task.Delay(10);

        Assert.IsTrue(token.IsCancellationRequested, "CompleteAsync should cancel the token threaded into ExecuteItemAsync.");

        // Drain.
        item.CompleteExecuteTask();
        await completing;
    }

    [TestMethod]
    public async Task TrailingExecutionTaskFailure_FailureContextCarriesPipelineTask()
    {
        var captured = new List<PipelineItemFailureContext>();
        var pipeline = Pipeline.Create<TestPipelineItem, FailureCapturingPolicy>(new(captured));

        // Item that will succeed the pipeline task but fail the trailing task.
        var trailingException = new InvalidOperationException("trailing failed");
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            HasTrailingTask = true,
            TrailingTaskException = trailingException,
        };

        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        // Complete the pipeline task successfully, then fail the trailing task.
        item.CompletePipelineTask();
        item.CompleteTrailingTask();

        await item.WaitForCompleteAsync();

        Assert.AreEqual(1, captured.Count, "Failure context should have been captured once.");
        Assert.AreEqual(PipelineItemFailureKind.TrailingExecutionTask, captured[0].Kind);
        Assert.IsNotNull(captured[0].PipelineTask, "PipelineTask should be preserved for TrailingExecutionTask failures so the policy can observe its outcome.");
        Assert.AreSame(trailingException, captured[0].Exception);
    }

    // -- Helpers --

    sealed class Counter
    {
        public int Value;
    }

    sealed class RecordingScheduler : PipelineScheduler
    {
        public int SubmitCount;

        public override void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true)
        {
            Interlocked.Increment(ref SubmitCount);
            System.Threading.ThreadPool.UnsafeQueueUserWorkItem(action, state, preferLocal);
        }
    }

    struct YieldCountingPolicy(Counter counter, Action? onIdle = null) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask YieldAfterFirstItem()
        {
            counter.Value++;
            return default;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken)
        {
            onIdle?.Invoke();
            return default;
        }
    }

    struct CustomSchedulerPolicy(PipelineScheduler scheduler) : IPipelinePolicy<TestPipelineItem>
    {
        public PipelineScheduler? ExecutionScheduler => scheduler;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    struct FailureCapturingPolicy(List<PipelineItemFailureContext> captured) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            captured.Add(context);
            recoveryItem = null;
            return false;
        }
    }

    struct PreferAsyncCapturingPolicy(List<bool> preferAsyncValues) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true)
        {
            preferAsyncValues.Add(preferAsync);
            item.Activate();
        }

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    struct ThrowingIdlePolicy(Exception toThrow) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => throw toThrow;
    }

    struct TokenCapturingPolicy(TaskCompletionSource<CancellationToken> capture) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            capture.TrySetResult(cancellationToken);
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    /// Reentrant Enqueue from ActivateHeadItem. SPSC enqueue runs on the executor thread
    /// (single producer at this moment per the public API contract), so it's safe. Follower is
    /// picked up on the inner loop's next iteration.
    [TestMethod]
    public async Task ReentrantEnqueueFromActivateHeadItem_FollowerCompletes()
    {
        var follower = new TestPipelineItem();
        var enqueued = false;
        QueuedPipeline<TestPipelineItem, ReentrantEnqueueOnActivatePolicy>? pipelineRef = null;

        var pipeline = Pipeline.Create<TestPipelineItem, ReentrantEnqueueOnActivatePolicy>(
            new ReentrantEnqueueOnActivatePolicy(_ =>
            {
                if (enqueued)
                    return;
                enqueued = true;
                pipelineRef!.Enqueue(follower).Execute();
            }));
        pipelineRef = pipeline;

        var first = new TestPipelineItem();
        pipeline.Enqueue(first).Execute();

        await first.WaitForCompleteAsync();
        await follower.WaitForCompleteAsync();

        Assert.IsNull(first.Exception);
        Assert.IsNull(follower.Exception);
        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Reentrant Enqueue from OnExecutionIdleAsync. The "refill a batch" pattern: idle hook
    /// enqueues the next batch, executor picks it up on the outer loop's next iteration without
    /// parking (queue is non-empty). Gated by a counter to avoid infinite refilling.
    [TestMethod]
    [Ignore("OnExecutionIdleAsync hook removed from IPipelinePolicy; restore via custom IPipelineSource later.")]
    public async Task ReentrantEnqueueFromOnExecutionIdleAsync_RefillsBatchUntilGated()
    {
        var refills = new TestPipelineItem[3];
        for (var i = 0; i < refills.Length; i++)
            refills[i] = new TestPipelineItem();

        var refillIdx = 0;
        QueuedPipeline<TestPipelineItem, ReentrantEnqueueOnIdlePolicy>? pipelineRef = null;

        var pipeline = Pipeline.Create<TestPipelineItem, ReentrantEnqueueOnIdlePolicy>(
            new ReentrantEnqueueOnIdlePolicy(() =>
            {
                if (refillIdx < refills.Length)
                    pipelineRef!.Enqueue(refills[refillIdx++]).Execute();
            }));
        pipelineRef = pipeline;

        var first = new TestPipelineItem();
        pipeline.Enqueue(first).Execute();

        await first.WaitForCompleteAsync();
        foreach (var item in refills)
            await item.WaitForCompleteAsync();

        Assert.AreEqual(refills.Length, refillIdx);
        Assert.AreEqual(0, pipeline.Depth);
        foreach (var item in refills)
            Assert.IsNull(item.Exception);
    }

    struct ReentrantEnqueueOnActivatePolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly Action<TestPipelineItem> _onActivate;
        public ReentrantEnqueueOnActivatePolicy(Action<TestPipelineItem> onActivate) => _onActivate = onActivate;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true)
        {
            _onActivate(item);
            item.Activate();
        }

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => default;
        public bool RunEnqueueAsynchronously => true;
        public ValueTask YieldAfterFirstItem() => default;
    }

    struct ReentrantEnqueueOnIdlePolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly Action _onIdle;
        public ReentrantEnqueueOnIdlePolicy(Action onIdle) => _onIdle = onIdle;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken)
        {
            _onIdle();
            return default;
        }

        public bool RunEnqueueAsynchronously => true;
        public ValueTask YieldAfterFirstItem() => default;
    }
}
