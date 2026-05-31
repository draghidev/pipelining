namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineLifecycleTests
{
    [TestMethod]
    public async Task CompleteAsync_CalledTwice_SecondCallReturnsSameTask()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        var first = pipeline.CompleteAsync();
        var second = pipeline.CompleteAsync();

        // First-writer guard: both calls observe the same underlying execution task.
        await first;
        await second;
        // Should not throw, both should complete cleanly.
        Assert.IsTrue(pipeline.CompletionToken.IsCancellationRequested);
    }

    /// CompleteAsync() signals the wake signal's CompletionToken. Policies that observe it from
    /// ExecuteItemAsync (e.g., to abort I/O on shutdown) should see cancellation when shutdown fires.
    [TestMethod]
    public async Task CompleteAsync_SignalsCompletionTokenObservedByExecuteItem()
    {
        var executeObservedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TokenObservingPolicy>(new(executeTokenSink: executeObservedCancellation));

        // Enqueue an item. Policy's ExecuteItemAsync awaits cancellation on the token.
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();

        // Give the executor a moment to enter ExecuteItemAsync.
        await Task.Delay(50);

        // Shutdown - policy's await should observe the cancellation.
        var completeTask = pipeline.CompleteAsync().AsTask();

        await executeObservedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// CompletionToken is also observable from OnExecutionIdleAsync. A policy that uses idle
    /// time for housekeeping (or just parks on a cancellable wait) should unblock on shutdown.
    [TestMethod]
    public async Task CompleteAsync_SignalsCompletionTokenObservedByIdle()
    {
        var idleObservedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TokenObservingPolicy>(new(idleTokenSink: idleObservedCancellation));

        // Enqueue an item to push the executor past WaitUnsynchronized into OnExecutionIdleAsync.
        // without this, the executor parks at the wake signal instead.
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        // CompleteAsync's task faults because the policy's OnExecutionIdleAsync rethrows the
        // OperationCanceledException - this is the expected propagation per Pipeline's docstring
        // ("Exceptions thrown by OnExecutionIdleAsync during shutdown DO fault the returned task").
        var completeTask = pipeline.CompleteAsync().AsTask();

        await idleObservedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<OperationCanceledException>(() => completeTask.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [TestMethod]
    public async Task CompleteAsync_WithException_PropagatesToDrainedItems()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Enqueue items that won't complete on their own.
        var items = new TestPipelineItem[3];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }
        for (var i = 0; i < items.Length; i++)
            await items[i].WaitForExecutedAsync();

        var ex = new InvalidOperationException("drain");
        await pipeline.CompleteAsync(ex);

        // All items should be completed with the supplied exception.
        for (var i = 0; i < items.Length; i++)
        {
            Assert.IsTrue(items[i].IsCompleted, $"Item {i} should be completed.");
            Assert.AreSame(ex, items[i].Exception, $"Item {i} should carry the drain exception.");
        }
    }

    /// Exception propagation reaches items in BOTH buckets: items already in _waiters with pending
    /// pipeline tasks, and items still in _queue that the executor hadn't dequeued yet.
    /// DrainOnCompletionAsync iterates both queues and calls CompleteWaiter(_, exception) for each.
    [TestMethod]
    public async Task CompleteAsync_WithException_PropagatesToItemsInQueueAndWaiters()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true, idleTcs: idleTcs));

        // Phase 1: enqueue async items + wait for executor to park. These end up in _waiters.
        var waitersItems = new TestPipelineItem[3];
        for (var i = 0; i < waitersItems.Length; i++)
        {
            waitersItems[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(waitersItems[i]).Execute();
        }
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Phase 2: enqueue more items WITHOUT calling Execute. They sit in _queue with executor parked.
        var queueItems = new TestPipelineItem[3];
        for (var i = 0; i < queueItems.Length; i++)
        {
            queueItems[i] = new TestPipelineItem();
            _ = pipeline.Enqueue(queueItems[i]);  // discard the EnqueueResult, don't wake executor
        }

        // CompleteAsync wakes the executor, which exits and DrainOnCompletionAsync drains both.
        var ex = new InvalidOperationException("drain");
        await pipeline.CompleteAsync(ex);

        foreach (var item in waitersItems)
        {
            Assert.IsTrue(item.IsCompleted, "Waiter-bucket item should be completed.");
            Assert.AreSame(ex, item.Exception, "Waiter-bucket item should carry the drain exception.");
        }
        foreach (var item in queueItems)
        {
            Assert.IsTrue(item.IsCompleted, "Queue-bucket item should be completed.");
            Assert.AreSame(ex, item.Exception, "Queue-bucket item should carry the drain exception.");
        }
    }

    [TestMethod]
    public void WaitForEmptyAsync_AlreadyIdle_CompletesImmediately()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Depth is 0 from construction. WaitForEmptyAsync should return a completed task synchronously.
        var task = pipeline.WaitForEmptyAsync();
        Assert.IsTrue(task.IsCompleted);
        Assert.IsTrue(task.IsCompletedSuccessfully);
    }

    [TestMethod]
    public async Task WaitForEmptyAsync_Cancelled_ThrowsTaskCanceled()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Hold an item in-flight so WaitForEmptyAsync has to wait.
        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        using var cts = new CancellationTokenSource();
        var idleTask = pipeline.WaitForEmptyAsync(cts.Token).AsTask();

        Assert.IsFalse(idleTask.IsCompleted);

        cts.Cancel();

        await Assert.ThrowsExactlyAsync<TaskCanceledException>(async () => await idleTask);

        // Drain.
        item.CompletePipelineTask();
        await item.WaitForCompleteAsync();
    }

    [TestMethod]
    public async Task OnExecutionIdleAsync_FiresWhenExecutorBecomesIdle()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true, idleTcs: idleTcs));

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        // Once the executor has nothing left to dequeue, OnExecutionIdleAsync should fire.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(idleTcs.Task.IsCompletedSuccessfully);
    }

    [TestMethod]
    public async Task CompleteItem_RemainingDepth_DecreasesPerCompletion()
    {
        var depths = new List<int>();
        var pipeline = Pipeline.Create<TestPipelineItem, DepthCapturingPolicy>(new(depths));

        const int count = 5;
        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem();
            pipeline.Enqueue(items[i]).Execute();
        }

        for (var i = 0; i < count; i++)
            await items[i].WaitForCompleteAsync();

        // Each completion reports the depth *after* the decrement: count-1, count-2, ..., 0.
        Assert.AreEqual(count, depths.Count);
        var expected = new int[count];
        for (var i = 0; i < count; i++)
            expected[i] = count - 1 - i;
        CollectionAssert.AreEqual(expected, depths);
    }

    /// Enqueue after CompleteAsync must throw - the wake signal is completed and the pipeline
    /// is shutting down, accepting new items would lose them in the drain.
    [TestMethod]
    public async Task EnqueueAfterCompleteAsync_Throws()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        await pipeline.CompleteAsync();

        var item = new TestPipelineItem();
        Assert.ThrowsExactly<InvalidOperationException>(() => pipeline.Enqueue(item));
    }

    /// First-writer-wins on CompleteAsync's exception: the first call's exception is what drained
    /// items see, subsequent CompleteAsync calls return the same execution task with no exception
    /// override.
    [TestMethod]
    public async Task CompleteAsync_TwiceWithDifferentExceptions_FirstWins()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var first = new InvalidOperationException("first");
        var second = new InvalidOperationException("second");

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        var firstTask = pipeline.CompleteAsync(first).AsTask();
        var secondTask = pipeline.CompleteAsync(second).AsTask();

        await firstTask.WaitAsync(TimeSpan.FromSeconds(5));
        await secondTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(item.IsCompleted);
        Assert.AreSame(first, item.Exception, "First caller's exception should propagate to drained items.");
    }

    /// CompleteAsync fires while the executor is awaiting an item's trailing execution task.
    /// Drain must let the trailing task complete (or observe its exception) before fully draining.
    [TestMethod]
    public async Task CompleteAsync_WithItemsMidTrailingTask_DrainsCleanly()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true, HasTrailingTask = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        // Executor is now awaiting the trailing task.

        var completeTask = pipeline.CompleteAsync().AsTask();

        // Let trailing + pipeline tasks complete, drain proceeds.
        item.CompleteTrailingTask();
        item.CompletePipelineTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
        await item.WaitForCompleteAsync();
    }

    /// CompleteAsync fires with a pending tail waiter (item stored as _tailWaiter, pipeline task
    /// still pending). CommitTailWaiter at line 238 must commit the tail before DrainOnCompletionAsync,
    /// and the drain exception must reach the committed tail.
    [TestMethod]
    public async Task CompleteAsync_WithPendingTailWaiter_DrainsTail()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        // Item is now _tailWaiter with a pending pipeline task.

        var ex = new InvalidOperationException("drain tail");
        await pipeline.CompleteAsync(ex).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(item.IsCompleted, "Pending tail waiter should be completed by drain.");
        Assert.AreSame(ex, item.Exception);
    }

    /// OnExecutionIdleAsync throwing must propagate: executor catches, calls CompleteAsync(ex),
    /// drains remaining items, then re-throws so the execution task faults with the idle exception.
    [TestMethod]
    public async Task OnExecutionIdleAsync_Throws_FaultsCompleteAsync()
    {
        var idleEx = new InvalidOperationException("idle fault");
        var pipeline = Pipeline.Create<TestPipelineItem, ThrowingIdlePolicy>(new(idleEx));

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();
        // Executor finishes item, then calls OnExecutionIdleAsync which throws.

        var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreSame(idleEx, ex);
    }

    /// WaitForEmptyAsync called after CompleteAsync has fully drained should return a completed
    /// task immediately (depth is 0, drain TCS is null because no one was waiting during drain).
    [TestMethod]
    public async Task WaitForEmptyAsync_AfterCompleteAsync_CompletesImmediately()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        await pipeline.CompleteAsync();

        var task = pipeline.WaitForEmptyAsync();
        Assert.IsTrue(task.IsCompleted, "WaitForEmptyAsync after CompleteAsync should be immediately completed.");
    }

    struct ThrowingIdlePolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly Exception _idleException;

        public ThrowingIdlePolicy(Exception idleException) => _idleException = idleException;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
            => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken)
            => ValueTask.FromException(_idleException);

        public ValueTask YieldAfterFirstItem() => default;
    }

    struct DepthCapturingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly List<int> _depths;

        public DepthCapturingPolicy(List<int> depths) => _depths = depths;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
        {
            _depths.Add(remainingDepth);
            item.Complete(exception);
        }

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    /// CompleteAsync invoked from inside ExecuteItemAsync (reentrant shutdown). The executor
    /// is mid-await on the policy's task when CompleteAsync sets _completing and signals the
    /// wake. The item completes normally, the inner loop exits on _wakeSignal.IsCompleted,
    /// and the execution task drains. Discard the returned ValueTask: awaiting it from inside
    /// the executor would self-deadlock.
    [TestMethod]
    public async Task CompleteAsync_FromInsideExecuteItemAsync_DrainsCleanly()
    {
        Pipeline<TestPipelineItem, ReentrantCompletePolicy>? pipelineRef = null;
        var pipeline = Pipeline.Create<TestPipelineItem, ReentrantCompletePolicy>(
            new ReentrantCompletePolicy(() => _ = pipelineRef!.CompleteAsync()));
        pipelineRef = pipeline;

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();

        await item.WaitForCompleteAsync();
        // Outside-thread CompleteAsync returns the same execution task. Awaiting confirms drain.
        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsNull(item.Exception);
        Assert.IsTrue(pipeline.CompletionToken.IsCancellationRequested);
    }

    struct ReentrantCompletePolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly Action _onExecute;
        public ReentrantCompletePolicy(Action onExecute) => _onExecute = onExecute;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            _onExecute();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => default;
        public bool RunEnqueueAsynchronously => true;
        public ValueTask YieldAfterFirstItem() => default;
    }

    /// Policy that awaits CompletionToken cancellation from ExecuteItemAsync and/or
    /// OnExecutionIdleAsync. Signals a TCS when the cancellation is observed.
    struct TokenObservingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly TaskCompletionSource? _executeTokenSink;
        readonly TaskCompletionSource? _idleTokenSink;

        public TokenObservingPolicy(TaskCompletionSource? executeTokenSink = null, TaskCompletionSource? idleTokenSink = null)
        {
            _executeTokenSink = executeTokenSink;
            _idleTokenSink = idleTokenSink;
        }

        public async ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            if (_executeTokenSink is not null)
            {
                try { await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { _executeTokenSink.TrySetResult(); throw; }
            }
            return new PipelineItemResult(default);
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception) => item.Complete(exception);

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public async ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken)
        {
            if (_idleTokenSink is null) return;
            try { await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) { _idleTokenSink.TrySetResult(); throw; }
        }

        public bool RunEnqueueAsynchronously => true;
        public ValueTask YieldAfterFirstItem() => default;
    }
}
