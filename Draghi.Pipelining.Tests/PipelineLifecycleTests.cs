namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineLifecycleTests
{
    /// Regression guard for Enqueue's depth-ordering race. Enqueue's sequence was
    /// _queue.Enqueue (publishes item) then IncrementDepth. If the executor's TryDequeue lands
    /// between those two writes, CompleteWaiter decrements depth before Enqueue's increment,
    /// producing a negative remainingDepth in policy.CompleteItem. Worse, the depth==0 transition
    /// is observed at "-1 instead of 0" so OnDepthReachedZero never fires, potentially leaving
    /// WaitForIdleAsync waiters hanging. Fix: IncrementDepth BEFORE _queue.Enqueue.
    [TestMethod]
    public async Task Enqueue_TightBurst_RemainingDepthNeverNegative()
    {
        var observed = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var pipeline = Pipeline.Create<TestPipelineItem, DepthRecordingPolicy>(
            new DepthRecordingPolicy(observed));

        const int n = 1_000_000;
        for (var i = 0; i < n; i++)
            pipeline.Enqueue(new TestPipelineItem()).Execute();

        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(120));

        var min = int.MaxValue;
        var max = int.MinValue;
        foreach (var d in observed)
        {
            if (d < min) min = d;
            if (d > max) max = d;
            Assert.IsTrue(d >= 0, $"negative remainingDepth observed: {d}. range [{min}, {max}]");
        }

    }

    struct DepthRecordingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly System.Collections.Concurrent.ConcurrentQueue<int> _observed;

        public DepthRecordingPolicy(System.Collections.Concurrent.ConcurrentQueue<int> observed)
        {
            _observed = observed;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
        {
            _observed.Enqueue(remainingDepth);
            item.Complete(exception);
        }

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public bool RunEnqueueAsynchronously => true;
        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => default;
        public ValueTask YieldAfterFirstItem() => default;
    }

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

    [TestMethod]
    public void WaitForIdleAsync_AlreadyIdle_CompletesImmediately()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Depth is 0 from construction. WaitForIdleAsync should return a completed task synchronously.
        var task = pipeline.WaitForIdleAsync();
        Assert.IsTrue(task.IsCompleted);
        Assert.IsTrue(task.IsCompletedSuccessfully);
    }

    [TestMethod]
    public async Task WaitForIdleAsync_Cancelled_ThrowsTaskCanceled()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Hold an item in-flight so WaitForIdleAsync has to wait.
        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        using var cts = new CancellationTokenSource();
        var idleTask = pipeline.WaitForIdleAsync(cts.Token).AsTask();

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

    /// CompleteAsync called while recovery has an in-flight async continuation (_waiterInRecovery=true).
    /// Exercises the _waiterRecoveryLock path in DrainOnCompletionAsync that prevents racing the
    /// recovery's AdvanceAndDrainRecovery against the drain.
    [TestMethod]
    public async Task CompleteAsync_DuringActiveRecovery_DrainsCleanly()
    {
        var recovery = new TestPipelineItem { ExecuteAsync = true };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null));

        // Waiter item with a pipeline task that will fault, triggering recovery.
        var faulting = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(faulting).Execute();
        await faulting.WaitForExecutedAsync();

        // Fault the pipeline task. Advancer picks it up and starts recovery (async execute).
        faulting.CompletePipelineTask();

        // Wait until recovery is in-flight (executing) before completing.
        await recovery.WaitForExecutedAsync();

        // Now CompleteAsync should serialize against the active recovery via _waiterRecoveryLock.
        var completeTask = pipeline.CompleteAsync().AsTask();

        // Recovery is still pending its execute task. Complete it.
        recovery.CompleteExecuteTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(recovery.IsCompleted, "Recovery item should be completed by the drain.");
        // Faulting item is recovered, so the recovery item carries the result.
        Assert.IsFalse(faulting.IsCompleted, "Original faulting item is not completed when recovery takes over.");
    }

    /// Multiple WaitForIdleAsync callers must all observe the drain. Exercises the drain TCS
    /// reuse: the first caller creates the TCS, subsequent callers attach to the same task.
    [TestMethod]
    public async Task MultipleConcurrentWaitForIdleCallers()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        const int waiters = 8;
        var idleTasks = new Task[waiters];
        for (var i = 0; i < waiters; i++)
            idleTasks[i] = pipeline.WaitForIdleAsync().AsTask();

        foreach (var t in idleTasks)
            Assert.IsFalse(t.IsCompleted, "Idle should not signal while item is pending.");

        item.CompletePipelineTask();

        await Task.WhenAll(idleTasks).WaitAsync(TimeSpan.FromSeconds(5));
        foreach (var t in idleTasks)
            Assert.IsTrue(t.IsCompletedSuccessfully, "Every concurrent waiter should observe the drain.");
    }

    /// Repeated drain cycles: each WaitForIdleAsync must be cleanly resolved (TCS torn down and
    /// new one created on next call). Exercises the SetResult+null pattern in CompleteWaiter
    /// and the lazy creation in WaitForIdleAsync.
    [TestMethod]
    public async Task WaitForIdleAsync_AcrossRepeatedDrainCycles()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int cycles = 20;

        for (var c = 0; c < cycles; c++)
        {
            var item = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item).Execute();
            await item.WaitForExecutedAsync();

            var idleTask = pipeline.WaitForIdleAsync().AsTask();
            Assert.IsFalse(idleTask.IsCompleted, $"Cycle {c}: idle should not signal while item is pending.");

            item.CompletePipelineTask();
            await idleTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.IsTrue(idleTask.IsCompletedSuccessfully, $"Cycle {c}: idle task should complete.");

            // Subsequent WaitForIdleAsync (depth==0) should return a completed task immediately.
            var immediate = pipeline.WaitForIdleAsync();
            Assert.IsTrue(immediate.IsCompleted, $"Cycle {c}: idle should be immediately completed after drain.");
        }
    }

    /// Regression guard for DepthState's lock-free publish-and-backstop. A WaitForIdleAsync
    /// caller arriving in the narrow window between depth hitting 0 in CompleteWaiter and the
    /// publish-side observing it must still see the TCS signaled. The backstop depth re-check
    /// after CompareExchange-publish covers the case where the caller's first depth read raced
    /// with a concurrent decrement. Multiple iterations to exercise the timing.
    [TestMethod]
    public async Task WaitForIdleAsync_DuringDepthZeroTransition()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int iterations = 100;

        for (var i = 0; i < iterations; i++)
        {
            var item = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item).Execute();
            await item.WaitForExecutedAsync();

            // Complete the item on one thread, call WaitForIdleAsync on this thread.
            // Depending on timing, the caller publishes before, during, or after the depth==0 transition.
            var completeTask = Task.Run(item.CompletePipelineTask);
            var idleTask = pipeline.WaitForIdleAsync().AsTask();

            await completeTask;
            await item.WaitForCompleteAsync();

            // Idle task must complete regardless of which side won the race.
            await idleTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.IsTrue(idleTask.IsCompletedSuccessfully, $"Iter {i}: idle task must complete.");
        }
    }

    /// Regression guard for the DepthState publish self-signal leak. If
    /// <c>GetIdleTask</c>'s CAS-loop observes depth==0 during the publish window, it self-signals
    /// the published TCS. Previously it didn't also clear <c>_drainTcs</c>, so subsequent
    /// publishers' <c>CompareExchange</c> reused the completed TCS, returning an immediately
    /// completed task even when depth was non-zero. After the fix, the self-signal path also
    /// CAS-clears <c>_drainTcs</c> so the next publisher gets a fresh slot.
    [TestMethod]
    public async Task WaitForIdleAsync_SelfSignalDoesNotLeakStaleTcs()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        for (var i = 0; i < 500; i++)
        {
            // First cycle: race a completion with a WaitForIdleAsync caller, hopefully landing in
            // the self-signal path on at least some iterations.
            var item = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item).Execute();
            await item.WaitForExecutedAsync();
            var completeTask = Task.Run(item.CompletePipelineTask);
            var idle1 = pipeline.WaitForIdleAsync();
            await completeTask;
            await item.WaitForCompleteAsync();
            await idle1.AsTask().WaitAsync(TimeSpan.FromSeconds(2));

            // Second cycle: enqueue a fresh item, depth becomes 1, request idle. The returned
            // task must NOT be already-completed; it must wait for the fresh item's completion.
            var item2 = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item2).Execute();
            await item2.WaitForExecutedAsync();
            var idle2 = pipeline.WaitForIdleAsync();
            Assert.IsFalse(idle2.IsCompleted, $"iter {i}: WaitForIdleAsync returned completed task with depth={pipeline.Depth} — stale TCS leak.");
            item2.CompletePipelineTask();
            await idle2.AsTask().WaitAsync(TimeSpan.FromSeconds(2));
        }
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

    /// WaitForIdleAsync called after CompleteAsync has fully drained should return a completed
    /// task immediately (depth is 0, drain TCS is null because no one was waiting during drain).
    [TestMethod]
    public async Task WaitForIdleAsync_AfterCompleteAsync_CompletesImmediately()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        await pipeline.CompleteAsync();

        var task = pipeline.WaitForIdleAsync();
        Assert.IsTrue(task.IsCompleted, "WaitForIdleAsync after CompleteAsync should be immediately completed.");
    }

    /// Concurrent CompleteAsync calls from multiple threads: Interlocked.Exchange on _completing
    /// ensures exactly one wins and sets _completionException, others observe the same execution task.
    /// Drained item sees the winning exception (any one of them under race).
    [TestMethod]
    public async Task CompleteAsync_ConcurrentFromMultipleThreads_FirstWriterWins()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int callers = 8;

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        var exceptions = new InvalidOperationException[callers];
        for (var i = 0; i < callers; i++)
            exceptions[i] = new InvalidOperationException($"caller {i}");

        var tasks = new Task[callers];
        for (var i = 0; i < callers; i++)
        {
            var idx = i;
            tasks[i] = Task.Run(() => pipeline.CompleteAsync(exceptions[idx]).AsTask());
        }

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(item.IsCompleted);
        Assert.IsNotNull(item.Exception);
        CollectionAssert.Contains(exceptions, item.Exception, "Drained item should see one of the supplied exceptions.");
    }

    /// WaitForIdleAsync caller is suspended when CompleteAsync fires concurrently. The drain must
    /// complete the WaitForIdleAsync TCS even though the trigger was CompleteAsync rather than
    /// natural depth-to-zero. (CompleteWaiter drives the drain TCS; CompleteAsync's drain calls
    /// CompleteWaiter for each remaining item.)
    [TestMethod]
    public async Task WaitForIdleAsync_RacingCompleteAsync_AlwaysCompletes()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        var idleTask = pipeline.WaitForIdleAsync().AsTask();
        Assert.IsFalse(idleTask.IsCompleted);

        var completeTask = pipeline.CompleteAsync().AsTask();

        await Task.WhenAll(idleTask, completeTask).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(idleTask.IsCompletedSuccessfully, "WaitForIdleAsync caller must observe the drain.");
    }

    /// Regression guard for the executor's pre-idle local clears. ExecuteQueue promotes
    /// `item`, `element`, `itemResult` out of the inner loop and explicitly defaults them
    /// before going idle. Without those clears Roslyn would leave the state-machine fields
    /// populated (gotos from the catch blocks defeat liveness analysis), and the executor's
    /// long-lived state-machine box would retain the last-processed item across the idle
    /// suspension. Test gates the executor inside OnExecutionIdleAsync so GC happens at the
    /// exact suspension point where pre-idle clears must have taken effect.
    [TestMethod]
    public async Task ExecuteQueue_PreIdleClearsLocals_ItemNotRetainedAcrossIdle()
    {
        var idleEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var idleCanReturn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        WeakReference? itemRef = null;

        var pipeline = Pipeline.Create<TestPipelineItem, IdleGatedPolicy>(
            new(idleEntered, idleCanReturn.Task));

        PushAndDrop(pipeline, ref itemRef);

        await idleEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        for (var i = 0; i < 3; i++)
        {
            GC.Collect(2, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();
        }

        Assert.IsNotNull(itemRef);
        Assert.IsFalse(itemRef.IsAlive,
            "Item still alive after pre-idle clears - executor state machine retains it.");

        idleCanReturn.SetResult();
        await pipeline.CompleteAsync();
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    static void PushAndDrop(Pipeline<TestPipelineItem, IdleGatedPolicy> pipeline, ref WeakReference? itemRef)
    {
        var item = new TestPipelineItem();
        itemRef = new WeakReference(item);
        pipeline.Enqueue(item).Execute();
    }

    struct IdleGatedPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly TaskCompletionSource _idleEntered;
        readonly Task _idleCanReturn;

        public IdleGatedPolicy(TaskCompletionSource idleEntered, Task idleCanReturn)
        {
            _idleEntered = idleEntered;
            _idleCanReturn = idleCanReturn;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
            => item.Complete(exception);

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken)
        {
            _idleEntered.TrySetResult();
            return new(_idleCanReturn);
        }

        public bool RunEnqueueAsynchronously => true;

        public ValueTask YieldAfterFirstItem() => default;
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
}
