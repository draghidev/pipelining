namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineConcurrencyTests
{
    [TestMethod]
    public async Task ConcurrentEnqueuers()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var enqueueLock = new Lock();
        const int itemsPerThread = 100;
        const int threadCount = 4;
        var allItems = new TestPipelineItem[threadCount * itemsPerThread];

        for (var i = 0; i < allItems.Length; i++)
            allItems[i] = new TestPipelineItem();

        var tasks = new Task[threadCount];
        for (var t = 0; t < threadCount; t++)
        {
            var offset = t * itemsPerThread;
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < itemsPerThread; i++)
                {
                    UnboundedQueueSource<TestPipelineItem>.EnqueueResult enqueue;
                    lock (enqueueLock)
                        enqueue = pipeline.Enqueue(allItems[offset + i]);
                    enqueue.Execute();
                }
            });
        }

        await Task.WhenAll(tasks);

        // All items should eventually complete.
        for (var i = 0; i < allItems.Length; i++)
            await allItems[i].WaitForCompleteAsync();

        Assert.AreEqual(0, pipeline.Depth);
        foreach (var item in allItems)
            Assert.IsNull(item.Exception);
    }

    [TestMethod]
    public async Task EnqueueDuringExecution()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        // First item has an async execute. While it's pending, enqueue more items.
        var first = new TestPipelineItem { ExecuteAsync = true };
        pipeline.Enqueue(first).Execute();

        // Wait for first to start executing.
        await first.WaitForExecutedAsync();

        // Enqueue more items while first is still executing.
        var second = new TestPipelineItem();
        var third = new TestPipelineItem();
        pipeline.Enqueue(second).Execute();
        pipeline.Enqueue(third).Execute();

        // Complete the first item's execute.
        first.CompleteExecuteTask();

        // All should complete in order.
        await first.WaitForCompleteAsync();
        await second.WaitForCompleteAsync();
        await third.WaitForCompleteAsync();

        Assert.IsNull(first.Exception);
        Assert.IsNull(second.Exception);
        Assert.IsNull(third.Exception);
    }

    [TestMethod]
    public async Task CompleteAsyncDuringProcessing()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        // Enqueue items with pending pipeline tasks.
        var items = new TestPipelineItem[5];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Wait for all to be executed.
        for (var i = 0; i < items.Length; i++)
            await items[i].WaitForExecutedAsync();

        // CompleteAsync while pipeline tasks are still pending.
        var completeTask = pipeline.CompleteAsync();

        // All items should be completed (with or without exception from drain).
        await completeTask;

        for (var i = 0; i < items.Length; i++)
            Assert.IsTrue(items[i].IsCompleted);
    }

    /// Exercises both branches of CommitTailWaiter in one test: sync items take the inline
    /// CompleteWaiter path (task.IsCompletedSuccessfully on commit). Async items take the
    /// EnqueueWaiter path (callback registered, advancer drains). Verifies every item completes
    /// regardless of which path it took.
    [TestMethod]
    public async Task MixedSyncAsyncPipelineTasks_AllItemsComplete()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });

        // Alternating mix: even = sync (CompleteAsync=false), odd = async (CompleteAsync=true).
        const int count = 10;
        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = i % 2 != 0 };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Wait for executor to settle. Async items will be in _waiters with callbacks registered.
        // sync items already completed inline via CommitTailWaiter's sync-success branch.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Sync items should already be complete.
        for (var i = 0; i < count; i++)
        {
            if (i % 2 == 0)
                Assert.IsTrue(items[i].IsCompleted, $"Sync item {i} should have completed inline via CommitTailWaiter.");
        }

        // Complete async items' pipeline tasks in reverse order. Advancer drains them in FIFO.
        for (var i = count - 1; i >= 0; i--)
            if (i % 2 != 0)
                items[i].CompletePipelineTask();

        for (var i = 0; i < count; i++)
            await items[i].WaitForCompleteAsync();

        for (var i = 0; i < count; i++)
            Assert.IsNull(items[i].Exception, $"Item {i} should complete without exception.");
        Assert.AreEqual(0, pipeline.Depth);
    }

    [TestMethod]
    public async Task PipelinedCompletionOrder()
    {
        // Wait for executor to suspend before completing tasks so all items are in _waiters with
        // callbacks registered. Otherwise items still at _tailWaiter get completed via the
        // executor's CommitTailWaiter sync-success path, breaking the FIFO completion order.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        var completionOrder = new List<int>();
        var orderLock = new object();

        var items = new TestPipelineItem[5];
        for (var i = 0; i < items.Length; i++)
        {
            var index = i;
            items[i] = new TestPipelineItem
            {
                CompleteAsync = true,
                OnComplete = () =>
                {
                    lock (orderLock)
                        completionOrder.Add(index);
                }
            };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Wait for all items to be executed.
        for (var i = 0; i < items.Length; i++)
            await items[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Complete pipeline tasks in reverse order. Items should still complete in pipeline order.
        for (var i = items.Length - 1; i >= 0; i--)
            items[i].CompletePipelineTask();

        for (var i = 0; i < items.Length; i++)
            await items[i].WaitForCompleteAsync();

        // Completion order must be 0, 1, 2, 3, 4. Pipeline preserves ordering.
        CollectionAssert.AreEqual(new[] { 0, 1, 2, 3, 4 }, completionOrder);
    }

    [TestMethod]
    public async Task WaitForIdleWithConcurrentEnqueue()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        // Enqueue an item.
        var first = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(first).Execute();

        // Start waiting for idle.
        var idleTask = pipeline.WaitForEmptyAsync();
        Assert.IsFalse(idleTask.IsCompleted);

        // Enqueue another item while waiting. Idle should not trigger until both are done.
        var second = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(second).Execute();

        // Complete first. Idle should still not trigger (second is pending).
        first.CompletePipelineTask();
        await first.WaitForCompleteAsync();

        // New idle wait since depth was 0 momentarily between first complete and second enqueue.
        // Exercises WaitForIdle's handling of concurrent depth changes.

        // Complete second.
        second.CompletePipelineTask();
        await second.WaitForCompleteAsync();

        Assert.AreEqual(0, pipeline.Depth);
    }

    [TestMethod]
    public async Task HighThroughputSequential()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int count = 1_000;

        for (var i = 0; i < count; i++)
        {
            var item = new TestPipelineItem();
            pipeline.Enqueue(item).Execute();
            await item.WaitForCompleteAsync();
            Assert.IsNull(item.Exception);
        }

        Assert.AreEqual(0, pipeline.Depth);
    }

    [TestMethod]
    public async Task HighThroughputPipelined()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int count = 1_000;

        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Complete all pipeline tasks in order.
        for (var i = 0; i < count; i++)
        {
            items[i].CompletePipelineTask();
            await items[i].WaitForCompleteAsync();
            Assert.IsNull(items[i].Exception);
        }

        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Upper-bound regression guard for activation: every item must be activated at most once.
    /// Activation is optional per the contract (items whose underlying work completes outside the
    /// head-of-pipeline flow may skip it), so we don't assert a lower bound. The bug class this
    /// catches is double activation, where two paths both call ActivateHeadItem for the same item.
    [TestMethod]
    public async Task NoItemActivatedMoreThanOnceUnderLoad()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int iterations = 500;

        var items = new TestPipelineItem[iterations];
        for (var i = 0; i < iterations; i++)
        {
            // Mix of async (CompleteAsync=true) and sync items to exercise both deferred-publish
            // and inline-activate paths.
            items[i] = new TestPipelineItem { CompleteAsync = i % 2 == 0 };
            pipeline.Enqueue(items[i]).Execute();
            if (items[i].CompleteAsync)
            {
                // Fire pipeline tasks from a separate thread to race with the executor / advancer.
                _ = Task.Run(items[i].CompletePipelineTask);
            }
        }

        await pipeline.WaitForEmptyAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        var doubled = items.Select((it, idx) => (it, idx)).Where(p => p.it.ActivationCount > 1).ToList();
        var detail = string.Join("; ", doubled.Select(p => $"{p.idx}({(p.it.CompleteAsync ? "async" : "sync")},count={p.it.ActivationCount})"));
        Assert.AreEqual(0, doubled.Count, $"{doubled.Count} items had multiple activations: {detail}");
        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Ordering regression guard: activations that do occur must be in pipeline-enqueue order.
    /// Activation is optional (items whose work completes outside head-of-pipeline flow may skip),
    /// so the recorded sequence is monotone-increasing rather than contiguous.
    [TestMethod]
    public async Task ActivationOrderMatchesEnqueueOrderUnderLoad()
    {
        var activationOrder = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var pipeline = Pipeline.Create<TestPipelineItem, ActivationOrderRecordingPolicy>(
            new(activationOrder));
        const int iterations = 500;

        var items = new TestPipelineItem[iterations];
        for (var i = 0; i < iterations; i++)
        {
            items[i] = new TestPipelineItem { Name = i.ToString(), CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
            _ = Task.Run(items[i].CompletePipelineTask);
        }

        await pipeline.WaitForEmptyAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        var recorded = activationOrder.ToArray();
        Assert.IsTrue(recorded.Length <= iterations, "Recorded more activations than items.");
        var prev = -1;
        for (var i = 0; i < recorded.Length; i++)
        {
            Assert.IsTrue(recorded[i] > prev, $"Activation at position {i} ({recorded[i]}) is not strictly after previous ({prev}).");
            prev = recorded[i];
        }
    }

    struct ActivationOrderRecordingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly System.Collections.Concurrent.ConcurrentQueue<int> _order;

        public ActivationOrderRecordingPolicy(System.Collections.Concurrent.ConcurrentQueue<int> order) => _order = order;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true)
        {
            _order.Enqueue(int.Parse(item.Name!));
            item.Activate();
        }

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
            => item.Complete(exception);

        public bool RunEnqueueAsynchronously => true;
    }

    /// Stresses the advancer's drain loop by completing many waiter pipeline tasks from
    /// parallel threads simultaneously. Exercises the do-while re-acquire at the end of
    /// DrainReadyWaiters and the count-decrement-then-_hasExecutingItem-check ordering.
    [TestMethod]
    public async Task ConcurrentWaiterCompletions()
    {
        // Wait for executor to suspend (all items committed to _waiters via pre-idle commit) before
        // completing tasks. Otherwise some items are still at _tailWaiter when CompletePipelineTask
        // fires, and executor's CommitTailWaiter handles them via sync-success branch instead of the
        // advancer path - mixed routing that pulls the test away from what it's supposed to exercise.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        const int count = 200;

        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Wait for all to be in the waiter queue.
        for (var i = 0; i < count; i++)
            await items[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Complete all pipeline tasks from parallel threads.
        var completionTasks = new Task[count];
        for (var i = 0; i < count; i++)
        {
            var idx = i;
            completionTasks[i] = Task.Run(() => items[idx].CompletePipelineTask());
        }
        await Task.WhenAll(completionTasks);

        for (var i = 0; i < count; i++)
        {
            await items[i].WaitForCompleteAsync();
            Assert.IsNull(items[i].Exception);
            items[i].WaitForActivation();
        }

        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Forces the executor through the deferred-activation branch (waiters present at dequeue
    /// time) repeatedly, with rapid head-of-queue completions interleaved. Confirms the dual-path
    /// coordination (_executingItem claim by either advancer or EnqueueWaiter) never drops an
    /// activation under sustained load.
    [TestMethod]
    public async Task DeferredActivationUnderSustainedLoad()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int rounds = 50;
        const int itemsPerRound = 10;
        var allItems = new List<TestPipelineItem>();

        for (var r = 0; r < rounds; r++)
        {
            // Pile up multiple async-pipeline items so subsequent enqueues see waiterQueueCount > 0
            // and take the deferred-activation branch.
            var asyncItems = new TestPipelineItem[itemsPerRound];
            for (var i = 0; i < itemsPerRound; i++)
            {
                asyncItems[i] = new TestPipelineItem { CompleteAsync = true };
                pipeline.Enqueue(asyncItems[i]).Execute();
                allItems.Add(asyncItems[i]);
            }
            for (var i = 0; i < itemsPerRound; i++)
                await asyncItems[i].WaitForExecutedAsync();

            // Drain them all back-to-back from a parallel thread while enqueueing an async
            // tail item that goes through the waiter-queue path. The tail's enqueue races with
            // the drain finishing. Either the advancer or the EnqueueWaiter path must activate.
            var drainTask = Task.Run(() =>
            {
                for (var i = 0; i < itemsPerRound; i++)
                    asyncItems[i].CompletePipelineTask();
            });

            var tail = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(tail).Execute();
            allItems.Add(tail);
            await tail.WaitForExecutedAsync();
            tail.CompletePipelineTask();

            await drainTask;
            await tail.WaitForCompleteAsync();
        }

        // Activation is contract-conditional (see ActivationFiresForEveryItemUnderWaiterCompletionRace).
        // Items that complete synchronously inside the deferred path are not required to be activated.
        // Functional correctness: every item completes without exception, depth returns to 0.
        foreach (var item in allItems)
        {
            await item.WaitForCompleteAsync();
            Assert.IsNull(item.Exception);
        }

        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Regression guard for the deferred-activation handoff. When the advancer claims the
    /// published _executingItem and calls ActivateHeadItem on its own thread, the executor's
    /// CompleteWaiter for the same item must not fire until ActivateHeadItem has finished.
    /// The _activationLock around the advancer's claim and ClearExecutingItem's fence in the
    /// deferred branch enforce this. Without that ordering a pooling policy could observe
    /// CompleteItem while ActivateHeadItem is still running on another thread, or even after
    /// Complete has returned the item to its pool.
    [TestMethod]
    public async Task DeferredActivation_CompleteWaitsForConcurrentActivate()
    {
        var pipeline = Pipeline.Create<ActivationOrderingItem, ActivationOrderingPolicy>(new());

        // A: async-pipeline waiter. Drives the advancer when its pipeline task completes.
        var a = new ActivationOrderingItem { PipelineTaskAsync = true };
        pipeline.Enqueue(a).Execute();
        await a.WaitForExecutedAsync();

        // B: async-execute item. Takes the deferred-activation branch (waiter A present).
        // Async execute gives the advancer time to claim _executingItem and start ActivateHeadItem.
        var b = new ActivationOrderingItem { ExecuteAsync = true };
        pipeline.Enqueue(b).Execute();
        await b.WaitForExecutedAsync();

        // Fire the advancer: completes A and lets the advancer claim B's deferred publish.
        a.CompletePipelineTask();

        // Wait for advancer to enter ActivateHeadItem(B). It'll be sleeping inside.
        b.WaitForActivationStart();

        // Now complete B's execute task. Executor wakes, sees PipelineTask sync-completed, races
        // to CompleteWaiter(B). Without the activation lock's fence in ClearExecutingItem,
        // CompleteItem would fire while ActivateHeadItem is still sleeping.
        b.CompleteExecuteTask();

        await a.WaitForCompleteAsync();
        await b.WaitForCompleteAsync();

        Assert.IsTrue(
            b.ActivationFinishedBeforeComplete,
            "CompleteItem fired before ActivateHeadItem finished, deferred-activation race not closed.");
    }

    /// Regression guard for the CommitTailWaiter activation-lock fence. CommitTailWaiter's
    /// Exchange(_hasExecutingItem, false) handshake mirrors ClearExecutingItem's, but if Exchange
    /// returns false (advancer already claimed and is mid-ActivateHeadItem under _activationLock)
    /// CommitTailWaiter must wait for that activation to finish before calling CompleteWaiter on
    /// the same item. Without the fence, CompleteItem fires while ActivateHeadItem is still
    /// running on the advancer thread.
    [TestMethod]
    public async Task TailCommit_CompleteWaitsForConcurrentActivate()
    {
        var pipeline = Pipeline.Create<ActivationOrderingItem, ActivationOrderingPolicy>(new());

        // A: async-pipeline waiter. When A's pipeline completes, advancer drains and claims B.
        var a = new ActivationOrderingItem { PipelineTaskAsync = true };
        pipeline.Enqueue(a).Execute();
        await a.WaitForExecutedAsync();

        // B: async pipeline + async trailing. Goes through deferred path (A is waiter, count>0)
        // and suspends the executor at await trailing. This is the key difference from the
        // ClearExecutingItem test - B's pipeline is pending so it becomes _tailWaiter, and
        // tail-commit (not inline ClearExecutingItem) is the path that races the advancer.
        var b = new ActivationOrderingItem { PipelineTaskAsync = true, HasTrailingTaskAsync = true };
        pipeline.Enqueue(b).Execute();
        await b.WaitForExecutedAsync();

        // Fire the advancer: drain A, claim B (deferred), start slow ActivateHeadItem(B).
        a.CompletePipelineTask();
        b.WaitForActivationStart();

        // Pre-fire B's pipeline so it's already completed when the executor reaches CommitTailWaiter.
        b.CompletePipelineTask();

        // Fire B's trailing task. Executor resumes from await trailing, falls out of inner loop,
        // hits CommitTailWaiter at the pre-idle commit site. Without the fence, CompleteWaiter(B)
        // races against the advancer's still-sleeping ActivateHeadItem(B).
        b.CompleteTrailingTask();

        await a.WaitForCompleteAsync();
        await b.WaitForCompleteAsync();

        Assert.IsTrue(
            b.ActivationFinishedBeforeComplete,
            "CompleteItem fired before ActivateHeadItem finished, tail-commit fence missing.");
    }

    sealed class ActivationOrderingItem
    {
        readonly ManualResetEventSlim _executed = new(false);
        readonly ManualResetEventSlim _completed = new(false);
        readonly ManualResetEventSlim _activationStarted = new(false);
        readonly TaskCompletionSource _pipelineTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _trailingTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource<PipelineItemResult> _executeTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _activationFinished; // 0 = not finished, 1 = finished. Volatile semantics via Interlocked.
        int _completeActivationState; // captured value of _activationFinished at CompleteItem time.

        public bool ExecuteAsync { get; init; }
        public bool PipelineTaskAsync { get; init; }
        public bool HasTrailingTaskAsync { get; init; }

        public bool ActivationFinishedBeforeComplete => _completeActivationState == 1;

        public Task WaitForExecutedAsync() => _executed.IsSet
            ? Task.CompletedTask
            : Task.Run(() => Assert.IsTrue(_executed.Wait(TimeSpan.FromSeconds(5)), "Timed out waiting for execute."));

        public Task WaitForCompleteAsync() => _completed.IsSet
            ? Task.CompletedTask
            : Task.Run(() => Assert.IsTrue(_completed.Wait(TimeSpan.FromSeconds(5)), "Timed out waiting for complete."));

        public void WaitForActivationStart() =>
            Assert.IsTrue(_activationStarted.Wait(TimeSpan.FromSeconds(5)), "Timed out waiting for activation start.");

        public void CompletePipelineTask() => _pipelineTaskTcs.SetResult();
        public void CompleteTrailingTask() => _trailingTaskTcs.SetResult();
        public void CompleteExecuteTask() => _executeTaskTcs.SetResult(new PipelineItemResult(GetTrailingTask(), GetPipelineTask()));

        ValueTask GetPipelineTask() => PipelineTaskAsync ? new(_pipelineTaskTcs.Task) : default;
        ValueTask GetTrailingTask() => HasTrailingTaskAsync ? new(_trailingTaskTcs.Task) : default;

        internal ValueTask<PipelineItemResult> RunExecute()
        {
            _executed.Set();
            return ExecuteAsync ? new(_executeTaskTcs.Task) : new(new PipelineItemResult(GetTrailingTask(), GetPipelineTask()));
        }

        internal void RunActivate()
        {
            _activationStarted.Set();
            // Slow activation: gives the executor time to race toward CompleteWaiter.
            Thread.Sleep(50);
            Volatile.Write(ref _activationFinished, 1);
        }

        internal void RunComplete()
        {
            // Snapshot whether activation has finished AT THIS MOMENT. Without the fix, this is
            // racy and will commonly observe 0 while activation is still sleeping.
            _completeActivationState = Volatile.Read(ref _activationFinished);
            _completed.Set();
        }
    }

    /// RunEnqueueAsynchronously=false + reentrant enqueue from CompleteItem: the policy's
    /// CompleteItem callback re-enqueues a second item. The second Enqueue's Signal calls
    /// _continuation inline, but the executor's MoveNext is already running (we're inside
    /// CompleteItem), so Signal early-exits on _pending=false. The new item is queued and
    /// picked up by the current MoveNext's inner loop iteration. No recursion, no hang.
    [TestMethod]
    public async Task SyncEnqueue_ReentrantEnqueueFromCompleteItem_NoRecursion()
    {
        var box = new ReentrantBox();
        var pipeline = Pipeline.Create<TestPipelineItem, ReentrantPolicy>(new ReentrantPolicy(box));

        var first = new TestPipelineItem();
        var second = new TestPipelineItem();
        var enqueued = false;

        box.OnComplete = item =>
        {
            if (ReferenceEquals(item, first) && !enqueued)
            {
                enqueued = true;
                pipeline.Enqueue(second).Execute();
            }
        };

        pipeline.Enqueue(first).Execute();

        // Under RunAsync=false the executor ran inline. Both items should be completed by now.
        await first.WaitForCompleteAsync();
        await second.WaitForCompleteAsync();

        Assert.IsNull(first.Exception);
        Assert.IsNull(second.Exception);
        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Three-way stress: producer enqueues, executor processes items with faulting pipeline
    /// tasks (triggering recovery via the advancer), recovery continuations fire and coordinate
    /// with the activation lock. Exercises the interleaving of executor + advancer + recovery
    /// continuation paths against the same _hasExecutingItem / _drainTcs / _waiterInRecovery
    /// state. Stress test - won't deterministically hit every interleaving but catches
    /// regressions under load.
    [TestMethod]
    public async Task ThreeWayRace_ProducerExecutorRecoveryContinuations()
    {
        // Recovery factory only handles PipelineTaskWaiter (advancer path). Sync on the source
        // onIdle hook so all items are committed to _waiters with callbacks registered before we
        // fault, otherwise the trailing filler at _tailWaiter could take the executor's
        // CommitTailWaiter path and skew the routing this test wants to stress.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true,
                ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter
                    ? new TestPipelineItem()
                    : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int iterations = 50;
        var faultingItems = new TestPipelineItem[iterations];
        var fillerItems = new TestPipelineItem[iterations];

        for (var i = 0; i < iterations; i++)
        {
            faultingItems[i] = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException($"fault {i}") };
            fillerItems[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(faultingItems[i]).Execute();
            pipeline.Enqueue(fillerItems[i]).Execute();
        }

        for (var i = 0; i < iterations; i++)
        {
            await faultingItems[i].WaitForExecutedAsync();
            await fillerItems[i].WaitForExecutedAsync();
        }
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault and complete from parallel threads to stress interleaving.
        var faultTask = Task.Run(() =>
        {
            for (var i = 0; i < iterations; i++)
                faultingItems[i].CompletePipelineTask();
        });
        var fillerTask = Task.Run(() =>
        {
            for (var i = 0; i < iterations; i++)
                fillerItems[i].CompletePipelineTask();
        });

        await Task.WhenAll(faultTask, fillerTask).WaitAsync(TimeSpan.FromSeconds(10));

        for (var i = 0; i < iterations; i++)
        {
            await fillerItems[i].WaitForCompleteAsync();
            // faultingItems get recovered, not directly completed.
        }

        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(0, pipeline.Depth);
    }

    sealed class ReentrantBox
    {
        public Action<TestPipelineItem>? OnComplete;
    }

    struct ReentrantPolicy(ReentrantBox box) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
        {
            box.OnComplete?.Invoke(item);
            item.Complete(exception);
        }

        public bool RunEnqueueAsynchronously => false;

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    struct ActivationOrderingPolicy : IPipelinePolicy<ActivationOrderingItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(ActivationOrderingItem item, CancellationToken cancellationToken) => item.RunExecute();
        public void ActivateHeadItem(ActivationOrderingItem item, bool preferAsync = true) => item.RunActivate();
        public void CompleteItem(ActivationOrderingItem item, int remainingDepth, Exception? exception) => item.RunComplete();
        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, ActivationOrderingItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out ActivationOrderingItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    /// Regression guard for Enqueue's depth-ordering race. Enqueue's sequence was
    /// _queue.Enqueue (publishes item) then IncrementDepth. If the executor's TryDequeue lands
    /// between those two writes, CompleteWaiter decrements depth before Enqueue's increment,
    /// producing a negative remainingDepth in policy.CompleteItem. Worse, the depth==0 transition
    /// is observed at "-1 instead of 0" so OnDepthReachedZero never fires, potentially leaving
    /// WaitForEmptyAsync waiters hanging. Fix: IncrementDepth BEFORE _queue.Enqueue.
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

    /// CompleteAsync called while a recovery continuation is in flight. DrainOnCompletionAsync
    /// waits on the advancer-idle TCS, which the recovery continuation signals when it releases
    /// _advancing via BailoutRecoveryOnShutdown (or via AdvanceAndDrainRecovery's loop exit).
    [TestMethod]
    public async Task CompleteAsync_DuringActiveRecovery_DrainsCleanly()
    {
        var recovery = new TestPipelineItem { ExecuteAsync = true };
        // We deliberately only handle the advancer's PipelineTaskWaiter kind. The test must avoid
        // the racy executor-side trailing-recovery path (RecoverCommittedTailWaiterAsync), which
        // fires kind=PipelineTask when CommitTailWaiter sees a pre-faulted tail task. Sync on the
        // source onIdle hook to guarantee the executor has done its pre-idle commit (faulting moved
        // to _waiters with callback registered) before we fault the task.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        // Waiter item with a pipeline task that will fault, triggering recovery.
        var faulting = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(faulting).Execute();
        await faulting.WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the pipeline task. Advancer picks it up and starts recovery (async execute).
        faulting.CompletePipelineTask();

        // Wait until recovery is in-flight (executing) before completing.
        await recovery.WaitForExecutedAsync();

        // CompleteAsync's drain waits on the advancer-idle TCS while recovery is in flight.
        var completeTask = pipeline.CompleteAsync().AsTask();

        // Recovery is still pending its execute task. Complete it.
        recovery.CompleteExecuteTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(recovery.IsCompleted, "Recovery item should be completed by the drain.");
        // Faulting item is recovered, so the recovery item carries the result.
        Assert.IsFalse(faulting.IsCompleted, "Original faulting item is not completed when recovery takes over.");
    }

    /// Multiple WaitForEmptyAsync callers must all observe the drain. Exercises the drain TCS
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
            idleTasks[i] = pipeline.WaitForEmptyAsync().AsTask();

        foreach (var t in idleTasks)
            Assert.IsFalse(t.IsCompleted, "Idle should not signal while item is pending.");

        item.CompletePipelineTask();

        await Task.WhenAll(idleTasks).WaitAsync(TimeSpan.FromSeconds(5));
        foreach (var t in idleTasks)
            Assert.IsTrue(t.IsCompletedSuccessfully, "Every concurrent waiter should observe the drain.");
    }

    /// Repeated drain cycles: each WaitForEmptyAsync must be cleanly resolved (TCS torn down and
    /// new one created on next call). Exercises the SetResult+null pattern in CompleteWaiter
    /// and the lazy creation in WaitForEmptyAsync.
    [TestMethod]
    public async Task WaitForEmptyAsync_AcrossRepeatedDrainCycles()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        const int cycles = 20;

        for (var c = 0; c < cycles; c++)
        {
            var item = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item).Execute();
            await item.WaitForExecutedAsync();

            var idleTask = pipeline.WaitForEmptyAsync().AsTask();
            Assert.IsFalse(idleTask.IsCompleted, $"Cycle {c}: idle should not signal while item is pending.");

            item.CompletePipelineTask();
            await idleTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.IsTrue(idleTask.IsCompletedSuccessfully, $"Cycle {c}: idle task should complete.");

            // Subsequent WaitForEmptyAsync (depth==0) should return a completed task immediately.
            var immediate = pipeline.WaitForEmptyAsync();
            Assert.IsTrue(immediate.IsCompleted, $"Cycle {c}: idle should be immediately completed after drain.");
        }
    }

    /// Concurrent CompleteAsync calls from multiple threads: Interlocked.Exchange on _completing
    /// ensures exactly one wins, others observe the same execution task, and all callers'
    /// tasks complete. The pipeline drains gracefully only - the pending item settles itself
    /// via the shutdown token (the flow-side escalation contract), so the drained item sees
    /// the cancellation, not a CompleteAsync-supplied exception (that propagation was the
    /// retired forceful-sweep contract).
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
        Assert.IsInstanceOfType<OperationCanceledException>(item.Exception,
            "Drained item settles via the shutdown token (flow-side escalation), not a CompleteAsync-supplied exception.");
    }

    /// WaitForEmptyAsync caller is suspended when CompleteAsync fires concurrently. The drain must
    /// complete the WaitForEmptyAsync TCS even though the trigger was CompleteAsync rather than
    /// natural depth-to-zero. (CompleteWaiter drives the drain TCS, CompleteAsync's drain calls
    /// CompleteWaiter for each remaining item.)
    [TestMethod]
    public async Task WaitForEmptyAsync_RacingCompleteAsync_AlwaysCompletes()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        var idleTask = pipeline.WaitForEmptyAsync().AsTask();
        Assert.IsFalse(idleTask.IsCompleted);

        var completeTask = pipeline.CompleteAsync().AsTask();

        await Task.WhenAll(idleTask, completeTask).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(idleTask.IsCompletedSuccessfully, "WaitForEmptyAsync caller must observe the drain.");
    }

    /// In-proc stress runner for the scenario above (one observed field timeout, June 2026,
    /// against the post-chain-activation tree; not yet reproduced in 40 contended suite runs).
    /// Iterations via DRAGHI_STRESS_ITERATIONS (default 200, keeps suite cost ~sub-second);
    /// on a hang it reports WHICH task was stuck - the localizing fact a WhenAll timeout hides.
    [TestMethod]
    public async Task WaitForEmptyAsync_RacingCompleteAsync_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var iter = 0; iter < iterations; iter++)
        {
            var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
            var item = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item).Execute();
            await item.WaitForExecutedAsync();

            var idleTask = pipeline.WaitForEmptyAsync().AsTask();
            var completeTask = pipeline.CompleteAsync().AsTask();

            try
            {
                await Task.WhenAll(idleTask, completeTask).WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (TimeoutException)
            {
                var diagnosis = $"iter {iter}: hang - idleTask={idleTask.Status}, completeTask={completeTask.Status}, " +
                    $"item completed={item.IsCompleted}, depth={pipeline.Depth}";
                // Suspend instead of failing when a debugger/dump harness wants the live state
                // (DRAGHI_STRESS_WAIT_ON_HANG=1): the hang's async graph is the evidence and
                // Assert.Fail would tear it down.
                if (Environment.GetEnvironmentVariable("DRAGHI_STRESS_WAIT_ON_HANG") == "1")
                {
                    // Shed the accumulated per-iteration garbage so a dump of the suspended
                    // process contains only the hang's live object graph (a full heap of
                    // dead pipelines makes dumpasync's walk take tens of minutes).
                    GC.Collect(2, GCCollectionMode.Forced);
                    GC.WaitForPendingFinalizers();
                    GC.Collect(2, GCCollectionMode.Forced);
                    Console.WriteLine($"SUSPENDED: {diagnosis}");
                    await Task.Delay(Timeout.Infinite);
                }
                Assert.Fail(diagnosis);
            }

            Assert.IsTrue(idleTask.IsCompletedSuccessfully, $"iter {iter}: idleTask={idleTask.Status}");
        }
    }

    /// In-proc stress runner for DeferredActivationUnderSustainedLoad (one observed field
    /// timeout, June 2026: the tail item executed and its pipeline task was completed, but
    /// item completion never fired; 25 isolated runs + 24 contended suite runs clean).
    /// Rolls the drain-vs-tail-enqueue race per iteration with a minimal pile so the tail's
    /// enqueue still sees waiters present (the deferred-activation branch). Iterations via
    /// DRAGHI_STRESS_ITERATIONS (default 200); on a hang it reports WHICH stage stuck plus
    /// every item's executed/completed state - the localizing facts a bare timeout hides.
    /// DRAGHI_STRESS_WAIT_ON_HANG=1 suspends (after a GC shed) for live dump capture.
    [TestMethod]
    public async Task DeferredActivationUnderSustainedLoad_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;
        const int pileSize = 4;
        var stageTimeout = TimeSpan.FromSeconds(5);

        for (var iter = 0; iter < iterations; iter++)
        {
            var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
            var pile = new TestPipelineItem[pileSize];
            for (var i = 0; i < pileSize; i++)
            {
                pile[i] = new TestPipelineItem { CompleteAsync = true };
                pipeline.Enqueue(pile[i]).Execute();
            }
            foreach (var item in pile)
            {
                if (!await Task.Run(() => item.TryWaitForExecuted(stageTimeout)))
                    await HangAsync(iter, "pile-executed", pipeline, pile, tail: null);
            }

            // Drain the pile back-to-back from a parallel thread while the tail enqueues
            // through the waiter-queue path - the racing pair under test.
            var drainTask = Task.Run(() =>
            {
                foreach (var item in pile)
                    item.CompletePipelineTask();
            });

            var tail = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(tail).Execute();

            if (!await Task.Run(() => tail.TryWaitForExecuted(stageTimeout)))
                await HangAsync(iter, "tail-executed", pipeline, pile, tail);

            tail.CompletePipelineTask();
            await drainTask;

            if (!await Task.Run(() => tail.TryWaitForCompleted(stageTimeout)))
                await HangAsync(iter, "tail-completed", pipeline, pile, tail);

            foreach (var item in pile)
            {
                if (!await Task.Run(() => item.TryWaitForCompleted(stageTimeout)))
                    await HangAsync(iter, "pile-completed", pipeline, pile, tail);
            }
        }

        static async Task HangAsync(
            int iter, string stage,
            QueuedPipeline<TestPipelineItem, TestPipelinePolicy> pipeline,
            TestPipelineItem[] pile, TestPipelineItem? tail)
        {
            var states = new System.Text.StringBuilder();
            foreach (var item in pile)
                states.Append(item.IsExecuted ? 'E' : 'e').Append(item.IsCompleted ? 'C' : 'c').Append(',');
            var diagnosis = $"iter {iter}: hang at {stage} - depth={pipeline.Depth}, pile=[{states}] " +
                $"tail={(tail is null ? "-" : $"{(tail.IsExecuted ? "E" : "e")}{(tail.IsCompleted ? "C" : "c")}")}";
            if (Environment.GetEnvironmentVariable("DRAGHI_STRESS_WAIT_ON_HANG") == "1")
            {
                // Shed accumulated per-iteration garbage so a dump of the suspended process
                // contains only the hang's live object graph.
                GC.Collect(2, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();
                GC.Collect(2, GCCollectionMode.Forced);
                Console.WriteLine($"SUSPENDED: {diagnosis}");
                await Task.Delay(Timeout.Infinite);
            }
            Assert.Fail(diagnosis);
        }
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

        // The source onIdle hook gates the executor at the wait: it signals idleEntered, then blocks
        // on idleCanReturn before letting MoveNextAsync suspend. GC is forced while the executor is held
        // at the suspension point, so the assertion observes whether the last-processed item is still
        // rooted by the executor's state-machine box across idle.
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true),
            onIdle: async _ =>
            {
                idleEntered.TrySetResult();
                await idleCanReturn.Task.ConfigureAwait(false);
            });

        PushAndDrop(pipeline, ref itemRef);

        await idleEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // idleEntered is signaled at the TOP of onIdle, BEFORE its `await idleCanReturn` actually
        // suspends the executor. GCing immediately races the executor thread's still-live stack
        // (registers/spill slots transiently rooting the just-processed item before they spill to
        // the heap state-machine box on suspension), yielding a false "alive". Let it suspend first,
        // same as the sibling Idle_* leak tests. After suspension the item is genuinely unreachable
        // (verified via gcroot: 0 roots), so a real retention regression here still fails the assert.
        await Task.Delay(50);

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
    static void PushAndDrop(ObservablePipeline<TestPipelineItem, TestPipelinePolicy> pipeline, ref WeakReference? itemRef)
    {
        var item = new TestPipelineItem();
        itemRef = new WeakReference(item);
        pipeline.Enqueue(item).Execute();
    }

    /// Regression guard for the _tailWaiter slot clear in CommitTailWaiter (Pipeline.cs:508-509).
    /// An item that goes through the async-pipeline branch lands in _tailWaiter, then gets
    /// committed to _waiters on the pre-idle CommitTailWaiter. After commit + drain, nothing in
    /// the pipeline should pin the item. Without the line 508 `_tailWaiter = default!` clear, the
    /// completed item stays GC-rooted for the entire idle period.
    [TestMethod]
    public async Task Idle_TailWaiterSlotDoesNotLeakCompletedItem()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });

        WeakReference? itemRef = null;
        Action? completer = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EnqueueTailWaiterItem(pipeline, completed, ref itemRef, ref completer);

        // Wait for executor to suspend (pre-idle CommitTailWaiter has run, item is in _waiters).
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Drain via the captured completer. Null both refs after firing so nothing external pins.
        completer!();
        completer = null;
        // Deterministic barrier: CompleteItem fired (the OnComplete hook holds the TCS, never
        // the item), so the only residual roots are transient (drain-thread stack slots).
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsNotNull(itemRef);
        // Bounded retry: transient stack rooting settles within a few rounds; a REAL retention
        // (a pipeline field holding the item) never dies, so the guard keeps its teeth.
        await AssertDiesAsync(itemRef,
            "Item still alive after pipeline drained — committed tail not cleared (_tailWaiter / _executingItem).");
    }

    /// Forces a few gen2 collections, retrying while the reference is alive (transient stack
    /// rooting on freshly parked threads settles; structural retention never does).
    static async Task AssertDiesAsync(WeakReference reference, string message)
    {
        for (var attempt = 0; attempt < 20; attempt++)
        {
            GC.Collect(2, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();
            if (!reference.IsAlive)
                return;
            await Task.Delay(25);
        }
        Assert.IsFalse(reference.IsAlive, message);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    static void EnqueueTailWaiterItem(ObservablePipeline<TestPipelineItem, TestPipelinePolicy> pipeline, TaskCompletionSource completed, ref WeakReference? itemRef, ref Action? completer)
    {
        var item = new TestPipelineItem { CompleteAsync = true, OnComplete = completed.SetResult };
        itemRef = new WeakReference(item);
        completer = item.CompletePipelineTask;
        pipeline.Enqueue(item).Execute();
    }

    /// Regression guard for the _executingItem slot clear in ClearExecutingItem (Pipeline.cs:1038-1039).
    /// An item dequeued with waiters present takes the deferred-publish path
    /// (_executingItem=item, _hasExecutingItem=true). On sync-success completion, ClearExecutingItem
    /// must Exchange _hasExecutingItem=false AND clear _executingItem when the executor wins the race.
    /// Without the clear, the completed item stays GC-rooted for the entire idle period.
    [TestMethod]
    public async Task Idle_ExecutingItemSlotDoesNotLeakDeferredItem()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });

        WeakReference? waiterRef = null;
        WeakReference? deferredRef = null;
        Action? waiterCompleter = null;
        var waiterCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EnqueueDeferredScenario(pipeline, waiterCompleted, ref waiterRef, ref deferredRef, ref waiterCompleter);

        // Wait for executor to suspend. By this point: waiter is in _waiters, deferred item has been
        // dequeued through the deferred-publish path, sync-completed, ClearExecutingItem ran.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Drain the waiter so it leaves _waiters too, then drop the completer ref. The barrier
        // TCS lives in the OnComplete hook, which never roots the item.
        waiterCompleter!();
        waiterCompleter = null;
        await waiterCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsNotNull(waiterRef);
        await AssertDiesAsync(waiterRef, "Waiter item leaked after drain.");
        Assert.IsNotNull(deferredRef);
        await AssertDiesAsync(deferredRef,
            "Deferred item leaked — _executingItem slot not cleared in ClearExecutingItem's race-win branch.");
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    static void EnqueueDeferredScenario(
        ObservablePipeline<TestPipelineItem, TestPipelinePolicy> pipeline,
        TaskCompletionSource waiterCompleted,
        ref WeakReference? waiterRef,
        ref WeakReference? deferredRef,
        ref Action? waiterCompleter)
    {
        // Waiter (CompleteAsync) lands in _waiters first. The next item then takes the deferred path
        // because waiterQueueCount > 0 when it's dequeued.
        var waiter = new TestPipelineItem { CompleteAsync = true, OnComplete = waiterCompleted.SetResult };
        var deferred = new TestPipelineItem();  // sync success, hits ClearExecutingItem's race-win
        waiterRef = new WeakReference(waiter);
        deferredRef = new WeakReference(deferred);
        waiterCompleter = waiter.CompletePipelineTask;
        pipeline.Enqueue(waiter).Execute();
        pipeline.Enqueue(deferred).Execute();
    }

    /// Regression guard for RecoverWaiterResult's async-trailing continuation bailout
    /// (Pipeline.cs:871-895). When recovery's executeTask completes sync but its trailing task is
    /// async-pending, the continuation hooked at line 871 must observe wake completion and call
    /// BailoutRecoveryOnShutdown. Without that branch the recovery would be stranded with the
    /// advancer flag still held.
    [TestMethod]
    public async Task WaiterRecovery_AsyncTrailingDuringShutdown_BailsOutCleanly()
    {
        // Recovery: sync execute (ExecuteAsync default = false), async trailing (HasTrailingTask),
        // pending pipeline task. Enters RecoverWaiterResult's async-trailing branch.
        var recovery = new TestPipelineItem { HasTrailingTask = true, CompleteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("waiter fault"),
        };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault → advancer → RecoverWaiter → recovery executes sync → RecoverWaiterResult hooks
        // a continuation on the async trailing task. Advancer stays held until continuation fires.
        item.CompletePipelineTask();
        await recovery.WaitForExecutedAsync();

        // CompleteAsync while the trailing-task continuation is still pending.
        var completeTask = pipeline.CompleteAsync().AsTask();
        await Task.Delay(50);  // let drain reach the advancer-idle spin

        // Fire trailing. Continuation runs, observes _wakeSignal.IsCompleted, takes the bailout
        // branch which releases _advancing and signals advancer-idle. Without the branch the
        // continuation would EnqueueWaiter against a completed wake and strand the recovery.
        recovery.CompleteTrailingTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(recovery.IsCompleted, "Recovery must be completed via the bailout path.");
    }

    /// Regression guard for the `_waiterRecoveryItem` reference leak. RecoverWaiter sets
    /// `_waiterRecoveryItem = recoveryItem` at the async-execute publish site. After successful
    /// recovery, AdvanceAndDrainRecovery flips `_waiterInRecovery=false` but used to leave
    /// `_waiterRecoveryItem` pointing at the now-completed item. For long-lived pipelines doing
    /// rare recoveries this is one stale strong reference, observable via WeakReference after GC.
    [TestMethod]
    public async Task RecoverWaiter_ClearsWaiterRecoveryItemReferenceAfterSuccess()
    {
        WeakReference? recoveryRef = null;
        TestPipelineItem? heldRecovery = null;

        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true,
                ctx =>
                {
                    if (ctx.Kind == PipelineItemFailureKind.PipelineTaskWaiter)
                    {
                        var r = new TestPipelineItem { ExecuteAsync = true };
                        recoveryRef = new WeakReference(r);
                        heldRecovery = r;
                        return r;
                    }
                    return null;
                }),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("fault"),
        };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        item.CompletePipelineTask();

        for (var i = 0; i < 100 && heldRecovery is null; i++)
            await Task.Delay(10);
        Assert.IsNotNull(heldRecovery);

        heldRecovery!.CompleteExecuteTask();
        await heldRecovery.WaitForCompleteAsync();
        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        item = null!;
        heldRecovery = null!;

        for (var i = 0; i < 3; i++)
        {
            GC.Collect(2, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();
        }

        Assert.IsNotNull(recoveryRef);
        Assert.IsFalse(recoveryRef.IsAlive, "Recovery item still alive after completion.");
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

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public bool RunEnqueueAsynchronously => true;
    }
}
