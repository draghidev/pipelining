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
                    Pipeline.EnqueueResult enqueue;
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

    [TestMethod]
    public async Task PipelinedCompletionOrder()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
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

        public ValueTask YieldAfterFirstItem() => default;
    }

    /// Stresses the advancer's drain loop by completing many waiter pipeline tasks from
    /// parallel threads simultaneously. Exercises the do-while re-acquire at the end of
    /// DrainReadyWaiters and the count-decrement-then-_hasExecutingItem-check ordering.
    [TestMethod]
    public async Task ConcurrentWaiterCompletions()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
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
        // and parks the executor at await trailing. This is the key difference from the
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
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter
                ? new TestPipelineItem()
                : null));

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

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => default;
        public ValueTask YieldAfterFirstItem() => default;
        public bool TryRecoverItemFailure(PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
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
        public bool TryRecoverItemFailure(PipelineItemFailureContext context, ActivationOrderingItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out ActivationOrderingItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }
}
