namespace Draghi.Pipelining.Tests;

// Catch-all for tests that can't be made deterministic via hooks (idleTcs, WaitForExecutedAsync).
// Most tests here use deterministic hooks and parallelize fine; the two *_Stress runners opt out
// per-method below - their 200-iteration loops with 10s timeouts can be skewed by cross-test TP
// pressure into spurious hangs.
[TestClass]
public class PipelineConcurrencyTests
{
    // Races enumeration with recovery-slot publication and clearing. The recovery has no second
    // enumerable location, so it may be missed by a best-effort pass but must never appear twice.
    [TestMethod]
    public async Task Enumeration_RacesRecoverySlotPublishAndClear()
    {
        var recoveryHolder = new TestPipelineItem?[1];
        var idleHolder = new TaskCompletionSource?[1];
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? Volatile.Read(ref recoveryHolder[0]) : null),
            onIdle: _ => { Volatile.Read(ref idleHolder[0])?.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var stop = false;
        Exception? hammerException = null;
        long hammerPasses = 0;
        // Ensure the hammer participates even under parallel-suite thread-pool pressure.
        var hammerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hammer = Task.Run(() =>
        {
            try
            {
                hammerStarted.TrySetResult();
                while (!Volatile.Read(ref stop))
                {
                    var recovery = Volatile.Read(ref recoveryHolder[0]);
                    var recoveryYields = 0;
                    foreach (var observed in pipeline)
                    {
                        if (ReferenceEquals(observed, recovery))
                            recoveryYields++;
                    }
                    Assert.IsTrue(recoveryYields <= 1, "Enumeration yielded the recovery twice in one pass.");
                    Interlocked.Increment(ref hammerPasses);
                }
            }
            catch (Exception ex)
            {
                Volatile.Write(ref hammerException, ex);
            }
        });

        await hammerStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        const int iterations = 100;
        for (var iteration = 0; iteration < iterations && Volatile.Read(ref hammerException) is null; iteration++)
        {
            var recovery = new TestPipelineItem { Name = $"recovery-{iteration}", CompleteAsync = true };
            Volatile.Write(ref recoveryHolder[0], recovery);
            var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref idleHolder[0], idleTcs);

            var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
            pipeline.Enqueue(item).Execute();
            // Commit before faulting so recovery runs from the dedicated in-flight slot.
            await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

            item.CompletePipelineTask();
            await recovery.WaitForExecutedAsync();

            Assert.IsTrue(SpinWait.SpinUntil(() =>
            {
                foreach (var observed in pipeline)
                {
                    if (ReferenceEquals(observed, recovery))
                        return true;
                }
                return false;
            }, TimeSpan.FromSeconds(10)), $"Iteration {iteration}: pending recovery never became enumerable.");
            Assert.IsFalse(recovery.IsCompleted);

            recovery.CompletePipelineTask();
            await recovery.WaitForCompleteAsync();
        }

        Volatile.Write(ref stop, true);
        await hammer;
        if (Volatile.Read(ref hammerException) is { } fault)
            throw fault;
        Assert.IsTrue(Interlocked.Read(ref hammerPasses) > 0, "Hammer never completed a pass.");
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    [TestMethod]
    public async Task ConcurrentEnqueuers()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
        foreach (var item in allItems)
            Assert.IsNull(item.Exception);
    }

    // ROOT-CAUSE REPRO (2026-07-11, EXPECTED RED until the fix - see the
    // draghi_advance_retire_dispatch_race memory): retirement-before-activation violated at its
    // premise. Pipeline-task completion is two-phase: (1) publish completed, (2) read the
    // (continuation, state) pair and dispatch - and phase 2 runs unlicensed on the completer's
    // thread. The done-arm judges retire-eligibility from phase 1 alone (TryClaimCompletedHead ->
    // IsCompleted), so a committer arm claims the head, GetResult-consumes it (resetting the
    // tenure, nulling the pair), and activates the successor while the predecessor's completion
    // dispatch is still in flight. The completer's phase-2 state read then yields null, and the
    // BCL ValueTaskAwaiter known callback throws the production "unexpected state object"
    // ArgumentOutOfRangeException.
    //
    // The two-phase window is owned entirely by the TEST source (TwoPhaseValueTaskSource) - no
    // pipeline hooks: the pipeline's own RegisterAdvanceCallback performs the real BCL registration
    // and the real Advance performs the racing GetResult. Deterministic, no stress.
    //
    // ASSERTS THE INVARIANT (the fix's acceptance): while a claimed-completed head's dispatch is
    // in flight, the done-arm must not retire it nor activate the successor (deposit instead -
    // the fire is guaranteed to arrive and serve it); the released completer must dispatch
    // cleanly, and only then does the walk retire A and activate B.
    [TestMethod]
    public async Task Advance_SuccessorActivationWaitsForPredecessorCompletionDispatch()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var sourceA = new TwoPhaseValueTaskSource();
        var itemA = new TestPipelineItem { Name = "A", PipelineTaskSource = sourceA };
        var itemB = new TestPipelineItem { Name = "B", CompleteAsync = true };
        var completer = Task.FromResult((Exception?)null);
        try
        {
            // A commits edge-owned at the empty edge; RegisterAdvanceCallback lands the BCL pairing
            // (s_invokeActionDelegate, _onAdvanceCallback) on sourceA - the registration under threat.
            pipeline.Enqueue(itemA).Execute();
            await itemA.WaitForExecutedAsync();
            Assert.IsTrue(sourceA.WaitForRegistration(TimeSpan.FromSeconds(10)), "advance-fire registration on A's task");

            // The completer publishes phase 1 and parks mid-dispatch, between its continuation-read
            // and its state-read - the live capture's thread-106 position.
            completer = Task.Run(() =>
            {
                try { sourceA.SetResult(); return (Exception?)null; }
                catch (Exception ex) { return ex; }
            });
            Assert.IsTrue(sourceA.WindowReached.Wait(TimeSpan.FromSeconds(10)), "completer parked in the dispatch window");

            // B's mid-chain commit arm wins the free license and advances. On the broken build it
            // claims A (phase 1 is out), GetResult-consumes it (the tear), retires it, and
            // activates B - synchronously on the executor strand, microseconds after B executes -
            // all while A's completion dispatch is provably parked. The invariant forbids all of it.
            pipeline.Enqueue(itemB).Execute();
            await itemB.WaitForExecutedAsync();
            var bActivatedDuringDispatch = SpinWait.SpinUntil(() => itemB.ActivationCount > 0, TimeSpan.FromSeconds(1));
            Assert.IsFalse(bActivatedDuringDispatch,
                "B activated while A's completion dispatch was in flight - the successor activated INSIDE the predecessor's completion (retire-before-activate broken at its premise).");
            Assert.IsFalse(itemA.IsCompleted,
                "A retired while its completion dispatch was in flight - its waiter tenure was consumed under the dispatcher.");

            // Release the completer: phase 2 dispatches the advance-fire cleanly, and only then
            // does the walk retire A and activate B, in order.
            sourceA.WindowRelease.Set();
            var completerException = await completer;
            Assert.IsNull(completerException, $"the completer's dispatch tore: {completerException}");
            await itemA.WaitForCompleteAsync();
            await itemB.WaitForActivationAsync();

            itemB.CompletePipelineTask();
            await itemB.WaitForCompleteAsync();
            PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
        }
        finally
        {
            // On a red run the window is still held and B is still pending: release the parked
            // completer (its AOORE lands in the captured task, observed or not) and settle B so
            // the pipeline can drain instead of wedging suite teardown.
            sourceA.WindowRelease.Set();
            itemB.CompletePipelineTask();
        }
    }

    [TestMethod]
    public async Task EnqueueDuringExecution()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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

    /// Mixed sync/async stream. A sync item (CompleteAsync=false) at the TRUE head (empty ROB) is
    /// driven to completion inline before the executor advances. A sync item with a pending async
    /// predecessor buffered in the store is head-gated through the ROB instead: its retirement
    /// defers until the predecessor completes, keeping retirement strictly FIFO (an inline complete
    /// there would jump the ROB and clear the head word out from under the live reader - the baton
    /// wedge). Async items (CompleteAsync=true) commit via CommitPendingTail and drain in FIFO.
    /// Verifies both behaviors and that every item completes.
    [TestMethod]
    public async Task MixedSyncAsyncPipelineTasks_AllItemsComplete()
    {
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Alternating mix: even = sync (CompleteAsync=false), odd = async (CompleteAsync=true).
        // Stream items in one at a time: each Execute() can wake the executor mid-stream, so a sync
        // item reaches head while async waiters are already pending. We do NOT pre-stage the backlog
        // - the whole point is to exercise sync-at-head inline completion under streaming.
        const int count = 10;
        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = i % 2 != 0 };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Per-boundary check, keyed off the NEXT item's SignalExecuted (the executor dispatching
        // item i+1; the executor loop is sequential). Item 0 is at the true head with an empty ROB:
        // it must complete inline before the executor advances. Every later sync item has a pending
        // async predecessor buffered in the store, so the head gate must DEFER its retirement (the
        // IsFalse is deterministic: the predecessor's pipeline task is only completed in the release
        // phase below, so nothing can have retired the sync item yet - observing it completed here
        // would be an out-of-FIFO inline retire, the baton-wedge defect).
        for (var i = 0; i < count; i++)
        {
            if (i % 2 == 0)
            {
                await items[i + 1].WaitForExecutedAsync();
                if (i == 0)
                    Assert.IsTrue(items[i].IsCompleted,
                        $"Sync item {i} at the empty-ROB head should have been driven to idle inline before the executor advanced to item {i + 1}.");
                else
                    Assert.IsFalse(items[i].IsCompleted,
                        $"Sync item {i} has a pending async predecessor; the head gate must defer its retirement through the ROB, not complete it inline out of FIFO order.");
            }
        }

        // Complete async items' pipeline tasks in reverse order. Advancer drains them in FIFO.
        for (var i = count - 1; i >= 0; i--)
            if (i % 2 != 0)
                items[i].CompletePipelineTask();

        for (var i = 0; i < count; i++)
            await items[i].WaitForCompleteAsync();

        for (var i = 0; i < count; i++)
            Assert.IsNull(items[i].Exception, $"Item {i} should complete without exception.");
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// The flip side of the inline-completion test: the pipeline STALL an activation-gated item
    /// imposes. An item whose execute task stays pending until it is activated holds the executor's
    /// single pump from dispatch until activation (its FIFO turn). While it is stalled awaiting
    /// activation behind a prior in-flight item, NO subsequent item can be dispatched - that
    /// occupied-pump window is the stall. It clears only when the prior item completes and the
    /// advancer hands activation to the stalled item.
    ///
    /// This is the cost of an item whose completion is gated on its own activation while it is being
    /// driven inline on the single pump: the pump cannot advance to later items until the gate
    /// opens. The companion MixedSyncAsyncPipelineTasks_AllItemsComplete pins the opposite property
    /// (an item that completes inline at head without ever holding the pump past its own dispatch).
    [TestMethod]
    public async Task ActivationGatedItem_StallsExecutorUntilActivated()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // A: a prior async item. Activated inline (no waiters at dispatch), then parks as a waiter
        // on its pending pipeline task - free-floating, it does NOT hold the executor pump.
        var a = new TestPipelineItem { CompleteAsync = true };
        // B: the activation-gated item. ExecuteAsync keeps its execute task pending so the executor
        // is forced to await it inline (as a synchronous body would be driven on the pump). It only
        // resolves once B is activated - the test plays the body below. B is dispatched while A is
        // still pending, so it is published for deferred activation, not activated at dispatch.
        var b = new TestPipelineItem { ExecuteAsync = true };
        // C: a follow-on item that can only be dispatched once the pump is freed from B.
        var c = new TestPipelineItem();

        pipeline.Enqueue(a).Execute();
        // Barrier: only A is enqueued, so the first idle deterministically means A was dispatched,
        // committed to _inFlight with its callback registered, and the pump is free and suspended.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        pipeline.Enqueue(b).Execute();
        // B is dequeued, deferred-activation-published (A still in flight), and the executor is now
        // suspended awaiting B's pending execute task. The single pump is held by B: the stall.
        await b.WaitForExecutedAsync();

        pipeline.Enqueue(c).Execute();

        // The stall, deterministically: C sits behind B in the queue and the pump is sequential, so
        // the executor cannot reach C until B's execute task resolves - which needs B's activation,
        // which needs A to complete. None of that has happened, so C is structurally un-dispatched.
        Assert.IsFalse(c.IsExecuted, "B (activation-gated) holds the single pump until activated; C must not be dispatched yet.");
        Assert.AreEqual(0, b.ActivationCount, "B must not be activated while A is still in flight (FIFO turn).");

        // Play B's body: it can only run once it gets its turn (activation), at which point it drives
        // to completion inline. Wire that drive to fire on activation.
        _ = b.WaitForActivationAsync().ContinueWith(_ => b.CompleteExecuteTask(), TaskScheduler.Default);

        // Completing A drains the prior waiter; the advancer's slot-drain C-path then claims B's
        // deferred-activation publish and activates the still-executing B (ActivateNextAfterSlotAdvance).
        a.CompletePipelineTask();
        await a.WaitForCompleteAsync();

        // The stall clears strictly in order: B activates only after A completed, B's body resolves
        // only after activation, and only then does the pump advance to dispatch C.
        await b.WaitForActivationAsync();
        await c.WaitForExecutedAsync();

        await b.WaitForCompleteAsync();
        await c.WaitForCompleteAsync();

        Assert.IsNull(a.Exception);
        Assert.IsNull(b.Exception);
        Assert.IsNull(c.Exception);
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    [TestMethod]
    public async Task PipelinedCompletionOrder()
    {
        // Wait for executor to suspend before completing tasks so all items are in _inFlight with
        // callbacks registered. Otherwise items still at _pendingTail get completed via the
        // executor's CommitPendingTail sync-success path, breaking the FIFO completion order.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    [TestMethod]
    public async Task HighThroughputSequential()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
        const int count = 1_000;

        for (var i = 0; i < count; i++)
        {
            var item = new TestPipelineItem();
            pipeline.Enqueue(item).Execute();
            await item.WaitForCompleteAsync();
            Assert.IsNull(item.Exception);
        }

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    [TestMethod]
    public async Task HighThroughputPipelined()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// Upper-bound regression guard for activation: every item must be activated at most once.
    /// Activation is optional per the contract (items whose underlying work completes outside the
    /// head-of-pipeline position may skip it), so we don't assert a lower bound. The bug class this
    /// catches is double activation, where two paths both call ActivateHeadItem for the same item.
    [TestMethod]
    public async Task NoItemActivatedMoreThanOnceUnderLoad()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// Cross-item single-reader guard - the invariant NoItemActivatedMoreThanOnceUnderLoad does NOT
    /// cover. That test asserts no SINGLE item is activated twice; this asserts no TWO items are ever
    /// in the activated-but-not-completed phase at once. Draghi's Complete(prev)-before-Activate(next)
    /// ordering makes "one reader at a time" a hard invariant, but nothing asserted it - and a policy
    /// with a shared per-connection reader resource (Slon's single read promise) CRASHES when two
    /// items are activated concurrently ("The async method is already executing"). The guard is a
    /// faithful mini-model of that shared baton: ActivateHeadItem takes it, CompleteItem releases it,
    /// two live holders = the collision.
    ///
    /// Trips on the executor's dispatch-time inline-activate (Count is 0 -> activate) racing the
    /// advancer's drain-time activation: the executor reads _inFlight.Count is 0 the instant the
    /// advancer has dequeued the last waiter but not yet activated it, so both strands activate the
    /// head - two readers on one baton. A stress loop because the window is narrow per iteration.
    [TestMethod, DoNotParallelize]
    public async Task NoTwoItemsActivatedConcurrently_SingleReaderInvariant()
    {
        // Default sized for strand-hunting, not just the baton race: the one observed permanent
        // strand (see the gauge-instrumented assert below) was a <=1/32k-iteration event, so a
        // few hundred iterations are statistically silent. ~1ms/iter keeps even this default
        // cheap; DRAGHI_STRESS_ITERATIONS overrides for dedicated hunts.
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 2500;

        // DRAGHI_TP_PRESSURE=<K>: recreate the threadpool-starvation weather the iter-602 strand
        // was observed under. At the time, FrameworkSlot's old holdRelease blocked ~60 TP workers
        // in Sleep(1) loops CONCURRENTLY with this test (parallel suite) - starving completion
        // Task.Runs and stretching every window. That accidental load generator was removed when
        // the slow test was fixed, which is the leading explanation for the strand going quiet
        // (environment deleted, not bug fixed). K saturator work items block-sleep for the whole
        // run, forcing injection churn; 0/unset = off.
        var pressure = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_TP_PRESSURE"), out var k) ? k : 0;
        var pressureStop = false;
        for (var p = 0; p < pressure; p++)
        {
            ThreadPool.UnsafeQueueUserWorkItem<object?>(_ =>
            {
                while (!Volatile.Read(ref pressureStop))
                    Thread.Sleep(1);
            }, null, preferLocal: false);
        }

        try
        {
        for (var iter = 0; iter < iterations; iter++)
        {
            var guard = new SingleReaderGuard();
            var pipeline = Pipeline.Create<TestPipelineItem, SingleReaderGuardPolicy>(new(guard));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
            const int count = 64;

            var items = new TestPipelineItem[count];
            for (var i = 0; i < count; i++)
            {
                // MIX: even = sync (default pipeline task -> executor sync-shortcut completion, no
                // advancer), odd = async (pipeline task pending until ACTIVATED by the policy). Models
                // Slon: sync flows complete at head inline; an async flow behind them can only complete
                // if activation lands. A lost activation => the async item strands forever (the hang).
                items[i] = new TestPipelineItem { Name = i.ToString(), CompleteAsync = i % 2 != 0 };
                pipeline.Enqueue(items[i]).Execute();
                // Third flavor (every fourth, a subset of the async half): the pipeline task settles
                // immediately off-thread, racing execute and commit. Produces completed-before-commit
                // heads and commit-window completions, the ingredient the peek-gated C-path license
                // and the completed-head D-arm guards exist for. Drain-only when the race wins
                // (completion without activation is legal), first-writer-wins when activation lands.
                if (i % 4 == 3)
                    ThreadPool.UnsafeQueueUserWorkItem(
                        static it => ((TestPipelineItem)it!).CompletePipelineTask(), items[i], preferLocal: false);
            }

            var emptyReturned = true;
            try { await pipeline.WaitForEmptyAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (TimeoutException) { emptyReturned = false; }

            // Snapshot the instant empty fired: distinguishes backlog-gap (items not dispatched) from
            // depth-undercount (Depth reads 0 while dispatched items are still in-flight/uncompleted).
            var depthAtFire = pipeline.Depth;
            var inflightAtFire = items.Count(it => it.IsExecuted && !it.IsCompleted);
            var backlogAtFire = items.Count(it => !it.IsExecuted);

            // WaitForEmptyAsync can return while the advancer is still draining (depth reaches 0 before
            // every CompleteItem runs). Poll for all items to complete to tell a PERMANENT strand
            // (some item never completes) from a PREMATURE-EMPTY (all complete, just after the signal).
            var settleSw = System.Diagnostics.Stopwatch.StartNew();
            while (items.Any(it => !it.IsCompleted) && settleSw.Elapsed < TimeSpan.FromSeconds(15))
                await Task.Delay(5);

            // Checked after the settle poll so the forensics read final counts: a completion that was
            // merely late must not classify as lost. DescribeViolation's completion/activation counts
            // for the stuck holder discriminate complete-then-activate / double-activation (the take
            // that never releases) from activation-without-drain (the strand family).
            if (guard.Violation is { } v)
                Assert.Fail($"iter {iter}: single-reader invariant violated - {v}\nFORENSICS:\n{guard.DescribeViolation()}\nBATON JOURNAL:\n{guard.DumpJournal()}");

            var incomplete = items.Where(it => !it.IsCompleted).ToList();
            if (incomplete.Count > 0)
            {
                // Snapshot the TP queue BEFORE the second-chance wait: thousands pending = the
                // completion chain may be starved-but-queued; ~zero pending with c=0 = the
                // obligation is not in the queue at all (genuine loss).
                var tpPending = ThreadPool.PendingWorkItemCount;
                var tpThreads = ThreadPool.ThreadCount;
                var stuck = incomplete
                    .Select(it => $"{it.Name}({(it.CompleteAsync ? "async" : "sync")},exec={it.IsExecuted},act={it.ActivationCount},task={(it.PipelineTaskCompleted ? 1 : 0)},c={guard.CompletionCount(it.Name!)})")
                    .ToList();
                // Delay-vs-loss discriminator: under TP pressure a starved (but queued) completion
                // chain resolves once thread injection catches up; a lost obligation never does.
                var lateSw = System.Diagnostics.Stopwatch.StartNew();
                while (items.Any(it => !it.IsCompleted) && lateSw.Elapsed < TimeSpan.FromSeconds(30))
                    await Task.Delay(50);
                var resolvedLate = !items.Any(it => !it.IsCompleted);
                // Under DELIBERATE pressure a resolved-late is the expected weather artifact
                // (verified: iter-1771 stalled >15s at tpPending=1787 and resolved at +9s - the
                // whole chain incl. the executor's wake continuation was TP-queued, nothing lost).
                // Only a genuinely-stuck set fails a pressure run. Without pressure, a 15s stall
                // is anomalous either way and still fails loudly below.
                if (resolvedLate && pressure > 0)
                    continue;
                // Gauges discriminate the strand class: Backlog ~= stuck count => the executor never
                // pulled them (parked / lost wake); Depth > 0 with Backlog low => pulled but never
                // executed/completed (dispatch- or activation-side drop); both 0 => completion
                // bookkeeping lost (test-side flags never set despite the pipeline draining).
                Assert.Fail($"iter {iter}: PERMANENT STRAND - {stuck.Count} never completed after 15s " +
                    $"(Depth={pipeline.Depth}, Backlog={pipeline.Backlog}, emptyReturned={emptyReturned}, " +
                    $"tpPending={tpPending}, tpThreads={tpThreads}; " +
                    $"{(resolvedLate ? $"RESOLVED LATE at +{lateSw.ElapsedMilliseconds}ms => starvation delay, not a loss" : "STILL STUCK after +30s => genuine lost obligation")}): " +
                    $"{string.Join("; ", stuck)}");
            }

            var doubles = items.Where(it => guard.CompletionCount(it.Name!) != 1)
                .Select(it => $"{it.Name}=c{guard.CompletionCount(it.Name!)}").ToList();
            var prematureMs = !emptyReturned ? -1 : settleSw.ElapsedMilliseconds;
            // Premature = the PIPELINE's gauges showed outstanding work at fire (undispatched
            // backlog or live depth). A zero-gauge fire with a small settle is the documented
            // contract: WaitForEmptyAsync fires from inside the final CompleteItem, so policy
            // bookkeeping (the test-side IsCompleted flags) may still be unwinding when it
            // returns. The 15s strand poll above already catches anything that never settles.
            var genuinePremature = depthAtFire > 0 || backlogAtFire > 0;
            if (doubles.Count > 0 || genuinePremature || !emptyReturned)
                Assert.Fail($"iter {iter}: all completed but empty fired early. " +
                    $"AT FIRE: Depth={depthAtFire}, inflight(exec,!done)={inflightAtFire}, backlog(!exec)={backlogAtFire}; " +
                    $"drain lagged ~{prematureMs}ms; {doubles.Count} double/zero: {string.Join("; ", doubles)}");
        }
        }
        finally
        {
            Volatile.Write(ref pressureStop, true);
        }
    }

    /// Recovery is a TOTAL, transparent substitution (see Slon's ResyncRecoveryFlow): when item X's
    /// trailing task faults after X was already activated (elided at count-0), RecoverTrailingFailure
    /// activates the recovery substitute R directly - the only channel by which R can learn it holds
    /// the turn, since there is no other way to notify it. R eclipses X: R (not X) is what
    /// ultimately completes. Deterministic (no stress loop needed): X is the sole item in an
    /// otherwise-empty pipeline, so it takes the trivial elision path (activated=true) with
    /// certainty. The single-reader guard must recognize this second ActivateHeadItem as a
    /// legitimate transfer (R supersedes X, recorded via TryRecoverItemFailure), not a collision -
    /// unlike an unrelated double-activation, which the main stress test already guards against.
    [TestMethod]
    public async Task RecoveryFromTrailingFailure_EclipsesOriginal_SingleReaderInvariant()
    {
        var guard = new SingleReaderGuard();
        TestPipelineItem? recovery = null;
        var pipeline = Pipeline.Create<TestPipelineItem, SingleReaderGuardPolicy>(new(guard,
            ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery = new TestPipelineItem { Name = "R" } : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
        guard.ObserveActivatedSlot(() => pipeline.Pipeline.ActivatedItem);

        var item = new TestPipelineItem
        {
            Name = "X",
            HasTrailingTask = true,
            TrailingTaskException = new InvalidOperationException("trailing failed"),
        };
        pipeline.Enqueue(item).Execute();

        await item.WaitForExecutedAsync();
        Assert.AreEqual(1, item.ActivationCount, "X must have been elided (activated) before its trailing task faults - the scenario under test.");
        item.CompleteTrailingTask();

        // The trailing TCS runs its continuation asynchronously (RunContinuationsAsynchronously), so
        // RecoverTrailingFailure fires on a threadpool hop, not inline with CompleteTrailingTask.
        Assert.IsTrue(SpinWait.SpinUntil(() => Volatile.Read(ref recovery) is not null, TimeSpan.FromSeconds(10)),
            "Recovery factory should have been invoked for the trailing failure.");
        await recovery!.WaitForCompleteAsync();

        Assert.AreEqual(1, recovery.ActivationCount, "R must have been activated exactly once - it eclipses X, it doesn't queue behind it.");
        Assert.AreEqual(0, guard.CompletionCount("X"), "X is never separately completed - it is eclipsed by R, matching Slon's ResyncRecoveryFlow semantics.");
        Assert.AreEqual(1, guard.CompletionCount("R"), "R is the identity that ultimately completes.");
        if (guard.Violation is { } v)
            Assert.Fail($"single-reader invariant violated - {v}\nFORENSICS:\n{guard.DescribeViolation()}\nBATON JOURNAL:\n{guard.DumpJournal()}");
    }

    /// Standing concurrency coverage for recovery: before this test, recovery paths (RecoverItem,
    /// RecoverTrailingFailure, RecoverCommittedPendingTailAsync) were entirely unexercised by the
    /// concurrency stress suite - the main NoTwoItemsActivatedConcurrently_SingleReaderInvariant
    /// loop never triggers a recovery factory. One in eight items faults its trailing task and gets
    /// eclipsed by a fresh substitute, mixed into the same sync/async concurrent load as the main
    /// stress test, so recovery's activation/completion machinery gets hammered against the same
    /// elision/empty-edge pass/pending-tail interleavings the rest of the suite already covers.
    ///
    /// FIXED (2026-07-10): ResolveEmptyEdgeHandoff peeks the published item before taking its
    /// generation, then activates it after releasing the edge lock. Recovery used to activate its
    /// substitute immediately after losing that race, allowing both activations to overlap. The fix doesn't touch the
    /// empty-edge pass (eliminating it breaks a genuine liveness case - a stuck backpressure write needs
    /// activation once the store drains) or the eclipse contract (R activating
    /// while X's activation is live is correct by design). It keys a wait to a dedicated
    /// per-generation stamp (ActivationGate.MarkHandoffResolved/IsHandoffResolved, written right after the
    /// empty-edge pass's ActivateHeadItem call returns) so a losing recovery waits for that activation to
    /// activation to have returned before proceeding - turning an unordered race into a clean,
    /// sequential eclipse transfer. See empty-edge pass_recovery_fix_spec.md for the full design history
    /// (five independent reviews) and the rejected alternatives.
    [TestMethod, DoNotParallelize]
    public async Task RecoveryEclipse_UnderConcurrentLoad_SingleReaderInvariant()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 500;

        for (var iter = 0; iter < iterations; iter++)
        {
            var guard = new SingleReaderGuard();
            var substitutes = new System.Collections.Concurrent.ConcurrentBag<TestPipelineItem>();
            var substituteCounter = 0;
            var pipeline = Pipeline.Create<TestPipelineItem, SingleReaderGuardPolicy>(new(guard, ctx =>
            {
                // Decline recovery for ExecuteItemTask failures (exercises RecoverItem's no-recovery-
                // decline path, which inline-completes the failed item directly - the site identified
                // as needing the same empty-edge pass-resolution wait as the inherit-and-activate arms) while
                // still offering a substitute for TrailingExecutionTask failures (the eclipse path).
                if (ctx.Kind is PipelineItemFailureKind.ExecuteItemTask)
                    return null;
                var r = new TestPipelineItem { Name = $"R{Interlocked.Increment(ref substituteCounter)}" };
                substitutes.Add(r);
                return r;
            }));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
            guard.ObserveActivatedSlot(() => pipeline.Pipeline.ActivatedItem);
            const int count = 64;

            var items = new TestPipelineItem[count];
            var recoveryTriggering = new HashSet<int>();
            var declinedFailing = new HashSet<int>();
            for (var i = 0; i < count; i++)
            {
                // Every eighth item (all odd, i.e. the async half - a synchronously-completing
                // pipeline task never reaches the pending-tail path a trailing fault needs) faults its
                // trailing task and is eclipsed by a recovery substitute.
                var triggersRecovery = i % 8 == 7;
                // A disjoint residue (i % 16 == 5 - avoids both i%8==7 and i%4==3) throws synchronously
                // from ExecuteItemAsync instead - routes through RecoverItem with no recovery offered,
                // so the item completes directly with its own exception (no substitute).
                var declinesRecovery = i % 16 == 5;
                if (triggersRecovery)
                    recoveryTriggering.Add(i);
                if (declinesRecovery)
                    declinedFailing.Add(i);
                items[i] = new TestPipelineItem
                {
                    Name = i.ToString(),
                    CompleteAsync = i % 2 != 0,
                    HasTrailingTask = triggersRecovery,
                    TrailingTaskException = triggersRecovery ? new InvalidOperationException($"trailing failed {i}") : null,
                    ThrowOnExecute = declinesRecovery ? new InvalidOperationException($"execute failed {i}") : null,
                };
                pipeline.Enqueue(items[i]).Execute();
                if (triggersRecovery)
                    ThreadPool.UnsafeQueueUserWorkItem(
                        static it => ((TestPipelineItem)it!).CompleteTrailingTask(), items[i], preferLocal: false);
                else if (i % 4 == 3)
                    ThreadPool.UnsafeQueueUserWorkItem(
                        static it => ((TestPipelineItem)it!).CompletePipelineTask(), items[i], preferLocal: false);
            }

            var emptyReturned = true;
            try { await pipeline.WaitForEmptyAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (TimeoutException) { emptyReturned = false; }

            var settleSw = System.Diagnostics.Stopwatch.StartNew();
            while ((items.Where((it, idx) => !recoveryTriggering.Contains(idx) && !it.IsCompleted).Any()
                    || substitutes.Count < recoveryTriggering.Count
                    || substitutes.Any(s => !s.IsCompleted))
                   && settleSw.Elapsed < TimeSpan.FromSeconds(15))
                await Task.Delay(5);

            if (guard.Violation is { } v)
                Assert.Fail($"iter {iter}: single-reader invariant violated - {v}\nFORENSICS:\n{guard.DescribeViolation()}\nBATON JOURNAL:\n{guard.DumpJournal()}");

            var nonRecoveryIncomplete = items.Where((it, idx) => !recoveryTriggering.Contains(idx) && !it.IsCompleted).ToList();
            var missingSubstitutes = recoveryTriggering.Count - substitutes.Count;
            var incompleteSubstitutes = substitutes.Count(s => !s.IsCompleted);
            if (nonRecoveryIncomplete.Count > 0 || missingSubstitutes > 0 || incompleteSubstitutes > 0)
                Assert.Fail($"iter {iter}: PERMANENT STRAND - {nonRecoveryIncomplete.Count} non-recovery items, " +
                    $"{missingSubstitutes} missing substitutes, {incompleteSubstitutes} incomplete substitutes " +
                    $"(Depth={pipeline.Depth}, Backlog={pipeline.Backlog}, emptyReturned={emptyReturned})");

            foreach (var idx in recoveryTriggering)
                Assert.AreEqual(0, guard.CompletionCount(items[idx].Name!), $"iter {iter}: X (item {idx}) must never separately complete - it is eclipsed by its substitute.");

            foreach (var idx in declinedFailing)
            {
                Assert.AreEqual(1, guard.CompletionCount(items[idx].Name!), $"iter {iter}: item {idx} (recovery declined) must complete exactly once, directly, with its own exception.");
                Assert.IsNotNull(items[idx].Exception, $"iter {iter}: item {idx} should have completed with its ThrowOnExecute exception.");
            }

            PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
        }
    }

    /// Faithful mini-model of Slon's shared per-connection read promise: exactly one item may hold
    /// the reader baton at a time. A second concurrent take is the shared-promise collision
    /// (PromiseAsyncValueTaskMethodBuilder.Start throwing "already executing, multiple callers").
    sealed class SingleReaderGuard
    {
        TestPipelineItem? _currentReader;
        Func<TestPipelineItem?>? _activatedSlot;
        volatile string? _violation;
        public string? Violation => _violation;
        // Captured at collision time so the failure dump can self-classify (see DescribeViolation).
        volatile TestPipelineItem? _violationHolder;
        volatile TestPipelineItem? _violationTaker;

        // Recovery-eclipse map (R -> X), recorded by TryRecoverItemFailure at the moment the real
        // framework constructs a substitute. Mirrors Slon's ResyncRecoveryFlow: the framework
        // activates the recovery item unconditionally (that's the ONLY channel by which an item
        // learns it holds the turn - there is no other way for R to know it may proceed), even
        // while X's own activation is still live. Slon's actual safety comes from the substitute
        // sequencing behind X's OutstandingPhaseTask before touching shared state, not from the
        // framework serializing the two ActivateHeadItem calls. So a second TakeBaton is a
        // legitimate TRANSFER, not a collision, precisely when it's a recorded eclipse of the
        // current holder - anything else is still a genuine double-activation.
        readonly System.Collections.Concurrent.ConcurrentDictionary<TestPipelineItem, TestPipelineItem> _eclipses = new();
        public void RecordEclipse(TestPipelineItem recoveryItem, TestPipelineItem failedItem) => _eclipses[recoveryItem] = failedItem;

        // Baton forensics: a small ring of take/release events so a violation names its own
        // interleaving. It records only the roughly two guard events per item that already do
        // interlocked work, keeping the diagnostic narrowly scoped to the guarded invariant.
        const int JournalSize = 256;
        readonly (string What, string? Item, string? Prev, int Thread)[] _journal = new (string, string?, string?, int)[JournalSize];
        int _journalTicket = -1;

        void Journal(string what, TestPipelineItem item, TestPipelineItem? prev)
        {
            var seq = Interlocked.Increment(ref _journalTicket);
            _journal[seq & (JournalSize - 1)] = (what, item.Name, prev?.Name, Environment.CurrentManagedThreadId);
        }

        public string DumpJournal()
        {
            var ticket = Volatile.Read(ref _journalTicket);
            var entries = new List<string>();
            var start = Math.Max(0, ticket - (JournalSize - 1));
            for (var seq = start; seq <= ticket; seq++)
            {
                var e = _journal[seq & (JournalSize - 1)];
                entries.Add($"#{seq} t{e.Thread} {e.What} '{e.Item}'{(e.Prev is null ? "" : $" prev='{e.Prev}'")}");
            }
            return string.Join("\n", entries);
        }

        // Per-item CompleteItem call counter: a value != 1 exposes a drain mis-route (double-complete
        // or skip). Keyed by item name so the report can line up with the activated/stuck lists.
        readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _completions = new();
        public void RecordCompletion(string name) => _completions.AddOrUpdate(name, 1, (_, c) => c + 1);
        public int CompletionCount(string name) => _completions.TryGetValue(name, out var c) ? c : 0;

        public void ObserveActivatedSlot(Func<TestPipelineItem?> read) => _activatedSlot = read;

        public void VerifyActivatedSlot(TestPipelineItem item)
        {
            var actual = _activatedSlot?.Invoke();
            if (_activatedSlot is not null && !ReferenceEquals(actual, item) && _violation is null)
            {
                _violationHolder = actual;
                _violationTaker = item;
                _violation = $"activated slot was '{actual?.Name ?? "null"}' while activating '{item.Name}'";
            }
        }

        // Mirrors ExecutePipelined.Start's TryStart on the shared promise: take the baton, and if
        // another item already holds it, record the collision. The brief spin gives a concurrent
        // activation a realistic overlap window (Slon's async Start lands on the TP, not inline).
        public void TakeBaton(TestPipelineItem item)
        {
            // Wedge-instant assert: catch activation-AFTER-completion at the take that enacts it. The
            // same item's CompleteItem already ran (CompletionCount >= 1), so this activation is a stale
            // arm firing on a retiree - the baton it takes is never released, so the downstream symptom
            // is the next item's collision. Pinning it here names the wedge rather than its aftermath.
            // Distinct from a re-entrant double-activation (that shows in ActivationCount).
            if (CompletionCount(item.Name!) >= 1 && _violation is null)
            {
                _violationHolder = item;
                _violationTaker = item;
                _violation = $"'{item.Name}' was activated AFTER its own completion (baton wedge: activation-after-retirement)";
            }
            var prev = Interlocked.CompareExchange(ref _currentReader, item, null);
            if (prev is not null && !ReferenceEquals(prev, item)
                && _eclipses.TryGetValue(item, out var eclipsed) && ReferenceEquals(eclipsed, prev))
            {
                // Legitimate recovery transfer: item (R) eclipses prev (X). CAS from prev to item -
                // a plain write would race a genuinely concurrent unrelated take.
                var transferred = Interlocked.CompareExchange(ref _currentReader, item, prev) == prev;
                Journal(transferred ? "take(transfer)" : "take(COLLISION)", item, prev);
                if (!transferred && _violation is null)
                {
                    _violationHolder = prev;
                    _violationTaker = item;
                    _violation = $"'{prev.Name}' still the reader when '{item.Name}' was activated (transfer raced an unrelated take)";
                }
                Thread.SpinWait(64);
                return;
            }
            Journal(prev is null ? "take" : ReferenceEquals(prev, item) ? "take(re-entrant)" : "take(COLLISION)", item, prev);
            if (prev is not null && !ReferenceEquals(prev, item) && _violation is null)
            {
                _violationHolder = prev;
                _violationTaker = item;
                _violation = $"'{prev.Name}' still the reader when '{item.Name}' was activated (two readers on one baton)";
            }
            Thread.SpinWait(64);
        }

        // Self-classifying forensics, evaluated at dump time (after the settle poll): the stuck
        // holder's completion count is the discriminator between the two shapes a leaked baton
        // can have. CompletionCount==0 => activation-without-drain (lost completion, the strand
        // family). CompletionCount>=1 with the take never released => the holder's CompleteItem
        // ran BEFORE (or without) a matching activation - complete-then-activate or a second
        // activation after retirement (double-activation shows in ActivationCount).
        public string DescribeViolation()
        {
            static string Flavor(TestPipelineItem it)
                => int.TryParse(it.Name, out var i)
                    ? (i % 2 == 0 ? "sync" : i % 4 == 3 ? "async+early-complete" : "async")
                    : "?";
            var parts = new List<string>();
            foreach (var (role, it) in new[] { ("holder", _violationHolder), ("taker", _violationTaker) })
            {
                if (it is null)
                    continue;
                parts.Add($"{role} '{it.Name}' [{Flavor(it)}]: completions={CompletionCount(it.Name!)}, " +
                          $"activations={it.ActivationCount}, isCompleted={it.IsCompleted}");
            }
            return string.Join("\n", parts);
        }

        // Mirrors GetResult releasing _started when the read completes.
        public void ReleaseBaton(TestPipelineItem item)
        {
            var released = Interlocked.CompareExchange(ref _currentReader, null, item);
            Journal(ReferenceEquals(released, item) ? "release" : "release(MISS)", item, released);
        }
    }

    struct SingleReaderGuardPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly SingleReaderGuard _guard;
        readonly Func<PipelineItemFailureContext, TestPipelineItem?>? _recoveryFactory;
        public SingleReaderGuardPolicy(SingleReaderGuard guard) : this(guard, null) { }
        public SingleReaderGuardPolicy(SingleReaderGuard guard, Func<PipelineItemFailureContext, TestPipelineItem?>? recoveryFactory)
        {
            _guard = guard;
            _recoveryFactory = recoveryFactory;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
        {
            if (item.ThrowOnExecute is { } ex)
                throw ex;
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true)
        {
            _guard.VerifyActivatedSlot(item);
            _guard.TakeBaton(item);
            item.Activate();
            // The read completes only AFTER activation (Slon's baton read). Async so it doesn't run
            // under any advancer latch inline. No activation => this never fires => the item strands.
            Task.Run(item.CompletePipelineTask);
        }

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
        {
            _guard.RecordCompletion(item.Name!);
            _guard.ReleaseBaton(item);
            item.Complete(exception);
        }

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = _recoveryFactory?.Invoke(context);
            if (recoveryItem is not null)
                _guard.RecordEclipse(recoveryItem, failedItem);
            return recoveryItem is not null;
        }

        public bool RunEnqueueAsynchronously => true;
    }

    /// Ordering regression guard: activations that do occur must be in pipeline-enqueue order.
    /// Activation is optional (items whose work completes outside head-of-pipeline position may skip),
    /// so the recorded sequence is monotone-increasing rather than contiguous.
    [TestMethod]
    public async Task ActivationOrderMatchesEnqueueOrderUnderLoad()
    {
        var activationOrder = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var pipeline = Pipeline.Create<TestPipelineItem, ActivationOrderRecordingPolicy>(
            new(activationOrder));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
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

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context,
            TestPipelineItem failedItem, CancellationToken cancellationToken,
            [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public bool RunEnqueueAsynchronously => true;
    }

    /// Stresses the advancer's drain loop by completing many waiter pipeline tasks from
    /// parallel threads simultaneously. Exercises the do-while re-acquire at the end of
    /// in-flight drain and the count-decrement-then-_executingItemActivationPending-check ordering.
    [TestMethod]
    public async Task ConcurrentPipelineTaskCompletions()
    {
        // Wait for executor to suspend (all items committed to _inFlight via pre-idle commit) before
        // completing tasks. Otherwise some items are still at _pendingTail when CompletePipelineTask
        // fires, and executor's CommitPendingTail handles them via sync-success branch instead of the
        // advancer path - mixed routing that pulls the test away from what it's supposed to exercise.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
        const int count = 200;

        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Wait for all to enter the in-flight store.
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
            // No activation assert: an item whose completion beats the D-arm peek is drain-only
            // (the completed-head guard skips it), so activation is legitimately absent here.
            // Lost drains are what WaitForCompleteAsync above catches.
        }

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// Forces the executor through the deferred-activation branch (waiters present at dequeue
    /// time) repeatedly, with rapid head-of-queue completions interleaved. Confirms the dual-path
    /// coordination (_executingItem claim by either advancer or CommitInFlightItem) never drops an
    /// activation under sustained load.
    [TestMethod]
    public async Task DeferredActivationUnderSustainedLoad()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
            // the drain finishing. Either the advancer or the CommitInFlightItem path must activate.
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

        // Activation is contract-conditional under the in-flight completion race.
        // Items that complete synchronously inside the deferred path are not required to be activated.
        // Functional correctness: every item completes without exception, depth returns to 0.
        foreach (var item in allItems)
        {
            await item.WaitForCompleteAsync();
            Assert.IsNull(item.Exception);
        }

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// THE invariant (fixed 2026-07-08): every activation the executor's own dispatch issues -
    /// inline elision or a self-resolved handoff - must pass preferAsync:false. It precedes
    /// ExecuteItemAsync on the very next line of the same dispatch, so nothing has suspended yet
    /// and there is no continuation to defer; only an advancer activation legitimately passes true.
    /// The regression fed a downstream bug where the advancer runs a synchronous item's body
    /// inline while another thread spins waiting on it.
    ///
    /// Executor-ness is certified by construction, not inference: the pipeline is built with
    /// runContinuationsAsynchronously:false, so enqueueing into a parked executor runs the whole
    /// drive inline within the test's own bracketed Enqueue().Execute() call - any item pushed
    /// into a synchronously-driven source is by definition dispatched on the driving thread.
    /// BeforeExecute splits the dispatch activation (asserted) from the post-execute commit walk
    /// within the same drive (a legitimate true). The self-resolved-handoff case needs the sole
    /// waiter's retirement to land inside the dispatch's publish-to-recheck window; the spin
    /// rendezvous puts both racers within nanoseconds of each other, which is what makes that
    /// window reachable at all from an inline drive (a bare Task.Run's µs-scale start latency
    /// never lands in it) - validated by flipping the self-act call site to true: 8/8 runs failed.
    [TestMethod]
    public async Task InlineActivation_NeverPrefersAsync()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true), runContinuationsAsynchronously: false);
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        static void EnqueueUnderExecutorDrive(UnboundedPipeline<TestPipelineItem, TestPipelinePolicy> pipeline, TestPipelineItem item)
        {
            TestPipelineItem.ExecutorDriveActive = true;
            try { pipeline.Enqueue(item).Execute(); }
            finally { TestPipelineItem.ExecutorDriveActive = false; }
        }

        const int rounds = 1000;
        var allItems = new List<TestPipelineItem>();

        for (var r = 0; r < rounds; r++)
        {
            var waiter = new TestPipelineItem { CompleteAsync = true };
            EnqueueUnderExecutorDrive(pipeline, waiter);
            allItems.Add(waiter);
            await waiter.WaitForExecutedAsync();

            var ready = 0;
            var go = 0;
            var drainTask = Task.Run(() =>
            {
                Volatile.Write(ref ready, 1);
                while (Volatile.Read(ref go) == 0)
                    Thread.SpinWait(1);
                waiter.CompletePipelineTask();
            });
            while (Volatile.Read(ref ready) == 0)
                Thread.SpinWait(1);
            Volatile.Write(ref go, 1);

            var tail = new TestPipelineItem { CompleteAsync = true };
            EnqueueUnderExecutorDrive(pipeline, tail);
            allItems.Add(tail);
            await tail.WaitForExecutedAsync();
            tail.CompletePipelineTask();

            await drainTask;
            await tail.WaitForCompleteAsync();
            await waiter.WaitForCompleteAsync();
        }

        var executorDispatchActivations = 0;
        foreach (var item in allItems)
        {
            if (item is { FirstActivationUnderExecutorDrive: true, FirstActivationBeforeExecute: true })
            {
                executorDispatchActivations++;
                Assert.IsFalse(item.FirstActivationPreferAsync,
                    $"{item}: activated by the executor's own dispatch (inside the test's drive, before the item's execute) - no continuation exists to defer, preferAsync must be false.");
            }
        }

        Assert.IsTrue(executorDispatchActivations > 0,
            "No executor-dispatch activation was observed - the assertion above ran vacuously.");

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// Regression guard for the deferred-activation handoff. When the advancer claims the
    /// published _executingItem and calls ActivateHeadItem on its own thread, the executor's
    /// RetireItem for the same item must not fire until ActivateHeadItem has finished.
    /// The _activationLock around the advancer's claim and ClearExecutingItem's fence in the
    /// deferred branch enforce this. Without that ordering a pooling policy could observe
    /// CompleteItem while ActivateHeadItem is still running on another thread, or even after
    /// Complete has returned the item to its pool.
    [TestMethod]
    public async Task DeferredActivation_CompleteWaitsForConcurrentActivate()
    {
        var pipeline = Pipeline.Create<ActivationOrderingItem, ActivationOrderingPolicy>(new());
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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
        // to RetireItem(B). Without the activation lock's fence in ClearExecutingItem,
        // CompleteItem would fire while ActivateHeadItem is still sleeping.
        b.CompleteExecuteTask();

        await a.WaitForCompleteAsync();
        await b.WaitForCompleteAsync();

        Assert.IsTrue(
            b.ActivationFinishedBeforeComplete,
            "CompleteItem fired before ActivateHeadItem finished, deferred-activation race not closed.");
    }

    /// Shutdown cancellation does not release a failed executor from an empty-edge activation that
    /// already claimed its item. The activation call owns that handoff until it returns.
    [TestMethod]
    public async Task DeferredActivation_ShutdownFailureWaitsForConcurrentActivate()
    {
        var pipeline = Pipeline.Create<ActivationOrderingItem, ActivationOrderingPolicy>(new());
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var resident = new ActivationOrderingItem { PipelineTaskAsync = true };
        pipeline.Enqueue(resident).Execute();
        await resident.WaitForExecutedAsync();

        var failed = new ActivationOrderingItem { ExecuteAsync = true };
        pipeline.Enqueue(failed).Execute();
        await failed.WaitForExecutedAsync();

        var shutdown = pipeline.CompleteAsync();
        resident.CompletePipelineTask();
        failed.WaitForActivationStart();
        failed.FailExecuteTask(new InvalidOperationException("execute failure during shutdown"));

        await resident.WaitForCompleteAsync();
        await failed.WaitForCompleteAsync();
        await shutdown;

        Assert.IsTrue(
            failed.ActivationFinishedBeforeComplete,
            "Shutdown let recovery retire the item before its claimed activation returned.");
    }

    /// Regression guard for the CommitPendingTail activation-lock fence. CommitPendingTail's
    /// Exchange(_executingItemActivationPending, false) handshake mirrors ClearExecutingItem's, but if Exchange
    /// returns false (advancer already claimed and is mid-ActivateHeadItem under _activationLock)
    /// CommitPendingTail must wait for that activation to finish before calling RetireItem on
    /// the same item. Without the fence, CompleteItem fires while ActivateHeadItem is still
    /// running on the advancer thread.
    [TestMethod]
    public async Task TailCommit_CompleteWaitsForConcurrentActivate()
    {
        var pipeline = Pipeline.Create<ActivationOrderingItem, ActivationOrderingPolicy>(new());
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // A: async-pipeline waiter. When A's pipeline completes, advancer drains and claims B.
        var a = new ActivationOrderingItem { PipelineTaskAsync = true };
        pipeline.Enqueue(a).Execute();
        await a.WaitForExecutedAsync();

        // B: async pipeline + async trailing. Goes through deferred path (A is waiter, count>0)
        // and suspends the executor at await trailing. This is the key difference from the
        // ClearExecutingItem test - B's pipeline is pending so it becomes _pendingTail, and
        // tail-commit (not inline ClearExecutingItem) is the path that races the advancer.
        var b = new ActivationOrderingItem { PipelineTaskAsync = true, HasTrailingTaskAsync = true };
        pipeline.Enqueue(b).Execute();
        await b.WaitForExecutedAsync();

        // Fire the advancer: drain A, claim B (deferred), start slow ActivateHeadItem(B).
        a.CompletePipelineTask();
        b.WaitForActivationStart();

        // Pre-fire B's pipeline so it's already completed when the executor reaches CommitPendingTail.
        b.CompletePipelineTask();

        // Fire B's trailing task. Executor resumes from await trailing, falls out of inner loop,
        // hits CommitPendingTail at the pre-idle commit site. Without the fence, RetireItem(B)
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
        readonly TaskCompletionSource _executedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _completedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _activationStartedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _pipelineTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource _trailingTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly TaskCompletionSource<PipelineItemResult> _executeTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int _activationFinished; // 0 = not finished, 1 = finished. Volatile semantics via Interlocked.
        int _completeActivationState; // captured value of _activationFinished at CompleteItem time.

        public bool ExecuteAsync { get; init; }
        public bool PipelineTaskAsync { get; init; }
        public bool HasTrailingTaskAsync { get; init; }

        public bool ActivationFinishedBeforeComplete => _completeActivationState == 1;

        public async Task WaitForExecutedAsync()
        {
            try { await _executedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false); }
            catch (TimeoutException) { Assert.Fail("Timed out waiting for execute."); }
        }

        public async Task WaitForCompleteAsync()
        {
            try { await _completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false); }
            catch (TimeoutException) { Assert.Fail("Timed out waiting for complete."); }
        }

        public void WaitForActivationStart() =>
            Assert.IsTrue(_activationStartedTcs.Task.Wait(TimeSpan.FromSeconds(10)), "Timed out waiting for activation start.");

        public void CompletePipelineTask() => _pipelineTaskTcs.SetResult();
        public void CompleteTrailingTask() => _trailingTaskTcs.SetResult();
        public void CompleteExecuteTask() => _executeTaskTcs.SetResult(new PipelineItemResult(GetTrailingTask(), GetPipelineTask()));
        public void FailExecuteTask(Exception exception) => _executeTaskTcs.SetException(exception);

        ValueTask GetPipelineTask() => PipelineTaskAsync ? new(_pipelineTaskTcs.Task) : default;
        ValueTask GetTrailingTask() => HasTrailingTaskAsync ? new(_trailingTaskTcs.Task) : default;

        internal ValueTask<PipelineItemResult> RunExecute()
        {
            _executedTcs.TrySetResult();
            return ExecuteAsync ? new(_executeTaskTcs.Task) : new(new PipelineItemResult(GetTrailingTask(), GetPipelineTask()));
        }

        internal void RunActivate()
        {
            _activationStartedTcs.TrySetResult();
            // Slow activation: gives the executor time to race toward RetireItem.
            Thread.Sleep(50);
            Volatile.Write(ref _activationFinished, 1);
        }

        internal void RunComplete()
        {
            // Snapshot whether activation has finished AT THIS MOMENT. Without the fix, this is
            // racy and will commonly observe 0 while activation is still sleeping.
            _completeActivationState = Volatile.Read(ref _activationFinished);
            _completedTcs.TrySetResult();
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    /// Three-way stress: producer enqueues, executor processes items with faulting pipeline
    /// tasks (triggering recovery via the advancer), recovery continuations fire and coordinate
    /// with the activation lock. Exercises the interleaving of executor + advancer + recovery
    /// continuation paths against the same _executingItemActivationPending / _drainTcs / in-flight recovery
    /// state. Stress test - won't deterministically hit every interleaving but catches
    /// regressions under load.
    [TestMethod]
    public async Task ThreeWayRace_ProducerExecutorRecoveryContinuations()
    {
        // Recovery factory only handles PipelineTask (advancer path). Sync on the source
        // onIdle hook so all items are committed to _inFlight with callbacks registered before we
        // fault, otherwise the trailing filler at _pendingTail could take the executor's
        // CommitPendingTail path and skew the routing this test wants to stress.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true,
                ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask
                    ? new TestPipelineItem()
                    : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
    }

    sealed class ReentrantBox
    {
        public Action<TestPipelineItem>? OnComplete;
    }

    struct ReentrantPolicy(ReentrantBox box) : IPipelinePolicy<TestPipelineItem>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
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
        public ValueTask<PipelineItemResult> ExecuteItemAsync(ActivationOrderingItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken) => item.RunExecute();
        public void ActivateHeadItem(ActivationOrderingItem item, bool preferAsync = true) => item.RunActivate();
        public void CompleteItem(ActivationOrderingItem item, int remainingDepth, Exception? exception) => item.RunComplete();
        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, ActivationOrderingItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out ActivationOrderingItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    /// Depth-accounting guard: no negative remainingDepth ever surfaces to policy.CompleteItem under
    /// a tight enqueue burst. Historically this hunted a PRODUCER-side race - Enqueue published the
    /// item then IncrementDepth, so the executor's TryDequeue could land between and decrement before
    /// the increment (negative depth; OnDepthReachedZero observed at -1 instead of 0, hanging
    /// WaitForEmptyAsync). That window is now structurally closed: depth is incremented at DISPATCH by
    /// the single-consumer executor (commit b0e45260), so Enqueue no longer touches depth and there is
    /// no producer race to fish for. What remains is the deterministic dispatch-increment /
    /// completion-decrement accounting, identical per item - a small burst exercises it fully (the old
    /// 1M-iteration brute force only re-ran the same single-consumer loop).
    [TestMethod]
    public async Task Enqueue_TightBurst_RemainingDepthNeverNegative()
    {
        var observed = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var pipeline = Pipeline.Create<TestPipelineItem, DepthRecordingPolicy>(
            new DepthRecordingPolicy(observed));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // A tight synchronous enqueue burst maximizes executor-wakeup churn (~130us/item here, far
        // pricier than a lock-batched enqueue), so a few hundred items already exercises the dispatch-
        // increment / completion-decrement accounting fully. (Was 1M, which only re-ran the same
        // single-consumer loop now that the producer-side depth race is structurally closed.)
        const int n = 500;
        for (var i = 0; i < n; i++)
            pipeline.Enqueue(new TestPipelineItem()).Execute();

        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));

        Assert.IsTrue(observed.Count >= n, $"expected a CompleteItem depth observation per item, got {observed.Count} for {n}.");
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
        // We deliberately only handle the advancer's PipelineTask kind. The test must avoid
        // the racy executor-side trailing-recovery path (RecoverCommittedPendingTailAsync), which
        // fires kind=PipelineTask when CommitPendingTail sees a pre-faulted tail task. Sync on the
        // source onIdle hook to guarantee the executor has done its pre-idle commit (faulting moved
        // to _inFlight with callback registered) before we fault the task.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // In-flight item with a pipeline task that will fault, triggering recovery.
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

    [TestMethod, DoNotParallelize]
    public async Task RecoveryCompletionRacingShutdown_RetiresPositionOnce()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var i = 0; i < iterations; i++)
        {
            await RaceAsync(executePending: true, i);
            await RaceAsync(executePending: false, i);
        }

        static async Task RaceAsync(bool executePending, int iteration)
        {
            var recovery = executePending
                ? new TestPipelineItem { Name = $"execute-{iteration}", ExecuteAsync = true }
                : new TestPipelineItem { Name = $"trailing-{iteration}", HasTrailingTask = true, CompleteAsync = true };
            var idle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
                new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null),
                onIdle: _ => { idle.TrySetResult(); return default; });
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

            var failed = new TestPipelineItem
            {
                CompleteAsync = true,
                PipelineTaskException = new InvalidOperationException("fault"),
            };
            pipeline.Enqueue(failed).Execute();
            await idle.Task.WaitAsync(TimeSpan.FromSeconds(5));
            failed.CompletePipelineTask();
            await recovery.WaitForExecutedAsync();

            using var start = new ManualResetEventSlim();
            var shutdown = Task.Run(async () =>
            {
                start.Wait();
                await pipeline.CompleteAsync();
            });
            var settle = Task.Run(() =>
            {
                start.Wait();
                if (executePending)
                    recovery.CompleteExecuteTask();
                else
                    recovery.CompleteTrailingTask();
            });

            start.Set();
            await Task.WhenAll(shutdown, settle).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.IsTrue(recovery.IsCompleted, $"iteration {iteration}: recovery did not complete.");
            PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
        }
    }

    /// Multiple WaitForEmptyAsync callers must all observe the drain. Exercises the drain TCS
    /// reuse: the first caller creates the TCS, subsequent callers attach to the same task.
    [TestMethod]
    public async Task MultipleConcurrentWaitForIdleCallers()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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
    /// new one created on next call). Exercises the SetResult+null pattern in RetireItem
    /// and the lazy creation in WaitForEmptyAsync.
    [TestMethod]
    public async Task WaitForEmptyAsync_AcrossRepeatedDrainCycles()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
    /// via the shutdown token (the item-side escalation contract), so the drained item sees
    /// the cancellation, not a CompleteAsync-supplied exception (that propagation was the
    /// retired forceful-sweep contract).
    [TestMethod]
    public async Task CompleteAsync_ConcurrentFromMultipleThreads_FirstWriterWins()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
            "Drained item settles via the shutdown token (item-side escalation), not a CompleteAsync-supplied exception.");
    }

    /// WaitForEmptyAsync caller is suspended when CompleteAsync fires concurrently. The drain must
    /// complete the WaitForEmptyAsync TCS even though the trigger was CompleteAsync rather than
    /// natural depth-to-zero. (RetireItem drives the drain TCS, CompleteAsync's drain calls
    /// RetireItem for each remaining item.)
    [TestMethod]
    public async Task WaitForEmptyAsync_RacingCompleteAsync_AlwaysCompletes()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        var idleTask = pipeline.WaitForEmptyAsync().AsTask();
        Assert.IsFalse(idleTask.IsCompleted);

        var completeTask = pipeline.CompleteAsync().AsTask();

        await Task.WhenAll(idleTask, completeTask).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(idleTask.IsCompletedSuccessfully, "WaitForEmptyAsync caller must observe the drain.");
    }

    /// In-proc stress runner for the scenario above (one observed field timeout, not yet
    /// reproduced in 40 contended suite runs). Iterations via DRAGHI_STRESS_ITERATIONS (default
    /// 200, keeps suite cost ~sub-second). On a hang it reports WHICH task was stuck, the
    /// localizing fact a WhenAll timeout hides.
    [TestMethod, DoNotParallelize]
    public async Task WaitForEmptyAsync_RacingCompleteAsync_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var iter = 0; iter < iterations; iter++)
        {
            var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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

    /// In-proc stress runner for the DrainSlotInline shutdown deposit-drop. The shape: two
    /// CompleteAsync waiters, then CompleteAsync. The strand needs item1 drained via the slot (its
    /// callback claims it pre-escalation) while item2's CommitPendingTail escalation is in flight, so
    /// CompleteAsync must land in the narrow window before the executor commits item2. Rare per
    /// iteration, so the loop rolls it. Iterations via DRAGHI_STRESS_ITERATIONS (default 200).
    /// DRAGHI_STRESS_WAIT_ON_HANG=1 suspends after a GC shed for live capture.
    [TestMethod, DoNotParallelize]
    public async Task CompleteAsyncDrainsInFlightItems_ShutdownDepositDrop_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var iter = 0; iter < iterations; iter++)
        {
            var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
            var item1 = new TestPipelineItem { CompleteAsync = true };
            var item2 = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(item1).Execute();
            pipeline.Enqueue(item2).Execute();
            await item1.WaitForExecutedAsync();
            await item2.WaitForExecutedAsync();

            var completeTask = pipeline.CompleteAsync().AsTask();

            try
            {
                await completeTask.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (TimeoutException)
            {
                var diagnosis = $"iter {iter}: hang - completeTask={completeTask.Status}, " +
                    $"item1 completed={item1.IsCompleted}, item2 completed={item2.IsCompleted}, depth={pipeline.Depth}";
                if (Environment.GetEnvironmentVariable("DRAGHI_STRESS_WAIT_ON_HANG") == "1")
                {
                    GC.Collect(2, GCCollectionMode.Forced);
                    GC.WaitForPendingFinalizers();
                    GC.Collect(2, GCCollectionMode.Forced);
                    Console.WriteLine($"SUSPENDED: {diagnosis}");
                    await Task.Delay(Timeout.Infinite);
                }
                Assert.Fail(diagnosis);
            }

            Assert.IsTrue(item1.IsCompleted, $"iter {iter}: item1 not completed");
            Assert.IsTrue(item2.IsCompleted, $"iter {iter}: item2 not completed");
            Assert.AreEqual(0, pipeline.Depth, $"iter {iter}: depth not zero");
        }
    }

    /// In-proc stress runner for DeferredActivationUnderSustainedLoad (one observed field
    /// timeout: the tail item executed and its pipeline task was completed, but item
    /// completion never fired. 25 isolated runs + 24 contended suite runs clean).
    /// Rolls the drain-vs-tail-enqueue race per iteration with a minimal pile so the tail's
    /// enqueue still sees waiters present (the deferred-activation branch). Iterations via
    /// DRAGHI_STRESS_ITERATIONS (default 200); on a hang it reports WHICH stage stuck plus
    /// every item's executed/completed state - the localizing facts a bare timeout hides.
    /// DRAGHI_STRESS_WAIT_ON_HANG=1 suspends (after a GC shed) for live dump capture.
    [TestMethod, DoNotParallelize]
    public async Task DeferredActivationUnderSustainedLoad_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;
        const int pileSize = 4;
        var stageTimeout = TimeSpan.FromSeconds(5);

        for (var iter = 0; iter < iterations; iter++)
        {
            var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
            var pile = new TestPipelineItem[pileSize];
            for (var i = 0; i < pileSize; i++)
            {
                pile[i] = new TestPipelineItem { CompleteAsync = true };
                pipeline.Enqueue(pile[i]).Execute();
            }
            foreach (var item in pile)
            {
                if (!await item.TryWaitForExecutedAsync(stageTimeout))
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

            if (!await tail.TryWaitForExecutedAsync(stageTimeout))
                await HangAsync(iter, "tail-executed", pipeline, pile, tail);

            tail.CompletePipelineTask();
            await drainTask;

            if (!await tail.TryWaitForCompletedAsync(stageTimeout))
                await HangAsync(iter, "tail-completed", pipeline, pile, tail);

            foreach (var item in pile)
            {
                if (!await item.TryWaitForCompletedAsync(stageTimeout))
                    await HangAsync(iter, "pile-completed", pipeline, pile, tail);
            }
        }

        static async Task HangAsync(
            int iter, string stage,
            UnboundedPipeline<TestPipelineItem, TestPipelinePolicy> pipeline,
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
    [DoNotParallelize]
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        PushAndDrop(pipeline, ref itemRef);

        await idleEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsNotNull(itemRef);
        // idleEntered fires at the TOP of onIdle, BEFORE the `await idleCanReturn` suspends the
        // executor - transient stack rooting on the executor thread may still pin the just-processed
        // item for a few ms. AssertDiesAsync's bounded retry rides out the suspension lag; a real
        // retention regression never dies and still fails the assert.
        await AssertDiesAsync(itemRef,
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

    /// Regression guard for the _pendingTail slot clear in CommitPendingTail (Pipeline.cs:508-509).
    /// An item that goes through the async-pipeline branch lands in _pendingTail, then gets
    /// committed to _inFlight on the pre-idle CommitPendingTail. After commit + drain, nothing in
    /// the pipeline should pin the item. Without the line 508 `_pendingTail = default!` clear, the
    /// completed item stays GC-rooted for the entire idle period.
    [TestMethod]
    [DoNotParallelize]
    public async Task Idle_PendingTailSlotDoesNotLeakCompletedItem()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        WeakReference? itemRef = null;
        Action? completer = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EnqueuePendingTailItem(pipeline, completed, ref itemRef, ref completer);

        // Wait for executor to suspend (pre-idle CommitPendingTail has run, item is in _inFlight).
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
            "Item still alive after pipeline drained — committed tail not cleared (_pendingTail / _executingItem).");
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
    static void EnqueuePendingTailItem(ObservablePipeline<TestPipelineItem, TestPipelinePolicy> pipeline, TaskCompletionSource completed, ref WeakReference? itemRef, ref Action? completer)
    {
        var item = new TestPipelineItem { CompleteAsync = true, OnComplete = completed.SetResult };
        itemRef = new WeakReference(item);
        completer = item.CompletePipelineTask;
        pipeline.Enqueue(item).Execute();
    }

    /// Regression guard for the _executingItem slot clear in ClearExecutingItem (Pipeline.cs:1038-1039).
    /// An item dequeued with waiters present takes the deferred-publish path
    /// (_executingItem=item, _executingItemActivationPending=true). On sync-success completion, ClearExecutingItem
    /// must Exchange _executingItemActivationPending=false AND clear _executingItem when the executor wins the race.
    /// Without the clear, the completed item stays GC-rooted for the entire idle period.
    [TestMethod]
    [DoNotParallelize]
    public async Task Idle_ExecutingItemSlotDoesNotLeakDeferredItem()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true), onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        WeakReference? waiterRef = null;
        WeakReference? deferredRef = null;
        Action? waiterCompleter = null;
        var waiterCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        EnqueueDeferredScenario(pipeline, waiterCompleted, ref waiterRef, ref deferredRef, ref waiterCompleter);

        // Wait for executor to suspend. By this point: waiter is in _inFlight, deferred item has been
        // dequeued through the deferred-publish path, sync-completed, ClearExecutingItem ran.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Drain the waiter so it leaves _inFlight too, then drop the completer ref. The barrier
        // TCS lives in the OnComplete hook, which never roots the item.
        waiterCompleter!();
        waiterCompleter = null;
        await waiterCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsNotNull(waiterRef);
        await AssertDiesAsync(waiterRef, "In-flight item leaked after drain.");
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
        // The async-pipeline item lands in _inFlight first. The next item then takes the deferred path
        // because waiterQueueCount > 0 when it's dequeued.
        var waiter = new TestPipelineItem { CompleteAsync = true, OnComplete = waiterCompleted.SetResult };
        var deferred = new TestPipelineItem();  // sync success, hits ClearExecutingItem's race-win
        waiterRef = new WeakReference(waiter);
        deferredRef = new WeakReference(deferred);
        waiterCompleter = waiter.CompletePipelineTask;
        pipeline.Enqueue(waiter).Execute();
        pipeline.Enqueue(deferred).Execute();
    }

    /// Regression guard for RecoverInFlightItemResult's async-trailing continuation bailout
    /// (Pipeline.cs:871-895). When recovery's executeTask completes sync but its trailing task is
    /// async-pending, the continuation hooked at line 871 must observe wake completion and call
    /// BailoutRecoveryOnShutdown. Without that branch the recovery would be stranded with the
    /// advancer flag still held.
    [TestMethod]
    public async Task InFlightRecovery_AsyncTrailingDuringShutdown_BailsOutCleanly()
    {
        // Recovery: sync execute (ExecuteAsync default = false), async trailing (HasTrailingTask),
        // pending pipeline task. Enters RecoverInFlightItemResult's async-trailing branch.
        var recovery = new TestPipelineItem { HasTrailingTask = true, CompleteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("waiter fault"),
        };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault → advancer → RecoverInFlightItem → recovery executes sync → RecoverInFlightItemResult hooks
        // a continuation on the async trailing task. Advancer stays held until continuation fires.
        item.CompletePipelineTask();
        await recovery.WaitForExecutedAsync();

        // CompleteAsync while the trailing-task continuation is still pending.
        var completeTask = pipeline.CompleteAsync().AsTask();
        await Task.Delay(50);  // let drain reach the advancer-idle spin

        // Fire trailing. Continuation runs, observes _wakeSignal.IsCompleted, takes the bailout
        // branch which releases _advancing and signals advancer-idle. Without the branch the
        // continuation would CommitInFlightItem against a completed wake and strand the recovery.
        recovery.CompleteTrailingTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(recovery.IsCompleted, "Recovery must be completed via the bailout path.");
    }

    /// Regression guard for the `_inFlightRecoveryItem` reference leak. RecoverInFlightItem sets
    /// `_inFlightRecoveryItem = recoveryItem` at the async-execute publish site. After successful
    /// recovery, AdvanceAndDrainRecovery flips `in-flight recovery=false` but used to leave
    /// `_inFlightRecoveryItem` pointing at the now-completed item. For long-lived pipelines doing
    /// rare recoveries this is one stale strong reference, observable via WeakReference after GC.
    [TestMethod]
    [DoNotParallelize]
    public async Task RecoverInFlightItem_ClearsInFlightRecoveryItemReferenceAfterSuccess()
    {
        WeakReference? recoveryRef = null;
        TestPipelineItem? heldRecovery = null;

        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true,
                ctx =>
                {
                    if (ctx.Kind == PipelineItemFailureKind.PipelineTask)
                    {
                        var r = new TestPipelineItem { ExecuteAsync = true };
                        recoveryRef = new WeakReference(r);
                        heldRecovery = r;
                        return r;
                    }
                    return null;
                }),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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

        Assert.IsNotNull(recoveryRef);
        await AssertDiesAsync(recoveryRef, "Recovery item still alive after completion.");
    }

    struct DepthRecordingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly System.Collections.Concurrent.ConcurrentQueue<int> _observed;

        public DepthRecordingPolicy(System.Collections.Concurrent.ConcurrentQueue<int> observed)
        {
            _observed = observed;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
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

    /// Stress test targeting the cancel-during-arm window in TestObservableQueueSource's
    /// WaitForNextAsync. The executor parks at
    /// `await wakeSignal.Arm()` after _pending=TRUE has been set under the wake lock; if the
    /// source CancellationToken fires while the state machine is in the microsecond window
    /// between Arm() and the await machinery's OnCompleted call, the state machine can
    /// unwind without invoking WaitOnCompleted - _pending leaks at TRUE with
    /// _waitContinuation NULL. A subsequent Signal claims _pending and dispatches a null
    /// delegate (NRE), or the wake silently routes nowhere (lost wake / hang).
    ///
    /// The race is exclusively probabilistic: any deterministic sync (waiting for idle TCS,
    /// WaitForExecuted, etc.) puts the executor PAST the synchronous arm-vs-register
    /// window, hiding it. Per-iteration shape is therefore: race enqueue + cancel from two
    /// threads with NO synchronization between them, staggered spin offsets to walk timing
    /// across iterations. High iteration count compensates for the tight window.
    ///
    /// Iterations via DRAGHI_STRESS_ITERATIONS (default 200; bump for soak runs).
    /// DRAGHI_STRESS_WAIT_ON_HANG=1 suspends (after a GC shed) for live dump capture
    /// instead of failing on the first hit.
    [TestMethod, DoNotParallelize]
    public async Task CancelDuringArm_NoCorruption_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var iter = 0; iter < iterations; iter++)
        {
            using var cts = new CancellationTokenSource();
            var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
                new(true), cancellationToken: cts.Token);
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

            // Two competing threads, no sync between them. The executor's first
            // WaitForNextAsync iteration is the prime target: it races the enqueue
            // (which signals) and the cancel (which signals via Complete).
            var enqueueTask = Task.Run(() =>
            {
                for (var i = 0; i < 5; i++)
                {
                    try { pipeline.Enqueue(new TestPipelineItem()).Execute(); }
                    catch (InvalidOperationException) { return; } // source completed
                }
            });

            var spinTarget = (iter * 13) % 128;
            var cancelTask = Task.Run(() =>
            {
                for (var s = 0; s < spinTarget; s++)
                    Thread.SpinWait(8);
                cts.Cancel();
            });

            try { await Task.WhenAll(enqueueTask, cancelTask).WaitAsync(TimeSpan.FromSeconds(5)); }
            catch (TimeoutException)
            {
                Assert.Fail($"iter {iter}: enqueue/cancel race did not settle in 5s - " +
                    "SourceWakeEvent likely wedged with stale _pending");
            }

            string? hangDiagnosis = null;
            try
            {
                await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
                // Token cancellation racing the drain is expected.
            }
            catch (TimeoutException)
            {
                hangDiagnosis = $"iter {iter}: CompleteAsync hung after cancel. " +
                    "Lost wake or corrupted _pending in SourceWakeEvent.";
            }
            catch (NullReferenceException ex)
            {
                hangDiagnosis = $"iter {iter}: NullReferenceException - DispatchClaimed " +
                    $"dereferenced a null _waitContinuation. {ex.Message}\n{ex.StackTrace}";
            }

            if (hangDiagnosis is not null)
            {
                if (Environment.GetEnvironmentVariable("DRAGHI_STRESS_WAIT_ON_HANG") == "1")
                {
                    Console.WriteLine($"SUSPENDED iter {iter}: {hangDiagnosis}");
                    await Task.Delay(Timeout.Infinite);
                    // The post-await reference forces the compiler to hoist `pipeline` into the
                    // state machine box, which the Task.Delay continuation roots for the duration
                    // of the suspend. The call itself never runs - the presence of the reference
                    // past the await is the load-bearing thing. A GC.KeepAlive BEFORE the await
                    // is just a barrier at that line and lets `pipeline` go after.
                    GC.KeepAlive(pipeline);
                }
                Assert.Fail(hangDiagnosis);
            }
        }
    }
}
