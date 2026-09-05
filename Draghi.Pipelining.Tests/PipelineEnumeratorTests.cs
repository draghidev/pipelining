namespace Draghi.Pipelining.Tests;

using Draghi.Pipelining.Internal;
using System.Reflection;
using System.Threading.Tasks.Sources;

[TestClass]
public class PipelineEnumeratorTests
{
    // await-foreach compat lives in PipelineSourceAsyncEnumerable, not on the source enumerator (the
    // pipeline drives the TryGetNext/WaitForNextAsync pull directly). Verifies the adapter binds and
    // yields the source's items in order.
    [TestMethod]
    public async Task AwaitForeach_OverAdapter_YieldsItems()
    {
        var source = UnboundedQueueSource<int>.Create();
        source.Enqueue(1);
        source.Enqueue(2);
        source.Enqueue(3);

        using var cts = new CancellationTokenSource();

        var adapter = new PipelineSourceAsyncEnumerable<int, UnboundedQueueSource<int>, UnboundedQueueSource<int>.Enumerator>(source);
        var observed = new List<int>();
        // The source is never completed. Cancel after the third item so the enumerator's token
        // registration completes the wake signal and the loop exits instead of parking forever.
        await foreach (var item in adapter.WithCancellation(cts.Token))
        {
            observed.Add(item);
            if (observed.Count == 3)
                cts.Cancel();
        }

        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, observed);
    }

    [TestMethod]
    public void Empty_YieldsNothing()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        var observed = new List<TestPipelineItem>();
        foreach (var item in pipeline)
            observed.Add(item);

        Assert.AreEqual(0, observed.Count);
    }

    [TestMethod]
    public async Task PendingInFlightItems_YieldsAllInEnqueueOrder()
    {
        // Items are CompleteAsync, so depth never reaches 0 - use the source onIdle hook as the
        // executor-at-rest signal (CommitPendingTail's transit window must be settled before enum).
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int count = 5;
        var enqueued = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            enqueued[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(enqueued[i]).Signal();
        }

        // Wait for all items to enter the in-flight state (executor pulled them, awaiting pipeline task).
        for (var i = 0; i < count; i++)
            await enqueued[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var observed = new List<TestPipelineItem>();
        foreach (var item in pipeline)
            observed.Add(item);

        Assert.AreEqual(count, observed.Count, "Enumerator should yield every in-flight item.");
        for (var i = 0; i < count; i++)
            Assert.AreSame(enqueued[i], observed[i], $"Item at position {i} should match enqueue order.");

        // Drain.
        for (var i = 0; i < count; i++)
            enqueued[i].CompletePipelineTask();
        for (var i = 0; i < count; i++)
            await enqueued[i].WaitForCompleteAsync();
    }

    [TestMethod]
    public async Task FrontierEnumerator_StableStore_ReportsCapturedPositions()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int count = 5;
        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { Name = $"frontier-{i}", CompleteAsync = true };
            pipeline.Enqueue(items[i]).Signal();
        }
        for (var i = 0; i < count; i++)
            await items[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var enumerator = pipeline.Pipeline.GetEnumerator(out var frontier);
        Assert.AreEqual((uint)count, frontier.DispatchedThrough - frontier.RetiredAtCapture);
        for (var i = 0; i < count; i++)
        {
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual(frontier.RetiredAtCapture + i + 1, enumerator.Position);
            Assert.AreSame(items[i], enumerator.Current);
        }
        Assert.IsFalse(enumerator.MoveNext());

        foreach (var item in items)
            item.CompletePipelineTask();
        foreach (var item in items)
            await item.WaitForCompleteAsync();
    }

    [TestMethod]
    public async Task FrontierEnumerator_RetirementAndLaterDispatch_NeverOvershootsCapturedHighWater()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int initialCount = 8;
        var initial = new TestPipelineItem[initialCount];
        for (var i = 0; i < initial.Length; i++)
        {
            initial[i] = new TestPipelineItem { Name = $"initial-{i}", CompleteAsync = true };
            pipeline.Enqueue(initial[i]).Signal();
        }
        for (var i = 0; i < initial.Length; i++)
            await initial[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var enumerator = pipeline.Pipeline.GetEnumerator(out var frontier);

        for (var i = 0; i < initial.Length / 2; i++)
        {
            initial[i].CompletePipelineTask();
            await initial[i].WaitForCompleteAsync();
        }

        // Dispatch enough successors to reuse queue storage and grow past the captured tail.
        var later = new TestPipelineItem[32];
        for (var i = 0; i < later.Length; i++)
        {
            later[i] = new TestPipelineItem { Name = $"later-{i}", CompleteAsync = true };
            pipeline.Enqueue(later[i]).Signal();
        }
        for (var i = 0; i < later.Length; i++)
            await later[i].WaitForExecutedAsync();

        while (enumerator.MoveNext())
        {
            Assert.IsTrue(
                frontier.DispatchedThrough >= enumerator.Position,
                $"Position {enumerator.Position} crossed captured high-water {frontier.DispatchedThrough}.");
            Assert.IsFalse(later.Contains(enumerator.Current),
                $"Post-frontier {enumerator.Current.Name} filled position {enumerator.Position} " +
                $"under high-water {frontier.DispatchedThrough}, retired-through {frontier.RetiredThrough}.");
        }

        for (var i = initial.Length / 2; i < initial.Length; i++)
            initial[i].CompletePipelineTask();
        foreach (var item in later)
            item.CompletePipelineTask();
        for (var i = initial.Length / 2; i < initial.Length; i++)
            await initial[i].WaitForCompleteAsync();
        foreach (var item in later)
            await item.WaitForCompleteAsync();
    }

    [TestMethod]
    public async Task FrontierEnumerator_RemovedBeforeRetirement_TrimsPostFrontierTail()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        var blockingSource = new BlockingGetResultSource();
        var first = new TestPipelineItem
        {
            Name = "captured-first",
            PipelineTaskSource = blockingSource
        };
        var second = new TestPipelineItem { Name = "captured-second", CompleteAsync = true };
        pipeline.Enqueue(first).Signal();
        pipeline.Enqueue(second).Signal();
        await first.WaitForExecutedAsync();
        await second.WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var frontier = pipeline.Pipeline.CaptureEnumerationFrontier();
        blockingSource.Complete();
        await blockingSource.GetResultEntered.WaitAsync(TimeSpan.FromSeconds(5));

        var later = new TestPipelineItem { Name = "post-frontier", CompleteAsync = true };
        try
        {
            pipeline.Enqueue(later).Signal();
            await later.WaitForExecutedAsync();

            var enumerator = pipeline.Pipeline.GetEnumerator(frontier);
            var observed = new List<TestPipelineItem>();
            while (enumerator.MoveNext())
                observed.Add(enumerator.Current);

            Assert.IsFalse(observed.Contains(later),
                "A head removed before its retirement publication must shorten the captured cohort, " +
                "not admit a post-frontier successor into its position.");
            CollectionAssert.Contains(observed, second);
        }
        finally
        {
            // Never strand the advancer thread if an assertion above fails.
            blockingSource.ReleaseGetResult();
            second.CompletePipelineTask();
            later.CompletePipelineTask();
            await first.WaitForCompleteAsync();
            await second.WaitForCompleteAsync();
            await later.WaitForCompleteAsync();
        }
    }

    sealed class BlockingGetResultSource : IValueTaskSource
    {
        ManualResetValueTaskSourceCore<bool> _core = new()
        {
            RunContinuationsAsynchronously = true
        };
        readonly TaskCompletionSource _getResultEntered =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        readonly ManualResetEventSlim _releaseGetResult = new();

        internal Task GetResultEntered => _getResultEntered.Task;
        internal void Complete() => _core.SetResult(true);
        internal void ReleaseGetResult() => _releaseGetResult.Set();

        void IValueTaskSource.GetResult(short token)
        {
            _getResultEntered.TrySetResult();
            _releaseGetResult.Wait();
            _ = _core.GetResult(token);
        }

        ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

        void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags)
            => _core.OnCompleted(continuation, state, token, flags);
    }

    [TestMethod]
    public async Task FrontierEnumerator_ReusedPipelineRun_InvalidatesStaleObservation()
    {
        var source = TestObservableQueueSource<TestPipelineItem>.Create();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy,
            TestObservableQueueSource<TestPipelineItem>,
            TestObservableQueueSource<TestPipelineItem>.Enumerator>(
            new(runEnqueueAsynchronously: true), source);
        var item = new TestPipelineItem { CompleteAsync = true };
        source.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();

        var stale = pipeline.GetEnumerator(out var frontier);
        Assert.IsTrue(frontier.DispatchedThrough > frontier.RetiredAtCapture);

        item.CompletePipelineTask();
        await pipeline.CompleteAsync();

        var nextSource = TestObservableQueueSource<TestPipelineItem>.Create();
        var reused = Pipeline.Create(
            new TestPipelinePolicy(runEnqueueAsynchronously: true), nextSource, pipeline);
        Assert.AreSame(pipeline, reused);
        Assert.IsTrue(frontier.IsRetired(frontier.DispatchedThrough));
        Assert.IsFalse(stale.MoveNext(),
            "An enumerator from a completed run must not expose retained identities after reuse.");

        await reused.CompleteAsync();
    }

    [TestMethod]
    public async Task EnumerationFrontier_Wraparound_UsesSignedModularOrdering()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true));
        _ = pipeline.Pipeline.GetEnumerator(out var currentRun);
        var frontier = new Pipeline<TestPipelineItem, TestPipelinePolicy,
            UnboundedQueueSource<TestPipelineItem>,
            UnboundedQueueSource<TestPipelineItem>.Enumerator>.EnumerationFrontier(
                pipeline.Pipeline, currentRun.RunGeneration,
                retiredThrough: uint.MaxValue - 2, dispatchedThrough: 2);

        Assert.IsTrue(frontier.IsRetired(uint.MaxValue - 2), "Capture low-water is not live.");
        Assert.IsTrue(frontier.IsRetired(uint.MaxValue - 1));
        Assert.IsTrue(frontier.IsRetired(uint.MaxValue));
        Assert.IsTrue(frontier.IsRetired(0));
        Assert.IsFalse(frontier.IsRetired(1));
        Assert.IsFalse(frontier.IsRetired(2));
        Assert.IsTrue(frontier.IsRetired(3), "A position beyond the captured high-water fails closed.");

        await pipeline.CompleteAsync();
    }

    [TestMethod]
    public void QueueFrontierEnumerator_TailPublicationGap_StopsAtCapturedHead()
    {
        var queue = new SingleProducerSingleConsumerQueue<int>();
        for (var i = 0; i < 7; i++)
            queue.Enqueue(i);

        // Enqueuing the eighth item publishes oldTail._next before advancing queue._tail. Recreate
        // that canonical SPSC publication interval after moving the consumer onto the new segment.
        var tailField = typeof(SingleProducerSingleConsumerQueue<int>).GetField(
            "_tail", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var precedingTail = tailField.GetValue(queue);
        queue.Enqueue(7);
        var publishedTail = tailField.GetValue(queue);
        Assert.AreNotSame(precedingTail, publishedTail);

        for (var i = 0; i < 7; i++)
        {
            Assert.IsTrue(queue.TryDequeue(out var item));
            Assert.AreEqual(i, item);
        }
        Assert.IsTrue(queue.TryPeek(out var head));
        Assert.AreEqual(7, head);

        SingleProducerSingleConsumerQueue<int>.FrontierEnumerator enumerator;
        tailField.SetValue(queue, precedingTail);
        try
        {
            enumerator = queue.GetFrontierEnumerator();
        }
        finally
        {
            tailField.SetValue(queue, publishedTail);
        }

        // Fill the captured head and force a later segment publication. A fixed frontier from the
        // publication gap must not follow that successor merely because its captured tail was behind.
        for (var i = 8; i < 22; i++)
            queue.Enqueue(i);
        queue.Enqueue(22);

        var observed = new List<int>();
        while (enumerator.MoveNext(out var item))
            observed.Add(item);

        CollectionAssert.AreEqual(new[] { 7 }, observed);
    }

    [TestMethod]
    public async Task SegmentGrowth_YieldsAllItems()
    {
        // SPSC initial segment size is 32. Enqueue more than that to force segment growth.
        // Items are CompleteAsync, so depth never reaches 0 - use the source onIdle hook as the
        // executor-at-rest signal (CommitPendingTail's transit window must be settled before enum).
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int count = 50;
        var enqueued = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            enqueued[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(enqueued[i]).Signal();
        }

        for (var i = 0; i < count; i++)
            await enqueued[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var observed = new List<TestPipelineItem>();
        foreach (var item in pipeline)
            observed.Add(item);

        Assert.AreEqual(count, observed.Count, "Enumerator should walk all segments and yield every item.");
        for (var i = 0; i < count; i++)
            Assert.AreSame(enqueued[i], observed[i]);

        // Drain.
        for (var i = 0; i < count; i++)
            enqueued[i].CompletePipelineTask();
        for (var i = 0; i < count; i++)
            await enqueued[i].WaitForCompleteAsync();
    }

    [TestMethod]
    public async Task AfterAllCompleted_YieldsNothing()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        const int count = 3;
        for (var i = 0; i < count; i++)
        {
            var item = new TestPipelineItem();
            pipeline.Enqueue(item).Signal();
            await item.WaitForCompleteAsync();
        }

        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);

        var observed = new List<TestPipelineItem>();
        foreach (var item in pipeline)
            observed.Add(item);

        Assert.AreEqual(0, observed.Count, "Drained pipeline should yield no items.");
    }

    [TestMethod]
    public async Task TwoSnapshots_BothObserveSameItems()
    {
        // Enumeration is non-mutating, repeating it yields the same items. Items are CompleteAsync,
        // so depth never reaches 0 during enumeration - WaitForEmptyAsync would hang. Need an
        // executor-at-rest signal independent of depth. Use the source onIdle hook.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int count = 4;
        var enqueued = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            enqueued[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(enqueued[i]).Signal();
        }
        for (var i = 0; i < count; i++)
            await enqueued[i].WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var first = new List<TestPipelineItem>();
        foreach (var item in pipeline) first.Add(item);

        var second = new List<TestPipelineItem>();
        foreach (var item in pipeline) second.Add(item);

        CollectionAssert.AreEqual(first, second, "Repeated enumerations should observe the same items (no mutation).");

        // Drain.
        for (var i = 0; i < count; i++)
            enqueued[i].CompletePipelineTask();
        for (var i = 0; i < count; i++)
            await enqueued[i].WaitForCompleteAsync();
    }

    [TestMethod]
    public void Enumerator_IsValueType()
    {
        // The enumerator should be a struct so `foreach` over the pipeline doesn't box.
        Assert.IsTrue(typeof(Pipeline<TestPipelineItem, TestPipelinePolicy, UnboundedQueueSource<TestPipelineItem>, UnboundedQueueSource<TestPipelineItem>.Enumerator>.Enumerator).IsValueType,
            "Pipeline enumerator should be a struct to support allocation-free foreach.");
    }

    // Regression: a recovery retained outside the in-flight store was invisible to heartbeat
    // enumeration, freezing a decoder timeout while it waited on socket input.
    [TestMethod]
    public async Task PendingInFlightRecovery_EnumeratedExactlyOnce_GoneAfterRetirement()
    {
        // A pending pipeline task keeps the recovery in its dedicated slot.
        var recovery = new TestPipelineItem { Name = "recovery", CompleteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(item).Signal();
        // Commit before faulting so recovery runs from the dedicated in-flight slot.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        item.CompletePipelineTask();
        await recovery.WaitForExecutedAsync();

        // SignalExecuted slightly precedes slot publication, so wait for visibility.
        Assert.IsTrue(SpinWait.SpinUntil(() => CountOccurrences(pipeline, recovery) > 0, TimeSpan.FromSeconds(10)),
            "Pending in-flight recovery never became enumerable.");
        Assert.AreEqual(1, CountOccurrences(pipeline, recovery), "Recovery must be yielded exactly once per enumeration.");
        Assert.AreEqual(0, CountOccurrences(pipeline, item), "The failed item was consumed from the store and must not reappear.");
        Assert.IsFalse(recovery.IsCompleted, "Recovery must still be pending while enumerated.");

        recovery.CompletePipelineTask();
        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
        Assert.AreEqual(0, CountOccurrences(pipeline, recovery), "Retired recovery must not linger in enumeration.");
    }

    static int CountOccurrences(ObservablePipeline<TestPipelineItem, TestPipelinePolicy> pipeline, TestPipelineItem item)
    {
        var count = 0;
        foreach (var observed in pipeline)
        {
            if (ReferenceEquals(observed, item))
                count++;
        }
        return count;
    }

    [TestMethod]
    public async Task ManualMoveNext_ReturnsFalseAfterLastItem()
    {
        // WaitForExecutedAsync only signals that SignalExecuted fired inside ExecuteItemAsync.
        // the executor hasn't yet committed the item to _pendingTail / _inFlight. Use the source
        // onIdle hook to wait until the executor is settled before enumerating, otherwise MoveNext
        // races the routing and may see no items.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var enumerator = pipeline.GetEnumerator();
        Assert.IsTrue(enumerator.MoveNext(), "First MoveNext should yield the single in-flight item.");
        Assert.AreSame(item, enumerator.Current);
        Assert.IsFalse(enumerator.MoveNext(), "Subsequent MoveNext should return false.");
        Assert.IsFalse(enumerator.MoveNext(), "Repeated MoveNext past end should keep returning false.");

        item.CompletePipelineTask();
        await item.WaitForCompleteAsync();
    }

}
