namespace Draghi.Pipelining.Tests;

using Draghi.Pipelining.Internal;

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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var observed = new List<TestPipelineItem>();
        foreach (var item in pipeline)
            observed.Add(item);

        Assert.AreEqual(0, observed.Count);
    }

    [TestMethod]
    public async Task PendingWaiters_YieldsAllInEnqueueOrder()
    {
        // Items are CompleteAsync, so depth never reaches 0 - use the source onIdle hook as the
        // executor-at-rest signal (CommitTailWaiter's transit window must be settled before enum).
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        const int count = 5;
        var enqueued = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            enqueued[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(enqueued[i]).Execute();
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
    public async Task SegmentGrowth_YieldsAllItems()
    {
        // SPSC initial segment size is 32. Enqueue more than that to force segment growth.
        // Items are CompleteAsync, so depth never reaches 0 - use the source onIdle hook as the
        // executor-at-rest signal (CommitTailWaiter's transit window must be settled before enum).
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        const int count = 50;
        var enqueued = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            enqueued[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(enqueued[i]).Execute();
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        const int count = 3;
        for (var i = 0; i < count; i++)
        {
            var item = new TestPipelineItem();
            pipeline.Enqueue(item).Execute();
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        const int count = 4;
        var enqueued = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            enqueued[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(enqueued[i]).Execute();
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

    [TestMethod]
    public async Task ManualMoveNext_ReturnsFalseAfterLastItem()
    {
        // WaitForExecutedAsync only signals that SignalExecuted fired inside ExecuteItemAsync.
        // the executor hasn't yet committed the item to _tailWaiter / _waiters. Use the source
        // onIdle hook to wait until the executor is settled before enumerating, otherwise MoveNext
        // races the routing and may see no items.
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();
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
