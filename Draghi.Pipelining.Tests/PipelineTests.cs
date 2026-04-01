namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineTests
{
    [TestMethod]
    public void SyncEnqueueComplete()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(false));
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        item.WaitForComplete();
        Assert.IsNull(item.Exception);
    }

    [TestMethod]
    public void SyncEnqueueCompleteMultiple()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(false));
        for (var i = 0; i < 100; i++)
        {
            var item = new TestPipelineItem();
            pipeline.Enqueue(item).Execute();
            item.WaitForComplete();
            Assert.IsNull(item.Exception);
        }
    }

    [TestMethod]
    public async Task AsyncEnqueueComplete()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();
        Assert.IsNull(item.Exception);
    }

    [TestMethod]
    public async Task PipelinedItems()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var items = new TestPipelineItem[10];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }

        // Complete pipeline tasks in order.
        for (var i = 0; i < items.Length; i++)
        {
            items[i].CompletePipelineTask();
            await items[i].WaitForCompleteAsync();
            Assert.IsNull(items[i].Exception);
        }
    }

    [TestMethod]
    public async Task ExecuteFailureCompletesItem()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("test") };
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();
        Assert.IsInstanceOfType<InvalidOperationException>(item.Exception);
    }

    [TestMethod]
    public async Task PipelineTaskFailureCompletesItem()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("test") };
        pipeline.Enqueue(item).Execute();
        item.CompletePipelineTask();
        await item.WaitForCompleteAsync();
        Assert.IsInstanceOfType<InvalidOperationException>(item.Exception);
    }

    [TestMethod]
    public async Task CompleteAsyncDrainsItems()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var items = new TestPipelineItem[5];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Execute();
        }

        await pipeline.CompleteAsync();

        for (var i = 0; i < items.Length; i++)
            Assert.IsTrue(items[i].IsCompleted);
    }

    [TestMethod]
    public async Task WaitForIdleAsync()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Execute();

        var drainTask = pipeline.WaitForIdleAsync();
        Assert.IsFalse(drainTask.IsCompleted);

        item.CompletePipelineTask();
        await drainTask;
    }

    [TestMethod]
    public void DepthTracking()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(false));
        Assert.AreEqual(0, pipeline.Depth);

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        item.WaitForComplete();
        Assert.AreEqual(0, pipeline.Depth);
    }

    [TestMethod]
    public void EnqueueAfterCompleteThrows()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(false));
        pipeline.CompleteAsync().AsTask().GetAwaiter().GetResult();
        Assert.Throws<InvalidOperationException>(() => pipeline.Enqueue(new TestPipelineItem()));
    }

    [TestMethod]
    public async Task CompletionTokenFiresOnComplete()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true));

        Assert.IsFalse(pipeline.CompletionToken.IsCancellationRequested);

        await pipeline.CompleteAsync();

        Assert.IsTrue(pipeline.CompletionToken.IsCancellationRequested);
    }

    [TestMethod]
    public async Task CompleteAsyncDrainsWaiters()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true));

        // Enqueue items with pending pipeline tasks — they become waiters.
        var item1 = new TestPipelineItem { CompleteAsync = true };
        var item2 = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item1).Execute();
        pipeline.Enqueue(item2).Execute();
        item1.WaitForExecuted();
        item2.WaitForExecuted();

        Assert.AreEqual(2, pipeline.Depth);

        // CompleteAsync drains waiters.
        await pipeline.CompleteAsync();

        Assert.IsTrue(item1.IsCompleted);
        Assert.IsTrue(item2.IsCompleted);
        Assert.AreEqual(0, pipeline.Depth);
    }
}
