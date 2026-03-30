namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineTests
{
    [TestMethod]
    public void SyncEnqueueComplete()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.SyncFirst));
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        item.WaitForComplete();
        Assert.IsNull(item.Exception);
    }

    [TestMethod]
    public void SyncEnqueueCompleteMultiple()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.SyncFirst));
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
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.Async));
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();
        Assert.IsNull(item.Exception);
    }

    [TestMethod]
    public async Task PipelinedItems()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.Async));
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
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.Async));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("test") };
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();
        Assert.IsInstanceOfType<InvalidOperationException>(item.Exception);
    }

    [TestMethod]
    public async Task PipelineTaskFailureCompletesItem()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.Async));
        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("test") };
        pipeline.Enqueue(item).Execute();
        item.CompletePipelineTask();
        await item.WaitForCompleteAsync();
        Assert.IsInstanceOfType<InvalidOperationException>(item.Exception);
    }

    [TestMethod]
    public async Task CompleteAsyncDrainsItems()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.Async));
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
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.Async));
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
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.SyncFirst));
        Assert.AreEqual(0, pipeline.Depth);

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Execute();
        item.WaitForComplete();
        Assert.AreEqual(0, pipeline.Depth);
    }

    [TestMethod]
    public void EnqueueAfterCompleteThrows()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(PipelineExecutionMode.SyncFirst));
        pipeline.CompleteAsync().AsTask().GetAwaiter().GetResult();
        Assert.Throws<InvalidOperationException>(() => pipeline.Enqueue(new TestPipelineItem()));
    }
}
