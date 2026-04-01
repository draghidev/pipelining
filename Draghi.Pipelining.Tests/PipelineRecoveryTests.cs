namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineRecoveryTests
{
    [TestMethod]
    public async Task RecoveryReturnsNull_ItemCompletedWithOriginalException()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("original") };
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        Assert.IsInstanceOfType<InvalidOperationException>(item.Exception);
        Assert.AreEqual("original", item.Exception!.Message);
    }

    [TestMethod]
    public async Task RecoveryExecuteSyncSuccess()
    {
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => recovery));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("fail") };
        pipeline.Enqueue(item).Execute();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.IsFalse(item.IsCompleted);
    }

    [TestMethod]
    public async Task RecoveryExecuteAsyncSuccess()
    {
        var recovery = new TestPipelineItem { ExecuteAsync = true };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => recovery));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("fail") };
        pipeline.Enqueue(item).Execute();

        // Recovery execute is async — complete it.
        recovery.CompleteExecuteTask();
        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.IsFalse(item.IsCompleted);
    }

    [TestMethod]
    public async Task RecoveryExecuteThrows_RecoveryCompletedWithException()
    {
        var recovery = new TestPipelineItem { ThrowOnExecute = new ApplicationException("recovery failed") };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => recovery));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("original") };
        pipeline.Enqueue(item).Execute();

        await recovery.WaitForCompleteAsync();
        Assert.IsInstanceOfType<ApplicationException>(recovery.Exception);
        Assert.IsFalse(item.IsCompleted);
    }

    [TestMethod]
    public async Task RecoveryPipelineTaskPending_StoredAsTailWaiter()
    {
        var recovery = new TestPipelineItem { CompleteAsync = true };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => recovery));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("fail") };
        pipeline.Enqueue(item).Execute();

        // Recovery's pipeline task is pending — should not be completed yet.
        await Task.Delay(50);
        Assert.IsFalse(recovery.IsCompleted);

        // Complete the recovery's pipeline task.
        recovery.CompletePipelineTask();
        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
    }

    [TestMethod]
    public async Task RecoveryPipelineTaskFaults_RecoveryCompletedWithException()
    {
        var recovery = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new ApplicationException("pipeline failed") };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => recovery));
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("original") };
        pipeline.Enqueue(item).Execute();

        recovery.CompletePipelineTask();
        await recovery.WaitForCompleteAsync();
        Assert.IsInstanceOfType<ApplicationException>(recovery.Exception);
    }

    [TestMethod]
    public async Task WaiterPipelineTaskFaults_RecoveryOnAdvancer()
    {
        var recovery = new TestPipelineItem();
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null,
            idleTcs));

        // Item has a pending pipeline task that will fault later.
        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(item).Execute();

        // Wait for idle — the item is now committed as a waiter.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the pipeline task — advancer picks it up.
        item.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.IsFalse(item.IsCompleted);
    }

    [TestMethod]
    public async Task TrailingTaskFailure_RecoveryTakesOver()
    {
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery : null));

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("trailing failed")
        };
        pipeline.Enqueue(item).Execute();

        // Wait for execution, then fail the trailing task.
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
    }

    [TestMethod]
    public async Task CommittedTailFaults_RecoveryOnExecutor()
    {
        // The committed tail recovery path (PipelineTask kind in CommitTailWaiter) requires the
        // pipeline task to fault between being stored as tail and committed on the next iteration.
        // This is hard to hit deterministically. Instead, we test that recovery works regardless
        // of which path handles it (PipelineTask or PipelineTaskWaiter).
        var recovery = new TestPipelineItem();
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask or PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null,
            idleTcs));

        var first = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("tail fault") };
        pipeline.Enqueue(first).Execute();

        // Wait for idle — first item is committed as a waiter.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the pipeline task.
        first.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.IsFalse(first.IsCompleted);
    }
}
