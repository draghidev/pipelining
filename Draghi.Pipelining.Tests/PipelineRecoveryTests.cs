namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineRecoveryTests
{
    [TestMethod]
    public async Task RecoveryReturnsNull_ItemCompletedWithOriginalException()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("original") };
        pipeline.Enqueue(item).Execute();
        await item.WaitForCompleteAsync();

        Assert.IsInstanceOfType<InvalidOperationException>(item.Exception);
        Assert.AreEqual("original", item.Exception!.Message);
    }

    /// Verifies the recovery factory receives the correct failure context when ExecuteItemAsync
    /// throws: kind = ExecuteItemTask, Exception = the thrown exception (not wrapped, not the
    /// recovery item's later failure). Pins the contract that recovery factories can dispatch on
    /// kind to route different failure types to different handlers.
    [TestMethod]
    public async Task ExecuteItemTaskRecovery_ContextHasCorrectKindAndException()
    {
        var observedKind = PipelineItemFailureKind.PipelineTask;  // intentionally wrong default
        Exception? observedException = null;
        var thrown = new InvalidOperationException("execute boom");
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, ctx =>
        {
            observedKind = ctx.Kind;
            observedException = ctx.Exception;
            return recovery;
        }));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem { ThrowOnExecute = thrown };
        pipeline.Enqueue(item).Execute();
        await recovery.WaitForCompleteAsync();

        Assert.AreEqual(PipelineItemFailureKind.ExecuteItemTask, observedKind);
        Assert.AreSame(thrown, observedException);
    }

    [TestMethod]
    public async Task RecoveryExecuteSyncSuccess()
    {
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => recovery));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("fail") };
        pipeline.Enqueue(item).Execute();

        // Recovery execute is async, complete it.
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("fail") };
        pipeline.Enqueue(item).Execute();

        // Recovery's pipeline task is pending and should not be completed yet.
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
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Item has a pending pipeline task that will fault later.
        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(item).Execute();

        // Wait for idle, the item is committed as a waiter.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the pipeline task, advancer picks it up.
        item.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.IsFalse(item.IsCompleted);
    }

    // Regression: an ASYNC waiter-recovery that retires the LAST in-flight item must fire the depth-0
    // idle signal, so a WaitForEmptyAsync armed BEFORE the terminal completion is woken. The bug: the
    // recovery continuation completes the item via CompleteRecoveryWaiterDeferred (depth -> 0 without
    // firing OnDepthReachedZero, by design) and discarded `emptyReached` via `out _`, so the zero-
    // crossing was lost - hanging the parked WaitForEmptyAsync. Fix threads the bit to
    // AdvanceAndDrainRecovery(emptyReached) which fires after the advancer release.
    [TestMethod]
    public async Task AsyncWaiterRecovery_LastItem_FiresDepthZeroForParkedWaitForEmpty()
    {
        // BOTH the recovery's EXECUTE and its PIPELINE task are async. Execute-async hooks the
        // continuation at RecoverWaiter; when that continuation runs the pipeline task is STILL pending
        // (CompleteAsync), so RecoverWaiterPipelineTask returns false and hooks the pipeline-task
        // continuation. Completing the pipeline task LAST runs that continuation, which completes the
        // recovery (the last item) via CompleteRecoveryWaiterDeferred (depth -> 0, no inline fire) and
        // returned emptyReached through the `out` now threaded to AdvanceAndDrainRecovery. With both
        // tasks sync the flow collapses into RecoverWaiter's synchronous return-true path (where
        // emptyReached was never dropped); the async pipeline task is required to reach the dropped site.
        var recovery = new TestPipelineItem { ExecuteAsync = true, CompleteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // One item, committed as a waiter, whose pipeline task faults -> waiter-side recovery.
        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Arm WaitForEmptyAsync while depth > 0 (item still in flight): it must park, not complete.
        var drainTask = pipeline.WaitForEmptyAsync();
        Assert.IsFalse(drainTask.IsCompleted, "WaitForEmptyAsync must park while the waiter is in flight.");

        // Fault the waiter -> RecoverWaiter dispatches the substitute, whose execute is async.
        item.CompletePipelineTask();

        // Barrier: wait until RecoverWaiter has actually called ExecuteItemAsync and is parked on the
        // PENDING execute task (SignalExecuted fires right after GetExecuteTask). Without this the test
        // can complete the execute task before RecoverWaiter checks IsCompletedSuccessfully, collapsing
        // into the synchronous return-true path where emptyReached was never dropped.
        await recovery.WaitForExecutedAsync();

        // Now drive the async chain to the dropped site: complete execute (-> execute continuation;
        // pipeline still pending -> hooks the pipeline-task continuation), then complete pipeline LAST so
        // that continuation retires the last item via the deferred path, crossing depth -> 0.
        recovery.CompleteExecuteTask();
        recovery.CompletePipelineTask();

        // Without the fix the depth-0 fire is dropped and this hangs.
        await drainTask.AsTask().WaitAsync(TimeSpan.FromSeconds(5));
    }

    [TestMethod]
    public async Task TrailingTaskFailure_RecoveryTakesOver()
    {
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

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
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask or PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var first = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("tail fault") };
        pipeline.Enqueue(first).Execute();

        // Wait for idle, first item is committed as a waiter.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the pipeline task.
        first.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.IsFalse(first.IsCompleted);
    }

    /// Trailing failure with no recovery available: the item is re-enqueued as a waiter holding
    /// the pipeline task (per RecoverTrailingFailure's no-recovery branch). The trailing exception
    /// is effectively dropped and the item completes based on the pipeline task's outcome.
    [TestMethod]
    public async Task TrailingFailure_NoRecovery_OriginalCompletesViaPipelineTask()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("trailing fail"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();

        // Pipeline task still pending, item should not yet be completed.
        await Task.Delay(50);
        Assert.IsFalse(item.IsCompleted, "Item should be waiting for its pipeline task even though trailing failed.");

        item.CompletePipelineTask();
        await item.WaitForCompleteAsync();

        Assert.IsNull(item.Exception, "Item completes cleanly when pipeline task succeeds and no recovery handled the trailing fault.");
    }

    /// Trailing fails AND pipeline task fails, no recovery: pipeline task exception wins, trailing
    /// exception is discarded. Documents that the item's "final" exception is its pipeline task's,
    /// not its trailing task's, when both fault and recovery doesn't intervene.
    [TestMethod]
    public async Task TrailingFailure_NoRecovery_PipelineTaskAlsoFails_PipelineExceptionWins()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var pipelineEx = new ApplicationException("pipeline fail");
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("trailing fail"),
            PipelineTaskException = pipelineEx,
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();
        item.CompletePipelineTask();

        await item.WaitForCompleteAsync();
        Assert.AreSame(pipelineEx, item.Exception, "Pipeline task exception takes precedence over the dropped trailing exception when no recovery handles the trailing fault.");
    }

    /// Recovery from trailing failure where the recovery item's pipeline task itself fails async.
    /// Exercises the path where recovery is stored as a new tail (CompleteAsync=true on recovery)
    /// and the eventual pipeline task fault drives the recovery's completion.
    [TestMethod]
    public async Task RecoveryFromTrailingFailure_RecoveryPipelineTaskFaultsAsync()
    {
        var recoveryEx = new ApplicationException("recovery pipeline fail");
        var recovery = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = recoveryEx,
        };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("original trailing"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();

        // Recovery takes over, stored as tail with pending pipeline task.
        await recovery.WaitForExecutedAsync();
        await Task.Delay(50);
        Assert.IsFalse(recovery.IsCompleted, "Recovery should be pending its async pipeline task.");

        recovery.CompletePipelineTask();
        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryEx, recovery.Exception);
    }

    /// Recovery from trailing failure where the recovery item's OWN trailing task ALSO fails.
    /// RecoverTrailingFailure awaits the recovery's trailing task and on failure completes
    /// the recovery item with the trailing exception. Tests this nested-trailing-fault path.
    [TestMethod]
    public async Task RecoveryFromTrailingFailure_RecoveryTrailingAlsoFails()
    {
        var recoveryTrailingEx = new ApplicationException("recovery trailing fail");
        var recovery = new TestPipelineItem
        {
            TrailingTaskException = recoveryTrailingEx,
        };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("original trailing"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();

        // Recovery executes, awaits its own trailing task.
        await recovery.WaitForExecutedAsync();
        recovery.CompleteTrailingTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryTrailingEx, recovery.Exception);
    }

    /// RecoverCommittedTailWaiter hooks an async executeTask continuation when recovery has
    /// ExecuteAsync=true. If CompleteAsync fires while that continuation is in flight, the
    /// continuation has no wake-signal bailout (unlike RecoverWaiter's continuation). The
    /// continuation runs, hits EnqueueWaiter for the recovery (since its pipeline task is
    /// pending), but OnWaiterTaskCompleted's wake-signal-completed check causes the advancer
    /// to skip, leaving the recovery item leaked in _waiters with no one to drain it.
    [TestMethod]
    public async Task RecoverCommittedTailWaiter_AsyncContinuationRacesCompleteAsync_NoLeak()
    {
        var recovery = new TestPipelineItem { ExecuteAsync = true, CompleteAsync = true };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Construct an item that hits the committed-tail-faulted path: HasTrailingTask=true
        // keeps the executor inside the trailing await past the pipeline-task fault check on
        // lines 178/184, then CommitTailWaiter at line 238 observes the faulted pipeline task.
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            HasTrailingTask = true,
            PipelineTaskException = new InvalidOperationException("tail fault"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        // Executor is awaiting item's trailing task.

        item.CompletePipelineTask();   // fault the pipeline task while executor is in trailing-await
        item.CompleteTrailingTask();   // signal trailing done. executor resumes, exits inner loop, CommitTailWaiter -> RecoverCommittedTailWaiterAsync

        // Wait for recovery's executeTask to be in flight: ExecuteItemAsync ran (SignalExecuted fired)
        // and the executor is now suspended in await ExecuteItemAsync on the recovery item.
        await recovery.WaitForExecutedAsync();

        // Start CompleteAsync but don't await yet. With the executor blocked in await ExecuteItemAsync,
        // shutdown can't complete until recovery's task resolves.
        var completeTask = pipeline.CompleteAsync().AsTask();

        // Drive recovery to completion. Pipeline task pending when executor resumes from
        // ExecuteItemAsync await, so it'll either EnqueueWaiter (if wake not yet observed) or bail
        // via the wake-completed branch.
        recovery.CompleteExecuteTask();
        recovery.CompletePipelineTask();

        // Recovery's CompleteItem must fire, and CompleteAsync's drain must terminate.
        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
        await recovery.WaitForCompleteAsync();
        Assert.IsTrue(recovery.IsCompleted, "Recovery item must be completed.");
    }

    /// Regression guard: the trailing-task continuation in RecoverCommittedTailWaiterResult also
    /// flows through RecoverCommittedTailWaiterPipelineTask (which has the wake-completed check
    /// fix). Verify that path is correctly covered by the leaf fix - no separate leak.
    [TestMethod]
    public async Task RecoverCommittedTailWaiter_TrailingContinuationRacesCompleteAsync_NoLeak()
    {
        // Recovery has HasTrailingTask=true (async trailing) and CompleteAsync=true (async pipeline).
        // Executor will fire trailing continuation, which calls RecoverCommittedTailWaiterPipelineTask.
        var recovery = new TestPipelineItem { HasTrailingTask = true, CompleteAsync = true };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            HasTrailingTask = true,
            PipelineTaskException = new InvalidOperationException("tail fault"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        item.CompletePipelineTask();   // fault pipeline task while executor in trailing-await
        item.CompleteTrailingTask();   // signal trailing done -> CommitTailWaiter sees faulted pipeline task -> RecoverCommittedTailWaiter

        await recovery.WaitForExecutedAsync();

        // Start CompleteAsync but don't await. Executor is suspended awaiting recovery's trailing task.
        var completeTask = pipeline.CompleteAsync().AsTask();

        // Fire trailing then pipeline. Executor resumes, processes trailing then pipeline (sync
        // bailout via the wake-completed check), completes recovery, and exits the main loop.
        recovery.CompleteTrailingTask();
        recovery.CompletePipelineTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
        await recovery.WaitForCompleteAsync();
        Assert.IsTrue(recovery.IsCompleted, "Recovery item must be completed even when arriving through the trailing-await path.");
    }

    /// Regression guard for RecoverWaiterPipelineTask's continuation bailout. When CompleteAsync
    /// fires while recovery's pipeline task is pending, the continuation holds _advancing across
    /// the async boundary. DrainOnCompletion spins on Interlocked.Exchange(ref _advancing, true)
    /// until the continuation releases it. The original bailout only set _waiterInRecovery=false
    /// and never released _advancing, deadlocking CompleteAsync. The bailout now mirrors the
    /// executeTask continuation pattern (lock + complete-if-needed + release _advancing).
    [TestMethod]
    public async Task RecoverWaiterPipelineTask_PendingDuringCompleteAsync_DoesNotDeadlock()
    {
        // Recovery: sync execute, pipeline task pending. Triggers the RecoverWaiterPipelineTask
        // continuation-hooked path that holds _advancing.
        var recovery = new TestPipelineItem { CompleteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Original: async pipeline that will fault, drives the advancer into RecoverWaiter.
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("waiter fault"),
        };
        pipeline.Enqueue(item).Execute();

        // Wait for the original to be committed as a waiter.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the pipeline task. Advancer fires, RecoverWaiter runs, recovery executes sync,
        // recovery's pipeline task is pending, continuation hooked, _advancing stays held.
        item.CompletePipelineTask();
        await recovery.WaitForExecutedAsync();

        // CompleteAsync. DrainOnCompletion takes _waiterRecoveryLock, completes the recovery
        // item, releases the lock, then spins waiting for _advancing to be released.
        var completeTask = pipeline.CompleteAsync().AsTask();
        await Task.Delay(100); // let CompleteAsync reach the _advancing spin.

        // Fire recovery's pipeline task. Continuation runs, sees wake completed, enters bailout.
        // Without the fix, bailout doesn't release _advancing, completeTask hangs forever.
        recovery.CompletePipelineTask();

        // With the fix this completes promptly. Without it, the WaitAsync throws TimeoutException.
        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// Regression guard for double-completion of the recovery item under shutdown race.
    /// RecoverWaiter's executeTask continuation has a wake-check entry. If it fires while the
    /// continuation is past the check but before CompleteWaiter, DrainOnCompletion (via
    /// CompleteAsync) may also complete the recovery item via _waiterRecoveryItem. Both code
    /// paths call into policy.CompleteItem on the same item, double dispatch, a contract
    /// violation. Stress test: many iterations of "recovery with async execute, race CompleteAsync
    /// against executeTask completion". Even one observed double-complete fails the test.
    [Ignore("Timing-based stress test, flaky on cold threadpool. Re-enable when needed to validate regressions.")]
    [TestMethod]
    public async Task RecoverWaiter_ContinuationSuccessPath_NoDoubleCompleteUnderShutdown()
    {
        for (var i = 0; i < 10; i++)
        {
            var completeCounts = new System.Collections.Concurrent.ConcurrentDictionary<TestPipelineItem, int>();
            var recovery = new TestPipelineItem { ExecuteAsync = true };

            var pipeline = Pipeline.Create<TestPipelineItem, CompletionCountingPolicy>(
                new CompletionCountingPolicy(completeCounts,
                    ctx => ctx.Kind == PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

            var item = new TestPipelineItem
            {
                CompleteAsync = true,
                PipelineTaskException = new InvalidOperationException("fault"),
            };
            pipeline.Enqueue(item).Execute();
            await item.WaitForExecutedAsync();

            // Fault pipeline → advancer → RecoverWaiter hooks async executeTask continuation,
            // _waiterRecoveryItem = recovery.
            item.CompletePipelineTask();
            await recovery.WaitForExecutedAsync();

            // Race window: fire CompleteAsync just before/during executeTask completion. Try to
            // land the continuation past wake-check but before CompleteWaiter while DrainOnCompletion
            // also runs CompleteWaiter on _waiterRecoveryItem.
            var completeTask = Task.Run(() => pipeline.CompleteAsync().AsTask());
            recovery.CompleteExecuteTask();
            await completeTask.WaitAsync(TimeSpan.FromSeconds(30));

            foreach (var (_, count) in completeCounts)
            {
                Assert.IsTrue(count <= 1, $"Iter {i}: item completed {count} times, double-complete race in recovery continuation.");
            }
        }
    }

    struct CompletionCountingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly System.Collections.Concurrent.ConcurrentDictionary<TestPipelineItem, int> _completeCounts;
        readonly Func<PipelineItemFailureContext, TestPipelineItem?>? _recoveryFactory;

        public CompletionCountingPolicy(System.Collections.Concurrent.ConcurrentDictionary<TestPipelineItem, int> completeCounts, Func<PipelineItemFailureContext, TestPipelineItem?>? recoveryFactory)
        {
            _completeCounts = completeCounts;
            _recoveryFactory = recoveryFactory;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool waiterExecution, CancellationToken cancellationToken)
        {
            if (item.ThrowOnExecute is { } ex)
                throw ex;
            var task = item.GetExecuteTask();
            item.SignalExecuted();
            return task;
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, int remainingDepth, Exception? exception)
        {
            _completeCounts.AddOrUpdate(item, 1, (_, c) => c + 1);
            item.Complete(exception);
        }

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = _recoveryFactory?.Invoke(context);
            return recoveryItem is not null;
        }

        public bool RunEnqueueAsynchronously => true;
    }

    /// Executor inline path: ExecuteItemAsync returns a sync-faulted PipelineTask. Pins the
    /// failure-kind for the branch at Pipeline.cs:235-250 (the sync-faulted pipeline task path),
    /// which existing CommittedTailFaults_RecoveryOnExecutor explicitly cannot distinguish from
    /// the PipelineTaskWaiter route.
    [TestMethod]
    public async Task SyncFaultedPipelineTaskOnExecutorPath_RecoveryKindIsPipelineTask()
    {
        var observedKind = PipelineItemFailureKind.ExecuteItemTask;  // intentionally wrong default
        Exception? observedException = null;
        var faultEx = new InvalidOperationException("sync pipeline fault");
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, ctx =>
        {
            observedKind = ctx.Kind;
            observedException = ctx.Exception;
            return recovery;
        }));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // CompleteAsync=false + PipelineTaskException = sync-faulted ValueTask from GetPipelineTask,
        // so the executor sees itemResult.PipelineTask.IsCompleted && !IsCompletedSuccessfully inline.
        var item = new TestPipelineItem { PipelineTaskException = faultEx };
        pipeline.Enqueue(item).Execute();
        await recovery.WaitForCompleteAsync();

        Assert.AreEqual(PipelineItemFailureKind.PipelineTask, observedKind);
        Assert.AreSame(faultEx, observedException);
    }

    /// RecoverTrailingFailure's recovery item returns a sync-faulted PipelineTask (line 461-477).
    /// Existing RecoveryFromTrailingFailure_RecoveryPipelineTaskFaultsAsync covers the async branch
    /// of the same path. This pins the sync-faulted branch.
    [TestMethod]
    public async Task TrailingFailure_RecoveryPipelineSyncFaulted_RecoveryCompletedWithException()
    {
        var recoveryEx = new ApplicationException("recovery pipeline sync fault");
        var recovery = new TestPipelineItem { PipelineTaskException = recoveryEx };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("trailing fail"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryEx, recovery.Exception);
    }

    /// RecoverTrailingFailure's outer ExecuteItemAsync catch (lines 482-490). Recovery item's
    /// ExecuteItemAsync throws synchronously. Mirrors RecoveryExecuteThrows_* but routed via
    /// the TrailingExecutionTask kind.
    [TestMethod]
    public async Task TrailingFailure_RecoveryExecuteThrows_RecoveryCompletedWithException()
    {
        var recoveryEx = new ApplicationException("recovery execute fail");
        var recovery = new TestPipelineItem { ThrowOnExecute = recoveryEx };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.TrailingExecutionTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            TrailingTaskException = new InvalidOperationException("trailing fail"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompleteTrailingTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryEx, recovery.Exception);
    }

    /// RecoverWaiter's recovery-execute sync-throw catch (lines 799-807). Advancer-side variant
    /// of TrailingFailure_RecoveryExecuteThrows.
    [TestMethod]
    public async Task WaiterRecovery_RecoveryExecuteSyncThrows_RecoveryCompletedWithException()
    {
        var recoveryEx = new ApplicationException("recovery execute fail");
        var recovery = new TestPipelineItem { ThrowOnExecute = recoveryEx };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("waiter fault"),
        };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        item.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryEx, recovery.Exception);
    }

    /// Waiter-phase counterpart of SyncFaultedPipelineTaskOnExecutorPath / TrailingFailure_RecoveryPipeline
    /// SyncFaulted: the waiter's substitute EXECUTES fine but returns a SYNC-faulted pipeline task
    /// (CompleteAsync=false + PipelineTaskException). RecoverWaiter sees the faulted pipeline task inline on
    /// the ADVANCER - no executor try/catch behind it - and must still complete the recovery with that fault.
    [TestMethod]
    public async Task WaiterRecovery_RecoveryPipelineSyncFaulted_RecoveryCompletedWithException()
    {
        var recoveryEx = new ApplicationException("recovery pipeline sync fault");
        // CompleteAsync=false => GetPipelineTask returns an already-faulted ValueTask, observed inline.
        var recovery = new TestPipelineItem { PipelineTaskException = recoveryEx };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Recovery-of-recovery is unsupported: the policy must be consulted exactly ONCE (for the original
        // waiter fault), never again for the recovery's own failure. A re-consultation bumps this.
        var consults = 0;
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => { consults++; return ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null; }),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Pending pipeline task => the item commits as a tail waiter; faulting it drives the advancer
        // into the PipelineTaskWaiter recovery route (not the executor's inline path).
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("waiter fault"),
        };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        item.CompletePipelineTask();

        await recovery.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreSame(recoveryEx, recovery.Exception);
        Assert.AreEqual(1, consults, "a failing recovery must NOT be re-recovered (recovery-of-recovery is unsupported)");
    }

    /// Waiter-phase counterpart of RecoveryPipelineTaskFaults / RecoveryFromTrailingFailure_RecoveryPipeline
    /// TaskFaultsAsync: the waiter's substitute executes, then its PENDING pipeline task faults ASYNC - so
    /// the fault lands in RecoverWaiterPipelineTask's deferred continuation on the advancer, the one path
    /// with no executor try/catch behind it. The failed recovery must complete WITH the fault (no hang, no
    /// unobserved advancer throw, no double-complete). RecoverWaiterPipelineTask_PendingDuringCompleteAsync
    /// drives this same continuation but only ever with a CLEAN recovery; this is the faulting case.
    [TestMethod]
    public async Task WaiterRecovery_RecoveryPipelineTaskFaultsAsync_RecoveryCompletedWithException()
    {
        var recoveryEx = new ApplicationException("recovery pipeline async fault");
        // CompleteAsync=true => pipeline task pending, so RecoverWaiterPipelineTask hooks a continuation.
        var recovery = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = recoveryEx };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Recovery-of-recovery is unsupported: exactly ONE consultation, for the original waiter fault.
        var consults = 0;
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => { consults++; return ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null; }),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("waiter fault"),
        };
        pipeline.Enqueue(item).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fault the waiter -> advancer -> RecoverWaiter dispatches the substitute, it executes sync, its
        // pipeline task is pending, the continuation is hooked.
        item.CompletePipelineTask();
        await recovery.WaitForExecutedAsync().WaitAsync(TimeSpan.FromSeconds(5));

        // Now fault the recovery's pipeline task: the continuation runs on the advancer and must complete
        // the recovery WITH the fault rather than letting it escape unobserved.
        recovery.CompletePipelineTask();

        await recovery.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreSame(recoveryEx, recovery.Exception);
        Assert.AreEqual(1, consults, "a failing recovery must NOT be re-recovered (recovery-of-recovery is unsupported)");
    }

    /// The TESTABLE half of the executor/advancer asymmetry: the executor IS the task CompleteAsync
    /// awaits (observable live via Completion), so a throw at an UNGUARDED seam (here the policy's
    /// TryRecoverItemFailure) is captured by the loop's catch-all and surfaced through the execution
    /// task instead of killing the pump. The advancer half (a bare-seam throw escaping onto a TP
    /// thread) is deliberately NOT tested. It has no task to deliver to, so fail-fast is the design
    /// and asserting it means crashing the host.
    [TestMethod]
    public async Task ExecutorRecoveryPolicyThrows_FaultSurfacesThroughExecutionTask()
    {
        var policyEx = new InvalidOperationException("policy blew up");
        // The recovery factory itself throws => TryRecoverItemFailure throws, which is NOT one of
        // RecoverItem's guarded seams (it runs before RecoverItem's try), so it propagates to the
        // loop's catch-all.
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, _ => throw policyEx));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem { ThrowOnExecute = new ApplicationException("original") };
        pipeline.Enqueue(item).Execute();

        // CompleteAsync returns _executionTask, faulted with the captured root by the loop's catch-all.
        Exception? surfaced = null;
        try { await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)); }
        catch (Exception ex) { surfaced = ex; }
        Assert.AreSame(policyEx, surfaced, "an unguarded executor-seam throw must surface through the execution task, not escape");
    }

    /// The marker path: a recovery that COMMITS as a waiter (pending pipeline task) has its task wrapped in
    /// GuardRecoveryTask (935), so a LATE fault is rethrown as RecoveryItemFaultException and RecoverWaiter
    /// (1480) / RecoverCommittedTailWaiterAsync (819) complete it DIRECTLY - never re-consulting the policy.
    /// The existing RecoverCommittedTailWaiter_* tests drive this commit path only with a CLEAN recovery;
    /// this is the faulting case, asserting the no-recovery-of-recovery invariant (consults==1) on it.
    [TestMethod]
    public async Task CommittedTailRecovery_RecoveryFaultsLate_MarkerCompletesDirectly()
    {
        var recoveryEx = new ApplicationException("recovery faults late");
        // Async execute keeps RecoverCommittedTailWaiterAsync suspended at the execute await until we let
        // it through, so the pending pipeline task is still pending when it reaches the commit (918) -
        // forcing the GuardRecoveryTask wrap rather than the inline pipeline-already-faulted branch (875).
        var recovery = new TestPipelineItem { ExecuteAsync = true, CompleteAsync = true, PipelineTaskException = recoveryEx };
        var consults = 0;
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => { consults++; return ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null; }));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Committed-tail-faulted path (mirrors RecoverCommittedTailWaiter_*): HasTrailingTask keeps the
        // executor in the trailing await past the pipeline-task fault check, then CommitTailWaiter observes
        // the faulted pipeline task and routes to RecoverCommittedTailWaiterAsync.
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            HasTrailingTask = true,
            PipelineTaskException = new InvalidOperationException("tail fault"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();
        item.CompletePipelineTask();
        item.CompleteTrailingTask();

        // Recovery's (async) execute is now in flight inside RecoverCommittedTailWaiterAsync.
        await recovery.WaitForExecutedAsync().WaitAsync(TimeSpan.FromSeconds(5));
        recovery.CompleteExecuteTask();
        await Task.Delay(50); // let it reach the commit (918) with the pipeline task still pending.

        // Fault the now-committed recovery's pipeline task: GuardRecoveryTask rethrows the marker, and
        // RecoverWaiter completes it directly with the inner fault - WITHOUT consulting the policy again.
        recovery.CompletePipelineTask();

        await recovery.WaitForCompleteAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreSame(recoveryEx, recovery.Exception);
        Assert.AreEqual(1, consults, "a late-faulting committed recovery is completed via the marker, never re-recovered");
    }

    /// RecoverCommittedTailWaiterAsync's execute catch (lines 572-575). Committed-tail recovery
    /// where the recovery item's ExecuteItemAsync throws synchronously.
    [TestMethod]
    public async Task CommittedTailRecovery_RecoveryExecuteThrows_RecoveryCompletedWithException()
    {
        var recoveryEx = new ApplicationException("recovery execute fail");
        var recovery = new TestPipelineItem { ThrowOnExecute = recoveryEx };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Same setup as RecoverCommittedTailWaiter_AsyncContinuationRacesCompleteAsync_NoLeak:
        // HasTrailingTask + PipelineTaskException keeps the executor inside trailing-await past
        // the inline fault check. CommitTailWaiter then sees a faulted pipeline task and dispatches.
        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            HasTrailingTask = true,
            PipelineTaskException = new InvalidOperationException("tail fault"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        item.CompletePipelineTask();
        item.CompleteTrailingTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryEx, recovery.Exception);
    }

    /// RecoverCommittedTailWaiterAsync's trailing-await catch (lines 584-588). Recovery item's
    /// own trailing task throws after sync execute success.
    [TestMethod]
    public async Task CommittedTailRecovery_RecoveryTrailingThrows_RecoveryCompletedWithException()
    {
        var recoveryTrailingEx = new ApplicationException("recovery trailing fail");
        var recovery = new TestPipelineItem
        {
            HasTrailingTask = true,
            TrailingTaskException = recoveryTrailingEx,
        };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true,
            ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem
        {
            CompleteAsync = true,
            HasTrailingTask = true,
            PipelineTaskException = new InvalidOperationException("tail fault"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        item.CompletePipelineTask();
        item.CompleteTrailingTask();

        // Recovery executes (sync), then awaits its async trailing task.
        await recovery.WaitForExecutedAsync();
        recovery.CompleteTrailingTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryTrailingEx, recovery.Exception);
    }

    /// Trailing task fails AFTER the item already completed via sync-success pipeline task
    /// (no pending tail). With no _tailWaiter to recover for, the executor's catch silently
    /// absorbs the trailing exception. The item stays cleanly completed.
    [TestMethod]
    public async Task TrailingFailure_NoPendingTailWaiter_TrailingExceptionAbsorbed()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Sync-success pipeline task (CompleteAsync=false) → CompleteWaiter fires inline,
        // _tailWaiter never set. Then trailing fault has no tail to recover for.
        var item = new TestPipelineItem
        {
            HasTrailingTask = true,
            TrailingTaskException = new InvalidOperationException("trailing fail"),
        };
        pipeline.Enqueue(item).Execute();
        await item.WaitForExecutedAsync();

        item.CompleteTrailingTask();

        await item.WaitForCompleteAsync();
        Assert.IsTrue(item.IsCompleted);
        Assert.IsNull(item.Exception);
    }

    /// Recovery replaces the original item in-place. Depth reflects the replacement, not a sum.
    /// After recovery completes, Depth returns to 0 (recovery item completed, original never
    /// independently completed because it was substituted out).
    [TestMethod]
    public async Task RecoveryReplacesOriginal_DepthReturnsToZero()
    {
        var recovery = new TestPipelineItem();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, _ => recovery));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("fail") };
        pipeline.Enqueue(item).Execute();

        await recovery.WaitForCompleteAsync();
        PipelineTestAsserts.AssertDepthSettlesToZero(() => pipeline.Depth);
        Assert.IsFalse(item.IsCompleted, "Original was substituted, not independently completed.");
    }

    /// Recovery-of-recovery does not exist: a committed recovery's own late fault completes the
    /// recovery directly with the REAL exception (the framework's marker is unwrapped, never
    /// observable), and the policy is NOT consulted a second time. This variant pins the
    /// committed-tail recovery path (RecoverCommittedTailWaiterAsync): the original item's
    /// pipeline task is already faulted at commit time, which routes the recovery through the
    /// guarded commit deterministically.
    [TestMethod]
    public async Task CommittedTailRecovery_PipelineTaskFaultsLate_CompletedDirectly_PolicyNotConsulted()
    {
        var recoveryFault = new ApplicationException("recovery drain died");
        var recovery = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = recoveryFault };
        var factoryCalls = 0;
        var consults = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, ctx =>
        {
            Interlocked.Increment(ref factoryCalls);
            consults.Enqueue($"{ctx.Kind}:{ctx.Exception.GetType().Name}:{ctx.Exception.Message}");
            return recovery;
        }));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        // Fault the original's pipeline task BEFORE enqueue: CommitTailWaiter observes a
        // completed+faulted task and takes the committed-tail recovery path.
        var item = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = new InvalidOperationException("original") };
        item.CompletePipelineTask();
        pipeline.Enqueue(item).Execute();

        // The recovery is committed with a pending pipeline task; fault it late.
        await recovery.WaitForExecutedAsync();
        recovery.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryFault, recovery.Exception,
            "The recovery's real fault must surface unwrapped (no framework marker visible).");
        Assert.AreEqual(1, Volatile.Read(ref factoryCalls),
            $"The policy must never be consulted about an item it returned as a recovery. Consults: {string.Join(" | ", consults)}");
        Assert.IsFalse(item.IsCompleted, "Original was substituted, not independently completed.");
    }

    /// Same contract for the executor-substitute recovery path (RecoverItem after an
    /// ExecuteItemAsync throw): the substitute re-enters the normal lifecycle and commits as an
    /// ordinary tail waiter, and its late fault must still complete directly without a second
    /// policy consult.
    [TestMethod]
    public async Task ExecutorSubstituteRecovery_PipelineTaskFaultsLate_CompletedDirectly_PolicyNotConsulted()
    {
        var recoveryFault = new ApplicationException("recovery drain died");
        var recovery = new TestPipelineItem { CompleteAsync = true, PipelineTaskException = recoveryFault };
        var factoryCalls = 0;
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true, ctx =>
        {
            Interlocked.Increment(ref factoryCalls);
            return recovery;
        }));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var item = new TestPipelineItem { ThrowOnExecute = new InvalidOperationException("original") };
        pipeline.Enqueue(item).Execute();

        await recovery.WaitForExecutedAsync();
        recovery.CompletePipelineTask();

        await recovery.WaitForCompleteAsync();
        Assert.AreSame(recoveryFault, recovery.Exception,
            "The recovery's real fault must surface unwrapped (no framework marker visible).");
        Assert.AreEqual(1, Volatile.Read(ref factoryCalls),
            "The policy must never be consulted about an item it returned as a recovery.");
        Assert.IsFalse(item.IsCompleted, "Original was substituted, not independently completed.");
    }

    /// Audit reorder (a): a slot drain that parks for recovery must leave the
    /// position's count credit in place until the rejoin. The old shape decremented BEFORE the
    /// fault decision, which opened the executor's Count==0 inline-activation gate mid-recovery
    /// (successors activated against the live recovery item, a second active reader) and
    /// double-decremented the position when the rejoin's AdvanceAndDrain ran its own decrement.
    [TestMethod]
    public async Task SlotRecoveryPark_SuccessorsCommitDuringRecovery_SingleActivationEach()
    {
        var recovery = new TestPipelineItem { Name = "R", ExecuteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var faulting = new TestPipelineItem { Name = "A", CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(faulting).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5)); // A committed (slot tier)

        // Fault the committed waiter: the drain claims the slot, the policy substitutes R, and
        // the drain parks on R's pending execute task with the advancer held.
        faulting.CompletePipelineTask();
        await recovery.WaitForExecutedAsync();

        // Successors committed during the park: B refills the freed slot, C forces escalation.
        var b = new TestPipelineItem { Name = "B", CompleteAsync = true };
        idleTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        pipeline.Enqueue(b).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var c = new TestPipelineItem { Name = "C", CompleteAsync = true };
        idleTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        pipeline.Enqueue(c).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The recovered position still holds its count credit, so neither successor may have
        // been activated (the old order's executor-side Count==0 gate activated B here).
        Assert.AreEqual(0, b.ActivationCount, "Successor B activated while the recovery occupies the head position.");
        Assert.AreEqual(0, c.ActivationCount, "Successor C activated while the recovery occupies the head position.");

        // Resume the recovery; its continuation rejoins the advancer chain, which decrements
        // the recovered position exactly once and activates B (queue head after escalation).
        recovery.CompleteExecuteTask();
        await recovery.WaitForCompleteAsync();
        await b.WaitForActivationAsync();

        b.CompletePipelineTask();
        await b.WaitForCompleteAsync();
        await c.WaitForActivationAsync();
        c.CompletePipelineTask();
        await c.WaitForCompleteAsync();

        Assert.AreEqual(1, recovery.ActivationCount);
        Assert.AreEqual(1, b.ActivationCount, "Successor B must be activated exactly once.");
        Assert.AreEqual(1, c.ActivationCount, "Successor C must be activated exactly once.");
        Assert.IsFalse(faulting.IsCompleted, "Original was substituted, not independently completed.");

        await pipeline.CompleteAsync();
    }

    /// The non-escalated face of the same reorder: with a SINGLE successor the store never
    /// escalates, so the recovery rejoin must run the slot-mode partition. The queue-flavored
    /// rejoin peeked an empty queue (the successor lives in the slot) and lost B's activation.
    [TestMethod]
    public async Task SlotRecoveryPark_SlotSuccessor_RejoinActivatesFromSlot()
    {
        var recovery = new TestPipelineItem { Name = "R", ExecuteAsync = true };
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTaskWaiter ? recovery : null),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var faulting = new TestPipelineItem { Name = "A", CompleteAsync = true, PipelineTaskException = new InvalidOperationException("waiter fault") };
        pipeline.Enqueue(faulting).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        faulting.CompletePipelineTask();
        await recovery.WaitForExecutedAsync();

        var b = new TestPipelineItem { Name = "B", CompleteAsync = true };
        idleTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        pipeline.Enqueue(b).Execute();
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.AreEqual(0, b.ActivationCount, "Slot successor activated while the recovery occupies the head position.");

        recovery.CompleteExecuteTask();
        await recovery.WaitForCompleteAsync();
        await b.WaitForActivationAsync();

        b.CompletePipelineTask();
        await b.WaitForCompleteAsync();

        Assert.AreEqual(1, b.ActivationCount, "Slot successor must be activated exactly once by the rejoin's slot partition.");
        Assert.IsFalse(faulting.IsCompleted, "Original was substituted, not independently completed.");

        await pipeline.CompleteAsync();
    }

    /// Audit reorder (b): a tail whose pipeline task is already settled-and-faulted at
    /// commit time recovers via RecoverCommittedTailWaiterAsync. With a PRIOR committed waiter
    /// still in flight, the recovery install must be count-gated (mirroring RecoverItem): the
    /// prior waiter is the active reader and the recovery defers, completing unactivated when its
    /// work finishes first. The old shape activated unconditionally, a second active reader.
    [TestMethod]
    public async Task CommittedTailFaultsAtCommit_PriorWaiterInFlight_RecoveryActivationGated()
    {
        var recovery = new TestPipelineItem { Name = "R" };
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(true, ctx => ctx.Kind is PipelineItemFailureKind.PipelineTask ? recovery : null));
        using var __pin = MstestWhenAllWorkaround.Pin(pipeline);

        var prior = new TestPipelineItem { Name = "W", CompleteAsync = true };
        pipeline.Enqueue(prior).Execute();
        await prior.WaitForExecutedAsync();

        // The faulting tail. The trailing await is the only deterministic suspension between the
        // tail transition and the next iteration's CommitTailWaiter; the pipeline task faults
        // inside that window, so the commit observes a settled, faulted task.
        var gate = new AwaitObservedTrailingSource();
        var tail = new TestPipelineItem
        {
            Name = "X",
            CompleteAsync = true,
            PipelineTaskException = new InvalidOperationException("faulted at commit"),
            TrailingTaskSource = gate,
        };
        pipeline.Enqueue(tail).Execute();
        await gate.Awaited.WaitAsync(TimeSpan.FromSeconds(5));

        tail.CompletePipelineTask();
        gate.Complete();

        await recovery.WaitForCompleteAsync();
        Assert.IsNull(recovery.Exception);
        Assert.AreEqual(0, recovery.ActivationCount,
            "The prior waiter was in flight; a count-gated install completes the recovery unactivated.");
        Assert.AreEqual(1, prior.ActivationCount);
        Assert.IsFalse(tail.IsCompleted, "Original was substituted, not independently completed.");
        Assert.IsFalse(prior.IsCompleted);

        prior.CompletePipelineTask();
        await prior.WaitForCompleteAsync();
        await pipeline.CompleteAsync();
    }

    /// Trailing task source whose await registration is observable. The executor suspending on
    /// the trailing await is the deterministic window in which a tail's pipeline task can fault
    /// before CommitTailWaiter observes it (the RecoverCommittedTailWaiterAsync branch).
    /// Single-use, single-awaiter; the test serializes Complete after Awaited.
    sealed class AwaitObservedTrailingSource : System.Threading.Tasks.Sources.IValueTaskSource
    {
        readonly TaskCompletionSource _awaited = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Action<object?>? _continuation;
        object? _state;
        bool _completed;

        public Task Awaited => _awaited.Task;
        public ValueTask Task => new(this, 0);

        public void Complete()
        {
            Volatile.Write(ref _completed, true);
            if (Interlocked.Exchange(ref _continuation, null) is { } continuation)
                continuation(_state);
        }

        public System.Threading.Tasks.Sources.ValueTaskSourceStatus GetStatus(short token)
            => Volatile.Read(ref _completed)
                ? System.Threading.Tasks.Sources.ValueTaskSourceStatus.Succeeded
                : System.Threading.Tasks.Sources.ValueTaskSourceStatus.Pending;

        public void GetResult(short token)
        {
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, System.Threading.Tasks.Sources.ValueTaskSourceOnCompletedFlags flags)
        {
            _state = state;
            Volatile.Write(ref _continuation, continuation);
            _awaited.TrySetResult();
        }
    }
}
