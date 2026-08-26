namespace Draghi.Pipelining.Tests;

[TestClass]
public class PipelineLifecycleTests
{
    [TestMethod]
    public async Task CompleteAsync_CalledTwice_SecondCallReturnsSameTask()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        var first = pipeline.CompleteAsync();
        var second = pipeline.CompleteAsync();

        // First-writer guard: both calls observe the same underlying execution task. Both
        // awaits complete cleanly. CompleteAsync's returned task is the proxy for "pipeline ran
        // to completion" — CompletionToken doesn't fire on CompleteAsync in the new design.
        await first;
        await second;
    }

    [TestMethod]
    public async Task SuccessfulCompletion_PreservesExecutionTaskIdentity()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));
        var execution = pipeline.Completion;

        Assert.IsFalse(execution.IsCompleted);
        await pipeline.CompleteAsync();

        Assert.AreSame(execution, pipeline.Completion,
            "completion must not publish a completed sentinel before the execution task itself retires");
    }

    /// CompleteAsync() signals the wake signal's CompletionToken. Policies that observe it from
    /// ExecuteItemAsync (e.g., to abort I/O on shutdown) should see cancellation when shutdown fires.
    [TestMethod]
    public async Task CompleteAsync_SignalsCompletionTokenObservedByExecuteItem()
    {
        var executeObservedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Pipeline.Create<TestPipelineItem, TokenObservingPolicy>(new(executeTokenSink: executeObservedCancellation));

        // Enqueue an item. Policy's ExecuteItemAsync awaits cancellation on the token.
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();

        // Give the executor a moment to enter ExecuteItemAsync.
        await Task.Delay(50);

        // Shutdown - policy's await should observe the cancellation.
        var completeTask = pipeline.CompleteAsync().AsTask();

        await executeObservedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// CompletionToken is also observable from OnExecutionIdleAsync. A policy that uses idle
    /// time for housekeeping (or just suspends on a cancellable wait) should unblock on shutdown.
    ///
    /// REWRITTEN (2026-07-10): this used to assert CompleteAsync's task FAULTS with the idle hook's
    /// rethrown OperationCanceledException (the pre-refactor executor caught the idle exception in a
    /// dedicated try/catch and rethrew it as a genuine fault). That's no longer how shutdown works -
    /// the idle hook now fires inside MoveNextAsync, and the executor's main-loop catch
    /// (catch (OperationCanceledException) when (_enumerator.CompletionToken.IsCancellationRequested))
    /// swallows an OCE raised during shutdown as a clean exit, verified directly: CompleteAsync's task
    /// completes successfully 8/8 runs once the idle hook has observed cancellation, never faults.
    /// This is an intentional design difference, not a latent bug - a non-OCE idle throw still faults
    /// (see OnExecutionIdleAsync_Throws_*); only the OCE-during-shutdown case is the clean-exit path.
    /// Asserts the current, correct behavior instead of the pre-refactor one.
    [TestMethod]
    public async Task CompleteAsync_SignalsCompletionTokenObservedByIdle()
    {
        var idleObservedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The source's onIdle hook waits on the completion token and rethrows the cancellation,
        // mirroring a policy that used idle time for a cancellable wait. The throw propagates out
        // of MoveNextAsync so CompleteAsync's task faults (old OnExecutionIdleAsync semantics).
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(),
            onIdle: async token =>
            {
                try { await Task.Delay(Timeout.Infinite, token).ConfigureAwait(false); }
                catch (OperationCanceledException) { idleObservedCancellation.TrySetResult(); throw; }
            });

        // Enqueue an item to push the executor past the cold wait into the post-batch idle hook.
        // Without this, the executor suspends at the wake signal without ever firing onIdle.
        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();
        await item.WaitForCompleteAsync();

        // CompleteAsync's task completes cleanly - the idle hook's rethrown OCE is swallowed by the
        // executor's main-loop shutdown catch, not propagated as a fault.
        var completeTask = pipeline.CompleteAsync().AsTask();

        await idleObservedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
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
            pipeline.Enqueue(items[i]).Signal();
        }
        for (var i = 0; i < items.Length; i++)
            await items[i].WaitForExecutedAsync();

        var ex = new InvalidOperationException("drain");
        await pipeline.CompleteAsync(ex);

        // All items should be completed with the supplied exception.
        for (var i = 0; i < items.Length; i++)
        {
            Assert.IsTrue(items[i].IsCompleted, $"Item {i} should be completed.");
            Assert.IsInstanceOfType<OperationCanceledException>(items[i].Exception,
                $"Item {i} settles via the shutdown token (item-side escalation), not the drain exception.");
        }
    }

    /// Exception propagation reaches items in BOTH buckets: items already in _inFlight with pending
    /// pipeline tasks, and items still in _queue that the executor hadn't dequeued yet.
    /// DrainOnCompletionAsync iterates both queues and calls RetireItem(_, exception) for each.
    [TestMethod]
    public async Task CompleteAsync_WithException_PropagatesToQueuedAndInFlightItems()
    {
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        // Phase 1: enqueue async items + wait for the executor to suspend. These end up in _inFlight.
        var waitersItems = new TestPipelineItem[3];
        for (var i = 0; i < waitersItems.Length; i++)
        {
            waitersItems[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(waitersItems[i]).Signal();
        }
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Phase 2: enqueue more items WITHOUT calling Execute. They sit in the source queue with the
        // executor suspended. CompleteAsync = true so they register the shutdown-token escalation: when
        // CompleteAsync wakes the executor, the source's drain-first MoveNextAsync still hands these
        // queued items back, the executor runs them, and the cancelled token settles them via OCE.
        var queueItems = new TestPipelineItem[3];
        for (var i = 0; i < queueItems.Length; i++)
        {
            queueItems[i] = new TestPipelineItem { CompleteAsync = true };
            _ = pipeline.Enqueue(queueItems[i]);  // discard the EnqueueSignal, don't wake executor
        }

        // CompleteAsync wakes the executor, which exits and DrainOnCompletionAsync drains both.
        var ex = new InvalidOperationException("drain");
        await pipeline.CompleteAsync(ex);

        foreach (var item in waitersItems)
        {
            Assert.IsTrue(item.IsCompleted, "In-flight item should be completed.");
            Assert.IsInstanceOfType<OperationCanceledException>(item.Exception,
                "In-flight item settles via the shutdown token (item-side escalation).");
        }
        foreach (var item in queueItems)
        {
            Assert.IsTrue(item.IsCompleted, "Queue-bucket item should be completed.");
            Assert.IsInstanceOfType<OperationCanceledException>(item.Exception,
                "Queue-bucket item settles via the shutdown token (item-side escalation).");
        }
    }

    [TestMethod]
    public void WaitForEmptyAsync_AlreadyIdle_CompletesImmediately()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Depth is 0 from construction. WaitForEmptyAsync should return a completed task synchronously.
        var task = pipeline.WaitForEmptyAsync();
        Assert.IsTrue(task.IsCompleted);
        Assert.IsTrue(task.IsCompletedSuccessfully);
    }

    [TestMethod]
    public async Task WaitForEmptyAsync_Cancelled_ThrowsTaskCanceled()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // Hold an item in-flight so WaitForEmptyAsync has to wait.
        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();

        using var cts = new CancellationTokenSource();
        var idleTask = pipeline.WaitForEmptyAsync(cts.Token).AsTask();

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
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(runEnqueueAsynchronously: true),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();
        await item.WaitForCompleteAsync();

        // Once the executor has nothing left to dequeue, OnExecutionIdleAsync should fire.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(idleTcs.Task.IsCompletedSuccessfully);
    }

    [TestMethod]
    public async Task OnIdle_FollowsFinalCompleteItem()
    {
        var events = new List<string>();
        var pipeline = Pipeline.Create<TestPipelineItem, IdleCapturingPolicy>(new(events));

        const int count = 5;
        // CompleteAsync holds each item's pipeline task pending, so every item dispatches and stays
        // in-flight (becomes the pending tail) without completing - in-flight Depth climbs to count. Under
        // in-flight depth the default sync-completing item drains before the next dispatches (Depth
        // caps at 1); holding the tasks pins all count items in-flight at once so the callback sequence
        // is deterministic.
        var items = new TestPipelineItem[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new TestPipelineItem { CompleteAsync = true };
            pipeline.Enqueue(items[i]).Signal();
        }

        // Wait until all count items are dispatched and in-flight (Depth == count).
        for (var i = 0; i < count; i++)
            await items[i].WaitForExecutedAsync();

        // Release head-first, one fully before the next, so callback ordering is deterministic.
        for (var i = 0; i < count; i++)
        {
            items[i].CompletePipelineTask();
            await items[i].WaitForCompleteAsync();
        }

        CollectionAssert.AreEqual(
            new[] { "complete", "complete", "complete", "complete", "complete", "idle" },
            events);
    }

    /// CompleteItem must latch on BOTH the trailing AND the pipeline task - not the trailing alone.
    /// An item whose trailing (write) settles first but whose pipeline (read) task is still in flight
    /// has NOT finished its pipelined phase: retiring it early hands the next item a wire whose
    /// terminating state (the RFQ a client reads at the end of the pipeline task) is unsettled. We
    /// settle trailing, leave the pipeline task pending, and assert the item stays in-flight until the
    /// pipeline task also settles.
    [TestMethod]
    public async Task CompleteItem_LatchesOnBothTasks_NotTrailingAlone()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(runEnqueueAsynchronously: true));

        // CompleteAsync => pipeline task is TCS-gated (CompletePipelineTask); HasTrailingTask =>
        // trailing task is separately TCS-gated (CompleteTrailingTask).
        var item = new TestPipelineItem { CompleteAsync = true, HasTrailingTask = true };
        pipeline.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();

        // Settle ONLY the trailing task. The pipeline task is still in flight.
        item.CompleteTrailingTask();

        Assert.IsFalse(await item.TryWaitForCompletedAsync(TimeSpan.FromMilliseconds(250)),
            "CompleteItem fired after trailing settled while the pipeline task was still pending - " +
            "completion latched on trailing alone instead of both tasks.");

        // Settling the pipeline task releases completion.
        item.CompletePipelineTask();
        await item.WaitForCompleteAsync();
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
        pipeline.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();

        var firstTask = pipeline.CompleteAsync(first).AsTask();
        var secondTask = pipeline.CompleteAsync(second).AsTask();

        await firstTask.WaitAsync(TimeSpan.FromSeconds(5));
        await secondTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(item.IsCompleted);
        // First-writer-wins is about the completion latch, not exception propagation: the
        // pipeline drains gracefully only, and the item settles via the shutdown token
        // (item-side escalation). The retired forceful sweep propagated _completionException.
        Assert.IsInstanceOfType<OperationCanceledException>(item.Exception);
    }

    /// CompleteAsync fires while the executor is awaiting an item's trailing execution task.
    /// Drain must let the trailing task complete (or observe its exception) before fully draining.
    [TestMethod]
    public async Task CompleteAsync_WithItemsMidTrailingTask_DrainsCleanly()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true, HasTrailingTask = true };
        pipeline.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();
        // Executor is now awaiting the trailing task.

        var completeTask = pipeline.CompleteAsync().AsTask();

        // Let trailing + pipeline tasks complete, drain proceeds.
        item.CompleteTrailingTask();
        item.CompletePipelineTask();

        await completeTask.WaitAsync(TimeSpan.FromSeconds(5));
        await item.WaitForCompleteAsync();
    }

    /// CompleteAsync fires with a pending tail waiter (item stored as _pendingTail, pipeline task
    /// still pending). CommitPendingTail at line 238 must commit the tail before DrainOnCompletionAsync,
    /// and the drain exception must reach the committed tail.
    [TestMethod]
    public async Task CompleteAsync_WithPendingPendingTail_DrainsTail()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));

        var item = new TestPipelineItem { CompleteAsync = true };
        pipeline.Enqueue(item).Signal();
        await item.WaitForExecutedAsync();
        // Item is now _pendingTail with a pending pipeline task.

        var ex = new InvalidOperationException("drain tail");
        await pipeline.CompleteAsync(ex).AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsTrue(item.IsCompleted, "Pending tail waiter should be completed by drain.");
        // Graceful-only drain: the committed tail settles via the shutdown token (item-side
        // escalation), not via _completionException (retired forceful-sweep contract).
        Assert.IsInstanceOfType<OperationCanceledException>(item.Exception);
    }

    /// OnExecutionIdleAsync throwing must propagate: executor catches, calls CompleteAsync(ex),
    /// drains remaining items, then re-throws so the execution task faults with the idle exception.
    [TestMethod]
    public async Task OnExecutionIdleAsync_Throws_FaultsCompleteAsync()
    {
        var idleEx = new InvalidOperationException("idle fault");
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(), onIdle: _ => throw idleEx);

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();
        await item.WaitForCompleteAsync();
        // Executor finishes item, then calls OnExecutionIdleAsync which throws.

        var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreSame(idleEx, ex);
    }

    /// A source pull surfacing OCE that carries the enumerator's own CompletionToken AND observed it
    /// fired is the idiomatic IAsyncEnumerator shutdown signal: swallowed, so the run completes cleanly
    /// rather than faulting. Pins both halves of the catch filter (token identity + actually fired).
    [TestMethod]
    public async Task SourcePull_OceWithFiredCompletionToken_TreatedAsCleanShutdown()
    {
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(), onIdle: token => { token.ThrowIfCancellationRequested(); return default; });

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();
        await item.WaitForCompleteAsync();
        // CompleteAsync fires the token; the next wait's onIdle observes it and throws the properly
        // tokened OCE, which the executor swallows as sanctioned shutdown - clean completion.
        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
    }

    /// An OCE carrying the CompletionToken while it has NOT fired is a mistranslated cancellation, not
    /// the shutdown idiom (the legitimate signal only ever throws after observing cancellation).
    /// Swallowing it walks the executor out of a live loop: the teardown then faults on the mid-flight
    /// state before the enumerator dispose, and the source wedges open, silently accepting enqueues
    /// nothing will ever pull. It must fault loudly with the origin exception instead.
    [TestMethod]
    public async Task SourcePull_OceWithUnfiredCompletionToken_Faults()
    {
        // The catch filter reads the token at CATCH time, so the test must not fire the token
        // (CompleteAsync does) while the throw is mid-unwind - that would legitimize the OCE and
        // it gets swallowed as clean shutdown. Sequencing without a race: the fault teardown
        // disposes the enumerator, which cancels the completion token, and teardown runs strictly
        // after the filter evaluated. Registering on the token before throwing therefore yields a
        // signal that fires only once the fault is fully latched; CompleteAsync afterward must
        // surface it.
        var settled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(), onIdle: token =>
            {
                token.Register(() => settled.TrySetResult());
                throw new OperationCanceledException(token);
            });

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();
        await item.WaitForCompleteAsync();
        await settled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
    }

    /// A source pull surfacing OCE carrying a FOREIGN token is not the sanctioned signal - it must
    /// fault the run and surface via CompleteAsync, not be swallowed. The inverse guard for the narrowing.
    [TestMethod]
    public async Task SourcePull_ForeignOce_Faults()
    {
        using var foreign = new CancellationTokenSource();
        foreign.Cancel();
        var pipeline = ObservablePipeline.Create<TestPipelineItem, TestPipelinePolicy>(
            new(), onIdle: _ => throw new OperationCanceledException(foreign.Token));

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();
        await item.WaitForCompleteAsync();

        var ex = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreEqual(foreign.Token, ex.CancellationToken);
    }

    /// When the loop faults AND the finally-teardown (DisposeAsync) also throws, the captured root
    /// cause must not be masked by the teardown throw - both surface (root preserved, teardown folded).
    [TestMethod]
    public async Task ExecutorFault_TeardownThrow_RootCausePreserved()
    {
        var rootEx = new InvalidOperationException("root: source WaitForNextAsync");
        var teardownEx = new InvalidOperationException("teardown: DisposeAsync");
        var source = ThrowingSource<TestPipelineItem>.Create(waitThrow: rootEx, disposeThrow: teardownEx);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy, ThrowingSource<TestPipelineItem>, ThrowingSource<TestPipelineItem>.Enumerator>(new(), source);

        var ex = await Assert.ThrowsExactlyAsync<AggregateException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        var inner = ex.Flatten().InnerExceptions;
        CollectionAssert.Contains(inner, rootEx, "root cause must survive the teardown throw");
        CollectionAssert.Contains(inner, teardownEx, "teardown throw should be folded in, not lost");
    }

    [TestMethod]
    public async Task TryGetNextFault_ReuseResetsPullingState()
    {
        var rootEx = new InvalidOperationException("root: source TryGetNext");
        var source = ThrowingSource<TestPipelineItem>.Create(pullThrow: rootEx);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy, ThrowingSource<TestPipelineItem>,
            ThrowingSource<TestPipelineItem>.Enumerator>(new(), source);

        var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));

        Assert.AreSame(rootEx, ex);

        var reused = Pipeline.Create<TestPipelineItem, TestPipelinePolicy, ThrowingSource<TestPipelineItem>,
            ThrowingSource<TestPipelineItem>.Enumerator>(new(), ThrowingSource<TestPipelineItem>.Create(), pipeline);
        Assert.AreSame(pipeline, reused);
        Assert.IsTrue(reused.IsEmpty(0), "a new run must not inherit transient pull ownership");
        await reused.Completion;
    }

    /// A source fault while an item is still in flight must still drain that item (its CompleteItem
    /// fires) rather than strand it, and then surface the root fault via CompleteAsync.
    [TestMethod]
    public async Task ExecutorFault_DrainsInFlightItem()
    {
        var rootEx = new InvalidOperationException("root: source fault while item in flight");
        var item = new TestPipelineItem { CompleteAsync = true }; // pending pipeline task => in flight
        var source = ThrowingSource<TestPipelineItem>.Create(items: new[] { item }, gateWait: true);
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy, ThrowingSource<TestPipelineItem>, ThrowingSource<TestPipelineItem>.Enumerator>(new(), source);

        await item.WaitForExecutedAsync();   // dispatched, pipeline task pending, executor parked on the gated wait
        source.TriggerWaitThrow(rootEx);     // fault lands while the item is in flight
        item.CompletePipelineTask();         // item retires during the faulted drain -> CompleteItem fires

        var ex = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () => await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreSame(rootEx, ex);
        await item.WaitForCompleteAsync();   // drained on the faulted exit, not stranded
    }

    /// WaitForEmptyAsync called after CompleteAsync has fully drained should return a completed
    /// task immediately (depth is 0, drain TCS is null because no one was waiting during drain).
    [TestMethod]
    public async Task WaitForEmptyAsync_AfterCompleteAsync_CompletesImmediately()
    {
        var pipeline = Pipeline.Create<TestPipelineItem, TestPipelinePolicy>(new(true));
        await pipeline.CompleteAsync();

        var task = pipeline.WaitForEmptyAsync();
        Assert.IsTrue(task.IsCompleted, "WaitForEmptyAsync after CompleteAsync should be immediately completed.");
    }

    struct IdleCapturingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly List<string> _events;

        public IdleCapturingPolicy(List<string> events) => _events = events;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            // Carry the item's pipeline task so CompleteAsync items pend (held in-flight) until
            // CompletePipelineTask releases them; default(CompleteAsync=false) stays sync-complete.
            return new(new PipelineItemResult(item.GetPipelineTask()));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();

        public void CompleteItem(TestPipelineItem item, Exception? exception)
        {
            _events.Add("complete");
            item.Complete(exception);
        }

        public void OnIdle() => _events.Add("idle");

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }
    }

    /// CompleteAsync invoked from inside ExecuteItemAsync (reentrant shutdown). The executor
    /// is mid-await on the policy's task when CompleteAsync sets _completing and signals the
    /// wake. The item completes normally, the inner loop exits on _wakeSignal.IsCompleted,
    /// and the execution task drains. Discard the returned ValueTask: awaiting it from inside
    /// the executor would self-deadlock.
    [TestMethod]
    public async Task CompleteAsync_FromInsideExecuteItemAsync_DrainsCleanly()
    {
        UnboundedPipeline<TestPipelineItem, ReentrantCompletePolicy>? pipelineRef = null;
        var pipeline = Pipeline.Create<TestPipelineItem, ReentrantCompletePolicy>(
            new ReentrantCompletePolicy(() => _ = pipelineRef!.CompleteAsync()));
        pipelineRef = pipeline;

        var item = new TestPipelineItem();
        pipeline.Enqueue(item).Signal();

        await item.WaitForCompleteAsync();
        // Outside-thread CompleteAsync returns the same execution task. Awaiting confirms drain.
        await pipeline.CompleteAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsNull(item.Exception);
    }

    struct ReentrantCompletePolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly Action _onExecute;
        public ReentrantCompletePolicy(Action onExecute) => _onExecute = onExecute;

        public ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            _onExecute();
            return new(new PipelineItemResult(default));
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, Exception? exception) => item.Complete(exception);

        public void OnIdle() { }

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public bool RunEnqueueAsynchronously => true;
    }

    /// Policy that awaits CompletionToken cancellation from ExecuteItemAsync. Signals a TCS when the
    /// cancellation is observed. (The idle-side observation now lives on the source's onIdle hook.)
    struct TokenObservingPolicy : IPipelinePolicy<TestPipelineItem>
    {
        readonly TaskCompletionSource? _executeTokenSink;

        public TokenObservingPolicy(TaskCompletionSource? executeTokenSink = null)
        {
            _executeTokenSink = executeTokenSink;
        }

        public async ValueTask<PipelineItemResult> ExecuteItemAsync(TestPipelineItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
        {
            item.SignalExecuted();
            if (_executeTokenSink is not null)
            {
                try { await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { _executeTokenSink.TrySetResult(); throw; }
            }
            return new PipelineItemResult(default);
        }

        public void ActivateHeadItem(TestPipelineItem item, bool preferAsync = true) => item.Activate();
        public void CompleteItem(TestPipelineItem item, Exception? exception) => item.Complete(exception);

        public void OnIdle() { }

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, TestPipelineItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TestPipelineItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public bool RunEnqueueAsynchronously => true;
    }
}
