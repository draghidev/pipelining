using System.Collections.Concurrent;

namespace Draghi.Pipelining.Tests;

[TestClass]
public class ValueTPipelineTests
{
    /// Smoke test for value-type T: pushes items through the full pipeline lifecycle and verifies
    /// activation + completion fire. Exercises the publish-pair fence paths that differ from ref-T
    /// (no GC write barrier on _executingItem / _tailWaiter), the Volatile.Write on _executingItemActivationPending
    /// at line 209, and the Volatile.Write on _hasTailWaiter publish.
    [TestMethod]
    public async Task ValueTypeT_BasicLifecycle()
    {
        var pool = new ValueItemPool();
        var pipeline = Pipeline.Create<ValueItem, ValueItemPolicy>(new(pool));

        const int count = 10;
        var ids = new int[count];
        for (var i = 0; i < count; i++)
        {
            ids[i] = pool.Allocate();
            pipeline.Enqueue(new ValueItem(ids[i])).Execute();
        }

        for (var i = 0; i < count; i++)
            await pool.Get(ids[i]).WaitForCompleteAsync();

        for (var i = 0; i < count; i++)
        {
            Assert.IsTrue(pool.Get(ids[i]).Activated);
            Assert.IsTrue(pool.Get(ids[i]).Completed);
        }
        Assert.AreEqual(0, pipeline.Depth);
    }

    /// Enumeration with value-type T. Exercises the Enumerator's reads of _tailWaiter / _waiters
    /// for value-T (no GC barrier on the ref write side). After the executor suspends, all items should
    /// be observable via the enumerator in enqueue order.
    [TestMethod]
    public async Task ValueTypeT_EnumeratorYieldsAllItems()
    {
        var pool = new ValueItemPool();
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<ValueItem, ValueItemPolicy>(
            new(pool),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int count = 6;
        var ids = new int[count];
        for (var i = 0; i < count; i++)
        {
            ids[i] = pool.Allocate(completeAsync: true);
            pipeline.Enqueue(new ValueItem(ids[i])).Execute();
        }

        // Wait for executor to settle so the tail is committed to _waiters with the Volatile.Write
        // publish fence observed. Enumerator gets a consistent view.
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var observed = new List<int>();
        foreach (var item in pipeline)
            observed.Add(item.Id);

        Assert.AreEqual(count, observed.Count, "Enumerator should yield every in-flight item.");
        for (var i = 0; i < count; i++)
            Assert.AreEqual(ids[i], observed[i], $"Item at position {i} should match enqueue order.");

        // Drain.
        for (var i = 0; i < count; i++)
            pool.Get(ids[i]).CompletePipelineTask();
        for (var i = 0; i < count; i++)
            await pool.Get(ids[i]).WaitForCompleteAsync();
    }

    /// DOCUMENTS a cross-cycle tearing limitation that affects enumerator-style readers racing
    /// the writer thread for large value-T. Two sites have this shape:
    ///
    /// 1. Pipeline.Enumerator reading `_tailWaiter`: the publish-pair fence on `_hasTailWaiter`
    ///    (Volatile.Write/Read) prevents single-cycle tearing, but across cycles the executor's
    ///    PLAIN clear of `_hasTailWaiter=false` may not propagate before the reader's Volatile.Read
    ///    sees the prior cycle's `true`, the reader then PLAIN-reads a multi-word `_tailWaiter`
    ///    that the executor is mid-writing for the next cycle.
    ///
    /// 2. SPSC.Enumerator reading an `_array[i]` slot: TryDequeue clears the slot to default after
    ///    reading (`array[first] = default!`, multi-word for large value-T). If the Enumerator
    ///    snapshots `_first` and reads `_array[i]` while the consumer is mid-clear, same torn read.
    ///
    /// The PRIMARY functions of both, single SPSC consumer reading slots that were written once
    /// before the producer's release of `_last`, and the executor itself reading its own
    /// `_tailWaiter`, are NOT affected: slots are read after a release-acquire pair, and
    /// `_tailWaiter` is single-thread for its primary reader.
    ///
    /// Enable manually to verify, or use as the template for a future snapshot-via-CAS publishing
    /// pattern that would close both races.
    [TestMethod]
    [Ignore("Cross-cycle Enumerator tearing on _tailWaiter for large value-T. Enable to reproduce.")]
    public async Task LargeValueTypeT_EnumeratorTailWaiterCrossCycleTearing()
    {
        var pool = new ValueItemPool();
        var pipeline = Pipeline.Create<LargeValueItem, LargeValueItemPolicy>(new(pool));

        const int producerItems = 1000;
        var producerDone = false;
        var producer = Task.Run(() =>
        {
            for (var i = 0; i < producerItems; i++)
            {
                var id = pool.Allocate();
                pipeline.Enqueue(new LargeValueItem(id, id, id, id)).Execute();
            }
            producerDone = true;
        });

        var tearObserved = false;
        var enumerator = Task.Run(() =>
        {
            while (!producerDone || pipeline.Depth > 0)
            {
                foreach (var item in pipeline)
                {
                    if (item.A != item.B || item.A != item.C || item.A != item.D)
                    {
                        tearObserved = true;
                        return;
                    }
                }
            }
        });

        await producer;
        await enumerator;

        // Expected today: tearObserved=true. Future fix would make this assertion pass cleanly.
        Assert.IsFalse(tearObserved, "Slot write torn — expected for large value-T without a snapshot-publishing fix.");
    }

    /// Value-type T with async pipeline tasks - exercises the deferred-publish + _tailWaiter
    /// path for value-T. Tests the publish-pair fence ordering under load.
    [TestMethod]
    public async Task ValueTypeT_DeferredPublishAndDrain()
    {
        var pool = new ValueItemPool();
        var idleTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = ObservablePipeline.Create<ValueItem, ValueItemPolicy>(
            new(pool),
            onIdle: _ => { idleTcs.TrySetResult(); return default; });

        const int count = 5;
        var ids = new int[count];
        for (var i = 0; i < count; i++)
        {
            ids[i] = pool.Allocate(completeAsync: true);
            pipeline.Enqueue(new ValueItem(ids[i])).Execute();
        }

        // Wait until the executor suspends (all items in _waiters with callbacks registered).
        await idleTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Complete pipeline tasks in order.
        for (var i = 0; i < count; i++)
            pool.Get(ids[i]).CompletePipelineTask();

        for (var i = 0; i < count; i++)
            await pool.Get(ids[i]).WaitForCompleteAsync();

        Assert.AreEqual(0, pipeline.Depth);
    }
}

/// Value-type pipeline item. Holds only an ID. Per-item state lives in a side pool so the
/// struct can be copied freely without losing observable state.
readonly record struct ValueItem(int Id);

/// Per-item mutable state for value-type T tests. Indexed by ID from the pool.
sealed class ValueItemState
{
    public bool CompleteAsync;
    public bool Activated;
    public bool Completed;
    public readonly ManualResetEventSlim _completed = new(false);
    public readonly TaskCompletionSource _pipelineTaskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask GetPipelineTask() => CompleteAsync ? new(_pipelineTaskTcs.Task) : default;

    public void CompletePipelineTask() => _pipelineTaskTcs.SetResult();

    public Task WaitForCompleteAsync()
    {
        if (_completed.IsSet) return Task.CompletedTask;
        return Task.Run(() => Assert.IsTrue(_completed.Wait(TimeSpan.FromSeconds(5)), "Timed out waiting for completion."));
    }

    public void Activate() => Activated = true;
    public void Complete()
    {
        Completed = true;
        _completed.Set();
    }
}

sealed class ValueItemPool
{
    readonly ConcurrentDictionary<int, ValueItemState> _states = new();
    int _next;

    public int Allocate(bool completeAsync = false)
    {
        var id = Interlocked.Increment(ref _next);
        _states[id] = new ValueItemState { CompleteAsync = completeAsync };
        return id;
    }

    public ValueItemState Get(int id) => _states[id];
}

readonly struct ValueItemPolicy(ValueItemPool pool) : IPipelinePolicy<ValueItem>
{
    public ValueTask<PipelineItemResult> ExecuteItemAsync(ValueItem item, CancellationToken cancellationToken)
        => new(new PipelineItemResult(default, pool.Get(item.Id).GetPipelineTask()));

    public void ActivateHeadItem(ValueItem item, bool preferAsync = true) => pool.Get(item.Id).Activate();

    public void CompleteItem(ValueItem item, int remainingDepth, Exception? exception) => pool.Get(item.Id).Complete();

    public bool TryRecoverItemFailure(in PipelineItemFailureContext context, ValueItem failedItem, CancellationToken cancellationToken, out ValueItem recoveryItem)
    {
        recoveryItem = default;
        return false;
    }

    public bool RunEnqueueAsynchronously => true;
}

/// Large value-T (4 longs = 32 bytes, well beyond native word size). All four fields are set to
/// the same value at construction. A torn read will observe them unequal.
readonly record struct LargeValueItem(long A, long B, long C, long D);

readonly struct LargeValueItemPolicy(ValueItemPool pool, Action<long, long, long, long>? onComplete = null) : IPipelinePolicy<LargeValueItem>
{
    public ValueTask<PipelineItemResult> ExecuteItemAsync(LargeValueItem item, CancellationToken cancellationToken)
        => new(new PipelineItemResult(default, default));

    public void ActivateHeadItem(LargeValueItem item, bool preferAsync = true) { }

    public void CompleteItem(LargeValueItem item, int remainingDepth, Exception? exception)
    {
        onComplete?.Invoke(item.A, item.B, item.C, item.D);
        pool.Get((int)item.A).Complete();
    }

    public bool TryRecoverItemFailure(in PipelineItemFailureContext context, LargeValueItem failedItem, CancellationToken cancellationToken, out LargeValueItem recoveryItem)
    {
        recoveryItem = default;
        return false;
    }

    public bool RunEnqueueAsynchronously => true;
}
