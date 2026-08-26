using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Draghi.Pipelining.Benchmarks;

/// <summary>
/// Benchmarks the raw Pipeline overhead using a minimal item type,
/// bypassing the ProtocolFlow state machine entirely.
/// </summary>
[IterationCount(15)]
[MemoryDiagnoser]
public class PipelineBenchmarks
{
    UnboundedPipeline<BareItem, BarePolicy> _pipeline = null!;
    BareItem _item = null!;

    [Params(false, true)]
    public bool RunAsync { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // RunAsync selects the source wake mode. With runContinuationsAsynchronously:false the
        // Signal() in Execute() resumes the parked executor inline on the producer thread, so the
        // whole enqueue->execute->complete round-trip runs synchronously (the ~60ns path). With
        // true, the wake dispatches to the scheduler (a thread-pool hop per item, ~2us).
        _pipeline = Pipeline.Create<BareItem, BarePolicy>(new BarePolicy(), runContinuationsAsynchronously: RunAsync);
        _item = new BareItem();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        // Null-guarded: targeted GlobalSetups mean only one of these exists per benchmark.
        _pipeline?.CompleteAsync().AsTask().GetAwaiter().GetResult();
        _structPipeline?.CompleteAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark]
    public void EnqueueComplete()
    {
        var item = _item;
        item.Reset();
        _pipeline.Enqueue(item).Execute();
        item.Wait();
    }

    const int BurstSize = 100;
    BareItem[] _burstItems = null!;

    [GlobalSetup(Target = nameof(EnqueueBurst))]
    public void SetupBurst()
    {
        Setup();
        _burstItems = new BareItem[BurstSize];
        for (var i = 0; i < _burstItems.Length; i++)
            _burstItems[i] = new BareItem();
    }

    /// Backlogged per-item cost: all items are enqueued before a single wake, so the executor
    /// drains the whole batch and every pull after the first finds an item already queued (the
    /// sync MoveNextAsync path). EnqueueComplete by contrast parks the executor on every item,
    /// so it never exercises this path. Per-op numbers are per item via OperationsPerInvoke.
    [Benchmark(OperationsPerInvoke = BurstSize)]
    public void EnqueueBurst()
    {
        var items = _burstItems;
        for (var i = 0; i < items.Length - 1; i++)
        {
            items[i].Reset();
            _pipeline.Enqueue(items[i]);
        }
        var last = items[items.Length - 1];
        last.Reset();
        // Single wake drains the backlog. Items complete in order on the sync path, so waiting
        // on the last is waiting on all.
        _pipeline.Enqueue(last).Execute();
        last.Wait();
    }

    UnboundedPipeline<BareInFlightItem, BareInFlightPolicy> _inFlightPipeline = null!;
    BareInFlightItem _inFlightItem = null!;

    [GlobalSetup(Target = nameof(EnqueueCompleteEscalated))]
    public void SetupEscalated()
    {
        _inFlightPipeline = Pipeline.Create<BareInFlightItem, BareInFlightPolicy>(new BareInFlightPolicy(), runContinuationsAsynchronously: RunAsync);
        // Force escalation once (it is sticky): two overlapping waiter commits move the store to
        // the queue tier and allocate the activation lock, the regime a long-lived pipeline runs
        // in permanently.
        var a = new BareInFlightItem();
        var b = new BareInFlightItem();
        _inFlightPipeline.Enqueue(a).Execute();
        _inFlightPipeline.Enqueue(b).Execute();
        a.CompletePipelineTask();
        b.CompletePipelineTask();
        a.Wait();
        b.Wait();
        _inFlightItem = new BareInFlightItem();
    }

    [GlobalCleanup(Target = nameof(EnqueueCompleteEscalated))]
    public void CleanupEscalated()
        => _inFlightPipeline.CompleteAsync().AsTask().GetAwaiter().GetResult();

    /// Escalated sequential per-item cost: the item enters the in-flight store (its pipeline task is
    /// pending at commit, completing afterwards like a wire round-trip) on an escalated store.
    /// This is the shape that pays the dispatch, commit, and drain coordination per item, which
    /// EnqueueComplete's inline-completing items never reach.
    [Benchmark]
    public void EnqueueCompleteEscalated()
    {
        var item = _inFlightItem;
        item.Reset();
        _inFlightPipeline.Enqueue(item).Execute();
        item.WaitExecuted();
        item.CompletePipelineTask();
        item.Wait();
    }

    UnboundedPipeline<BareItemHandle, BareHandlePolicy> _structPipeline = null!;

    [GlobalSetup(Target = nameof(EnqueueCompleteStructT))]
    public void SetupStructT()
    {
        _structPipeline = Pipeline.Create<BareItemHandle, BareHandlePolicy>(new BareHandlePolicy(), runContinuationsAsynchronously: RunAsync);
        _item = new BareItem();
    }

    /// Forced-specialization A/B: T is a single-ref struct wrapper, so the instantiation is fully
    /// specialized (struct type arguments never share __Canon code) - every constrained call on the
    /// enumerator inlines and the generic-dictionary/instantiating-stub dispatch the ref-T shape
    /// pays disappears. The delta against EnqueueComplete is the shared-generics tax.
    [Benchmark]
    public void EnqueueCompleteStructT()
    {
        var item = _item;
        item.Reset();
        _structPipeline.Enqueue(new BareItemHandle(item)).Execute();
        item.Wait();
    }
}

readonly struct BareItemHandle(BareItem item)
{
    public readonly BareItem Item = item;
}

struct BareHandlePolicy : IPipelinePolicy<BareItemHandle>
{
    public ValueTask<PipelineItemResult> ExecuteItemAsync(BareItemHandle item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
        => new(new PipelineItemResult(ValueTask.CompletedTask));

    public void ActivateHeadItem(BareItemHandle item, bool preferAsync = true) { }

    public void CompleteItem(BareItemHandle item, int remainingDepth, Exception? exception)
        => item.Item.SignalComplete();

    public bool TryRecoverItemFailure(in PipelineItemFailureContext context, BareItemHandle failedItem, CancellationToken cancellationToken, out BareItemHandle recoveryItem)
    {
        recoveryItem = default;
        return false;
    }
}

/// <summary>
/// Minimal pipeline item, no ProtocolFlow, no CAS state machine, no ValueTaskSource.
/// Just a ManualResetEventSlim for synchronous waiting.
/// </summary>
sealed class BareItem
{
    ManualResetEventSlim _completed = new(false);

    public void SignalComplete() => _completed.Set();

    public void Wait() => _completed.Wait();

    public void Reset() => _completed.Reset();
}

/// In-flight item whose pipeline task is pending at commit and completes afterwards.
/// Reusable through the versioned source, zero alloc per op.
sealed class BareInFlightItem : System.Threading.Tasks.Sources.IValueTaskSource
{
    // RunContinuationsAsynchronously stays false (the default), and it is load-bearing: the
    // pipeline's commit registers its completion callback on this task, so false runs the drain
    // inline on the thread that calls CompletePipelineTask. True would dispatch the drain through
    // the thread pool, poisoning any bench comparing dispatch backends.
    System.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _source;
    ManualResetEventSlim _completed = new(false);
    ManualResetEventSlim _executed = new(false);

    public ValueTask PipelineTask => new(this, _source.Version);

    public void SignalExecuted() => _executed.Set();

    // The drain thread is part of what these benches measure. Waiting for dispatch before
    // completing keeps the commit-vs-complete race from routing the drain onto the executor's
    // thread on some iterations (the commit fires the callback inline when the task is already
    // completed at commit time), so every iteration drains on this caller's thread.
    public void WaitExecuted() => _executed.Wait();

    public void CompletePipelineTask() => _source.SetResult(true);

    public void SignalComplete() => _completed.Set();

    public void Wait() => _completed.Wait();

    public void Reset()
    {
        _completed.Reset();
        _executed.Reset();
        _source.Reset();
    }

    void System.Threading.Tasks.Sources.IValueTaskSource.GetResult(short token) => _source.GetResult(token);

    System.Threading.Tasks.Sources.ValueTaskSourceStatus System.Threading.Tasks.Sources.IValueTaskSource.GetStatus(short token)
        => _source.GetStatus(token);

    void System.Threading.Tasks.Sources.IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, System.Threading.Tasks.Sources.ValueTaskSourceOnCompletedFlags flags)
        => _source.OnCompleted(continuation, state, token, flags);
}

struct BareInFlightPolicy : IPipelinePolicy<BareInFlightItem>
{
    public ValueTask<PipelineItemResult> ExecuteItemAsync(BareInFlightItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
    {
        item.SignalExecuted();
        return new(new PipelineItemResult(item.PipelineTask));
    }

    public void ActivateHeadItem(BareInFlightItem item, bool preferAsync = true) { }

    public void CompleteItem(BareInFlightItem item, int remainingDepth, Exception? exception)
        => item.SignalComplete();

    public bool TryRecoverItemFailure(in PipelineItemFailureContext context, BareInFlightItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out BareInFlightItem? recoveryItem)
    {
        recoveryItem = null;
        return false;
    }
}

/// <summary>
/// Minimal policy that immediately completes items during execution.
/// No trailing work, no pipelining phase, just the fastest possible round-trip.
/// </summary>
struct BarePolicy : IPipelinePolicy<BareItem>
{
    public ValueTask<PipelineItemResult> ExecuteItemAsync(BareItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
    {
        return new(new PipelineItemResult(ValueTask.CompletedTask));
    }

    public void ActivateHeadItem(BareItem item, bool preferAsync = true) { }

    public void CompleteItem(BareItem item, int remainingDepth, Exception? exception)
    {
        item.SignalComplete();
    }

    public bool TryRecoverItemFailure(in PipelineItemFailureContext context, BareItem failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out BareItem? recoveryItem)
    {
        recoveryItem = null;
        return false;
    }
}
