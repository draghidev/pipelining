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
    Pipeline<BareItem, BarePolicy> _pipeline = null!;
    BareItem _item = null!;

    [Params(PipelineExecutionMode.SyncFirst, PipelineExecutionMode.Async)]
    public PipelineExecutionMode Mode { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pipeline = Pipeline.Create<BareItem, BarePolicy>(new BarePolicy(Mode));
        _item = new BareItem();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _pipeline.CompleteAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark]
    public void EnqueueComplete()
    {
        var item = _item;
        item.Reset();
        _pipeline.Enqueue(item).Execute();
        item.Wait();
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

/// <summary>
/// Minimal policy that immediately completes items during execution.
/// No trailing work, no pipelining phase, just the fastest possible round-trip.
/// </summary>
struct BarePolicy(PipelineExecutionMode mode) : IPipelinePolicy<BareItem>
{
    public ValueTask<PipelineItemResult> ExecuteItemAsync(BareItem item, CancellationToken cancellationToken)
    {
        return new(new PipelineItemResult(ValueTask.CompletedTask));
    }

    public void ActivateHeadItem(BareItem item, bool schedule = true) { }

    public void CompleteItem(BareItem item, int remainingDepth, Exception? exception)
    {
        item.SignalComplete();
    }

    public BareItem? TryRecoverItemFailure(PipelineItemFailureContext context, BareItem failedItem, CancellationToken cancellationToken)
        => null;

    public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

    public PipelineExecutionMode ExecutionMode => mode;
}
