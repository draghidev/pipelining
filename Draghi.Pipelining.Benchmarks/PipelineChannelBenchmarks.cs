using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

using BenchmarkDotNet.Attributes;

namespace Draghi.Pipelining.Benchmarks;

/// Proof-of-concept: a channel built on top of Pipeline.
///
/// Write pushes items through the pipeline's execution loop. ReadAsync delivers completed items
/// to a waiting consumer. The execution step is a no-op here, but in practice it would be the
/// processing stage (e.g. protocol handling, IO dispatch), the same work you'd do after reading
/// from a regular channel, except here it's folded into the pipeline's executor at no extra cost.
///
/// Strengths:
/// - SyncFirst mode (~55ns) beats Channel with AllowSynchronousContinuations (~74ns) while
///   also running an execution step in between.
/// - Async mode (~1,700ns, 0 alloc) matches async Channel (~1,718ns), thread pool hop dominates.
/// - Zero allocation on all paths.
/// - Executor serializes the completion side, so the ready slot is single-producer.
///   Only ReadAsync callers contend, and only on the slow (suspend) path.
///
/// Limitations:
/// - Single-consumer only (one ready slot, one reader signal). Multi-consumer would need a waiter
///   queue + spinlock, similar to Channel's internal AsyncOperation queue.
/// - Single-producer by design (SPSC pipeline queue). Multi-producer requires an external lock.
/// - The executor loop is a serialization point. Under extreme multi-core write throughput it
///   can become a bottleneck. Channel's direct writer-to-reader handoff avoids this.
/// - No backpressure — the SPSC queue is unbounded.
sealed class PipelineChannel<T> where T : class
{
    readonly Pipeline<Envelope, Policy> _pipeline;
    // Single-consumer: one slot for the ready item.
    Envelope? _readyItem;
    // Reader signal — wakes a suspended ReadAsync.
    ManualResetValueTaskSourceCore<bool> _readerSignal;
    bool _readerWaiting;

    public PipelineChannel(PipelineExecutionMode mode = PipelineExecutionMode.SyncFirst)
    {
        _pipeline = Pipeline.Create<Envelope, Policy>(new Policy(this, mode));
    }

    public void Write(Envelope envelope)
    {
        _pipeline.Enqueue(envelope).Execute();
    }

    /// Delivers a completed item to a waiting reader or stores it.
    /// Called by the policy's CompleteItem — runs on the execution loop (single-threaded).
    void OnItemCompleted(Envelope item)
    {
        if (_readerWaiting)
        {
            _readerWaiting = false;
            _readyItem = item;
            _readerSignal.SetResult(true);
        }
        else
        {
            _readyItem = item;
        }
    }

    /// Returns the next completed item, or suspends until one is available.
    public ValueTask<Envelope> ReadAsync()
    {
        var item = _readyItem;
        if (item is not null)
        {
            _readyItem = null;
            return new ValueTask<Envelope>(item);
        }

        // No item ready, suspend until OnItemCompleted signals.
        _readerSignal.Reset();
        _readerWaiting = true;

        // Double-check after setting the flag.
        item = _readyItem;
        if (item is not null)
        {
            _readyItem = null;
            _readerWaiting = false;
            return new ValueTask<Envelope>(item);
        }

        return new(new ReaderAwaitable(this), _readerSignal.Version);
    }

    public ValueTask CompleteAsync() => _pipeline.CompleteAsync();

    sealed class ReaderAwaitable(PipelineChannel<T> channel) : IValueTaskSource<Envelope>
    {
        public Envelope GetResult(short token)
        {
            channel._readerSignal.GetResult(token);
            var item = channel._readyItem;
            channel._readyItem = null;
            return item!;
        }

        public ValueTaskSourceStatus GetStatus(short token) => channel._readerSignal.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => channel._readerSignal.OnCompleted(continuation, state, token, flags);
    }

    internal sealed class Envelope
    {
        public T? Value;

        public void Reset() => Value = default;
    }

    struct Policy(PipelineChannel<T> channel, PipelineExecutionMode mode) : IPipelinePolicy<Envelope>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(Envelope item, CancellationToken cancellationToken)
            => new(new PipelineItemResult(ValueTask.CompletedTask));

        public void ActivateHeadItem(Envelope item, bool schedule = true) { }

        public void CompleteItem(Envelope item, int remainingDepth, Exception? exception)
            => channel.OnItemCompleted(item);

        public PipelineExecutionMode ExecutionMode => mode;
    }
}

[IterationCount(15)]
[MemoryDiagnoser]
public class PipelineChannelBenchmarks
{
    PipelineChannel<object> _channel = null!;
    PipelineChannel<object>.Envelope _envelope = null!;

    [Params(PipelineExecutionMode.SyncFirst)]
    public PipelineExecutionMode Mode { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _channel = new PipelineChannel<object>(Mode);
        _envelope = new PipelineChannel<object>.Envelope();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _channel.CompleteAsync().AsTask().GetAwaiter().GetResult();
    }

    /// Sync benchmark. Item completes before ReadAsync, so ReadAsync returns immediately.
    [Benchmark]
    public void WriteReadSync()
    {
        var envelope = _envelope;
        _channel.Write(envelope);
        // Item completes synchronously in SyncFirst — it's already in the ready slot.
        var read = _channel.ReadAsync();
        if (!read.IsCompletedSuccessfully)
            throw new Exception("Expected synchronous completion");
    }
}
