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
sealed class PipelineChannel<T>
{
    readonly UnboundedPipeline<T, Policy> _pipeline;
    // Single-consumer: one slot for the ready item. _hasReady is the readiness indicator
    // so the slot can hold any T (including default for value types).
    T _readyItem = default!;
    bool _hasReady;
    // Reader signal — wakes a suspended ReadAsync.
    ManualResetValueTaskSourceCore<bool> _readerSignal;
    bool _readerWaiting;

    public PipelineChannel(bool runContinuationsAsynchronously = false)
    {
        // runContinuationsAsynchronously:false resumes the parked executor inline on the Write()
        // caller's thread, so the item is completed (and lands in the ready slot) before Write
        // returns. That is what lets the WriteReadSync benchmark observe a synchronous ReadAsync.
        // With true the wake hops the scheduler and Write returns before completion.
        _pipeline = Pipeline.Create<T, Policy>(new Policy(this), runContinuationsAsynchronously: runContinuationsAsynchronously);
    }

    public void Write(T item)
    {
        _pipeline.Enqueue(item).Execute();
    }

    /// Delivers a completed item to a waiting reader or stores it.
    /// Called by the policy's CompleteItem — runs on the execution loop (single-threaded).
    void OnItemCompleted(T item)
    {
        _readyItem = item;
        _hasReady = true;
        if (_readerWaiting)
        {
            _readerWaiting = false;
            _readerSignal.SetResult(true);
        }
    }

    /// Returns the next completed item, or suspends until one is available.
    public ValueTask<T> ReadAsync()
    {
        if (_hasReady)
        {
            var item = _readyItem;
            _readyItem = default!;
            _hasReady = false;
            return new ValueTask<T>(item);
        }

        // No item ready, suspend until OnItemCompleted signals.
        _readerSignal.Reset();
        _readerWaiting = true;

        // Double-check after setting the flag.
        if (_hasReady)
        {
            var item = _readyItem;
            _readyItem = default!;
            _hasReady = false;
            _readerWaiting = false;
            return new ValueTask<T>(item);
        }

        return new(new ReaderAwaitable(this), _readerSignal.Version);
    }

    public ValueTask CompleteAsync() => _pipeline.CompleteAsync();

    sealed class ReaderAwaitable(PipelineChannel<T> channel) : IValueTaskSource<T>
    {
        public T GetResult(short token)
        {
            channel._readerSignal.GetResult(token);
            var item = channel._readyItem;
            channel._readyItem = default!;
            channel._hasReady = false;
            return item;
        }

        public ValueTaskSourceStatus GetStatus(short token) => channel._readerSignal.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => channel._readerSignal.OnCompleted(continuation, state, token, flags);
    }

    struct Policy(PipelineChannel<T> channel) : IPipelinePolicy<T>
    {
        public ValueTask<PipelineItemResult> ExecuteItemAsync(T item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
            => new(new PipelineItemResult(ValueTask.CompletedTask));

        public void ActivateHeadItem(T item, bool preferAsync = true) { }

        public void CompleteItem(T item, int remainingDepth, Exception? exception)
            => channel.OnItemCompleted(item);

        // Override IPipelinePolicy DIM methods explicitly. DIM dispatch through an interface
        // call on a struct boxes the struct on every call. Explicit overrides avoid the box.
        public bool TryRecoverItemFailure(PipelineItemFailureContext context, T failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out T? recoveryItem)
        {
            recoveryItem = default;
            return false;
        }
    }
}

[IterationCount(15)]
[MemoryDiagnoser]
public class PipelineChannelBenchmarks
{
    PipelineChannel<object> _channel = null!;
    object _item = null!;

    [Params(false)]
    public bool RunAsync { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _channel = new PipelineChannel<object>(RunAsync);
        _item = new object();
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
        _channel.Write(_item);
        // Item completes synchronously, it's already in the ready slot.
        var read = _channel.ReadAsync();
        if (!read.IsCompletedSuccessfully)
            throw new Exception("Expected synchronous completion");
    }
}
