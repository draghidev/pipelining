using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// <summary>
/// Single-producer single-consumer queue-backed source. Wraps an SPSC queue plus a wake signal
/// and exposes an <see cref="Enqueue"/> method that the producer calls to feed items into the
/// pipeline.
/// </summary>
/// <remarks>
/// This is a struct so it composes as a generic source argument without boxing. It holds a
/// reference-typed inner state object. Copies of the struct share the same producer/consumer
/// queue and wake signal.
/// <para>
/// Construct with <see cref="Create"/>, pass to <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}"/>
/// (or use <see cref="Pipeline.Create{T,TPolicy,TSource,TEnumerator}"/>), then call
/// <see cref="Enqueue"/> as items become available. The pipeline's executor walks the source's
/// enumerator via <c>await foreach</c>. The enumerator blocks on the wake signal when the queue
/// is empty and wakes when <see cref="Enqueue"/> signals.
/// </para>
/// </remarks>
public readonly struct UnboundedQueueSource<T> : IPipelineSource<T, UnboundedQueueSource<T>.Enumerator>
{
    readonly State _state;

    UnboundedQueueSource(State state) => _state = state;

    /// <summary>Create an unbounded queue source.</summary>
    /// <param name="runContinuationsAsynchronously">When true, signal continuations dispatch to the scheduler.
    /// When false, signal continuations run inline on the caller's thread.</param>
    /// <param name="executionScheduler">Scheduler used for inline-async dispatch. Falls back to ThreadPool when null.</param>
    public static UnboundedQueueSource<T> Create(bool runContinuationsAsynchronously = true, PipelineScheduler? executionScheduler = null, CancellationToken cancellationToken = default)
        => new(new State(runContinuationsAsynchronously, executionScheduler ?? PipelineScheduler.ThreadPool, cancellationToken));

    /// <summary>The source-level cancellation token (set at <see cref="Create"/>). Stable for the
    /// source's lifetime. Distinct from the per-enumeration token captured by the enumerator.</summary>
    public CancellationToken CancellationToken => _state.CancellationToken;

    /// <summary>Enqueues an item for processing. Returns an <see cref="EnqueueResult"/>.</summary>
    /// <remarks>
    /// Invoke <see cref="EnqueueResult.Execute"/> outside any held lock to signal the executor.
    /// The signal may synchronously dispatch the executor on the caller's thread if the source was
    /// constructed with <c>runContinuationsAsynchronously: false</c>.
    /// </remarks>
    /// <exception cref="InvalidOperationException">Thrown if the source has been completed.</exception>
    public EnqueueResult Enqueue(T item)
    {
        if (_state.WakeSignal.IsCompleted)
            ThrowCompleted();

        // Notify the pipeline before publishing the item, so a producer thread that follows up by
        // reading Pipeline.Depth always observes its own contribution. Ordering matters: the
        // pipeline's executor can dequeue and complete the item before Enqueue returns, so the
        // increment must precede the queue write that makes the item visible to the executor.
        _state.OnEnqueue?.Invoke();
        _state.NotEmpty = true;
        _state.Queue.Enqueue(item);
        return new(_state.WakeSignal);
    }

    /// <summary>Returns a struct enumerator that the pipeline drives via <c>await foreach</c>.</summary>
    /// <remarks>
    /// Registers a callback on <paramref name="cancellationToken"/> that completes the wake signal,
    /// ensuring that consumer cancellation propagates to a parked MoveNextAsync. Without this,
    /// the enumerator would wait forever on the wake signal while the pipeline considers itself
    /// shut down.
    /// </remarks>
    public Enumerator GetAsyncEnumerator(Action? onEnqueue = null, CancellationToken cancellationToken = default)
    {
        _state.OnEnqueue = onEnqueue;
        return new(_state, cancellationToken);  // Enumerator combines _state.CancellationToken internally.
    }

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    /// <summary>Reference-typed inner state shared across struct copies.</summary>
    /// <remarks>
    /// <c>Current</c> lives here (rather than on the struct enumerator) because the enumerator is a
    /// struct that callers hold and copy: a write to a struct field would land on a copy and be
    /// lost. Routing the produced item through the class-backed state keeps it observable.
    /// <para>
    /// The pull is <c>=&gt; WakeSignal.Rendezvous(_resolve)</c>: the wake signal owns the park
    /// lifecycle and value-task plumbing, and this state supplies only a cached resolver run under
    /// the wake lock. A pull that finds an item (or observes completion) returns an already-completed
    /// task with no state-machine box; only an actual park hands out an IValueTaskSource-backed one.
    /// </para>
    /// </remarks>
    internal sealed class State
    {
        public readonly SingleProducerSingleConsumerQueue<T> Queue = new();
        public bool NotEmpty;
        public readonly WakeSignal WakeSignal;
        public T Current = default!;
        public Action? OnEnqueue;
        // Source-level cancellation. Per-enumeration CT (passed to GetAsyncEnumerator) gets
        // linked with this in the Enumerator's CTS construction.
        public readonly CancellationToken CancellationToken;

        // The active enumeration's combined (source + per-call) token, published by the enumerator
        // at construction. The resolver reads it to translate cancellation into a completed (false)
        // result, matching the WakeSignal.IsCompleted check.
        public CancellationToken EnumerationToken;

        readonly Func<WakeOutcome> _resolve;

        public State(bool runContinuationsAsynchronously, PipelineScheduler scheduler, CancellationToken cancellationToken)
        {
            WakeSignal = new(runContinuationsAsynchronously, scheduler);
            CancellationToken = cancellationToken;
            _resolve = Resolve;
        }

        public ValueTask<bool> MoveNextAsync() => WakeSignal.Rendezvous(_resolve);

        // Runs under the wake lock (WakeSignal.Pump holds it across this call). Pure: a single
        // dequeue attempt, no user code, so no reentrancy or park-hook concern.
        WakeOutcome Resolve()
        {
            NotEmpty = false;

            if (Queue.TryDequeue(out var current))
            {
                Current = current!;
                return WakeOutcome.GotItem;
            }

            // No item to hand back: release the previously-yielded Current so the last item is not
            // GC-rooted by this field across the park. The executor only reads Current after a
            // GotItem, so clearing it on a non-GotItem outcome is safe.
            if (System.Runtime.CompilerServices.RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                Current = default!;

            if (WakeSignal.IsCompleted || EnumerationToken.IsCancellationRequested)
                return WakeOutcome.Completed;

            return WakeOutcome.Park;
        }
    }

    /// <summary>
    /// Represents a deferred enqueue completion returned by <see cref="Enqueue"/>.
    /// The item is already in the queue. Calling <see cref="Execute"/> may signal the execution loop to process it.
    /// This two-step design exists because the signal may synchronously run the execution loop on the caller's
    /// thread (when the source was created with <c>runContinuationsAsynchronously: false</c>), so it must be
    /// invoked outside any held lock.
    /// </summary>
    public readonly struct EnqueueResult
    {
        readonly WakeSignal? _signal;
        internal EnqueueResult(WakeSignal? signal) => _signal = signal;

        /// <summary>Signals the execution loop, which may run the executor inline on the calling thread.</summary>
        public void Execute() => _signal?.Signal();
    }

    /// <summary>The struct enumerator driven by the pipeline's <c>await foreach</c>.</summary>
    public struct Enumerator : IPipelineEnumerator<T>
    {
        readonly State _state;
        readonly CancellationTokenSource _cts;
        // Captured at construction. CancellationToken is a struct holding a reference to the CTS;
        // IsCancellationRequested is a passive read of the source's state that works even after
        // Dispose (the state field retains its Notifying value). Capturing avoids the
        // ObjectDisposedException that would fire if we read _cts.Token after Dispose.
        readonly CancellationToken _completionToken;

        internal Enumerator(State state, CancellationToken perCallCt)
        {
            _state = state;
            var sourceCt = state.CancellationToken;
            // Combine source-level CT (set at UnboundedQueueSource.Create) and per-call CT
            // (passed to GetAsyncEnumerator, e.g., from await foreach with WithCancellation).
            // Linked source forwards cancellation from either.
            _cts = (sourceCt.CanBeCanceled, perCallCt.CanBeCanceled) switch
            {
                (true, true) => CancellationTokenSource.CreateLinkedTokenSource(sourceCt, perCallCt),
                (true, false) => CancellationTokenSource.CreateLinkedTokenSource(sourceCt),
                (false, true) => CancellationTokenSource.CreateLinkedTokenSource(perCallCt),
                (false, false) => new CancellationTokenSource(),
            };
            _completionToken = _cts.Token;
            // Publish the combined token to the state so the pump can observe cancellation.
            state.EnumerationToken = _completionToken;
            _completionToken.UnsafeRegister(static state => ((State)state!).WakeSignal.Complete(), _state);
        }

        public T Current => _state.Current;
        public CancellationToken CompletionToken => _completionToken;

        public void Complete() => _cts.Cancel();

        public ValueTask<bool> MoveNextAsync() => _state.MoveNextAsync();

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();   // idempotent, in case caller skipped Complete()
            _cts.Dispose();  // releases linked-CTS registration on the external token's source
            return default;
        }
    }
}
