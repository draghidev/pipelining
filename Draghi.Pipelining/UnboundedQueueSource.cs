using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
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
/// enumerator through its pull/wait seam. The enumerator waits on the wake signal when the queue
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
    /// <param name="cancellationToken">The cancellation token used to complete the executor.</param>
    public static UnboundedQueueSource<T> Create(bool runContinuationsAsynchronously = true, PipelineScheduler? executionScheduler = null, CancellationToken cancellationToken = default)
        => new(new State(runContinuationsAsynchronously, executionScheduler ?? PipelineScheduler.ThreadPool, cancellationToken));

    /// <summary>The source-level cancellation token (set at <see cref="Create"/>). Stable for the
    /// source's lifetime. Distinct from the per-enumeration token captured by the enumerator.</summary>
    public CancellationToken CancellationToken => _state.CancellationToken;

    /// <summary>Enqueues an item for processing. Returns an <see cref="EnqueueSignal"/>.</summary>
    /// <remarks>
    /// Invoke <see cref="EnqueueSignal.Signal"/> outside any held lock to signal the executor.
    /// The signal may synchronously dispatch the executor on the caller's thread if the source was
    /// constructed with <c>runContinuationsAsynchronously: false</c>.
    /// </remarks>
    /// <exception cref="InvalidOperationException">Thrown if the source has been completed.</exception>
    public EnqueueSignal Enqueue(T item)
    {
        if (_state.WakeEvent.IsCompleted)
            ThrowCompleted();

        _state.QueueNotEmpty = true;
        _state.Queue.Enqueue(item);
        return new(_state.WakeEvent);
    }

    /// <summary>Backlog: items enqueued but not yet dispatched (the queue length). With
    /// <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}.Depth"/> (in-flight = dispatched - completed),
    /// <c>Depth + Backlog</c> is the total outstanding. Lock-free read, may be stale.</summary>
    public int Backlog => _state.Queue.Count;

    /// <summary>Returns the struct enumerator driven by the pipeline.</summary>
    /// <remarks>
    /// Registers a callback on <paramref name="cancellationToken"/> that completes the wake signal,
    /// ensuring that consumer cancellation propagates to a pending wait. Without this,
    /// the enumerator would wait forever on the wake signal while the pipeline considers itself
    /// shut down.
    /// </remarks>
    public Enumerator CreateEnumerator(CancellationToken cancellationToken = default)
    {
        return new(_state, cancellationToken);  // Enumerator combines _state.CancellationToken internally.
    }

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    /// <summary>Reference-typed inner state shared across struct copies.</summary>
    internal sealed class State
    {
        public readonly SingleProducerSingleConsumerQueue<T> Queue = new();
        public bool QueueNotEmpty;
        public readonly SourceWakeEvent WakeEvent;
        // Source-level cancellation. Per-enumeration CT (passed to CreateEnumerator) gets
        // linked with this in the Enumerator's CTS construction.
        public readonly CancellationToken CancellationToken;

        public State(bool runContinuationsAsynchronously, PipelineScheduler scheduler, CancellationToken cancellationToken)
        {
            WakeEvent = new(runContinuationsAsynchronously, scheduler);
            CancellationToken = cancellationToken;
        }
    }

    /// <summary>
    /// Represents a deferred executor signal returned by <see cref="Enqueue"/>.
    /// The item is already in the queue. Calling <see cref="Signal"/> may signal the execution loop to process it.
    /// This two-step design exists because the signal may synchronously run the execution loop on the caller's
    /// thread (when the source was created with <c>runContinuationsAsynchronously: false</c>), so it must be
    /// invoked outside any held lock.
    /// </summary>
    public readonly struct EnqueueSignal
    {
        readonly SourceWakeEvent? _signal;
        internal EnqueueSignal(SourceWakeEvent? signal) => _signal = signal;

        /// <summary>Signals the execution loop, which may run the executor inline on the calling thread.</summary>
        public void Signal() => _signal?.Set();
    }

    /// <summary>The struct enumerator driven by the pipeline.</summary>
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
            // (passed to CreateEnumerator, e.g., from await foreach with WithCancellation).
            // Linked source forwards cancellation from either.
            _cts = (sourceCt.CanBeCanceled, perCallCt.CanBeCanceled) switch
            {
                (true, true) => CancellationTokenSource.CreateLinkedTokenSource(sourceCt, perCallCt),
                (true, false) => CancellationTokenSource.CreateLinkedTokenSource(sourceCt),
                (false, true) => CancellationTokenSource.CreateLinkedTokenSource(perCallCt),
                (false, false) => new CancellationTokenSource(),
            };
            _completionToken = _cts.Token;
            _completionToken.UnsafeRegister(static state => ((State)state!).WakeEvent.Complete(), _state);
        }

        public CancellationToken CompletionToken => _completionToken;

        public void Complete() => _cts.Cancel();

        /// <summary>Lock-free synchronous pull: the SPSC consumer side needs no synchronization,
        /// the wake lock exists only for the miss-then-wait rendezvous (see <see cref="WaitForNextAsync"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNext([MaybeNullWhen(false)] out T item) => _state.Queue.TryDequeue(out item);

        /// <summary>
        /// Miss path. Re-checks availability under the wake lock; a genuine wait arms the signal
        /// with the lock held through the awaiter's continuation registration
        /// (lock-through-OnCompleted), so a producer's Enqueue+Signal racing the miss either
        /// resolves this call to an immediate retry or wakes the armed wait - never a lost wake.
        /// NOTE: no AggressiveInlining here - measured +3ns WORSE with it on the wait-per-item
        /// shape (forced inline bloats the executor's state machine for a once-per-wait call).
        /// </summary>
        public WaitForNextAwaitable WaitForNextAsync()
        {
            var wakeSignal = _state.WakeEvent;
            using var wait = wakeSignal.BeginWait();
            // Best-effort window-narrower: the producer sets QueueNotEmpty before its queue write, so
            // an in-flight enqueue whose item is not yet visible resolves to a retry instead of
            // a suspended wait. Its loss is harmless - the producer's Signal covers the armed wait.
            _state.QueueNotEmpty = false;
            if (_state.Queue.IsEmpty && !_state.QueueNotEmpty)
            {
                if (wakeSignal.IsCompleted || _completionToken.IsCancellationRequested)
                {
                    return WaitForNextAwaitable.Completed;
                }
                return wait.WaitAsync();
            }

            return WaitForNextAwaitable.Retry;
        }

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();   // idempotent, in case caller skipped Complete()
            _cts.Dispose();  // releases linked-CTS registration on the external token's source
            return default;
        }
    }
}
