using System.Diagnostics;
using System.Runtime.CompilerServices;

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
    public static UnboundedQueueSource<T> Create(bool runContinuationsAsynchronously = true, PipelineScheduler? executionScheduler = null)
        => new(new State(runContinuationsAsynchronously, executionScheduler ?? PipelineScheduler.ThreadPool));

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

    /// <summary>See <see cref="IPipelineSource{T,TEnumerator}.AttachDepthHook"/>.</summary>
    public void AttachDepthHook(Action onEnqueue) => _state.OnEnqueue = onEnqueue;

    /// <summary>Returns a struct enumerator that the pipeline drives via <c>await foreach</c>.</summary>
    /// <remarks>
    /// Registers a callback on <paramref name="cancellationToken"/> that completes the wake signal,
    /// ensuring that consumer cancellation propagates to a parked MoveNextAsync. Without this,
    /// the enumerator would wait forever on the wake signal while the pipeline considers itself
    /// shut down.
    /// </remarks>
    public Enumerator GetAsyncEnumerator(CancellationToken cancellationToken)
    {
        if (cancellationToken.CanBeCanceled)
            cancellationToken.UnsafeRegister(static state => ((State)state!).WakeSignal.Complete(), _state);
        return new(_state, cancellationToken);
    }

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    /// <summary>Reference-typed inner state shared across struct copies.</summary>
    /// <remarks>
    /// <c>Current</c> lives here (rather than on the struct enumerator) because the enumerator's
    /// <c>MoveNextAsync</c> is <c>async</c>: the C# compiler captures <c>this</c> by value when it
    /// builds the state machine, so any field mutation inside the async method updates the state
    /// machine's local copy of the struct rather than the caller's field. Storing the produced
    /// item on the class-backed <see cref="State"/> makes the mutation visible to the caller.
    /// </remarks>
    internal sealed class State
    {
        public readonly SingleProducerSingleConsumerQueue<T> Queue = new();
        public bool NotEmpty;
        public readonly WakeSignal WakeSignal;
        public T Current = default!;
        public Action? OnEnqueue;

        public State(bool runContinuationsAsynchronously, PipelineScheduler scheduler)
            => WakeSignal = new(runContinuationsAsynchronously, scheduler);
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
    public struct Enumerator : IAsyncEnumerator<T>
    {
        readonly State _state;
        readonly CancellationToken _cancellationToken;

        internal Enumerator(State state, CancellationToken cancellationToken)
        {
            _state = state;
            _cancellationToken = cancellationToken;
        }

        public T Current => _state.Current;

        public async ValueTask<bool> MoveNextAsync()
        {
            // Dequeue first, exit only when queue is empty AND the source is shutting down. The
            // outer-predicate variant would strand items admitted before Complete().
            // After WaitUnsynchronized resumes (whether woken by Signal or Complete), loop back
            // to retry the dequeue. The await's boolean result is not load-bearing because the
            // top-of-loop completion check handles the "completed and empty" case.
            while (true)
            {
                _state.WakeSignal.AcquireWakeLock();
                _state.NotEmpty = false;

                if (_state.Queue.TryDequeue(out _state.Current))
                {
                    _state.WakeSignal.ReleaseWakeLock();
                    return true;
                }

                if (_state.WakeSignal.IsCompleted || _cancellationToken.IsCancellationRequested)
                {
                    _state.WakeSignal.ReleaseWakeLock();
                    return false;
                }

                // OnCompleted releases the lock after storing the continuation. Signal re-acquires
                // to claim and dispatch it.
                await _state.WakeSignal.WaitUnsynchronized();
            }
        }

        public ValueTask DisposeAsync()
        {
            _state.WakeSignal.Complete();
            return default;
        }
    }
}

/// <summary>
/// Single-consumer single-producer wake signal for suspending and waking the source's enumerator.
/// Doubles as its own awaitable and awaiter (GetAwaiter returns this).
/// </summary>
/// <remarks>
/// The wake lock is held from WaitUnsynchronized through OnCompleted, which stores the continuation
/// and releases the lock. Signal re-acquires the lock to claim the continuation.
/// A spinlock is used instead of Lock because the critical section is a few field reads/writes
/// with at most two threads contending (enumerator and Signal caller).
/// No code under the lock re-enters or is user code that may, so reentrancy support is not needed.
/// </remarks>
public sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler executionScheduler)
    : IThreadPoolWorkItem
{
    readonly CancellationTokenSource _cts = new();
    Action? _continuation;
    bool _pending;
    int _wakeLock;

    public PipelineScheduler Scheduler { get; } = executionScheduler;
    public CancellationToken CompletionToken => _cts.Token;
    public bool IsCompleted => _cts.IsCancellationRequested;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AcquireWakeLock()
    {
        if (Interlocked.Exchange(ref _wakeLock, 1) != 0)
            AcquireWakeLockSlow();

        [MethodImpl(MethodImplOptions.NoInlining)]
        void AcquireWakeLockSlow()
        {
            var spinner = new SpinWait();
            while (Interlocked.Exchange(ref _wakeLock, 1) != 0)
                spinner.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseWakeLock() => Volatile.Write(ref _wakeLock, 0);

    /// <summary>
    /// Prepares the signal for a new wait. Must be called under the wake lock.
    /// Returns an awaiter that holds the lock through OnCompleted.
    /// </summary>
    public Awaiter WaitUnsynchronized()
    {
        Debug.Assert(!_pending, "Concurrent wait calls.");
        _pending = true;
        return new(this);
    }

    public void Signal() => SignalCore(runContinuationsAsynchronously);

    void SignalCore(bool runContinuationsAsynchronously)
    {
        AcquireWakeLock();
        try
        {
            if (!_pending)
                return;
            _pending = false;
        }
        finally
        {
            ReleaseWakeLock();
        }

        if (runContinuationsAsynchronously)
            Scheduler.SubmitDetached(this, preferLocal: true);
        else
            _continuation!();
    }

    void IThreadPoolWorkItem.Execute() => _continuation!();

    /// <summary>Marks the source as completed, wakes any pending wait.</summary>
    public void Complete()
    {
        _cts.Cancel();
        SignalCore(runContinuationsAsynchronously: true);
    }

    public readonly struct Awaiter(WakeSignal signal) : ICriticalNotifyCompletion
    {
        public Awaiter GetAwaiter() => this;

        public bool IsCompleted => false;

        public bool GetResult() => !signal.IsCompleted;

        public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

        public void UnsafeOnCompleted(Action continuation)
        {
            if (!ReferenceEquals(signal._continuation, continuation))
                signal._continuation = continuation;
            signal.ReleaseWakeLock();

            if (signal.IsCompleted)
                signal.SignalCore(runContinuationsAsynchronously: true);
        }
    }
}
