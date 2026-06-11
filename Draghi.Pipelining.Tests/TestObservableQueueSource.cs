using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining.Tests;

// Test-only source modeled on UnboundedQueueSource<T>, with an observation hook that re-homes the
// IPipelinePolicy.OnExecutionIdleAsync callback removed during the source-driven refactor:
//
//   onIdle — fires each time the executor reaches the park after an active batch drained (>= 1 item
//            returned since the last genuine park), passing the enumerator's completion token. Never
//            on a cold/empty park. May enqueue reentrantly or throw; a throw propagates out of the
//            pull so the executor faults (CompleteAsync(ex) + drain + rethrow), matching the old
//            OnExecutionIdleAsync semantics.
//
// Built on WakeSignal.Rendezvous: the pure dequeue is the resolver (run under the wake lock), and
// onIdle rides the async onPark seam (fired out-of-lock so a reentrant enqueue from it can't
// deadlock). When no onIdle is supplied the pull is the zero-box production path.
[System.Diagnostics.CodeAnalysis.Experimental("DRAGHI001")]
readonly struct TestObservableQueueSource<T> : IPipelineSource<T, TestObservableQueueSource<T>.Enumerator>
{
    readonly State _state;

    TestObservableQueueSource(State state) => _state = state;

    public static TestObservableQueueSource<T> Create(
        bool runContinuationsAsynchronously = true,
        PipelineScheduler? executionScheduler = null,
        Func<CancellationToken, ValueTask>? onIdle = null,
        CancellationToken cancellationToken = default)
        => new(new State(runContinuationsAsynchronously, executionScheduler ?? PipelineScheduler.ThreadPool, onIdle, cancellationToken));

    public CancellationToken CancellationToken => _state.CancellationToken;

    public EnqueueResult Enqueue(T item)
    {
        if (_state.WakeSignal.IsCompleted)
            ThrowCompleted();

        _state.OnEnqueue?.Invoke();
        _state.NotEmpty = true;
        _state.Queue.Enqueue(item);
        return new(_state.WakeSignal);
    }

    public Enumerator GetAsyncEnumerator(Action? onEnqueue = null, CancellationToken cancellationToken = default)
    {
        _state.OnEnqueue = onEnqueue;
        return new(_state, cancellationToken);
    }

    static void ThrowCompleted() => throw new InvalidOperationException("The source has been completed.");

    // Reference-typed inner state shared across struct copies. Current lives here for the same
    // struct-copy reason documented on UnboundedQueueSource.State.
    internal sealed class State
    {
        public readonly SingleProducerSingleConsumerQueue<T> Queue = new();
        public bool NotEmpty;
        public readonly WakeSignal WakeSignal;
        public T Current = default!;
        public Action? OnEnqueue;
        public readonly CancellationToken CancellationToken;
        public CancellationToken EnumerationToken;
        public readonly Func<CancellationToken, ValueTask>? OnIdle;
        // Items returned since the last genuine park. Mutated under the wake lock (the resolver's
        // GotItem ++ and the park hook's read+reset) so the wake-thread resolve and the consumer's
        // park hook don't race on it.
        public int DequeuedSincePark;

        readonly Func<WakeOutcome> _resolve;
        readonly Func<ValueTask> _onPark;

        public State(
            bool runContinuationsAsynchronously,
            PipelineScheduler scheduler,
            Func<CancellationToken, ValueTask>? onIdle,
            CancellationToken cancellationToken)
        {
            WakeSignal = new(runContinuationsAsynchronously, scheduler);
            OnIdle = onIdle;
            CancellationToken = cancellationToken;
            _resolve = Resolve;
            _onPark = OnParkAsync;
        }

        // No onPark when there is no idle hook: that is the zero-box production pull.
        public ValueTask<bool> MoveNextAsync() => WakeSignal.Rendezvous(_resolve, OnIdle is null ? null : _onPark);

        // Runs under the wake lock (WakeSignal.Pump holds it across this call).
        WakeOutcome Resolve()
        {
            NotEmpty = false;

            if (Queue.TryDequeue(out var current))
            {
                Current = current!;
                DequeuedSincePark++;
                return WakeOutcome.GotItem;
            }

            // Release the previously-yielded Current so it is not GC-rooted across the park (matches
            // UnboundedQueueSource; the executor only reads Current after a GotItem).
            if (System.Runtime.CompilerServices.RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                Current = default!;

            if (WakeSignal.IsCompleted || EnumerationToken.IsCancellationRequested)
                return WakeOutcome.Completed;

            return WakeOutcome.Park;
        }

        // Runs out-of-lock when the pull parks. Take the lock only to read+reset DequeuedSincePark
        // (shared with the resolver), release it, THEN await onIdle: onIdle is user code that may
        // re-enqueue (which takes the wake lock), so it must not run under the lock. Fire only when
        // an active batch drained, mirroring the old "idle after a batch, never on a cold park."
        async ValueTask OnParkAsync()
        {
            WakeSignal.AcquireWakeLock();
            var fire = DequeuedSincePark > 0;
            DequeuedSincePark = 0;
            WakeSignal.ReleaseWakeLock();

            if (fire && OnIdle is { } onIdle)
                await onIdle(EnumerationToken).ConfigureAwait(false);
        }
    }

    public readonly struct EnqueueResult
    {
        readonly WakeSignal? _signal;
        internal EnqueueResult(WakeSignal? signal) => _signal = signal;

        public void Execute() => _signal?.Signal();
    }

    public struct Enumerator : IPipelineEnumerator<T>
    {
        readonly State _state;
        readonly CancellationTokenSource _cts;
        readonly CancellationToken _completionToken;

        internal Enumerator(State state, CancellationToken perCallCt)
        {
            _state = state;
            var sourceCt = state.CancellationToken;
            _cts = (sourceCt.CanBeCanceled, perCallCt.CanBeCanceled) switch
            {
                (true, true) => CancellationTokenSource.CreateLinkedTokenSource(sourceCt, perCallCt),
                (true, false) => CancellationTokenSource.CreateLinkedTokenSource(sourceCt),
                (false, true) => CancellationTokenSource.CreateLinkedTokenSource(perCallCt),
                (false, false) => new CancellationTokenSource(),
            };
            _completionToken = _cts.Token;
            _state.EnumerationToken = _completionToken;
            _completionToken.UnsafeRegister(static state => ((State)state!).WakeSignal.Complete(), _state);
        }

        public T Current => _state.Current;
        public CancellationToken CompletionToken => _completionToken;

        public void Complete() => _cts.Cancel();

        public ValueTask<bool> MoveNextAsync() => _state.MoveNextAsync();

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _cts.Dispose();
            return default;
        }
    }
}
