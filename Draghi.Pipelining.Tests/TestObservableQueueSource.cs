using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining.Tests;

// Test-only source modeled on UnboundedQueueSource<T>, with an observation hook that re-homes the
// IPipelinePolicy.OnExecutionIdleAsync callback removed during the source-driven refactor:
//
//   onIdle — fires each time the executor reaches the wait after an active batch drained (>= 1 item
//            returned since the last genuine wait), passing the enumerator's completion token. Never
//            on a cold/empty wait. May enqueue reentrantly or throw; a throw propagates out of the
//            pull so the executor faults (CompleteAsync(ex) + drain + rethrow), matching the old
//            OnExecutionIdleAsync semantics.
//
// Built on WakeSignal.Rendezvous: the pure dequeue is the resolver (run under the wake lock), and
// onIdle rides the async onWaiting seam (fired out-of-lock so a reentrant enqueue from it can't
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

    // Reference-typed inner state shared across struct copies.
    internal sealed class State
    {
        public readonly SingleProducerSingleConsumerQueue<T> Queue = new();
        public bool NotEmpty;
        public readonly WakeSignal WakeSignal;
        public Action? OnEnqueue;
        public readonly CancellationToken CancellationToken;
        public CancellationToken EnumerationToken;
        public readonly Func<CancellationToken, ValueTask>? OnIdle;
        // Items returned since the last genuine wait. Mutated under the wake lock (the resolver's
        // GotItem ++ and the wait hook's read+reset) so the wake-thread resolve and the consumer's
        // wait hook don't race on it.
        public int DequeuedSinceWait;

        readonly Func<WakeOutcome> _resolvePeek;
        readonly Func<ValueTask> _onWaiting;

        public State(
            bool runContinuationsAsynchronously,
            PipelineScheduler scheduler,
            Func<CancellationToken, ValueTask>? onIdle,
            CancellationToken cancellationToken)
        {
            WakeSignal = new(runContinuationsAsynchronously, scheduler);
            OnIdle = onIdle;
            CancellationToken = cancellationToken;
            _resolvePeek = ResolvePeek;
            _onWaiting = OnWaitingAsync;
        }

        // Wait via the rendezvous protocol with a PEEK resolver: a wake observes availability
        // without consuming, the value task resolves true, and the executor's TryGetNext retry
        // takes the item. Keeps the onIdle observation hook on WakeSignal's onWaiting seam.
        public ValueTask<bool> WaitForNextAsync() => WakeSignal.Rendezvous(_resolvePeek, OnIdle is null ? null : _onWaiting);

        // Runs under the wake lock (WakeSignal.Pump holds it across this call). Never consumes.
        WakeOutcome ResolvePeek()
        {
            NotEmpty = false;

            if (Queue.TryPeek(out _))
                return WakeOutcome.GotItem;

            if (WakeSignal.IsCompleted || EnumerationToken.IsCancellationRequested)
                return WakeOutcome.Completed;

            return WakeOutcome.Wait;
        }

        // Runs out-of-lock when the pull suspends, before the wait. DequeuedSinceWait is
        // consumer-thread-only now (TryGetNext increments, this gate reads/resets; the peek
        // resolver never touches it), so no locking is needed. onIdle is user code that may
        // re-enqueue; it must not run under the wake lock and does not here. Fires only when an
        // active batch drained, mirroring the old "idle after a batch, never on a cold wait."
        async ValueTask OnWaitingAsync()
        {
            var fire = DequeuedSinceWait > 0;
            DequeuedSinceWait = 0;

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

        public CancellationToken CompletionToken => _completionToken;

        public void Complete() => _cts.Cancel();

        public bool TryGetNext([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out T item)
        {
            if (_state.Queue.TryDequeue(out item))
            {
                _state.DequeuedSinceWait++;
                return true;
            }
            return false;
        }

        public WaitForNextAwaitable WaitForNextAsync() => WaitForNextAwaitable.FromTask(_state.WaitForNextAsync());

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _cts.Dispose();
            return default;
        }
    }
}
