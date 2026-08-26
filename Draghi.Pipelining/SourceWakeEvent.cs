using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining.Internal;

/// <summary>
/// Serializes a failed source pull with its next producer notification. <see cref="BeginWait"/>
/// keeps the empty check atomic with continuation registration; <see cref="Set()"/> claims and
/// resumes an established wait.
/// </summary>
/// <remarks>
/// The signal invokes the async method's cached continuation directly, avoiding a separate promise.
/// Sources may use this internal building block when implementing <see cref="IPipelineSource{T,TEnumerator}"/>.
/// </remarks>
[Experimental("DRAGHI001")]
public sealed class SourceWakeEvent(bool runContinuationsAsynchronously, PipelineScheduler scheduler)
    : IThreadPoolWorkItem
{
    readonly CancellationTokenSource _cts = new();
    bool _waitPending;
    int _waitLock;
    Action? _afterWaitLock;
    // The async method normally supplies the same cached delegate on every wait.
    Action? _waitContinuation;

    public PipelineScheduler Scheduler { get; } = scheduler;
    public CancellationToken CompletionToken => _cts.Token;
    public bool IsCompleted => _cts.IsCancellationRequested;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void EnterWaitLock()
    {
        if (Interlocked.Exchange(ref _waitLock, 1) != 0)
            EnterWaitLockSlow();

        [MethodImpl(MethodImplOptions.NoInlining)]
        void EnterWaitLockSlow()
        {
            var spinner = new SpinWait();
            while (Volatile.Read(ref _waitLock) != 0 || Interlocked.Exchange(ref _waitLock, 1) != 0)
                spinner.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ExitWaitLock() => Volatile.Write(ref _waitLock, 0);

    /// <summary>
    /// Begins the consumer's empty-check. Dispose releases the check; <see cref="WaitScope.WaitAsync"/>
    /// instead transfers release to continuation registration, closing the miss-and-arm race.
    /// </summary>
    public WaitScope BeginWait()
    {
        EnterWaitLock();
        return new(this);
    }

    /// <summary>
    /// Holds the wait lock while attempting to claim a published wait. A successful claim must be
    /// dispatched after leaving the scope.
    /// </summary>
    public ClaimScope BeginClaim()
    {
        EnterWaitLock();
        return new(this);
    }

    /// <summary>
    /// Arms after a locked source check found no item. Continuation registration releases the
    /// wait lock, preventing a notification between the miss and the arm.
    /// </summary>
    WaitForNextAwaitable Arm()
    {
        _waitPending = true;
        return new WaitForNextAwaitable(this);
    }

    /// <summary>
    /// Registers an armed wait before releasing the wait lock. Completion is
    /// checked afterwards so one that raced registration still wakes the consumer.
    /// </summary>
    public void WaitOnCompleted(Action continuation)
    {
        Action? afterWaitLock = null;
        try
        {
            if (!ReferenceEquals(_waitContinuation, continuation))
                _waitContinuation = continuation;
            OnWaitReady?.Invoke(new(this));
            afterWaitLock = _afterWaitLock;
        }
        finally
        {
            _afterWaitLock = null;
            ExitWaitLock();
        }

        // Deferred actions complete an already-owned wait obligation and must not throw. The wait
        // lock is released, but swallowing a failure here would turn a contract violation into a strand.
        afterWaitLock?.Invoke();

        if (IsCompleted)
            SignalCore(runContinuationsAsynchronously: true);
    }

    /// <summary>Invoked under the wait lock when the consumer wait becomes claimable.</summary>
    /// <remarks>
    /// The handler must remain bounded and non-throwing. Use
    /// <see cref="WaitReadyContext.RunAfterWaitLock"/> for signalling or scheduling.
    /// </remarks>
    public Action<WaitReadyContext>? OnWaitReady { get; set; }

    public bool Set() => SignalCore(runContinuationsAsynchronously);

    /// <summary>
    /// Signals using an explicit dispatch mode. False resumes the consumer inline on this thread;
    /// true submits it through the configured scheduler.
    /// </summary>
    /// <returns><c>true</c> if a waiting pull was claimed and woken, <c>false</c> if no one was
    /// waiting (the signal was a no-op).</returns>
    public bool Set(bool runContinuationsAsynchronously) => SignalCore(runContinuationsAsynchronously);

    bool SignalCore(bool runContinuationsAsynchronously)
    {
        // Registration releases the lock only after storing the continuation.
        EnterWaitLock();
        bool claimed;
        try
        {
            claimed = TryClaimLocked();
        }
        finally
        {
            ExitWaitLock();
        }

        if (!claimed)
            return false;
        DispatchClaimed(runContinuationsAsynchronously);
        return true;
    }

    /// <summary>
    /// Claims an armed wait without dispatching it.
    /// </summary>
    bool TryClaimLocked()
    {
        if (!_waitPending)
            return false;
        _waitPending = false;
        return true;
    }

    /// <summary>Dispatches a claimed wait after releasing the wait lock. Inline dispatch may
    /// immediately re-enter the source wait protocol.</summary>
    void DispatchClaimed(bool runContinuationsAsynchronously)
    {
        if (runContinuationsAsynchronously)
            Scheduler.SubmitDetached(this, preferLocal: true);
        else
            _waitContinuation!();
    }

    void IThreadPoolWorkItem.Execute() => _waitContinuation!();

    /// <summary>Marks the source completed and wakes any waiting pull.</summary>
    public void Complete()
    {
        _cts.Cancel();
        SignalCore(runContinuationsAsynchronously: true);
    }

    public struct WaitScope : IDisposable
    {
        SourceWakeEvent? _signal;

        internal WaitScope(SourceWakeEvent signal) => _signal = signal;

        public WaitForNextAwaitable WaitAsync()
        {
            var signal = _signal ?? throw new InvalidOperationException("The wait scope has already ended.");
            _signal = null;
            return signal.Arm();
        }

        public void Dispose()
        {
            _signal?.ExitWaitLock();
            _signal = null;
        }
    }

    public ref struct ClaimScope : IDisposable
    {
        SourceWakeEvent? _signal;

        internal ClaimScope(SourceWakeEvent signal) => _signal = signal;

        public bool TryClaim(out WaitClaim claim)
        {
            var signal = _signal ?? throw new InvalidOperationException("The claim scope has already ended.");
            if (signal.TryClaimLocked())
            {
                claim = new(signal);
                return true;
            }
            claim = default;
            return false;
        }

        public void Dispose()
        {
            _signal?.ExitWaitLock();
            _signal = null;
        }
    }

    public readonly struct WaitClaim
    {
        readonly SourceWakeEvent? _signal;

        internal WaitClaim(SourceWakeEvent signal) => _signal = signal;

        public void Dispatch(bool runContinuationsAsynchronously)
            => (_signal ?? throw new InvalidOperationException("No wait was claimed."))
                .DispatchClaimed(runContinuationsAsynchronously);
    }

    /// <summary>
    /// Context supplied while a consumer wait is being published. It may claim that
    /// wait immediately without exposing the event's exclusion mechanism.
    /// </summary>
    public readonly struct WaitReadyContext
    {
        readonly SourceWakeEvent _signal;

        internal WaitReadyContext(SourceWakeEvent signal) => _signal = signal;

        /// <summary>Registers the single non-throwing action to run after wait publication releases its lock.</summary>
        public void RunAfterWaitLock(Action action)
        {
            ArgumentNullException.ThrowIfNull(action);
            if (_signal._afterWaitLock is not null)
                throw new InvalidOperationException("An action is already registered for this wait publication.");
            _signal._afterWaitLock = action;
        }

        public bool TryClaim(out WaitClaim claim)
        {
            if (_signal.TryClaimLocked())
            {
                claim = new(_signal);
                return true;
            }
            claim = default;
            return false;
        }
    }
}
