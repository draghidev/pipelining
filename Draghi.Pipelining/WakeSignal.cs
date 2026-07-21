using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining.Internal;

/// <summary>
/// Coordinates a pipeline source's failed pull with its next producer wake. The consumer checks
/// the source and arms while holding the wake lock; continuation registration stores the callback
/// and releases that lock. A producer publishes its item, claims the wait under the same lock, and
/// dispatches the callback after releasing it.
/// </summary>
/// <remarks>
/// The signal invokes the async method's cached continuation directly, avoiding a separate promise.
/// Sources may use this internal building block when implementing <see cref="IPipelineSource{T,TEnumerator}"/>.
/// </remarks>
[Experimental("DRAGHI001")]
public sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler scheduler, bool enableWaitForSuspended = false)
    : IThreadPoolWorkItem
{
    // Optional rendezvous for producers that must not return until the consumer is suspended.
    readonly ManualResetEventSlim? _suspendedMres = enableWaitForSuspended ? new(false) : null;

    readonly CancellationTokenSource _cts = new();
    bool _pending;
    int _wakeLock;
    // The async method normally supplies the same cached delegate on every wait.
    Action? _waitContinuation;

    public PipelineScheduler Scheduler { get; } = scheduler;
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
            // Avoid repeatedly invalidating the owner's cache line while it holds the lock.
            while (Volatile.Read(ref _wakeLock) != 0 || Interlocked.Exchange(ref _wakeLock, 1) != 0)
                spinner.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseWakeLock() => Volatile.Write(ref _wakeLock, 0);

    /// <summary>
    /// Arms after an under-lock source check found no item. The caller leaves the wake lock held;
    /// continuation registration releases it, preventing a signal between the miss and the arm.
    /// </summary>
    public WaitForNextAwaitable Arm()
    {
        _pending = true;
        return new WaitForNextAwaitable(this);
    }

    /// <summary>
    /// Registers an armed wait while the wake lock is still held, then releases it. Completion is
    /// checked afterwards so one that raced registration still wakes the consumer.
    /// </summary>
    public void WaitOnCompleted(Action continuation)
    {
        if (!ReferenceEquals(_waitContinuation, continuation))
            _waitContinuation = continuation;
        // Set and claim-side Reset both occur under the lock, preventing a stale observation from
        // surviving into the next wait cycle.
        _suspendedMres?.Set();
        // Sources may need to notify the particular producer whose item the pull missed. Keeping
        // this callback under the lock orders that notification against a concurrent claim.
        OnSuspended?.Invoke();
        ReleaseWakeLock();

        if (IsCompleted)
            SignalCore(runContinuationsAsynchronously: true);
    }

    /// Invoked under the wake lock when the consumer suspends, after publishing that observation.
    /// Sources use this to complete a producer-side handoff tied to the missed pull.
    public Action? OnSuspended { get; set; }

    /// <summary>
    /// Blocks until an armed consumer has registered its continuation. Requires
    /// <c>enableWaitForSuspended</c>; claiming the wait clears the observation before dispatch.
    /// </summary>
    public void WaitForSuspended() => _suspendedMres!.Wait();

    public bool Signal() => SignalCore(runContinuationsAsynchronously);

    /// <summary>
    /// Signals using an explicit dispatch mode. False resumes the consumer inline on this thread;
    /// true submits it through the configured scheduler.
    /// </summary>
    /// <returns><c>true</c> if a waiting pull was claimed and woken, <c>false</c> if no one was
    /// waiting (the signal was a no-op).</returns>
    public bool Signal(bool runContinuationsAsynchronously) => SignalCore(runContinuationsAsynchronously);

    bool SignalCore(bool runContinuationsAsynchronously)
    {
        // Registration releases the lock only after storing the continuation.
        AcquireWakeLock();
        bool claimed;
        try
        {
            claimed = TryClaimLocked();
        }
        finally
        {
            ReleaseWakeLock();
        }

        if (!claimed)
            return false;
        DispatchClaimed(runContinuationsAsynchronously);
        return true;
    }

    /// <summary>
    /// Claims an armed wait without dispatching it. Callers use this under the wake lock when a
    /// separate gate check must be atomic with the claim. A successful claim must be followed by
    /// <see cref="DispatchClaimed"/> after releasing the lock.
    /// </summary>
    public bool TryClaimLocked()
    {
        if (!_pending)
            return false;
        _pending = false;
        // Clear before dispatch so the observation describes only the current suspension.
        _suspendedMres?.Reset();
        return true;
    }

    /// <summary>Dispatches a claimed wait after releasing the wake lock. Inline dispatch may
    /// immediately re-enter the source wait protocol.</summary>
    public void DispatchClaimed(bool runContinuationsAsynchronously)
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
}
