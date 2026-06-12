using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining.Internal;

/// <summary>
/// Single-producer single-consumer wake signal for a pipeline source's pull. Owns the wait
/// lifecycle of the TryGetNext/WaitForNextAsync seam: the wake lock, the armed-wait flag, the
/// bare-delegate continuation a wake invokes directly, and the scheduler-vs-inline dispatch of
/// the wake. The consumer arms under the lock (<see cref="Arm"/>) and the lock is held through
/// continuation registration (lock-through-OnCompleted); a producer makes its item visible and
/// calls <see cref="Signal()"/>.
/// </summary>
/// <remarks>
/// A suspending wait stores the builder's cached delegate and invokes it directly, without a
/// value-task source or version bookkeeping.
/// <para>
/// A spinlock guards the rendezvous because the critical section is a few field reads/writes with
/// at most two threads contending (the consumer's wait path and a Signal caller). The protocol
/// consists of the under-lock re-check, lock-through registration, claim-clears-suspension, and
/// the sync-handoff rendezvous.
/// </para>
/// <para>
/// Exposed under <see cref="Draghi.Pipelining.Internal"/> as a building block for callers composing
/// their own <see cref="IPipelineSource{T,TEnumerator}"/> implementations.
/// </para>
/// </remarks>
[Experimental("DRAGHI001")]
public sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler scheduler, bool enableWaitForSuspended = false)
    : IThreadPoolWorkItem
{
    // Suspension observation for sync-handoff producers (see WaitForSuspended). Opt-in so
    // sources without a handoff seam don't pay the per-wait MRES traffic.
    readonly ManualResetEventSlim? _suspendedMres = enableWaitForSuspended ? new(false) : null;

    readonly CancellationTokenSource _cts = new();
    bool _pending;
    int _wakeLock;
    // The wake invokes this bare delegate directly. Cached across cycles (the async-method
    // builder reuses the box's action), so the ReferenceEquals in WaitOnCompleted skips the
    // write after the first wait.
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
    /// Arms the wait for the TryGetNext/WaitForNextAsync protocol. MUST be called under the wake
    /// lock, after the source observed nothing to yield; the returned awaitable's continuation
    /// registration is what releases the lock (lock-through-OnCompleted), which closes the
    /// miss-then-arm race against a producer's Signal without any re-resolve machinery.
    /// </summary>
    public WaitForNextAwaitable Arm()
    {
        _pending = true;
        return new WaitForNextAwaitable(this);
    }

    /// <summary>
    /// Continuation registration for an armed wait (called by <see cref="WaitForNextAwaitable.Awaiter"/>
    /// with the wake lock still held). Stores the bare delegate and releases the lock; a
    /// completion that raced the registration self-signals so the wake is not lost.
    /// </summary>
    public void WaitOnCompleted(Action continuation)
    {
        if (!ReferenceEquals(_waitContinuation, continuation))
            _waitContinuation = continuation;
        ReleaseWakeLock();
        // Suspension observation fires AFTER the lock release: the observing producer's
        // Signal must find the lock free and the continuation stored (lock-through closed
        // the registration gap; the order here keeps the wake path uncontended).
        _suspendedMres?.Set();

        if (IsCompleted)
            SignalCore(runContinuationsAsynchronously: true);
    }

    /// <summary>
    /// Blocks until the consumer's wait is armed and its continuation registered (the suspension
    /// observation a sync-handoff producer rendezvouses on before claiming the wait inline with
    /// <see cref="Signal(bool)"/>). Requires construction with <c>enableWaitForSuspended</c>.
    /// Every claim clears the observation before dispatch so it stays accurate across resume.
    /// A stale observation would let a later handoff producer skip its wait and return with its
    /// flow unexecuted.
    /// </summary>
    public void WaitForSuspended() => _suspendedMres!.Wait();

    public bool Signal() => SignalCore(runContinuationsAsynchronously);

    /// <summary>
    /// Signal with an explicit dispatch mode, overriding the construction-time default. When false
    /// the consumer's continuation runs inline on the calling thread (the call site takes over the
    /// executor for one turn). Use when a producer chooses dispatch per item.
    /// </summary>
    /// <returns><c>true</c> if a waiting pull was claimed and woken, <c>false</c> if no one was
    /// waiting (the signal was a no-op).</returns>
    public bool Signal(bool runContinuationsAsynchronously) => SignalCore(runContinuationsAsynchronously);

    bool SignalCore(bool runContinuationsAsynchronously)
    {
        // Claim and dispatch the bare continuation directly. The lock-through-OnCompleted
        // discipline guarantees the continuation is stored before _pending can be claimed
        // (a Signal blocks on the lock until registration released it).
        AcquireWakeLock();
        try
        {
            if (!_pending)
                return false;
            _pending = false;
        }
        finally
        {
            ReleaseWakeLock();
        }

        // Clear the suspension observation BEFORE dispatch so it stays accurate across
        // resume: a stale observation would let a sync-handoff producer skip its
        // rendezvous (WaitProtocol.tla, ClearOnClaimFix witness).
        _suspendedMres?.Reset();

        if (runContinuationsAsynchronously)
            Scheduler.SubmitDetached(this, preferLocal: true);
        else
            _waitContinuation!();
        return true;
    }

    void IThreadPoolWorkItem.Execute() => _waitContinuation!();

    /// <summary>Marks the source completed and wakes any waiting pull.</summary>
    public void Complete()
    {
        _cts.Cancel();
        SignalCore(runContinuationsAsynchronously: true);
    }
}
