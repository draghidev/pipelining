using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Draghi.Pipelining.Internal;

/// Outcome a source's resolver reports to <see cref="WakeSignal"/> under the wake lock.
public enum WakeOutcome
{
    /// An item was produced (the resolver set the source's Current). Complete the pull with true.
    GotItem,
    /// The source is completed/cancelled and drained. Complete the pull with false.
    Completed,
    /// Nothing available; wait until the next Signal, then re-resolve.
    Wait,
}

/// <summary>
/// Single-producer single-consumer wake signal and rendezvous for a pipeline source's pull. Owns
/// the entire wait lifecycle: the wake lock, the value-task source backing a waiting pull, the
/// re-resolve-on-wake loop, and the scheduler-vs-inline dispatch of the wake. A source supplies a
/// resolver (its dequeue, run under the wake lock) and calls <see cref="Signal()"/> when it makes
/// an item visible.
/// </summary>
/// <remarks>
/// The source's pull collapses to <c>=&gt; _wake.Rendezvous(_resolve)</c>: a pull that finds an item
/// (or observes completion) returns an already-completed <see cref="ValueTask{TResult}"/> with no
/// state-machine box, no ExecutionContext capture, and none of the per-invocation save/restore an
/// async method pays. Only an actual wait hands out an <see cref="IValueTaskSource{TResult}"/>-backed
/// task; the wake re-resolves and completes it.
/// <para>
/// A spinlock guards the rendezvous because the critical section is a few field reads/writes with at
/// most two threads contending (the consumer's pull and a Signal caller). The wake dispatch (inline
/// vs scheduler) is decided once here, so the value-task continuation runs inline on whichever thread
/// the wake landed on. <see cref="ManualResetValueTaskSourceCore{TResult}.RunContinuationsAsynchronously"/>
/// defaults to false, which is exactly that.
/// </para>
/// <para>
/// Exposed under <see cref="Draghi.Pipelining.Internal"/> as a building block for callers composing
/// their own <see cref="IPipelineSource{T,TEnumerator}"/> implementations.
/// </para>
/// </remarks>
[Experimental("DRAGHI001")]
public sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler scheduler)
    : IValueTaskSource<bool>, IThreadPoolWorkItem
{
    readonly CancellationTokenSource _cts = new();
    Func<WakeOutcome>? _resolve;
    bool _pending;
    int _wakeLock;
    // ManualResetValueTaskSourceCore on purpose: a bespoke single-consumer core (continuation
    // field as the whole state machine, sentinel for completed-before-registered, no version/
    // status bookkeeping) was built and MEASURED SLOWER on the wait-per-item shape - 88.0ns vs 83.6ns
    // with MRVTSC, even with the no-RMW plain-read fast path on the completion side. The
    // runtime's tuned core wins; don't re-litigate without numbers.
    ManualResetValueTaskSourceCore<bool> _core;
    // Wait-protocol continuation (TryGetNext/WaitForNextAsync sources): a bare delegate the wake
    // invokes directly, no value-task source in between. Cached across cycles (the async-method
    // builder reuses the box's action), so the ReferenceEquals in WaitOnCompleted skips the
    // write after the first wait. A signal instance serves exactly one source, which uses
    // exactly one protocol: _resolve set = rendezvous protocol, _resolve null = wait protocol.
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
            while (Interlocked.Exchange(ref _wakeLock, 1) != 0)
                spinner.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseWakeLock() => Volatile.Write(ref _wakeLock, 0);

    /// <summary>
    /// Drives one pull. The resolver runs under the wake lock; on a synchronous outcome the returned
    /// task is already completed (no box). On Wait the returned task completes when the next Signal
    /// re-resolves to GotItem/Completed. The resolver is expected to be a cached delegate on the
    /// source's state, so this allocates nothing on the hot path.
    /// </summary>
    /// <param name="resolveUnderLock">The source's dequeue, invoked under the wake lock. Sets the
    /// source's Current and returns GotItem, observes completion and returns Completed, or returns
    /// Wait when nothing is available.</param>
    /// <param name="onWaiting">
    /// Optional async work to run each time the pull waits (e.g. an idle-flush, or a test
    /// observation hook). Runs OUTSIDE the wake lock and AFTER <c>_pending</c> is armed: it is user
    /// code that may re-enqueue (which takes the wake lock and Signals), so it must not run under the
    /// lock, and a re-enqueue from it must find the wait already armed so its Signal wakes this very
    /// pull. Supplying a hook makes only the WAIT path async; the dequeue-hit fast path stays
    /// zero-box. Pure sources pass none and never box.
    /// </param>
    public ValueTask<bool> Rendezvous(Func<WakeOutcome> resolveUnderLock, Func<ValueTask>? onWaiting = null)
    {
        _resolve = resolveUnderLock;
        switch (Pump())
        {
            case WakeOutcome.GotItem:
                return new ValueTask<bool>(true);
            case WakeOutcome.Completed:
                return new ValueTask<bool>(false);
            default:
                return onWaiting is null
                    ? new ValueTask<bool>(this, _core.Version)
                    : AwaitWaiting(onWaiting);
        }
    }

    // One resolve attempt under the wake lock. The lock spans the resolve through the wait-arm so an
    // Enqueue+Signal racing in between cannot be lost: a producer must take the lock to Signal, and
    // by then either our resolve already took the item or we have armed _pending.
    //
    // The value-task core is never touched here: GetResult resets it after each consumed wait
    // (the canonical resettable-VTS pattern), so by the time any pull re-arms, the core already
    // carries the next version. Synchronous outcomes never touch the core at all.
    WakeOutcome Pump()
    {
        AcquireWakeLock();
        var outcome = _resolve!();
        if (outcome != WakeOutcome.Wait)
        {
            ReleaseWakeLock();
            return outcome;
        }

        _pending = true;
        ReleaseWakeLock();
        return WakeOutcome.Wait;
    }

    // Wait path with an async hook. _pending is already armed (see Pump), so a re-enqueue from the
    // hook will Signal this wait. Run the hook, then await the value task the wake completes. The
    // hook running before the await is deliberate: the core stores a result set before its
    // continuation is registered, so a reentrant wake during the hook is not lost. A throwing hook
    // propagates out of MoveNextAsync, faulting the executor (matching the old
    // OnExecutionIdleAsync semantics).
    async ValueTask<bool> AwaitWaiting(Func<ValueTask> onWaiting)
    {
        await onWaiting().ConfigureAwait(false);
        return await new ValueTask<bool>(this, _core.Version).ConfigureAwait(false);
    }

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

        if (IsCompleted)
            SignalCore(runContinuationsAsynchronously: true);
    }

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
        // Wait protocol: claim and dispatch the bare continuation directly. The lock-through-
        // OnCompleted discipline guarantees the continuation is stored before _pending can be
        // claimed (a Signal blocks on the lock until registration released it).
        if (_resolve is null)
        {
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

            if (runContinuationsAsynchronously)
                Scheduler.SubmitDetached(this, preferLocal: true);
            else
                _waitContinuation!();
            return true;
        }

        if (runContinuationsAsynchronously)
        {
            // Async dispatch: claim only; the resolve must run on the scheduler thread (OnWake),
            // not the producer's, so the executor's continuation lands there.
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

            Scheduler.SubmitDetached(this, preferLocal: true);
            return true;
        }

        // Inline dispatch: fold the claim and the resolve into one critical section instead of
        // claim-release-reacquire (the wake path's second Interlocked.Exchange). The resolver runs
        // on this (producer) thread either way in inline mode - the lock is what serializes the
        // consumer-role migration. SetResult must run OUTSIDE the lock: the continuation it invokes
        // re-enters Pump (the executor's next pull), which re-acquires.
        WakeOutcome outcome;
        AcquireWakeLock();
        try
        {
            if (!_pending)
                return false;
            _pending = false;

            outcome = _resolve!();
            if (outcome == WakeOutcome.Wait)
                _pending = true;  // spurious wake: re-arm under the same hold
        }
        finally
        {
            ReleaseWakeLock();
        }

        if (outcome != WakeOutcome.Wait)
            SetResultCore(outcome == WakeOutcome.GotItem);
        return true;
    }

    void IThreadPoolWorkItem.Execute()
    {
        if (_resolve is null)
            _waitContinuation!();
        else
            OnWake();
    }

    // Runs when an async-dispatched wake fires on the scheduler. Re-resolve: complete the value
    // task on an item or completion, otherwise the pump re-armed and we wait for the next wake.
    void OnWake()
    {
        switch (Pump())
        {
            case WakeOutcome.GotItem:
                SetResultCore(true);
                break;
            case WakeOutcome.Completed:
                SetResultCore(false);
                break;
            // Wait: spurious wake, _pending re-armed, nothing to complete.
        }
    }

    /// <summary>Marks the source completed and wakes any waiting pull.</summary>
    public void Complete()
    {
        _cts.Cancel();
        SignalCore(runContinuationsAsynchronously: true);
    }

    void SetResultCore(bool result) => _core.SetResult(result);

    bool IValueTaskSource<bool>.GetResult(short token)
    {
        var result = _core.GetResult(token);
        // Canonical resettable-VTS pattern: the consumer observes a wait's result exactly once, so
        // resetting here readies the core for the next wait - on the resume path, outside the wake
        // lock, and race-free (no Signal can target the core again until the next Pump arms
        // _pending, which happens-after this pull returns). Synchronous pulls never touch the core.
        _core.Reset();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
