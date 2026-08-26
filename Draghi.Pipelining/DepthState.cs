using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Draghi.Pipelining;

/// Tracks dispatched items through retirement and coordinates momentary-empty observation. Dispatch
/// and retirement counters are separated because their writers may run on different cores. Empty
/// waits use an arm-and-recheck handshake so either side of a concurrent zero transition signals.
struct DepthState
{
    // Explicit layout preserves isolation inside this ref-bearing auto-layout struct.
    [StructLayout(LayoutKind.Explicit, Size = 256)]
    struct PaddedDispatchCounter
    {
        [FieldOffset(128)] public uint Dispatched;
        // This executor-owned snapshot deliberately shares the dispatch line.
        [FieldOffset(132)] public uint RetiredCache;
    }

    PaddedDispatchCounter _dispatchCounter;
    uint _retiredCount;
    // Non-null both stores the waiter and publishes that empty observation is armed.
    TaskCompletionSource? _emptyWaiter;

    /// <summary>Returns a lock-free snapshot of dispatched items not yet retired.</summary>
    public int Depth
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            // Retirement cannot precede dispatch, so this order cannot produce a negative result.
            var retired = Volatile.Read(ref _retiredCount);
            var dispatched = Volatile.Read(ref _dispatchCounter.Dispatched);
            return (int)(dispatched - retired);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordDispatch()
    {
        // Dispatch is executor-owned; only its publication requires a release store.
        var next = _dispatchCounter.Dispatched + 1;
        // Read the cross-core completion counter only near the overflow limit.
        if (next - _dispatchCounter.RetiredCache > (uint)int.MaxValue)
            RefreshCacheOrThrow(next);
        Volatile.Write(ref _dispatchCounter.Dispatched, next);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void RefreshCacheOrThrow(uint next)
    {
        var retired = Volatile.Read(ref _retiredCount);
        if (next - retired > (uint)int.MaxValue)
            throw new InvalidOperationException("Pipeline depth overflow.");
        _dispatchCounter.RetiredCache = retired;
    }

    /// <summary>Records retirement and returns the resulting depth. A zero result must be passed to
    /// <see cref="OnDepthReachedZero"/> after retirement finishes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int RecordRetirement()
    {
        // Retirements may overlap, so only the increment winner observes each count.
        var retired = Interlocked.Increment(ref _retiredCount);
        var dispatched = Volatile.Read(ref _dispatchCounter.Dispatched);
        return (int)(dispatched - retired);
    }

    /// <summary>
    /// Returns a task for momentary emptiness. The caller supplies backlog because this component
    /// counts only dispatched items; both values must be zero. After publishing the waiter, it
    /// rechecks both halves to cover a zero transition that preceded the arm.
    /// </summary>
    public ValueTask WaitForEmptyAsync(int backlog, CancellationToken cancellationToken)
    {
        if (backlog is 0 && Depth is 0)
            return ValueTask.CompletedTask;

        var newTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Publish the arm before rechecking for a zero transition that may already have occurred.
        var tcs = Interlocked.CompareExchange(ref _emptyWaiter, newTcs, null) ?? newTcs;
        if (backlog is 0 && Depth is 0)
            SignalEmptyWaiter();

        return new(tcs.Task.WaitAsync(cancellationToken));
    }

    /// <summary>
    /// Rechecks an armed wait using the executor's current backlog observation.
    /// </summary>
    public void RecheckEmpty(int backlog)
    {
        if (backlog is 0 && Depth is 0)
            SignalEmptyWaiter();
    }

    /// <summary>
    /// Handles a retirement that observed zero. The waiter is taken and depth revalidated because
    /// signalling may be deferred until after another dispatch or a newer arm.
    /// </summary>
    public void OnDepthReachedZero()
    {
        // The retirement RMW orders this read. A later arm is covered by its own recheck.
        if (Volatile.Read(ref _emptyWaiter) is null)
            return;

        while (true)
        {
            var tcs = Interlocked.Exchange(ref _emptyWaiter, null);
            if (tcs is null)
                return;

            // An arm published after the observed zero requires a fresh depth check.
            if (Depth is 0)
            {
                tcs.TrySetResult();
                return;
            }

            // Return a waiter taken for a stale zero unless another arm replaced it.
            if (Interlocked.CompareExchange(ref _emptyWaiter, tcs, null) is not null)
                return;

            // Republishing is another arm, so cover a zero transition during the return.
            if (Depth is not 0)
                return;
        }
    }

    void SignalEmptyWaiter()
    {
        // Exchange makes one caller responsible for each armed waiter.
        var tcs = Interlocked.Exchange(ref _emptyWaiter, null);
        tcs?.TrySetResult();
    }

    /// <summary>Starts a reused pipeline shell with a fresh ledger.</summary>
    public void Reset()
    {
        // A waiter left by the prior tenure is condemned with that tenure. Completing it here would
        // let an obsolete continuation run after the shell has been rebound to unrelated work.
        _emptyWaiter = null;
        _dispatchCounter = default;
        _retiredCount = 0;
    }
}
