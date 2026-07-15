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
    struct PaddedProducerCounter
    {
        [FieldOffset(128)] public uint Value;
        // This producer-owned snapshot deliberately shares the dispatch line.
        [FieldOffset(132)] public uint RetiredCache;
    }

    PaddedProducerCounter _dispatched;
    uint _retired;
    // Non-null both stores the waiter and publishes that empty observation is armed.
    TaskCompletionSource? _emptyWaiter;

    /// <summary>Returns a lock-free snapshot of dispatched items not yet retired.</summary>
    public int Depth
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            // Retirement cannot precede dispatch, so this order cannot produce a negative result.
            var comp = Volatile.Read(ref _retired);
            var enq = Volatile.Read(ref _dispatched.Value);
            return (int)(enq - comp);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void IncrementDepth()
    {
        // Dispatch is executor-owned; only its publication requires a release store.
        var next = _dispatched.Value + 1;
        // Read the cross-core completion counter only near the overflow limit.
        if (next - _dispatched.RetiredCache > (uint)int.MaxValue)
            RefreshCacheOrThrow(next);
        Volatile.Write(ref _dispatched.Value, next);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void RefreshCacheOrThrow(uint next)
    {
        var comp = Volatile.Read(ref _retired);
        if (next - comp > (uint)int.MaxValue)
            throw new InvalidOperationException("Pipeline depth overflow.");
        _dispatched.RetiredCache = comp;
    }

    /// <summary>Records retirement and returns the resulting depth. A zero result must be passed to
    /// <see cref="OnDepthReachedZero"/> after retirement finishes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int DecrementDepth()
    {
        // Retirements may overlap, so only the increment winner observes each count.
        var comp = Interlocked.Increment(ref _retired);
        var enq = Volatile.Read(ref _dispatched.Value);
        return (int)(enq - comp);
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
}
