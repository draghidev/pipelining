using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Draghi.Pipelining;

/// Non-reentrant mutex for short empty-edge sections. State is 0 when free, 1 when held, and 2
/// when held with possible waiters. Contention lazily allocates the park handle.
internal sealed class EdgeLock
{
    int _state;
    AutoResetEvent? _wake;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Enter()
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            EnterSlow();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void EnterSlow()
    {
        var wake = _wake ?? EnsureWake();
        // A slow-path winner retains the waiter bit so its release wakes parked rivals. Auto-reset
        // absorbs a wake delivered between the exchange and WaitOne.
        while (Interlocked.Exchange(ref _state, 2) != 0)
            wake.WaitOne();
    }

    AutoResetEvent EnsureWake()
    {
        var created = new AutoResetEvent(false);
        var existing = Interlocked.CompareExchange(ref _wake, created, null);
        if (existing is null)
            return created;
        created.Dispose();
        return existing;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Exit()
    {
        // The RMW observes a racing waiter publication; a plain store could strand that waiter.
        if (Interlocked.Exchange(ref _state, 0) == 2)
            _wake!.Set();
    }
}
