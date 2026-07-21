using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Non-reentrant mutex for short empty-edge sections. Contention lazily allocates the park handle.
sealed class EdgeLock
{
    const int Unlocked = 0;
    const int Locked = 1;
    const int Contended = 2;

    int _state;
    AutoResetEvent? _wake;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Enter()
    {
        if (Interlocked.CompareExchange(ref _state, Locked, Unlocked) != Unlocked)
            EnterSlow();

        [MethodImpl(MethodImplOptions.NoInlining)]
        void EnterSlow()
        {
            var wake = _wake ?? EnsureWake();
            // A slow-path winner retains the waiter bit so its release wakes parked rivals. Auto-reset
            // absorbs a wake delivered between the exchange and WaitOne.
            while (Interlocked.Exchange(ref _state, Contended) != Unlocked)
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
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Exit()
    {
        // The RMW observes a racing waiter publication; a plain store could strand that waiter.
        if (Interlocked.Exchange(ref _state, Unlocked) == Contended)
            _wake!.Set();
    }
}
