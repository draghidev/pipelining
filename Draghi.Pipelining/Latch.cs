using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Non-blocking SR latch: try-or-bail mutual exclusion.
/// <remarks>
/// Both Acquire and Release use full-fence Interlocked.Exchange (seq-cst). Release-only
/// (Volatile.Write STLR on ARM) is not enough: a racing acquirer's Exchange can read a stale
/// held=true and bail out, stranding work.
///
/// A Lock can afford release-only because missed acquirers park-and-wait-for-wake. A Latch has no
/// wake path, missed acquirers walk away. The release must win the visibility race the first time.
/// </remarks>
struct Latch
{
    bool _held;

    /// Attempts to acquire the latch. Returns true if acquired (was not held), false if another
    /// caller currently holds it. Never blocks.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAcquire() => !Interlocked.Exchange(ref _held, true);

    /// Releases the latch. Must only be called by the current holder.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Release() => Interlocked.Exchange(ref _held, false);

    /// Reads the current held state. Volatile.Read to defeat JIT caching across calls. The
    /// hardware doesn't actually need a fence for visibility (cache coherence delivers writes),
    /// but the JIT might otherwise hoist the read.
    public bool IsHeld
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Volatile.Read(ref _held);
    }
}
