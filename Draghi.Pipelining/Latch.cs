using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Non-blocking pending-word latch: try-or-bail mutual exclusion with obligation transfer.
///
/// A tri-state word (free / held / held+pending), the state half of a Drepper-style futex
/// mutex with the kernel wait removed. A plain SR latch (held/free) plus a separate signal
/// flag makes every release/recheck pair a two-location Dekker rendezvous, where a contended
/// acquirer's one-shot wake can land against a holder whose recheck already ran, stranding
/// the work. Folding the obligation INTO the latch word
/// makes deposit-vs-release a single-cell atomic race that neither side can lose silently:
///
///   - TryAcquireOrFlagPending: CAS free -> held wins the latch; against a holder it CASes
///     the pending bit on instead. Either the caller holds the latch or the holder now owes
///     a post-release serve. No third outcome.
///   - ReleaseAndCheckPending: a single Exchange to free reads the pending bit atomically
///     with releasing. A deposit either lands before the Exchange (the releaser learns of
///     it, re-acquires, and serves) or after it (the depositor's CAS sees free and wins the
///     latch itself). The lost-wake window is gone by construction.
///
/// There are no waiter semantics: nobody waits, the pending bit carries no identity or
/// count. It is one bit of "somebody bailed against this hold", and a spurious serve pass
/// costs one no-op drain probe.
///
/// All ops are full-fence interlocked RMW on the one word, so the SR latch's release
/// visibility race (stale held=true read by an acquirer) cannot occur here either.
struct Latch
{
    const int Free = 0;
    const int HeldBit = 1;
    const int PendingBit = 2;

    int _word;

    /// Attempts to acquire the latch. Returns true if acquired (was free), false if another
    /// caller currently holds it. Never blocks, never deposits - callers off the wake
    /// protocol (no signal to strand) use this form.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAcquire() => Interlocked.CompareExchange(ref _word, HeldBit, Free) == Free;

    /// Attempts to acquire the latch; on contention, deposits the pending obligation on the
    /// current holder instead. Returns true if acquired (caller is the drainer), false if
    /// the obligation was deposited (the holder's ReleaseAndCheckPending serves it). Every
    /// wake-protocol acquirer must use this form: a plain failed TryAcquire against a
    /// transient hold is a one-shot wake spent against a recheck that already ran.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAcquireOrFlagPending()
    {
        var word = Volatile.Read(ref _word);
        while (true)
        {
            var desired = word == Free ? HeldBit : word | PendingBit;
            var seen = Interlocked.CompareExchange(ref _word, desired, word);
            if (seen == word)
                return word == Free;
            word = seen;
        }
    }

    /// Releases the latch, atomically consuming and reporting a deposited obligation. Must
    /// only be called by the current holder. Returns true if a contended acquirer deposited
    /// pending during this hold - the caller is then obligated to re-acquire (via
    /// TryAcquireOrFlagPending) and serve, or otherwise guarantee the work is rechecked.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ReleaseAndCheckPending()
    {
        var previous = Interlocked.Exchange(ref _word, Free);
        Debug.Assert((previous & HeldBit) != 0, "Latch released while not held.");
        return (previous & PendingBit) != 0;
    }

    /// Reads the current held state. Volatile.Read to defeat JIT caching across calls. The
    /// hardware doesn't actually need a fence for visibility (cache coherence delivers writes),
    /// but the JIT might otherwise hoist the read.
    public bool IsHeld
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (Volatile.Read(ref _word) & HeldBit) != 0;
    }
}
