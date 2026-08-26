using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Owns the lifetime boundary of the FIFO head: callback-delivery exclusion and the
/// monotonically increasing identity assigned when the head is claimed.
struct ItemTenure
{
    // Zero means that no head carries an undelivered callback. Head identities start at one.
    long _armedCallbackSequence;
    long _lastClaimedSequence;
    int _activeCompletionCallbacks;

    public long LastClaimedSequence => Volatile.Read(ref _lastClaimedSequence);
    public long NextSequence => Volatile.Read(ref _lastClaimedSequence) + 1;

    /// Must precede callback registration. An already-completed task may deliver inline from
    /// UnsafeOnCompleted, so delivery must be able to clear the arm during registration.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ArmCompletionCallback(long sequence) => Volatile.Write(ref _armedCallbackSequence, sequence);

    /// Called as the callback's first action. Once cleared, consuming the task can no longer tear
    /// callback dispatch's reads of its continuation pair. Active publication precedes the clear:
    /// once another driver may consume the item, teardown can already see that this callback still
    /// has an acquire-or-deposit tail to run.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkCompletionCallbackDelivered()
    {
        Interlocked.Increment(ref _activeCompletionCallbacks);
        Volatile.Write(ref _armedCallbackSequence, 0);
    }

    public bool HasActiveCompletionCallback => Volatile.Read(ref _activeCompletionCallbacks) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CompleteCompletionCallback()
    {
        var remaining = Interlocked.Decrement(ref _activeCompletionCallbacks);
        Debug.Assert(remaining >= 0, "Completion callback activity under-ran.");
    }

    /// Claims remain serialized by the advance license. A stale nonzero arm can only cause a
    /// harmless declined claim; the callback's subsequent license acquisition re-drives it.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsCompletionCallbackPendingForHead()
        => Volatile.Read(ref _armedCallbackSequence) == Volatile.Read(ref _lastClaimedSequence) + 1;

    /// Advances the tenure identity in the same exclusive operation that removes the FIFO head.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ClaimHead()
    {
        var sequence = _lastClaimedSequence + 1;
        Volatile.Write(ref _lastClaimedSequence, sequence);
        return sequence;
    }

    public void EnsureIdle()
    {
        var armedSequence = Volatile.Read(ref _armedCallbackSequence);
        var activeCallbacks = Volatile.Read(ref _activeCompletionCallbacks);
        if (armedSequence != 0 || activeCallbacks != 0)
        {
            throw new UnreachableException(
                $"Completion callback remained live at structural quiescence " +
                $"(sequence={armedSequence}, active={activeCallbacks}).");
        }
    }

    public void Reset()
    {
        _armedCallbackSequence = 0;
        _activeCompletionCallbacks = 0;
    }
}
