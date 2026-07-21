using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Owns the lifetime boundary of the FIFO head: callback-delivery exclusion and the
/// monotonically increasing identity assigned when the head is claimed.
struct ItemTenure
{
    // Zero means that no head carries an undelivered callback. Head identities start at one.
    long _armedCallbackSequence;
    long _headSequence;

    public long LastClaimedSequence => Volatile.Read(ref _headSequence);
    public long HeadSequence => Volatile.Read(ref _headSequence) + 1;

    /// Must precede callback registration. An already-completed task may deliver inline from
    /// UnsafeOnCompleted, so delivery must be able to clear the arm during registration.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ArmCompletionCallback(long sequence) => Volatile.Write(ref _armedCallbackSequence, sequence);

    /// Called as the callback's first action. Once cleared, consuming the task can no longer tear
    /// callback dispatch's reads of its continuation pair.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkCompletionCallbackDelivered() => Volatile.Write(ref _armedCallbackSequence, 0);

    /// Claims remain serialized by the advance license. A stale nonzero arm can only cause a
    /// harmless declined claim; the callback's subsequent license acquisition re-drives it.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsCompletionCallbackPendingForHead()
        => Volatile.Read(ref _armedCallbackSequence) == Volatile.Read(ref _headSequence) + 1;

    /// Advances the tenure identity in the same exclusive operation that removes the FIFO head.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ClaimHead()
    {
        var sequence = _headSequence + 1;
        Volatile.Write(ref _headSequence, sequence);
        return sequence;
    }

    public void Reset()
    {
        Debug.Assert(Volatile.Read(ref _armedCallbackSequence) == 0);
    }
}
