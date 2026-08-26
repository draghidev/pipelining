using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Owns the unique activation turn and the versioned handoff of deferred activation.
struct ActivationGate<T>()
{
    const long HandoffStateMask = 1;
    const long NoHandoff = 0;
    const long Handoff = 1;

    // Zero is unowned, a negative generation is provisional, and a positive head sequence is committed.
    long _turnOwner;
    long _generation;
    long _resolvedHandoffGeneration;
    // Packed handoff generation and Handoff/NoHandoff state.
    long _handoffState;
    T _handoffItem = default!;

    static long StateOf(long state) => state & HandoffStateMask;
    static long GenerationOf(long state) => state >> 1;

    public EdgeLock EdgeLock { get; } = new();

    public long TurnOwner => Volatile.Read(ref _turnOwner);
    public bool IsTurnOwned => Volatile.Read(ref _turnOwner) != 0;
    public bool HasHandoff => StateOf(Volatile.Read(ref _handoffState)) == Handoff;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long NextGeneration() => Interlocked.Increment(ref _generation);

    public bool TryClaimProvisionalTurn(long generation)
    {
        Debug.Assert(generation >= 1);
        return Interlocked.CompareExchange(ref _turnOwner, -generation, 0) == 0;
    }

    public long ClaimOrInheritProvisionalTurn(long generation)
    {
        Debug.Assert(generation >= 1);
        var turnOwner = Volatile.Read(ref _turnOwner);
        if (turnOwner == -generation)
            return generation;
        return Interlocked.CompareExchange(ref _turnOwner, -generation, 0) == 0 ? generation : 0;
    }

    public bool CommitTurn(long generation, long sequence)
    {
        var turnOwner = Volatile.Read(ref _turnOwner);
        if (generation != 0 && turnOwner == -generation)
        {
            Volatile.Write(ref _turnOwner, sequence);
            return true;
        }
        Debug.Assert(turnOwner == 0,
            $"Edge commit over a foreign activation turn: owner={turnOwner}, generation={generation}, sequence={sequence}.");
        Volatile.Write(ref _turnOwner, sequence);
        return false;
    }

    public void AssignTurnAtStop(long sequence)
    {
        Debug.Assert(Volatile.Read(ref _turnOwner) == 0, "Stop assigned over a live activation turn.");
        Volatile.Write(ref _turnOwner, sequence);
    }

    public void AssignTurnForRecovery(long sequence)
    {
        var turnOwner = Volatile.Read(ref _turnOwner);
        Debug.Assert(turnOwner == 0 || turnOwner == sequence, "Recovery assigned over a foreign activation turn.");
        Volatile.Write(ref _turnOwner, sequence);
    }

    public bool TryReleaseTurn(long owner)
    {
        Debug.Assert(owner != 0);
        return Interlocked.CompareExchange(ref _turnOwner, 0, owner) == owner;
    }

    /// Publishes an executor-owned handoff. The item is written before its versioned publication,
    /// binding an empty-edge observer to that placement.
    public long PublishHandoff(T item)
    {
        Debug.Assert(StateOf(Volatile.Read(ref _handoffState)) == NoHandoff,
            "Activation handoff published over an unresolved handoff.");
        _handoffItem = item;
        var generation = Interlocked.Increment(ref _generation);
        Volatile.Write(ref _handoffState, (generation << 1) | Handoff);
        return generation;
    }

    public void ClearConsumedHandoffItem()
    {
        if (!HasHandoff && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _handoffItem = default!;
    }

    public bool TryTakeHandoff()
    {
        while (true)
        {
            var state = Volatile.Read(ref _handoffState);
            if (StateOf(state) != Handoff)
                return false;
            if (Interlocked.CompareExchange(ref _handoffState,
                    GenerationOf(state) << 1 | NoHandoff, state) == state)
                return true;
        }
    }

    /// Dispatch-time reclaim follows the same claim-before-take order as the empty-edge pass.
    /// Its callers establish that no resident activation turn can be live.
    public bool TryReclaimHandoff(long generation)
    {
        Debug.Assert(generation >= 1);
        if (!TryClaimProvisionalTurn(generation))
            return false;
        if (TryTakeHandoff(generation))
            return true;
        TryReleaseTurn(-generation);
        return false;
    }

    public bool TryPeekHandoff(out long generation, out T item)
    {
        var state = Volatile.Read(ref _handoffState);
        if (StateOf(state) != Handoff)
        {
            generation = 0;
            item = default!;
            return false;
        }
        generation = GenerationOf(state);
        item = _handoffItem;
        return true;
    }

    public bool TryTakeHandoff(long generation)
    {
        var state = Volatile.Read(ref _handoffState);
        if (StateOf(state) != Handoff || GenerationOf(state) != generation)
            return false;
        return Interlocked.CompareExchange(ref _handoffState,
            generation << 1 | NoHandoff, state) == state;
    }

    public void MarkHandoffResolved(long generation)
        => Volatile.Write(ref _resolvedHandoffGeneration, generation);

    public bool IsHandoffResolved(long generation)
        => Volatile.Read(ref _resolvedHandoffGeneration) >= generation;

    public void EnsureIdle()
    {
        var turnOwner = Volatile.Read(ref _turnOwner);
        var handoffState = Volatile.Read(ref _handoffState);
        if (turnOwner != 0 || StateOf(handoffState) != NoHandoff)
        {
            throw new UnreachableException(
                $"Activation gate remained live at structural quiescence " +
                $"(turnOwner={turnOwner}, handoff={StateOf(handoffState) == Handoff}, " +
                $"generation={GenerationOf(handoffState)}).");
        }
        EdgeLock.EnsureIdle();
    }

    public void Reset()
    {
        _turnOwner = 0;
        var generation = Volatile.Read(ref _generation);
        _resolvedHandoffGeneration = generation;
        _handoffState = generation << 1 | NoHandoff;
        _handoffItem = default!;
        EdgeLock.Reset();
    }
}
