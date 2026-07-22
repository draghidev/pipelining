using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Owns the unique activation turn and the versioned handoff of deferred activation.
struct ActivationGate<T>()
{
    const long HandoffStateMask = 1;
    const long NoHandoff = 0;
    const long Handoff = 1;

    long _turn;
    long _generation;
    long _resolvedHandoffGeneration;
    long _handoffWord;
    T _handoffItem = default!;

    static long StateOf(long word) => word & HandoffStateMask;
    static long GenerationOf(long word) => word >> 1;

    public EdgeLock EdgeLock { get; } = new();

    public long Turn => Volatile.Read(ref _turn);
    public bool HasTurn => Volatile.Read(ref _turn) != 0;
    public bool HasHandoff => StateOf(Volatile.Read(ref _handoffWord)) == Handoff;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long NextGeneration() => Interlocked.Increment(ref _generation);

    public bool TryClaimProvisionalTurn(long generation)
    {
        Debug.Assert(generation >= 1);
        return Interlocked.CompareExchange(ref _turn, -generation, 0) == 0;
    }

    public long ClaimOrInheritProvisionalTurn(long generation)
    {
        Debug.Assert(generation >= 1);
        var turn = Volatile.Read(ref _turn);
        if (turn == -generation)
            return generation;
        return Interlocked.CompareExchange(ref _turn, -generation, 0) == 0 ? generation : 0;
    }

    public bool CommitTurn(long generation, long sequence)
    {
        var turn = Volatile.Read(ref _turn);
        if (generation != 0 && turn == -generation)
        {
            Volatile.Write(ref _turn, sequence);
            return true;
        }
        Debug.Assert(turn == 0,
            $"Edge commit over a foreign activation turn: turn={turn}, generation={generation}, sequence={sequence}.");
        Volatile.Write(ref _turn, sequence);
        return false;
    }

    public void AssignTurnAtStop(long sequence)
    {
        Debug.Assert(Volatile.Read(ref _turn) == 0, "Stop assigned over a live activation turn.");
        Volatile.Write(ref _turn, sequence);
    }

    public void AssignTurnForRecovery(long sequence)
    {
        var turn = Volatile.Read(ref _turn);
        Debug.Assert(turn == 0 || turn == sequence, "Recovery assigned over a foreign activation turn.");
        Volatile.Write(ref _turn, sequence);
    }

    public bool Release(long owner)
    {
        Debug.Assert(owner != 0);
        return Interlocked.CompareExchange(ref _turn, 0, owner) == owner;
    }

    /// Publishes an executor-owned handoff. The item is written before its versioned publication,
    /// binding an empty-edge observer to that placement.
    public long PublishHandoff(T item)
    {
        Debug.Assert(StateOf(Volatile.Read(ref _handoffWord)) == NoHandoff,
            "Activation handoff published over an unresolved handoff.");
        _handoffItem = item;
        var generation = Interlocked.Increment(ref _generation);
        Volatile.Write(ref _handoffWord, (generation << 1) | Handoff);
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
            var word = Volatile.Read(ref _handoffWord);
            if (StateOf(word) != Handoff)
                return false;
            if (Interlocked.CompareExchange(ref _handoffWord,
                    GenerationOf(word) << 1 | NoHandoff, word) == word)
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
        Release(-generation);
        return false;
    }

    public bool TryPeekHandoff(out long generation, out T item)
    {
        var word = Volatile.Read(ref _handoffWord);
        if (StateOf(word) != Handoff)
        {
            generation = 0;
            item = default!;
            return false;
        }
        generation = GenerationOf(word);
        item = _handoffItem;
        return true;
    }

    public bool TryTakeHandoff(long generation)
    {
        var word = Volatile.Read(ref _handoffWord);
        if (StateOf(word) != Handoff || GenerationOf(word) != generation)
            return false;
        return Interlocked.CompareExchange(ref _handoffWord,
            generation << 1 | NoHandoff, word) == word;
    }

    public void MarkHandoffResolved(long generation)
        => Volatile.Write(ref _resolvedHandoffGeneration, generation);

    public bool IsHandoffResolved(long generation)
        => Volatile.Read(ref _resolvedHandoffGeneration) >= generation;

    public void Reset()
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _handoffItem = default!;
        var turn = Volatile.Read(ref _turn);
        Debug.Assert(turn == 0, $"Activation turn still live at reset: {turn}.");
        Debug.Assert(StateOf(Volatile.Read(ref _handoffWord)) == NoHandoff);
    }

}
