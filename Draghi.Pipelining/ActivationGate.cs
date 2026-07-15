using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Owns the unique activation turn and the executor-to-empty-edge deferral handoff.
struct ActivationGate<T>
{
    const long DeferredStateMask = 1;
    const long NoDeferral = 0;
    const long Deferred = 1;

    readonly EdgeLock _edgeLock;
    long _turn;
    long _grantGeneration;
    long _resolvedHandoffGeneration;
    long _deferredWord;
    T _deferredItem = default!;

    static long StateOf(long word) => word & DeferredStateMask;
    static long GenerationOf(long word) => word >> 1;

    public ActivationGate()
    {
        _edgeLock = new EdgeLock();
    }

    public EdgeLock EdgeLock => _edgeLock;
    public long Turn => Volatile.Read(ref _turn);
    public bool HasTurn => Volatile.Read(ref _turn) != 0;
    public bool HandoffVisible => StateOf(Volatile.Read(ref _deferredWord)) == Deferred;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long NextGrantGeneration() => Interlocked.Increment(ref _grantGeneration);

    public bool TryClaimGrant(long generation)
    {
        Debug.Assert(generation >= 1);
        return Interlocked.CompareExchange(ref _turn, -generation, 0) == 0;
    }

    public long ClaimOrInherit(long generation)
    {
        Debug.Assert(generation >= 1);
        var turn = Volatile.Read(ref _turn);
        if (turn == -generation)
            return generation;
        return Interlocked.CompareExchange(ref _turn, -generation, 0) == 0 ? generation : 0;
    }

    public bool AssignAtCommit(long generation, long tenure)
    {
        var turn = Volatile.Read(ref _turn);
        if (generation != 0 && turn == -generation)
        {
            Volatile.Write(ref _turn, tenure);
            return true;
        }
        Debug.Assert(turn == 0,
            $"Edge commit over a foreign activation turn: turn={turn}, generation={generation}, tenure={tenure}.");
        Volatile.Write(ref _turn, tenure);
        return false;
    }

    public void AssignAtStop(long tenure)
    {
        Debug.Assert(Volatile.Read(ref _turn) == 0, "Stop assigned over a live activation turn.");
        Volatile.Write(ref _turn, tenure);
    }

    public void AssignAtRecovery(long tenure)
    {
        var turn = Volatile.Read(ref _turn);
        Debug.Assert(turn == 0 || turn == tenure, "Recovery assigned over a foreign activation turn.");
        Volatile.Write(ref _turn, tenure);
    }

    public bool Release(long owner)
    {
        Debug.Assert(owner != 0);
        return Interlocked.CompareExchange(ref _turn, 0, owner) == owner;
    }

    /// Publishes an executor-owned activation deferral. The item precedes the release publication
    /// of its versioned word, binding an empty-edge observer to this exact placement.
    public long PlaceHandoff(T item)
    {
        Debug.Assert(StateOf(Volatile.Read(ref _deferredWord)) == NoDeferral,
            "Activation handoff published over an unresolved handoff.");
        _deferredItem = item;
        var generation = Interlocked.Increment(ref _grantGeneration);
        Volatile.Write(ref _deferredWord, (generation << 1) | Deferred);
        return generation;
    }

    public void ClearStaleHandoffItem()
    {
        if (!HandoffVisible && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _deferredItem = default!;
    }

    public bool TryConsumeHandoff()
    {
        while (true)
        {
            var word = Volatile.Read(ref _deferredWord);
            if (StateOf(word) != Deferred)
                return false;
            if (Interlocked.CompareExchange(ref _deferredWord,
                    GenerationOf(word) << 1 | NoDeferral, word) == word)
                return true;
        }
    }

    /// Dispatch-time reclaim follows the same claim-before-consume order as the empty-edge pass.
    /// Its callers establish that no resident activation turn can be live.
    public bool TryReclaimHandoff(long generation)
    {
        Debug.Assert(generation >= 1);
        if (!TryClaimGrant(generation))
            return false;
        if (TryConsumeHandoff(generation))
            return true;
        Release(-generation);
        return false;
    }

    public bool TryPeekHandoff(out long generation, out T item)
    {
        var word = Volatile.Read(ref _deferredWord);
        if (StateOf(word) != Deferred)
        {
            generation = 0;
            item = default!;
            return false;
        }
        generation = GenerationOf(word);
        item = _deferredItem;
        return true;
    }

    public bool TryConsumeHandoff(long generation)
    {
        var word = Volatile.Read(ref _deferredWord);
        if (StateOf(word) != Deferred || GenerationOf(word) != generation)
            return false;
        return Interlocked.CompareExchange(ref _deferredWord,
            generation << 1 | NoDeferral, word) == word;
    }

    public void MarkHandoffResolved(long generation)
        => Volatile.Write(ref _resolvedHandoffGeneration, generation);

    public bool IsHandoffResolved(long generation)
        => Volatile.Read(ref _resolvedHandoffGeneration) >= generation;

    public void Reset()
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _deferredItem = default!;
        var turn = Volatile.Read(ref _turn);
        Debug.Assert(turn == 0, $"Activation turn still live at reset: {turn}.");
        Debug.Assert(StateOf(Volatile.Read(ref _deferredWord)) == NoDeferral);
    }

    internal string DebugWordStates()
    {
        var turn = Volatile.Read(ref _turn);
        var deferred = Volatile.Read(ref _deferredWord);
        return $"turn={(turn < 0 ? $"grant(g{-turn})" : turn.ToString())},handoff={(StateOf(deferred) == Deferred ? $"deferred(g{GenerationOf(deferred)})" : "none")}";
    }
}
