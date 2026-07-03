using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// Env-gated event-ring tracer for the drain/activation/latch interleavings (DRAGHI_DRAIN_TRACE=1).
/// Temporary hunt instrumentation, stripped when the strand is classified. One global ring across
/// all pipelines. Test harnesses Reset() per iteration and print Dump() in the failure message so
/// the interleaving that produced a strand is on tape, no dump archaeology. When the env var is
/// unset Enabled is a JIT-time constant false and every guarded Record call folds away.
static class DrainTrace
{
    public static readonly bool Enabled = Environment.GetEnvironmentVariable("DRAGHI_DRAIN_TRACE") is "1";

    public enum Kind : byte
    {
        None,
        // Waiter-task completion callback (OnWaiterTaskCompleted). a: store escalated.
        CbFire,
        CbAcq,
        CbBail,
        // Slot-mode drain (DrainSlotInline).
        SlotClaim,
        // Claim failed, released. a: deposit consumed, b: reacquired to serve it.
        SlotFailRel,
        SlotDPathAct,
        // D-path peeked a completed-at-commit head, left it for the reclaim.
        SlotDPathDone,
        // D-path peek miss, published _pendingHeadActivation. a: claimed back, b: head task completed.
        SlotDPathPend,
        SlotCPathAct,
        // a: store count, b: _executingItemActivationPending.
        SlotCPathSkip,
        // Main release. a: deposit consumed, b: emptyReached.
        SlotRel,
        // a: reacquire outcome for the consumed deposit.
        SlotServeReacq,
        // a: reclaim taken (signal set and latch acquired).
        SlotReclaim,
        // Queue-mode drain (DrainReadyWaiters). a: store count at pass start.
        QPass,
        QDrain,
        QCPathAct,
        // a: 0 = count 0 and gate free, +1 = gate live, +2 = count nonzero. b: pending flag.
        QCPathSkip,
        // License declined on residency: count 0 but a head is resident (its increment not landed).
        QCPathPeekDecline,
        QDPathAct,
        // D-path peeked a completed-at-commit head, left it for this pass's own drain.
        QDPathDone,
        // Signal-conservation check on an empty pass. a: restored, b: store count.
        QConserve,
        // Release with no deposit.
        QRel,
        // Release consumed a deposit. a: reacquire outcome.
        QRelServe,
        // a: recheck verdict (cancellation and _drainSignal).
        QRecheck,
        QReclaimBail,
        // a: 1 = found on the bounded stale-view retry, 0 = first peek.
        QReclaimHit,
        // Reclaim acquired but peek found no completed head. a: deposit consumed on release, b: reacquired.
        QReclaimMiss,
        // Waiter committed (CommitWaiter). a: store count after enqueue, b: slotWasMoved.
        Commit,
        // wasEmpty self-activate granted.
        CommitSelfAct,
        // wasEmpty self-activate declined, activation turn is live.
        CommitGateSkip,
        // Post-grant verify tripped: the granted head was retired inside the lock hold (count
        // zero, head taken), so the commit released its own grant and cleared the activated slot.
        CommitVerifyClear,
        // Completion callback landed in a commit's registration-to-publish window; recorded for
        // the commit's post-publish replay instead of running against the not-yet-visible item.
        CbDeferred,
        // Moved-pair compensation claim. a: won the flag exchange, b: moved task completed.
        CommitMoved,
        // Escalation nudge. a: latch acquired (0 = deposited on the holder).
        CommitNudge,
        // ActivateHeadItem, every activation path.
        Act,
        // CompleteWaiterDeferred, every retirement. a: depth after decrement, b: faulted.
        Retire,
    }

    public struct Entry
    {
        public int Seq;
        public Kind Kind;
        public object? Item;
        public int A;
        public int B;
        public int Thread;
    }

    const int Size = 4096;
    static readonly Entry[] _ring = Enabled ? new Entry[Size] : [];
    static int _ticket = -1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Record(Kind kind, int a = 0, int b = 0)
    {
        if (Enabled)
            RecordSlow(kind, null, a, b);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordItem<TItem>(Kind kind, TItem item, int a = 0, int b = 0)
    {
        if (Enabled)
            RecordSlow(kind, item, a, b);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static void RecordSlow(Kind kind, object? item, int a, int b)
    {
        var seq = Interlocked.Increment(ref _ticket);
        ref var e = ref _ring[seq & (Size - 1)];
        e.Seq = seq;
        e.Kind = kind;
        e.Item = item;
        e.A = a;
        e.B = b;
        e.Thread = Environment.CurrentManagedThreadId;
    }

    /// Clears the ring. Call at the start of a hunt iteration so a failure dump holds only that
    /// iteration's events. A straggler thread from a prior iteration may still record afterwards,
    /// benign noise at the front of the tape.
    public static void Reset()
    {
        if (!Enabled)
            return;
        Array.Clear(_ring);
        Volatile.Write(ref _ticket, -1);
    }

    /// Formats the ring oldest to newest, one event per line: seq, thread, kind, item, args.
    public static string Dump()
    {
        if (!Enabled)
            return "(drain trace off, set DRAGHI_DRAIN_TRACE=1)";
        var ticket = Volatile.Read(ref _ticket);
        if (ticket < 0)
            return "(drain trace empty)";
        var sb = new System.Text.StringBuilder();
        var first = Math.Max(0, ticket - (Size - 1));
        for (var seq = first; seq <= ticket; seq++)
        {
            var e = _ring[seq & (Size - 1)];
            // A racing writer can tear an in-flight entry, mark instead of misattributing.
            if (e.Seq != seq)
            {
                sb.AppendLine($"#{seq} <torn/overwritten>");
                continue;
            }
            sb.Append('#').Append(e.Seq).Append(" t").Append(e.Thread).Append(' ').Append(e.Kind);
            if (e.Item is not null)
                sb.Append(" item=").Append(e.Item);
            sb.Append(" a=").Append(e.A).Append(" b=").Append(e.B).AppendLine();
        }
        return sb.ToString();
    }
}
