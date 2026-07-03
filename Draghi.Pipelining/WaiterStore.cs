using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// Storage for committed waiters across the slot and SPSC queue tiers. Hides the slot-vs-queue
/// routing from Pipeline so commit, snapshot, and drain code is uniform.
///
/// The slot is a single inline (item, task) pair guarded by the tri-state _slotState word. It
/// absorbs the first committed waiter without allocating. When a second waiter arrives while the
/// slot is still occupied, the store escalates: lazily allocates the SPSC queue, moves the slot
/// contents to queue head (FIFO preserved) via a quiescent-only CAS, and enqueues the new waiter.
/// After escalation the slot stays empty for the rest of the run, and all subsequent commits use
/// the queue.
///
/// The _slotState word (0 empty / 1 occupied / 2 consuming) is the contract that lets the slot's
/// UnsafeOnCompleted callback, a stale-token reclaim, and the executor's escalation move race
/// safely (see TryClaimSlotForDrain). Slot contents may move to inline-storage backed by a class
/// in a future variant (e.g. when empirical workloads show most pipelines escalate, indirecting
/// the slot saves bytes per shell). The struct API stays the same either way.
[StructLayout(LayoutKind.Auto)]
struct WaiterStore<T>
{
    // Tri-state slot word, the latch's medicine applied to the slot.
    // 0 empty / 1 occupied / 2 consuming. The two-state word
    // (empty/occupied) plus the SPSC fields tore: a claim Exchange that won between a successor
    // commit's flag publish and its field writes read a torn or stale pair. The consuming state
    // brackets the drainer's read+clear so neither a successor commit nor the escalation move can
    // touch the cell mid-claim, and the commit goes data-then-license (fields, THEN publish) so a
    // visible Occupied always means the fields are complete.
    const int SlotEmpty = 0;
    const int SlotOccupied = 1;
    const int SlotConsuming = 2;

    T _slotItem;
    ValueTask _slotTask;
    int _slotState;
    // The moved task's completion, read under the escalation's won slot claim (the last point the
    // producer may legally touch the task). Executor-only: written by TryEscalateOrEnqueue's move,
    // read by the same thread's TakeMovedSlotPair.
    bool _movedWasCompleted;
    SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>? _queue;
    int _count; // Combined slot + queue.

    /// Total committed waiters (slot + queue). Volatile-read, reflecting the latest committed
    /// increment/decrement across executor and advancer threads. Clamped at zero: the internal
    /// count dips to a bounded -1 transient (see DecrementCount) that no consumer needs to
    /// observe - the negative never leaves this type.
    public int Count => Math.Max(Volatile.Read(ref _count), 0);

    /// True once the store has escalated to the queue tier. Stable after first true.
    public bool IsEscalated => Volatile.Read(ref _queue) is not null;

    /// Commit (item, task) to whichever tier the store routes it to: the inline slot when
    /// pre-escalation and the slot is empty (zero alloc), otherwise the SPSC queue (allocating
    /// it on the first overlap and atomically moving the slot contents to queue head if still
    /// owned). Returns the new count. <paramref name="isSlot"/> tells the caller which tier
    /// landed the item so the right completion callback can be wired.
    /// <paramref name="slotWasMoved"/> is TRUE iff this call performed the first escalation AND
    /// the executor's slot-claim CAS won. That is the only case where the post-publish race
    /// window could have stranded a slot callback, signaling that the caller should run the
    /// nudge (TryAcquire advancer + DrainReadyWaiters) afterwards.
    ///
    /// Single Volatile.Read(_queue) for the slot-or-queue routing. The slot commit needs no CAS:
    /// the executor is the single producer, the only writer that raises the word from empty, so a
    /// plain empty-check then field writes then a release publish is race-free (claims only go
    /// 1 -> 2 -> 0, escalation only 1 -> 0; none touch the empty state). The escalation slot-claim
    /// is a CAS(1 -> 0), quiescent-only: a consuming word (2) reads as not-moved and the drainer
    /// completing it under the latch keeps FIFO. It only runs on the first escalation
    /// (post-escalation the slot is guaranteed empty by PostEscalationSlotQuiescent, so subsequent
    /// calls take the queue-only fast path with no slot touch).
    public int TryEscalateOrEnqueue(T item, ValueTask task, out bool isSlot, out bool slotWasMoved)
    {
        slotWasMoved = false;
        var queue = Volatile.Read(ref _queue);
        if (queue is null)
        {
            // Pre-escalation: try the zero-alloc slot path first. Data, then license: the fields
            // land before the release-store that publishes Occupied, so a claimer that reads
            // Occupied is guaranteed the complete pair. No CAS (single producer raises the word).
            if (Volatile.Read(ref _slotState) == SlotEmpty)
            {
                _slotItem = item;
                _slotTask = task;
                Volatile.Write(ref _slotState, SlotOccupied);
                isSlot = true;
                return Interlocked.Increment(ref _count);
            }
            // Slot was occupied, so escalate. Allocate queue, CAS-claim the slot (1 -> 0, taking
            // only a quiescent occupant) against any concurrent slot callback, and move slot
            // contents to queue head if still owned. A consuming drainer (state 2) reads as
            // not-moved: it completes the occupant under the latch, so FIFO holds without a move.
            queue = new();
            Volatile.Write(ref _queue, queue);
            slotWasMoved = Interlocked.CompareExchange(ref _slotState, SlotEmpty, SlotOccupied) == SlotOccupied;
            if (slotWasMoved)
            {
                // Capture the moved task's completion NOW, while the won CAS still confers
                // exclusive ownership: the enqueue below publishes the pair to the queue head
                // where a drain may claim and consume it, ending the task token's lifetime -
                // after that no producer-side read of the task is legal. The caller's
                // compensation decision (activate vs leave for drain) consumes this captured
                // verdict via TakeMovedSlotPair.
                _movedWasCompleted = _slotTask.IsCompleted;
                queue.Enqueue((_slotItem, _slotTask));
                // Fields deliberately NOT cleared here: the caller pulls its copy of the moved
                // pair via TakeMovedSlotPair (compensation check needs the identity, and the
                // executor may not TryPeek - it is the SPSC producer). Safe to linger: every
                // other reader is gated on _slotState == Occupied (now Empty) or discards on
                // IsEscalated (now true), and post-escalation nothing writes the slot fields again.
                // Count carries through: slot's 1 → queue's 1.
            }
        }
        queue.Enqueue((item, task));
        isSlot = false;
        return Interlocked.Increment(ref _count);
    }

    /// Executor-only, immediately after a slotWasMoved = true return from
    /// <see cref="TryEscalateOrEnqueue"/>: hands over the moved item's identity and the
    /// completion verdict captured at the escalation claim (for the chain-arm compensation
    /// check), and clears the slot fields. The task itself is deliberately NOT handed out: the
    /// pair is published at the queue head and a drain may consume it at any moment, so no
    /// producer-side read of it is legal past the claim. Must be called exactly once per
    /// slotWasMoved so a cached-but-idle shell doesn't pin the moved references.
    public void TakeMovedSlotPair(out T item, out bool wasCompletedAtMove)
    {
        item = _slotItem;
        wasCompletedAtMove = _movedWasCompleted;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _slotItem = default!;
        _slotTask = default;
    }

    /// Try to take ownership of the slot for draining. Returns true if the caller now owns the
    /// (item, task), false if the slot was empty, held a still-running task, or was claimed
    /// elsewhere (escalation or a concurrent drainer). The caller is responsible for decrementing
    /// the count via <see cref="DecrementCount"/> after processing.
    ///
    /// Tri-state protocol: peek the task under the stable Occupied state. A
    /// 1 -> 0 -> 1 cycle is impossible (the only droppers are this latch-serialized claimer and
    /// the escalation, which drops permanently), so the field read is licensed. A still-running
    /// task bails with NO state change: the occupant's wired callback drains it on completion, so
    /// a stale-token reclaim that lands here neither claims a live task nor blocks in GetResult.
    /// A completed task claims the consuming state (CAS 1 -> 2), reads + clears the pair under
    /// exclusive ownership (no successor commit or escalation can touch the cell at state 2), then
    /// releases (2 -> 0). The CAS losing means escalation took the pair between the peek and the
    /// claim - the occupant is the queue head now, and the caller's IsEscalated check reroutes.
    public bool TryClaimSlotForDrain(out T item, out ValueTask task)
    {
        if (Volatile.Read(ref _slotState) == SlotOccupied && _slotTask.IsCompleted
            && Interlocked.CompareExchange(ref _slotState, SlotConsuming, SlotOccupied) == SlotOccupied)
        {
            item = _slotItem;
            task = _slotTask;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _slotItem = default!;
            _slotTask = default;
            Volatile.Write(ref _slotState, SlotEmpty);
            return true;
        }
        item = default!;
        task = default;
        return false;
    }

    /// Accounts one consumed entry against the combined count. Returns TRUE when this decrement
    /// drained the store (no successor's commit counted before it), FALSE when a successor's
    /// commit-increment preceded it. The raw count and its skew floor are internal: a consumer
    /// taking a visible-but-uncounted entry (the committer is between its enqueue/slot-write and
    /// its increment - the increment is deliberately LAST, it is the release that licenses the
    /// consumer's peek) sends the count transiently negative, bounded at -1 by the single
    /// producer. Hence drained means at-or-below zero, never exactly zero: the -1 face says the
    /// in-flight commit's entry was itself the one consumed, so nothing peekable remains - treating
    /// it as not-drained would peek an empty store.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool DecrementCount()
    {
        var count = Interlocked.Decrement(ref _count);
        Debug.Assert(count >= -1);
        return count <= 0;
    }

    /// Advancer-latch-holder-only peek of the slot occupant for chain activation
    /// (DrainSlotInline's D-path arm). Only valid after the caller's DecrementCount
    /// returned FALSE (not drained): the occupant's commit wrote the slot fields before its count increment,
    /// and the caller's atomic decrement observed that increment, so the fields are fully
    /// published to the caller. Returns false when a first escalation raced in and owns (or
    /// owned) the contents - the occupant is, or is about to be, the queue head. The queue
    /// is published before the escalation's slot claim, so an escalation clear that raced
    /// the field reads here is always caught by the post-barrier queue check.
    public bool TryPeekSlotForActivation(out T item, out ValueTask task)
    {
        item = _slotItem;
        task = _slotTask;
        Interlocked.MemoryBarrier();
        if (Volatile.Read(ref _queue) is not null)
        {
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                item = default!;
            task = default;
            return false;
        }
        return true;
    }

    /// Peek the queue head. Returns false (and default entry) when the store has not escalated.
    public bool TryPeek(out (T Waiter, ValueTask WaiterTask) entry)
    {
        var queue = _queue;
        if (queue is null)
        {
            entry = default;
            return false;
        }
        return queue.TryPeek(out entry);
    }

    /// Dequeue the queue head. Returns false (and default entry) when the store has not escalated.
    public bool TryDequeue(out (T Waiter, ValueTask WaiterTask) entry)
    {
        var queue = _queue;
        if (queue is null)
        {
            entry = default;
            return false;
        }
        return queue.TryDequeue(out entry);
    }

    /// Public-Enumerator snapshot of the inline slot. Volatile-read so cross-thread snapshots
    /// observe whether the slot is currently occupied.
    public bool TrySnapshotSlot(out T item)
    {
        if (Volatile.Read(ref _slotState) == SlotOccupied)
        {
            item = _slotItem;
            return true;
        }
        item = default!;
        return false;
    }

    /// Committer-side probe for the commit verify: whether the just-committed sole head has been
    /// taken by a drain (slot claimed or cleared, or the queue emptied). Only meaningful under the
    /// commit's activation lock and AFTER a Count re-read observed zero: that acquire orders these
    /// reads behind the drain's fenced decrement so they are fresh, and the committer is the sole
    /// producer so nothing can refill the store during the hold.
    public bool CommitHeadTaken()
        => _queue is null ? Volatile.Read(ref _slotState) != SlotOccupied : _queue.IsEmpty;

    /// The underlying queue if escalated. Used by the public Enumerator to walk queue entries.
    public SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>? Queue => _queue;

    /// Defaults the per-run reference fields (slot contents) so a cached-but-idle Pipeline shell
    /// doesn't pin them across an idle period. The SPSC queue, if allocated, is structurally
    /// reusable across runs (always empty at completion) and is intentionally kept.
    public void Reset()
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _slotItem = default!;
        _slotTask = default;
        Debug.Assert(_slotState == SlotEmpty);
        Debug.Assert(_count == 0);
    }
}
