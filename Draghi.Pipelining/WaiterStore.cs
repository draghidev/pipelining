using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// Storage for committed waiters across the slot and SPSC queue tiers. Hides the slot-vs-queue
/// routing from Pipeline so commit, snapshot, and drain code is uniform.
///
/// The slot is a single inline (item, task) pair guarded by a CAS-able _hasSlot flag. It absorbs
/// the first committed waiter without allocating. When a second waiter arrives while the slot is
/// still occupied, the store escalates: lazily allocates the SPSC queue, atomically moves the
/// slot contents to queue head (FIFO preserved), and enqueues the new waiter. After escalation
/// the slot stays empty for the rest of the run, and all subsequent commits use the queue.
///
/// The CAS on _hasSlot is the contract that lets the slot's UnsafeOnCompleted callback and the
/// executor's escalation move race safely: whoever wins the Exchange owns the slot contents.
/// Slot contents may move to inline-storage backed by a class in a future variant (e.g. when
/// empirical workloads show most pipelines escalate, indirecting the slot saves bytes per shell).
/// The struct API stays the same either way.
[StructLayout(LayoutKind.Auto)]
struct WaiterStore<T>
{
    T _slotItem;
    ValueTask _slotTask;
    int _hasSlot; // 0 = empty, 1 = occupied
    SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>? _queue;
    int _count; // Combined slot + queue.

    /// Total committed waiters (slot + queue). Volatile-read, reflecting the latest committed
    /// increment/decrement across executor and advancer threads.
    public int Count => Volatile.Read(ref _count);

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
    /// Single Volatile.Read(_queue) for the slot-or-queue routing. The slot-claim CAS on the
    /// pre-escalation path is needed to handle the slot-callback race. The escalation slot-claim
    /// CAS only runs on the first escalation (loosened-CAS optimization: post-escalation the
    /// slot is guaranteed empty by the invariant the spec verifies as PostEscalationSlotEmpty,
    /// so subsequent calls take the queue-only fast path with no slot touch).
    public int TryEscalateOrEnqueue(T item, ValueTask task, out bool isSlot, out bool slotWasMoved)
    {
        slotWasMoved = false;
        var queue = Volatile.Read(ref _queue);
        if (queue is null)
        {
            // Pre-escalation: try the zero-alloc slot path first.
            if (Interlocked.CompareExchange(ref _hasSlot, 1, 0) == 0)
            {
                _slotItem = item;
                _slotTask = task;
                isSlot = true;
                return Interlocked.Increment(ref _count);
            }
            // Slot was occupied, so escalate. Allocate queue, race the slot CAS-claim against
            // any concurrent slot callback, and move slot contents to queue head if still owned.
            queue = new();
            Volatile.Write(ref _queue, queue);
            slotWasMoved = Interlocked.Exchange(ref _hasSlot, 0) == 1;
            if (slotWasMoved)
            {
                queue.Enqueue((_slotItem, _slotTask));
                // Fields deliberately NOT cleared here: the caller pulls its copy of the moved
                // pair via TakeMovedSlotPair (compensation check needs the identity, and the
                // executor may not TryPeek - it is the SPSC producer). Safe to linger: every
                // other reader is gated on _hasSlot == 1 (now 0) or discards on IsEscalated
                // (now true), and post-escalation nothing writes the slot fields again.
                // Count carries through: slot's 1 → queue's 1.
            }
        }
        queue.Enqueue((item, task));
        isSlot = false;
        return Interlocked.Increment(ref _count);
    }

    /// Executor-only, immediately after a slotWasMoved = true return from
    /// <see cref="TryEscalateOrEnqueue"/>: hands over the moved pair (for the chain-arm
    /// compensation check) and clears the slot fields. Must be called exactly once per
    /// slotWasMoved so a cached-but-idle shell doesn't pin the moved references.
    public void TakeMovedSlotPair(out T item, out ValueTask task)
    {
        item = _slotItem;
        task = _slotTask;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _slotItem = default!;
        _slotTask = default;
    }

    /// Try to take ownership of the slot for draining. Returns true if the caller now owns the
    /// (item, task), false if the slot was already empty or claimed elsewhere (escalation or a
    /// concurrent drainer). The caller is responsible for decrementing the count via
    /// <see cref="DecrementCount"/> after processing.
    public bool TryClaimSlotForDrain(out T item, out ValueTask task)
    {
        if (Interlocked.Exchange(ref _hasSlot, 0) == 1)
        {
            item = _slotItem;
            task = _slotTask;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _slotItem = default!;
            _slotTask = default;
            return true;
        }
        item = default!;
        task = default;
        return false;
    }

    /// Decrement the combined count. Returns the new value.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int DecrementCount() => Interlocked.Decrement(ref _count);

    /// Advancer-latch-holder-only peek of the slot occupant for chain activation
    /// (DrainSlotInline's count > 0 arm). Only valid after the caller's DecrementCount
    /// returned > 0: the occupant's commit wrote the slot fields before its count increment,
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
        if (Volatile.Read(ref _hasSlot) == 1)
        {
            item = _slotItem;
            return true;
        }
        item = default!;
        return false;
    }

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
        Debug.Assert(_hasSlot == 0);
        Debug.Assert(_count == 0);
    }
}
