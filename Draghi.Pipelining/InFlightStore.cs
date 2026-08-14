using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// Stores committed items in an inline slot and permanently adds an SPSC queue on first overlap.
/// The original slot occupant remains the FIFO head after escalation. Commits increment the count
/// before publishing storage, so zero is exact while a positive count may briefly have no visible
/// head. The count word also owns advancement and records a pending advance request.
[StructLayout(LayoutKind.Auto)]
struct InFlightStore<T>
{
    public InFlightStore()
    {
    }

    // Data precedes Occupied publication; claims move Occupied -> Consuming -> Empty.
    const int SlotEmpty = 0;
    const int SlotOccupied = 1;
    const int SlotConsuming = 2;

    T _slotItem = default!;
    ValueTask _slotTask;
    int _slotState;
    SingleProducerSingleConsumerQueue<(T Item, ValueTask PipelineTask)>? _queue;

    // Folding count and advance ownership lets the zero transition release or retain advancement
    // in the same atomic operation.
    long _countWord;
    const long CountMask = 0xFFFF_FFFFL;
    const long AdvanceHeldBit = 1L << 61;
    const long AdvancePendingBit = 1L << 62;
    static int CountOf(long word) => (int)(word & CountMask);
    static bool IsAdvanceHeld(long word) => (word & AdvanceHeldBit) != 0;
    static bool IsAdvancePending(long word) => (word & AdvancePendingBit) != 0;

    /// Zero is exact; a positive value may briefly precede publication of its item.
    public int Count => CountOf(Volatile.Read(ref _countWord));
    public bool HasAdvanceOwner => IsAdvanceHeld(Volatile.Read(ref _countWord));

    /// Once the overflow queue is published, the store remains escalated across reuse.
    public bool IsEscalated => Volatile.Read(ref _queue) is not null;

    /// Counts a commit before its storage publication and returns the preceding count.
    public int IncrementCommitCount()
        => CountOf(Interlocked.Add(ref _countWord, 1)) - 1;

    /// Publishes a previously counted commit. The single producer writes slot data before publishing
    /// Occupied. On first overlap, the existing slot remains head and the new item enters the queue.
    public void PublishCommitted(T item, ValueTask task, out bool isSlot)
    {
        var queue = Volatile.Read(ref _queue);
        if (queue is null)
        {
            // Single producer: publish data, then Occupied.
            if (Volatile.Read(ref _slotState) == SlotEmpty)
            {
                _slotItem = item;
                _slotTask = task;
                Volatile.Write(ref _slotState, SlotOccupied);
                isSlot = true;
                return;
            }
            // Sticky escalation leaves the existing slot head in place.
            queue = new();
            Volatile.Write(ref _queue, queue);
        }
        queue.Enqueue((item, task));
        isSlot = false;
    }

    /// Claims the completed FIFO head and advances its tenure. Advance ownership serializes claims.
    /// A pending completion callback declines the claim without mutation so that callback can drive
    /// advancement itself.
    public bool TryClaimCompletedHead(ref ItemTenure tenure, out T item, out ValueTask task, out long sequence, out bool completionCallbackPending)
    {
        item = default!;
        task = default;
        sequence = 0;
        completionCallbackPending = false;
        // Escalation publishes slot occupancy before the queue. Reading the queue first ensures that
        // observing escalation also orders the following slot-state read.
        var queue = Volatile.Read(ref _queue);
        var slotState = Volatile.Read(ref _slotState);
        if (slotState != SlotEmpty)
        {
            // Check callback ownership only after establishing that this head is complete.
            if (slotState == SlotOccupied && _slotTask.IsCompleted && tenure.IsCompletionCallbackPendingForHead())
            {
                completionCallbackPending = true;
                return false;
            }
            // A non-drainable slot remains head; never fall through to a later queued item.
            return TryClaimCompletedSlot(ref tenure, out item, out task, out sequence);
        }

        // Once the slot is empty, the queue contains the head.
        if (queue is not null && queue.TryPeek(out var entry) && entry.PipelineTask.IsCompleted)
        {
            // As above, decline before mutation when the callback owns delivery.
            if (tenure.IsCompletionCallbackPendingForHead())
            {
                completionCallbackPending = true;
                return false;
            }
            queue.TryDequeue(out _);
            // The head leaves the store at the tenure boundary.
            sequence = tenure.ClaimHead();
            item = entry.Item;
            task = entry.PipelineTask;
            // A null reference claim would strand the advance license, so keep this guard in
            // release builds rather than turning it into a distant timeout.
            if (item is null && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                throw new InvalidOperationException(
                    $"TryClaimCompletedHead(queue leg): claimed a null item. headSequence(post)={tenure.LastClaimedSequence}, " +
                    $"taskCompleted={entry.PipelineTask.IsCompleted}.");
            return true;
        }
        item = default!;
        task = default;
        sequence = 0;
        return false;
    }

    /// Claims a completed slot occupant. Occupied is changed to Consuming before the pair is read and
    /// cleared, preventing a successor publication until Empty is released. Advance ownership ensures
    /// no other caller can consume the task concurrently.
    public bool TryClaimCompletedSlot(ref ItemTenure tenure, out T item, out ValueTask task, out long sequence)
    {
        item = default!;
        task = default;
        sequence = 0;

        if (Volatile.Read(ref _slotState) != SlotOccupied || !_slotTask.IsCompleted)
            return false;

        if (Interlocked.CompareExchange(ref _slotState, SlotConsuming, SlotOccupied) == SlotOccupied)
        {
            item = _slotItem;
            task = _slotTask;
            // Capture and validate before clearing so a corrupt claim fails at its source.
            if (item is null && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                throw new InvalidOperationException(
                    $"TryClaimCompletedSlot: won the Occupied->Consuming CAS but _slotItem was null. " +
                    $"headSequence(pre)={tenure.LastClaimedSequence}, taskCompleted={_slotTask.IsCompleted}.");
            // Advance tenure before Empty permits publication of a successor.
            sequence = tenure.ClaimHead();
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _slotItem = default!;
            _slotTask = default;
            Volatile.Write(ref _slotState, SlotEmpty);
            return true;
        }
        return false;
    }

    /// Decrements the committed count after a claim and reports whether it reached zero.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool DecrementCount()
    {
        var count = CountOf(Interlocked.Add(ref _countWord, -1));
        Debug.Assert(count >= 0, "Count under-ran: a decrement without a preceding increment-first commit.");
        return count == 0;
    }

    // Advancement ownership

    /// Acquires advancement or records a pending request for its current owner. Recording the request
    /// is required at the empty edge: the owner may already have passed its final head check while this
    /// caller is the only remaining driver.
    public bool TryAcquireAdvanceOrRequest()
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            var desired = IsAdvanceHeld(word) ? word | AdvancePendingBit : word | AdvanceHeldBit;
            if (Interlocked.CompareExchange(ref _countWord, desired, word) == word)
            {
                var won = !IsAdvanceHeld(word);
                return won;
            }
        }
    }

    /// Acquires advancement only when free. Commits behind an existing head need not record a pending
    /// request: the current owner will continue through the chain, and doing so would add an atomic
    /// request-and-serve cycle for every pipelined item.
    public bool TryAcquireAdvanceIfFree()
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            if (IsAdvanceHeld(word))
            {
                return false;
            }
            if (Interlocked.CompareExchange(ref _countWord, word | AdvanceHeldBit, word) == word)
            {
                return true;
            }
        }
    }

    /// Releases advancement when no request is pending. A pending request is consumed while retaining
    /// ownership, and the caller must probe again; releasing and reacquiring would create a lost-wake
    /// window.
    public bool ReleaseAdvance()
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            Debug.Assert(IsAdvanceHeld(word), "ReleaseAdvance without the license held.");
            var pending = IsAdvancePending(word);
            var desired = pending ? word & ~AdvancePendingBit : word & ~AdvanceHeldBit;
            if (Interlocked.CompareExchange(ref _countWord, desired, word) == word)
            {
                return pending;
            }
        }
    }

    /// Decrements at the possible zero edge while atomically deciding advancement ownership. With a
    /// pending request, ownership is retained and <paramref name="serve"/> asks the caller to probe
    /// again. Otherwise reaching zero releases ownership in the same CAS.
    public bool DecrementCountAtEdge(out bool serve)
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            Debug.Assert(IsAdvanceHeld(word), "Edge decrement without the license held.");
            var cnt = CountOf(word) - 1;
            long desired;
            if (cnt > 0)
            {
                desired = word - 1; // successors remain: keep the license, plain count step
                serve = false;
            }
            else if (IsAdvancePending(word))
            {
                desired = (word - 1) & ~AdvancePendingBit; // serve: consume the deposit, KEEP the license
                serve = true;
            }
            else
            {
                desired = (word - 1) & ~AdvanceHeldBit; // release folded into the edge decrement
                serve = false;
            }
            if (Interlocked.CompareExchange(ref _countWord, desired, word) == word)
            {
                Debug.Assert(cnt >= 0, "Count under-ran at the edge.");
                return cnt == 0;
            }
        }
    }

    /// Peeks the FIFO head while holding advancement. A false result does not imply zero count: a
    /// commit may have incremented the count without publishing its item yet.
    public bool TryPeekHead(out T item, out ValueTask task)
    {
        // Match the escalation publication order: observing the queue must order the later slot read.
        var queue = Volatile.Read(ref _queue);
        // Occupied publishes the slot fields; the slot remains ahead of every queued item.
        if (Volatile.Read(ref _slotState) == SlotOccupied)
        {
            item = _slotItem;
            task = _slotTask;
            return true;
        }
        if (queue is not null && queue.TryPeek(out var entry))
        {
            item = entry.Item;
            task = entry.PipelineTask;
            return true;
        }
        item = default!;
        task = default;
        return false;
    }

    /// Dequeue the queue head. Returns false (and default entry) when the store has not escalated.
    /// Used only by focused store tests; pipeline advancement claims the unified FIFO head instead.
    public bool TryDequeue(out (T Item, ValueTask PipelineTask) entry)
    {
        var queue = Volatile.Read(ref _queue);
        if (queue is null)
        {
            entry = default;
            return false;
        }
        return queue.TryDequeue(out entry);
    }

    /// Takes a best-effort snapshot of the inline slot.
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

    /// Snapshots the slot and queue for enumeration. Queue must be read first so observing escalation
    /// orders the following slot-state read. The caller presents the slot before queued entries.
    public void SnapshotForEnumeration(out T slotItem, out bool hasSlotItem, out SingleProducerSingleConsumerQueue<(T Item, ValueTask PipelineTask)>? queue)
    {
        queue = Volatile.Read(ref _queue);
        hasSlotItem = Volatile.Read(ref _slotState) == SlotOccupied;
        slotItem = hasSlotItem ? _slotItem : default!;
    }

    /// <summary>Validates that executor termination reached structural quiescence.</summary>
    public void EnsureIdle()
    {
        var word = Volatile.Read(ref _countWord);
        var slotState = Volatile.Read(ref _slotState);
        var queueEmpty = Volatile.Read(ref _queue) is not { } queue || queue.IsEmpty;
        if (word == 0 && slotState == SlotEmpty && queueEmpty)
            return;

        throw new UnreachableException(
            $"Pipeline store remained live at structural quiescence " +
            $"(count={CountOf(word)}, advanceHeld={IsAdvanceHeld(word)}, " +
            $"advancePending={IsAdvancePending(word)}, slotState={slotState}, queueEmpty={queueEmpty}).");
    }

    /// Clears all per-run storage and ownership state. An allocated queue is retained for reuse.
    public void Reset()
    {
        _slotState = SlotEmpty;
        _slotItem = default!;
        _slotTask = default;
        _queue?.Clear();
        // Structural termination owns imbalance diagnostics. Reuse starts a fresh tenure even when
        // the preceding one was condemned, so no storage, count, or advance-license state may carry
        // across it.
        _countWord = 0;
    }
}
