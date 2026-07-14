using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// Storage for committed waiters across the slot and SPSC queue tiers, and the OWNER of the FIFO
/// head identity and the retirement-ordering contract. Pipeline commits and drains through a
/// tier-free head-oriented API (<see cref="TryClaimCompletedHead"/> / <see cref="TryPeekHead"/>) and
/// never reasons about which tier holds the head; the slot-vs-queue routing, the tri-state slot word,
/// and the escalation transition are all private to this type.
///
/// The slot is a single inline (item, task) pair guarded by the tri-state _slotState word. It
/// absorbs the first committed waiter without allocating. When a second waiter arrives while the
/// slot is still occupied, the store escalates: lazily allocates the SPSC queue, publishes it, and
/// enqueues ONLY the new (overflow) waiter. LEAVE-HEAD: the slot occupant STAYS in the slot and
/// retires through the slot tier; escalation never claims or moves it. After escalation the slot is
/// never written again (it may stay occupied by the pre-escalation head until that head drains), and
/// all subsequent commits use the queue. Escalation is binary and sticky.
///
/// THE ORDERING CONTRACT (formally adjudicated GREEN, StoreEscalation.tla, leave-head): while the
/// store is escalated the FIFO head is the slot occupant until it retires; a queue head is the head
/// only once the slot is confirmed empty, RE-DERIVED on every claim (no side-local latch - the store
/// is multi-drainer). Both head-API methods read the slot first, then the queue, and never surface a
/// queue head while the slot is occupied. No caller can violate the contract because no caller can
/// see the tiers.
///
/// The _slotState word (0 empty / 1 occupied / 2 consuming) lets the slot's UnsafeOnCompleted
/// callback and a stale-token reclaim race the drain claim safely (see TryClaimSlotForDrain). The
/// consuming state brackets a drainer's read+clear against a concurrent successor commit (which
/// raises empty->occupied): without it a drain that claimed occupied->empty could have its fields
/// overwritten by a successor's data-then-license publish before it read them. Escalation is not a
/// slot writer under leave-head, so bracketing a successor commit against a drainer's read+clear is
/// the word's sole duty.
///
/// EDGE-LOCK PROTOCOL (2026-07-09 redesign; LockedWalk.tla + EdgeLockBail.tla): the count is
/// INCREMENT-FIRST (it over-promises the store - exact at every zero gate, no bias, no clamp, no -1
/// skew), the turn is ONE owner-identity field (a claim ordinal, or the pre-commit executor
/// sentinel), the exec word is two states with a one-winner CAS shared by the census take and the
/// executor reclaim, and the cnt==0 turn assigners serialize on the edge lock. Claims stay
/// license-serialized; the stop takes no lock (the count partition - TLC-certified); mid-chain
/// commits touch neither lock nor turn. The generation vocabulary (placement gens, pinned claims,
/// unclaim, retire-turn, the ACTIVATING observe-rendezvous and its unbounded spin, the head-tenure
/// attach record and its brackets) is deleted wholesale - each replaced by lock ordering, license
/// ordering, or pre-publication exclusivity.
[StructLayout(LayoutKind.Auto)]
struct WaiterStore<T>
{
    // Tri-state slot word. 0 empty / 1 occupied / 2 consuming. The commit goes data-then-license
    // (fields, THEN publish) so a visible Occupied always means the fields are complete; claims go
    // 1 -> 2 -> 0, and (pre-escalation) the slot is only ever raised 0 -> 1 by the single producer.
    const int SlotEmpty = 0;
    const int SlotOccupied = 1;
    const int SlotConsuming = 2;

    T _slotItem = default!;
    ValueTask _slotTask;
    int _slotState;
    SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>? _queue;

    // DIAGNOSTIC (2026-07-09, hunting the phantom-bail unlicensed-repeek hypothesis): 0 = free,
    // otherwise the managed thread id currently inside a queue-consumer touch (TryPeekHead's or
    // TryClaimCompletedHead's queue leg). The SPSC queue's own contract is single-consumer,
    // peeks included (see SingleProducerSingleConsumerQueue's TryPeek remarks) - two threads ever
    // overlapping here is the corruption instant itself, upstream of the null-claim, the silent
    // stall, and the double-attach crash. Loud and unconditional, matching the null-item guards.
    int _consumerGuardThread;

    void EnterConsumerGuard(string member)
    {
        var me = Environment.CurrentManagedThreadId;
        var prev = Interlocked.CompareExchange(ref _consumerGuardThread, me, 0);
        if (prev != 0 && prev != me)
            throw new InvalidOperationException(
                $"{member}: concurrent consumer-side queue access detected. Held by thread {prev}, " +
                $"entered by thread {me}.");
    }

    void ExitConsumerGuard() => Volatile.Write(ref _consumerGuardThread, 0);
    // The COUNT WORD: the combined slot+queue count fused with the advance-license bits, so the
    // license release can ride the same CAS as the edge decrement (release-and-serve is one
    // atomic op - the deposit can never be left ownerless) and a fire's acquire-or-flag replaces
    // the attach cell's consume as the advance's one-winner arbiter.
    //   low 32 bits: count. INCREMENT-FIRST: the committer counts BEFORE it publishes, so the
    //                count OVER-promises the store - a advancer can observe count>0 with nothing
    //                peekable (the phantom edge, covered by the two-sided bail/arm protocol) but
    //                count==0 is EXACT: no unpublished committed item can exist behind it.
    //   bit 61:      HELD - the advance license (the owner is program position: whoever's acquire
    //                won holds it until its own release)
    //   bit 62:      PEND - the deposited obligation (a fire that lost the acquire; consumed
    //                atomically by the holder's release-and-serve)
    long _countWord;
    const long CntMask = 0xFFFF_FFFFL;
    const long HeldBit = 1L << 61;
    const long PendBit = 1L << 62;
    static int CntOf(long word) => (int)(word & CntMask);
    static bool HeldOf(long word) => (word & HeldBit) != 0;
    static bool PendOf(long word) => (word & PendBit) != 0;

    // The edge lock (see EdgeLock.cs): serializes the cnt==0 turn-assigner family - the prev==0
    // commit's assign+publish, the census take+assign, the executor's pre-commit self-assigns
    // (elision / race-back / exec-recovery re-placement), and the advance-side recovery placement.
    // NOT taken by the stop, mid-chain commits, claims, or fires.
    readonly EdgeLock _edgeLock;

    // THE FIRE-DELIVERY GATE (the retire-vs-dispatch fix). Waiter-task completion is TWO-PHASE:
    // the completer publishes completed, THEN reads the registered (continuation, state) pair and
    // dispatches - and that second phase runs unlicensed on the completer's own thread. A tenure
    // carrying the advance-fire trampoline becomes status-visible (claimable) at phase 1, while
    // the completer may still hold reads INTO the core; a claim's GetResult (which resets the
    // tenure) then tears the pair under the dispatcher (the null-state AOORE in the BCL's known
    // callback). Unregistered tenures are immune: they stay Pending until the completion sentinel
    // lands, i.e. until phase 2 has finished touching the core - so ONLY armed tenures need a gate.
    // Protocol: the frontier owner records the armed tenure BEFORE registering the trampoline
    // (an already-completed task fires inline during registration, and delivery-marking reads the
    // armed word); the trampoline records delivery as its FIRST act, before its acquire-or-deposit
    // (the count-word RMW chain then orders the delivery write before any serve's gate re-read).
    // A claim finding the head armed-and-undelivered declines: the fire is guaranteed in flight
    // (a completer is mid-dispatch by definition), and acquire-or-deposit + release-or-serve keep
    // the decline lossless. ONE WORD, not an armed/delivered pair: delivery CLEARS the arm. Sound
    // by the SINGLE-ARMED INVARIANT - at most one armed-undelivered tenure exists (arming happens
    // only at the frontier, edge commit / stop, and the previously armed tenure can only have
    // been retired through this gate, i.e. after its own delivery cleared the word), so the only
    // fire that can ever deliver is the currently-armed tenure's own. The seq (rather than a
    // bool) keeps the gate self-checking against a cross-tenure fire instead of trusting the
    // invariant blindly. Zero (no valid head seq - HeadSeq starts at 1) is the none-sentinel, so
    // no struct-initializer is needed.
    //
    // WEAK MEMORY - why arm-vs-gate is NOT a Dekker pair (no full fence needed): the gate read is
    // never a bare store-vs-load crossing, because a claimer cannot REACH a claimable head except
    // through one of two synchronization chains that each carry the arm with it.
    //   (1) The edge-owned and stop arms run UNDER THE LICENSE: arm -> registration -> Release()
    //       (count-word RMW, full fence - the arm store cannot stay buffered past it), and the
    //       gate read runs under a later acquire of the same word. Arm < release < acquire <
    //       gate-read: a lock-handoff message chain.
    //   (2) The edge-contended arm runs PRE-PUBLISH: arm (volatile store) precedes the publish
    //       (release store), so publish-visible implies arm-visible; the claimer reads occupancy
    //       (acquire) before the gate. Plain message-passing - and reads-from causality survives
    //       RCpc (ldapr), so this leg does not lean on ldar.
    // The completer carries NO ordering to the armer and needs none: the claimer's obligation is
    // reachability->arm, not completed->arm. The one reachable staleness is the OTHER write - a
    // buffered MarkFireDelivered clear read as still-armed - and its sign is safe: a SPURIOUS
    // DECLINE, resolved by release-or-serve against the fire's clear-then-TryAcquire (RMW) order,
    // costing one deposit round-trip, never a strand. Contrast the deferral race-back (Pipeline's
    // dispatch: PlaceExecDeferred -> Interlocked.MemoryBarrier -> count read): THAT is a true
    // Dekker pair - independent words, store->load on both sides, nothing mediating - which is
    // why the explicit full fence lives there and not here.
    long _armedFireSeq;

    /// Record the tenure about to carry the advance-fire trampoline. MUST precede the
    /// registration itself (an already-completed task delivers INLINE inside UnsafeOnCompleted,
    /// and delivery clears this word); exclusion against claims is the caller's (both callers are
    /// frontier owners: the edge committer under its license/pre-publish invisibility, the stop
    /// under the license).
    public void ArmFire(long seq) => Volatile.Write(ref _armedFireSeq, seq);

    /// The trampoline's first act: the completer's dispatch has reached the fire, so its pair
    /// reads are behind us and the armed tenure is safely claimable. Unconditional clear - under
    /// the single-armed invariant this fire IS the armed tenure's.
    public void MarkFireDelivered() => Volatile.Write(ref _armedFireSeq, 0);

    /// True when the CURRENT head tenure is armed and its fire has not yet been delivered - a
    /// completed head in this state must not be claimed (its completer is mid-dispatch). Callers
    /// hold the advance license (claims are license-serialized), which covers the head-seq read.
    bool IsFireDeliveryPendingForHead() => Volatile.Read(ref _armedFireSeq) == _headTicket + 1;

    /// Explicit construction is REQUIRED (and enforced by callers constructing via new()): a struct's
    /// field initializers do not run on default-initialization, and the edge lock must exist.
    public WaiterStore()
    {
        _edgeLock = new EdgeLock();
    }

    /// The edge lock. Held only across chain-boundary turn assignment + publish windows (never a
    /// policy call, never a task read that could block, never the drain).
    public EdgeLock EdgeLock => _edgeLock;

    /// Total counted waiters. INCREMENT-FIRST makes zero exact (see the count word doc): a zero read
    /// can never hide a committed-but-uncounted item. Nonzero may transiently over-promise.
    public int Count => CntOf(Volatile.Read(ref _countWord));

    /// True once the store has escalated to the queue tier. Stable after first true.
    public bool IsEscalated => Volatile.Read(ref _queue) is not null;

    /// The commit's count increment, taken BEFORE the publish (increment-first). Returns the count
    /// BEFORE this commit: 0 routes the caller onto the edge path (activate + locked assign+publish +
    /// acquire-or-flag arm), anything else onto the mid-chain path (bare publish + plain-read arm).
    /// prev==0 additionally proves this item will be the sole head: the count was exact at zero, so
    /// no unclaimed resident and no credit-held recovery position exists.
    public int IncrementCommitCount()
        => CntOf(Interlocked.Add(ref _countWord, 1)) - 1;

    /// Publish (item, task) to whichever tier the store routes it to: the inline slot when
    /// pre-escalation and the slot is empty (zero alloc), otherwise the SPSC queue (allocating it on
    /// the first overlap). LEAVE-HEAD: the first escalation publishes the queue and enqueues ONLY
    /// this overflow waiter; the slot occupant is left in place and retires through the slot tier.
    /// COUNT-FREE: the caller already counted via <see cref="IncrementCommitCount"/> - on the edge
    /// path this publish runs INSIDE the edge-lock hold (after the turn assign), on the mid-chain
    /// path bare. <paramref name="isSlot"/> tells the caller which tier landed the item.
    ///
    /// Single Volatile.Read(_queue) for the slot-or-queue routing. The slot commit needs no CAS:
    /// the executor is the single producer, the only writer that raises the word from empty, so a
    /// plain empty-check then field writes then a release publish is race-free (claims only go
    /// 1 -> 2 -> 0, none touch the empty state, and escalation never touches the slot).
    public void PublishCommitted(T item, ValueTask task, out bool isSlot)
    {
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
                return;
            }
            // Slot occupied, so escalate. Allocate + publish the queue. LEAVE-HEAD: the occupant
            // stays in the slot (never claimed, never moved) and retires through the slot tier; the
            // queue receives only this overflow waiter. Escalation stays binary+sticky: the slot is
            // never written again after this point, so subsequent commits take the queue-only path.
            queue = new();
            Volatile.Write(ref _queue, queue);
        }
        queue.Enqueue((item, task));
        isSlot = false;
    }

    /// Claim the current FIFO head for retirement, iff its task is completed. THE ordering contract
    /// lives here (store-private, re-derived every call): an occupied slot IS the FIFO head under
    /// leave-head, so the slot is consulted first; a queue head is claimable only once a slot-empty
    /// read has been taken. Returns false (and default out) when the head is not yet completed or the
    /// store is empty - a slot occupant whose task is not yet done is a head that is not drainable,
    /// and the queue is NOT consulted in that case so no later-committed queue head retires ahead of
    /// it. The caller (the license-holding advancer) processes the returned (item, task) uniformly; it
    /// cannot tell nor need tell which tier retired. Claims are license-serialized: exactly one
    /// advancer claims at a time, which is what makes the head's task reads here (IsCompleted, and the
    /// caller's GetResult after) exclusive against every other token consumer.
    /// The queue leg is sound only POST-escalation: once escalated the slot is never written again,
    /// so a slot-empty read is absorbing and the fused empty-check-then-queue-claim cannot race a
    /// commit into the slot. A pre-escalation caller must not reach the queue leg - there a commit
    /// can still occupy the slot after the empty read, and the claim would retire out of FIFO order.
    /// A successful claim advances the head ticket: the claimed tenure's ordinal is
    /// <see cref="LastClaimedSeq"/>, the turn identity its owner releases at retirement.
    public bool TryClaimCompletedHead(out T item, out ValueTask task) => TryClaimCompletedHead(out item, out task, out _, out _);

    /// See the four-out overload; drops the fire-gate flag for callers that only need the claim.
    public bool TryClaimCompletedHead(out T item, out ValueTask task, out long seq) => TryClaimCompletedHead(out item, out task, out seq, out _);

    /// Same claim, plus the claimed tenure's ordinal read out of the SAME operation that advances
    /// _headTicket - not a separate LastClaimedSeq read afterward. A caller that captures the ordinal
    /// as a second, later step reopens a window: if the ticket is bumped again before that read runs,
    /// the caller attributes the wrong ordinal to this claim's item, and a turn release keyed on it
    /// hits the wrong owner. Atomic-with-the-claim closes that window structurally.
    /// <paramref name="fireDeliveryPending"/>: the head WAS completed and claimable but is armed
    /// with an undelivered advance-fire - its completer is mid-dispatch, so the claim declined
    /// (see the fire-delivery gate doc on the fields). The caller must EXIT its advance through
    /// the release tail, never spin: the in-flight fire re-drives via acquire-or-deposit.
    public bool TryClaimCompletedHead(out T item, out ValueTask task, out long seq, out bool fireDeliveryPending)
    {
        item = default!;
        task = default;
        seq = 0;
        fireDeliveryPending = false;
        // Read _queue BEFORE _slotState - the opposite order from the two writes. PublishCommitted's
        // escalation writes _slotState (the pre-escalation resident's occupancy) release-before it
        // writes _queue (the later, separate release that publishes the escalation itself). Two
        // acquire reads on DIFFERENT addresses, even in program order, do not by themselves guarantee
        // "seeing the later write implies seeing the earlier one" - that guarantee (the message-
        // passing pattern) only holds when the READER's order matches read-the-later-signal-first,
        // read-the-earlier-data-second. Reading slot-state first (the prior shape) had no such
        // guarantee: this thread's slot read could race ahead and return a stale Empty from before
        // an escalating commit, while its LATER _queue read still caught that same commit's escalation
        // - falling through to a queue that looks live while still trusting a stale "slot empty",
        // letting a queue claim run ahead of a still-resident, unclaimed slot occupant (the observed
        // out-of-order claim: a later-committed item retiring before an earlier, still-pending one).
        // Reading _queue first means any time this thread observes a non-null queue, it does so via
        // the same acquire that then makes its immediately-following slot-state read consistent with
        // whatever commit produced that queue state.
        var queue = Volatile.Read(ref _queue);
        // An acquire read of the word: Occupied/Consuming means the slot holds the head.
        if (Volatile.Read(ref _slotState) != SlotEmpty)
        {
            // Fire-delivery gate, checked only where the claim would otherwise proceed (occupant
            // present AND completed - an incomplete head returns the ordinary false so the stop
            // path runs). No state is mutated on a gated decline.
            if (Volatile.Read(ref _slotState) == SlotOccupied && _slotTask.IsCompleted && IsFireDeliveryPendingForHead())
            {
                fireDeliveryPending = true;
                return false;
            }
            // Slot is the head. Its own claim protocol decides drainability; on a not-yet-completed
            // occupant this returns false, and we deliberately do NOT fall through to the queue -
            // that would retire a later head out of order.
            return TryClaimSlotForDrain(out item, out task, out seq);
        }

        // Slot empty (confirmed by the acquire read above): the head, if any, is the queue head.
        EnterConsumerGuard(nameof(TryClaimCompletedHead));
        try
        {
            if (queue is not null && queue.TryPeek(out var entry) && entry.WaiterTask.IsCompleted)
            {
                // Fire-delivery gate - same placement rule as the slot leg: after the completed
                // check, before any mutation (the peek is consumer-side but non-consuming).
                if (IsFireDeliveryPendingForHead())
                {
                    fireDeliveryPending = true;
                    return false;
                }
                queue.TryDequeue(out _);
                // The tenure boundary: the head has left the store. Single writer under the advance license.
                var newTicket = _headTicket + 1;
                Volatile.Write(ref _headTicket, newTicket);
                seq = newTicket;
                item = entry.Waiter;
                task = entry.WaiterTask;
                // DIAGNOSTIC (2026-07-09, tracking a silent-null-item claim seen in the wild): a
                // reference-typed claim must never return a null item - it stalls the advance chain
                // silently (a "claim" with nothing to complete, license never released), which is
                // exactly the shape of the observed hang. Loud, immediate, unconditional (not
                // Debug.Assert - the release suite must catch this too) rather than a 30s watchdog
                // timeout with an ambiguous trace gap.
                if (item is null && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                    throw new InvalidOperationException(
                        $"TryClaimCompletedHead(queue leg): claimed a null item. headTicket(post)={_headTicket}, " +
                        $"taskCompleted={entry.WaiterTask.IsCompleted}.");
                return true;
            }
            item = default!;
            task = default;
            seq = 0;
            return false;
        }
        finally
        {
            ExitConsumerGuard();
        }
    }

    /// Try to take ownership of the slot occupant for draining. Returns true if the caller now owns
    /// the (item, task), false if the slot was empty, held a still-running task, or was claimed
    /// elsewhere. Slot-tier primitive: <see cref="TryClaimCompletedHead"/> composes it for the
    /// unified head claim.
    ///
    /// Tri-state protocol: peek the task under the stable Occupied state. A still-running task bails
    /// with NO state change (the head's carrier fires on completion). A completed task claims the
    /// consuming state (CAS 1 -> 2), reads + clears the pair under exclusive ownership (no successor
    /// commit can raise the cell out of the consuming state), then releases (2 -> 0). Under
    /// leave-head escalation does not write the slot, so a CAS-loss arises only from a concurrent
    /// (license-serialized) drainer, never the producer. The IsCompleted read is license-covered:
    /// claims are the only token consumers and they hold the license, so no rival GetResult can
    /// overlap this read (the old ticket bracket and the activation-word rendezvous both guarded
    /// against populations that no longer exist).
    public bool TryClaimSlotForDrain(out T item, out ValueTask task) => TryClaimSlotForDrain(out item, out task, out _);

    /// Same claim, plus the claimed tenure's ordinal - see the matching overload on TryClaimCompletedHead.
    public bool TryClaimSlotForDrain(out T item, out ValueTask task, out long seq)
    {
        item = default!;
        task = default;
        seq = 0;

        if (Volatile.Read(ref _slotState) != SlotOccupied || !_slotTask.IsCompleted)
            return false;

        if (Interlocked.CompareExchange(ref _slotState, SlotConsuming, SlotOccupied) == SlotOccupied)
        {
            item = _slotItem;
            task = _slotTask;
            // DIAGNOSTIC (2026-07-09, tracking a silent-null-item claim seen in the wild): see the
            // matching guard in TryClaimCompletedHead's queue leg. Captured BEFORE the clear below
            // so the exception message reflects the state that actually produced the null, not the
            // post-clear state.
            if (item is null && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                throw new InvalidOperationException(
                    $"TryClaimSlotForDrain: won the Occupied->Consuming CAS but _slotItem was null. " +
                    $"headTicket(pre)={_headTicket}, taskCompleted={_slotTask.IsCompleted}.");
            // Inside the exclusive-ownership window (Consuming), BEFORE the SlotEmpty release makes
            // the successor peek-visible: the tenure boundary advances the ticket. The SlotEmpty
            // release-store publishes it to anyone whose peek surfaces the successor.
            var newTicket = _headTicket + 1;
            Volatile.Write(ref _headTicket, newTicket);
            seq = newTicket;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _slotItem = default!;
            _slotTask = default;
            Volatile.Write(ref _slotState, SlotEmpty);
            return true;
        }
        return false;
    }

    /// Accounts one consumed entry against the combined count. Returns TRUE when this decrement
    /// drained the store to zero. INCREMENT-FIRST keeps the count non-negative structurally: a
    /// decrement only follows a claim of a published item, and every publish was preceded by its
    /// increment.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool DecrementCount()
    {
        var count = CntOf(Interlocked.Add(ref _countWord, -1));
        Debug.Assert(count >= 0, "Count under-ran: a decrement without a preceding increment-first commit.");
        return count == 0;
    }

    // ---- the fold: advance-license primitives on the count word ----

    /// The fire's one-winner arbiter (TryAcquireOrFlagPending): CAS the license bit on, or flag
    /// the pending obligation on the holder. TRUE = the caller is the advancer; FALSE = deposited
    /// (the holder's release-and-serve owns the redrive). Never blocks, never loses silently.
    /// ALSO the EDGE committer's arm: at prev==0 the deposit is load-bearing - the holder can be a
    /// pre-publish fire's advancer mid-census whose exit never re-peeks, and with the carrier already
    /// consumed the deposit is the only remaining driver (LockedWalk v2, wedge-witnessed).
    public bool TryAcquire()
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            var desired = HeldOf(word) ? word | PendBit : word | HeldBit;
            if (Interlocked.CompareExchange(ref _countWord, desired, word) == word)
            {
                var won = !HeldOf(word);
                return won;
            }
        }
    }

    /// The MID-CHAIN committer's arm: acquire the license only when it is FREE, never deposit. A
    /// mid-chain deposit would land once per pipelined item against a continuously-held license (a
    /// CAS plus a forced serve at item rate - the convoying tax through the back door). Sound
    /// without the deposit because every holder-exit that could miss this commit's publish is
    /// covered: the phantom bail re-peeks, a pend serves, and the census is count-gated away
    /// (mid-chain means count >= 2). Returns TRUE iff the caller acquired (and must advance).
    public bool TryAcquireIfFree()
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            if (HeldOf(word))
            {
                return false;
            }
            if (Interlocked.CompareExchange(ref _countWord, word | HeldBit, word) == word)
            {
                return true;
            }
        }
    }

    /// The holder's release, atomically consuming a deposit (ReleaseAndCheckPending): TRUE = a
    /// deposit landed during this hold and the license is KEPT - the caller re-probes (the serve
    /// never lets the license go: release-then-reacquire has a lost-wake window, keep-and-reprobe
    /// does not). FALSE = released clean. Must only be called by the current holder.
    public bool Release()
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            Debug.Assert(HeldOf(word), "Release without the license held.");
            var pending = PendOf(word);
            var desired = pending ? word & ~PendBit : word & ~HeldBit;
            if (Interlocked.CompareExchange(ref _countWord, desired, word) == word)
            {
                return pending;
            }
        }
    }

    /// The edge decrement: the count may reach the idle floor, so the decrement, the license
    /// release, and the serve consume must be ONE CAS (the fold's zero-added-RMW steady path).
    /// Mid-advance decrements (the caller's count view > 1; the view can only over-promise the
    /// PEEKABLE store, never the counted total, and decrements are ours alone) stay on the
    /// wait-free DecrementCount XADD.
    /// Returns TRUE when the count drained to zero (the idle edge); <paramref name="serve"/> reports
    /// a consumed deposit - the caller keeps the license and re-probes after its idle-edge census.
    /// On the no-deposit edge the license is RELEASED HERE and the census runs after a re-acquire.
    public bool DecrementCountAtEdge(out bool serve)
    {
        while (true)
        {
            var word = Volatile.Read(ref _countWord);
            Debug.Assert(HeldOf(word), "Edge decrement without the license held.");
            var cnt = CntOf(word) - 1;
            long desired;
            if (cnt > 0)
            {
                desired = word - 1; // successors remain: keep the license, plain count step
                serve = false;
            }
            else if (PendOf(word))
            {
                desired = (word - 1) & ~PendBit; // serve: consume the deposit, KEEP the license
                serve = true;
            }
            else
            {
                desired = (word - 1) & ~HeldBit; // release folded into the edge decrement
                serve = false;
            }
            if (Interlocked.CompareExchange(ref _countWord, desired, word) == word)
            {
                Debug.Assert(cnt >= 0, "Count under-ran at the edge.");
                return cnt == 0;
            }
        }
    }

    /// License-holder-only peek of the current FIFO head. Tier-free, same slot-first-then-queue
    /// ordering as the claim: returns the slot occupant while present, else the queue head, and
    /// false when nothing is peekable. INCREMENT-FIRST caveat: a false return does NOT mean the
    /// count is zero - a committer between its increment and its publish is a counted invisible
    /// (the phantom edge). The advancer's bail/re-peek and the committer's arm own that window.
    public bool TryPeekHead(out T item, out ValueTask task)
    {
        // Read _queue before _slotState - see the matching comment on TryClaimCompletedHead. Same
        // message-passing ordering requirement: the write order is slot-state-then-queue (a single
        // escalating commit), so the read order must be queue-then-slot-state for "observed the
        // queue" to carry "therefore also observes that commit's slot-state write" - reading slot-
        // state first has no such guarantee and can return a stale Empty from before an escalating
        // commit even while a later queue read catches that same commit.
        var queue = Volatile.Read(ref _queue);
        // Slot first. Occupied (or a self-owned consuming, which never coexists with this call under
        // the single-advancer license) means the slot holds the head. The acquire read orders the
        // field reads after the commit's Occupied publish.
        if (Volatile.Read(ref _slotState) == SlotOccupied)
        {
            item = _slotItem;
            task = _slotTask;
            return true;
        }
        EnterConsumerGuard(nameof(TryPeekHead));
        try
        {
            if (queue is not null && queue.TryPeek(out var entry))
            {
                item = entry.Waiter;
                task = entry.WaiterTask;
                return true;
            }
            item = default!;
            task = default;
            return false;
        }
        finally
        {
            ExitConsumerGuard();
        }
    }

    /// Dequeue the queue head. Returns false (and default entry) when the store has not escalated.
    /// Store-internal / test primitive; Pipeline drains through <see cref="TryClaimCompletedHead"/>.
    public bool TryDequeue(out (T Waiter, ValueTask WaiterTask) entry)
    {
        var queue = Volatile.Read(ref _queue);
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

    /// Snapshot for the public Enumerator: the slot occupant (if any) and the queue reference (if
    /// escalated), read in the ONE order that's safe regardless of a concurrent escalation - queue
    /// first, slot second. PublishCommitted's escalation writes _slotState (the pre-escalation
    /// resident's occupancy) before it writes _queue (the later, separate release publishing the
    /// escalation itself); reading in the matching order means observing a non-null queue here
    /// carries the guarantee that the immediately-following slot read also observes that same
    /// commit's slot-state write (see TryClaimCompletedHead's matching comment - this is the same
    /// hazard, generalized). Combined into one call so no external caller (a diagnostic enumerator,
    /// anything with no other synchronization pairing it with this store) has to know the ordering
    /// matters or can get it backwards - the raw queue reference is deliberately not exposed on its
    /// own for that reason. Under leave-head the slot occupant is the FIFO-earliest head, so the
    /// caller should present the slot snapshot before these queue entries to keep FIFO order.
    public void SnapshotForEnumeration(out T slotItem, out bool hasSlotItem, out SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>? queue)
    {
        queue = Volatile.Read(ref _queue);
        hasSlotItem = Volatile.Read(ref _slotState) == SlotOccupied;
        slotItem = hasSlotItem ? _slotItem : default!;
    }

    // ---- the turn: one owner-identity field ----

    // The head-tenure ticket: counts successful head claims, which ARE the tenure boundaries (every
    // store head leaves via exactly one claim; nothing bumps this mid-tenure). Single writer -
    // claims are single-consumer under the advance license. Under FIFO retirement the ticket is also
    // the ordinal clock: the k-th committed item is claimed by the k-th claim, so the CURRENT
    // head's ordinal is ticket+1 and a just-claimed tenure's ordinal is the post-bump ticket.
    long _headTicket;

    /// The ordinal of the most recently claimed tenure (the post-bump ticket). License-holder read,
    /// taken immediately after a successful claim: the retiring owner's turn identity.
    public long LastClaimedSeq => Volatile.Read(ref _headTicket);

    /// The ordinal of the current (unclaimed) FIFO head. License-holder read (the stop's assign).
    public long HeadSeq => Volatile.Read(ref _headTicket) + 1;

    // THE TURN: which tenure owns the single activation. 0 = none; a POSITIVE value = a resident
    // tenure's claim ordinal (seq >= 1); a NEGATIVE -g (g >= 1) = an EXECUTOR-SIDE GRANT carrying
    // the grant GENERATION g. EVERY executor-side activation (elision, race-back, census,
    // recovery) carries its own -g - there is no shared sentinel. This is the model's distinct
    // per-grant identity (turn = the granted item): a committing item inherits ONLY -(its own gen),
    // so a foreign grant can never be stolen (the iter-83 steal, where item 9 inherited the census's
    // shared TurnExec as its own, is structurally impossible). The generation is minted per grant
    // (NextGrantGen for a self-act with no deferral; PlaceExecDeferred stamps one into the deferral).
    // WRITE DISCIPLINE (every grant is FAIL-IF-LIVE - a live turn declines, so no grant stomps
    //   another): elision / race-back / recovery-inline claim -(their minted gen); the census peeks
    //   the deferral gen, claims -g, then GEN-PINS the consume (a recycled deferral - the stale idle
    //   edge - declines and releases -g). The prev==0 commit converts -(its own gen) -> seq
    //   (inherit) else assigns fresh. cnt>=1 under the LICENSE: the stop / recovery placement (seq).
    // RELEASES are CAS-if-mine (owner identity), so a release can never clobber a rival's assign.
    long _turn;
    long _grantGen; // monotonic grant-identity counter, executor strand.

    // The census-resolution stamp: written by RunIdleCensus (Pipeline.cs) immediately after its
    // ActivateHeadItem(captured, ...) call RETURNS, to the exact gen it granted. A recovery losing
    // TryConsumeExecDeferred to a census (that gen's grant already won cleanly) waits for THIS gen's
    // stamp rather than for the advance license generally: single writer (censuses are
    // license-serialized - never two in flight), monotonic (one executor strand places deferrals off
    // the same _grantGen counter, consumed one at a time in placement order), and precise - the
    // waiter for gen g is never blocked behind an unrelated later census or an unrelated in-flight
    // advance-side recovery sharing the same license hold, both of which a license-keyed wait would
    // be exposed to (checked and found benign in practice, but only via non-local invariants; this
    // stamp is exact by construction and doesn't depend on them). No wake mechanism of its own -
    // callers spin/poll it; the tail being waited for is a single synchronous policy call.
    long _censusResolvedGen;

    /// Called by RunIdleCensus after its ActivateHeadItem(captured, ...) call returns. Single writer
    /// (license-serialized), monotonic - never regresses a later grant's stamp with an earlier one
    /// under normal operation, but callers only ever compare for "at least gen", so ordering between
    /// unrelated gens doesn't matter to correctness.
    public void RecordCensusResolved(long gen) => Volatile.Write(ref _censusResolvedGen, gen);

    /// True once the census that won gen <paramref name="gen"/>'s grant has returned from its
    /// ActivateHeadItem call. A losing TryConsumeExecDeferred(gen) proves exactly one census's
    /// gen-pinned consume of this gen succeeded, so this can only ever become true for a real,
    /// already-in-flight grant - never a hang on a gen nothing claimed.
    public bool IsCensusResolved(long gen) => Volatile.Read(ref _censusResolvedGen) >= gen;

    public const long TurnNone = 0;

    /// Volatile read of the turn word, for the re-checks (the reclaim loser's resolution read, the
    /// stop's live-owner arm). The AUTHORITATIVE re-checks happen under the edge lock; bare reads
    /// are best-effort routing.
    public long Turn => Volatile.Read(ref _turn);

    /// Mint a fresh grant generation. For a self-act with no deferral (inline elision, inline
    /// recovery); PlaceExecDeferred mints from the same counter, so every identity is unique.
    public long NextGrantGen() => Interlocked.Increment(ref _grantGen);

    /// FAIL-IF-LIVE executor-grant claim (under the edge lock): CAS TurnNone -> -gen, declining to a
    /// live turn (a resident seq or another grant). The -gen is item-specific, so a committing item
    /// inherits ONLY its own grant. Used by elision / race-back / recovery-inline (their minted
    /// gen) and the census (the peeked deferral gen). A decline routes the item for a later stop.
    public bool TryClaimTurnGrant(long gen)
    {
        Debug.Assert(gen >= 1);
        return Interlocked.CompareExchange(ref _turn, -gen, TurnNone) == TurnNone;
    }

    /// The recovery re-placement's claim, under the edge lock: inherit the failed predecessor's live
    /// grant (its -inheritGen transfers to the substitute in place - vacuous for the predecessor),
    /// else FAIL-IF-LIVE claim -inheritGen fresh. Returns the gen now held, or 0 if a foreign turn
    /// is live (the caller then defers/routes the substitute).
    public long ClaimOrInherit(long inheritGen)
    {
        Debug.Assert(inheritGen >= 1);
        var turn = Volatile.Read(ref _turn);
        if (turn == -inheritGen)
            return inheritGen;
        return Interlocked.CompareExchange(ref _turn, -inheritGen, TurnNone) == TurnNone ? inheritGen : 0;
    }

    /// The edge commit's turn step, under the lock hold: convert THIS item's OWN grant (-myGen) to
    /// its resident ordinal (INHERIT), or assign fresh when no grant is held (myGen = 0 for a plain
    /// item that was never granted). A turn holding a FOREIGN grant (-otherGen) is never inherited
    /// (the steal fix). Returns TRUE when inherited (the grant already activated it - skip the policy).
    public bool AssignTurnAtCommit(long myGen, long seq)
    {
        var turn = Volatile.Read(ref _turn);
        if (myGen != 0 && turn == -myGen)
        {
            Volatile.Write(ref _turn, seq);
            return true;
        }
        Debug.Assert(turn == TurnNone, "Edge commit over a foreign turn.");
        Volatile.Write(ref _turn, seq);
        return false;
    }

    /// The stop's turn assign (license-held, LOCK-FREE - the count partition: every rival assigner
    /// is in the cnt==0 family and a stop implies cnt>=1). Plain volatile write; the caller checked
    /// Turn == TurnNone under the same license that excludes every cnt>=1 rival.
    public void AssignTurnAtStop(long seq)
    {
        Debug.Assert(Volatile.Read(ref _turn) == TurnNone, "Stop assign over a live turn.");
        Volatile.Write(ref _turn, seq);
    }

    /// The advance-side recovery placement's assign, under the edge lock (the credit-held window):
    /// inherit when the faulted head already held the turn (its ordinal), else assign.
    public void AssignTurnAtRecovery(long seq)
    {
        var turn = Volatile.Read(ref _turn);
        Debug.Assert(turn == TurnNone || turn == seq, "Recovery placement over a foreign turn.");
        Volatile.Write(ref _turn, seq);
    }

    /// Release the turn iff this owner holds it. CAS-if-mine: a stale release (the owner was
    /// already released, or a successor assigned) declines silently rather than clobbering.
    /// Returns true iff this call released a live turn.
    public bool ReleaseTurn(long owner)
    {
        Debug.Assert(owner != TurnNone);
        return Interlocked.CompareExchange(ref _turn, TurnNone, owner) == owner;
    }

    // ---- the exec word: VERSIONED two-state, no rendezvous ----

    // The deferred-publish carrier: the executor dispatched its current item behind waiters
    // (count>0 at dispatch) and deferred the activation decision. Word = (gen << 1) | state, state
    // in bit 0 (0 none / 1 deferred). The generation is the DEFERRAL-IDENTITY PIN, and its ONLY job
    // is to let the CENSUS decline a recycled deferral - it does NOT reintroduce the activation
    // word's mint/claim/unclaim/edge-pin family, because those existed to force a MUST-SUCCEED claim
    // through benign mints, and the census MUST-DECLINE-instead: it is a backup grant, the
    // executor's own reclaim is the primary, so a version mismatch is just "the executor already
    // handled this deferral" -> decline, no retry.
    //
    // The one-winner consume (deferred -> none) is shared by the census (version-pinned) and the
    // executor reclaim (plain, its own current deferral); the loser learns the resolution from its
    // loss and the TURN says who was granted. No ACTIVATING state, no observe-rendezvous, no spin.
    // The deleted rendezvous carried (a) the census's grant-item exclusion - now carried by the
    // dedicated _execDeferredItem field (written before the release-place, read under the version
    // pin, so the census grants EXACTLY the consumed deferral's item; capture-before-take was
    // unsound - it could read a stale item while consuming a recycled deferral, the
    // activation-after-retirement collision), and (b) activate-before-complete for early-completed
    // lost items - carried by ROUTE-LOST-THROUGH-THE-STORE (a reclaim loser never inline-completes;
    // the license orders the census's activation before any advance claim can retire the item).
    long _execWord;
    T _execDeferredItem = default!;
    const long ExecStateMask = 1;
    const long ExecNone = 0;
    const long ExecDeferred = 1;
    static long ExecStateOf(long w) => w & ExecStateMask;
    static long ExecGenOf(long w) => w >> 1;

    /// Publish the deferral with its item, returning the grant GENERATION the caller carries to its
    /// commit (so AssignTurnAtCommit can inherit its own grant) and the census stamps into
    /// the turn. Executor strand, SINGLE WRITER. The item is written BEFORE the release-store of the
    /// word, so a reader that acquires a Deferred word sees the matching item; the fresh generation
    /// is the recycle pin (a suspended census that peeked a prior placement declines its gen-pinned
    /// consume) AND the grant identity.
    public long PlaceExecDeferred(T item)
    {
        Debug.Assert(ExecStateOf(Volatile.Read(ref _execWord)) == ExecNone, "Deferral published over an unresolved deferral.");
        _execDeferredItem = item;
        var gen = Interlocked.Increment(ref _grantGen);   // same counter as NextGrantGen: unique
        Volatile.Write(ref _execWord, (gen << 1) | ExecDeferred);
        return gen;
    }

    /// True while a deferral is visible. Entry filter for the census (cheap decline path).
    public bool ExecDeferredVisible => ExecStateOf(Volatile.Read(ref _execWord)) == ExecDeferred;

    /// Clears a stale (already-resolved) deferred-item reference so a suspending executor doesn't
    /// keep it GC-rooted across an idle period - neither consumer (TryConsumeExecDeferred, the
    /// census's TryConsumeExecDeferredGen) clears it on its own, since PublishExecDeferred normally
    /// overwrites it on the next placement, and clearing eagerly on every consume would race a
    /// cross-thread placement that could land between the consume's CAS and the clear. SAFE ONLY
    /// when called by the executor strand about to suspend: it is the sole writer of the deferred-
    /// item field (via PlaceExecDeferred) and the sole thing that can ever transition the word back
    /// to Deferred, so if it observes ExecNone here, that state cannot change until it changes it
    /// itself later (no concurrent census can be mid-grant of a deferral that does not exist yet).
    public void ClearStaleExecDeferredItem()
    {
        if (!ExecDeferredVisible && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _execDeferredItem = default!;
    }

    /// The executor's own reclaim: consume the CURRENT deferral (whatever generation). Plain - the
    /// executor is the single placer, so the current deferral is always its own most-recent one.
    /// TRUE = the caller consumed it; FALSE = the census took it first. Used at COMPLETION-TIME
    /// reclaim sites (ClearExecutingItem, CommitTailWaiter, the recovery swap/reclaims) where a
    /// FOREIGN RESIDENT (an unrelated item's positive turn seq) may legitimately be live - a
    /// fail-if-live TURN claim there would wrongly read "occupied" and treat an unrelated resident
    /// as if it were this item's own census grant. See TryReclaimExecDeferred for the CLAIM-FIRST
    /// variant used at the DISPATCH-TIME sites, which are structurally gated on Count==0 (no
    /// resident can be live there) and so can safely arbitrate via the turn CAS.
    /// VERIFIED (2026-07-09): this plain consume can race a census's claim-then-consume in the
    /// OPPOSITE order and "win" the raw exec-word CAS out from under a census that already claimed
    /// the turn (the double-bail class) - but this is benign, not a lost obligation. Both sides then
    /// attempt ReleaseTurn on the same -gen: CAS-if-mine means exactly one succeeds and the other
    /// silently declines (no double-release, no stuck turn). And "this item completes without ever
    /// being activated" is not a violated invariant here - every call site (ClearExecutingItem's two
    /// use sites, CommitTailWaiter) either drain-completes the item inline through the documented-
    /// legal completion-without-activation path when the reclaim wins, or explicitly routes the item
    /// through the normal store commit (CommitWaiter, sentinelHeld/own = false) when it loses - no
    /// branch drops it. Activation itself remains AT MOST once (0 or 1, never 2): the invariant this
    /// whole exec-word/turn protocol exists to hold is never-twice, not always-once.
    public bool TryConsumeExecDeferred()
    {
        while (true)
        {
            var w = Volatile.Read(ref _execWord);
            if (ExecStateOf(w) != ExecDeferred)
                return false;
            if (Interlocked.CompareExchange(ref _execWord, (ExecGenOf(w) << 1) | ExecNone, w) == w)
                return true;
        }
    }

    /// The executor's DISPATCH-TIME reclaim, CLAIM-FIRST (matches the census's protocol exactly:
    /// claim -gen fail-if-live, THEN gen-pinned consume). SAFE ONLY where the caller has already
    /// gated on Count==0 (elision / race-back): a resident cannot exist there (residents require a
    /// counted commit), so the turn word can only hold None or a foreign census's grant of THIS
    /// exact gen - never an unrelated resident's positive seq - so a fail-if-live claim correctly
    /// arbitrates. Do NOT call this without that Count==0 pre-gate (see TryConsumeExecDeferred for
    /// the plain variant used where residents may legitimately be live). This is the fix for the
    /// double-bail: because gen is unique per placement and both sides now claim before consuming,
    /// only the claim's winner ever attempts the exec-word consume, so it is then uncontested.
    public bool TryReclaimExecDeferred(long gen)
    {
        Debug.Assert(gen >= 1);
        if (!TryClaimTurnGrant(gen))
            return false;
        if (TryConsumeExecDeferredGen(gen))
            return true;
        // Defensive only - unreachable under the Count==0-gated claim-first protocol (nobody else
        // could be attempting THIS gen's consume once we hold its claim).
        ReleaseTurn(-gen);
        return false;
    }

    /// The census's PEEK of the current deferral (under the edge lock): reports the grant gen and
    /// item WITHOUT consuming, so the census can FAIL-IF-LIVE claim the turn (-gen) before it
    /// commits to the consume. The item is read under the peeked word; the subsequent gen-pinned
    /// consume validates it.
    public bool TryPeekExecDeferred(out long gen, out T item)
    {
        var w = Volatile.Read(ref _execWord);
        if (ExecStateOf(w) != ExecDeferred)
        {
            gen = 0;
            item = default!;
            return false;
        }
        gen = ExecGenOf(w);
        item = _execDeferredItem;
        return true;
    }

    /// The census's GEN-PINNED consume (under the edge lock): consume ONLY the exact <paramref
    /// name="gen"/> the census peeked and claimed the turn for. Succeeds iff nothing recycled the
    /// deferral since the peek - so the grant is bound to the identity the turn now holds (-gen).
    /// A mismatch (the executor resolved this deferral and placed a fresh one - the stale idle edge)
    /// DECLINES; the caller releases its -gen turn claim and the executor's own reclaim covers the
    /// deferral. No retry, no mint beyond the pin.
    public bool TryConsumeExecDeferredGen(long gen)
    {
        var w = Volatile.Read(ref _execWord);
        if (ExecStateOf(w) != ExecDeferred || ExecGenOf(w) != gen)
            return false;
        return Interlocked.CompareExchange(ref _execWord, (gen << 1) | ExecNone, w) == w;
    }

    /// Defaults the per-run reference fields (slot contents) so a cached-but-idle Pipeline shell
    /// doesn't pin them across an idle period. The SPSC queue, if allocated, is structurally
    /// reusable across runs (always empty at completion) and is intentionally kept.
    public void Reset()
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
        {
            _slotItem = default!;
            _execDeferredItem = default!;
        }
        _slotTask = default;
        Debug.Assert(_slotState == SlotEmpty);
        Debug.Assert(CntOf(Volatile.Read(ref _countWord)) == 0);
        Debug.Assert(Volatile.Read(ref _turn) == TurnNone);
        Debug.Assert(ExecStateOf(Volatile.Read(ref _execWord)) == ExecNone);
    }

    /// Diagnostic readout of the turn + exec words, for test forensics. Plain volatile reads,
    /// callable from any thread, no side effects.
    internal string DebugWordStates()
    {
        var turn = Volatile.Read(ref _turn);
        var exec = Volatile.Read(ref _execWord);
        return $"turn={(turn < 0 ? $"grant(g{-turn})" : turn.ToString())},exec={(ExecStateOf(exec) == ExecDeferred ? $"deferred(g{ExecGenOf(exec)})" : "none")},ticket={Volatile.Read(ref _headTicket)}";
    }
}
