----------------------------- MODULE WaiterStore -----------------------------
(* State and operations of WaiterStore<T> (WaiterStore.cs), extracted from Pipeline.tla
   so the store's contract surface has its own module boundary - the same line as the
   code's: Pipeline orchestrates roles (executor/advancer/callbacks), the store owns
   slot/queue storage, the escalation protocol, and the count.

   Pipeline.tla EXTENDS this module, so variable names are shared directly (the same
   pattern as WeakMemory.tla). Operators below model the store's API:

     StoreSlotCommit / StoreQueueEnqueue      TryEscalateOrEnqueue, slot / post-escalation
                                              queue paths. StoreQueueEnqueue is the LEGACY
                                              FUSED fiction (enqueue + increment atomic).
     StoreQueueEnqueueVisible /               The queue commit's REAL two steps (June 2026
       StoreCommitCount                       count-skew find): queue.Enqueue makes the entry
                                              visible/consumable, Interlocked.Increment lands
                                              later. A drain consuming the entry in between
                                              sends the count NEGATIVE (StoreCountNonNegative,
                                              the field NRE at DrainReadyWaiters' D-arm).
                                              Enabled by Pipeline's SplitCountCommit.
     StoreEscalateEnqueueTail                 First escalation's tail append split from its
                                              count increment (same code lines 83-85; the
                                              split escalation routes through StoreCommitCount
                                              with the compensate/idle next-phase).
     StoreEscalatePublish / ClaimSlot /       TryEscalateOrEnqueue's first-escalation steps,
       Move / Enqueue                         split because callbacks interleave with them
                                              (publish queue -> Exchange slot -> move pair ->
                                              enqueue new). escPhase is the method's program
                                              counter, escTail its argument, escSlotClaimed
                                              the Exchange result, escMoved the deferred
                                              moved-pair fields (cleared by TakeMovedSlotPair).
     StoreTakeMoved                           TakeMovedSlotPair: caller copies the moved pair
                                              (read escMoved unprimed) and the fields clear.
     StoreClaimSlotForDrain                   TryClaimSlotForDrain's Exchange-claim + clear.
     StoreDecrementCount                      DecrementCount (count only; claim already
                                              emptied the slot - the in-flight-claim window).
     StoreDequeueHead                         TryDequeue + DecrementCount fused (the queue
                                              drain's head consumption).

   Read-side API (Count, IsEscalated, TryPeek, TryPeekSlotForActivation, TrySnapshotSlot)
   needs no operators: EXTENDS shares the variables, and reads in Pipeline actions reference
   them directly - matching the code, where reads are plain/volatile loads with no protocol.

   Known modeling gaps tracked in Pipeline.tla's "things to add": #7 the slot field writes
   vs the _hasSlot flag are fused here (commit CAS-then-write and claim read-then-clear are
   atomic operators), hiding the instruction-scale torn-pair window. *)

EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
  NumItems

VARIABLES
  \* Slot tier: single inline (item) pair guarded by the CAS-able _hasSlot flag.
  hasSlot,              \* WaiterStore._hasSlot (TRUE = slot occupied).
  slotItem,             \* the Item currently in the slot, or NoItem.
  escalated,            \* WaiterStore._queue is non-null (monotonic).
  \* Queue tier, FIFO. Only used once escalated = TRUE.
  waiters,
  \* Combined slot + queue count (WaiterStore._count).
  storeCount,
  \* First-escalation in-flight state: TryEscalateOrEnqueue's program counter and locals,
  \* visible because callbacks race the steps. NoItem in escTail means "not mid-escalation."
  escPhase,             \* "idle" | "publish_done" | "cas_done" | "move_done" | "compensate"
  escTail,              \* Item or NoItem - the new tail item the executor is escalating
  escSlotClaimed,       \* BOOLEAN - did the executor's CAS-claim of the slot succeed
  escMoved              \* Item or NoItem - the moved pair held in the (deliberately
                        \* uncleared) slot fields until TakeMovedSlotPair; the compensation
                        \* check activates THIS identity, never the current queue head.

slot_vars  == <<hasSlot, slotItem, escalated>>
esc_vars   == <<escPhase, escTail, escSlotClaimed, escMoved>>
store_vars == <<hasSlot, slotItem, escalated, waiters, storeCount,
                escPhase, escTail, escSlotClaimed, escMoved>>

Item == 1..NumItems
NoItem == 0

StoreInit ==
  /\ hasSlot = FALSE
  /\ slotItem = NoItem
  /\ escalated = FALSE
  /\ waiters = <<>>
  /\ storeCount = 0
  /\ escPhase = "idle"
  /\ escTail = NoItem
  /\ escSlotClaimed = FALSE
  /\ escMoved = NoItem

WaiterHead == IF Len(waiters) > 0 THEN Head(waiters) ELSE NoItem

(* ===========================================================================
   Commit paths (TryEscalateOrEnqueue, non-escalating outcomes)
   =========================================================================== *)

\* Slot path: pre-escalation CAS-claim of the empty slot, fields written, count incremented.
StoreSlotCommit(i) ==
  /\ ~escalated
  /\ ~hasSlot
  /\ hasSlot' = TRUE
  /\ slotItem' = i
  /\ storeCount' = storeCount + 1
  /\ UNCHANGED <<escalated, waiters, escPhase, escTail, escSlotClaimed, escMoved>>

\* Post-escalation queue path (loosened CAS: no slot touch - PostEscalationSlotEmpty).
\* LEGACY FUSED fiction: enqueue + increment as one atomic step. The code is two steps;
\* use the Visible/CommitCount pair below for the truthful model (SplitCountCommit).
StoreQueueEnqueue(i) ==
  /\ escalated
  /\ waiters' = Append(waiters, i)
  /\ storeCount' = storeCount + 1
  /\ UNCHANGED <<hasSlot, slotItem, escalated, escPhase, escTail, escSlotClaimed, escMoved>>

\* Truthful queue commit, step 1: queue.Enqueue - the entry is visible and consumable from
\* this state on, while _count still excludes it. escPhase doubles as TryEscalateOrEnqueue's
\* program counter for the steady-state call too (single producer = one in-flight call);
\* ph carries the caller's activation variant to the count step ("q_enq_act"/"q_enq_noact").
StoreQueueEnqueueVisible(i, ph) ==
  /\ escalated
  /\ escPhase = "idle"
  /\ waiters' = Append(waiters, i)
  /\ escTail' = i
  /\ escPhase' = ph
  /\ UNCHANGED <<hasSlot, slotItem, escalated, storeCount, escSlotClaimed, escMoved>>

\* Truthful queue commit, step 2 (shared by the steady-state and split-escalation paths):
\* Interlocked.Increment(_count) - the return value is the caller's wasEmpty partition.
\* nextPhase routes the split escalation to "compensate" (slotWasMoved chain handoff) or
\* "idle"; steady-state callers always pass "idle".
StoreCommitCount(nextPhase) ==
  /\ escPhase \in {"q_enq_act", "q_enq_noact", "esc_enqueued"}
  /\ storeCount' = storeCount + 1
  /\ escPhase' = nextPhase
  /\ escTail' = NoItem
  \* escSlotClaimed (the code's slotWasMoved local) survives to the post-commit nudge check
  \* (escPhase "nudge_check"); the nudge step clears it.
  /\ escSlotClaimed' = (IF nextPhase \in {"compensate", "nudge_check"} THEN escSlotClaimed ELSE FALSE)
  /\ escMoved' = (IF nextPhase = "compensate" THEN escMoved ELSE NoItem)
  /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters>>

(* ===========================================================================
   First escalation (TryEscalateOrEnqueue, slot occupied), four steps
   =========================================================================== *)

\* Step 1: Volatile.Write(_queue) - escalation becomes visible. Slot untouched.
StoreEscalatePublish(i) ==
  /\ escPhase = "idle"
  /\ ~escalated
  /\ hasSlot
  /\ escalated' = TRUE
  /\ escTail' = i
  /\ escPhase' = "publish_done"
  /\ escSlotClaimed' = FALSE
  /\ escMoved' = NoItem
  /\ UNCHANGED <<hasSlot, slotItem, waiters, storeCount>>

\* Step 2: Interlocked.Exchange(_hasSlot, 0); records whether the executor won the claim
\* against a concurrent slot callback. Slot fields stay populated until the move.
StoreEscalateClaimSlot ==
  /\ escPhase = "publish_done"
  /\ escSlotClaimed' = hasSlot
  /\ hasSlot' = FALSE
  /\ escPhase' = "cas_done"
  /\ UNCHANGED <<slotItem, escalated, waiters, storeCount, escTail, escMoved>>

\* Step 3: move the claimed pair to queue head; the moved identity stays readable in
\* escMoved (the code's deliberately deferred field clear, consumed by TakeMovedSlotPair).
StoreEscalateMove ==
  /\ escPhase = "cas_done"
  /\ escPhase' = "move_done"
  /\ escMoved' = IF escSlotClaimed THEN slotItem ELSE NoItem
  /\ IF escSlotClaimed
       THEN /\ waiters' = Append(waiters, slotItem)
            /\ slotItem' = NoItem
       ELSE /\ waiters' = waiters
            /\ slotItem' = slotItem
  /\ UNCHANGED <<hasSlot, escalated, storeCount, escTail, escSlotClaimed>>

\* Step 4: enqueue the new tail, increment count, clear escalation locals. nextPhase is
\* caller-routed: "compensate" when the chain-activation handoff check follows a moved
\* slot (CommitWaiter's slotWasMoved tail), else "idle".
StoreEscalateEnqueue(nextPhase) ==
  /\ escPhase = "move_done"
  /\ waiters' = Append(waiters, escTail)
  /\ storeCount' = storeCount + 1
  /\ escPhase' = nextPhase
  /\ escTail' = NoItem
  /\ escSlotClaimed' = FALSE
  /\ escMoved' = IF nextPhase = "compensate" THEN escMoved ELSE NoItem
  /\ UNCHANGED <<hasSlot, slotItem, escalated>>

\* Truthful split of Step 4's append half: enqueue the new tail, count untouched. The
\* increment lands separately via StoreCommitCount (escSlotClaimed survives until then so
\* the count step can compute the compensate routing). Same count-skew window as the
\* steady-state pair: the tail is consumable from here while still uncounted.
StoreEscalateEnqueueTail ==
  /\ escPhase = "move_done"
  /\ waiters' = Append(waiters, escTail)
  /\ escPhase' = "esc_enqueued"
  /\ UNCHANGED <<hasSlot, slotItem, escalated, storeCount, escTail, escSlotClaimed, escMoved>>

\* TakeMovedSlotPair: the caller copies the moved identity (read escMoved unprimed) and
\* the deferred fields clear; the escalation call site returns to idle.
StoreTakeMoved ==
  /\ escPhase = "compensate"
  \* CommitWaiter's nudge check follows the compensation in code; route through it.
  /\ escPhase' = "nudge_check"
  /\ escMoved' = NoItem
  /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters, storeCount, escTail, escSlotClaimed>>

(* ===========================================================================
   Drain-side operations
   =========================================================================== *)

\* TryClaimSlotForDrain: Exchange-claim wins, fields read+cleared. Count untouched - the
\* claimed-but-uncounted window is the caller's (CountConsistency's in-flight-claim term).
StoreClaimSlotForDrain ==
  /\ hasSlot
  /\ hasSlot' = FALSE
  /\ slotItem' = NoItem
  /\ UNCHANGED <<escalated, waiters, storeCount, escPhase, escTail, escSlotClaimed, escMoved>>

\* DecrementCount: the position republish (the caller captures the returned value as its
\* activation-responsibility partition).
StoreDecrementCount ==
  /\ storeCount' = storeCount - 1
  /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters, escPhase, escTail, escSlotClaimed, escMoved>>

\* Queue drain head consumption: TryDequeue + DecrementCount (the queue drain consumes at
\* the head and decrements after processing; fused here as in the original atomic action).
StoreDequeueHead ==
  /\ Len(waiters) > 0
  /\ waiters' = Tail(waiters)
  /\ storeCount' = storeCount - 1
  /\ UNCHANGED <<hasSlot, slotItem, escalated, escPhase, escTail, escSlotClaimed, escMoved>>

(* ===========================================================================
   Store-internal invariants (cross-module invariants - CountConsistency's drain term,
   the loc-based consistency clauses - live in Pipeline.tla)
   =========================================================================== *)

StoreTypeOK ==
  \* Range admits negatives: under the truthful split commit a drain can consume a
  \* visible-but-uncounted entry and decrement past zero (the June 2026 count-skew bug).
  \* The named witness for that state is StoreCountNonNegative, not TypeOK, so traces
  \* stay attributable.
  /\ storeCount \in -NumItems..NumItems
  /\ hasSlot \in BOOLEAN
  /\ slotItem \in Item \cup {NoItem}
  /\ escalated \in BOOLEAN
  /\ escPhase \in {"idle", "publish_done", "cas_done", "move_done", "esc_enqueued",
                   "q_enq_act", "q_enq_noact", "compensate", "nudge_check"}
  /\ escTail \in Item \cup {NoItem}
  /\ escSlotClaimed \in BOOLEAN
  /\ escMoved \in Item \cup {NoItem}

\* The count never observes a value below zero. EXPECTED FALSE under SplitCountCommit with
\* the shipped drain (the queue drain's DecrementCount on a visible-but-uncounted entry,
\* proven in the field June 2026: Debug.Assert(count >= 0) fired in DrainReadyWaiters under
\* DeferredActivationUnderSustainedLoad_Stress). Must HOLD again once the commit/drain
\* partition is redesigned against the split.
StoreCountNonNegative ==
  storeCount >= 0

\* The single-producer skew bound: the executor has at most ONE commit in flight between
\* its enqueue and its increment, so the count under-runs residency by at most 1 and a
\* drain's decrement bottoms out at -1. This is physics (commit protocol + single producer),
\* not a fix: it holds with or without SkewTolerantPartition, and it is the keystone of the
\* partition repair's soundness argument - count <= 0 at the drain means "no committed
\* positions remain except possibly one already-consumed in-flight entry", which is exactly
\* the C-path case. Checked in Pipeline_Contract.cfg over the full split state space.
BoundedCountSkew ==
  storeCount >= -1

\* THE invariant justifying the loosened CAS in TryEscalateOrEnqueue: outside of
\* mid-escalation, the slot is definitively empty once escalated. No code path refills the
\* slot post-escalation, so subsequent commits skip the slot CAS safely. The escPhase
\* gating admits the first escalation's intermediate phases (queue published, slot still
\* populated between publish and CAS/move) - windows only the escalating executor itself
\* observes from the commit side.
PostEscalationSlotEmpty ==
  (escalated /\ escPhase = "idle") => ~hasSlot

\* Escalation program-counter coherence (store-internal part; the loc[escTail] clauses
\* stay in Pipeline.tla).
StoreEscalationConsistent ==
  /\ (escTail # NoItem) <=> (escPhase \in {"publish_done", "cas_done", "move_done",
                                           "esc_enqueued", "q_enq_act", "q_enq_noact"})
  /\ (escPhase # "idle") => escalated
  /\ (escPhase = "compensate") => (escMoved # NoItem)

=============================================================================
