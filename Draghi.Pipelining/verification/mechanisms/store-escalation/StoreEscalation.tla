-------------------------- MODULE StoreEscalation --------------------------
(* Slot-to-queue escalation model. The existing slot head remains in place and
   only overflow is published into the queue. After escalation, a
   retirement pass must resolve the still-resident slot head before the
   overflow queue head. Escalation publication, slot claim and queue enqueue
   remain separate actions so their contention is explicit. The model checks
   FIFO and exactly-once retirement, activation tenure and eventual draining. *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANT N

Items  == 1..N
NoItem == 0

VARIABLES
  \* -- store surface (WaiterStore fields) --
  slotState,   \* "empty" | "occupied" | "consuming"  (WaiterStore._slotState)
  slotItem,    \* the item in the slot, or NoItem       (WaiterStore._slotItem)
  queue,       \* FIFO of items                          (the SPSC queue)
  escalated,   \* _queue != null (monotonic)             (WaiterStore.IsEscalated)
  \* -- first-escalation state --
  escalationPhase,    \* "idle" | "published"
  escalationTail,     \* the overflow tail item being escalated, or NoItem
  \* -- item lifecycle --
  nextCommit,  \* next item to commit (1..N+1)
  taskDone,    \* [Items -> BOOLEAN]  the waiter task has completed
  retired,     \* [Items -> BOOLEAN]  ghost: _policy.CompleteItem ran (retirement)
  retireCount, \* [Items -> Nat]      ghost: retirement fire count (exactly-once)
  activated    \* [Items -> Nat]      ghost: _policy.ActivateHeadItem fire count

vars == <<slotState, slotItem, queue, escalated, escalationPhase, escalationTail,
          nextCommit, taskDone,
          retired, retireCount, activated>>

\* The FIFO logical head of the whole store: the slot occupant if present
\* (leave-head keeps the head here through the transient), else the queue head.
LogicalHead ==
  IF slotState \in {"occupied", "consuming"} THEN slotItem
  ELSE IF Len(queue) > 0 THEN Head(queue) ELSE NoItem

TypeOK ==
  /\ slotState \in {"empty", "occupied", "consuming"}
  /\ slotItem \in Items \cup {NoItem}
  /\ queue \in Seq(Items)
  /\ escalated \in BOOLEAN
  /\ escalationPhase \in {"idle", "published"}
  /\ escalationTail \in Items \cup {NoItem}
  /\ nextCommit \in 1..(N + 1)
  /\ taskDone \in [Items -> BOOLEAN]
  /\ retired \in [Items -> BOOLEAN]
  /\ retireCount \in [Items -> 0..3]
  /\ activated \in [Items -> 0..3]

Init ==
  /\ slotState = "empty"
  /\ slotItem = NoItem
  /\ queue = <<>>
  /\ escalated = FALSE
  /\ escalationPhase = "idle"
  /\ escalationTail = NoItem
  /\ nextCommit = 1
  /\ taskDone = [i \in Items |-> FALSE]
  /\ retired = [i \in Items |-> FALSE]
  /\ retireCount = [i \in Items |-> 0]
  /\ activated = [i \in Items |-> 0]

(* ===========================================================================
   Producer (executor, single thread) - CommitWaiter / TryEscalateOrEnqueue
   =========================================================================== *)

\* Pre-escalation zero-alloc slot commit. Arm A (the wasEmpty self-activate) lives
\* HERE, at the slot tier, as a SYNCHRONOUS window: the sole committed head may
\* self-activate in the same atomic step as the commit. This is arm A's whole
\* correctness argument - capture and use never straddle a retirement, so it can
\* never activate a retiree. selfAct is the nondeterministic was-empty
\* self-activation branch.
PublisherCommitSlot(selfAct) ==
  /\ escalationPhase = "idle"
  /\ ~escalated
  /\ slotState = "empty"
  /\ nextCommit <= N
  /\ slotState' = "occupied"
  /\ slotItem'  = nextCommit
  /\ nextCommit' = nextCommit + 1
  /\ activated' = IF selfAct THEN [activated EXCEPT ![nextCommit] = @ + 1] ELSE activated
  /\ UNCHANGED <<queue, escalated, escalationPhase, escalationTail, taskDone, retired, retireCount>>

\* Post-escalation steady-state queue commit (no slot touch).
PublisherCommitOverflow ==
  /\ escalationPhase = "idle"
  /\ escalated
  /\ nextCommit <= N
  /\ queue' = Append(queue, nextCommit)
  /\ nextCommit' = nextCommit + 1
  /\ UNCHANGED <<slotState, slotItem, escalated, escalationPhase, escalationTail, taskDone, retired,
                 retireCount, activated>>

\* First escalation, step 1: Volatile.Write(_queue) publishes the queue. The slot
\* is UNTOUCHED and still holds the head - the transient a reader must respect.
BeginEscalation ==
  /\ escalationPhase = "idle"
  /\ ~escalated
  /\ slotState \in {"occupied", "consuming"}
  /\ nextCommit <= N
  /\ escalated' = TRUE
  /\ escalationPhase'  = "published"
  /\ escalationTail'   = nextCommit
  /\ nextCommit' = nextCommit + 1
  /\ UNCHANGED <<slotState, slotItem, queue, taskDone, retired, retireCount, activated>>

\* LEAVE-HEAD escalation, step 2: enqueue ONLY the overflow tail. The slot is never
\* touched; the head retires through the slot tier.
PublishLeaveHeadOverflow ==
  /\ escalationPhase = "published"
  /\ queue' = Append(queue, escalationTail)
  /\ escalationPhase' = "idle"
  /\ escalationTail'  = NoItem
  /\ UNCHANGED <<slotState, slotItem, escalated, nextCommit, taskDone, retired, retireCount, activated>>

(* ===========================================================================
   Item task completion (async, arbitrary thread)
   =========================================================================== *)
PublishTaskCompletion(i) ==
  /\ i < nextCommit
  /\ ~taskDone[i]
  /\ taskDone' = [taskDone EXCEPT ![i] = TRUE]
  /\ UNCHANGED <<slotState, slotItem, queue, escalated, escalationPhase, escalationTail, nextCommit, retired,
                 retireCount, activated>>

(* ===========================================================================
   Drains.  Two tiers.  All retire the head they physically hold; FIFO is a
   CHECKED property, never a guard.
   =========================================================================== *)

\* Pre-escalation slot drain (DrainSlotInline via TryClaimSlotForDrain), modeled
\* as a two-step CAS.
\* Step 1: CAS occupied -> consuming.
ClaimSlotHead ==
  /\ ~escalated
  /\ slotState = "occupied"
  /\ taskDone[slotItem]
  /\ slotState' = "consuming"
  /\ UNCHANGED <<slotItem, queue, escalated, escalationPhase, escalationTail, nextCommit, taskDone, retired,
                 retireCount, activated>>

\* Step 2: read+clear+retire under the consuming state, release to empty. Not gated
\* on ~escalated: once "consuming", a racing escalation CAS already lost, so the
\* drain completes and retires the head from the slot (FIFO held without a move).
ReleaseSlotClaim ==
  /\ slotState = "consuming"
  /\ retired'     = [retired     EXCEPT ![slotItem] = TRUE]
  /\ retireCount' = [retireCount EXCEPT ![slotItem] = @ + 1]
  /\ slotState' = "empty"
  /\ slotItem'  = NoItem
  /\ UNCHANGED <<queue, escalated, escalationPhase, escalationTail, nextCommit, taskDone, activated>>

\* Escalated drain (DrainReadyWaiters).  Under LEAVE-HEAD it must retire the slot-
\* resident head before the queue - the callback dispatched here by IsEscalated=TRUE,
\* and post-escalation this is the ONLY drain path (DrainSlotInline is entered only
\* when !IsEscalated). Escalation never touches the slot, so a plain claim is faithful.
DrainSlotHead ==
  /\ escalated
  /\ slotState = "occupied"
  /\ taskDone[slotItem]
  /\ retired'     = [retired     EXCEPT ![slotItem] = TRUE]
  /\ retireCount' = [retireCount EXCEPT ![slotItem] = @ + 1]
  /\ slotState' = "empty"
  /\ slotItem'  = NoItem
  /\ UNCHANGED <<queue, escalated, escalationPhase, escalationTail, nextCommit, taskDone, activated>>

\* Escalated queue drain: the resident slot head must be confirmed gone first.
DrainQueueHead ==
  /\ escalated
  /\ Len(queue) > 0
  /\ taskDone[Head(queue)]
  \* Advancer-latch cross-path exclusion (modeled minimally): the escalated drain
  \* cannot run while a pre-escalation slot-drain holds the latch mid-claim
  \* (slotState="consuming").  Without this the model admits a slot-drain / queue-
  \* drain OVERLAP the real single-advancer latch forbids - a spurious FIFO break
  \* (the two drain PASSES never run concurrently in code).
  /\ slotState # "consuming"
  \* THE ORDERING DISCIPLINE (intra-pass, latch-independent): a SINGLE DRW pass must
  \* retire the slot-resident head before the queue head.  Dropping it is the weakening.
  /\ slotState = "empty"
  /\ retired'     = [retired     EXCEPT ![Head(queue)] = TRUE]
  /\ retireCount' = [retireCount EXCEPT ![Head(queue)] = @ + 1]
  /\ queue' = Tail(queue)
  /\ UNCHANGED <<slotState, slotItem, escalated, escalationPhase, escalationTail, nextCommit, taskDone,
                 activated>>

(* ===========================================================================
   The SAFE successor/head activation arm (the D-path).  Activates the current
   logical head exactly once, and NEVER a retired item (guarded ~retired).  This
   It reads the live store head rather than a captured stale identity.
   =========================================================================== *)
ActivateIncompleteHead ==
  /\ LogicalHead # NoItem
  /\ ~retired[LogicalHead]
  /\ ~taskDone[LogicalHead]         \* a completed head is drain-only, not activated
  /\ activated[LogicalHead] = 0
  /\ activated' = [activated EXCEPT ![LogicalHead] = @ + 1]
  /\ UNCHANGED <<slotState, slotItem, queue, escalated, escalationPhase, escalationTail, nextCommit, taskDone,
                 retired, retireCount>>

(* =========================================================================== *)

PublisherStep ==
  \/ (\E b \in BOOLEAN : PublisherCommitSlot(b))
  \/ PublisherCommitOverflow
  \/ BeginEscalation
EscalationStep ==
  PublishLeaveHeadOverflow
RetirementStep ==
  \/ ClaimSlotHead \/ ReleaseSlotClaim \/ DrainSlotHead \/ DrainQueueHead
CompletionStep ==
  \E i \in Items : PublishTaskCompletion(i)

Next ==
  \/ PublisherStep
  \/ EscalationStep
  \/ CompletionStep
  \/ RetirementStep
  \/ ActivateIncompleteHead

Fairness ==
  /\ WF_vars(PublisherStep)
  /\ WF_vars(EscalationStep)
  /\ WF_vars(CompletionStep)
  /\ WF_vars(RetirementStep)

Spec == Init /\ [][Next]_vars /\ Fairness

(* ===========================================================================
   Properties
   =========================================================================== *)

\* No activation lands after the item retired.
RetiredItemNeverActivated ==
  [][ \A i \in Items : (activated'[i] > activated[i]) => ~retired[i] ]_vars

\* At most one activation per item tenure (no double-baton).
AtMostOneActivationPerTenure ==
  \A i \in Items : activated[i] <= 1

\* Together with EveryCommittedItemEventuallyRetires, this establishes
\* exactly-once retirement.
ExactlyOnceRetirement ==
  \A i \in Items : retireCount[i] <= 1

\* FIFO retirement: a retired item implies every earlier-committed item retired.
\* FIFO retirement: the slot head retires before overflow.
ItemsRetireInFifoOrder ==
  \A i \in Items : retired[i] => (\A j \in 1..(i - 1) : retired[j])

\* Structural head-consistency: while the slot holds a head and the queue is non-
\* empty, the slot item is the earlier (smaller = earlier-committed) one - the slot
\* is the true FIFO head through the leave-head transient.  A sanity invariant.
SlotItemPrecedesOverflowQueue ==
  (slotState \in {"occupied", "consuming"} /\ Len(queue) > 0)
    => (\A q \in DOMAIN queue : slotItem < queue[q])

\* Non-vacuity probe: activations DO occur (arm A synchronous + the D-path).  EXPECT
\* RED under the main leave-head config - proves RetiredItemNeverActivated holds
\* meaningfully (real activations exist and every one is retirement-safe), not because
\* the pipeline never activates anything.
ActivationIsUnreachable ==
  \A i \in Items : activated[i] = 0

\* Liveness: every committed item is eventually retired.
EveryCommittedItemEventuallyRetires ==
  <>[] (nextCommit = N + 1 /\ (\A i \in Items : retired[i]))

=============================================================================
