----------------------------- MODULE ItemTenure -----------------------------
(* Per-item lifetime contract.

   Completion has two phases: publish the completionPublished state, then read the task
   pair and completionDispatch the callbackRegistered callback. The delivery arm is installed
   before callback registration, cleared as callback delivery begins, and read
   again immediately before a completionPublished head may be completionConsumed.

   Consumption performs the tenure reset. It must not overlap the callback's
   pair reads; otherwise the callback can observe state from a later tenure.
   The weak-clear mode permits a stale delivery-arm observation only as a
   spurious claim decline, never as permission to consume too early.

   Storage tiers belong to InFlightStore. Activation turns, the edge lock and
   empty-edge handoff belong to ActivationGate. This model represents storage as
   one abstract FIFO and relies on serialized claim attempts.                  *)
EXTENDS Naturals, TLC, WeakMemory

CONSTANT N

ASSUME N \in Nat /\ N >= 1

VARIABLES
  completionPublished,     \* [1..N -> BOOLEAN]  phase 1 published (status-visible)
  callbackRegistered,    \* [1..N -> BOOLEAN]  trampoline callbackRegistered (at most once)
  completionDispatch,      \* [1..N -> {"none","inflight","done","acquired"}]
  completionConsumed,      \* [1..N -> BOOLEAN]  GetResult+reset ran
  completionDispatchTorn,          \* [1..N -> BOOLEAN]  consume raced phase 2 (the AOORE fact)
  localDeliveryArmSequence, visibleDeliveryArmSequence,   \* the arm word, writer-local vs globally visible
  headTicket,    \* last retired ticket; head tenure = headTicket+1
  advanceLicenseState, advancePending,  \* advance license: "free"/"held", pending redrive
  retirementPassPc            \* advancer pc: "idle" | "driving" | "arming"

vars == <<completionPublished, callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn,
          localDeliveryArmSequence, visibleDeliveryArmSequence, headTicket, advanceLicenseState, advancePending, retirementPassPc>>

HeadSequence == headTicket + 1
AllItemsRetired == headTicket = N
HeadDeliveryPending == visibleDeliveryArmSequence = HeadSequence

Init ==
  /\ completionPublished  = [i \in 1..N |-> FALSE]
  /\ callbackRegistered = [i \in 1..N |-> FALSE]
  /\ completionDispatch   = [i \in 1..N |-> "none"]
  /\ completionConsumed   = [i \in 1..N |-> FALSE]
  /\ completionDispatchTorn       = [i \in 1..N |-> FALSE]
  /\ localDeliveryArmSequence = 0 /\ visibleDeliveryArmSequence = 0
  /\ headTicket = 0
  /\ advanceLicenseState = "free" /\ advancePending = FALSE
  /\ retirementPassPc = "idle"

(* --------------------------- completer side ------------------------------- *)
\* Phase 1: publish completionPublished. Any in-flight tenure, any order (ROB-style).
PublishCompletion(i) ==
  /\ ~completionPublished[i] /\ i > headTicket
  /\ completionPublished' = [completionPublished EXCEPT ![i] = TRUE]
  /\ UNCHANGED <<callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn, localDeliveryArmSequence, visibleDeliveryArmSequence,
                 headTicket, advanceLicenseState, advancePending, retirementPassPc>>

\* Phase 2 entry: completer of a callbackRegistered tenure begins the unlicensed pair-read.
\* Reads INTO the core are live from here. Unregistered tenures never completionDispatch.
BeginCompletionDispatch(i) ==
  /\ completionPublished[i] /\ callbackRegistered[i] /\ completionDispatch[i] = "none" /\ ~completionConsumed[i]
  /\ completionDispatch' = [completionDispatch EXCEPT ![i] = "inflight"]
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionConsumed, completionDispatchTorn, localDeliveryArmSequence, visibleDeliveryArmSequence,
                 headTicket, advanceLicenseState, advancePending, retirementPassPc>>

\* The trampoline runs: delivery-mark first. The local clear is weak and the
\* visible value may lag until PropagateDeliveryArmClear or the RMW below.
DeliverCompletionCallback(i) ==
  /\ completionDispatch[i] = "inflight"
  /\ localDeliveryArmSequence' = 0
  /\ UNCHANGED visibleDeliveryArmSequence
  /\ completionDispatch' = [completionDispatch EXCEPT ![i] = "done"]
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionConsumed, completionDispatchTorn, headTicket, advanceLicenseState, advancePending, retirementPassPc>>

\* Acquire-or-advancePending: the count-word RMW — a full fence, so the clear propagates
\* here at the latest. Winning takes the license and drives; losing deposits.
CompletionCallbackAcquireOrDeposit(i) ==
  /\ completionDispatch[i] = "done"
  /\ visibleDeliveryArmSequence' = localDeliveryArmSequence
  /\ completionDispatch' = [completionDispatch EXCEPT ![i] = "acquired"]
  /\ IF advanceLicenseState = "free"
       THEN advanceLicenseState' = "held" /\ advancePending' = advancePending /\ retirementPassPc' = "driving"
       ELSE advanceLicenseState' = advanceLicenseState /\ advancePending' = TRUE /\ retirementPassPc' = retirementPassPc
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionConsumed, completionDispatchTorn, localDeliveryArmSequence, headTicket>>

\* Cache coherence closes the local/visible gap eventually (WF).
PropagateDeliveryArmClear ==
  /\ visibleDeliveryArmSequence # localDeliveryArmSequence
  /\ visibleDeliveryArmSequence' = localDeliveryArmSequence
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn, localDeliveryArmSequence,
                 headTicket, advanceLicenseState, advancePending, retirementPassPc>>

(* ---------------------------- advancer side ------------------------------- *)
RetirementPassAcquire ==
  /\ retirementPassPc = "idle" /\ advanceLicenseState = "free" /\ ~AllItemsRetired
  /\ advanceLicenseState' = "held" /\ retirementPassPc' = "driving"
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn, localDeliveryArmSequence,
                 visibleDeliveryArmSequence, headTicket, advancePending>>

\* Gated decline: head completionPublished, arm pending -> no mutation; release-and-serve.
TryClaimHeadDeclineForPendingDelivery ==
  /\ retirementPassPc = "driving" /\ ~AllItemsRetired /\ completionPublished[HeadSequence]
  /\ HeadDeliveryPending
  /\ IF advancePending
       THEN advanceLicenseState' = "held" /\ advancePending' = FALSE /\ retirementPassPc' = "driving"
       ELSE advanceLicenseState' = "free" /\ advancePending' = FALSE /\ retirementPassPc' = "idle"
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn, localDeliveryArmSequence,
                 visibleDeliveryArmSequence, headTicket>>

\* Claim + consume (GetResult + reset). Tears phase 2 if the completer is mid-
\* completionDispatch (inflight), or will yet begin it (callbackRegistered, completionPublished, none): both
\* are reads into a reset core. Recorded, not blocked — the invariant names it.
TryClaimHeadConsume ==
  /\ retirementPassPc = "driving" /\ ~AllItemsRetired /\ completionPublished[HeadSequence]
  /\ ~HeadDeliveryPending
  /\ completionConsumed' = [completionConsumed EXCEPT ![HeadSequence] = TRUE]
  /\ completionDispatchTorn' = [completionDispatchTorn EXCEPT ![HeadSequence] =
                (completionDispatch[HeadSequence] = "inflight")
                \/ (callbackRegistered[HeadSequence] /\ completionDispatch[HeadSequence] = "none")]
  /\ headTicket' = headTicket + 1
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionDispatch, localDeliveryArmSequence, visibleDeliveryArmSequence,
                 advanceLicenseState, advancePending, retirementPassPc>>

\* Frontier, first half: arm, then register the trampoline on the incomplete
\* head. The arm precedes registration because registration may fire inline.
ArmAndRegisterCompletionCallback ==
  /\ retirementPassPc = "driving" /\ ~AllItemsRetired /\ ~completionPublished[HeadSequence] /\ ~callbackRegistered[HeadSequence]
  /\ callbackRegistered' = [callbackRegistered EXCEPT ![HeadSequence] = TRUE]
  /\ localDeliveryArmSequence' = HeadSequence /\ visibleDeliveryArmSequence' = HeadSequence
  /\ retirementPassPc' = "arming"
  /\ UNCHANGED <<completionPublished, completionDispatch, completionConsumed, completionDispatchTorn, headTicket, advanceLicenseState, advancePending>>

ReleaseFrontier ==
  /\ retirementPassPc = "arming"
  /\ UNCHANGED <<localDeliveryArmSequence, visibleDeliveryArmSequence>>
  /\ IF advancePending
       THEN advanceLicenseState' = "held" /\ advancePending' = FALSE /\ retirementPassPc' = "driving"
       ELSE advanceLicenseState' = "free" /\ advancePending' = FALSE /\ retirementPassPc' = "idle"
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn, headTicket>>

\* Nothing to do at the head (already callbackRegistered, not yet completionPublished) or done.
RetirementPassExit ==
  /\ retirementPassPc = "driving"
  /\ AllItemsRetired \/ (~AllItemsRetired /\ ~completionPublished[HeadSequence] /\ callbackRegistered[HeadSequence])
  /\ IF advancePending
       THEN advanceLicenseState' = "held" /\ advancePending' = FALSE /\ retirementPassPc' = "driving"
       ELSE advanceLicenseState' = "free" /\ advancePending' = FALSE /\ retirementPassPc' = "idle"
  /\ UNCHANGED <<completionPublished, callbackRegistered, completionDispatch, completionConsumed, completionDispatchTorn, localDeliveryArmSequence,
                 visibleDeliveryArmSequence, headTicket>>

Terminating == AllItemsRetired /\ UNCHANGED vars

Next ==
  \/ \E i \in 1..N : PublishCompletion(i) \/ BeginCompletionDispatch(i) \/ DeliverCompletionCallback(i)
                       \/ CompletionCallbackAcquireOrDeposit(i)
  \/ PropagateDeliveryArmClear
  \/ RetirementPassAcquire \/ TryClaimHeadDeclineForPendingDelivery \/ TryClaimHeadConsume
  \/ ArmAndRegisterCompletionCallback \/ ReleaseFrontier \/ RetirementPassExit
  \/ Terminating

Spec ==
  /\ Init /\ [][Next]_vars
  /\ WF_vars(PropagateDeliveryArmClear) /\ WF_vars(RetirementPassAcquire)
  /\ WF_vars(TryClaimHeadDeclineForPendingDelivery) /\ WF_vars(TryClaimHeadConsume)
  /\ WF_vars(ArmAndRegisterCompletionCallback) /\ WF_vars(ReleaseFrontier) /\ WF_vars(RetirementPassExit)
  /\ \A i \in 1..N :
       WF_vars(PublishCompletion(i)) /\ WF_vars(BeginCompletionDispatch(i))
         /\ WF_vars(DeliverCompletionCallback(i)) /\ WF_vars(CompletionCallbackAcquireOrDeposit(i))

(* ------------------------------ properties -------------------------------- *)
\* AOORE property: no consumption tears a live-or-pending phase-2 completionDispatch.
CompletionDispatchNeverTorn == \A i \in 1..N : ~completionDispatchTorn[i]

\* The mid-completionDispatch subclass alone (the literal AOORE: pair-reads live at consume).
\* Persistent fact: a completionConsumed tenure whose completionDispatch is still marked inflight was
\* completionConsumed mid-completionDispatch (completionDispatch only advances past inflight via DeliverCompletionCallback,
\* which the tear does not undo in this model).
NoRetirementDuringCompletionDispatch == \A i \in 1..N : ~(completionConsumed[i] /\ completionDispatch[i] = "inflight" /\ completionDispatchTorn[i])

\* Single-armed: a set arm word names an unretired tenure (local view; visible may
\* lag — that staleness is the safe direction, checked separately below).
DeliveryArmNamesUnretiredItem == localDeliveryArmSequence # 0 => localDeliveryArmSequence > headTicket

\* Staleness soundness: local/visible divergence only ever shows a STALE ARM
\* (visible armed, locally cleared) — the spurious-decline direction — never a
\* premature clear (visible clear while locally armed = false accept).
DeliveryArmVisibilityCanOnlyRetainStaleSet ==
  (visibleDeliveryArmSequence # localDeliveryArmSequence) => localDeliveryArmSequence = 0

\* Liveness: the gate's declines are lossless — every tenure retires.
EveryItemEventuallyRetires == <>[](headTicket = N)
=============================================================================
