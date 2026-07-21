------------------------ MODULE Pipeline ------------------------
(* End-to-end composition of the four planned code components:

     PipelineSource - ordered delivery and source-to-executor ownership transfer
     InFlightStore  - membership, tiered storage, FIFO observation and count
     ItemTenure     - completion dispatch, delivery arm, claim and retirement
     ActivationGate - activation turn, edge lock and activation handoffs

   The model includes direct and resident retirement, dispatcher-side and
   retirement-pass recovery, the advance license, and the two-sided empty-edge
   handoff. The empty-edge handoff resolves a dispatcher's published activation
   handoff when the last resident item retires.

   Instruction boundaries are intentional. In particular, handoff publication,
   its fence, the in-flight-count read, head observations, turn claims, callback
   delivery, license release, count decrement and license reacquisition are
   separate actions.

   Weak-memory scope is limited to the delivery-arm clear and activation-handoff
   visibility required by the implementation's ordering arguments.

   Source delivery is passive: the bounded source always has its next item ready.
   Concrete pulling and wake mechanics refine that contract without changing it.

   Not modeled here: public Depth, trailing-task successor gating,
   shutdown wakeups, pipeline reuse, physical worker scheduling, and distinct
   concrete object identity for multiple recovery substitutes at one FIFO position.
   ItemTaskGates discharges the trailing-task and pipeline-task assumption.

   Spec intentionally has no fairness assumptions. Deadlock checks plus safety
   invariants provide progress evidence; focused mechanism models and runtime
   soaks own the stronger environment-dependent liveness claims.                 *)
EXTENDS Integers, Sequences, TLC, PipelineSource

ASSUME N \in Nat /\ N >= 2

NONE == 0
RetirementPasses == {"W", "P", "E"}   \* E = the committer's own walk identity (EdgeLockBail)

VARIABLES
  \* ---- InFlightStore custody ----
  inFlightCount, slot, queuePublished, overflowQueue, headTicket,
  \* ---- ItemTenure custody (per item) ----
  completionPublished,    \* phase 1 published (status-visible)
  completionDispatch,     \* [1..N -> "none"|"inflight"|"done"] phase 2 (pair-reads live at inflight)
  completionConsumed, completionDispatchTorn,
  completionCallbackRegistered,      \* [1..N -> BOOLEAN] a live trampoline registration exists
  localDeliveryArmItem, visibleDeliveryArmItem,   \* the delivery arm (weak clear)
  \* ---- ActivationGate custody ----
  advanceOwner, advancePending,   \* the license (acquire-or-deposit / release-or-serve)
  edgeLockOwner,        \* the edge lock: "0" | "E" | retirement pass id (chain-boundary assigns only)
  activationTurn,         \* NONE | owner identity (resident sequence or negative generation)
  activationPerformed,    \* [1..N -> BOOLEAN] the policy EFFECT (set outside every hold)
  executionKind,       \* [1..N -> {"self","gated"}] gated completes only once activationPerformed
  localHandoffGeneration, visibleHandoffGeneration,  \* the handoff word's GEN (weak local/visible; 0 = none)
  handoffItem,             \* the item riding the CURRENT placement (versioned-word capture shape)
  generationCounter, executorGeneration,      \* generation mint; the committer's own placement gen
  itemGeneration,             \* [1..N -> gen] each item's generation identity (0 = none)
  recoveryAttempted,           \* [1..N -> BOOLEAN] one recovery per item (state-space bound)
  passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem,   \* the empty-edge handoff's PIN: peeked (gen, item) per retirement pass
  emptyEdgeActivationBusy,          \* an empty-edge handoff ActivateHeadItem call is IN FLIGHT (entered, not returned)
  resolvedEmptyEdgeGeneration,   \* stamp written after the empty-edge handoff's policy call returns
  faultConcurrentActivation,  \* a substitute activation overlapped an in-flight empty-edge handoff act
  handoffClaimed,   \* the published handoff was taken
  faultDoubleActivate, faultTokenRead, lateCompletionCallback,
  \* ---- populations ----
  executorPc, executorItem,     \* committer: "incrementStoreCount"|"publishStoreItem"|"readAdvanceLicense"|"finishOwnedPass"|"next"|"off"
  openedEmptyEdge,    \* the increment saw prev==0: this commit is the edge (gate custody
                \* fact — NOT the storage tier, which can be queue while escalated)
  handoffFenceEpoch,       \* Dekker barrier epoch (ExecutorFenceHandoffPublication advances; publishes NOTHING)
  passDecrementEpoch,    \* [RetirementPasses -> epoch at their last count-decrement RMW]
  passPc,          \* [RetirementPasses -> "off"|"readFirstTier"|"readSecondTier"|"decideHeadClaim"|"armHeadDelivery"|"releasePhantomEdge"|"exitPass"
                \*  |"releaseActivationTurn"|"decrementInFlightCount"|"reacquireLicense"|"phantomReadFirstTier"|"phantomReadSecondTier"|"phantomDecide"
                \*  |"emptyEdgeLock"|"emptyEdgeObserve"|"emptyEdgeUnlock"|"emptyEdgeTurnClaim"|"emptyEdgeHandoffTake"
                \*  |"emptyEdgeActivate"|"emptyEdgeRecordResolution"
                \*  |"recoveryLock"|"recoveryPrepare"|"recoveryActivate"|"awaitEmptyEdgeResolution"|"recoveryDecrement"]
  passSlotSeen, passQueueSeen,   \* the claim's two read snapshots (per retirement pass)
  fifoViolation

vars == <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn,
          completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn, activationPerformed,
          executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem,
          handoffClaimed, faultDoubleActivate, faultTokenRead, itemGeneration, recoveryAttempted,
          lateCompletionCallback, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch,
          executorPc, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation>>

QueueHead == IF overflowQueue = <<>> THEN NONE ELSE overflowQueue[1]
StoreHead == IF slot # NONE THEN slot ELSE QueueHead
\* Gate pending = the armed item IS the current store head. Direct-retired items
\* never mint a position, so ticket arithmetic (visibleDeliveryArmItem = headTicket+1)
\* no longer coincides with item identity - compare identities directly.
HeadDeliveryPending == visibleDeliveryArmItem # NONE /\ visibleDeliveryArmItem = StoreHead

Init ==
  /\ inFlightCount = 0 /\ slot = NONE /\ queuePublished = FALSE /\ overflowQueue = <<>> /\ headTicket = 0
  /\ completionPublished = [i \in 1..N |-> FALSE]
  /\ completionDispatch  = [i \in 1..N |-> "none"]
  /\ completionConsumed  = [i \in 1..N |-> FALSE]
  /\ completionDispatchTorn      = [i \in 1..N |-> FALSE]
  /\ completionCallbackRegistered   = [i \in 1..N |-> FALSE]
  /\ localDeliveryArmItem = 0 /\ visibleDeliveryArmItem = 0
  /\ advanceOwner = "0" /\ advancePending = FALSE /\ edgeLockOwner = "0"
  /\ activationTurn = NONE /\ activationPerformed = [i \in 1..N |-> FALSE]
  /\ executionKind \in [1..N -> {"self","gated"}]
  /\ localHandoffGeneration = NONE /\ visibleHandoffGeneration = NONE /\ handoffItem = NONE /\ generationCounter = 0 /\ executorGeneration = 0
  /\ passEmptyEdgeHandoffGeneration = [t \in RetirementPasses |-> NONE] /\ passEmptyEdgeHandoffItem = [t \in RetirementPasses |-> NONE]
  /\ itemGeneration = [i \in 1..N |-> 0] /\ recoveryAttempted = [i \in 1..N |-> FALSE]
  /\ handoffClaimed = FALSE
  /\ faultDoubleActivate = FALSE /\ faultTokenRead = FALSE /\ lateCompletionCallback = FALSE
  /\ emptyEdgeActivationBusy = FALSE /\ resolvedEmptyEdgeGeneration = 0 /\ faultConcurrentActivation = FALSE
  /\ executorPc = "dispatch" /\ SourceInit(executorItem) /\ openedEmptyEdge = FALSE
  /\ handoffFenceEpoch = 0 /\ passDecrementEpoch = [t \in RetirementPasses |-> 0]
  /\ passPc = [t \in RetirementPasses |-> "off"]
  /\ passSlotSeen = [t \in RetirementPasses |-> NONE] /\ passQueueSeen = [t \in RetirementPasses |-> NONE]
  /\ fifoViolation = FALSE

(* ------------------------- committer (E strand) --------------------------- *)
\* Dispatch: place the exec-handoff (weak), fence, read the count — the
\* composed ActivationGate Dekker. inFlightCount>0: a chain exists, its empty-edge handoff/stop is
\* obligated; inFlightCount==0: no retirement pass can exist — SELF-GRANT under the edge lock.
ExecutorDispatch ==
  /\ executorPc = "dispatch" /\ executorItem <= N
  \* Every completionDispatch places a handoff (the fast item's is eaten by the
  \* completion-time reclaim); only the gated (sync-body) executionKind WAITS on it.
  /\ generationCounter' = generationCounter + 1
  /\ executorGeneration' = generationCounter + 1
  /\ itemGeneration' = [itemGeneration EXCEPT ![executorItem] = generationCounter + 1]
  \* Idle-regime elision (executor dispatch): count 0 with no handoff
  \* outstanding = no concurrent activation decider; SKIP the placement and claim
  \* the activationTurn fail-if-live (lock-free). Everything else places the handoff.
  /\ IF inFlightCount = 0 /\ localHandoffGeneration = NONE
       THEN /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
            /\ executorPc' = "claimElidedTurn"
       ELSE /\ localHandoffGeneration' = generationCounter + 1 /\ visibleHandoffGeneration' = visibleHandoffGeneration   \* WEAK place (gen)
            /\ handoffItem' = executorItem
            /\ handoffClaimed' = FALSE
            /\ executorPc' = IF executionKind[executorItem] = "gated" THEN "fenceHandoff" ELSE "takeOwnHandoff"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, recoveryAttempted>>

\* The fail-if-live TurnExec claim (ActivationGate.TryClaimProvisionalTurn) is lock-free. The gate
\* excluded residents, but an empty-edge pass may own a NON-RESIDENT turn (activationTurn live at
\* count 0), so the activationTurn - not the stale gate - is the authority: decline on a
\* live activationTurn and fall back to a normal handoff publication (the loss branch,
\* the executor dispatch fallback).
ExecutorClaimElidedTurn ==
  /\ executorPc = "claimElidedTurn"
  /\ IF activationTurn = NONE
       THEN /\ activationTurn' = -executorGeneration
            /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
            /\ executorPc' = "activateElidedItem"
       ELSE /\ localHandoffGeneration' = executorGeneration /\ visibleHandoffGeneration' = visibleHandoffGeneration
            /\ handoffItem' = executorItem /\ handoffClaimed' = FALSE
            /\ UNCHANGED activationTurn
            /\ executorPc' = IF executionKind[executorItem] = "gated" THEN "fenceHandoff" ELSE "takeOwnHandoff"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner,
                 activationPerformed, executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The elision's policy call - outside everything (no hold exists on this path).
\* Both flavors proceed straight to the commit: the gated window is satisfied.
ExecutorActivateElidedItem ==
  /\ executorPc = "activateElidedItem"
  /\ faultDoubleActivate' = (faultDoubleActivate \/ activationPerformed[executorItem])
  /\ activationPerformed' = [activationPerformed EXCEPT ![executorItem] = TRUE]
  /\ executorPc' = "incrementStoreCount"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The executor's post-handoff publication barrier is relational, not
\* publishing: a fence does not flush the store into one
\* universal view; it advances the epoch. An observer whose count-RMW POSTDATES
\* this epoch is synchronized (reads truth); one whose RMW predates it may
\* still read the stale view. Only the true both-miss is thereby excluded.
ExecutorFenceHandoffPublication ==
  /\ executorPc = "fenceHandoff"
  /\ handoffFenceEpoch' = handoffFenceEpoch + 1
  /\ UNCHANGED visibleHandoffGeneration
  /\ executorPc' = "readInFlightCount"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem,
                 openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PropagateHandoff ==
  /\ visibleHandoffGeneration # localHandoffGeneration
  /\ visibleHandoffGeneration' = localHandoffGeneration
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc,
                 executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorReadInFlightCount ==
  /\ executorPc = "readInFlightCount"
  /\ executorPc' = IF inFlightCount = 0 THEN "acquireSelfClaimLock" ELSE "awaitHandoffResolution"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Self-claim: take the handoff and assign activationTurn under the lock; activate outside it.
ExecutorAcquireSelfClaimLock ==
  /\ executorPc = "acquireSelfClaimLock"
  /\ UNCHANGED edgeLockOwner
  /\ executorPc' = "observeSelfClaim"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem,
                 openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorObserveSelfClaim ==
  /\ executorPc = "observeSelfClaim"
  /\ IF localHandoffGeneration = executorGeneration /\ ~handoffClaimed   \* own placement (by GEN) still there
       THEN /\ UNCHANGED <<activationTurn, handoffClaimed, localHandoffGeneration, visibleHandoffGeneration>>
            /\ executorPc' = "claimProvisionalTurn"
            /\ UNCHANGED edgeLockOwner
       ELSE /\ UNCHANGED <<activationTurn, handoffClaimed, localHandoffGeneration, visibleHandoffGeneration>>
            /\ executorPc' = "awaitHandoffResolution"              \* another strand owns or will resolve the handoff
            /\ UNCHANGED edgeLockOwner
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationPerformed,
                 executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* TURN-CLAIM-FIRST (code TryReclaimExecDeferred): the fail-if-live TURN claim is
\* the shared authority (the empty-edge handoff's take-claim uses the same word) - a live activationTurn
\* (empty-edge handoff or recovery turn) declines, leaving the handoff
\* for its owner; only the claim winner takes it.
ExecutorClaimProvisionalTurn ==
  /\ executorPc = "claimProvisionalTurn"
  /\ IF activationTurn = NONE
       THEN /\ activationTurn' = -executorGeneration
            /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed, edgeLockOwner>>
            /\ executorPc' = "takeSelfHandoff"
       ELSE /\ UNCHANGED <<activationTurn, localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed, edgeLockOwner>>
            /\ executorPc' = "awaitHandoffResolution"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationPerformed,
                 executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Claim-winner's consume: the handoff is exclusively ours (the empty-edge handoff bails its
\* take on the live activationTurn we now hold), so a plain clear, then the policy call.
ExecutorTakeSelfHandoff ==
  /\ executorPc = "takeSelfHandoff"
  /\ localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE /\ handoffClaimed' = TRUE
  /\ UNCHANGED edgeLockOwner
  /\ executorPc' = "activateSelfClaimedItem"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The policy call runs outside the hold (the assign-to-activate gap is real).
ExecutorActivateSelfClaimedItem ==
  /\ executorPc = "activateSelfClaimedItem"
  /\ faultDoubleActivate' = (faultDoubleActivate \/ activationPerformed[executorItem])
  /\ activationPerformed' = [activationPerformed EXCEPT ![executorItem] = TRUE]
  /\ executorPc' = "incrementStoreCount"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The sync-body window: the gated body blocks inside execution until the
\* activation occurs—the commit cannot arrive. The empty-edge handoff and self-claim are the only
\* exits; a chain's stop CANNOT rescue this item (it is not resident yet).
ExecutorAwaitHandoffResolution ==
  /\ executorPc = "awaitHandoffResolution" /\ activationPerformed[executorItem]
  /\ executorPc' = "takeOwnHandoff"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* CommitPendingTail's reclaim site: the PREVIOUS item's handoff is resolved at
\* the next loop boundary - plain consume of its own leftover placement (skip when a
\* another strand took it), release-if-mine on the -gen. The last item's leftover
\* is resolved by the completion-time ReclaimCompletionCallback instead.
ExecutorResolvePriorHandoff ==
  /\ executorPc = "resolvePriorHandoff"
  /\ IF localHandoffGeneration # NONE /\ localHandoffGeneration = executorGeneration
       THEN /\ localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE
            /\ handoffClaimed' = TRUE
            \* NO activationTurn release here (finding #7-rev): the owned-activationTurn release is
            \* COMPLETION/RETIRE property (RetireItem ownedTurn -> the claim's
            \* release-if-mine), not a loop-boundary one - releasing at resolve
            \* freed a still-live gated item's turn and let the stop assign it again.
            /\ UNCHANGED activationTurn
       ELSE UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed, activationTurn>>
  /\ executorPc' = "dispatch"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationPerformed,
                 executionKind, faultDoubleActivate, faultTokenRead, lateCompletionCallback, executorItem,
                 openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, generationCounter, executorGeneration,
                 passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* TRAILING-FAILURE RECOVERY: the gated tail's body faults instead of
\* completing; the executor swaps a substitute. Its reclaim of the original
\* handoff: WIN = owned (activate substitute inline); LOSE = an empty-edge handoff won
\* EXACTLY this gen - park on the generation-keyed resolution stamp.
\* before the substitute's activation.
ExecutorDiscoverRecovery ==
  /\ UNCHANGED <<handoffFenceEpoch, passDecrementEpoch>>
  /\ executorPc = "awaitHandoffResolution" /\ executorItem <= N /\ ~activationPerformed[executorItem] /\ ~recoveryAttempted[executorItem]
  /\ recoveryAttempted' = [recoveryAttempted EXCEPT ![executorItem] = TRUE]   \* ONE recovery per item (bound)
  /\ executorPc' = "reclaimFailedHandoff"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 faultTokenRead, lateCompletionCallback, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration,
                 faultConcurrentActivation, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, itemGeneration>>

ExecutorReclaimFailedHandoff ==
  /\ executorPc = "reclaimFailedHandoff"
  /\ IF localHandoffGeneration = executorGeneration
       THEN \* reclaim WINS: owned. Release an empty-edge handoff's stale claim of MY gen
            \* (CAS-if-mine - the double-bail's dual release); substitute mints
            \* fresh at act.
            /\ localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE
            /\ handoffClaimed' = TRUE
            /\ activationTurn' = IF activationTurn = -executorGeneration THEN NONE ELSE activationTurn
            /\ executorPc' = "activateOwnedRecovery"
       ELSE \* LOST: an empty-edge handoff won this exact gen
            /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed, activationTurn>>
            /\ executorPc' = "awaitEmptyEdgeResolution"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationPerformed,
                 executionKind, faultDoubleActivate, faultTokenRead, lateCompletionCallback, emptyEdgeActivationBusy,
                 resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, itemGeneration, recoveryAttempted>>

ExecutorAwaitEmptyEdgeResolution ==
  /\ UNCHANGED <<handoffFenceEpoch, passDecrementEpoch>>
  /\ executorPc = "awaitEmptyEdgeResolution" /\ resolvedEmptyEdgeGeneration >= executorGeneration   \* the EXACT stamp
  /\ executorPc' = "activateInheritedRecovery"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 faultTokenRead, lateCompletionCallback, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration,
                 faultConcurrentActivation, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, itemGeneration, recoveryAttempted>>

\* The substitute's activation is unsafe while the original turn's policy call
\* is still in flight (the cross-strand overlap the stamp exists to prevent).
ExecutorActivateRecovery ==
  /\ UNCHANGED <<handoffFenceEpoch, passDecrementEpoch>>
  \* WIN path: a -executorGeneration still visible is an empty-edge handoff's DOOMED CLAIM (its consume
  \* must lose - the double-bail), never ownership: wait out its release, then
  \* Claim fresh if free. The post-stamp inherit path uses -executorGeneration as the
  \* completionPublished turn - ClaimOrInheritProvisionalTurn transfers it in place.
  \* Both arms are Count==0-gated (RecoverItem): at count>0 the substitute only
  \* RE-PLACES (ExecutorRepublishRecoveryHandoff) - no recovery-time activationTurn touch; a live empty-edge provisional turn
  \* is inherited later at the substitute's COMMIT (-gen -> seq). The ungated
  \* mint at count>0 was finding #10 (deadlocked PassPrepareRecoveryReplacement on a foreign -gen that
  \* could never release: FIFO put its item behind the recovering head).
  /\ \/ (/\ executorPc = "activateOwnedRecovery"
         /\ inFlightCount = 0
         /\ activationTurn = NONE
         /\ generationCounter' = generationCounter + 1
         /\ executorGeneration' = generationCounter + 1
         /\ itemGeneration' = [itemGeneration EXCEPT ![executorItem] = generationCounter + 1]
         /\ activationTurn' = -(generationCounter + 1))
     \/ (/\ executorPc = "activateInheritedRecovery"
         /\ inFlightCount = 0
         /\ activationTurn = -executorGeneration
         /\ UNCHANGED <<activationTurn, generationCounter, executorGeneration, itemGeneration>>)
  /\ faultConcurrentActivation' = (faultConcurrentActivation \/ emptyEdgeActivationBusy)
  /\ activationPerformed' = [activationPerformed EXCEPT ![executorItem] = TRUE]
  /\ faultDoubleActivate' = faultDoubleActivate   \* substitute re-tenure: act is its own
  /\ executorPc' = "takeOwnHandoff"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultTokenRead, lateCompletionCallback, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, handoffItem, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, recoveryAttempted>>

\* Inherit-path substitute at count>0 (finding #11 companion): NO act here (the
\* empty-edge handoff activationPerformed), NO re-place (a re-mint severs the -executorGeneration turn linkage,
\* finding #11's deadlock) - route straight to the commit chain; ExecutorAssignAtEmptyEdge's
\* inherit converts -executorGeneration to the resident sequence at CommitInFlightItem.
ExecutorCommitInheritedRecoveryTurn ==
  /\ UNCHANGED <<handoffFenceEpoch, passDecrementEpoch>>
  /\ executorPc = "activateInheritedRecovery" /\ inFlightCount > 0
  /\ executorPc' = "takeOwnHandoff"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 faultTokenRead, lateCompletionCallback, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration,
                 faultConcurrentActivation, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, itemGeneration, recoveryAttempted>>

\* Executor-recovery replacement (ClaimOrInheritProvisionalTurn's home):
\* a win-path substitute may, instead of inline-acting, RE-PLACE a fresh
\* handoff under the edge lock (lock-free rivals respected via the guard) and
\* re-enter the full completionDispatch Dekker (fence -> read -> turn-claim protocol). This is
\* the second-epoch mint - the last candidate opener for the pin's
\* consume-guard face.
ExecutorRepublishRecoveryHandoff ==
  /\ executorPc = "activateOwnedRecovery"
  /\ edgeLockOwner = "0"                  \* the re-placement is an edge-lock section
  /\ generationCounter' = generationCounter + 1
  /\ executorGeneration' = generationCounter + 1
  /\ itemGeneration' = [itemGeneration EXCEPT ![executorItem] = generationCounter + 1]
  /\ localHandoffGeneration' = generationCounter + 1 /\ visibleHandoffGeneration' = visibleHandoffGeneration   \* weak re-place
  /\ handoffItem' = executorItem /\ handoffClaimed' = FALSE
  /\ executorPc' = "fenceHandoff"              \* re-enter the Dekker: fence -> read -> claim
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, faultDoubleActivate, faultTokenRead, lateCompletionCallback,
                 emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, executorItem, openedEmptyEdge,
                 passPc, passSlotSeen, passQueueSeen, fifoViolation, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, recoveryAttempted>>

\* Ownership is determined before the commit (CommitPendingTail and CommitInFlightItem's handoff assertion):
\* consume the owned handoff now. Consuming a live owned placement retains ownership (this strand
\* keeps the activation obligation); gone = the empty-edge pass took ownership
\* (the empty-edge handoff owns activation). Elision and race-back routes bypass
\* this step: they ARE tailActivated (own-strand activation, handoff resolved).
ExecutorTakeOwnHandoff ==
  /\ executorPc = "takeOwnHandoff"
  /\ IF localHandoffGeneration = executorGeneration /\ localHandoffGeneration # NONE
       THEN /\ localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE
            /\ handoffClaimed' = TRUE
            /\ executorPc' = "incrementStoreCount"
       ELSE /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
            /\ executorPc' = "inclost"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* DIRECT SYNCHRONOUS RETIREMENT (the naked fast path, CommitPendingTail
\* CommitPendingTail's synchronous shortcut: an owned, synchronously successful item at Count==0 retires without
\* ever entering the store - no increment, no publication, no completionCallbackRegistered, no
\* advancer. GetResult consumes the token; the owned activationTurn (an elide/race-back
\* -executorGeneration claim, if any) releases CAS-if-mine - which can free an empty-edge handoff's
\* doomed claim of the same gen (the dual-release double-bail, intended).
\* Nondeterministic alternative to StoreIncrementInFlightCount: the model's "the body completionPublished
\* synchronously" choice. Gated bodies require activation first (sync window).
ExecutorDirectRetire ==
  /\ executorPc = "incrementStoreCount" /\ executorItem <= N /\ inFlightCount = 0
  /\ (executionKind[executorItem] = "self" \/ activationPerformed[executorItem])
  /\ completionPublished' = [completionPublished EXCEPT ![executorItem] = TRUE]
  /\ completionConsumed'  = [completionConsumed EXCEPT ![executorItem] = TRUE]
  /\ activationTurn' = IF activationTurn = -executorGeneration \/ (itemGeneration[executorItem] # 0 /\ activationTurn = -itemGeneration[executorItem])
               THEN NONE ELSE activationTurn
  /\ executorPc' = "next"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionDispatch,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem,
                 openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* InFlightStore.StoreIncrementInFlightCount: increment-first.
StoreIncrementInFlightCount ==
  /\ executorPc \in {"incrementStoreCount", "inclost"} /\ executorItem <= N
  /\ openedEmptyEdge' = (inFlightCount = 0)     \* edge status from the increment-first read of prev
  /\ inFlightCount' = inFlightCount + 1             \* COUNT-WORD RMW (the fold): fences THIS
                                \* strand's stores (the place), nothing foreign
  /\ visibleHandoffGeneration' = localHandoffGeneration
  /\ executorPc' = IF executorPc = "incrementStoreCount" THEN "publishStoreItem" ELSE "publost"
  /\ UNCHANGED <<slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn,
                 completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, executorItem, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* InFlightStore.StorePublishCommittedItem (internal tier choice, leave-head) plus the edge arm:
\* a wasEmpty commit is the frontier owner — it arms + registers the trampoline
\* PRE-PUBLISH-READ (ItemTenure.ArmAndRegister, edge executionKind: arm is chain-
\* mediated => fenced). Mid-chain commits attach NOTHING (frontier invariant).
StorePublishCommittedItem ==
  /\ executorPc \in {"publishStoreItem", "publost"}
  /\ IF openedEmptyEdge
       THEN \* Edge commit (prev==0): publish belongs inside the hold (EdgeLockBail's
            \* ELockDo: lock { attach + arm + assign + publish }; only the
            \* policy activate is outside). Nothing published here.
            /\ UNCHANGED <<slot, queuePublished, overflowQueue, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem>>
            /\ executorPc' = IF executorPc = "publishStoreItem" THEN "prepareEmptyEdgeActivation" ELSE "acquireEmptyEdgeLock"
       ELSE \* mid-chain: publish outside the lock (tier = store custody);
            \* no attach, no arm; the word-read covers (two-sided).
            /\ (IF slot = NONE /\ ~queuePublished
                  THEN slot' = executorItem /\ UNCHANGED <<queuePublished, overflowQueue>>
                  ELSE queuePublished' = TRUE /\ overflowQueue' = Append(overflowQueue, executorItem) /\ UNCHANGED slot)
            /\ UNCHANGED <<completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem>>
            /\ executorPc' = "readAdvanceLicense"
  /\ UNCHANGED <<inFlightCount, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn, advanceOwner, advancePending,
                 executorItem, passPc, passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Edge-lock section: LOCK { assign the activationTurn iff not already ours } UNLOCK;
\* the policy activate runs outside; the activation turn is the authority.
\* Activate before publishing (CommitInFlightItem's empty-store edge): a never-activationPerformed
\* incomplete head gets its policy call HERE, pre-publication - exclusive by
\* INVISIBILITY (unpublished => unclaimable => no recovery substitute can exist).
\* A completionPublished-at-commit item needs no activation at all; callback
\* registration and the delivery arm drive its advance.
ExecutorPrepareEmptyEdgeActivation ==
  /\ executorPc = "prepareEmptyEdgeActivation"
  /\ IF ~completionPublished[executorItem] /\ ~activationPerformed[executorItem]
       THEN /\ faultDoubleActivate' = (faultDoubleActivate \/ activationPerformed[executorItem])
            /\ activationPerformed' = [activationPerformed EXCEPT ![executorItem] = TRUE]
       ELSE UNCHANGED <<faultDoubleActivate, activationPerformed>>
  /\ executorPc' = "eacq"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Licensed owned-edge commit (CommitInFlightItem's TryAcquireIfFree branch):
\* own + TryAcquireIfFree -> hold the ADVANCE LICENSE (not the edge lock) across
\* assign + publish + POST-PUBLISH attach - registration after publication is
\* safe because claims are license-serialized and we hold it (CompletionNeverReadsRetiredTenure
\* checks that argument here). A racing fire deposits on our license
\* (CompletionCallbackAcquireOrDeposit's advanceOwner#0 arm); release-or-serve consumes it and the committer
\* advances its own sole head (identity E). Acquire MISS -> the contended
\* edge-lock path (ExecutorAcquireEmptyEdgeLock), matching CommitInFlightItem's contended edge branch.
ExecutorAcquireAdvanceLicense ==
  /\ executorPc = "eacq"
  /\ IF advanceOwner = "0"
       THEN /\ advanceOwner' = "E" /\ executorPc' = "licassign"
       ELSE /\ UNCHANGED advanceOwner /\ executorPc' = "acquireEmptyEdgeLock"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advancePending, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorAssignAndPublishAtEmptyEdge ==
  /\ executorPc = "licassign"
  /\ activationTurn \in {NONE, -executorGeneration}   \* ActivationGate.CommitTurn premise
  /\ (IF activationTurn = -executorGeneration
        THEN /\ activationTurn' = executorItem   \* inherit: -gen -> seq
             /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
        ELSE /\ activationTurn' = executorItem
             /\ IF localHandoffGeneration = executorGeneration /\ executorGeneration # 0
                  THEN localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE /\ handoffClaimed' = TRUE
                  ELSE UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>)
  /\ (IF slot = NONE /\ ~queuePublished
        THEN slot' = executorItem /\ UNCHANGED <<queuePublished, overflowQueue>>
        ELSE queuePublished' = TRUE /\ overflowQueue' = Append(overflowQueue, executorItem) /\ UNCHANGED slot)
  /\ executorPc' = "licattach"
  /\ UNCHANGED <<inFlightCount, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn, completionCallbackRegistered,
                 localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationPerformed,
                 executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* POST-PUBLISH registration under the advanceOwner license (the code's bet, now checked):
\* a racing fire between publish and here deposits rather than claiming.
ExecutorAttachEmptyEdgeCallback ==
  /\ executorPc = "licattach"
  /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![executorItem] = TRUE]
  /\ faultTokenRead' = (faultTokenRead \/ completionConsumed[executorItem])
  /\ localDeliveryArmItem' = executorItem /\ visibleDeliveryArmItem' = executorItem
  /\ executorPc' = "licrel"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, advanceOwner, advancePending, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorReleaseAdvanceLicense ==
  /\ executorPc = "licrel"
  /\ IF advancePending
       THEN /\ advancePending' = FALSE /\ UNCHANGED advanceOwner
            /\ passPc' = [passPc EXCEPT !["E"] = "readFirstTier"]
            /\ executorPc' = "finishOwnedPass"
       ELSE /\ advanceOwner' = "0" /\ UNCHANGED advancePending
            /\ UNCHANGED passPc
            /\ executorPc' = "next"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem, openedEmptyEdge,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorAcquireEmptyEdgeLock ==
  /\ executorPc = "acquireEmptyEdgeLock"
  /\ edgeLockOwner = "0" /\ edgeLockOwner' = "E"
  /\ executorPc' = "assignAtEmptyEdge"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorItem,
                 openedEmptyEdge, passPc, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorAssignAtEmptyEdge ==
  /\ executorPc = "assignAtEmptyEdge"
  \* in-hold: attach + arm, assign-or-inherit, publish (tier choice)
  /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![executorItem] = TRUE]
  /\ localDeliveryArmItem' = executorItem /\ visibleDeliveryArmItem' = executorItem
  /\ UNCHANGED faultTokenRead
  /\ (IF slot = NONE /\ ~queuePublished
        THEN slot' = executorItem /\ UNCHANGED <<queuePublished, overflowQueue>>
        ELSE queuePublished' = TRUE /\ overflowQueue' = Append(overflowQueue, executorItem) /\ UNCHANGED slot)
  \* The prev==0 commit CONVERTS -(its own gen) -> seq (inherit), else assigns
  \* fresh on a free activationTurn; a FOREIGN live activationTurn defers to a later stop.
  /\ IF activationTurn = -executorGeneration
       THEN \* INHERIT own turn: convert to resident sequence, no re-activation.
            /\ activationTurn' = executorItem
            /\ UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
            /\ executorPc' = "next"
       ELSE IF activationTurn = NONE
       THEN /\ activationTurn' = executorItem
            /\ IF localHandoffGeneration = executorGeneration /\ executorGeneration # 0
                 THEN localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE /\ handoffClaimed' = TRUE
                 ELSE UNCHANGED <<localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
            /\ executorPc' = "next"
       ELSE \* ActivationGate.CommitTurn: a foreign edge turn is unreachable
            \* (release-before-decrement makes prev==0 imply the prior activationTurn is
            \* gone). Model as the assert: this branch blocks; reachability shows
            \* as deadlock, mirroring the Debug.Assert.
            /\ FALSE
            /\ UNCHANGED <<activationTurn, localHandoffGeneration, visibleHandoffGeneration, handoffItem, handoffClaimed>>
            /\ executorPc' = executorPc
  /\ edgeLockOwner' = "0"
  /\ UNCHANGED <<inFlightCount, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, advanceOwner, advancePending, activationPerformed,
                 executionKind, faultDoubleActivate, executorItem, openedEmptyEdge, passPc, passSlotSeen, passQueueSeen,
                 fifoViolation, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorActivateEmptyEdgeItem ==
  /\ executorPc = "activateEmptyEdgeItem"
  /\ faultDoubleActivate' = (faultDoubleActivate \/ activationPerformed[executorItem])
  /\ activationPerformed' = [activationPerformed EXCEPT ![executorItem] = TRUE]
  /\ executorPc' = "next"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, executorItem, openedEmptyEdge, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* EdgeLockBail: the committer's post-publish word-read — advanceOwner==0 means any
\* retirement pass has bailed or never was; the committer WALKS ITSELF (identity E,
\* verbatim EdgeLockBail: executorPc waits in "finishOwnedPass" until E's walk exits).
ExecutorReadAdvanceLicense ==
  /\ executorPc = "readAdvanceLicense"
  /\ visibleHandoffGeneration' = localHandoffGeneration
  /\ IF advanceOwner = "0"
       THEN /\ advanceOwner' = "E" /\ passPc' = [passPc EXCEPT !["E"] = "readFirstTier"]
            /\ executorPc' = "finishOwnedPass" /\ advancePending' = advancePending
       ELSE \* acquire-or-deposit (InFlightStore.TryAcquire): an advanceOwner license
            \* takes the pending bit - the holder's release-or-serve re-probes
            \* and drives this commit; the handoff is lossless BY CONSTRUCTION.
            /\ UNCHANGED <<advanceOwner, passPc>> /\ advancePending' = TRUE /\ executorPc' = "next"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, executorItem, passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorFinishOwnedPass ==
  /\ executorPc = "finishOwnedPass" /\ passPc["E"] = "off"
  /\ executorPc' = "next"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, executorItem, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

ExecutorNext ==
  /\ executorPc = "next"
  /\ openedEmptyEdge' = openedEmptyEdge
  /\ IF SourceHasSuccessor(executorItem)
       THEN /\ SourceDeliverSuccessor(executorItem)
            /\ executorPc' = "resolvePriorHandoff"
       ELSE /\ SourceIsExhausted(executorItem)
            /\ UNCHANGED executorItem
            /\ executorPc' = "off"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

(* --------------------- ItemTenure: two-phase completion ------------------- *)
\* Phase 1: publish completionPublished (status-visible / claimable). Resident items only.
PublishCompletion(i) ==
  /\ ~completionPublished[i] /\ ((slot = i) \/ (\E j \in 1..Len(overflowQueue) : overflowQueue[j] = i)
                       \* the retirement-pass recovery substitute completes outside the store
                       \* (position dequeued, credit advanceOwner) - scoped to a LIVE walk
                       \* episode so C-side-recoveryAttempted (store-resident) items stay
                       \* on the store-resident path.
                       \/ (recoveryAttempted[i] /\ ~completionConsumed[i]
                           /\ \E t \in RetirementPasses : passPc[t] \in {"recoveryActivate", "awaitEmptyEdgeResolution"}
                                /\ (passSlotSeen[t] = i \/ passQueueSeen[t] = i)))
  /\ (executionKind[i] = "gated") => activationPerformed[i]     \* the sync-body structure
  /\ (handoffItem # i \/ localHandoffGeneration = NONE)  \* completion AFTER handoff resolution
                                        \* (ClearExecutingItem before SetResult)
  /\ completionPublished' = [completionPublished EXCEPT ![i] = TRUE]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionDispatch, completionConsumed, completionDispatchTorn,
                 completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, executorPc, executorItem, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Phase 2 entry: the completer of a REGISTERED tenure begins the unlicensed
\* pair-read. Unregistered tenures never completionDispatch (immune).
BeginCompletionDispatch(i) ==
  /\ completionPublished[i] /\ completionCallbackRegistered[i] /\ completionDispatch[i] = "none" /\ ~completionConsumed[i]
  /\ completionDispatch' = [completionDispatch EXCEPT ![i] = "inflight"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionConsumed, completionDispatchTorn,
                 completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, executorPc, executorItem, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Phase 2 delivery = the FIRE (ItemTenure.DeliverCompletionCallback fused with the gate's
\* Fire): clear the arm FIRST (weak), consume the registration, then the
\* acquire-or-deposit on the REAL license — the RMW propagates the clear.
DeliverCompletionCallback(i) ==
  /\ completionDispatch[i] = "inflight"
  /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![i] = FALSE]
  /\ localDeliveryArmItem' = IF localDeliveryArmItem = i THEN 0 ELSE localDeliveryArmItem   \* self-checking seq
  /\ visibleDeliveryArmItem' = visibleDeliveryArmItem
  /\ completionDispatch' = [completionDispatch EXCEPT ![i] = "done"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionConsumed, completionDispatchTorn,
                 advanceOwner, advancePending, executorPc, executorItem, passPc, passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The fire's acquire-or-deposit (count-word RMW = full fence: clear propagates).
CompletionCallbackAcquireOrDeposit(i) ==
  /\ completionDispatch[i] = "done"
  /\ completionDispatch' = [completionDispatch EXCEPT ![i] = "acquired"]
  /\ lateCompletionCallback' = (lateCompletionCallback \/ completionConsumed[i])   \* STRAGGLER FIRE: execution tail outlives the tenure (benign probe)
  /\ visibleDeliveryArmItem' = localDeliveryArmItem
  /\ \E t \in RetirementPasses :
       IF advanceOwner = "0" /\ passPc[t] = "off"
         THEN advanceOwner' = t /\ advancePending' = advancePending /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
         ELSE advanceOwner' = advanceOwner /\ advancePending' = TRUE /\ passPc' = passPc
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionConsumed, completionDispatchTorn,
                 completionCallbackRegistered, localDeliveryArmItem, executorPc, executorItem, passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Background coherence for the weak clear.
ReclaimCompletionCallback(i) ==
  \* Completion-time reclaim (ActivationGate.TryTakeHandoff, plain): lock-free, no
  \* activationTurn claim (a foreign resident may hold the activationTurn). Can win the word out
  \* from under an empty-edge handoff that already CLAIMED (the double-bail); both then
  \* release the same -gen, CAS-if-mine. Completing without activation is
  \* This is permitted: the invariant is at most once, not always once.
  \* ClearExecutingItem ordering: the reclaim PRECEDES completion-publication;
  \* a gated body cannot reach it before activation (sync body blocked).
  /\ handoffItem = i /\ localHandoffGeneration # NONE
  /\ (executionKind[i] = "self" \/ activationPerformed[i])
  \* ActivationGate.TryTakeHandoff uses only a word CAS: the plain
  \* consume never touches the activationTurn - the -gen release is a COMPLETION/RETIRE
  \* property (RetireItem ownedTurn -> the claim path's itemGeneration-matched
  \* release-if-mine). Bundling the release here freed a still-live gated item's
  \* turn before completion and let the stop assign it again (the 102k-state CE).
  /\ UNCHANGED activationTurn
  /\ localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE
  /\ handoffClaimed' = TRUE
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationPerformed,
                 executionKind, faultDoubleActivate, executorPc, executorItem, openedEmptyEdge, passPc, passSlotSeen,
                 passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration,
                 passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PropagateDeliveryArmClear ==
  /\ visibleDeliveryArmItem # localDeliveryArmItem
  /\ visibleDeliveryArmItem' = localDeliveryArmItem
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, advanceOwner, advancePending, executorPc, executorItem, passPc,
                 passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

(* ------------------- the walk: licensed claim (composed) ------------------ *)
\* InFlightStore's queue-first, slot-second observation boundary.
PassReadFirstStoreTier(t) ==
  /\ passPc[t] = "readFirstTier"
  /\ passQueueSeen' = [passQueueSeen EXCEPT ![t] = QueueHead] /\ UNCHANGED passSlotSeen
  /\ passPc' = [passPc EXCEPT ![t] = "readSecondTier"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, executorPc, executorItem, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassReadSecondStoreTier(t) ==
  /\ passPc[t] = "readSecondTier"
  /\ passSlotSeen' = [passSlotSeen EXCEPT ![t] = slot] /\ UNCHANGED passQueueSeen
  /\ passPc' = [passPc EXCEPT ![t] = "decideHeadClaim"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, executorPc, executorItem, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The composed decision: InFlightStore legs + ItemTenure's gate + consume+tear.
\* Gate placement per the code: AFTER the completionPublished check, BEFORE any mutation.
\* An incomplete head is the stop: arm and register (the second frontier owner),
\* then exit. A gated decline -> bail (release-or-serve + repeek, the two-sided
\* protocol carried from EdgeLockBail via ActivationGate).
PassDecideHeadClaim(t) ==
  /\ passPc[t] = "decideHeadClaim"
  /\ \E recover \in BOOLEAN :
     LET sSeen == passSlotSeen[t]
         qSeen == passQueueSeen[t]
         target == IF sSeen # NONE THEN sSeen ELSE qSeen
     IN
     IF target = NONE
       THEN \* nothing visible: phantom (inFlightCount>0: bail + repeek) versus the true idle
            \* edge (inFlightCount=0), where the empty-edge handoff runs.
            /\ passPc' = [passPc EXCEPT ![t] = IF inFlightCount > 0 THEN "phantomReadFirstTier" ELSE "emptyEdgeLock"]
            /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch,
                           completionConsumed, completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner,
                           advancePending, fifoViolation, activationTurn, activationPerformed, localHandoffGeneration, visibleHandoffGeneration,
                           handoffClaimed, edgeLockOwner, faultDoubleActivate, executionKind,
                           faultTokenRead>>
     ELSE IF ~completionPublished[target]
       THEN \* the stop: frontier moves here — arm, register, assign and activate
            \* (fused, UNLOCKED: the count-partition bet, carried) on the
            \* incomplete head. Re-check by TURN IDENTITY (never `activationPerformed`).
            /\ IF completionCallbackRegistered[target]
                 THEN UNCHANGED <<completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, faultTokenRead>>
                 ELSE /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![target] = TRUE]
                      /\ faultTokenRead' = (faultTokenRead \/ completionConsumed[target])
                      /\ localDeliveryArmItem' = target /\ visibleDeliveryArmItem' = target
            /\ IF activationTurn = NONE
                 THEN \* Only a free activationTurn is assignable (write discipline:
                      \* a live activationTurn - resident sequence or provisional generation - declines).
                      /\ activationTurn' = target
                      /\ faultDoubleActivate' = (faultDoubleActivate \/ activationPerformed[target])
                      /\ activationPerformed' = [activationPerformed EXCEPT ![target] = TRUE]
                 ELSE UNCHANGED <<activationTurn, activationPerformed, faultDoubleActivate>>
            /\ passPc' = [passPc EXCEPT ![t] = "exitPass"]
            /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch,
                           completionConsumed, completionDispatchTorn, advanceOwner, advancePending, fifoViolation, localHandoffGeneration, visibleHandoffGeneration,
                           handoffClaimed, edgeLockOwner, executionKind>>
     ELSE IF HeadDeliveryPending
       THEN \* gated decline: no mutation; bail out (lossless via deposit/serve).
            /\ passPc' = [passPc EXCEPT ![t] = "releasePhantomEdge"]
            /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch,
                           completionConsumed, completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner,
                           advancePending, fifoViolation, activationTurn, activationPerformed, localHandoffGeneration, visibleHandoffGeneration,
                           handoffClaimed, edgeLockOwner, faultDoubleActivate, executionKind,
                           faultTokenRead>>
     ELSE \* CLAIM + CONSUME (GetResult + reset): the tear fact if phase 2 is
          \* in flight or a registered-completionPublished tenure has not begun it.
          /\ IF sSeen # NONE
               THEN IF slot = sSeen
                      THEN IF recover /\ sSeen = 2 /\ ~recoveryAttempted[sSeen]
                      THEN \* FAULTED claim (walk-side recovery): position dequeued,
                           \* Count credit remains held; the failure read is a GetResult -
                           \* the token is completionConsumed here. -> recassign (tail keying).
                           /\ slot' = NONE /\ UNCHANGED <<overflowQueue, queuePublished>>
                           /\ headTicket' = headTicket + 1
                           /\ UNCHANGED <<inFlightCount, visibleDeliveryArmItem, visibleHandoffGeneration, completionDispatchTorn>>
                           /\ completionConsumed' = [completionConsumed EXCEPT ![sSeen] = TRUE]
                           /\ fifoViolation' = (fifoViolation \/ (\E jj \in 1..N : jj < sSeen /\ (slot = jj \/ (\E kk \in 1..Len(overflowQueue) : overflowQueue[kk] = jj))))
                      ELSE /\ slot' = NONE /\ UNCHANGED <<overflowQueue, queuePublished>>
                           /\ headTicket' = headTicket + 1
                           /\ UNCHANGED <<inFlightCount, visibleDeliveryArmItem, visibleHandoffGeneration>>
                           /\ completionConsumed' = [completionConsumed EXCEPT ![sSeen] = TRUE]
                           /\ completionDispatchTorn' = [completionDispatchTorn EXCEPT ![sSeen] =
                                        (completionDispatch[sSeen] = "inflight")
                                        \/ (completionCallbackRegistered[sSeen] /\ completionDispatch[sSeen] = "none")]
                           /\ fifoViolation' = (fifoViolation \/ (\E jj \in 1..N : jj < sSeen /\ (slot = jj \/ (\E kk \in 1..Len(overflowQueue) : overflowQueue[kk] = jj))))
                      ELSE /\ UNCHANGED <<slot, overflowQueue, queuePublished, headTicket, inFlightCount,
                                          completionConsumed, completionDispatchTorn, fifoViolation, edgeLockOwner, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed>>
               ELSE IF QueueHead = qSeen
                      THEN IF recover /\ qSeen = 2 /\ ~recoveryAttempted[qSeen]
                      THEN /\ overflowQueue' = SubSeq(overflowQueue, 2, Len(overflowQueue)) /\ UNCHANGED <<slot, queuePublished>>
                           /\ headTicket' = headTicket + 1
                           /\ UNCHANGED <<inFlightCount, visibleDeliveryArmItem, visibleHandoffGeneration, completionDispatchTorn>>
                           /\ completionConsumed' = [completionConsumed EXCEPT ![qSeen] = TRUE]
                           /\ fifoViolation' = (fifoViolation \/ (\E jj \in 1..N : jj < qSeen /\ (slot = jj \/ (\E kk \in 1..Len(overflowQueue) : overflowQueue[kk] = jj))))
                      ELSE /\ overflowQueue' = SubSeq(overflowQueue, 2, Len(overflowQueue)) /\ UNCHANGED <<slot, queuePublished>>
                           /\ headTicket' = headTicket + 1
                           /\ UNCHANGED <<inFlightCount, visibleDeliveryArmItem, visibleHandoffGeneration>>
                           /\ completionConsumed' = [completionConsumed EXCEPT ![qSeen] = TRUE]
                           /\ completionDispatchTorn' = [completionDispatchTorn EXCEPT ![qSeen] =
                                        (completionDispatch[qSeen] = "inflight")
                                        \/ (completionCallbackRegistered[qSeen] /\ completionDispatch[qSeen] = "none")]
                           /\ fifoViolation' = (fifoViolation \/ (\E jj \in 1..N : jj < qSeen /\ (slot = jj \/ (\E kk \in 1..Len(overflowQueue) : overflowQueue[kk] = jj))))
                      ELSE /\ UNCHANGED <<slot, overflowQueue, queuePublished, headTicket, inFlightCount,
                                          completionConsumed, completionDispatchTorn, fifoViolation, edgeLockOwner, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed>>
          \* The implementation's retirement order (RetireItemDeferred): release the
          \* owned activationTurn, run CompleteItem (arbitrary
          \* policy - the LARGE window), only then decrement the store count.
          \* Here: the claim dequeues; trel releases; tdec decrements. The
          \* trel->tdec gap IS the CompleteItem window, independently schedulable.
          /\ UNCHANGED activationTurn
          /\ passPc' = [passPc EXCEPT ![t] =
               IF completionConsumed' = completionConsumed THEN "readFirstTier"
               ELSE LET tgt == IF slot' # slot THEN passSlotSeen[t] ELSE passQueueSeen[t] IN
                    IF recover /\ tgt = 2 /\ ~recoveryAttempted[tgt] THEN "recoveryLock" ELSE "releaseActivationTurn"]
          /\ UNCHANGED <<completionPublished, completionDispatch, completionCallbackRegistered, localDeliveryArmItem,
                         advanceOwner, advancePending, activationPerformed, localHandoffGeneration, handoffClaimed,
                         edgeLockOwner, faultDoubleActivate, faultTokenRead>>
  /\ passSlotSeen' = [passSlotSeen EXCEPT ![t] =
       IF passPc'[t] \in {"releaseActivationTurn", "recoveryLock"} /\ slot' # slot THEN @ ELSE NONE]
  /\ passQueueSeen' = [passQueueSeen EXCEPT ![t] =
       IF passPc'[t] \in {"releaseActivationTurn", "recoveryLock"} /\ overflowQueue' # overflowQueue THEN @ ELSE NONE]
  /\ UNCHANGED <<executorPc, executorItem, openedEmptyEdge, executionKind, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Licensed phantom re-peek (Pipeline.Advance): a count-positive peek miss
\* re-peeks WHILE STILL HOLDING the advance license - the SPSC peek is a
\* mutating consumer op and must stay single-consumer (the release-then-repeek
\* shape is the REJECTED protocol; the post-release repeek tail below remains
\* as the two-sided cover, EdgeLockBail lineage). Visible again -> walk on,
\* still advanceOwner; still invisible -> release-or-serve (the bail).
\* Two-read granularity: the second peek is the same
\* queue-first/slot-second observation protocol as the ordinary walk (TryPeekHead
\* is not one atomic head read) - a producer can publish or escalate BETWEEN the
\* reads, and the queue-first ordering must govern this peek too.
PassPhantomReadFirstTier(t) ==
  /\ passPc[t] = "phantomReadFirstTier"
  /\ passQueueSeen' = [passQueueSeen EXCEPT ![t] = QueueHead] /\ UNCHANGED passSlotSeen
  /\ passPc' = [passPc EXCEPT ![t] = "phantomReadSecondTier"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassPhantomReadSecondTier(t) ==
  /\ passPc[t] = "phantomReadSecondTier"
  /\ passSlotSeen' = [passSlotSeen EXCEPT ![t] = slot] /\ UNCHANGED passQueueSeen
  /\ passPc' = [passPc EXCEPT ![t] = "phantomDecide"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassPhantomDecide(t) ==
  /\ passPc[t] = "phantomDecide"
  /\ IF passSlotSeen[t] # NONE \/ passQueueSeen[t] # NONE
       THEN passPc' = [passPc EXCEPT ![t] = "readFirstTier"]    \* visible again: walk on, still advanceOwner
       ELSE passPc' = [passPc EXCEPT ![t] = "releasePhantomEdge"] \* still phantom: release-or-serve
  /\ passSlotSeen' = [passSlotSeen EXCEPT ![t] = NONE]
  /\ passQueueSeen' = [passQueueSeen EXCEPT ![t] = NONE]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The owned-activationTurn release: AFTER the claim/dequeue, BEFORE the store decrement
\* (the RetireItemDeferred order). The window everything downstream must
\* tolerate is activationTurn-free-while-count-high (the trel->tdec CompleteItem gap) -
\* the benign stale-nonzero direction the code's comments name.
PassReleaseActivationTurn(t) ==
  /\ passPc[t] = "releaseActivationTurn"
  /\ LET tgt == IF passSlotSeen[t] # NONE THEN passSlotSeen[t] ELSE passQueueSeen[t] IN
       activationTurn' = IF activationTurn = tgt \/ (itemGeneration[tgt] # 0 /\ activationTurn = -itemGeneration[tgt])
                 THEN NONE ELSE activationTurn
  /\ UNCHANGED <<passSlotSeen, passQueueSeen>>
  /\ passPc' = [passPc EXCEPT ![t] = "decrementInFlightCount"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>


\* The store-count decrement, AFTER the CompleteItem window (the trel->tdec
\* gap): everything reading inFlightCount in that window sees the RETIRED item still
\* counted with its activationTurn already free - the benign stale-nonzero direction.
PassDecrementInFlightCount(t) ==
  /\ passPc[t] = "decrementInFlightCount"
  /\ inFlightCount' = inFlightCount - 1
  /\ passDecrementEpoch' = [passDecrementEpoch EXCEPT ![t] = handoffFenceEpoch]
  /\ visibleDeliveryArmItem' = localDeliveryArmItem
  /\ passSlotSeen' = [passSlotSeen EXCEPT ![t] = NONE]
  /\ passQueueSeen' = [passQueueSeen EXCEPT ![t] = NONE]
  \* Folded license transition (InFlightStore.DecrementCountAtEdge /
  \* Pipeline.AdvanceDecrement): count stays positive -> keep the license and walk on;
  \* count hits zero WITH a deposit -> consume it atomically, keep the license
  \* (the serve); zero with no deposit releases the license inside the
  \* RMW and the empty-edge handoff requires a fresh TryAcquire - the reacquire gap is
  \* contestable (another fire or committer can win or deposit in it).
  /\ IF inFlightCount' > 0
       THEN /\ UNCHANGED <<advanceOwner, advancePending>>
            /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
       ELSE IF advancePending
       THEN /\ advancePending' = FALSE /\ UNCHANGED advanceOwner
            /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
       ELSE /\ advanceOwner' = "0" /\ UNCHANGED advancePending
            /\ passPc' = [passPc EXCEPT ![t] = "reacquireLicense"]
  /\ UNCHANGED <<slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, itemGeneration, recoveryAttempted>>

\* The post-release reacquire before the empty-edge handoff (the contestable gap): a rival
\* fire/committer may have taken the license or deposited; a loss cedes the walk.
PassTryReacquireLicense(t) ==
  /\ passPc[t] = "reacquireLicense"
  /\ IF advanceOwner = "0"
       THEN /\ advanceOwner' = t /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"] /\ UNCHANGED advancePending
       ELSE \* acquire-or-deposit: the loss transfers the empty-edge handoff
            \* obligation to the rival holder via the pending bit.
            /\ UNCHANGED advanceOwner /\ advancePending' = TRUE /\ passPc' = [passPc EXCEPT ![t] = "off"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* ---- Walk-side recovery (advancer side; monolith RecAssign*/RecAct/RecRetire) ----
\* The claimed head's completion was a fault verdict: position already dequeued
\* (credit held), token completionConsumed by the failure read. The substitution takes the
\* item over IN PLACE under the recovery lock: activationTurn to the item (inherit-or-
\* assign fused - a FOREIGN live activationTurn PARKS this step until it clears), tenure
\* reset (new task: token/completionDispatch/activation reset, fresh registration),
\* recoveryAttempted stamped. Policy reactivation runs outside the hold; recompletion rides
\* the normal tenure machinery (PublishCompletion's recovery disjunct); the second retire
\* returns the advance-owner credit and resumes its claim loop.
RecoveryHead(t) == IF passSlotSeen[t] # NONE THEN passSlotSeen[t] ELSE passQueueSeen[t]

PassAcquireRecoveryLock(t) ==
  /\ passPc[t] = "recoveryLock"
  /\ edgeLockOwner = "0" /\ edgeLockOwner' = t
  /\ passPc' = [passPc EXCEPT ![t] = "recoveryPrepare"]
  /\ UNCHANGED <<passSlotSeen, passQueueSeen>>
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassPrepareRecoveryReplacement(t) ==
  /\ passPc[t] = "recoveryPrepare"
  /\ LET H == RecoveryHead(t) IN
       /\ activationTurn \in {NONE, H, -itemGeneration[H]}
       /\ activationTurn' = H
       /\ completionPublished' = [completionPublished EXCEPT ![H] = FALSE]
       /\ completionConsumed'  = [completionConsumed EXCEPT ![H] = FALSE]
       /\ completionDispatch'  = [completionDispatch EXCEPT ![H] = "none"]
       /\ completionCallbackRegistered'   = [completionCallbackRegistered EXCEPT ![H] = TRUE]
       /\ activationPerformed' = [activationPerformed EXCEPT ![H] = FALSE]
       /\ recoveryAttempted' = [recoveryAttempted EXCEPT ![H] = TRUE]
  /\ edgeLockOwner' = "0"
  /\ passPc' = [passPc EXCEPT ![t] = "recoveryActivate"]
  /\ UNCHANGED <<passSlotSeen, passQueueSeen>>
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionDispatchTorn, localDeliveryArmItem, visibleDeliveryArmItem,
                 advanceOwner, advancePending, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 executorPc, executorItem, openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration>>

PassActivateRecoveryReplacement(t) ==
  /\ passPc[t] = "recoveryActivate"
  /\ LET H == RecoveryHead(t) IN
       /\ faultDoubleActivate' = (faultDoubleActivate \/ activationPerformed[H])
       /\ activationPerformed' = [activationPerformed EXCEPT ![H] = TRUE]
  /\ passPc' = [passPc EXCEPT ![t] = "awaitEmptyEdgeResolution"]
  /\ UNCHANGED <<passSlotSeen, passQueueSeen>>
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, edgeLockOwner,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The substitute's retire: the advanceOwner credit returns.
\* Recovery retirement mirrors the normal split: release
\* the activationTurn + run CompleteItem's window, only THEN the store decrement + fold.
PassRetireRecoveredItem(t) ==
  /\ passPc[t] = "awaitEmptyEdgeResolution"
  /\ LET H == RecoveryHead(t) IN
       /\ completionPublished[H]
       /\ completionConsumed' = [completionConsumed EXCEPT ![H] = TRUE]
       /\ activationTurn' = IF activationTurn = H \/ (itemGeneration[H] # 0 /\ activationTurn = -itemGeneration[H]) THEN NONE ELSE activationTurn
  /\ UNCHANGED <<inFlightCount, visibleDeliveryArmItem, passSlotSeen, passQueueSeen>>
  /\ passPc' = [passPc EXCEPT ![t] = "recoveryDecrement"]
  /\ UNCHANGED <<slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, advanceOwner, advancePending, activationPerformed, edgeLockOwner,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The recovery decrement returns the advance-owner credit and performs the same edge fold.
PassDecrementRecoveredItem(t) ==
  /\ passPc[t] = "recoveryDecrement"
  /\ inFlightCount' = inFlightCount - 1
  /\ passDecrementEpoch' = [passDecrementEpoch EXCEPT ![t] = handoffFenceEpoch]
  /\ visibleDeliveryArmItem' = localDeliveryArmItem
  /\ passSlotSeen' = [passSlotSeen EXCEPT ![t] = NONE]
  /\ passQueueSeen' = [passQueueSeen EXCEPT ![t] = NONE]
  /\ IF inFlightCount' > 0
       THEN /\ UNCHANGED <<advanceOwner, advancePending>>
            /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
       ELSE IF advancePending
       THEN /\ advancePending' = FALSE /\ UNCHANGED advanceOwner
            /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
       ELSE /\ advanceOwner' = "0" /\ UNCHANGED advancePending
            /\ passPc' = [passPc EXCEPT ![t] = "reacquireLicense"]
  /\ UNCHANGED <<slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, edgeLockOwner, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, itemGeneration, recoveryAttempted>>

\* Empty-edge handoff: after the resident count reaches zero, lock, read the visible handoff, consume and assign
\* activationTurn under the lock; the policy activation runs outside the hold; then exit.
PassAcquireEmptyEdgeLock(t) ==
  /\ passPc[t] = "emptyEdgeLock"
  /\ edgeLockOwner = "0" /\ edgeLockOwner' = t
  /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeObserve"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Empty-edge handoff OBSERVE (the lock, when taken, spans observe+take: instruction
\* granularity; a fused check-and-take would hide the relevant interleaving).
PassObserveEmptyEdgeHandoff(t) ==
  /\ passPc[t] = "emptyEdgeObserve"
  \* Recheck while holding the lock (the monolith v1 finding, reproduced by this
  \* composition as the stale-empty-edge handoff face): the idle premise (inFlightCount=0) must be
  \* re-validated inside the lock - an empty-edge handoff entered from a stale zero-count observation
  \* observation must self-neutralize, or it takes a LATER epoch's handoff
  \* and tramples a live activationTurn.
  \* OBSERVATION (relational Dekker): a retirement pass whose decrement-RMW postdates the
  \* dispatcher's barrier epoch is synchronized and reads the true word; one
  \* whose RMW predates it reads the visible (possibly stale) copy.
  /\ LET obs == IF passDecrementEpoch[t] >= handoffFenceEpoch /\ handoffFenceEpoch > 0
                  THEN localHandoffGeneration ELSE visibleHandoffGeneration IN
     IF inFlightCount = 0 /\ obs # NONE /\ ~handoffClaimed
       THEN /\ passEmptyEdgeHandoffGeneration' = [passEmptyEdgeHandoffGeneration EXCEPT ![t] = obs]      \* generation pin
            /\ passEmptyEdgeHandoffItem' = [passEmptyEdgeHandoffItem EXCEPT ![t] = handoffItem]
            /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeTurnClaim"]
       ELSE /\ UNCHANGED <<passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem>>
            /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeUnlock"]
  /\ UNCHANGED edgeLockOwner
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassReleaseEmptyEdgeLock(t) ==
  /\ passPc[t] = "emptyEdgeUnlock"
  /\ edgeLockOwner' = "0"
  /\ passPc' = [passPc EXCEPT ![t] = "exitPass"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationTurn, activationPerformed,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate, executorPc, executorItem,
                 openedEmptyEdge, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* Empty-edge handoff take: TryClaimProvisionalTurn declines any live activation turn.
PassClaimEmptyEdgeTurn(t) ==
  /\ passPc[t] = "emptyEdgeTurnClaim"
  /\ IF activationTurn # NONE
       THEN \* Layer 2 claims only if free: nothing taken, nothing trampled.
            /\ UNCHANGED <<activationTurn, handoffClaimed, localHandoffGeneration, visibleHandoffGeneration, handoffItem>>
            /\ edgeLockOwner' = "0"
            /\ passPc' = [passPc EXCEPT ![t] = "exitPass"]
       ELSE \* CLAIM the activationTurn as -(pinned gen) FIRST (the shipped order), then the
            \* generation-pinned consume as its own step - the plain reclaim can interleave
            \* between them (the double-bail class).
            /\ activationTurn' = -passEmptyEdgeHandoffGeneration[t]
            /\ UNCHANGED <<handoffClaimed, localHandoffGeneration, visibleHandoffGeneration, handoffItem>>
            /\ UNCHANGED edgeLockOwner
            /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeHandoffTake"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationPerformed,
                 executionKind, faultDoubleActivate, executorPc, executorItem, openedEmptyEdge, passSlotSeen, passQueueSeen,
                 fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The empty-edge handoff's gen-pinned consume, SEPARATE from its claim (double-bail window).
PassTakeEmptyEdgeHandoff(t) ==
  /\ passPc[t] = "emptyEdgeHandoffTake"
  /\ IF localHandoffGeneration = passEmptyEdgeHandoffGeneration[t] /\ localHandoffGeneration # NONE
       THEN \* consume wins for the pinned generation
            /\ handoffClaimed' = TRUE
            /\ localHandoffGeneration' = NONE /\ visibleHandoffGeneration' = NONE /\ handoffItem' = NONE
            /\ edgeLockOwner' = "0"
            /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeActivate"]
            /\ UNCHANGED activationTurn
       ELSE \* consume LOST (recycled epoch under pin, or the reclaim ate it):
            \* release the -gen claim, CAS-if-mine.
            /\ activationTurn' = IF activationTurn = -passEmptyEdgeHandoffGeneration[t] THEN NONE ELSE activationTurn
            /\ UNCHANGED <<handoffClaimed, localHandoffGeneration, visibleHandoffGeneration, handoffItem>>
            /\ edgeLockOwner' = "0"
            /\ passPc' = [passPc EXCEPT ![t] = "exitPass"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, activationPerformed,
                 executionKind, faultDoubleActivate, executorPc, executorItem, openedEmptyEdge, passSlotSeen, passQueueSeen,
                 fifoViolation, faultTokenRead, lateCompletionCallback, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassActivateEmptyEdgeItem(t) ==
  /\ passPc[t] = "emptyEdgeActivate"
  /\ emptyEdgeActivationBusy' = TRUE          \* ActivateHeadItem entered (returns in the next step)
  /\ LET g == passEmptyEdgeHandoffItem[t] IN
     /\ faultDoubleActivate' = (faultDoubleActivate \/ (g # NONE /\ activationPerformed[g]))
     /\ activationPerformed' = IF g # NONE THEN [activationPerformed EXCEPT ![g] = TRUE] ELSE activationPerformed
  /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeRecordResolution"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, executorPc, executorItem, openedEmptyEdge,
                 passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead, lateCompletionCallback, handoffItem,
                 generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

\* The policy call returns; the resolution stamp is written afterward -
\* "that ordering, not vacuity, is what makes the substitute's activation safe".
PassRecordEmptyEdgeResolution(t) ==
  /\ UNCHANGED <<handoffFenceEpoch, passDecrementEpoch>>
  /\ passPc[t] = "emptyEdgeRecordResolution"
  /\ emptyEdgeActivationBusy' = FALSE
  /\ resolvedEmptyEdgeGeneration' = passEmptyEdgeHandoffGeneration[t]
  /\ passPc' = [passPc EXCEPT ![t] = "exitPass"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed,
                 completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, visibleDeliveryArmItem, advanceOwner, advancePending, edgeLockOwner, activationTurn,
                 activationPerformed, executionKind, localHandoffGeneration, visibleHandoffGeneration, handoffClaimed, faultDoubleActivate,
                 executorPc, executorItem, openedEmptyEdge, passSlotSeen, passQueueSeen, fifoViolation, faultTokenRead,
                 lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem,
                 faultConcurrentActivation, itemGeneration, recoveryAttempted>>

\* Bail: release-or-serve, then the one-shot re-peek (the two-sided protocol).
PassReleaseAfterPhantomEdge(t) ==
  /\ passPc[t] = "releasePhantomEdge"
  /\ visibleDeliveryArmItem' = localDeliveryArmItem /\ visibleHandoffGeneration' = localHandoffGeneration
  /\ IF advancePending
       THEN /\ advancePending' = FALSE /\ advanceOwner' = advanceOwner
            /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
       ELSE /\ advanceOwner' = "0" /\ advancePending' = advancePending
            /\ passPc' = [passPc EXCEPT ![t] = "off"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, executorPc, executorItem, passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

PassExit(t) ==
  /\ passPc[t] = "exitPass"
  /\ visibleDeliveryArmItem' = localDeliveryArmItem /\ visibleHandoffGeneration' = localHandoffGeneration
  /\ IF advanceOwner = t
       THEN IF advancePending
              THEN /\ advancePending' = FALSE /\ advanceOwner' = advanceOwner /\ passPc' = [passPc EXCEPT ![t] = "readFirstTier"]
              ELSE /\ advanceOwner' = "0" /\ advancePending' = advancePending /\ passPc' = [passPc EXCEPT ![t] = "off"]
       ELSE /\ UNCHANGED <<advanceOwner, advancePending>> /\ passPc' = [passPc EXCEPT ![t] = "off"]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, completionPublished, completionDispatch, completionConsumed, completionDispatchTorn, completionCallbackRegistered, localDeliveryArmItem, executorPc, executorItem, passSlotSeen, passQueueSeen, fifoViolation, openedEmptyEdge, edgeLockOwner, activationTurn, activationPerformed, executionKind, localHandoffGeneration, handoffClaimed, faultDoubleActivate, faultTokenRead, lateCompletionCallback, handoffItem, generationCounter, executorGeneration, passEmptyEdgeHandoffGeneration, passEmptyEdgeHandoffItem, emptyEdgeActivationBusy, resolvedEmptyEdgeGeneration, faultConcurrentActivation, handoffFenceEpoch, passDecrementEpoch, itemGeneration, recoveryAttempted>>

AllDone ==
  \* completionConsumed-all, not headTicket=N: direct-retired items never consume a FIFO
  \* position (the naked path bypasses the store entirely).
  /\ (\A i \in 1..N : completionConsumed[i]) /\ executorPc = "off"
  /\ \A t \in RetirementPasses : passPc[t] = "off"
  /\ \A i \in 1..N : completionDispatch[i] \in {"none", "acquired"}
  /\ localHandoffGeneration = NONE
Finished == AllDone /\ UNCHANGED vars

Next ==
  \/ ExecutorDispatch \/ ExecutorClaimElidedTurn \/ ExecutorActivateElidedItem \/ ExecutorTakeOwnHandoff \/ ExecutorDirectRetire \/ ExecutorFenceHandoffPublication \/ PropagateHandoff \/ ExecutorReadInFlightCount \/ ExecutorAwaitHandoffResolution \/ ExecutorAcquireSelfClaimLock \/ ExecutorObserveSelfClaim \/ ExecutorClaimProvisionalTurn \/ ExecutorTakeSelfHandoff \/ ExecutorActivateSelfClaimedItem
  \/ ExecutorDiscoverRecovery \/ ExecutorReclaimFailedHandoff \/ ExecutorAwaitEmptyEdgeResolution \/ ExecutorActivateRecovery \/ ExecutorCommitInheritedRecoveryTurn \/ ExecutorRepublishRecoveryHandoff \/ ExecutorResolvePriorHandoff \/ StoreIncrementInFlightCount \/ StorePublishCommittedItem \/ ExecutorPrepareEmptyEdgeActivation \/ ExecutorAcquireAdvanceLicense \/ ExecutorAssignAndPublishAtEmptyEdge \/ ExecutorAttachEmptyEdgeCallback \/ ExecutorReleaseAdvanceLicense \/ ExecutorAcquireEmptyEdgeLock \/ ExecutorAssignAtEmptyEdge \/ ExecutorActivateEmptyEdgeItem \/ ExecutorReadAdvanceLicense \/ ExecutorFinishOwnedPass \/ ExecutorNext
  \/ \E i \in 1..N : PublishCompletion(i) \/ BeginCompletionDispatch(i) \/ DeliverCompletionCallback(i) \/ CompletionCallbackAcquireOrDeposit(i) \/ ReclaimCompletionCallback(i)
  \/ PropagateDeliveryArmClear
  \/ \E t \in RetirementPasses : PassReadFirstStoreTier(t) \/ PassReadSecondStoreTier(t) \/ PassDecideHeadClaim(t) \/ PassReleaseAfterPhantomEdge(t) \/ PassPhantomReadFirstTier(t) \/ PassPhantomReadSecondTier(t) \/ PassPhantomDecide(t) \/ PassReleaseActivationTurn(t) \/ PassDecrementInFlightCount(t) \/ PassTryReacquireLicense(t) \/ PassDecrementRecoveredItem(t)
                          \/ PassAcquireRecoveryLock(t) \/ PassPrepareRecoveryReplacement(t) \/ PassActivateRecoveryReplacement(t) \/ PassRetireRecoveredItem(t)
                          \/ PassExit(t)
                          \/ PassAcquireEmptyEdgeLock(t) \/ PassObserveEmptyEdgeHandoff(t) \/ PassReleaseEmptyEdgeLock(t) \/ PassClaimEmptyEdgeTurn(t) \/ PassTakeEmptyEdgeHandoff(t) \/ PassActivateEmptyEdgeItem(t) \/ PassRecordEmptyEdgeResolution(t)
  \/ Finished

Spec == Init /\ [][Next]_vars

(* ------------------------------ properties -------------------------------- *)
\* Composed AOORE property (ItemTenure's theorem with its relies discharged).
CompletionDispatchNeverTorn == \A i \in 1..N : ~completionDispatchTorn[i]

\* The composed July-9 face (InFlightStore's theorem under the real license).
ItemsRetireInFifoOrder == ~fifoViolation

\* Staleness soundness (ItemTenure's lemma at composition scope).
DeliveryArmVisibilityCanOnlyRetainStaleSet ==
  (visibleDeliveryArmItem # localDeliveryArmItem) => localDeliveryArmItem = 0

\* Count never under-runs (increment-first at composition scope).
InFlightCountNonNegative == inFlightCount >= 0

\* Single-armed invariant (the arm names an unretired tenure).
DeliveryArmNamesUnretiredItem == localDeliveryArmItem # 0 => localDeliveryArmItem > headTicket

\* The policy contract: at-most-once activation (assign->activate gap included).
ItemActivatedAtMostOnce == ~faultDoubleActivate

\* Turn coherence: the activationTurn, when advanceOwner, names an unretired item - EXCEPT inside
\* the straggler window (count decremented, owned-activationTurn release pending): there the
\* activationTurn legitimately names the just-retired item until the retiring retirement pass's trel
\* step lands. The window is bounded: exactly one retirement pass holds it as its release
\* obligation. Everything the invariant guarded still holds outside the window.
ActivationTurnNamesLiveTenure ==
  activationTurn > 0 =>
    \/ activationTurn > headTicket
    \* pending release: the retiring retirement pass holds the release obligation between
    \* its claim (dequeue) and trel - the activationTurn legitimately names the dequeued item.
    \/ \E t \in RetirementPasses :
         /\ passPc[t] = "releaseActivationTurn"
         /\ (passSlotSeen[t] = activationTurn \/ passQueueSeen[t] = activationTurn)
    \* the walk-recovery episode: the substitute holds the activationTurn on its DEQUEUED
    \* position by design (in-place takeover, credit advanceOwner until the re-retire).
    \/ \E t \in RetirementPasses :
         /\ passPc[t] \in {"recoveryLock", "recoveryPrepare", "recoveryActivate", "awaitEmptyEdgeResolution"}
         /\ (passSlotSeen[t] = activationTurn \/ passQueueSeen[t] = activationTurn)

\* Handoff visibility (composed Dekker): local/visible divergence is one-sided.
HandoffVisibilityCannotNameDifferentGeneration == (visibleHandoffGeneration # localHandoffGeneration) => (visibleHandoffGeneration = NONE \/ localHandoffGeneration = NONE)

\* The task contract: no attach ever touches a completionConsumed token (v2 TaskContractRespected).
CompletionNeverReadsRetiredTenure == ~faultTokenRead

\* The recovery-sequencing contract: no substitute activation overlaps an
\* in-flight empty-edge handoff policy call.
RecoveryDoesNotOverlapActivation == ~faultConcurrentActivation

\* Reachability probe: STRAGGLER FIRES happen — a fire is CAUSED by completion,
\* but its execution tail can land after the claim retired the tenure
\* (registration is irrevocable; retire cannot unregister). "Post-retire fire"
\* retired as a name: it misattributed the cause.
CompletionCallbackNeverOutlivesTenure == ~lateCompletionCallback

\* Non-vacuity check for the recovery paths included by the canonical model.
RecoveryIsUnreachable == \A i \in 1..N : ~recoveryAttempted[i]

=============================================================================
