-------------------------- MODULE ActivationGate --------------------------
(* Activation-turn component contract.

   The dispatcher publishes an activation handoff, fences that publication,
   then reads the in-flight count. A retirement pass that reaches the transition
   to zero resident items observes the handoff from the other side. Exactly one
   side resolves it:

     dispatcher:      publish handoff -> fence -> read resident count
     retirement pass: decrement count  -> claim empty-edge handoff

   If the dispatcher observes zero it may self-claim under the edge lock. If the
   final retirement pass observes the handoff it resolves the empty-edge handoff
   under the same lock. Generation/turn arbitration prevents both paths from
   activating the item.

   The model also includes resident-head activation and phantom-edge release.
   Storage tiers and completion dispatch are abstract relies owned by
   InFlightStore and ItemTenure. Recovery is composed in Pipeline. *)
EXTENDS Integers, Sequences, TLC

CONSTANT N

Items == 1..N
NONE == 0
RetirementPasses == {"W", "P", "E"}

VARIABLES
    executionKind,      \* [Items -> {"self","gated"}] gated completes only once activationPerformed
    storePublished,   \* [Items -> BOOL] store-visible (the enqueue landed)
    activationPerformed, completionPublished, retirementCompleted,  \* [Items -> BOOL]
    completionCallbackRegistered,     \* [Items -> BOOL] one-shot trampoline registered (set at activation/attach)
    store,       \* FIFO of storePublished items
    inFlightCount,         \* the count (increment-FIRST: over-promises the store)
    advanceOwner, advancePending,  \* the walk license (acquire-or-flag / release-or-serve)
    edgeLockOwner,       \* the edge lock: "0" | retirement pass id
    publisherPc,         \* "next" | "acquireEmptyEdgeLock" | "publishResidentItem" | "readAdvanceLicense" | "finishRetirementPass" | "finished"
    publisherItem,         \* the item the executor is committing
    passPc,         \* [RetirementPasses -> "off" | "claimHead" | "activateResident" | "emptyEdgeHandoff" | "recheckAfterRelease"]
    passHead,       \* [RetirementPasses -> item] the stop's target
    duplicateActivation,
    localActivationHandoff,    \* dispatcher-local placement (writer view)
    visibleActivationHandoff,  \* globally visible placement (weak until fence/propagate)
    handoffClaimed,  \* the published handoff was taken
    dispatcherPc          \* dispatcher: "idle"|"handoffPublished"|"readInFlightCount"|"acquireSelfClaimLock"|"observeSelfClaim"|"finished"

vars == <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
          edgeLockOwner, publisherPc, publisherItem, passPc, passHead, duplicateActivation,
          localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

InFlightHead == IF store = <<>> THEN NONE ELSE store[1]

Init ==
    /\ executionKind \in [Items -> {"self","gated"}]
    /\ storePublished = [i \in Items |-> FALSE] /\ activationPerformed = [i \in Items |-> FALSE]
    /\ completionPublished = [i \in Items |-> FALSE] /\ retirementCompleted = [i \in Items |-> FALSE]
    /\ completionCallbackRegistered = [i \in Items |-> FALSE]
    /\ store = <<>> /\ inFlightCount = 0 /\ advanceOwner = "0" /\ advancePending = FALSE /\ edgeLockOwner = "0"
    /\ publisherPc = "next" /\ publisherItem = 0
    /\ passPc = [t \in RetirementPasses |-> "off"] /\ passHead = [t \in RetirementPasses |-> NONE]
    /\ duplicateActivation = FALSE
    /\ localActivationHandoff = FALSE /\ visibleActivationHandoff = FALSE /\ handoffClaimed = FALSE /\ dispatcherPc = "idle"

-------------------------------------------------------------------------------
(* Task completions and fires. *)

PublishTaskCompletion(i) ==
    /\ storePublished[i] /\ ~completionPublished[i]
    /\ (executionKind[i] = "gated") => activationPerformed[i]
    /\ completionPublished' = [completionPublished EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
                   edgeLockOwner, publisherPc, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

RunCompletionCallback(t, i) ==
    /\ t \in {"W","P"} /\ passPc[t] = "off"
    /\ completionPublished[i] /\ completionCallbackRegistered[i] /\ ~retirementCompleted[i]
    /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![i] = FALSE]     \* one-shot
    /\ IF advanceOwner = "0"
         THEN /\ advanceOwner' = t /\ advancePending' = advancePending
              /\ passPc' = [passPc EXCEPT ![t] = "claimHead"]
         ELSE /\ advancePending' = TRUE /\ advanceOwner' = advanceOwner
              /\ UNCHANGED passPc
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, store, inFlightCount,
                   edgeLockOwner, publisherPc, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

-------------------------------------------------------------------------------
(* The walk (licensed). Claim fuses dequeue + retire + decrement (the count is
   decremented per retirement; the edge/phantom split reads it afterwards). *)

PassTryClaimHead(t) ==
    /\ passPc[t] = "claimHead"
    /\ IF InFlightHead # NONE /\ completionPublished[InFlightHead]
         THEN /\ retirementCompleted' = [retirementCompleted EXCEPT ![InFlightHead] = TRUE]
              /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![InFlightHead] = FALSE]
              /\ store' = SubSeq(store, 2, Len(store))
              /\ inFlightCount' = inFlightCount - 1
              /\ UNCHANGED <<passPc, passHead, advanceOwner, advancePending>>
         ELSE IF InFlightHead # NONE
           THEN \* incomplete visible head: the stop (lock-scoped activation).
                /\ passHead' = [passHead EXCEPT ![t] = InFlightHead]
                /\ passPc' = [passPc EXCEPT ![t] = "activateResident"]
                /\ UNCHANGED <<retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending>>
         ELSE IF inFlightCount = 0
           THEN \* count reached zero: perform the locked empty-edge handoff (contents out of scope here).
                /\ passPc' = [passPc EXCEPT ![t] = "emptyEdgeHandoff"]
                /\ UNCHANGED <<retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending, passHead>>
         ELSE \* Phantom edge observed (counted but not yet storePublished). The bail's
              \* release is a SEPARATE step (the real instruction boundary - the
              \* committer's publish+word-read can land inside the gap; that gap is
              \* exactly what the two-sided protocol must survive).
              /\ passPc' = [passPc EXCEPT ![t] = "releasePhantomEdge"]
              /\ UNCHANGED <<retirementCompleted, completionCallbackRegistered, store, inFlightCount, passHead, advanceOwner, advancePending>>
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, edgeLockOwner, publisherPc, publisherItem, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* The bail's release-or-serve: a served deposit re-probes (licensed); a true
\* release proceeds to the one-shot re-peek.
PassReleasePhantomEdge(t) ==
    /\ passPc[t] = "releasePhantomEdge"
    /\ IF advancePending
         THEN /\ advancePending' = FALSE /\ advanceOwner' = advanceOwner
              /\ passPc' = [passPc EXCEPT ![t] = "claimHead"]
         ELSE /\ advanceOwner' = "0" /\ advancePending' = advancePending
              /\ passPc' = [passPc EXCEPT ![t] = "off"]
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   edgeLockOwner, publisherPc, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* The stop: lock-scoped activation of the incomplete head. The lock section
\* rechecks activation before assigning the turn.
PassAcquireResidentActivationLock(t) ==
    /\ passPc[t] = "activateResident"
    /\ edgeLockOwner = "0" /\ edgeLockOwner' = t
    /\ passPc' = [passPc EXCEPT ![t] = "activateResidentUnderLock"]
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, publisherPc, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

PassActivateResidentHead(t) ==
    /\ passPc[t] = "activateResidentUnderLock"
    /\ LET H == passHead[t] IN
       /\ IF InFlightHead # H \/ retirementCompleted[H]
            THEN \* claimed through under the stop: whoever took it carries. Exit.
                 /\ passPc' = [passPc EXCEPT ![t] = "exit"]
                 /\ UNCHANGED <<activationPerformed, completionCallbackRegistered, duplicateActivation>>
          ELSE IF completionPublished[H]
            THEN \* completed UNDER the stop (the CompletedWhileResident recheck): a
                 \* completed head is the walk's to claim - back to the loop top.
                 /\ passPc' = [passPc EXCEPT ![t] = "claimHead"]
                 /\ UNCHANGED <<activationPerformed, completionCallbackRegistered, duplicateActivation>>
          ELSE IF ~activationPerformed[H]
            THEN /\ duplicateActivation' = (duplicateActivation \/ activationPerformed[H])
                 /\ activationPerformed' = [activationPerformed EXCEPT ![H] = TRUE]
                 /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![H] = TRUE]
                 /\ passPc' = [passPc EXCEPT ![t] = "exit"]
          ELSE \* already activationPerformed (a prior tenure's completionCallbackRegistered may be consumed): re-bind.
                 /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![H] = TRUE]
                 /\ passPc' = [passPc EXCEPT ![t] = "exit"]
                 /\ UNCHANGED <<activationPerformed, duplicateActivation>>
       /\ edgeLockOwner' = "0"
       /\ UNCHANGED <<executionKind, storePublished, completionPublished, retirementCompleted, store, inFlightCount, advanceOwner, advancePending, publisherPc, publisherItem, passHead, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* Empty-edge handoff: under the edge lock, read the visible placement and take it.
PassAcquireEmptyEdgeHandoffLock(t) ==
    /\ passPc[t] = "emptyEdgeHandoff"
    /\ edgeLockOwner = "0" /\ edgeLockOwner' = t
    /\ passPc' = [passPc EXCEPT ![t] = "observeEmptyEdgeHandoff"]
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, publisherPc, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* Empty-edge handoff, step 1: OBSERVE the visible placement. Claiming its turn is
\* a separate step; the lock spans both, while bare operations may interleave.
PassObserveEmptyEdgeHandoff(t) ==
    /\ passPc[t] = "observeEmptyEdgeHandoff"
    /\ IF visibleActivationHandoff
         THEN /\ passPc' = [passPc EXCEPT ![t] = "resolveEmptyEdgeHandoff"]
              /\ UNCHANGED edgeLockOwner
         ELSE /\ passPc' = [passPc EXCEPT ![t] = "exit"]
              /\ edgeLockOwner' = "0"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, publisherPc, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff,
                   handoffClaimed, dispatcherPc>>

\* Empty-edge handoff, step 2: claim and clear the handoff, then unlock.
PassResolveEmptyEdgeHandoff(t) ==
    /\ passPc[t] = "resolveEmptyEdgeHandoff"
    /\ duplicateActivation' = (duplicateActivation \/ handoffClaimed)
    /\ handoffClaimed' = TRUE
    /\ localActivationHandoff' = FALSE /\ visibleActivationHandoff' = FALSE
    /\ edgeLockOwner' = "0"
    /\ passPc' = [passPc EXCEPT ![t] = "exit"]
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, publisherPc, publisherItem, passHead, dispatcherPc>>

\* Exit: release-or-serve.
PassExit(t) ==
    /\ passPc[t] = "exit"
    /\ IF advanceOwner = t
         THEN IF advancePending
                THEN /\ advancePending' = FALSE /\ advanceOwner' = advanceOwner
                     /\ passPc' = [passPc EXCEPT ![t] = "claimHead"]
                ELSE /\ advanceOwner' = "0" /\ advancePending' = advancePending
                     /\ passPc' = [passPc EXCEPT ![t] = "off"]
         ELSE /\ UNCHANGED <<advanceOwner, advancePending>>
              /\ passPc' = [passPc EXCEPT ![t] = "off"]
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   edgeLockOwner, publisherPc, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

-------------------------------------------------------------------------------
(* The executor strand: increment-first commits. *)

PublisherIncrementInFlightCount ==
    /\ publisherPc = "next"
    /\ \E i \in Items :
         /\ ~storePublished[i] /\ i = publisherItem + 1     \* FIFO dispatch, publisherItem tracks progress
         /\ publisherItem' = i
         /\ inFlightCount' = inFlightCount + 1
         /\ publisherPc' = IF inFlightCount = 0 THEN "acquireEmptyEdgeLock" ELSE "publishResidentItem"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store,
                   advanceOwner, advancePending, edgeLockOwner, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* prev == 0: the edge-lock section - publish, activate own, attach, unlock.
PublisherAcquireEmptyEdgeLock ==
    /\ publisherPc = "acquireEmptyEdgeLock" /\ edgeLockOwner = "0"
    /\ edgeLockOwner' = "E"
    /\ publisherPc' = "publishAtEmptyEdge"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

PublisherPublishAtEmptyEdge ==
    /\ publisherPc = "publishAtEmptyEdge"
    /\ storePublished' = [storePublished EXCEPT ![publisherItem] = TRUE]
    /\ store' = Append(store, publisherItem)
    /\ duplicateActivation' = (duplicateActivation \/ activationPerformed[publisherItem])
    /\ activationPerformed' = [activationPerformed EXCEPT ![publisherItem] = TRUE]
    /\ completionCallbackRegistered' = [completionCallbackRegistered EXCEPT ![publisherItem] = TRUE]
    /\ edgeLockOwner' = "0"
    /\ publisherPc' = "next"
    /\ UNCHANGED <<executionKind, completionPublished, retirementCompleted, inFlightCount, advanceOwner, advancePending, publisherItem, passPc, passHead, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* prev > 0: publish outside the lock...
PublisherPublishResidentItem ==
    /\ publisherPc = "publishResidentItem"
    /\ storePublished' = [storePublished EXCEPT ![publisherItem] = TRUE]
    /\ store' = Append(store, publisherItem)
    /\ publisherPc' = "readAdvanceLicense"
    /\ UNCHANGED <<executionKind, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, inFlightCount, advanceOwner, advancePending,
                   edgeLockOwner, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

\* ...then acquire-or-deposit: a bailed-out (or absent) retirement pass lets the
\* publisher acquire; a live owner receives a pending re-probe obligation.
PublisherReadAdvanceLicense ==
    /\ publisherPc = "readAdvanceLicense"
    /\ IF advanceOwner = "0"
         THEN /\ advanceOwner' = "E" /\ advancePending' = advancePending
              /\ passPc' = [passPc EXCEPT !["E"] = "claimHead"]
              /\ publisherPc' = "ewalk"
         ELSE /\ UNCHANGED <<advanceOwner, passPc>>
              /\ advancePending' = TRUE
              /\ publisherPc' = "next"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   edgeLockOwner, publisherItem, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

PublisherFinishRetirementPass ==
    /\ publisherPc = "ewalk" /\ passPc["E"] = "off"
    /\ publisherPc' = "next"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, edgeLockOwner, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

PublisherFinish ==
    /\ publisherPc = "next" /\ publisherItem = N
    /\ publisherPc' = "finished"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, edgeLockOwner, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed, dispatcherPc>>

-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
(* The dispatcher: the handoff-turn Dekker (place -> fence -> count read). *)

DispatcherPublishHandoff ==
    /\ dispatcherPc = "idle"
    /\ localActivationHandoff' = TRUE /\ visibleActivationHandoff' = visibleActivationHandoff      \* WEAK place
    /\ dispatcherPc' = "handoffPublished"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, edgeLockOwner, publisherPc, publisherItem, passPc, passHead, duplicateActivation, handoffClaimed>>

DispatcherFenceHandoffPublication ==
    /\ dispatcherPc = "handoffPublished"
    /\ visibleActivationHandoff' = localActivationHandoff /\ dispatcherPc' = "readInFlightCount"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount,
                   advanceOwner, advancePending, edgeLockOwner, publisherPc, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, handoffClaimed>>

PropagateHandoff ==
    /\ visibleActivationHandoff # localActivationHandoff
    /\ visibleActivationHandoff' = localActivationHandoff
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
                   edgeLockOwner, publisherPc, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, handoffClaimed, dispatcherPc>>

DispatcherReadInFlightCount ==
    /\ dispatcherPc = "readInFlightCount"
    /\ dispatcherPc' = IF inFlightCount = 0 THEN "acquireSelfClaimLock" ELSE "finished"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
                   edgeLockOwner, publisherPc, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed>>

DispatcherAcquireSelfClaimLock ==
    /\ dispatcherPc = "acquireSelfClaimLock"
    /\ edgeLockOwner = "0" /\ edgeLockOwner' = "D"
    /\ dispatcherPc' = "observeSelfClaim"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
                   publisherPc, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed>>

\* Self-claim, step 1: OBSERVE own placement (empty-edge handoff may have taken it).
DispatcherObserveSelfClaim ==
    /\ dispatcherPc = "observeSelfClaim"
    /\ IF localActivationHandoff
         THEN /\ dispatcherPc' = "resolveSelfClaim" /\ UNCHANGED edgeLockOwner
         ELSE /\ dispatcherPc' = "finished"
              /\ edgeLockOwner' = "0"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
                   publisherPc, publisherItem, passPc, passHead, duplicateActivation, localActivationHandoff, visibleActivationHandoff, handoffClaimed>>

\* Self-claim, step 2: the TAKE, then unlock.
DispatcherResolveSelfClaim ==
    /\ dispatcherPc = "resolveSelfClaim"
    /\ duplicateActivation' = (duplicateActivation \/ handoffClaimed)
    /\ handoffClaimed' = TRUE
    /\ localActivationHandoff' = FALSE /\ visibleActivationHandoff' = FALSE
    /\ edgeLockOwner' = "0"
    /\ dispatcherPc' = "finished"
    /\ UNCHANGED <<executionKind, storePublished, activationPerformed, completionPublished, retirementCompleted, completionCallbackRegistered, store, inFlightCount, advanceOwner, advancePending,
                   publisherPc, publisherItem, passPc, passHead>>

-------------------------------------------------------------------------------

AllRetired == (\A i \in Items : retirementCompleted[i])
              /\ (dispatcherPc \in {"idle", "finished"}) /\ (dispatcherPc = "finished" => handoffClaimed)
Finished == AllRetired /\ UNCHANGED vars

Next ==
    \/ \E i \in Items : PublishTaskCompletion(i)
    \/ \E t \in {"W","P"}, i \in Items : RunCompletionCallback(t, i)
    \/ \E t \in RetirementPasses :
         PassTryClaimHead(t) \/ PassReleasePhantomEdge(t) \/ PassAcquireResidentActivationLock(t) \/ PassActivateResidentHead(t)
         \/ PassAcquireEmptyEdgeHandoffLock(t) \/ PassObserveEmptyEdgeHandoff(t) \/ PassResolveEmptyEdgeHandoff(t) \/ PassExit(t)
    \/ PublisherIncrementInFlightCount \/ PublisherAcquireEmptyEdgeLock \/ PublisherPublishAtEmptyEdge \/ PublisherPublishResidentItem \/ PublisherReadAdvanceLicense \/ PublisherFinishRetirementPass \/ PublisherFinish
    \/ DispatcherPublishHandoff \/ DispatcherFenceHandoffPublication \/ PropagateHandoff \/ DispatcherReadInFlightCount \/ DispatcherAcquireSelfClaimLock \/ DispatcherObserveSelfClaim \/ DispatcherResolveSelfClaim
    \/ Finished

Spec == Init /\ [][Next]_vars

-------------------------------------------------------------------------------
(* INVARIANTS *)

ActivationOccursAtMostOnce == ~duplicateActivation

AtMostOneActivatedItemResident ==
    \A i, j \in Items :
        (activationPerformed[i] /\ ~retirementCompleted[i] /\ activationPerformed[j] /\ ~retirementCompleted[j]) => i = j

PendingAdvanceHasOwner == advancePending => advanceOwner # "0"

EdgeLockOwnerValid == edgeLockOwner \in {"0", "D"} \cup RetirementPasses   \* structural (typing)

InFlightCountNonNegative == inFlightCount >= 0   \* over-promise never under-runs: no -1 skew exists

\* NO-ORPHAN, by deadlock detection (the EdgeLockBail discipline): Finished is the
\* The only self-loop requires every item retirementCompleted and the handoff turn
\* delivered (dispatcherPc="finished" => handoffClaimed). Any quiet state short of that - a completionPublished
\* head nobody will claim, an undelivered visible place with the chain drained -
\* has no enabled action and TLC reports DEADLOCK. A state-predicate NoOrphan
\* (the NoOrphanWalk form) is unsound at this scope: "all actors quiet" is not
\* "no wake source exists" while future completions can relight the walk; the
\* richer structural form graduates to the composition.

================================================================================
