--------------------------- MODULE WaitProtocol ---------------------------
(*
WakeSignal's wait protocol (the TryGetNext/WaitForNextAsync pull seam) - the
thin path of WaitForNextAwaitable - plus the sync-handoff rendezvous primitive
(WakeSignal.WaitForSuspended) that PgClientFlowSource's EnqueueSyncWithHandoff
rides. Models UnboundedQueueSource.Enumerator + WakeSignal against a single
async producer (SPSC contract) and a single sync-handoff producer (Pg's
SyncWaiterLock baton serializes concurrent sync producers, so one suffices).

Code shape (Pipeline.cs executor, UnboundedQueueSource.WaitForNextAsync,
WakeSignal.Arm/WaitOnCompleted/SignalCore wait path, PgClientFlowSource
EnqueueSyncWithHandoff):

  Consumer pull:  TryGetNext = lock-free SPSC dequeue. On miss:
                  WaitForNextAsync: ACQUIRE wake lock; NotEmpty := FALSE;
                  re-check (queue empty && !NotEmpty)
                    -> retry (release, immediate)        [WaitRecheckRetry]
                    -> completed (release, done)         [WaitRecheckCompleted]
                    -> arm: _pending := TRUE, LOCK HELD  [WaitArm]
                  awaiter registration: store continuation, RELEASE lock,
                  set the suspended signal               [WaitRegister]
  Async producer: NotEmpty := TRUE; queue.Enqueue (publish); then Signal:
                  acquire lock, claim _pending (consume), clear the suspended
                  signal, release, invoke the stored continuation.
  Sync handoff:   publish the flow into the handoff slot; WaitForSuspended
                  (block on the suspended signal); Signal inline - the claim
                  consumes the wait and the executor runs the flow ON THE
                  PRODUCER'S THREAD before EnqueueSyncWithHandoff returns
                  (modeled atomically: claim-inline consumes the handoff).
                  The executor cannot snipe the handoff slot outside this
                  rendezvous (the HandoffAcked gate in Pg) - hence no direct
                  consumer action on the slot.
  Complete:       cancel + the same Signal claim path.

Load-bearing mechanisms, each with a witness toggle:

  RecheckFix      - the under-lock re-check before arming. Without it, an item
                    published between the lock-free miss and the lock acquire
                    is never noticed -> suspended forever with work queued.
  LockThroughFix  - the lock held from arm through registration. Without it, a
                    Signal can claim _pending in the gap and invoke a
                    continuation that is not stored yet (null invoke).
  ClearOnClaimFix - every claim clears the suspended signal before dispatch
                    (Pg: "parked state stays accurate across resume"). Without
                    it, a STALE signal lets a later handoff producer skip its
                    wait, no-op its claim (consumer is running, nothing
                    pending), and return with its flow unexecuted - breaking
                    EnqueueSyncWithHandoff's runs-inline-before-return
                    contract.

Memory-model fidelity: SC modeling is faithful - every cross-thread edge is
bracketed by the wake lock's Interlocked acquire / volatile release, the one
lock-free read is re-validated under the lock before arming, and the suspended
signal is an MRES (full synchronization on Set/Wait).

Properties:
  TypeOK
  NoNullInvoke    - a claimed wake always finds the continuation stored
                    (fails without LockThroughFix)
  HandoffInline   - a handoff producer that returned saw its flow consumed
                    (fails without ClearOnClaimFix)
  AllConsumed     - liveness: everything (queue + handoff) is consumed and
                    completion observed (fails without RecheckFix)
  HandoffReturns  - liveness: the handoff producer is never stranded waiting
*)

EXTENDS Naturals, TLC

CONSTANTS MaxItems, RecheckFix, LockThroughFix, ClearOnClaimFix

VARIABLES
  queue,      \* published items pending consumption
  enqueued,   \* total ever published (producer stops at MaxItems)
  consumed,   \* total consumed
  notEmpty,   \* producer's pre-publish flag
  lock,       \* wake spinlock held?
  pending,    \* _pending armed
  contStored, \* continuation registered for the current wait
  completed,  \* Complete() requested (cancel observed by re-checks)
  lostWake,   \* a Signal claim consumed _pending without a stored continuation
  cpc,        \* consumer: "run" | "locked" | "armed" | "suspended" | "done"
  ppc,        \* producer: "idle" | "flagged" | "published" (signal pending)
  suspendedSig, \* the MRES: set when the consumer's registration completed
  handoff,      \* 0..1: the sync flow published into the handoff slot
  hpc           \* handoff producer: "idle" | "waiting" | "signaling" | "done"

vars == <<queue, enqueued, consumed, notEmpty, lock, pending, contStored,
          completed, lostWake, cpc, ppc, suspendedSig, handoff, hpc>>

TypeOK ==
  /\ queue \in 0..MaxItems
  /\ enqueued \in 0..MaxItems
  /\ consumed \in 0..MaxItems
  /\ consumed + queue = enqueued
  /\ notEmpty \in BOOLEAN
  /\ lock \in BOOLEAN
  /\ pending \in BOOLEAN
  /\ contStored \in BOOLEAN
  /\ completed \in BOOLEAN
  /\ lostWake \in BOOLEAN
  /\ cpc \in {"run", "locked", "armed", "suspended", "done"}
  /\ ppc \in {"idle", "flagged", "published"}
  /\ suspendedSig \in BOOLEAN
  /\ handoff \in 0..1
  /\ hpc \in {"idle", "waiting", "signaling", "done"}

Init ==
  /\ queue = 0 /\ enqueued = 0 /\ consumed = 0
  /\ notEmpty = FALSE /\ lock = FALSE /\ pending = FALSE
  /\ contStored = FALSE /\ completed = FALSE /\ lostWake = FALSE
  /\ cpc = "run" /\ ppc = "idle"
  /\ suspendedSig = FALSE /\ handoff = 0 /\ hpc = "idle"

-----------------------------------------------------------------------------
\* Async producer (single, per the SPSC source contract).

EnqFlag ==
  /\ ppc = "idle" /\ enqueued < MaxItems
  /\ notEmpty' = TRUE
  /\ ppc' = "flagged"
  /\ UNCHANGED <<queue, enqueued, consumed, lock, pending, contStored,
                 completed, lostWake, cpc, suspendedSig, handoff, hpc>>

EnqPublish ==
  /\ ppc = "flagged"
  /\ queue' = queue + 1 /\ enqueued' = enqueued + 1
  /\ ppc' = "published"
  /\ UNCHANGED <<consumed, notEmpty, lock, pending, contStored, completed,
                 lostWake, cpc, suspendedSig, handoff, hpc>>

\* Signal: acquire the lock (disabled while held), claim _pending, clear the
\* suspended signal (under the fix), release, invoke. Claim + invoke collapse
\* into one step per outcome; the lock gating is represented exactly.
SignalClaimWakes ==
  /\ ppc = "published" /\ ~lock
  /\ hpc \notin {"waiting", "signaling"}   \* Execute's HandoffActive gate: async signals defer during the handoff window
  /\ pending
  /\ contStored
  /\ pending' = FALSE
  /\ contStored' = FALSE
  /\ suspendedSig' = IF ClearOnClaimFix THEN FALSE ELSE suspendedSig
  /\ cpc' = "run"
  /\ ppc' = "idle"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, completed,
                 lostWake, handoff, hpc>>

\* The hole LockThroughFix closes: claim in the arm-to-registration gap.
SignalClaimLost ==
  /\ ppc = "published" /\ ~lock
  /\ hpc \notin {"waiting", "signaling"}   \* Execute's HandoffActive gate: async signals defer during the handoff window
  /\ pending
  /\ ~contStored
  /\ pending' = FALSE
  /\ lostWake' = TRUE
  /\ ppc' = "idle"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, contStored,
                 completed, cpc, suspendedSig, handoff, hpc>>

SignalNoop ==
  /\ ppc = "published" /\ ~lock
  /\ hpc \notin {"waiting", "signaling"}   \* Execute's HandoffActive gate: async signals defer during the handoff window
  /\ ~pending
  /\ ppc' = "idle"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, pending,
                 contStored, completed, lostWake, cpc, suspendedSig,
                 handoff, hpc>>

\* Complete: shutdown after the async producer is done; rides the claim path.
\* The claim DEFERS during the handoff window, same as async signals: TLC
\* exhibited the steal otherwise (Complete claims the suspended wait between
\* the handoff producer's observe and its inline claim, running the sync flow
\* on the wrong thread). NOTE: this gate is MISSING in the shipped
\* PgClientFlowSource (State.Complete calls Vts.SetResult directly, bypassing
\* Execute's HandoffActive check) - a real shutdown-vs-sync-handoff hazard the
\* port must fix. Deferral loses no liveness: the handoff's inline claim wakes
\* the executor, whose next wait resolves Completed.
CompleteWakes ==
  /\ ppc = "idle" /\ enqueued = MaxItems /\ ~completed /\ ~lock
  /\ completed' = TRUE
  /\ IF hpc \in {"waiting", "signaling"}
       THEN UNCHANGED <<pending, contStored, cpc, lostWake, suspendedSig>>
       ELSE IF pending /\ contStored
         THEN /\ pending' = FALSE /\ contStored' = FALSE /\ cpc' = "run"
              /\ suspendedSig' = IF ClearOnClaimFix THEN FALSE ELSE suspendedSig
              /\ lostWake' = lostWake
         ELSE IF pending /\ ~contStored
           THEN /\ pending' = FALSE /\ lostWake' = TRUE
                /\ UNCHANGED <<contStored, cpc, suspendedSig>>
           ELSE UNCHANGED <<pending, contStored, cpc, lostWake, suspendedSig>>
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, ppc, handoff, hpc>>

-----------------------------------------------------------------------------
\* Sync-handoff producer (Pg's EnqueueSyncWithHandoff; the SyncWaiterLock baton
\* serializes concurrent sync producers, so one models the protocol).

\* Publish the flow into the handoff slot, then wait for the suspended signal.
HandoffPublish ==
  /\ hpc = "idle" /\ handoff = 0 /\ ~completed
  /\ handoff' = 1
  /\ hpc' = "waiting"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, pending,
                 contStored, completed, lostWake, cpc, ppc, suspendedSig>>

\* WaitForSuspended satisfied (the MRES fired - or was STALE without the fix).
HandoffObserve ==
  /\ hpc = "waiting"
  /\ suspendedSig
  /\ hpc' = "signaling"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, pending,
                 contStored, completed, lostWake, cpc, ppc, suspendedSig,
                 handoff>>

\* Inline claim: consume the wait and run the flow on this thread. Modeled
\* atomically (claim + the executor's inline consumption of the handoff slot)
\* per the code's contract: by the time SetResult/Signal(false) returns, the
\* executor has pulled the handoff and processed it on the caller's thread.
HandoffClaimInline ==
  /\ hpc = "signaling" /\ ~lock
  /\ pending /\ contStored
  /\ pending' = FALSE
  /\ contStored' = FALSE
  /\ suspendedSig' = IF ClearOnClaimFix THEN FALSE ELSE suspendedSig
  /\ handoff' = 0
  /\ cpc' = "run"
  /\ hpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, completed,
                 lostWake, ppc>>

\* Claim into the registration gap (reachable only without LockThroughFix).
HandoffClaimLost ==
  /\ hpc = "signaling" /\ ~lock
  /\ pending /\ ~contStored
  /\ pending' = FALSE
  /\ lostWake' = TRUE
  /\ hpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, contStored,
                 completed, cpc, ppc, suspendedSig, handoff>>

\* Claim no-op: nothing pending - the consumer was RUNNING, the observation was
\* stale. The producer returns with its flow unexecuted (handoff still 1).
\* Reachable only without ClearOnClaimFix.
HandoffClaimNoop ==
  /\ hpc = "signaling" /\ ~lock
  /\ ~pending
  /\ hpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, lock, pending,
                 contStored, completed, lostWake, cpc, ppc, suspendedSig,
                 handoff>>

-----------------------------------------------------------------------------
\* Consumer (the executor's pull loop).

\* TryGetNext hit: lock-free dequeue. Never the handoff slot (HandoffAcked).
PullHit ==
  /\ cpc = "run" /\ queue > 0
  /\ queue' = queue - 1 /\ consumed' = consumed + 1
  /\ UNCHANGED <<enqueued, notEmpty, lock, pending, contStored, completed,
                 lostWake, cpc, ppc, suspendedSig, handoff, hpc>>

\* TryGetNext miss -> WaitForNextAsync acquires the wake lock.
WaitAcquire ==
  /\ cpc = "run" /\ queue = 0 /\ ~lock
  /\ lock' = TRUE
  /\ notEmpty' = FALSE
  /\ cpc' = "locked"
  /\ UNCHANGED <<queue, enqueued, consumed, pending, contStored, completed,
                 lostWake, ppc, suspendedSig, handoff, hpc>>

WaitRecheckRetry ==
  /\ RecheckFix
  /\ cpc = "locked"
  /\ (queue > 0 \/ notEmpty)
  /\ lock' = FALSE
  /\ cpc' = "run"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, pending, contStored,
                 completed, lostWake, ppc, suspendedSig, handoff, hpc>>

\* Completed and drained - but a pending handoff still needs its rendezvous:
\* the executor must not resolve Completed out from under a waiting sync
\* producer (in Pg the handoff window gates completion; modeled as the guard).
WaitRecheckCompleted ==
  /\ cpc = "locked"
  /\ queue = 0 /\ ~notEmpty
  /\ completed
  /\ handoff = 0 /\ hpc # "waiting" /\ hpc # "signaling"
  /\ lock' = FALSE
  /\ cpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, pending, contStored,
                 completed, lostWake, ppc, suspendedSig, handoff, hpc>>

\* Arm. With the fix the guard is the full under-lock re-check: empty, no
\* in-flight publish, and NOT the completed-resolution case (note: completed
\* with a handoff rendezvous still pending DOES arm - the executor waits for
\* the sync producer's inline signal rather than resolving Completed under it).
WaitArm ==
  /\ cpc = "locked"
  /\ IF RecheckFix
       THEN /\ queue = 0 /\ ~notEmpty
            /\ ~(completed /\ handoff = 0 /\ hpc \notin {"waiting", "signaling"})
       ELSE TRUE
  /\ pending' = TRUE
  /\ lock' = IF LockThroughFix THEN TRUE ELSE FALSE
  /\ cpc' = "armed"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, contStored, completed,
                 lostWake, ppc, suspendedSig, handoff, hpc>>

\* Registration: store the continuation, release the lock, set the suspended
\* signal (WaitOnCompleted; the MRES set comes after the store + release).
WaitRegister ==
  /\ cpc = "armed"
  /\ contStored' = TRUE
  /\ lock' = FALSE
  /\ suspendedSig' = TRUE
  /\ cpc' = "suspended"
  /\ UNCHANGED <<queue, enqueued, consumed, notEmpty, pending, completed,
                 lostWake, ppc, handoff, hpc>>

-----------------------------------------------------------------------------

Next ==
  \/ EnqFlag \/ EnqPublish
  \/ SignalClaimWakes \/ SignalClaimLost \/ SignalNoop
  \/ CompleteWakes
  \/ HandoffPublish \/ HandoffObserve
  \/ HandoffClaimInline \/ HandoffClaimLost \/ HandoffClaimNoop
  \/ PullHit \/ WaitAcquire
  \/ WaitRecheckRetry \/ WaitRecheckCompleted \/ WaitArm \/ WaitRegister

Fairness ==
  /\ WF_vars(EnqPublish)
  /\ WF_vars(SignalClaimWakes) /\ WF_vars(SignalClaimLost) /\ WF_vars(SignalNoop)
  /\ WF_vars(CompleteWakes)
  /\ WF_vars(HandoffObserve)
  /\ WF_vars(HandoffClaimInline) /\ WF_vars(HandoffClaimLost) /\ WF_vars(HandoffClaimNoop)
  /\ WF_vars(PullHit) /\ WF_vars(WaitAcquire)
  /\ WF_vars(WaitRecheckRetry) /\ WF_vars(WaitRecheckCompleted)
  /\ WF_vars(WaitArm) /\ WF_vars(WaitRegister)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

\* A claimed wake never fires into a missing continuation.
NoNullInvoke == ~lostWake

\* A handoff producer that returned saw its flow consumed inline.
HandoffInline == (hpc = "done") => (handoff = 0)

\* Everything produced (queue + handoff) is consumed and completion observed.
AllConsumed == (completed /\ enqueued = MaxItems) ~>
                 (consumed = MaxItems /\ handoff = 0 /\ cpc = "done")

\* The handoff producer is never stranded at the rendezvous.
HandoffReturns == (hpc = "waiting") ~> (hpc = "done")

=============================================================================
