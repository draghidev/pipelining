-------------------------- MODULE WaitProtocolHeldSlot --------------------------
(*
Slon's PgClientFlowSource drive / handoff / enqueue protocol (the SHIPPED
July-2026 shape: _driving/_redrive single-runner latch, HeldSyncFlow,
TakeoverPending/TakeoverActive, ParkedAtSyncHead, RunLoop, and the
OnExecutorSuspended self-heal). Models Slon/Slon/Pg/Protocol/PgClientFlowSource.cs
(State + Enumerator) composed with WakeSignal.cs. SEPARATE from WaitProtocol.tla /
WaitProtocolCompensated.tla: those model the OLD EnqueueSyncWithHandoff
(WaitForSuspended rendezvous); this models the NEW caller-handoff path plus the
async enqueue-after-idle cycle.

V2 FIDELITY (this round): WaitCore's arm decision and the awaiter's continuation
registration are TWO actions (ArmDecide -> pumpPc="arming" -> Register), matching
the code's two phases. NOTE the shipped discipline verified from WakeSignal.cs:
the wake LOCK IS HELD from Arm() (L75-79) through WaitOnCompleted's release
(L86-102) - lock-through-OnCompleted (WakeSignal.cs L10-11, L137-139; the
WaitForNextAwaitable Signal shape always reaches UnsafeOnCompleted, no early
release). So no LOCK-TAKING action can interleave into the armed-but-unregistered
window; what CAN interleave is everything that does not take the wake lock:
  - a producer's storage.Enqueue (under the protocol's _syncRoot, NOT the wake
    lock) - so Register's OnExecutorSuspended HasItem re-read (L199) can see an
    item the ArmDecide miss-check did not. This is the window the self-heal and
    the L206-207 redrive-record exist for.
  - the TSO commit of a buffered publish (CommitVis).
The LockThrough CONSTANT witnesses the discipline itself: FALSE releases the lock
in the window (the hypothesized separate-acquisition shape) and a claim then
dispatches an unregistered/torn continuation -> NoNullInvoke goes RED. TRUE is
the shipped code. OnExecutorSuspended runs at Register (WakeSignal.cs L96-101:
MRES set, OnSuspended invoke, THEN release), not at ArmDecide - moved
accordingly. The post-release IsCompleted self-signal (L104-105) is not modeled
(completion is out of scope).

A second real gap the split exposes (present even in v1): the turn-end gap on the
runner thread between registration's lock release and RunLoop's post-lock acquire
(RunLoop L307->L308). _driving is still 1 there; a Drive in that gap records
_redrive (shipped) or is DROPPED (G7=FALSE) - the _redrive deletability question.

Captured hang #1 (sync): 8 protocols x 20 sequential sync commands; one wedges at
progress=14/20 backlog=1 outstanding=1 executor=null activated=null - a sync flow
in _storage, never pulled, executor parked, enqueuer blocked on its MRES forever.
Captured hang #2 (async-only): sequential enqueue/consume/sync-dispose rounds, a
fresh probe enqueue hangs. Both are a LOST DISPATCH WAKE.

Code line -> action map (PgClientFlowSource.cs unless noted):
  EnqueueSyncWaiter L330-340 ......... SyncEnq
  WaitForExecutor kick  L357 ......... SyncKick (DriveBranch)
  WaitForExecutor loop  L359-402 ..... SyncWaitWake + SyncTakeoverDone
  WaitForExecutor close L410 ......... SyncClose (DriveBranch)
  EnqueueItem L415 / Execute L477 .... AsyncEnq(k) + AsyncExecuteLocked(k) (DriveBranch)
                                       + AsyncExecuteFast(k) (the PROPOSED fast path)
  Drive L267-291 ..................... DriveBranch (inlined at call sites)
  SubmitDetached L288 (TP hop) ....... runloopPending -> RunLoopStart (delayed)
  RunLoop L302-325 ................... RunLoopStart / RunLoopContinue / RunLoopRelinquish
  DispatchClaimed pump (RunLoop L307)  the Pull* turn actions (TryGetNext, lock-free)
  TryGetNext L509-565 ................ PullTakePending / PullTakePendingTrailing /
                                       PullTakeActiveMiss / PullBlockedHeld / PullEmpty /
                                       PullAsyncComplete / PullAsyncTrailing / PullHoldSync
  WaitCore L579-621 (wake lock) ...... WaitCoreRetry / WaitCoreTakeActiveDecide /
                                       WaitCoreArmDecide (Arm(): WakeSignal.cs L75-79)
  WaitOnCompleted (WakeSignal L86-102) Register (OnExecutorSuspended L172-211 runs HERE)
  self-heal DispatchClaimed L208-209 . selfhealPending -> SelfhealStart (delayed, NO _driving)
  trailing await off-park L194-196 ... PullAsyncTrailing / PullTakePendingTrailing ->
                                       TrailingResume (driving==0 resume, no RunLoop wrapper)

Guard/compensation inventory, each a Toggle CONSTANT (house methodology):
  G1 ClearParkedAtTakeover  - ParkedAtSyncHead=false at the takeover claim (L386)
  G2 SuspendHeldDrivingGate - the _driving==0 gate on OnExecutorSuspended's held
                              signal (L187). NOTE: "RunLoop hand-back-after-drop
                              ordering" (L317-321) is NOT a separate toggle: drop
                              and Set are one wake-lock hold, the caller's wake
                              path re-acquires the lock before reading _driving,
                              so the within-hold order is unobservable. The
                              observable discipline is G2 (early-signal vs
                              relinquish-signal); the relinquish hand-back itself
                              is modeled in RunLoopRelinquish.
  G4 WaitCoreRetry          - WaitCore's held==null && HasItem retry (L611)
  G5 SelfHealSuspend        - OnExecutorSuspended's HasItem branch (L199-209),
                              BOTH faces: the driving==0 claim+dispatch AND the
                              driving==1 redrive record (L206-207)
  G6 RedriveExcludesParked  - RunLoop's !(ParkedAtSyncHead) redrive exclusion (L310)
  G7 RedriveMechanism       - the _redrive record+serve as a whole: Drive's
                              driving==1 record (L277), OnExecutorSuspended's
                              driving==1 record (L206-207), RunLoop's serve
                              (L309-315). FALSE = "delete _redrive": those Drives
                              DROP, RunLoop always relinquishes. _driving itself
                              stays (it is the claim gate, not under test).
  G8 ParkedFlagSeparate     - TRUE = shipped: Drive (L272) and RunLoop (L310) read
                              the dedicated ParkedAtSyncHead flag. FALSE = the
                              proposed simplification: derive "reserved" from
                              HeldSyncFlow != null at the same sites (under the
                              same lock). Differs because held is non-null from
                              PullHoldSync until PullTakePending - a superset of
                              the parked window (includes mid-turn hold and the
                              takeover body), where the reserved branch DROPS a
                              wake without recording a redrive.
  LockThrough               - TRUE = shipped lock-through-OnCompleted. FALSE =
                              witness: lock released between arm and registration.

Weak memory: PublishFence=FALSE buffers the producer's storage publish (TSO store
buffer, qBuf); the producer's own Drive (AcquireWakeLock, a full fence) drains it;
CommitVis is natural propagation. The pump dequeues only visible items; the
re-checks (WaitCore retry, Register's HasItem) read the visible prefix.

=== The proposed lock-free enqueue-side fast path (FastPath round) ===
Producer protocol: enqueue (release store) -> FULL FENCE -> read pumpWord ->
Running => return (no lock, no redrive); Idle => today's locked Drive. Async
producers only; sync callers unchanged. Adjudicated GREEN protocol, mapped to
the PgClientFlowSource.cs sites it would change:
  - pumpWord := Running at every dispatch-token mint: Drive's claim (L280-282),
    WaitForExecutor's takeover claim (L371-386), OnExecutorSuspended's self-heal
    claim (L208-209), RunLoop's redrive re-claim (L311-313). All under the wake
    lock (its release orders the store; producers read latest-committed).
  - pumpWord := Idle in OnExecutorSuspended (i.e. at REGISTRATION, WakeSignal
    L96-101), as an Interlocked store placed BEFORE the L174 HeldSyncFlow read
    and the L199 HasItem() recheck. THIS PLACEMENT IS THE CRUX: the pump's last
    storage recheck of the turn must FOLLOW the fenced Idle store, so the Dekker
    pairs - producer [item-store, fence, word-read] vs pump [Idle-store, fence,
    storage-recheck]. Placing the Idle store at RunLoop's relinquish instead
    (IdleAtRelinquish=TRUE) is RED: the relinquish never re-peeks storage, so a
    fast-path return in the turn-end gap strands (the witness).
  - The word stays Running across an off-park suspension (trailing/backpressure:
    no registration happens, so no Idle store) - fast-path producers no-op there
    and the resume turn's WaitCore recheck (G4) covers them.
  - The producer's full fence between enqueue and word-read is load-bearing:
    FastPathFence=FALSE (release-only) is RED under TSO - the item can sit in
    the store buffer while the pump goes Idle and rechecks empty.
Model notes: pump-side word stores are modeled committed-at-once (the proposal
makes them Interlocked; the store->recheck ordering is inside one atomic action,
which IS the fence claim being verified). The producer's read-verdict and the
locked Drive are fused in AsyncExecuteLocked - safe: the locked branch re-reads
everything under the lock, so a stale Idle verdict only takes the conservative
path. qBuf is a pooled store buffer across producers (a fence by one drains all
- an over-approximation of visibility shared with the earlier rounds; each
producer enqueues once and sync items are fenced by their own kick immediately,
so the affected window is negligible).

Simplifications (current):
  - Completion/IsCompleted not modeled (neither captured hang involves it); the
    post-release IsCompleted self-signal (WakeSignal L104) therefore absent.
  - FlushGate abstracted away (its fake-miss is re-armed by WaitCore's Retry -
    spurious spin, cannot strand).
  - Async flow bodies complete inline (PullAsyncComplete) OR park once on a
    trailing await (PullAsyncTrailing) - the general mid-flow multi-park is
    collapsed to one park, which preserves the driving==0 resume shape.
  - The wake lock is not a separate variable: lock-held critical sections are
    atomic actions, EXCEPT the arm->register window which is explicit (pumpPc
    ="arming"); with LockThrough, lock-taking actions are disabled in it, which
    is exactly the lock's effect there. Weaker-than-TSO reordering of the
    lock-free TryPeek under a held lock is NOT modeled (v1 report's residual).
  - MRES is level-triggered (syncMres BOOLEAN), reset only by the caller's wake
    path, faithful to ManualResetEventSlim under the wake-lock discipline.

Properties: TypeOK, SingleRunner, OneDriver, NoDoubleProc, NoNullInvoke, FifoSync
(safety); AllProcessed, SyncReturns (liveness). WindowProbe is a REACHABILITY
probe (expected violated in the probe config): an item became visible during the
armed-but-unregistered window - evidence TLC explores the v2 window.
*)

EXTENDS Naturals, Sequences, TLC

CONSTANTS
  NSync,          \* number of sequential sync ops (0..2)
  NAsync,         \* number of concurrent async producers (1..2)
  G1, G2, G4, G5, G6, G7, G8,
  LockThrough,    \* TRUE = shipped lock-through-OnCompleted
  PublishFence,   \* TRUE = SC publish; FALSE = TSO store-buffered publish
  AllowTrailing,  \* TRUE = model the off-executor / fault off-park resume
  \* === The proposed lock-free enqueue-side Dekker fast path (adjudication) ===
  FastPath,       \* TRUE = async producers may skip the wake lock when the pump
                  \* word reads Running: enqueue (release) -> full fence -> read
                  \* pumpWord -> Running => RETURN (no lock, no redrive); else
                  \* fall into today's locked Drive. Sync callers ALWAYS take
                  \* the locked path (WaitForExecutor semantics unchanged).
  FastPathFence,  \* TRUE = the proposal's full fence between the enqueue store
                  \* and the pumpWord read (the producer's own store buffer is
                  \* drained before the read). FALSE = weakened (release-only)
                  \* witness: the item can still be buffered when the producer
                  \* reads Running and returns.
  IdleAtRelinquish \* Word->Idle placement. FALSE = the GREEN placement: Register
                  \* stores Idle (fenced) immediately BEFORE its OnSuspended
                  \* HasItem recheck, so the pump's LAST storage recheck follows
                  \* the fenced Idle store (sound Dekker pairing). TRUE = the
                  \* witness placement: a runloop-owned turn keeps Running
                  \* through Register and stores Idle at RunLoopRelinquish,
                  \* which never re-peeks storage - a fast-path return in the
                  \* turn-end gap then has no covering recheck.

SyncIds  == IF NSync = 0 THEN {} ELSE 1..NSync
AsyncIds == { 2 + r : r \in 1..NAsync }
AllIds   == SyncIds \cup AsyncIds
IsSync(f) == f < 3

VARIABLES
  storage, qBuf, done,
  held, takePending, takeActive, parkedSyncHead,
  driving, redrive, armed,
  pumpPc, owner, runloopPending, selfhealPending,
  trailingPark, trailingFlow, current,
  scpc, syncOp, syncWaitFlow, syncMres,
  apc,
  doubleDispatch, nullInvoke, armSawEmpty,
  \* Fast-path state: the producer-readable pump-state word, and the diagnostic
  \* record of which turn phases a fast-path return was taken in (non-vacuity).
  pumpWord,       \* "running" | "idle" - all pump-side stores modeled FENCED
                  \* (the proposal's Interlocked); producers read latest committed
                  \* (TSO loads are coherent; only own-store forwarding exists,
                  \* and producers never write the word).
  fastMask        \* subset of {"owned","unowned","offpark","pending"}: phases in
                  \* which a fast-path return occurred (owner=runloop / selfheal /
                  \* off-park trailing / dispatch-token pending).

vars == <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
          driving, redrive, armed, pumpPc, owner, runloopPending, selfhealPending,
          trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow, syncMres,
          apc, doubleDispatch, nullInvoke, armSawEmpty, pumpWord, fastMask>>

VisibleLen == Len(storage) - qBuf
HasItemVis == VisibleLen > 0
HeadVis    == storage[1]

\* The producer-facing "this park is reserved" read at Drive L272 / RunLoop L310.
\* G8=TRUE: the shipped dedicated flag. G8=FALSE: derived from HeldSyncFlow.
Reserved == IF G8 THEN parkedSyncHead ELSE held # 0

\* Lock-through: while the pump is between arm and registration it HOLDS the wake
\* lock, so lock-taking actions are disabled. FALSE = the witness shape.
WakeLockFree == IF LockThrough THEN pumpPc # "arming" ELSE TRUE

TypeOK ==
  /\ storage \in Seq(AllIds) /\ Len(storage) <= 4
  /\ qBuf \in 0..4 /\ qBuf <= Len(storage)
  /\ done \subseteq AllIds
  /\ held \in {0, 1, 2}
  /\ takePending \in BOOLEAN /\ takeActive \in BOOLEAN /\ parkedSyncHead \in BOOLEAN
  /\ driving \in 0..1 /\ redrive \in BOOLEAN /\ armed \in BOOLEAN
  /\ pumpPc \in {"parked", "pull", "wait", "arming"}
  /\ owner \in {"none", "runloop", "selfheal"}
  /\ runloopPending \in BOOLEAN /\ selfhealPending \in BOOLEAN
  /\ trailingPark \in BOOLEAN /\ trailingFlow \in {0, 1, 2, 3, 4}
  /\ current \in {0, 1, 2, 3, 4}
  /\ scpc \in {"enq", "kick", "wait", "takeover", "close", "done"}
  /\ syncOp \in 1..3
  /\ syncWaitFlow \in {0, 1, 2}
  /\ syncMres \in BOOLEAN
  /\ apc \in [1..NAsync -> {"enq", "drive", "done"}]
  /\ doubleDispatch \in BOOLEAN /\ nullInvoke \in BOOLEAN /\ armSawEmpty \in BOOLEAN
  /\ pumpWord \in {"running", "idle"}
  /\ fastMask \subseteq {"owned", "unowned", "offpark", "pending"}

Init ==
  /\ storage = << >> /\ qBuf = 0 /\ done = {}
  /\ held = 0 /\ takePending = FALSE /\ takeActive = FALSE /\ parkedSyncHead = FALSE
  /\ driving = 0 /\ redrive = FALSE /\ armed = TRUE       \* executor started, armed+registered on empty
  /\ pumpPc = "parked" /\ owner = "none"
  /\ runloopPending = FALSE /\ selfhealPending = FALSE
  /\ trailingPark = FALSE /\ trailingFlow = 0 /\ current = 0
  /\ scpc = IF NSync = 0 THEN "done" ELSE "enq"
  /\ syncOp = 1 /\ syncWaitFlow = 0 /\ syncMres = FALSE
  /\ apc = [k \in 1..NAsync |-> "enq"]
  /\ doubleDispatch = FALSE /\ nullInvoke = FALSE /\ armSawEmpty = TRUE
  /\ pumpWord = "idle" /\ fastMask = {}

-----------------------------------------------------------------------------
\* Drive (L267-291), inlined. One wake-lock critical section: the acquire is the
\* caller's fence (drains its store buffer). Disabled while the pump lock-throughs
\* the arm window. Branches: reserved no-op (L272-275) / live-runner redrive record
\* L277 (G7; DROP without it) / claim L278-283 (queues RunLoopStart; a claim landing
\* in a LockThrough=FALSE window dispatches an unregistered continuation) / drop.
DriveBranch ==
  /\ WakeLockFree
  /\ qBuf' = 0
  /\ IF Reserved
       THEN /\ UNCHANGED <<driving, redrive, armed, runloopPending, pumpWord>>
            /\ nullInvoke' = nullInvoke
       ELSE IF driving = 1
         THEN /\ redrive' = (redrive \/ G7)
              /\ UNCHANGED <<driving, armed, runloopPending, pumpWord>>
              /\ nullInvoke' = nullInvoke
         ELSE IF armed
           THEN /\ driving' = 1 /\ redrive' = FALSE /\ armed' = FALSE
                /\ runloopPending' = TRUE
                /\ pumpWord' = "running"   \* a dispatch token is minted: the word
                                           \* covers the token-pending window too
                /\ nullInvoke' = (nullInvoke \/ pumpPc = "arming")
           ELSE /\ UNCHANGED <<driving, redrive, armed, runloopPending, pumpWord>>
                /\ nullInvoke' = nullInvoke

-----------------------------------------------------------------------------
\* Sync caller (sequential; disabled when NSync=0).

SyncEnq ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ scpc = "enq" /\ syncOp <= NSync
  /\ storage' = Append(storage, syncOp)
  /\ qBuf' = IF PublishFence THEN qBuf ELSE qBuf + 1
  /\ syncWaitFlow' = syncOp
  /\ scpc' = "kick"
  /\ UNCHANGED <<done, held, takePending, takeActive, parkedSyncHead, driving,
                 redrive, armed, pumpPc, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, syncOp, syncMres, apc,
                 doubleDispatch, nullInvoke, armSawEmpty>>

SyncKick ==
  /\ UNCHANGED fastMask
  /\ scpc = "kick"
  /\ DriveBranch
  /\ scpc' = "wait"
  /\ UNCHANGED <<storage, done, held, takePending, takeActive, parkedSyncHead,
                 pumpPc, owner, selfhealPending, trailingPark, trailingFlow, current,
                 syncOp, syncWaitFlow, syncMres, apc, doubleDispatch, armSawEmpty>>

\* WaitForExecutor loop body (L359-402): mres.Wait returned; under the wake lock
\* reset the MRES then try the takeover claim (win => become the runner inline,
\* G1 clears the reservation L386; lose => re-wait). Blocked during a lock-through
\* window; with LockThrough=FALSE a window claim tears the continuation.
SyncWaitWake ==
  /\ UNCHANGED fastMask
  /\ WakeLockFree
  /\ scpc = "wait" /\ syncMres = TRUE
  /\ syncMres' = FALSE
  /\ IF held = syncWaitFlow /\ held # 0 /\ driving = 0 /\ armed
       THEN /\ driving' = 1 /\ redrive' = FALSE /\ armed' = FALSE
            /\ takePending' = TRUE
            /\ parkedSyncHead' = IF G1 THEN FALSE ELSE parkedSyncHead
            /\ owner' = "runloop" /\ pumpPc' = "pull"
            /\ scpc' = "takeover"
            /\ doubleDispatch' = IF pumpPc # "parked" THEN TRUE ELSE doubleDispatch
            /\ nullInvoke' = (nullInvoke \/ pumpPc = "arming")
            /\ pumpWord' = "running"   \* takeover claim = a turn starts
       ELSE /\ scpc' = "wait"
            /\ UNCHANGED <<driving, redrive, armed, takePending, parkedSyncHead,
                           owner, pumpPc, doubleDispatch, nullInvoke, pumpWord>>
  /\ UNCHANGED <<storage, qBuf, done, held, takeActive, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, syncOp, syncWaitFlow, apc,
                 armSawEmpty>>

SyncTakeoverDone ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ scpc = "takeover" /\ owner = "none" /\ driving = 0
  /\ scpc' = "close"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, pumpPc, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, syncOp, syncWaitFlow, syncMres,
                 apc, doubleDispatch, nullInvoke, armSawEmpty>>

SyncClose ==
  /\ UNCHANGED fastMask
  /\ scpc = "close"
  /\ DriveBranch
  /\ syncWaitFlow' = 0
  /\ syncOp' = syncOp + 1
  /\ scpc' = IF syncOp >= NSync THEN "done" ELSE "enq"
  /\ UNCHANGED <<storage, done, held, takePending, takeActive, parkedSyncHead,
                 pumpPc, owner, selfhealPending, trailingPark, trailingFlow, current,
                 syncMres, apc, doubleDispatch, armSawEmpty>>

-----------------------------------------------------------------------------
\* Async producers: NAsync INDEPENDENT, CONCURRENT producers (the shared wire).
\* Producer k enqueues its one flow (id 2+k) then Drives; both in flight at once.
\* The ENQUEUE takes the protocol's _syncRoot, NOT the wake lock - it is NOT
\* gated on WakeLockFree, so it can land inside the arm->register window. Only
\* the Drive (AsyncExecute) takes the wake lock.

AsyncEnq(k) ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ apc[k] = "enq"
  /\ storage' = Append(storage, 2 + k)
  /\ qBuf' = IF PublishFence THEN qBuf ELSE qBuf + 1
  /\ apc' = [apc EXCEPT ![k] = "drive"]
  /\ UNCHANGED <<done, held, takePending, takeActive, parkedSyncHead, driving,
                 redrive, armed, pumpPc, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow,
                 syncMres, doubleDispatch, nullInvoke, armSawEmpty>>

\* Locked path: today's Execute -> TryClaim -> Drive. Taken always when FastPath
\* is off; with FastPath on, taken when the word read is Idle. (The read verdict
\* and the locked drive are fused into one action: the locked branch re-reads all
\* protocol state under the lock, so any flip between the producer's read and its
\* lock acquire is already handled by DriveBranch's atomic branches - the fusion
\* can only make the model take the SAFE path more often, never skip a wake.)
AsyncExecuteLocked(k) ==
  /\ UNCHANGED fastMask
  /\ apc[k] = "drive"
  /\ (FastPath => pumpWord = "idle")
  /\ DriveBranch
  /\ apc' = [apc EXCEPT ![k] = "done"]
  /\ UNCHANGED <<storage, done, held, takePending, takeActive, parkedSyncHead,
                 pumpPc, owner, selfhealPending, trailingPark, trailingFlow, current,
                 scpc, syncOp, syncWaitFlow, syncMres, doubleDispatch, armSawEmpty>>

\* THE PROPOSED FAST PATH: enqueue already done (release store, possibly still in
\* the producer's buffer under TSO) -> full fence (FastPathFence: drains the
\* producer's buffer; the weakened witness skips the drain) -> read pumpWord ->
\* Running => RETURN. No lock, no redrive, no token. NOT gated on WakeLockFree:
\* it takes no lock, so it may run inside the arm window too. Records which pump
\* phase it fired in (fastMask) for the non-vacuity probes.
AsyncExecuteFast(k) ==
  /\ FastPath
  /\ apc[k] = "drive"
  /\ pumpWord = "running"
  /\ qBuf' = IF FastPathFence THEN 0 ELSE qBuf
  /\ apc' = [apc EXCEPT ![k] = "done"]
  /\ fastMask' = fastMask \cup
       {IF owner = "runloop" THEN "owned"
        ELSE IF owner = "selfheal" THEN "unowned"
        ELSE IF trailingPark THEN "offpark"
        ELSE "pending"}
  /\ UNCHANGED <<storage, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, pumpPc, owner, runloopPending,
                 selfhealPending, trailingPark, trailingFlow, current, scpc, syncOp,
                 syncWaitFlow, syncMres, doubleDispatch, nullInvoke, armSawEmpty,
                 pumpWord>>

-----------------------------------------------------------------------------
\* Pump dispatch starts (each consumes its token; requires a parked pump).

RunLoopStart ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ runloopPending /\ pumpPc = "parked"
  /\ runloopPending' = FALSE
  /\ owner' = "runloop" /\ pumpPc' = "pull"
  /\ doubleDispatch' = IF owner # "none" THEN TRUE ELSE doubleDispatch
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, selfhealPending, trailingPark, trailingFlow,
                 current, scpc, syncOp, syncWaitFlow, syncMres, apc, nullInvoke,
                 armSawEmpty>>

\* Self-heal dispatch: DispatchClaimed WITHOUT _driving (L208-209). Modeled exactly.
SelfhealStart ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ selfhealPending /\ pumpPc = "parked"
  /\ selfhealPending' = FALSE
  /\ owner' = "selfheal" /\ pumpPc' = "pull"
  /\ doubleDispatch' = IF owner # "none" THEN TRUE ELSE doubleDispatch
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, runloopPending, trailingPark, trailingFlow,
                 current, scpc, syncOp, syncWaitFlow, syncMres, apc, nullInvoke,
                 armSawEmpty>>

\* Trailing await completed on a TP thread: the pump RESUMES off-park, _driving==0,
\* no RunLoop wrapper (the parked flow's body finishes here, off-executor).
TrailingResume ==
  /\ UNCHANGED fastMask
  /\ trailingPark /\ pumpPc = "parked" /\ owner = "none"
  /\ pumpWord' = "running"
  /\ trailingPark' = FALSE
  /\ done' = done \cup {trailingFlow}
  /\ trailingFlow' = 0
  /\ owner' = "selfheal" /\ pumpPc' = "pull"
  /\ UNCHANGED <<storage, qBuf, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, runloopPending, selfhealPending, current,
                 scpc, syncOp, syncWaitFlow, syncMres, apc, doubleDispatch, nullInvoke,
                 armSawEmpty>>

-----------------------------------------------------------------------------
\* Pump turn: TryGetNext (L509-565), lock-free.

PullTakePending ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "pull" /\ takePending
  /\ takePending' = FALSE /\ takeActive' = TRUE
  /\ current' = held /\ done' = done \cup {held}
  /\ held' = 0 /\ pumpPc' = "pull"
  /\ UNCHANGED <<storage, qBuf, parkedSyncHead, driving, redrive, armed, owner,
                 runloopPending, selfhealPending, trailingPark, trailingFlow, scpc,
                 syncOp, syncWaitFlow, syncMres, apc, doubleDispatch, nullInvoke,
                 armSawEmpty>>

\* The taken-over SYNC flow's own body FAULTS (L194-196's named path): its drain
\* parks the pump on a trailing await, the inline takeover RunLoop relinquishes
\* (_driving=0; armed=FALSE so nothing to re-claim), the body completes later
\* off-thread (TrailingResume). takeActive stays set for the resumed turn.
PullTakePendingTrailing ==
  /\ UNCHANGED fastMask
  /\ AllowTrailing
  /\ pumpPc = "pull" /\ takePending
  /\ takePending' = FALSE /\ takeActive' = TRUE
  /\ current' = held /\ trailingPark' = TRUE /\ trailingFlow' = held
  /\ held' = 0
  /\ driving' = 0 /\ owner' = "none" /\ pumpPc' = "parked"
  \* Fused RunLoop relinquish at an off-park suspension: under the GREEN placement
  \* the word STAYS Running through the off-park (face 3: fast-path producers
  \* no-op; the resume's WaitCore recheck covers). The witness placement idles it.
  /\ pumpWord' = IF IdleAtRelinquish THEN "idle" ELSE pumpWord
  /\ UNCHANGED <<storage, qBuf, done, parkedSyncHead, redrive, armed,
                 runloopPending, selfhealPending, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

PullTakeActiveMiss ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "pull" /\ ~takePending /\ takeActive
  /\ pumpPc' = "wait"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

PullBlockedHeld ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "pull" /\ ~takePending /\ ~takeActive /\ held # 0
  /\ pumpPc' = "wait"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

PullEmpty ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "pull" /\ ~takePending /\ ~takeActive /\ held = 0 /\ ~HasItemVis
  /\ pumpPc' = "wait"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

PullAsyncComplete ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "pull" /\ ~takePending /\ ~takeActive /\ held = 0 /\ HasItemVis
  /\ ~IsSync(HeadVis)
  /\ storage' = Tail(storage)
  /\ current' = HeadVis /\ done' = done \cup {HeadVis}
  /\ pumpPc' = "pull"
  /\ UNCHANGED <<qBuf, held, takePending, takeActive, parkedSyncHead, driving,
                 redrive, armed, owner, runloopPending, selfhealPending, trailingPark,
                 trailingFlow, scpc, syncOp, syncWaitFlow, syncMres, apc,
                 doubleDispatch, nullInvoke, armSawEmpty>>

\* Async head dispatched but its drain parks on a trailing (non-WakeSignal) await -
\* the off-executor completion path. A RunLoop owner relinquishes now (armed is
\* FALSE, TryClaimLocked fails - RunLoop L311 - regardless of redrive).
PullAsyncTrailing ==
  /\ UNCHANGED fastMask
  /\ AllowTrailing
  /\ pumpPc = "pull" /\ ~takePending /\ ~takeActive /\ held = 0 /\ HasItemVis
  /\ ~IsSync(HeadVis)
  /\ storage' = Tail(storage)
  /\ current' = HeadVis
  /\ trailingPark' = TRUE /\ trailingFlow' = HeadVis
  /\ driving' = 0 /\ owner' = "none"
  /\ pumpPc' = "parked"
  /\ pumpWord' = IF IdleAtRelinquish THEN "idle" ELSE pumpWord
  /\ UNCHANGED <<qBuf, done, held, takePending, takeActive, parkedSyncHead, redrive,
                 armed, runloopPending, selfhealPending, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

PullHoldSync ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "pull" /\ ~takePending /\ ~takeActive /\ held = 0 /\ HasItemVis
  /\ IsSync(HeadVis)
  /\ storage' = Tail(storage)
  /\ current' = HeadVis /\ held' = HeadVis
  /\ pumpPc' = "wait"
  /\ UNCHANGED <<qBuf, done, takePending, takeActive, parkedSyncHead, driving, redrive,
                 armed, owner, runloopPending, selfhealPending, trailingPark, trailingFlow,
                 scpc, syncOp, syncWaitFlow, syncMres, apc, doubleDispatch, nullInvoke,
                 armSawEmpty>>

-----------------------------------------------------------------------------
\* WaitCore (L579-621) - the arm DECISION, one wake-lock hold. The registration
\* (Register below) is a LATER phase; with LockThrough the lock spans both.

\* Retry: work visible and no sync flow held (L611). Re-pull, no suspension.
WaitCoreRetry ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "wait" /\ ~takeActive
  /\ held = 0 /\ HasItemVis /\ G4
  /\ pumpPc' = "pull"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

\* One-shot takeover hand-back (L587-591): reset TakeoverActive, Arm() (_pending
\* := TRUE, WakeSignal L75-79). Registration follows as Register.
WaitCoreTakeActiveDecide ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "wait" /\ takeActive
  /\ takeActive' = FALSE
  /\ armed' = TRUE
  /\ armSawEmpty' = ~HasItemVis
  /\ pumpPc' = "arming"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, parkedSyncHead, driving,
                 redrive, owner, runloopPending, selfhealPending, trailingPark,
                 trailingFlow, current, scpc, syncOp, syncWaitFlow, syncMres, apc,
                 doubleDispatch, nullInvoke>>

\* Plain arm (L620): empty, or a sync flow held, or G4 off with items.
WaitCoreArmDecide ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ pumpPc = "wait" /\ ~takeActive
  /\ ~(held = 0 /\ HasItemVis /\ G4)
  /\ armed' = TRUE
  /\ armSawEmpty' = ~HasItemVis
  /\ pumpPc' = "arming"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, owner, runloopPending, selfhealPending, trailingPark,
                 trailingFlow, current, scpc, syncOp, syncWaitFlow, syncMres, apc,
                 doubleDispatch, nullInvoke>>

\* Registration (WakeSignal.WaitOnCompleted L86-102): continuation stored, MRES
\* set, OnSuspended invoked UNDER the lock, then release. OnExecutorSuspended
\* (L172-211) therefore runs HERE, reading CURRENT state - including items that
\* became visible during the window (a producer's enqueue takes _syncRoot, not
\* the wake lock). held cannot have changed (all held writers hold the wake lock).
\*   held#0: ParkedAtSyncHead := TRUE (L179); signal the held caller iff
\*           ~G2 \/ driving==0 (L187-188).
\*   held=0: ParkedAtSyncHead := FALSE; if HasItem (L199) and G5:
\*           driving==1 -> record redrive L206-207 (G7; DROP without it);
\*           driving==0 -> TryClaimLocked + DispatchClaimed-on-TP L208-209
\*           (consumes the arm - unless a LockThrough=FALSE window claim already
\*           took it, then TryClaimLocked fails and nothing dispatches).
\* An unowned (selfheal/trailing-resume) dispatch ends at its suspension: owner
\* clears here. A runloop turn keeps its owner (RunLoop post-lock still to run).
\* Word placement (the fast path's crux): the GREEN placement stores Idle here,
\* fenced, IMMEDIATELY BEFORE the OnSuspended HasItem recheck (same atomic action
\* = store-fence-recheck ordering). Dekker soundness: a producer that read Running
\* did so before this Idle store committed; its item (committed before its own
\* fenced read) is therefore visible to the recheck that follows the store. A
\* producer reading after the store sees Idle and takes the locked path. The
\* self-heal claim mints a new token, so its net word is Running. The witness
\* placement (IdleAtRelinquish) keeps a runloop-owned turn Running through here.
Register ==
  /\ UNCHANGED fastMask
  /\ pumpPc = "arming"
  /\ IF held # 0
       THEN /\ parkedSyncHead' = TRUE
            /\ UNCHANGED <<redrive, selfhealPending, armed>>
            /\ syncMres' = IF (~G2 \/ driving = 0) /\ held = syncWaitFlow
                             THEN TRUE ELSE syncMres
            /\ pumpWord' = IF IdleAtRelinquish /\ owner = "runloop"
                             THEN pumpWord ELSE "idle"
       ELSE /\ parkedSyncHead' = FALSE /\ syncMres' = syncMres
            /\ IF HasItemVis /\ G5
                 THEN IF driving = 1
                        THEN /\ redrive' = (redrive \/ G7)
                             /\ UNCHANGED <<armed, selfhealPending>>
                             /\ pumpWord' = IF IdleAtRelinquish /\ owner = "runloop"
                                              THEN pumpWord ELSE "idle"
                        ELSE IF armed
                          THEN /\ armed' = FALSE /\ selfhealPending' = TRUE
                               /\ UNCHANGED redrive
                               /\ pumpWord' = "running"   \* claim mints a token
                          ELSE /\ UNCHANGED <<redrive, selfhealPending, armed>>
                               /\ pumpWord' = IF IdleAtRelinquish /\ owner = "runloop"
                                                THEN pumpWord ELSE "idle"
                 ELSE /\ UNCHANGED <<redrive, selfhealPending, armed>>
                      /\ pumpWord' = IF IdleAtRelinquish /\ owner = "runloop"
                                       THEN pumpWord ELSE "idle"
  /\ owner' = IF owner = "selfheal" THEN "none" ELSE owner
  /\ pumpPc' = "parked"
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, driving,
                 runloopPending, trailingPark, trailingFlow, current, scpc, syncOp,
                 syncWaitFlow, apc, doubleDispatch, nullInvoke, armSawEmpty>>

-----------------------------------------------------------------------------
\* RunLoop post-lock (L308-324), owner=runloop, after the turn suspended (pump
\* parked, registration done). The turn-end gap BEFORE this action - registration
\* release to this acquire, _driving still 1 - is where a Drive's redrive record
\* (or its G7-off DROP) lands. Serve a re-drive (re-claim, continue) or relinquish
\* (drop _driving, hand a reserved sync-head park to its caller - drop and Set are
\* one lock hold, see the G2 note in the header).

RunLoopContinue ==
  /\ UNCHANGED fastMask
  /\ owner = "runloop" /\ pumpPc = "parked"
  /\ redrive /\ (IF G6 THEN ~Reserved ELSE TRUE) /\ armed
  /\ redrive' = FALSE /\ armed' = FALSE /\ pumpPc' = "pull"
  /\ pumpWord' = "running"   \* re-claim = the turn continues
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 driving, owner, runloopPending, selfhealPending, trailingPark,
                 trailingFlow, current, scpc, syncOp, syncWaitFlow, syncMres, apc,
                 doubleDispatch, nullInvoke, armSawEmpty>>

RunLoopRelinquish ==
  /\ UNCHANGED fastMask
  /\ owner = "runloop" /\ pumpPc = "parked"
  /\ ~(redrive /\ (IF G6 THEN ~Reserved ELSE TRUE) /\ armed)
  /\ driving' = 0 /\ owner' = "none"
  /\ syncMres' = IF held # 0 /\ held = syncWaitFlow THEN TRUE ELSE syncMres
  \* Witness placement: Idle stored HERE, after the last storage recheck already
  \* ran (Register) - the relinquish itself never re-peeks storage.
  /\ pumpWord' = IF IdleAtRelinquish THEN "idle" ELSE pumpWord
  /\ UNCHANGED <<storage, qBuf, done, held, takePending, takeActive, parkedSyncHead,
                 redrive, armed, pumpPc, runloopPending, selfhealPending, trailingPark,
                 trailingFlow, current, scpc, syncOp, syncWaitFlow, apc,
                 doubleDispatch, nullInvoke, armSawEmpty>>

-----------------------------------------------------------------------------
CommitVis ==
  /\ UNCHANGED <<pumpWord, fastMask>>
  /\ qBuf > 0 /\ qBuf' = qBuf - 1
  /\ UNCHANGED <<storage, done, held, takePending, takeActive, parkedSyncHead,
                 driving, redrive, armed, pumpPc, owner, runloopPending, selfhealPending,
                 trailingPark, trailingFlow, current, scpc, syncOp, syncWaitFlow,
                 syncMres, apc, doubleDispatch, nullInvoke, armSawEmpty>>

-----------------------------------------------------------------------------
\* The one intended quiescent state, as an explicit self-loop so any OTHER stuck
\* state is a structural deadlock error (the captured hangs are such states).
Done ==
  /\ scpc = "done" /\ (\A k \in 1..NAsync : apc[k] = "done") /\ done = AllIds
  /\ pumpPc = "parked" /\ owner = "none" /\ Len(storage) = 0
  /\ UNCHANGED vars

Next ==
  \/ SyncEnq \/ SyncKick \/ SyncWaitWake \/ SyncTakeoverDone \/ SyncClose
  \/ (\E k \in 1..NAsync : AsyncEnq(k) \/ AsyncExecuteLocked(k) \/ AsyncExecuteFast(k))
  \/ RunLoopStart \/ SelfhealStart \/ TrailingResume
  \/ PullTakePending \/ PullTakePendingTrailing \/ PullTakeActiveMiss
  \/ PullBlockedHeld \/ PullEmpty
  \/ PullAsyncComplete \/ PullAsyncTrailing \/ PullHoldSync
  \/ WaitCoreRetry \/ WaitCoreTakeActiveDecide \/ WaitCoreArmDecide \/ Register
  \/ RunLoopContinue \/ RunLoopRelinquish
  \/ CommitVis
  \/ Done

Fairness ==
  /\ WF_vars(SyncEnq) /\ WF_vars(SyncKick) /\ WF_vars(SyncWaitWake)
  /\ WF_vars(SyncTakeoverDone) /\ WF_vars(SyncClose)
  /\ \A k \in 1..NAsync : WF_vars(AsyncEnq(k)) /\ WF_vars(AsyncExecuteLocked(k))
                          /\ WF_vars(AsyncExecuteFast(k))
  /\ WF_vars(RunLoopStart) /\ WF_vars(SelfhealStart) /\ WF_vars(TrailingResume)
  /\ WF_vars(PullTakePending) /\ WF_vars(PullTakeActiveMiss) /\ WF_vars(PullBlockedHeld)
  /\ WF_vars(PullEmpty) /\ WF_vars(PullAsyncComplete) /\ WF_vars(PullHoldSync)
  /\ WF_vars(WaitCoreRetry) /\ WF_vars(WaitCoreTakeActiveDecide)
  /\ WF_vars(WaitCoreArmDecide) /\ WF_vars(Register)
  /\ WF_vars(RunLoopContinue) /\ WF_vars(RunLoopRelinquish)
  /\ WF_vars(CommitVis)
  \* PullTakePendingTrailing / PullAsyncTrailing are NOT fair: the fault /
  \* off-executor path is optional (bodies may complete inline instead).

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
\* Safety.

\* The pump continuation is never live on two threads (Draghi's "already
\* executing" collision). SingleRunner = operational detector; OneDriver =
\* structural: while a turn executes (incl. the arm window) no dispatch token
\* is outstanding that could start a second.
SingleRunner == ~doubleDispatch
OneDriver == (pumpPc \in {"pull", "wait", "arming"})
               => (runloopPending = FALSE /\ selfhealPending = FALSE)

\* A claim never dispatches an armed-but-unregistered continuation (torn
\* MoveNext / stale-delegate invoke). Guaranteed by lock-through-OnCompleted;
\* the LockThrough=FALSE witness violates it.
NoNullInvoke == ~nullInvoke

NoDoubleProc ==
  /\ \A i, j \in 1..Len(storage) : i # j => storage[i] # storage[j]
  /\ \A f \in done : \A i \in 1..Len(storage) : storage[i] # f

FifoSync == (NSync = 2) => ((2 \in done) => (1 \in done))

\* REACHABILITY PROBE (not a correctness property): expected VIOLATED in the
\* probe config - an item became visible inside the armed-but-unregistered
\* window, proving TLC explores the v2 window interleavings.
WindowProbe == ~(pumpPc = "arming" /\ armSawEmpty /\ HasItemVis)

\* Fast-path non-vacuity probes (expected VIOLATED in their probe configs): a
\* fast-path return was taken during a live runloop-owned turn / during an
\* unowned (self-heal or trailing-resume) turn.
FastOwnedProbe   == "owned" \notin fastMask
FastUnownedProbe == "unowned" \notin fastMask

-----------------------------------------------------------------------------
\* Liveness (the captured hangs violate these).

AllProcessed == <>(done = AllIds)
SyncReturns  == <>(scpc = "done")

=============================================================================
