-------------------------- MODULE WaitProtocolCompensated --------------------------
(*
PgClientFlowSource's variant of the wait protocol (audit C2/C3), modeled
SEPARATELY from WaitProtocol.tla because it differs in load-bearing shape:

  - publish-then-flag: Enqueue writes the queue, THEN QueueNotEmpty (UQS is
    flag-then-publish), and the wait re-check reads FLAGS ONLY (HandoffAcked,
    HandoffActive, QueueNotEmpty), never the queue.
  - consumer-side flag clear: TryDequeue clears QueueNotEmpty on
    dequeue-to-empty (UQS clears its flag at wait-acquire).
  - DROP, not defer: an async producer's signal during a handoff window is
    DROPPED (EnqueueResult.Execute returns; nobody retries). The handoff
    close-out compensates: it clears HandoffActive and re-delivers a wake if
    QueueNotEmpty or IsCompleted accrued during the window. WaitProtocol.tla
    models the deferral as a DELAYED signal (fairness re-fires it), which
    cannot express the compensation or its failure modes at all.
  - RECLAIM, not drain on completion: the consumer's wait resolves completed
    BEFORE the queue check (completion BEATS the queue) and STOPS pulling, even
    with items still queued. The undispatched residual is reclaimed by Shutdown
    (DrainInertItems), unblocked by the DrainSignal the completed-resolution
    fires - modeled by ShutdownDrain, gated on cpc="done" (cpc reaches it only
    through that resolution, so it stands in for the signal AND for the executor
    having stopped: ShutdownDrain is thus the SOLE consumer, no race). UQS/
    WaitProtocol.tla instead drain the residual through the executor
    (queue-before-completion); this reorder is why PgClientFlowSource needs no
    second consumer of the SPSC queue at shutdown. The C6 hot-spin (PeekRecheck
    witness) is unaffected: it lives in the not-yet-completed phase, where the
    reorder changes nothing.

Three failure families this module exists to pin:

  STALE CLOSE (audit C2, code fixed 672e1f45): the close-out compensation is
  a pair of store->load Dekker races - producer flag-store -> HandoffActive
  read (Enqueue/Execute and Complete) against close-out HandoffActive-clear ->
  flag read. Store->load is the one reorder x86 TSO permits, so each side's
  store can sit in its store buffer past the other's load: the producer drops
  its signal against a stale-open window while the close-out skips the
  compensation against a stale-empty flag. Lost wake (item queued, executor
  suspended forever) or, on the IsCompleted face, a completion never
  re-delivered (drain hang). Modeled with buffered/committed store pairs;
  CloseFence and SignalFence toggle the Interlocked fences the fix added -
  each is INDEPENDENTLY necessary (either missing alone admits the loss).

  STALE OPEN (audit C7): the SYMMETRIC twin of the stale close, on the window
  OPEN. The close-out clear is Interlocked (full fence, the C2 fix), but the
  window OPEN is a plain Volatile.Write (release) - asymmetric. A concurrent
  Complete/Execute reading HandoffActive can see it stale-CLOSED while the
  handoff is genuinely open: it then does NOT defer/drop, but proceeds to
  claim - stealing the suspended wait the handoff is rendezvousing on (the
  executor wakes on a scheduler thread, runs the sync flow on the wrong thread
  or, on the completion face, resolves Completed out from under the handoff,
  stranding it). The MRES-wait fence that would commit the open does NOT cover
  the HEAD's fast path: only a NON-head waits on its MRES; the head sets
  HandoffActive and claims directly, its only fence being its own claim-loop
  AcquireWakeLock - which does NOT commit the store for a reader on another
  core. OpenFence toggles the fix (an Interlocked open, matching the clear).

  GATE RACE / handoff steal (audit C5, found by this module's construction):
  Execute's and Complete's HandoffActive gate is read OUTSIDE the wake lock,
  then the claim happens inside it - read-then-claim, not atomic.
  WaitProtocol.tla fused gate and claim into one action, hiding the gap. The
  interleaving: the gate reads window-closed (genuinely - the handoff hasn't
  opened yet), the handoff then opens, publishes, observes the suspension and
  acks, and only THEN the stale-gated claim fires: it steals the suspended
  wait, the executor wakes on a scheduler thread, its TryGetNext takes the
  ACKED handoff slot, and the sync flow runs on the WRONG thread while
  EnqueueSyncWithHandoff returns mid-flight. No weak memory required - this
  is a pure interleaving hole. GateUnderLock=TRUE models the fix: the gate
  re-read and the claim execute under one wake-lock hold. The fix is sound
  without fencing the window-open store because of the MRES ordering: a claim
  that precedes the open clears the suspension observation (ClearOnClaim), so
  the handoff producer cannot observe it and blocks until the executor
  re-arms; a claim after the handoff observed implies the open committed
  (the producer's MRES wait fences its own prior open store), so the
  under-lock re-read sees the window and defers. (NOTE: this soundness rests
  on the open being committed by the time it is observed - which OpenFence=FALSE
  exposes as NOT guaranteed for the head fast path.)

Modeling notes (fidelity decisions):
  - The window OPEN is now modeled with a buffered/committed pair
    (openBuf/windowActive), toggled by OpenFence - NO LONGER atomic-committed.
    The handoff's own wake-lock acquire (HandoffObserveAck / HandoffClaimInline)
    commits its buffered open (a fence on its own thread); CommitOpen models
    natural propagation; the close-out's Interlocked clear drains it.
  - The consumer's flag clear is modeled committed: its staleness can only
    produce a SPURIOUS compensation wake (reads stale TRUE), which is benign.
  - The three legacy mechanisms (under-lock re-check, lock-through
    registration, clear-on-claim) are hardcoded TRUE; their witnesses live in
    WaitProtocol.tla.
  - A claim's lock acquire is a full fence on the claiming thread, so it
    commits that thread's buffered store (qne for the producer, isCompleted
    for the completer, the window clear for the close-out wake, the window
    open for the handoff).

Properties:
  TypeOK
  NoWrongThreadTake - no scheduler-thread TryGetNext ever takes an acked
                      handoff (fails with GateUnderLock=FALSE: the steal;
                      and with OpenFence=FALSE: the stale-open steal)
  HandoffInline     - a handoff producer past its claim saw its flow consumed
  Drains            - liveness: every queued item drains - via PullHit while
                      running, or ShutdownDrain (the reclaim) once completed
                      (fails with PeekRecheck=FALSE: the C6 hot-spin)
  AllConsumed       - liveness: everything drains and completion is observed
                      (fails with either close fence toggle FALSE: the stale
                      close)
  HandoffReturns    - the handoff producer is never stranded (fails with
                      OpenFence=FALSE: a stale-open Complete resolves Completed
                      out from under the rendezvous)
*)

EXTENDS Naturals, TLC

CONSTANTS MaxItems, CloseFence, SignalFence, OpenFence, GateUnderLock, PeekRecheck

VARIABLES
  queue,        \* published items pending consumption
  enqueued,     \* total ever published
  consumed,     \* total consumed
  qne,          \* committed QueueNotEmpty
  qneBuf,       \* producer's buffered QueueNotEmpty := TRUE (store buffer)
  isCompleted,  \* committed IsCompleted
  compBuf,      \* completer's buffered IsCompleted := TRUE
  windowActive, \* committed HandoffActive
  openBuf,      \* handoff's buffered HandoffActive := TRUE (store buffer)
  clearBuf,     \* close-out's buffered HandoffActive := FALSE
  lock,         \* wake spinlock held?
  pending,      \* armed wait
  contStored,   \* continuation registered
  suspendedSig, \* the suspension MRES
  handoff,      \* 0..1: the sync flow in the handoff slot
  acked,        \* HandoffAcked
  cpc,          \* consumer: "run" | "locked" | "armed" | "suspended" | "done"
  ppc,          \* producer: "idle" | "published" | "flagged" | "gated"
  hpc,          \* handoff:  "idle" | "waiting" | "acked" | "closing" | "closeRead" | "done"
  kpc,          \* completer: "idle" | "stored" | "gated" | "done"
  wrongThreadTake

vars == <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
          windowActive, openBuf, clearBuf, lock, pending, contStored, suspendedSig,
          handoff, acked, cpc, ppc, hpc, kpc, wrongThreadTake>>

TypeOK ==
  /\ queue \in 0..MaxItems
  /\ enqueued \in 0..MaxItems
  /\ consumed \in 0..MaxItems
  /\ consumed + queue = enqueued
  /\ qne \in BOOLEAN /\ qneBuf \in BOOLEAN
  /\ isCompleted \in BOOLEAN /\ compBuf \in BOOLEAN
  /\ windowActive \in BOOLEAN /\ openBuf \in BOOLEAN /\ clearBuf \in BOOLEAN
  /\ lock \in BOOLEAN /\ pending \in BOOLEAN /\ contStored \in BOOLEAN
  /\ suspendedSig \in BOOLEAN
  /\ handoff \in 0..1 /\ acked \in BOOLEAN
  /\ cpc \in {"run", "locked", "armed", "suspended", "done"}
  /\ ppc \in {"idle", "published", "flagged", "gated"}
  /\ hpc \in {"idle", "waiting", "acked", "closing", "closeRead", "done"}
  /\ kpc \in {"idle", "stored", "gated", "done"}
  /\ wrongThreadTake \in BOOLEAN

Init ==
  /\ queue = 0 /\ enqueued = 0 /\ consumed = 0
  /\ qne = FALSE /\ qneBuf = FALSE
  /\ isCompleted = FALSE /\ compBuf = FALSE
  /\ windowActive = FALSE /\ openBuf = FALSE /\ clearBuf = FALSE
  /\ lock = FALSE /\ pending = FALSE /\ contStored = FALSE
  /\ suspendedSig = FALSE
  /\ handoff = 0 /\ acked = FALSE
  /\ cpc = "run" /\ ppc = "idle" /\ hpc = "idle" /\ kpc = "idle"
  /\ wrongThreadTake = FALSE

-----------------------------------------------------------------------------
\* The claim, shared shape: consume the armed wait, reset the suspension
\* observation (clear-on-claim, hardcoded), wake the consumer. Callers bracket
\* it with their own fence/commit effects.

ClaimWakes ==
  /\ pending /\ contStored
  /\ pending' = FALSE
  /\ contStored' = FALSE
  /\ suspendedSig' = FALSE
  /\ cpc' = "run"

ClaimNoop ==
  /\ ~pending
  /\ UNCHANGED <<pending, contStored, suspendedSig, cpc>>

-----------------------------------------------------------------------------
\* Async producer (single, serialized upstream). Pg order: publish, then flag,
\* then the HandoffActive gate, then the claim.

EnqPublish ==
  /\ ppc = "idle" /\ enqueued < MaxItems /\ ~isCompleted
  /\ queue' = queue + 1 /\ enqueued' = enqueued + 1
  /\ ppc' = "published"
  /\ UNCHANGED <<consumed, qne, qneBuf, isCompleted, compBuf, windowActive,
                 openBuf, clearBuf, lock, pending, contStored, suspendedSig,
                 handoff, acked, cpc, hpc, kpc, wrongThreadTake>>

\* QueueNotEmpty := TRUE. Fenced (Interlocked, the C2 fix) commits at once;
\* unfenced sits in the store buffer until CommitQne or a later lock fence.
EnqFlag ==
  /\ ppc = "published"
  /\ IF SignalFence
       THEN qne' = TRUE /\ qneBuf' = qneBuf
       ELSE qneBuf' = TRUE /\ qne' = qne
  /\ ppc' = "flagged"
  /\ UNCHANGED <<queue, enqueued, consumed, isCompleted, compBuf, windowActive,
                 openBuf, clearBuf, lock, pending, contStored, suspendedSig,
                 handoff, acked, cpc, hpc, kpc, wrongThreadTake>>

\* Store buffer drain (eventual; a fence elsewhere on the thread also drains).
CommitQne ==
  /\ qneBuf
  /\ qne' = TRUE /\ qneBuf' = FALSE
  /\ UNCHANGED <<queue, enqueued, consumed, isCompleted, compBuf, windowActive,
                 openBuf, clearBuf, lock, pending, contStored, suspendedSig,
                 handoff, acked, cpc, ppc, hpc, kpc, wrongThreadTake>>

\* SHIPPED gate shape (~GateUnderLock): the HandoffActive read is its own
\* instruction, outside the wake lock. A TRUE read DROPS the signal (the
\* close-out compensates); a FALSE read proceeds to the claim with a verdict
\* that can go stale before the claim runs - the steal window.
ExecuteGateRead ==
  /\ ~GateUnderLock
  /\ ppc = "flagged"
  /\ IF windowActive
       THEN ppc' = "idle"
       ELSE ppc' = "gated"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, lock, pending, contStored,
                 suspendedSig, handoff, acked, cpc, hpc, kpc, wrongThreadTake>>

ExecuteClaim ==
  /\ ~GateUnderLock
  /\ ppc = "gated" /\ ~lock
  /\ (ClaimWakes \/ ClaimNoop)
  /\ qne' = (qne \/ qneBuf)    \* the lock acquire fences the producer's buffer
  /\ qneBuf' = FALSE
  /\ ppc' = "idle"
  /\ UNCHANGED <<queue, enqueued, consumed, isCompleted, compBuf, windowActive,
                 openBuf, clearBuf, lock, handoff, acked, hpc, kpc, wrongThreadTake>>

\* FIXED gate shape (GateUnderLock): gate re-read and claim under one wake-lock
\* hold. The acquire is the fence (commits the producer's flag); the re-read
\* sees the committed window state at claim time.
ExecuteGatedClaim ==
  /\ GateUnderLock
  /\ ppc = "flagged" /\ ~lock
  /\ qne' = (qne \/ qneBuf)
  /\ qneBuf' = FALSE
  /\ IF windowActive
       THEN UNCHANGED <<pending, contStored, suspendedSig, cpc>>
       ELSE (ClaimWakes \/ ClaimNoop)
  /\ ppc' = "idle"
  /\ UNCHANGED <<queue, enqueued, consumed, isCompleted, compBuf, windowActive,
                 openBuf, clearBuf, lock, handoff, acked, hpc, kpc, wrongThreadTake>>

-----------------------------------------------------------------------------
\* Complete (one-shot, after the producer is done): IsCompleted store, then
\* the same gate-then-claim shape as Execute, with the same toggles.

CompleteStore ==
  /\ kpc = "idle" /\ enqueued = MaxItems /\ ppc = "idle"
  /\ IF SignalFence
       THEN isCompleted' = TRUE /\ compBuf' = compBuf
       ELSE compBuf' = TRUE /\ isCompleted' = isCompleted
  /\ kpc' = "stored"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, windowActive,
                 openBuf, clearBuf, lock, pending, contStored, suspendedSig,
                 handoff, acked, cpc, ppc, hpc, wrongThreadTake>>

CommitComp ==
  /\ compBuf
  /\ isCompleted' = TRUE /\ compBuf' = FALSE
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, windowActive,
                 openBuf, clearBuf, lock, pending, contStored, suspendedSig,
                 handoff, acked, cpc, ppc, hpc, kpc, wrongThreadTake>>

CompleteGateRead ==
  /\ ~GateUnderLock
  /\ kpc = "stored"
  /\ IF windowActive
       THEN kpc' = "done"      \* dropped; the close-out re-delivers
       ELSE kpc' = "gated"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, lock, pending, contStored,
                 suspendedSig, handoff, acked, cpc, ppc, hpc, wrongThreadTake>>

CompleteClaim ==
  /\ ~GateUnderLock
  /\ kpc = "gated" /\ ~lock
  /\ (ClaimWakes \/ ClaimNoop)
  /\ isCompleted' = (isCompleted \/ compBuf)
  /\ compBuf' = FALSE
  /\ kpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, windowActive,
                 openBuf, clearBuf, lock, handoff, acked, ppc, hpc, wrongThreadTake>>

CompleteGatedClaim ==
  /\ GateUnderLock
  /\ kpc = "stored" /\ ~lock
  /\ isCompleted' = (isCompleted \/ compBuf)
  /\ compBuf' = FALSE
  /\ IF windowActive
       THEN UNCHANGED <<pending, contStored, suspendedSig, cpc>>
       ELSE (ClaimWakes \/ ClaimNoop)
  /\ kpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, windowActive,
                 openBuf, clearBuf, lock, handoff, acked, ppc, hpc, wrongThreadTake>>

-----------------------------------------------------------------------------
\* Sync-handoff producer (one chain; the SyncWaiterLock baton serializes).

\* Open the window (Volatile.Write HandoffActive := TRUE under SyncWaiterLock)
\* + publish the slot, then block on the suspension observation. OpenFence=TRUE
\* models the fix (Interlocked open, committed at once); FALSE (the shipped
\* code) buffers the open - a concurrent reader keeps seeing the window CLOSED
\* until openBuf commits (the handoff's own fence, or CommitOpen propagation).
HandoffOpen ==
  /\ hpc = "idle" /\ ~isCompleted
  /\ IF OpenFence
       THEN windowActive' = TRUE /\ openBuf' = openBuf
       ELSE openBuf' = TRUE /\ windowActive' = windowActive
  /\ handoff' = 1
  /\ hpc' = "waiting"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 clearBuf, lock, pending, contStored, suspendedSig, acked,
                 cpc, ppc, kpc, wrongThreadTake>>

\* Store buffer drain for the open (eventual; the handoff's own fence below
\* also drains it).
CommitOpen ==
  /\ openBuf
  /\ windowActive' = TRUE /\ openBuf' = FALSE
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 clearBuf, lock, pending, contStored, suspendedSig, handoff,
                 acked, cpc, ppc, hpc, kpc, wrongThreadTake>>

\* WaitForSuspended returned + HandoffAcked := TRUE (the ack is ordered after
\* the observation in code; fusing them loses no interleaving because nothing
\* the protocol does distinguishes the gap - the acked flag is what gates
\* consumption, and it is set here). The MRES wait is a full fence: it commits
\* this thread's buffered open.
HandoffObserveAck ==
  /\ hpc = "waiting"
  /\ suspendedSig
  /\ windowActive' = (windowActive \/ openBuf)
  /\ openBuf' = FALSE
  /\ acked' = TRUE
  /\ hpc' = "acked"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 clearBuf, lock, pending, contStored,
                 suspendedSig, handoff, cpc, ppc, kpc, wrongThreadTake>>

\* Inline claim: consume the wait, run the executor on this thread, and the
\* executor's retry takes the acked slot - fused, per the code's contract that
\* the flow has been processed by the time Signal(false) returns. The claim's
\* lock acquire is a full fence: it commits this thread's buffered open.
HandoffClaimInline ==
  /\ hpc = "acked" /\ ~lock
  /\ pending /\ contStored
  /\ pending' = FALSE
  /\ contStored' = FALSE
  /\ suspendedSig' = FALSE
  /\ windowActive' = (windowActive \/ openBuf)
  /\ openBuf' = FALSE
  /\ handoff' = 0
  /\ acked' = FALSE
  /\ cpc' = "run"
  /\ hpc' = "closing"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 clearBuf, lock, ppc, kpc, wrongThreadTake>>

\* Claim found nothing pending: the rendezvous was stolen (the gate race took
\* the wait first). The code's Debug.Assert(claimed) face; the producer
\* proceeds to close out regardless.
HandoffClaimNoop ==
  /\ hpc = "acked" /\ ~lock
  /\ ~pending
  /\ windowActive' = (windowActive \/ openBuf)
  /\ openBuf' = FALSE
  /\ hpc' = "closing"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 clearBuf, lock, pending, contStored,
                 suspendedSig, handoff, acked, cpc, ppc, kpc, wrongThreadTake>>

\* Close-out, step 1: clear HandoffActive. Fenced (Interlocked, the C2 fix)
\* commits at once AND drains this thread's buffered open (same location, the
\* Interlocked supersedes the prior Volatile.Write); unfenced sits in the
\* buffer - others keep reading the window as OPEN while this thread's
\* compensation reads run.
CloseClear ==
  /\ hpc = "closing"
  /\ openBuf' = FALSE
  /\ IF CloseFence
       THEN windowActive' = FALSE /\ clearBuf' = clearBuf
       ELSE clearBuf' = TRUE /\ windowActive' = windowActive
  /\ hpc' = "closeRead"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 lock, pending, contStored, suspendedSig, handoff, acked,
                 cpc, ppc, kpc, wrongThreadTake>>

\* Close-out, step 2a: compensation reads saw deferred work - deliver the wake
\* (Signal(async): the lock acquire fences this thread, committing the clear).
CloseReadWake ==
  /\ hpc = "closeRead" /\ ~lock
  /\ qne \/ isCompleted
  /\ (ClaimWakes \/ ClaimNoop)
  /\ windowActive' = FALSE
  /\ clearBuf' = FALSE
  /\ hpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 openBuf, lock, handoff, acked, ppc, kpc, wrongThreadTake>>

\* Close-out, step 2b: nothing accrued (per this thread's possibly-stale view)
\* - no wake, no fence; a buffered clear drains via CommitClear eventually.
CloseReadSkip ==
  /\ hpc = "closeRead"
  /\ ~(qne \/ isCompleted)
  /\ hpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, lock, pending, contStored,
                 suspendedSig, handoff, acked, cpc, ppc, kpc, wrongThreadTake>>

CommitClear ==
  /\ clearBuf
  /\ windowActive' = FALSE /\ clearBuf' = FALSE
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 openBuf, lock, pending, contStored, suspendedSig, handoff, acked,
                 cpc, ppc, hpc, kpc, wrongThreadTake>>

-----------------------------------------------------------------------------
\* Consumer (the executor's pull loop, Pg shape).

\* The wait re-check's work test (audit C6). PeekRecheck=TRUE is the fix: peek
\* the QUEUE (consumer-side, SPSC-legal; under publish-then-flag a TRUE flag
\* implies the item is already visible, so the peek subsumes it). FALSE is the
\* shipped flag-only re-check: a stale TRUE flag - the producer's flag store
\* landing after the consumer's dequeue-to-empty clear - makes the re-check
\* retry forever against an empty queue: the executor hot-spins instead of
\* arming, and completed-resolution is starved behind it (drain hang at 100%
\* core). The flag's remaining role under the fix is the close-out
\* compensation only, where a stale TRUE merely costs a spurious wake.
RecheckSeesWork == IF PeekRecheck THEN queue > 0 ELSE qne

\* TryGetNext queue hit: gated on the window (don't hijack the sync caller's
\* thread); clears the flag on dequeue-to-empty (consumer-side clear).
PullHit ==
  /\ cpc = "run" /\ ~windowActive /\ queue > 0
  /\ queue' = queue - 1 /\ consumed' = consumed + 1
  /\ qne' = IF queue - 1 = 0 THEN FALSE ELSE qne
  /\ UNCHANGED <<enqueued, qneBuf, isCompleted, compBuf, windowActive,
                 openBuf, clearBuf, lock, pending, contStored, suspendedSig,
                 handoff, acked, cpc, ppc, hpc, kpc, wrongThreadTake>>

\* TryGetNext handoff take on a NON-inline wake: only reachable when a stale
\* gate stole the rendezvous (the inline claim consumes the slot atomically,
\* so an acked slot coexisting with a running consumer IS the steal). The sync
\* flow runs on the wrong thread; the property below pins it.
PullHandoffStolen ==
  /\ cpc = "run" /\ acked /\ handoff = 1
  /\ handoff' = 0
  /\ acked' = FALSE
  /\ wrongThreadTake' = TRUE
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, lock, pending, contStored,
                 suspendedSig, cpc, ppc, hpc, kpc>>

\* Miss -> WaitForNextAsync acquires the wake lock. Pg clears nothing here.
WaitAcquire ==
  /\ cpc = "run" /\ ~lock
  /\ ~(acked /\ handoff = 1)
  /\ (windowActive \/ queue = 0)
  /\ lock' = TRUE
  /\ cpc' = "locked"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, pending, contStored, suspendedSig,
                 handoff, acked, ppc, hpc, kpc, wrongThreadTake>>

\* Flag-only availability re-check (peek-style; consumption stays in
\* TryGetNext): HandoffAcked always retries; queue work retries only when NOT
\* completed - completion now BEATS the queue (see WaitRecheckCompleted), so a
\* queued item no longer outranks a pending completion.
WaitRecheckRetry ==
  /\ cpc = "locked"
  /\ acked \/ (~isCompleted /\ ~windowActive /\ RecheckSeesWork)
  /\ lock' = FALSE
  /\ cpc' = "run"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, pending, contStored, suspendedSig,
                 handoff, acked, ppc, hpc, kpc, wrongThreadTake>>

\* Completed-resolution BEATS the queue (the reclaim policy): it resolves even
\* with items still queued - the residual is reclaimed by ShutdownDrain, not
\* drained through the executor. Still DEFERS during a window (resolving under a
\* waiting sync producer strands its rendezvous; the close-out re-delivers). The
\* DrainSignal Shutdown waits on is fired here in the code; cpc'="done" stands in
\* for it (ShutdownDrain gates on it).
WaitRecheckCompleted ==
  /\ cpc = "locked"
  /\ ~acked
  /\ isCompleted /\ ~windowActive
  /\ lock' = FALSE
  /\ cpc' = "done"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, pending, contStored, suspendedSig,
                 handoff, acked, ppc, hpc, kpc, wrongThreadTake>>

WaitArm ==
  /\ cpc = "locked"
  /\ ~(acked \/ (~isCompleted /\ ~windowActive /\ RecheckSeesWork))
  /\ ~(~acked /\ isCompleted /\ ~windowActive)
  /\ pending' = TRUE
  /\ cpc' = "armed"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, lock, contStored, suspendedSig,
                 handoff, acked, ppc, hpc, kpc, wrongThreadTake>>

\* Registration: store the continuation, set the suspension observation under
\* the lock (the C1 bracket), release (lock-through).
WaitRegister ==
  /\ cpc = "armed"
  /\ contStored' = TRUE
  /\ suspendedSig' = TRUE
  /\ lock' = FALSE
  /\ cpc' = "suspended"
  /\ UNCHANGED <<queue, enqueued, consumed, qne, qneBuf, isCompleted, compBuf,
                 windowActive, openBuf, clearBuf, pending, handoff, acked, ppc, hpc,
                 kpc, wrongThreadTake>>

\* Shutdown's residual reclaim (DrainInertItems), unblocked by the DrainSignal
\* that WaitRecheckCompleted fires at the completed-resolution. Modeled by the
\* cpc="done" gate: cpc reaches "done" ONLY through that resolution, so it stands
\* in for the fired signal AND for the executor having stopped pulling - which is
\* what makes this the SOLE consumer of the queue (the barrier the DrainSignal
\* buys). This replaces the executor draining the queue itself on completion:
\* under completion-before-queue the executor stops with items still queued, and
\* the residual is reclaimed here. (Under the old queue-before-completion shape
\* the executor drains via PullHit first, so cpc="done" finds an empty queue and
\* this is a no-op - harmless.)
ShutdownDrain ==
  /\ cpc = "done"
  /\ queue > 0
  /\ queue' = 0
  /\ consumed' = consumed + queue
  /\ qne' = FALSE
  /\ UNCHANGED <<enqueued, qneBuf, isCompleted, compBuf, windowActive, openBuf,
                 clearBuf, lock, pending, contStored, suspendedSig, handoff, acked,
                 cpc, ppc, hpc, kpc, wrongThreadTake>>

-----------------------------------------------------------------------------

Next ==
  \/ EnqPublish \/ EnqFlag \/ CommitQne
  \/ ExecuteGateRead \/ ExecuteClaim \/ ExecuteGatedClaim
  \/ CompleteStore \/ CommitComp
  \/ CompleteGateRead \/ CompleteClaim \/ CompleteGatedClaim
  \/ HandoffOpen \/ CommitOpen \/ HandoffObserveAck \/ HandoffClaimInline \/ HandoffClaimNoop
  \/ CloseClear \/ CloseReadWake \/ CloseReadSkip \/ CommitClear
  \/ PullHit \/ PullHandoffStolen
  \/ WaitAcquire \/ WaitRecheckRetry \/ WaitRecheckCompleted
  \/ WaitArm \/ WaitRegister
  \/ ShutdownDrain

\* The producer may stop before MaxItems (no WF on EnqPublish); everything
\* in-flight finishes, buffers drain, and the chains run to completion.
Fairness ==
  /\ WF_vars(EnqFlag) /\ WF_vars(CommitQne)
  /\ WF_vars(ExecuteGateRead) /\ WF_vars(ExecuteClaim) /\ WF_vars(ExecuteGatedClaim)
  \* No WF on CompleteStore: completion is a shutdown event and may never
  \* arrive; mid-run losses must be caught without its end-of-run sweep (Drains).
  /\ WF_vars(CommitComp)
  /\ WF_vars(CompleteGateRead) /\ WF_vars(CompleteClaim) /\ WF_vars(CompleteGatedClaim)
  /\ WF_vars(CommitOpen)
  /\ WF_vars(HandoffObserveAck) /\ WF_vars(HandoffClaimInline) /\ WF_vars(HandoffClaimNoop)
  /\ WF_vars(CloseClear) /\ WF_vars(CloseReadWake) /\ WF_vars(CloseReadSkip)
  /\ WF_vars(CommitClear)
  /\ WF_vars(PullHit) /\ WF_vars(PullHandoffStolen)
  /\ WF_vars(WaitAcquire) /\ WF_vars(WaitRecheckRetry)
  /\ WF_vars(WaitRecheckCompleted) /\ WF_vars(WaitArm) /\ WF_vars(WaitRegister)
  /\ WF_vars(ShutdownDrain)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

\* No scheduler-thread consumer ever takes an acked handoff: the sync flow
\* runs on its caller's thread, full stop.
NoWrongThreadTake == ~wrongThreadTake

\* A handoff producer past its claim saw its flow leave the slot.
HandoffInline == (hpc \in {"closing", "closeRead", "done"}) => (handoff = 0)

\* Every published item is eventually consumed - drained through the executor
\* (PullHit) while running, or reclaimed by ShutdownDrain once completed. A
\* mid-run lost wake (queue > 0, NOT yet completed, executor stuck) reaches
\* neither - ShutdownDrain gates on cpc="done" - so it still fails this rather
\* than hiding behind completion's sweep.
Drains == (queue > 0) ~> (queue = 0)

\* Everything produced is consumed and completion observed.
AllConsumed == (isCompleted /\ enqueued = MaxItems /\ kpc = "done") ~>
                 (consumed = MaxItems /\ handoff = 0 /\ cpc = "done")

\* The handoff producer is never stranded at the rendezvous.
HandoffReturns == (hpc = "waiting") ~> (hpc = "done")

=============================================================================
