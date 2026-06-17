--------------------------- MODULE PipelineShutdown ---------------------------
(*
Pipeline shutdown coordination. Three INDEPENDENT shutdown-regime protocols share
this module, each its own decoupled Spec (they have no shared variables; the
module composes them by freezing the other protocols' variables at the Next
level, NOT as a product - a product would make each witness explore the other
protocols' interleavings and muddy its focused trace). Pick one Spec per config:

  GateSpec  - the lifetime contract between CompleteAsync, the executor's
              terminal Dispose, and an external CancellationToken (the
              Complete-after-Dispose race). Configs: PipelineShutdown_Gate.cfg,
              PipelineShutdown_GateRaceWitness.cfg.

  SlotSpec  - DrainSlotInline's main-release deposit-serve under shutdown (the
              lost wake behind CompleteAsyncDrainsWaiters' intermittent hang).
              Configs: PipelineShutdown_Slot.cfg, PipelineShutdown_SlotDropWitness.cfg.

  QueueSpec - the queue twin of the slot drop. DrainReadyWaiters' deposit-serve is
              already unconditional, so the question is who drains an item that
              materializes after the drainer's cancellation-gated recheck is off.
              Configs: PipelineShutdown_Queue.cfg, PipelineShutdown_QueueBaseline.cfg,
              PipelineShutdown_QueueGatedWitness.cfg.

==============================================================================
GATE PROTOCOL (GateSpec)
==============================================================================
The actors:

  CompleteAsync caller - external thread invoking pipeline.CompleteAsync. The first
                         caller through the _completing CAS sets _completing = TRUE
                         and calls _enumerator.Complete(); subsequent callers just
                         return the executor task. _enumerator.Complete drives the
                         source's wake/cancel which makes the executor's
                         WaitForNextAsync resolve and the main loop break.

  External CT cancellation - the test's CancellationTokenSource, linked into the
                             Enumerator's internal _cts via
                             CancellationTokenSource.CreateLinkedTokenSource. When
                             the test cancels, the linkage fires a CTS.Cancel on
                             the Enumerator's _cts, which runs the registered
                             callback (TestObservableQueueSource line 153:
                             WakeSignal.Complete). This wakes the executor without
                             touching Pipeline._completing.

  Executor exit - the executor's main loop returns false from WaitForNextAsync
                  (the source signaled completion via either of the two paths above)
                  and falls through to terminal cleanup: _enumerator.DisposeAsync()
                  which disposes _cts. Once DisposeAsync returns the _executionTask
                  resolves.

The hazard: when external-CT cancellation drives the executor exit, _completing
stays FALSE. A later CompleteAsync caller passes the _completing CAS, calls
_enumerator.Complete on a disposed _cts, and observes an ObjectDisposedException
(or a cascading NRE through the now-broken CTS callback registration). The
_completing flag does not capture the "executor self-completed via external CT"
state, so it cannot guard the dispose race.

This spec drives the fix: extend the _completing flag (or pair it with an executor-
exit signal) so that the executor's terminal path SHUTS THE DOOR on external
CompleteAsync callers before _cts is disposed.

Witness toggle:
  ExecutorClosesGate - the fix: the executor sets _completing = TRUE before
                       DisposeAsync. A subsequent CompleteAsync sees _completing
                       and skips _enumerator.Complete. Without it: CompleteAsync
                       races DisposeAsync and may invoke a disposed CTS.

Implementation note (June 2026, post-spec): the caller-signals / executor-awaits
handoff is implemented over two fields (Pipeline._shutdownState and
Pipeline._shutdownCallerDone) and is sensitive to the StoreLoad-reorder hazard
that this spec does NOT model. A first implementation used Volatile.Write of
state=Done on the caller followed by Volatile.Read of the TCS field, with the
symmetric pair on the executor side (V.Write tcs, V.Read state). On weak-memory
architectures the StoreLoad reorder lets both sides observe each other as "not
yet written" - the executor parks on a TCS the caller has already read as null
(observed: standalone HangRepro hung at iter 51 on Apple Silicon; dump showed
_shutdownState=2, _shutdownCallerDone=executor's TCS, executor d__41 parked at
the await with the caller's TrySetResult already past). Replacing both Writes
with Interlocked.Exchange (full fences) precludes the IRIW-shaped trace and
matches this spec's intent: the gate-close and the handoff are a single ordered
event each side observes consistently.

Gate properties:
  NoCompleteAfterDispose - _enumerator.Complete is never invoked after
                           _enumerator.DisposeAsync has run.
  ShutdownTerminates     - liveness: every shutdown path eventually reaches a
                           quiescent state with executor exited and disposed.

==============================================================================
SLOT-SERVE PROTOCOL (SlotSpec)
==============================================================================
DrainSlotInline's main-release deposit-serve under shutdown (Pipeline.cs
~1200-1228). The lost wake behind CompleteAsyncDrainsWaiters' intermittent hang
(dump .repros/whenall_lost_sources/dumps/rider_wedge_post_pin.dmp: drainSignal
armed, advancer word free, one waiter resident with a completed task, its
callback continuation already fired, nobody draining). DrainOnCompletionAsync
then waits on Count > 0 forever. The park point is D2 (store callbacks) crossed
with the PendingWordLatch deposit/serve protocol (D1), in the one regime the
Pipeline.tla state machine does not model: CompletionToken cancelled.

Two threads share the PendingWordLatch word (free / held / held+pending) plus
the _drainSignal dirty flag.

  DRAINER (item1's DrainSlotInline, already past its claimed item, at the release
           tail):

    var serveDeposit = _advancing.ReleaseAndCheckPending(); // latch -> free,
                                                            // consume pending bit
    // SHIPPED, BUGGY (ServeDepositUnconditional = FALSE):
    if (!CompletionToken.IsCancellationRequested        // <- gates BOTH
        && (serveDeposit || _drainSignal)
        && _advancing.TryAcquireOrFlagPending())
    { reclaim; }
    // FIX (ServeDepositUnconditional = TRUE), mirroring the two sites that
    // already get it right (DrainSlotInline's claim-fail path; DrainReadyWaiters'
    // tail):
    if (serveDeposit)                                   // hard obligation: ALWAYS serve
    { if (_advancing.TryAcquireOrFlagPending()) reclaim; }
    else if (!CompletionToken.IsCancellationRequested  // dirty-flag reclaim: gated
             && _drainSignal && _advancing.TryAcquireOrFlagPending())
    { reclaim; }

  SUCCESSOR (item2's OnWaiterTaskCompleted, fired when item2's pipeline task
             faults under shutdown):

    _drainSignal = true;
    if (!_advancing.TryAcquireOrFlagPending()) return;  // latch busy -> DEPOSIT, bail
    DrainReadyWaiters();                                // latch free -> drain self

The bug: the successor bails while the drainer holds the latch, so its
TryAcquireOrFlagPending DEPOSITS the obligation and bails (its one wake spent).
The drainer's ReleaseAndCheckPending CONSUMES that deposit (Latch.cs), but the
shipped reclaim gate `!IsCancellationRequested && (serveDeposit || _drainSignal)`
short-circuits under shutdown, so the just-consumed deposit is DROPPED. The
dirty-flag branch cannot rescue it (same gate); no further callback fires. The
fix serves the consumed deposit UNCONDITIONALLY (a deposit is a hard obligation
handoff) and gates only the dirty-flag reclaim on cancellation.

Slot toggles:
  Cancelled                 - TRUE models the CompletionToken cancelled regime
                              (CompleteAsync has fired). The whole scenario is
                              post-cancellation: item2's task faults BECAUSE of the
                              cancel, so every step runs with IsCancellationRequested
                              true. Monotonic, so modeled as a constant.
  ServeDepositUnconditional - FALSE = shipped code (the deposit and the dirty-flag
                              reclaim share one !IsCancellationRequested gate -> the
                              deposit is dropped under shutdown). TRUE = the fix.

Slot property:
  EventuallyDrained - a successor whose task completed eventually drains. FAILS
                      with ServeDepositUnconditional = FALSE under Cancelled = TRUE;
                      HOLDS with TRUE. Under Cancelled = FALSE both serve
                      unconditionally, so it holds either way (baseline).
*)

EXTENDS Naturals, TLC

CONSTANTS
    \* Gate protocol.
    MaxCallers,        \* bound on number of CompleteAsync callers
    ExecutorClosesGate,\* fix toggle: executor sets _completing before DisposeAsync
    \* Slot-serve protocol.
    Cancelled,                 \* CompletionToken cancelled regime (shared with the
                               \* queue-serve protocol below)
    ServeDepositUnconditional, \* FALSE = shipped/buggy, TRUE = the fix
    \* Queue-serve protocol (gap #2: the queue twin of the slot deposit-drop).
    MaterializerTailUngated    \* The materializing commit's OWN tail (escalation nudge /
                               \* inline+registered OnWaiterTaskCompleted) is the item's
                               \* guaranteed servicer - it always exists. TRUE = shipped
                               \* code: that tail is UNGATED, so it serves regardless of
                               \* cancellation. FALSE = the hypothetical where the tail is
                               \* ALSO under the !IsCancellationRequested gate (like the
                               \* slot serve was) - then under shutdown nothing serves the
                               \* item and the queue strands. The discriminator that shows
                               \* the ungated tail (not the gated recheck) is load-bearing.

VARIABLES
    \* Gate protocol.
    completing,        \* Pipeline._completing flag
    ctCanceled,        \* external CancellationToken has been canceled
    executorPhase,     \* "running" | "waking" | "exiting" | "disposing" | "done"
    enumeratorDisposed,\* Enumerator._cts has been disposed
    callerPhase,       \* "idle" | "claimed" | "calledComplete" | "returned"
    callersServed,     \* number of CompleteAsync calls completed
    completeAfterDispose, \* TRUE if a Complete call ran after disposal (the bug)
    \* Slot-serve protocol.
    latch,        \* the PendingWordLatch word: "free" | "held" | "heldPending"
    drainSignal,  \* _drainSignal dirty flag
    serveDeposit, \* the drainer's captured ReleaseAndCheckPending result (its local)
    drn,          \* drainer pc
    succ,         \* successor (item2) lifecycle
    \* Queue-serve protocol.
    qItem,        \* a committed item materializing (mid enqueue->increment / slot->queue
                  \* move): "materializing" | "ready" | "drained"
    qSignal,      \* _drainSignal for the queue pass (the SignalConservation token)
    qDrainer      \* the drainer doing a pass that finds nothing: "pass" | "released" | "done"

gateVars == << completing, ctCanceled, executorPhase, enumeratorDisposed,
               callerPhase, callersServed, completeAfterDispose >>
slotVars == << latch, drainSignal, serveDeposit, drn, succ >>
queueVars == << qItem, qSignal, qDrainer >>
vars     == << completing, ctCanceled, executorPhase, enumeratorDisposed,
               callerPhase, callersServed, completeAfterDispose,
               latch, drainSignal, serveDeposit, drn, succ,
               qItem, qSignal, qDrainer >>

TypeOK ==
    \* Gate.
    /\ completing \in BOOLEAN
    /\ ctCanceled \in BOOLEAN
    /\ executorPhase \in {"running", "waking", "exiting", "disposing", "done"}
    /\ enumeratorDisposed \in BOOLEAN
    /\ callerPhase \in {"idle", "claimed", "calledComplete", "returned"}
    /\ callersServed \in 0..MaxCallers
    /\ completeAfterDispose \in BOOLEAN
    \* Slot.
    /\ latch \in {"free", "held", "heldPending"}
    /\ drainSignal \in BOOLEAN
    /\ serveDeposit \in BOOLEAN
    /\ drn \in {"release", "reclaimDecide", "serving", "deposited", "done"}
    /\ succ \in {"running", "ready", "bailed", "acquired", "done"}
    \* Queue.
    /\ qItem \in {"materializing", "ready", "drained"}
    /\ qSignal \in BOOLEAN
    /\ qDrainer \in {"pass", "released", "done"}

Init ==
    \* Gate.
    /\ completing = FALSE
    /\ ctCanceled = FALSE
    /\ executorPhase = "running"
    /\ enumeratorDisposed = FALSE
    /\ callerPhase = "idle"
    /\ callersServed = 0
    /\ completeAfterDispose = FALSE
    \* Slot: the drainer already claimed and drained item1; it now holds the latch
    \* at its release tail. item2 is committed with its pipeline task still in flight.
    /\ latch = "held"
    /\ drainSignal = FALSE
    /\ serveDeposit = FALSE
    /\ drn = "release"
    /\ succ = "running"
    \* Queue: a committed item is mid-materializing; a drainer pass is about to
    \* consume the signal and find nothing peekable yet.
    /\ qItem = "materializing"
    /\ qSignal = TRUE
    /\ qDrainer = "pass"

\* ============================================================================
\* GATE PROTOCOL ACTIONS (over gateVars)
\* ============================================================================

\* External CancellationToken cancellation. The CTS linkage drives _enumerator's
\* _cts.Cancel synchronously; the registered WakeSignal.Complete callback wakes
\* the executor. We collapse the linkage+wake into one step.
ExternalCTCancel ==
    /\ ~ctCanceled
    /\ ctCanceled' = TRUE
    /\ executorPhase' = IF executorPhase = "running" THEN "waking" ELSE executorPhase
    /\ UNCHANGED << completing, enumeratorDisposed, callerPhase,
                    callersServed, completeAfterDispose >>

CallerClaimCompletingFirst ==
    /\ callerPhase = "idle"
    /\ callersServed < MaxCallers
    /\ ~completing
    /\ completing' = TRUE
    /\ callerPhase' = "claimed"
    /\ executorPhase' = IF executorPhase = "running" THEN "waking" ELSE executorPhase
    /\ UNCHANGED << ctCanceled, enumeratorDisposed, callersServed, completeAfterDispose >>

CallerClaimCompletingLate ==
    /\ callerPhase = "idle"
    /\ callersServed < MaxCallers
    /\ completing
    /\ callerPhase' = "idle"
    /\ callersServed' = callersServed + 1
    /\ UNCHANGED << completing, ctCanceled, executorPhase, enumeratorDisposed,
                    completeAfterDispose >>

CallerCallEnumeratorComplete ==
    /\ callerPhase = "claimed"
    /\ completeAfterDispose' = enumeratorDisposed \/ completeAfterDispose
    /\ callerPhase' = "calledComplete"
    /\ UNCHANGED << completing, ctCanceled, executorPhase, enumeratorDisposed,
                    callersServed >>

CallerReturns ==
    /\ callerPhase = "calledComplete"
    /\ callerPhase' = "idle"
    /\ callersServed' = callersServed + 1
    /\ UNCHANGED << completing, ctCanceled, executorPhase, enumeratorDisposed,
                    completeAfterDispose >>

ExecutorBreaksLoop ==
    /\ executorPhase = "waking"
    /\ executorPhase' = "exiting"
    /\ UNCHANGED << completing, ctCanceled, enumeratorDisposed, callerPhase,
                    callersServed, completeAfterDispose >>

\* Fix path: executor claims _completing before disposing.
ExecutorClaimsCompleting ==
    /\ ExecutorClosesGate
    /\ executorPhase = "exiting"
    /\ completing' = TRUE
    /\ executorPhase' = "disposing"
    /\ UNCHANGED << ctCanceled, enumeratorDisposed, callerPhase,
                    callersServed, completeAfterDispose >>

\* No-fix path: executor proceeds straight to disposing without claiming.
ExecutorProceedsToDispose ==
    /\ ~ExecutorClosesGate
    /\ executorPhase = "exiting"
    /\ executorPhase' = "disposing"
    /\ UNCHANGED << completing, ctCanceled, enumeratorDisposed, callerPhase,
                    callersServed, completeAfterDispose >>

ExecutorDisposes ==
    /\ executorPhase = "disposing"
    /\ callerPhase # "claimed"  \* respect any in-flight Complete call
    /\ enumeratorDisposed' = TRUE
    /\ executorPhase' = "done"
    /\ UNCHANGED << completing, ctCanceled, callerPhase, callersServed,
                    completeAfterDispose >>

GateNext ==
    \/ ExternalCTCancel
    \/ CallerClaimCompletingFirst
    \/ CallerClaimCompletingLate
    \/ CallerCallEnumeratorComplete
    \/ CallerReturns
    \/ ExecutorBreaksLoop
    \/ ExecutorClaimsCompleting
    \/ ExecutorProceedsToDispose
    \/ ExecutorDisposes

\* Fairness subscripted by gateVars (not the full vars): under GateSpec the slot
\* vars are frozen, and each gate action defines only gateVars, so WF over the gate
\* tuple is the well-defined form (gateVars is the original 7-var tuple, so this is
\* equivalent to the pre-merge WF_vars).
GateFairness ==
    /\ WF_gateVars(ExternalCTCancel)
    /\ WF_gateVars(CallerClaimCompletingFirst)
    /\ WF_gateVars(CallerClaimCompletingLate)
    /\ WF_gateVars(CallerCallEnumeratorComplete)
    /\ WF_gateVars(CallerReturns)
    /\ WF_gateVars(ExecutorBreaksLoop)
    /\ WF_gateVars(ExecutorClaimsCompleting)
    /\ WF_gateVars(ExecutorProceedsToDispose)
    /\ WF_gateVars(ExecutorDisposes)

\* Terminal stuttering: once the executor is done AND no more callers can enter,
\* allow stuttering so TLC doesn't flag the quiescent state as a deadlock.
Termination ==
    /\ executorPhase = "done"
    /\ callerPhase = "idle"
    /\ callersServed = MaxCallers

\* GateSpec: run the gate machine; freeze the slot + queue variables at Init.
GateSpec == Init
            /\ [][ (GateNext /\ UNCHANGED <<slotVars, queueVars>>)
                   \/ (Termination /\ UNCHANGED vars) ]_vars
            /\ GateFairness

\* ============================================================================
\* SLOT-SERVE PROTOCOL ACTIONS (over slotVars)
\* ============================================================================

\* item2's pipeline task settles (faults under shutdown); its callback becomes
\* eligible to fire. The wake protocol is agnostic to WHY the task completed.
SuccReady ==
    /\ succ = "running"
    /\ succ' = "ready"
    /\ UNCHANGED << latch, drainSignal, serveDeposit, drn >>

\* `_drainSignal = true; TryAcquireOrFlagPending()` against a BUSY latch: the
\* obligation is deposited into the word (held -> heldPending) and the callback
\* bails. Its one wake is now spent; only a serve drains item2.
SuccCbBail ==
    /\ succ = "ready"
    /\ latch \in {"held", "heldPending"}
    /\ latch' = "heldPending"
    /\ drainSignal' = TRUE
    /\ succ' = "bailed"
    /\ UNCHANGED << serveDeposit, drn >>

\* `_drainSignal = true; TryAcquireOrFlagPending()` against a FREE latch: the
\* callback wins the latch and drains item2 itself.
SuccCbAcquire ==
    /\ succ = "ready"
    /\ latch = "free"
    /\ latch' = "held"
    /\ drainSignal' = TRUE   \* the store precedes the acquire; pass-top clear below
    /\ succ' = "acquired"
    /\ UNCHANGED << serveDeposit, drn >>

\* The callback (holding the latch) drains item2 and releases. item2 is the only
\* item, so any deposit that landed during this hold (the drainer's gated
\* dirty-flag reclaim losing the acquire) is for already-drained work - dropping
\* it on release is a harmless no-op serve.
SuccDrains ==
    /\ succ = "acquired"
    /\ succ' = "done"
    /\ drainSignal' = FALSE  \* pass-top Exchange(_drainSignal, false)
    /\ latch' = "free"
    /\ UNCHANGED << serveDeposit, drn >>

\* ReleaseAndCheckPending: Exchange the word to free, reading the pending bit out
\* atomically into the local serveDeposit (Latch.cs).
DrainerRelease ==
    /\ drn = "release"
    /\ serveDeposit' = (latch = "heldPending")
    /\ latch' = "free"
    /\ drn' = "reclaimDecide"
    /\ UNCHANGED << drainSignal, succ >>

\* Whether the drainer attempts the post-release reclaim acquire.
\*   FALSE (shipped): !cancelled && (serveDeposit || drainSignal) - one gate over both.
\*   TRUE  (fix):     serveDeposit (always) || (!cancelled && drainSignal).
DrnWantsReclaim ==
    IF ServeDepositUnconditional
      THEN serveDeposit \/ (~Cancelled /\ drainSignal)
      ELSE ~Cancelled /\ (serveDeposit \/ drainSignal)

\* The reclaim acquire wins (latch free): re-acquire and serve.
ReclaimAcquire ==
    /\ drn = "reclaimDecide"
    /\ DrnWantsReclaim
    /\ latch = "free"
    /\ latch' = "held"
    /\ drn' = "serving"
    /\ UNCHANGED << drainSignal, serveDeposit, succ >>

\* The reclaim acquire loses (the successor's callback holds the latch): deposit on
\* the winner (uniform PendingWordLatch rule) and return. The winner self-drains and
\* the spurious obligation discharges harmlessly.
ReclaimDeposit ==
    /\ drn = "reclaimDecide"
    /\ DrnWantsReclaim
    /\ latch \in {"held", "heldPending"}
    /\ latch' = "heldPending"
    /\ drn' = "deposited"
    /\ UNCHANGED << drainSignal, serveDeposit, succ >>

\* The gate is closed (shipped: cancellation; or genuinely nothing to do): the
\* drainer returns WITHOUT serving. Under shutdown with the shipped gate this is
\* where a just-consumed deposit (serveDeposit = TRUE) is DROPPED - the lost wake.
ReclaimSkip ==
    /\ drn = "reclaimDecide"
    /\ ~DrnWantsReclaim
    /\ drn' = "done"
    /\ UNCHANGED << latch, drainSignal, serveDeposit, succ >>

\* The drainer, holding the latch via the served deposit, drains item2 (the reroute
\* into DrainReadyWaiters) and releases.
DrainerServes ==
    /\ drn = "serving"
    /\ succ = "bailed"
    /\ succ' = "done"
    /\ drainSignal' = FALSE
    /\ latch' = "free"
    /\ drn' = "done"
    /\ UNCHANGED << serveDeposit >>

SlotNext ==
    \/ SuccReady \/ SuccCbBail \/ SuccCbAcquire \/ SuccDrains
    \/ DrainerRelease \/ ReclaimAcquire \/ ReclaimDeposit \/ ReclaimSkip
    \/ DrainerServes

\* Fairness subscripted by slotVars (the gate vars are frozen under SlotSpec, and
\* each slot action defines only slotVars).
SlotFairness ==
    /\ WF_slotVars(SuccReady)
    /\ WF_slotVars(SuccCbBail)
    /\ WF_slotVars(SuccCbAcquire)
    /\ WF_slotVars(SuccDrains)
    /\ WF_slotVars(DrainerRelease)
    /\ WF_slotVars(ReclaimAcquire)
    /\ WF_slotVars(ReclaimDeposit)
    /\ WF_slotVars(ReclaimSkip)
    /\ WF_slotVars(DrainerServes)

\* SlotSpec: run the slot machine; freeze the gate + queue variables at Init.
\* Terminal slot states stutter under [][...]_vars (use CHECK_DEADLOCK FALSE).
SlotSpec == Init
            /\ [][ SlotNext /\ UNCHANGED <<gateVars, queueVars>> ]_vars
            /\ SlotFairness

\* ============================================================================
\* QUEUE-SERVE PROTOCOL ACTIONS (over queueVars) - gap #2
\* ============================================================================
\* The queue twin of the slot deposit-drop. Unlike the slot, DrainReadyWaiters'
\* deposit-serve (the ReleaseAndCheckPending pendReacq) is ALREADY unconditional
\* (Pipeline.cs ~1467), so a consumed deposit is never dropped on the queue. The
\* only cancellation-gated edge is the dirty-flag RECHECK (Pipeline.cs ~1479), a
\* NON-consuming read. The open question: a drain pass that consumes the signal
\* and finds nothing peekable restores it (SignalConservation) for a MATERIALIZING
\* item (mid enqueue->increment, or mid slot->queue move). Under shutdown that
\* drainer's recheck is gated off - so who drains the item once it materializes?
\* Shipped answer: the materializing commit's OWN tail - the escalation nudge
\* (slotWasMoved, ungated) or the item's inline/UnsafeOnCompleted OnWaiterTaskCompleted
\* (ungated). MaterializerSelfServes models whether that ungated tail exists.

\* The drainer pass: consume the signal, peek the still-materializing head, drain
\* nothing, restore the token (SignalConservation, storeCount>0), release.
QDrainerPass ==
    /\ qDrainer = "pass"
    /\ qItem = "materializing"
    /\ qSignal' = TRUE          \* clear-then-restore = net set (the conserved token)
    /\ qDrainer' = "released"
    /\ UNCHANGED << qItem >>

\* The in-flight commit finishes materializing (increment lands / move completes):
\* the item is now a ready, peekable, counted queue head.
QMaterialize ==
    /\ qItem = "materializing"
    /\ qItem' = "ready"
    /\ UNCHANGED << qSignal, qDrainer >>

\* The drainer's post-release dirty-flag recheck. UNGATED only when ~Cancelled
\* (Pipeline.cs:1479: recheck = !IsCancellationRequested && _drainSignal). When it
\* fires against a ready head it drains it; under shutdown it never fires.
QRecheckDrains ==
    /\ qDrainer = "released"
    /\ ~Cancelled
    /\ qSignal
    /\ qItem = "ready"
    /\ qItem' = "drained"
    /\ qDrainer' = "done"
    /\ UNCHANGED << qSignal >>

\* The recheck is gated off (shutdown), or finds nothing to do: the drainer returns.
QRecheckExits ==
    /\ qDrainer = "released"
    /\ (Cancelled \/ ~qSignal \/ qItem # "ready")
    /\ qDrainer' = "done"
    /\ UNCHANGED << qItem, qSignal >>

\* The materializing commit's OWN tail services the item - the escalation nudge or
\* its inline/registered OnWaiterTaskCompleted. The item's guaranteed servicer (it
\* always exists); the drainer's recheck is only an optimization on top. Shipped:
\* UNGATED, so it serves even under shutdown. MaterializerTailUngated = FALSE models
\* the hypothetical where it too is cancellation-gated, so under Cancelled it does
\* not serve and the item strands (the discriminator).
QMaterializerServes ==
    /\ qItem = "ready"
    /\ (MaterializerTailUngated \/ ~Cancelled)
    /\ qItem' = "drained"
    /\ UNCHANGED << qSignal, qDrainer >>

QueueNext ==
    \/ QDrainerPass \/ QMaterialize \/ QRecheckDrains \/ QRecheckExits
    \/ QMaterializerServes

QueueFairness ==
    /\ WF_queueVars(QDrainerPass)
    /\ WF_queueVars(QMaterialize)
    /\ WF_queueVars(QRecheckDrains)
    /\ WF_queueVars(QRecheckExits)
    /\ WF_queueVars(QMaterializerServes)

\* QueueSpec: run the queue machine; freeze the gate + slot variables at Init.
QueueSpec == Init
             /\ [][ QueueNext /\ UNCHANGED <<gateVars, slotVars>> ]_vars
             /\ QueueFairness

\* ============================================================================
\* Properties
\* ============================================================================

\* Gate invariant: no Complete call ever observes a disposed enumerator.
NoCompleteAfterDispose == ~completeAfterDispose

\* Gate liveness: every shutdown path eventually reaches the quiescent terminal.
ShutdownTerminates == <>(executorPhase = "done")

\* Slot liveness: a successor whose pipeline task has completed eventually drains.
EventuallyDrained == (succ \in {"ready", "bailed", "acquired"}) ~> (succ = "done")

\* Strand -> hang composition (gap #4): the actual user-observable. The successor's
\* drain is the last committed waiter; until it drains, the store Count stays > 0 and
\* DrainOnCompletionAsync's `while (IsHeld || Count > 0)` sweep never quiesces - so
\* CompleteAsync hangs (the dump's symptom). Expressing it as <>(succ drained) makes
\* the property the sweep's termination, not the internal strand: HOLDS under the fix,
\* VIOLATES under the deposit-drop (the sweep wedged on Count > 0 forever). Stronger
\* than EventuallyDrained's conditional leads-to - it asserts the sweep DOES complete.
\* (succ = "done" is the successful-drain terminal; both drain paths land there.)
SweepQuiesces == <>(succ = "done")

\* Queue liveness (gap #2): a materializing item eventually drains. HOLDS under
\* Cancelled = TRUE with the recheck gated off because the materializer's own tail is
\* UNGATED (MaterializerTailUngated = TRUE, shipped); VIOLATES under shutdown if that
\* tail were also cancellation-gated (FALSE) - proving the ungated tail, not the gated
\* recheck, is the load-bearing servicer, i.e. the queue's recheck-gating is gate-safe.
QEventuallyDrained == (qItem \in {"materializing", "ready"}) ~> (qItem = "drained")

================================================================================
