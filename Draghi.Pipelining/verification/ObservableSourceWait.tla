--------------------------- MODULE ObservableSourceWait ---------------------------
(*
TestObservableQueueSource.State.WaitForNextAsync protocol - distinct from
UnboundedQueueSource.WaitForNextAsync (the WaitProtocol.tla case). Two
shape differences:

  (1) PRE-ARM ASYNC HOOK. WaitForNextAsync awaits OnWaitingAsync() BEFORE
      acquiring the wake lock; OnWaitingAsync may itself await user code
      (the onIdle callback). The hook runs strictly out-of-lock. From the
      WakeSignal's perspective the consumer is "not yet at Arm" during the
      hook - the state machine box is parked on the hook's await, not on
      WakeSignal.

  (2) THE CONSUMER IS A STATE MACHINE THAT CAN BE CANCELED MID-WINDOW.
      The enumerator registers WakeSignal.Complete() on its
      _completionToken (TestObservableQueueSource.cs:153). If the test
      cancels mid-flight, Complete() runs from arbitrary thread context
      while the consumer state machine is somewhere in its loop. The
      hazard window we're modeling: cancellation lands AFTER the consumer
      called Arm() (set _pending = TRUE under the lock) but BEFORE the
      await machinery's OnCompleted (which would store _waitContinuation
      and release the lock). The state machine's await may unwind via the
      cancellation exception, exiting the loop without ever invoking
      WaitOnCompleted. The lock is released (eventually, via a different
      path - exception unwind, finally, or just GC pressure releasing
      contention), but _pending remains TRUE with _waitContinuation
      still NULL.

This is the exact shape observed in a June 2026 PipelineConcurrencyTests
dump (TestObservableQueueSource's WakeSignal in
PipelineConcurrencyTests+<MixedSyncAsyncPipelineTasks_AllItemsComplete>):
_pending = 1, _waitContinuation = 0, _wakeLock = 0. From the documented
WaitProtocol that state is unreachable (the lock is held from Arm through
WaitOnCompleted - any release implies registration). The reach is via the
cancellation unwind path: the state machine never reaches the
WaitOnCompleted call.

Subsequent damage: a later WakeSignal cycle (the next consumer wait, or
a producer trying to Signal) reads _pending = TRUE without a registered
continuation - SignalCore's claim sets _pending = FALSE and calls
DispatchClaimed, which dereferences a null _waitContinuation (NRE in
release: `_waitContinuation!();`). Or worse, the spurious claim consumes
a legitimate future wake - the producer's actual signal turns into a
no-op (`!_pending` -> return false in TryClaimLocked).

Witness toggle:
  CleanupArmOnCancel - the fix would clear _pending in the cancellation
                       unwind path so the WakeSignal is returned to
                       quiescent state. Without it: _pending leaks, the
                       next cycle observes corrupted state.

Properties:
  TypeOK
  NoSpuriousPending - WakeSignal._pending = TRUE always implies a
                      registered continuation is either stored or about
                      to be stored under the held lock (the protocol
                      invariant). Fails without CleanupArmOnCancel.
  NoNullDispatch    - no SignalCore claim ever dispatches a NULL
                      _waitContinuation.
  ConsumerTerminates - liveness: every WaitForNextAsync invocation
                       eventually returns.
*)

EXTENDS Naturals, TLC

CONSTANTS MaxCancels, CleanupArmOnCancel

VARIABLES
    pending,            \* WakeSignal._pending
    waitContinuation,   \* WakeSignal._waitContinuation: NONE | "moveNext"
    wakeLock,           \* 0/1 spinlock
    cancelRequested,    \* the enumeration token has been canceled
    consumer,           \* "running" | "hookSuspended" | "lockHeld" | "armed"
                        \* | "registering" | "suspended" | "done" | "unwound"
    cancelsSeen,        \* count of Complete() callbacks delivered
    nullDispatchSeen    \* TRUE if a SignalCore ever dispatched a null continuation

vars == << pending, waitContinuation, wakeLock, cancelRequested,
           consumer, cancelsSeen, nullDispatchSeen >>

NONE == "<none>"

TypeOK ==
    /\ pending \in BOOLEAN
    /\ waitContinuation \in {NONE, "moveNext"}
    /\ wakeLock \in {0, 1}
    /\ cancelRequested \in BOOLEAN
    /\ consumer \in {"running", "hookSuspended", "lockHeld", "armed",
                     "registering", "suspended", "done", "unwound"}
    /\ cancelsSeen \in 0..MaxCancels
    /\ nullDispatchSeen \in BOOLEAN

Init ==
    /\ pending = FALSE
    /\ waitContinuation = NONE
    /\ wakeLock = 0
    /\ cancelRequested = FALSE
    /\ consumer = "running"
    /\ cancelsSeen = 0
    /\ nullDispatchSeen = FALSE

\* ====================================================================
\* Consumer: TestObservableQueueSource.State.WaitForNextAsync's flow.
\* ====================================================================

\* Enter the hook (out-of-lock, async). Models the await of OnWaitingAsync
\* which may suspend on user callback (onIdle).
ConsumerEnterHook ==
    /\ consumer = "running"
    /\ consumer' = "hookSuspended"
    /\ UNCHANGED << pending, waitContinuation, wakeLock, cancelRequested,
                    cancelsSeen, nullDispatchSeen >>

\* Hook completes; consumer proceeds to acquire wake lock.
ConsumerHookCompletes ==
    /\ consumer = "hookSuspended"
    /\ wakeLock = 0
    /\ wakeLock' = 1
    /\ consumer' = "lockHeld"
    /\ UNCHANGED << pending, waitContinuation, cancelRequested,
                    cancelsSeen, nullDispatchSeen >>

\* Under the lock: cancellation already in flight -> bail with token check.
\* (Mirrors `if (wakeSignal.IsCompleted) { release; return false; }`.)
ConsumerCancelBail ==
    /\ consumer = "lockHeld"
    /\ cancelRequested
    /\ wakeLock' = 0
    /\ consumer' = "done"
    /\ UNCHANGED << pending, waitContinuation, cancelRequested,
                    cancelsSeen, nullDispatchSeen >>

\* Arm: set _pending = TRUE. Lock still held. Consumer is now in the
\* CRITICAL WINDOW between arm and registration.
ConsumerArm ==
    /\ consumer = "lockHeld"
    /\ ~cancelRequested
    /\ pending' = TRUE
    /\ consumer' = "armed"
    /\ UNCHANGED << waitContinuation, wakeLock, cancelRequested,
                    cancelsSeen, nullDispatchSeen >>

\* The await machinery calls Awaiter.OnCompleted -> WakeSignal.WaitOnCompleted:
\* stores _waitContinuation, releases lock. Consumer suspended awaiting wake.
ConsumerRegister ==
    /\ consumer = "armed"
    /\ ~cancelRequested
    /\ waitContinuation' = "moveNext"
    /\ wakeLock' = 0
    /\ consumer' = "suspended"
    /\ UNCHANGED << pending, cancelRequested, cancelsSeen, nullDispatchSeen >>

\* CANCELLATION UNWIND: cancellation lands while the state machine is in
\* the arm-to-registration gap. The state machine's await throws OCE; the
\* loop unwinds without WaitOnCompleted ever being called. The lock is
\* released by the exception path (or via Complete's own SignalCore
\* claim attempt which acquires/releases the lock). Whether _pending is
\* cleaned up depends on the CleanupArmOnCancel toggle.
ConsumerCancelMidArm ==
    /\ consumer = "armed"
    /\ cancelRequested
    /\ wakeLock' = 0
    /\ pending' = IF CleanupArmOnCancel THEN FALSE ELSE pending
    /\ consumer' = "unwound"
    /\ UNCHANGED << waitContinuation, cancelRequested,
                    cancelsSeen, nullDispatchSeen >>

\* The dispatched wake fires - continuation invoked. Only legitimate when
\* registered (waitContinuation # NONE). The consumer transitions out.
ConsumerWakeFires ==
    /\ consumer = "suspended"
    /\ waitContinuation # NONE
    /\ ~pending  \* signal claimed pending and dispatched
    /\ consumer' = "done"
    /\ UNCHANGED << pending, waitContinuation, wakeLock,
                    cancelRequested, cancelsSeen, nullDispatchSeen >>

\* After unwound the test eventually exits (terminal sink so liveness
\* doesn't deadlock on the cancellation path).
ConsumerUnwoundExits ==
    /\ consumer = "unwound"
    /\ consumer' = "done"
    /\ UNCHANGED << pending, waitContinuation, wakeLock, cancelRequested,
                    cancelsSeen, nullDispatchSeen >>

\* ====================================================================
\* External signal events.
\* ====================================================================

\* Producer signal. Acquires lock, claims _pending if set, dispatches.
\* If _pending was TRUE but _waitContinuation is NULL (the bug shape),
\* dispatch hits null - NullDispatchSeen.
ProducerSignal ==
    /\ wakeLock = 0
    /\ pending
    /\ wakeLock' = 0  \* model the entire acquire/claim/release atomically
    /\ pending' = FALSE
    /\ nullDispatchSeen' = (waitContinuation = NONE) \/ nullDispatchSeen
    /\ UNCHANGED << waitContinuation, cancelRequested, cancelsSeen, consumer >>

\* The enumerator's completion token fires. Models the
\* _completionToken.UnsafeRegister(state => state.WakeSignal.Complete())
\* in TestObservableQueueSource.cs:153. Limited to MaxCancels to bound
\* state space.
TokenCancel ==
    /\ ~cancelRequested
    /\ cancelsSeen < MaxCancels
    /\ cancelRequested' = TRUE
    /\ cancelsSeen' = cancelsSeen + 1
    /\ UNCHANGED << pending, waitContinuation, wakeLock,
                    consumer, nullDispatchSeen >>

\* WakeSignal.Complete() runs its SignalCore. Same claim/dispatch shape
\* as ProducerSignal; modeled separately so the cancellation-event
\* ordering is explicit.
CompleteSignals ==
    /\ cancelRequested
    /\ wakeLock = 0
    /\ pending
    /\ pending' = FALSE
    /\ nullDispatchSeen' = (waitContinuation = NONE) \/ nullDispatchSeen
    /\ UNCHANGED << waitContinuation, wakeLock, cancelRequested,
                    cancelsSeen, consumer >>

\* ====================================================================
\* Next
\* ====================================================================

Next ==
    \/ ConsumerEnterHook
    \/ ConsumerHookCompletes
    \/ ConsumerCancelBail
    \/ ConsumerArm
    \/ ConsumerRegister
    \/ ConsumerCancelMidArm
    \/ ConsumerWakeFires
    \/ ConsumerUnwoundExits
    \/ ProducerSignal
    \/ TokenCancel
    \/ CompleteSignals

Fairness ==
    /\ WF_vars(ConsumerEnterHook)
    /\ WF_vars(ConsumerHookCompletes)
    /\ WF_vars(ConsumerCancelBail)
    /\ WF_vars(ConsumerArm)
    /\ WF_vars(ConsumerRegister)
    /\ WF_vars(ConsumerCancelMidArm)
    /\ WF_vars(ConsumerWakeFires)
    /\ WF_vars(ConsumerUnwoundExits)
    /\ WF_vars(ProducerSignal)
    /\ WF_vars(CompleteSignals)

\* Allow stuttering once consumer reaches "done" so termination isn't
\* flagged as deadlock.
Termination == consumer = "done"

Spec == Init /\ [][Next \/ (Termination /\ UNCHANGED vars)]_vars /\ Fairness

\* ====================================================================
\* Properties
\* ====================================================================

\* Invariant: WakeSignal._pending only ever TRUE while the lock is HELD
\* and the consumer is in arm-to-registration window, OR the registration
\* completed (waitContinuation # NONE). Without CleanupArmOnCancel, the
\* cancel-mid-arm path leaks _pending = TRUE with waitContinuation = NONE
\* AND lock = 0, which violates the invariant.
NoSpuriousPending ==
    pending => \/ wakeLock = 1
              \/ waitContinuation # NONE

\* No SignalCore ever dispatched a null continuation - the NRE shape.
NoNullDispatch ==
    ~nullDispatchSeen

\* Liveness: every WaitForNextAsync invocation eventually leaves the loop
\* (returns from the async method, or unwinds via cancel).
ConsumerTerminates ==
    [](consumer \in {"running", "hookSuspended", "lockHeld", "armed",
                     "registering", "suspended"})
    ~> (consumer \in {"done", "unwound"})

================================================================================
