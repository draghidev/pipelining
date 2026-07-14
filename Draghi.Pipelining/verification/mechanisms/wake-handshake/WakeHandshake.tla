--------------------------- MODULE WakeHandshake ---------------------------
(*
The StoreLoad / Dekker fence-pairing lost-wake. Two
threads each STORE their own flag then LOAD the other's; if a STORE->LOAD edge is
not full-fenced it can reorder, the LOAD reads stale, and both sides can miss each
other. The canonical model represents the fully fenced implementation.

    WaiterEventuallyResumes holds because both StoreLoad edges are fenced.

Threads (feature-agnostic; A = the side that PARKS, B = the side that SIGNALS):

  A: STORE waiterArmed := set        (publish my arm / presence)
     LOAD  progressPublished               (did the other side make progress?)
     -> observed B  => resolve (no wait);  ~observed B => PARK
  B: STORE progressPublished := set        (publish my progress)
     LOAD  waiterArmed               (did A arm?)
     -> observed A => SIGNAL (wakes a parked A);  ~observed A => SKIP

Property:
  WaiterEventuallyResumes - the parking side eventually completes.
*)

EXTENDS Naturals, TLC

VARIABLES
  waiterArmed,    \* A's store (its arm/presence); B reads it
  progressPublished,    \* B's store (its progress); A reads it
  waiterPc,      \* A pc: "idle" | "stored" | "parked" | "done"
  signalerPc,      \* B pc: "idle" | "stored" | "done"
  waiterSawProgress,     \* the value A's LOAD of progressPublished observed
  signalerSawWaiter,     \* the value B's LOAD of waiterArmed observed
  signaled  \* B signalled (wakes a parked A)

vars == << waiterArmed, progressPublished, waiterPc, signalerPc, waiterSawProgress, signalerSawWaiter, signaled >>

TypeOK ==
  /\ waiterArmed \in BOOLEAN
  /\ progressPublished \in BOOLEAN
  /\ waiterPc \in {"idle", "stored", "parked", "done"}
  /\ signalerPc \in {"idle", "stored", "done"}
  /\ waiterSawProgress \in BOOLEAN
  /\ signalerSawWaiter \in BOOLEAN
  /\ signaled \in BOOLEAN

Init ==
  /\ waiterArmed = FALSE
  /\ progressPublished = FALSE
  /\ waiterPc = "idle"
  /\ signalerPc = "idle"
  /\ waiterSawProgress = FALSE
  /\ signalerSawWaiter = FALSE
  /\ signaled = FALSE

-----------------------------------------------------------------------------
\* Thread A: publish the arm, then observe progress across the full fence.
WaiterPublishArm ==
  /\ waiterPc = "idle"
  /\ waiterArmed' = TRUE
  /\ waiterSawProgress' = progressPublished
  /\ waiterPc' = "stored"
  /\ UNCHANGED << progressPublished, signalerPc, signalerSawWaiter, signaled >>

\* Observed B (progressPublished) -> resolve, no wait.
WaiterObserveProgress ==
  /\ waiterPc = "stored"
  /\ waiterSawProgress
  /\ waiterPc' = "done"
  /\ UNCHANGED << waiterArmed, progressPublished, signalerPc, waiterSawProgress, signalerSawWaiter, signaled >>

\* Did not observe B -> park, awaiting B's signal.
WaiterPark ==
  /\ waiterPc = "stored"
  /\ ~waiterSawProgress
  /\ waiterPc' = "parked"
  /\ UNCHANGED << waiterArmed, progressPublished, signalerPc, waiterSawProgress, signalerSawWaiter, signaled >>

\* B's signal woke A.
WaiterResume ==
  /\ waiterPc = "parked"
  /\ signaled
  /\ waiterPc' = "done"
  /\ UNCHANGED << waiterArmed, progressPublished, signalerPc, waiterSawProgress, signalerSawWaiter, signaled >>

-----------------------------------------------------------------------------
\* Thread B (the signalling side): STORE progressPublished, snapshot the LOAD of waiterArmed.
SignalerPublishProgress ==
  /\ signalerPc = "idle"
  /\ progressPublished' = TRUE
  /\ signalerSawWaiter' = waiterArmed
  /\ signalerPc' = "stored"
  /\ UNCHANGED << waiterArmed, waiterPc, waiterSawProgress, signaled >>

\* Observed A (waiterArmed) -> signal (wakes a parked A; harmless if A resolved).
SignalerWakeWaiter ==
  /\ signalerPc = "stored"
  /\ signalerSawWaiter
  /\ signaled' = TRUE
  /\ signalerPc' = "done"
  /\ UNCHANGED << waiterArmed, progressPublished, waiterPc, waiterSawProgress, signalerSawWaiter >>

\* Did not observe A -> skip. If A also parked, the wake is lost.
SignalerSkipWake ==
  /\ signalerPc = "stored"
  /\ ~signalerSawWaiter
  /\ signalerPc' = "done"
  /\ UNCHANGED << waiterArmed, progressPublished, waiterPc, waiterSawProgress, signalerSawWaiter, signaled >>

-----------------------------------------------------------------------------

Next ==
  \/ WaiterPublishArm \/ WaiterObserveProgress \/ WaiterPark \/ WaiterResume
  \/ SignalerPublishProgress \/ SignalerWakeWaiter \/ SignalerSkipWake

Fairness ==
  /\ WF_vars(WaiterPublishArm)
  /\ WF_vars(WaiterObserveProgress)
  /\ WF_vars(WaiterPark)
  /\ WF_vars(WaiterResume)
  /\ WF_vars(SignalerPublishProgress)
  /\ WF_vars(SignalerWakeWaiter)
  /\ WF_vars(SignalerSkipWake)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

\* The parking side always completes under the symmetric fenced handshake.
WaiterEventuallyResumes == <>(waiterPc = "done")

=============================================================================
