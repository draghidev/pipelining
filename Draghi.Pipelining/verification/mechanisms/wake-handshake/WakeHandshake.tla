--------------------------- MODULE WakeHandshake ---------------------------
(*
The synchronous disposer arms itself and then observes either the task-source CAS follower
(`terminalPublished`) or the sticky progress level. Terminal delivery may lose its CAS to an
already-completed generation, so terminalPublished is not an unconditional publication.

The old implementation published progress only when it observed an already-armed disposer. It
covered the concurrent Dekker race, but not the sequential ordering where a losing terminal CAS
ran before the disposer armed. The shipped implementation publishes sticky progress at every
terminal, independently of the CAS and of whether a disposer is already visible.
*)

EXTENDS Naturals, TLC

CONSTANT UNCONDITIONAL_PROGRESS

VARIABLES
  waiterArmed,
  terminalPublished,
  progressPublished,
  waiterPc,
  signalerPc,
  waiterSawTerminal,
  waiterSawProgress,
  signalerSawWaiter,
  signaled

vars == << waiterArmed, terminalPublished, progressPublished, waiterPc, signalerPc,
           waiterSawTerminal, waiterSawProgress, signalerSawWaiter, signaled >>

TypeOK ==
  /\ UNCONDITIONAL_PROGRESS \in BOOLEAN
  /\ waiterArmed \in BOOLEAN
  /\ terminalPublished \in BOOLEAN
  /\ progressPublished \in BOOLEAN
  /\ waiterPc \in {"idle", "stored", "parked", "done"}
  /\ signalerPc \in {"idle", "stored", "done"}
  /\ waiterSawTerminal \in BOOLEAN
  /\ waiterSawProgress \in BOOLEAN
  /\ signalerSawWaiter \in BOOLEAN
  /\ signaled \in BOOLEAN

Init ==
  /\ waiterArmed = FALSE
  /\ terminalPublished = FALSE
  /\ progressPublished = FALSE
  /\ waiterPc = "idle"
  /\ signalerPc = "idle"
  /\ waiterSawTerminal = FALSE
  /\ waiterSawProgress = FALSE
  /\ signalerSawWaiter = FALSE
  /\ signaled = FALSE

WaiterPublishArm ==
  /\ waiterPc = "idle"
  /\ waiterArmed' = TRUE
  /\ waiterSawTerminal' = terminalPublished
  /\ waiterSawProgress' = progressPublished
  /\ waiterPc' = "stored"
  /\ UNCHANGED << terminalPublished, progressPublished, signalerPc,
                  signalerSawWaiter, signaled >>

WaiterObserveProgress ==
  /\ waiterPc = "stored"
  /\ waiterSawTerminal \/ waiterSawProgress
  /\ waiterPc' = "done"
  /\ UNCHANGED << waiterArmed, terminalPublished, progressPublished, signalerPc,
                  waiterSawTerminal, waiterSawProgress, signalerSawWaiter, signaled >>

WaiterPark ==
  /\ waiterPc = "stored"
  /\ ~waiterSawTerminal
  /\ ~waiterSawProgress
  /\ waiterPc' = "parked"
  /\ UNCHANGED << waiterArmed, terminalPublished, progressPublished, signalerPc,
                  waiterSawTerminal, waiterSawProgress, signalerSawWaiter, signaled >>

WaiterResume ==
  /\ waiterPc = "parked"
  /\ signaled
  /\ waiterPc' = "done"
  /\ UNCHANGED << waiterArmed, terminalPublished, progressPublished, signalerPc,
                  waiterSawTerminal, waiterSawProgress, signalerSawWaiter, signaled >>

\* The task-source CAS landed. The follower is visible even without sticky progress.
SignalerTerminalLanded ==
  /\ signalerPc = "idle"
  /\ terminalPublished' = TRUE
  /\ progressPublished' = UNCONDITIONAL_PROGRESS
  /\ signalerSawWaiter' = waiterArmed
  /\ signalerPc' = "stored"
  /\ UNCHANGED << waiterArmed, waiterPc, waiterSawTerminal, waiterSawProgress, signaled >>

\* The CAS lost to an already-completed generation. Only unconditional progress can record terminal.
SignalerTerminalLost ==
  /\ signalerPc = "idle"
  /\ terminalPublished' = FALSE
  /\ progressPublished' = UNCONDITIONAL_PROGRESS
  /\ signalerSawWaiter' = waiterArmed
  /\ signalerPc' = "stored"
  /\ UNCHANGED << waiterArmed, waiterPc, waiterSawTerminal, waiterSawProgress, signaled >>

SignalerWakeWaiter ==
  /\ signalerPc = "stored"
  /\ signalerSawWaiter
  /\ signaled' = TRUE
  /\ signalerPc' = "done"
  /\ UNCHANGED << waiterArmed, terminalPublished, progressPublished, waiterPc,
                  waiterSawTerminal, waiterSawProgress, signalerSawWaiter >>

SignalerSkipWake ==
  /\ signalerPc = "stored"
  /\ ~signalerSawWaiter
  /\ signalerPc' = "done"
  /\ UNCHANGED << waiterArmed, terminalPublished, progressPublished, waiterPc,
                  waiterSawTerminal, waiterSawProgress, signalerSawWaiter, signaled >>

Next ==
  \/ WaiterPublishArm \/ WaiterObserveProgress \/ WaiterPark \/ WaiterResume
  \/ SignalerTerminalLanded \/ SignalerTerminalLost \/ SignalerWakeWaiter \/ SignalerSkipWake

Fairness ==
  /\ WF_vars(WaiterPublishArm)
  /\ WF_vars(WaiterObserveProgress)
  /\ WF_vars(WaiterPark)
  /\ WF_vars(WaiterResume)
  /\ WF_vars(SignalerTerminalLanded)
  /\ WF_vars(SignalerTerminalLost)
  /\ WF_vars(SignalerWakeWaiter)
  /\ WF_vars(SignalerSkipWake)

Spec == Init /\ [][Next]_vars /\ Fairness

WaiterEventuallyResumes == <>(waiterPc = "done")
NoStrandedWaiter == ~(waiterPc = "parked" /\ signalerPc = "done" /\ ~signaled)

=============================================================================
