------------------------------ MODULE ItemTaskGates ------------------------------
(* The two independent task gates at the ExecuteSource seam.

   PipelineTask gates completion and ordered retirement. TrailingExecutionTask
   gates issue of the next source item. Recovery waits for unresolved trailing
   work before touching shared output, and the tail item remains reachable until
   its obligations resolve. Neither task substitutes for the other.

   Out of scope: activation turns, storage/ROB mechanics, callback delivery,
   condemnation, source wakeups, shutdown, depth and weak memory beyond atomic
   task-state publication.                                                   *)
EXTENDS Integers, TLC

CONSTANT N
ASSUME N \in Nat /\ N >= 2
NONE == 0
Items == 1..N
Resolved == {"absent", "succeeded", "handled"}

VARIABLES
  pipelineState,   \* [Items -> pending|succeeded|faulted|handled]
  trailingState,   \* [Items -> absent|pending|succeeded|faulted|handled]
  tailResident,    \* NONE | item retained as tail waiter
  retired,         \* [Items -> BOOLEAN]
  faultDoubleRetire,
  successorIssued, \* [Items -> BOOLEAN]
  recoveryState,   \* [Items -> none|waitingTrailing|active|completed]
  sharedOutputTouched,
  executorItem, executorPc

vars == <<pipelineState, trailingState, tailResident, retired, faultDoubleRetire,
          successorIssued, recoveryState, sharedOutputTouched, executorItem, executorPc>>

Init ==
  /\ pipelineState = [i \in Items |-> "unstarted"]
  /\ trailingState = [i \in Items |-> "unstarted"]
  /\ tailResident = NONE
  /\ retired = [i \in Items |-> FALSE] /\ faultDoubleRetire = FALSE
  /\ successorIssued = [i \in Items |-> FALSE]
  /\ recoveryState = [i \in Items |-> "none"]
  /\ sharedOutputTouched = [i \in Items |-> FALSE]
  /\ executorItem = 1 /\ executorPc = "startItem"

RetireItem(i) ==
  /\ faultDoubleRetire' = (faultDoubleRetire \/ retired[i])
  /\ retired' = [retired EXCEPT ![i] = TRUE]

(* -------- environment: task completions, independent + nondeterministic --- *)
PipelineComplete(i) ==
  /\ pipelineState[i] = "pending"
  /\ \E v \in {"succeeded", "faulted"} : pipelineState' = [pipelineState EXCEPT ![i] = v]
  /\ UNCHANGED <<trailingState, tailResident, retired, faultDoubleRetire,
                 successorIssued, recoveryState, sharedOutputTouched, executorItem, executorPc>>

TrailingComplete(i) ==
  /\ trailingState[i] = "pending"
  /\ \E v \in {"succeeded", "faulted"} : trailingState' = [trailingState EXCEPT ![i] = v]
  /\ UNCHANGED <<pipelineState, tailResident, retired, faultDoubleRetire,
                 successorIssued, recoveryState, sharedOutputTouched, executorItem, executorPc>>

\* A faulted trailing is HANDLED only by entering the failure path explicitly -
\* never merely because the task object completed.
TrailingFaultHandle(i) ==
  /\ trailingState[i] = "faulted"
  /\ trailingState' = [trailingState EXCEPT ![i] = "handled"]
  /\ UNCHANGED <<pipelineState, tailResident, retired, faultDoubleRetire,
                 successorIssued, recoveryState, sharedOutputTouched, executorItem, executorPc>>

(* -------- executor: the ExecuteSource phase boundaries -------------------- *)
ExecutorStartItem ==   \* execute policy: both tasks come into being (trailing optional)
  /\ executorPc = "startItem" /\ executorItem \in Items
  /\ pipelineState' = [pipelineState EXCEPT ![executorItem] = "pending"]
  /\ \E t \in {"absent", "pending"} : trailingState' = [trailingState EXCEPT ![executorItem] = t]
  /\ executorPc' = "inspectPipelineTask"
  /\ UNCHANGED <<tailResident, retired, faultDoubleRetire, successorIssued,
                 recoveryState, sharedOutputTouched, executorItem>>

ExecutorInspectPipelineTask ==   \* inspect PipelineTask: direct-retire (naked) or publish tail waiter
  /\ executorPc = "inspectPipelineTask"
  /\ IF pipelineState[executorItem] = "succeeded"
        /\ trailingState[executorItem] \in Resolved
       THEN /\ RetireItem(executorItem)
            /\ UNCHANGED <<tailResident>>
            /\ executorPc' = "issueSuccessor"
       ELSE /\ tailResident' = executorItem
            /\ UNCHANGED <<retired, faultDoubleRetire>>
            \* A FAULTED pipeline is discovered at the commit regardless of the
            \* trailing task (code :988: the fault-commit branch runs before any
            \* trailing await - trailing gates ISSUE, not fault discovery), so
            \* recovery can receive OUTSTANDING trailing work. Success paths
            \* await; the fault path goes straight to the commit.
            /\ executorPc' = IF pipelineState[executorItem] = "faulted" THEN "commitItem" ELSE "await"
  /\ UNCHANGED <<pipelineState, trailingState, successorIssued, recoveryState,
                 sharedOutputTouched, executorItem>>

ExecutorAwaitTrailingTask ==
  /\ executorPc = "await"
  /\ trailingState[executorItem] \in Resolved
  /\ executorPc' = "commitItem"
  /\ UNCHANGED <<pipelineState, trailingState, tailResident, retired, faultDoubleRetire,
                 successorIssued, recoveryState, sharedOutputTouched, executorItem>>

ExecutorCommitItem ==   \* commit/complete the tail waiter (retire, or hand to recovery)
  /\ executorPc = "commitItem"
  /\ IF tailResident = NONE
       THEN /\ UNCHANGED <<pipelineState, retired, faultDoubleRetire, tailResident, recoveryState>>
            /\ executorPc' = "issueSuccessor"
       ELSE LET i == tailResident IN
            IF pipelineState[i] = "succeeded"
              THEN /\ RetireItem(i) /\ tailResident' = NONE
                   /\ UNCHANGED <<pipelineState, recoveryState>>
                   /\ executorPc' = "issueSuccessor"
            ELSE IF pipelineState[i] = "faulted"
              THEN \* pipeline fault: capture outstanding trailing, enter recovery
                   /\ recoveryState' = [recoveryState EXCEPT ![i] = "waitingTrailing"]
                   /\ UNCHANGED <<pipelineState, retired, faultDoubleRetire, tailResident>>
                   /\ executorPc' = "recovery"
            ELSE FALSE   \* wait for the pipeline task
  /\ UNCHANGED <<trailingState, successorIssued, sharedOutputTouched, executorItem>>

RecoveryAwaitTrailingTask ==
  /\ executorPc = "recovery" /\ tailResident # NONE
  /\ LET i == tailResident IN
     /\ recoveryState[i] = "waitingTrailing"
     /\ trailingState[i] \in Resolved
     /\ recoveryState' = [recoveryState EXCEPT ![i] = "active"]
  /\ UNCHANGED <<pipelineState, trailingState, tailResident, retired, faultDoubleRetire,
                 successorIssued, sharedOutputTouched, executorItem, executorPc>>

RecoveryTouchSharedOutput ==   \* the substitute touches shared output
  /\ executorPc = "recovery" /\ tailResident # NONE
  /\ LET i == tailResident IN
     /\ recoveryState[i] = "active"
     /\ sharedOutputTouched' = [sharedOutputTouched EXCEPT ![i] = TRUE]
     /\ recoveryState' = [recoveryState EXCEPT ![i] = "completed"]
  /\ UNCHANGED <<pipelineState, trailingState, tailResident, retired, faultDoubleRetire,
                 successorIssued, executorItem, executorPc>>

RecoveryRetireItem ==
  /\ executorPc = "recovery" /\ tailResident # NONE
  /\ LET i == tailResident IN
     /\ recoveryState[i] = "completed"
     /\ pipelineState' = [pipelineState EXCEPT ![i] = "handled"]
     /\ RetireItem(i) /\ tailResident' = NONE
  /\ executorPc' = "issueSuccessor"
  /\ UNCHANGED <<trailingState, successorIssued, recoveryState, sharedOutputTouched, executorItem>>

ExecutorIssueSuccessor ==
  /\ executorPc = "issueSuccessor"
  /\ IF executorItem < N
       THEN /\ successorIssued' = [successorIssued EXCEPT ![executorItem + 1] = TRUE]
            /\ executorItem' = executorItem + 1 /\ executorPc' = "startItem"
       ELSE /\ UNCHANGED <<successorIssued, executorItem>> /\ executorPc' = "off"
  /\ UNCHANGED <<pipelineState, trailingState, tailResident, retired, faultDoubleRetire,
                 recoveryState, sharedOutputTouched>>

AllItemsRetired == executorPc = "off" /\ \A i \in Items : retired[i]
Finished == AllItemsRetired /\ UNCHANGED vars

Next ==
  \/ ExecutorStartItem \/ ExecutorInspectPipelineTask \/ ExecutorAwaitTrailingTask \/ ExecutorCommitItem
  \/ RecoveryAwaitTrailingTask \/ RecoveryTouchSharedOutput \/ RecoveryRetireItem \/ ExecutorIssueSuccessor
  \/ \E i \in Items : PipelineComplete(i) \/ TrailingComplete(i) \/ TrailingFaultHandle(i)
  \/ Finished

Spec == Init /\ [][Next]_vars

(* ------------------------------ properties -------------------------------- *)
RetirementWaitsForPipelineTask ==
  \A i \in Items : retired[i] => pipelineState[i] \in {"succeeded", "handled"}

SuccessorWaitsForTrailingTask ==
  \A i \in 1..(N-1) : successorIssued[i + 1] => trailingState[i] \in Resolved

RecoveryWaitsForTrailingTask ==
  \A i \in Items : sharedOutputTouched[i] => trailingState[i] \in Resolved

ItemRetiredAtMostOnce == ~faultDoubleRetire

ItemsRetireInFifoOrder == \A i \in 1..(N-1) : retired[i + 1] => retired[i]

TailRetainedUntilBothTasksResolve ==
  \A i \in Items :
    (pipelineState[i] = "pending" \/ trailingState[i] = "pending")
      => (executorItem = i \/ tailResident = i)
=============================================================================
