------------------------------- MODULE Pipeline -------------------------------
(* TLA+ model of the source-driven Draghi.Pipelining executor/advancer/callback protocol.

   The queue variant of Pipeline is verified separately and known correct; this spec
   targets the source-driven variant where the pipeline consumes from an
   IPipelineSource<T, TEnumerator> via `await foreach`. The source owns item storage
   and idle/wake semantics; the pipeline just consumes. Slon's idle handoff
   collapses into the source's MoveNextAsync logic.

   Captures:
     1. FIFO ordering on _waiters (sequence).
     2. The do-while retry in DrainReadyWaiters (TryReclaimAdvancerForWork) -
        the mechanism that heals stranded callback increments.
     3. The inline-callback race in EnqueueWaiter when task already completed.
     4. Weak-memory toggles on the deferred-publish handshake and advancer-latch
        release (see WeakMemory.tla).
     5. Explicit fairness so liveness checks are meaningful.

   Safety invariants:
     - ItemConservation: every item is in exactly one bucket.
     - QueueCountConsistency: _waiterQueueCount = Len(waiters).
     - NoTripleActivation: activations[i] <= 2 for all i.

   Liveness:
     - EventuallyCompleted: every yielded item with a completed task is
       eventually drained to Completed (under fair scheduling).
*)

EXTENDS Integers, Sequences, FiniteSets, TLC, WeakMemory

CONSTANTS
  NumItems,
  WeakAdvancerRelease,    \* TRUE: AdvancerRelease modeled as Volatile.Write (visibility delay).
                             \* FALSE: modeled as Interlocked.Exchange (immediate global visibility).
  WeakHasExecutingPublish,\* TRUE: SourceYieldDeferred's set-to-TRUE on _hasExecutingItem is
                             \* Volatile.Write (release-only, no global fence). FALSE: Interlocked.Exchange.
  IsReferenceT               \* TRUE: T is a reference type (GC write barrier guarantees release-fence
                             \* on plain reference writes). FALSE: T is a value type (no implicit fence).

VARIABLES
  loc,                  \* [Item -> Location] - per-item bucket.
  taskDone,             \* SUBSET Item - items whose pipelineTask SetResult has fired.
  activations,          \* [Item -> Nat] - per-item activation counter.
  callbackFired,        \* SUBSET Item - items whose OnWaiterTaskCompleted callback has fired.

  \* Deferred-publish handshake. *Visible fields lag the writer-local one under the relaxed toggles
  \* (Volatile.Write / plain-value-T write); see the toggle comments for which fence is which.
  executingItem,
  executingItemVisible,
  hasExecuting,
  hasExecutingVisible,

  \* Pending tail (executor-only writes in real code; not modelled with weak memory).
  tailWaiter,
  hasTail,

  \* Advancer latch + its weak-memory shadow.
  advancing,
  advancingVisible,

  \* Counters: waiter queue length, completed-but-not-drained.
  queueCount,
  completedCount,

  \* Waiter queue, FIFO.
  waiters

\* Variable groupings - used as `UNCHANGED group_name` in action bodies for compactness.
publish_vars == <<executingItem, executingItemVisible, hasExecuting, hasExecutingVisible>>
tail_vars    == <<tailWaiter, hasTail>>
adv_vars     == <<advancing, advancingVisible>>
counters     == <<queueCount, completedCount>>
item_vars    == <<loc, taskDone, activations, callbackFired>>

vars == <<loc, taskDone, activations, callbackFired,
          executingItem, executingItemVisible, hasExecuting, hasExecutingVisible,
          tailWaiter, hasTail, advancing, advancingVisible, queueCount, completedCount,
          waiters>>

Item == 1..NumItems
NoItem == 0
\* InWaitersPending: transient state inside EnqueueWaiter between _waiters.Enqueue (count incremented)
\* and either the inline OnWaiterTaskCompleted call (if wasEmpty && task done) or callback registration.
\* During this window, CompleteTask can fire and turn the would-be register into an inline callback.
\* Nowhere: not yet yielded by the source.
Locations == {"Nowhere", "Executing", "InTail", "InWaitersPending", "InWaiters", "Completed"}

(* ===========================================================================
   Init
   =========================================================================== *)

Init ==
  /\ loc = [i \in Item |-> "Nowhere"]
  /\ taskDone = {}
  /\ activations = [i \in Item |-> 0]
  /\ executingItem = NoItem
  /\ executingItemVisible = NoItem
  /\ hasExecuting = FALSE
  /\ hasExecutingVisible = FALSE
  /\ tailWaiter = NoItem
  /\ hasTail = FALSE
  /\ advancing = FALSE
  /\ advancingVisible = FALSE
  /\ queueCount = 0
  /\ completedCount = 0
  /\ waiters = <<>>
  /\ callbackFired = {}

(* ===========================================================================
   Helpers
   =========================================================================== *)

WaiterHead == IF Len(waiters) > 0 THEN Head(waiters) ELSE NoItem

(* ===========================================================================
   External actions: task completion
   =========================================================================== *)

\* Test thread sets pipelineTask result.
CompleteTask(i) ==
  /\ i \notin taskDone
  /\ loc[i] \in {"Executing", "InTail", "InWaitersPending", "InWaiters"}
  /\ taskDone' = taskDone \cup {i}
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters,
                 loc, activations, callbackFired, waiters>>

(* ===========================================================================
   Executor actions: source yield + activation decision
   =========================================================================== *)

\* Source yields next item with no existing waiters - inline activation.
\* The source's MoveNextAsync is the wait; when it returns, we're already in the
\* executor with an item ready to dispatch. No queue, no wake signal.
SourceYieldInline ==
  /\ queueCount = 0
  /\ ~hasTail  \* tail must be committed before next yield
  /\ \A i \in Item : loc[i] # "Executing"  \* executor is sequential
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"  \* item not yet yielded by source
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       /\ activations' = [activations EXCEPT ![i] = @ + 1]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters,
                 taskDone, callbackFired, waiters>>

\* Source yields next item with existing waiters - deferred publish.
SourceYieldDeferred ==
  /\ queueCount > 0
  /\ ~hasTail
  /\ \A i \in Item : loc[i] # "Executing"
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       \* Reference write: ref-T gets GC barrier release (fenced); value-T gets plain STR (relaxed).
       /\ IF IsReferenceT
            THEN FencedWriteOk(executingItem', executingItemVisible', i)
            ELSE WeakWriteOk(executingItem', executingItemVisible', i, executingItemVisible)
       /\ IF WeakHasExecutingPublish
            THEN WeakWriteOk(hasExecuting', hasExecutingVisible', TRUE, hasExecutingVisible)
            ELSE FencedWriteOk(hasExecuting', hasExecutingVisible', TRUE)
  /\ UNCHANGED <<tail_vars, adv_vars, counters,
                 taskDone, activations, callbackFired, waiters>>

\* Executor stores item as tail after ExecuteItemAsync returns (async pipelineTask).
ExecSetTail ==
  \E i \in Item :
    /\ loc[i] = "Executing"
    /\ loc' = [loc EXCEPT ![i] = "InTail"]
    /\ tailWaiter' = i
    /\ hasTail' = TRUE
    /\ UNCHANGED <<publish_vars, adv_vars, counters, waiters,
                   taskDone, activations, callbackFired>>

(* ===========================================================================
   CommitTailWaiter: the central handshake
   =========================================================================== *)

\* CommitTailWaiter, executor wins the Exchange race (alreadyActivated = false).
ExecCommitTailExecutorWins ==
  /\ hasTail
  /\ hasExecutingVisible  \* Exchange reads visible; TRUE → wins
  /\ LET i == tailWaiter IN
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ IF i \in taskDone
          THEN \* sync-success: CompleteWaiter inline
            /\ loc' = [loc EXCEPT ![i] = "Completed"]
            /\ activations' = activations
            /\ queueCount' = queueCount
            /\ waiters' = waiters
            /\ completedCount' = completedCount
          ELSE \* EnqueueWaiter; B-path activates if waiters was empty
            /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
            /\ waiters' = Append(waiters, i)
            /\ queueCount' = queueCount + 1
            /\ activations' = IF Len(waiters) = 0
                              THEN [activations EXCEPT ![i] = @ + 1]
                              ELSE activations
            /\ completedCount' = completedCount
  /\ UNCHANGED <<adv_vars, taskDone, callbackFired>>

\* CommitTailWaiter, advancer won (alreadyActivated = true). Executor skips _executingItem clear.
ExecCommitTailExecutorLoses ==
  /\ hasTail
  /\ ~hasExecutingVisible  \* executor's atomic Exchange reads visible; sees FALSE → loses
  /\ LET i == tailWaiter IN
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ IF i \in taskDone
          THEN
            /\ loc' = [loc EXCEPT ![i] = "Completed"]
            /\ activations' = activations
            /\ queueCount' = queueCount
            /\ waiters' = waiters
            /\ completedCount' = completedCount
          ELSE \* EnqueueWaiter(activated=true), no B-path activate. Same InWaitersPending window.
            /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
            /\ waiters' = Append(waiters, i)
            /\ queueCount' = queueCount + 1
            /\ activations' = activations
            /\ completedCount' = completedCount
  /\ UNCHANGED <<publish_vars, adv_vars, taskDone, callbackFired>>

(* Inline-callback race in EnqueueWaiter: at the post-Enqueue check, if task is already done
   the executor fires OnWaiterTaskCompleted inline; otherwise it registers a callback that
   fires on the threadpool later. Modeled by leaving the item in InWaitersPending and resolving
   the branch via the three transitions below. *)

ExecutorRegistersCallback ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i \notin taskDone
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, waiters,
                   taskDone, activations, callbackFired>>

ExecutorInlineCallbackBecomesAdvancer ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i \in taskDone
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ callbackFired' = callbackFired \cup {i}
    /\ completedCount' = completedCount + 1
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    /\ UNCHANGED <<publish_vars, tail_vars, waiters,
                   taskDone, activations, queueCount>>

ExecutorInlineCallbackBailsOut ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i \in taskDone
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ callbackFired' = callbackFired \cup {i}
    /\ completedCount' = completedCount + 1
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, waiters,
                   taskDone, activations, queueCount>>

(* ===========================================================================
   Callbacks (OnWaiterTaskCompleted)
   =========================================================================== *)

\* Callback for an item in _waiters whose task completed. Reads `advancingVisible` via Exchange.
CallbackBecomesAdvancer ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InWaiters"
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    /\ completedCount' = completedCount + 1
    /\ callbackFired' = callbackFired \cup {i}
    /\ UNCHANGED <<publish_vars, tail_vars, waiters,
                   loc, taskDone, activations, queueCount>>

CallbackBailsOut ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InWaiters"
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ completedCount' = completedCount + 1
    /\ callbackFired' = callbackFired \cup {i}
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, waiters,
                   loc, taskDone, activations, queueCount>>

(* ===========================================================================
   Advancer actions
   =========================================================================== *)

\* Advancer drains the head of _waiters if its task is completed.
\* Models DrainReadyWaiters inner loop: TryPeek + TryDequeue + Decrement + CompleteWaiter.
AdvancerDrainHead ==
  /\ advancing
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  /\ LET i == Head(waiters) IN
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ waiters' = Tail(waiters)
       /\ queueCount' = queueCount - 1
       /\ completedCount' = completedCount - 1
       /\ \* D-path activate the next head, if any.
          activations' = IF Len(waiters) > 1
                         THEN [activations EXCEPT ![Head(Tail(waiters))] = @ + 1]
                         ELSE activations
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars,
                 taskDone, callbackFired>>

\* Advancer at count=0, executes C-path activation under lock.
AdvancerCPath ==
  /\ advancing
  /\ queueCount = 0
  /\ hasExecutingVisible
  /\ Len(waiters) = 0
  /\ executingItemVisible # NoItem
  /\ LET exec == executingItemVisible IN
       /\ activations' = [activations EXCEPT ![exec] = @ + 1]
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
  /\ UNCHANGED <<tail_vars, adv_vars, counters, waiters,
                 loc, taskDone, callbackFired>>

\* Advancer release (line 736 or TryReclaim's line 753). Toggle picks Volatile.Write vs Exchange.
AdvancerRelease ==
  /\ advancing
  /\ \/ Len(waiters) = 0
     \/ /\ Len(waiters) > 0
        /\ Head(waiters) \notin taskDone
  /\ IF WeakAdvancerRelease
       THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
       ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
  /\ UNCHANGED <<publish_vars, tail_vars, counters, waiters,
                 loc, taskDone, activations, callbackFired>>

\* TryReclaimAdvancerForWork's acquire: Interlocked.Exchange (seq-cst).
AdvancerReclaim ==
  /\ ~advancingVisible
  /\ completedCount > 0
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, waiters,
                 loc, taskDone, activations, callbackFired>>

\* Visibility-propagation transitions for the *Visible shadow fields. Only fire when the
\* corresponding relaxed toggle is on; otherwise local and visible are always equal.
PropagateAdvancing ==
  /\ advancing # advancingVisible
  /\ PropagateOk(advancing, advancingVisible')
  /\ UNCHANGED <<publish_vars, tail_vars, counters, waiters,
                 loc, taskDone, activations, advancing, callbackFired>>

PropagateHasExecuting ==
  /\ hasExecuting # hasExecutingVisible
  /\ PropagateOk(hasExecuting, hasExecutingVisible')
  /\ UNCHANGED <<tail_vars, adv_vars, counters, waiters,
                 loc, taskDone, activations, executingItem, executingItemVisible, hasExecuting, callbackFired>>

PropagateExecutingItem ==
  /\ executingItem # executingItemVisible
  /\ PropagateOk(executingItem, executingItemVisible')
  /\ UNCHANGED <<tail_vars, adv_vars, counters, waiters,
                 loc, taskDone, activations, executingItem, hasExecuting, hasExecutingVisible, callbackFired>>

(* ===========================================================================
   Next-state relation
   =========================================================================== *)

Next ==
  \/ \E i \in Item : CompleteTask(i)
  \/ SourceYieldInline
  \/ SourceYieldDeferred
  \/ ExecSetTail
  \/ ExecCommitTailExecutorWins
  \/ ExecCommitTailExecutorLoses
  \/ ExecutorRegistersCallback
  \/ ExecutorInlineCallbackBecomesAdvancer
  \/ ExecutorInlineCallbackBailsOut
  \/ CallbackBecomesAdvancer
  \/ CallbackBailsOut
  \/ AdvancerDrainHead
  \/ AdvancerCPath
  \/ AdvancerRelease
  \/ AdvancerReclaim
  \/ PropagateAdvancing
  \/ PropagateHasExecuting
  \/ PropagateExecutingItem

Spec == Init /\ [][Next]_vars
        \* Fairness: assume the executor and advancer chain make progress.
        /\ WF_vars(SourceYieldInline \/ SourceYieldDeferred)
        /\ WF_vars(ExecSetTail)
        /\ WF_vars(ExecCommitTailExecutorWins \/ ExecCommitTailExecutorLoses)
        /\ WF_vars(AdvancerDrainHead)
        /\ WF_vars(AdvancerCPath)
        /\ WF_vars(AdvancerRelease)
        /\ WF_vars(AdvancerReclaim)
        /\ WF_vars(CallbackBecomesAdvancer)
        /\ WF_vars(CallbackBailsOut)
        /\ WF_vars(ExecutorRegistersCallback)
        /\ WF_vars(ExecutorInlineCallbackBecomesAdvancer)
        /\ WF_vars(ExecutorInlineCallbackBailsOut)
        \* Hardware: pending writes do propagate eventually (cache coherence).
        /\ WF_vars(PropagateAdvancing)
        /\ WF_vars(PropagateHasExecuting)
        /\ WF_vars(PropagateExecutingItem)
        \* External: assume the test thread eventually completes every task.
        /\ \A i \in Item : WF_vars(CompleteTask(i))

(* ===========================================================================
   Invariants
   =========================================================================== *)

\* Every item is in exactly one logical location.
TypeOK ==
  /\ loc \in [Item -> Locations]
  /\ activations \in [Item -> 0..3]  \* bounded for TLC
  /\ queueCount \in 0..NumItems
  /\ completedCount \in -NumItems..NumItems

\* _waiterQueueCount tracks Len(waiters). They should agree.
QueueCountConsistency ==
  queueCount = Len(waiters)

\* No item activated more than twice (1 expected, 2 caught the double-activation bug).
NoTripleActivation ==
  \A i \in Item : activations[i] <= 2

\* Items in waiters are exactly those with loc in {InWaitersPending, InWaiters}.
WaitersConsistent ==
  /\ \A i \in Item : (loc[i] \in {"InWaitersPending", "InWaiters"}) <=> (\E k \in 1..Len(waiters) : waiters[k] = i)

\* Combined safety invariants. Single name keeps the .cfg simple and makes adding a new
\* invariant a one-file change rather than a two-file change.
Invariants ==
  /\ TypeOK
  /\ QueueCountConsistency
  /\ NoTripleActivation
  /\ WaitersConsistent

(* ===========================================================================
   Liveness
   =========================================================================== *)

\* Every item that gets yielded and has its task completed should eventually
\* reach Completed. (For items whose task is never completed, no expectation.)
EventuallyCompleted ==
  \A i \in Item :
    (loc[i] \in {"Executing", "InTail", "InWaitersPending", "InWaiters"} /\ i \in taskDone)
      ~> (loc[i] = "Completed")

(* ===========================================================================
   Things to add as the model evolves
   ===========================================================================

   1. Inline OnWaiterTaskCompleted from EnqueueWaiter when wasEmpty && task done.
      Currently the model rolls that into ExecCommitTail*; should be a separate
      transition to expose the race window.

   2. RecoverCommittedTailWaiterAsync path - the executor's "task already
      faulted at commit time" branch. This was the source of the most recent
      test bug (CompleteAsync_DuringActiveRecovery_DrainsCleanly).

   3. Multiple concurrent callback firings - the model fires one at a time;
      real callbacks can interleave with each other and with the advancer.

   4. Source-side gating and pacing - the source's MoveNextAsync can defer
      yielding for reasons the pipeline doesn't see (backpressure, transaction
      state, connection state). Modeling this as a SourceGate predicate that
      blocks SourceYield* would let us verify properties like "the pipeline
      makes progress whenever the source is willing to yield."

   5. Recovery flow (RecoverWaiter, _waiterRecoveryItem) - significant added
      state but covers a real bug class.
*)

=============================================================================
