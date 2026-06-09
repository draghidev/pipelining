------------------------------- MODULE Pipeline -------------------------------
(* TLA+ model of the source-driven Draghi.Pipelining executor/advancer/callback protocol.

   The queue variant of Pipeline is verified separately and known correct; this spec
   targets the source-driven variant where the pipeline consumes from an
   IPipelineSource<T, TEnumerator> via `await foreach`. The source owns item storage
   and idle/wake semantics; the pipeline just consumes. Slon's idle handoff
   collapses into the source's MoveNextAsync logic.

   The waiter store has two tiers:
     - Inline slot (single (item, task) pair, zero-alloc storage). Holds the first
       committed waiter pre-escalation.
     - SPSC queue (lazy, allocated on the first overlap when the slot is occupied
       and a new waiter needs to be committed). FIFO once escalated.

   Captures:
     1. FIFO ordering: slot first (pre-escalation), then queue (post-escalation).
        On escalation, slot contents move to queue head if still held.
     2. The slot-vs-escalation CAS race on _hasSlot (executor wants to move,
        callback wants to drain).
     3. The do-while retry in DrainReadyWaiters (TryReclaimAdvancerForWork) - the
        mechanism that heals stranded callback increments, including the race
        recovery where a slot callback fired before EscalateAndEnqueue published
        the queue.
     4. The inline-callback race in CommitWaiter when task already completed.
     5. The loosened-CAS optimization in EscalateAndEnqueue: post-escalation the
        slot CAS is skipped because the slot stays definitively empty. Justified
        by invariant PostEscalationSlotEmpty.
     6. Weak-memory toggles on the deferred-publish handshake and advancer-latch
        release (see WeakMemory.tla).
     7. Explicit fairness so liveness checks are meaningful.

   Safety invariants:
     - ItemConservation: every item is in exactly one bucket.
     - CountConsistency: _waiters.Count = (hasSlot ? 1 : 0) + Len(waiters).
     - NoTripleActivation: activations[i] <= 2 for all i.
     - SlotConsistent: hasSlot <=> slotItem # NoItem <=> (some loc = InSlot).
     - PostEscalationSlotEmpty: escalated => ~hasSlot. The basis for the
       loosened-CAS optimization in EscalateAndEnqueue.

   Liveness:
     - EventuallyCompleted: every yielded item with a completed task is
       eventually drained to Completed (under fair scheduling). Holds when
       EmptySignalDeferred = TRUE (models post-fix code). Fails when
       EmptySignalDeferred = FALSE (models pre-fix code), with TLC producing
       a SlotCallbackBailsOut counterexample. The constant gates whether the
       depth/idle-TCS layer ordering is in place; see the SlotCallbackBailsOut
       transition comment.
*)

EXTENDS Integers, Sequences, FiniteSets, TLC, WeakMemory

CONSTANTS
  NumItems,
  WeakAdvancerRelease,    \* TRUE: AdvancerRelease modeled as Volatile.Write (visibility delay).
                             \* FALSE: modeled as Interlocked.Exchange (immediate global visibility).
  WeakHasExecutingPublish,\* TRUE: SourceYieldDeferred's set-to-TRUE on _hasExecutingItem is
                             \* Volatile.Write (release-only, no global fence). FALSE: Interlocked.Exchange.
  IsReferenceT,              \* TRUE: T is a reference type (GC write barrier guarantees release-fence
                             \* on plain reference writes). FALSE: T is a value type (no implicit fence).
  EmptySignalDeferred,       \* TRUE: models post-fix code where DrainSlotInline signals
                             \* OnDepthReachedZero AFTER _advancing.Release(), so the
                             \* WaitForEmptyAsync awaiter cannot resume early enough to commit a
                             \* slot waiter whose callback would bail on TryAcquire (the
                             \* SlotCallbackBailsOut transition is disabled). Liveness holds.
                             \* FALSE: models pre-fix code where the signal fired before the
                             \* release, opening the stranding window. Liveness fails with the
                             \* SlotCallbackBailsOut counterexample. Default TRUE.
  NudgeEnabled               \* TRUE: the post-escalation nudge transition is in the spec with WF
                             \* fairness (matches current code). FALSE: nudge is absent, liveness
                             \* must hold via callback-driven drainer chains alone. Toggle to verify
                             \* whether the nudge is load-bearing or a latency optimization.

VARIABLES
  loc,                  \* [Item -> Location] - per-item bucket.
  taskDone,             \* SUBSET Item - items whose pipelineTask SetResult has fired.
  activations,          \* [Item -> Nat] - per-item activation counter.
  callbackFired,        \* SUBSET Item - items whose completion callback has fired.

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

  \* WaiterStore counters: total count (slot + queue), completed-but-not-drained.
  storeCount,
  completedCount,

  \* WaiterStore slot tier.
  hasSlot,              \* WaiterStore._hasSlot (TRUE = slot occupied).
  slotItem,             \* the Item currently in the slot, or NoItem.
  escalated,            \* WaiterStore._queue is non-null (monotonic).

  \* WaiterStore queue tier, FIFO. Only used once escalated = TRUE.
  waiters,

  \* Mid-escalation tracking. Lets the spec visit the mixed states between Volatile.Write(_queue),
  \* Interlocked.Exchange(_hasSlot), the move enqueue, and the new-item enqueue inside the code's
  \* EscalateAndEnqueue. NoItem in escTail means "not mid-escalation."
  escPhase,        \* "idle" | "publish_done" | "cas_done" | "move_done"
  escTail,         \* Item or NoItem - the new tail item the executor is escalating
  escSlotClaimed,  \* BOOLEAN - did the executor's CAS-claim of the slot succeed this round

  \* TRUE while a drainer chain (DrainReadyWaiters' do-while loop) is in progress: set when
  \* a callback acquires advancer or the nudge fires, kept TRUE through AdvancerRelease
  \* (the do-while continues), cleared by DrainerChainExit when reclaim preconditions don't
  \* match (the do-while exits naturally). Real code's TryReclaimAdvancerForWork only runs
  \* inside this loop; gating AdvancerReclaim on the flag mirrors that constraint.
  drainerActive

\* Variable groupings - used as `UNCHANGED group_name` in action bodies for compactness.
publish_vars == <<executingItem, executingItemVisible, hasExecuting, hasExecutingVisible>>
tail_vars    == <<tailWaiter, hasTail>>
adv_vars     == <<advancing, advancingVisible>>
counters     == <<storeCount, completedCount>>
slot_vars    == <<hasSlot, slotItem, escalated>>
esc_vars     == <<escPhase, escTail, escSlotClaimed>>
drainer_vars == <<drainerActive>>
item_vars    == <<loc, taskDone, activations, callbackFired>>

vars == <<loc, taskDone, activations, callbackFired,
          executingItem, executingItemVisible, hasExecuting, hasExecutingVisible,
          tailWaiter, hasTail, advancing, advancingVisible, storeCount, completedCount,
          hasSlot, slotItem, escalated, waiters,
          escPhase, escTail, escSlotClaimed, drainerActive>>

Item == 1..NumItems
NoItem == 0
\* InWaitersPending: transient state inside CommitWaiter's queue path between waiters.Enqueue
\* (count incremented) and either the inline OnWaiterTaskCompleted call (if wasEmpty && task done)
\* or callback registration. During this window CompleteTask can fire and turn the would-be
\* register into an inline callback.
\* InSlot: the WaiterStore's inline slot is occupied with this item.
\* Nowhere: not yet yielded by the source.
\* InEscalation: the new tail item the executor took out of InTail and is mid-escalating.
\* It's in flight through PublishQueue → CASSlot → MoveSlot → EnqueueNew; not visible to
\* anything else (the slot tier doesn't see it, the queue doesn't have it yet) until the
\* final step lands it in InWaitersPending.
Locations == {"Nowhere", "Executing", "InTail", "InSlot", "InEscalation",
              "InWaitersPending", "InWaiters", "Completed"}

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
  /\ storeCount = 0
  /\ completedCount = 0
  /\ hasSlot = FALSE
  /\ slotItem = NoItem
  /\ escalated = FALSE
  /\ waiters = <<>>
  /\ callbackFired = {}
  /\ escPhase = "idle"
  /\ escTail = NoItem
  /\ escSlotClaimed = FALSE
  /\ drainerActive = FALSE

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
  /\ loc[i] \in {"Executing", "InTail", "InSlot", "InEscalation", "InWaitersPending", "InWaiters"}
  /\ taskDone' = taskDone \cup {i}
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 loc, activations, callbackFired, waiters, esc_vars, drainer_vars>>

(* ===========================================================================
   Executor actions: source yield + activation decision
   =========================================================================== *)

\* Source yields next item with no existing waiters - inline activation.
\* The source's MoveNextAsync is the wait; when it returns, we're already in the
\* executor with an item ready to dispatch. No queue, no wake signal.
SourceYieldInline ==
  /\ storeCount = 0
  /\ ~hasTail  \* tail must be committed before next yield
  /\ escPhase = "idle"  \* executor only yields next item after current commit completes
  /\ \A i \in Item : loc[i] # "Executing"  \* executor is sequential
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"  \* item not yet yielded by source
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       /\ activations' = [activations EXCEPT ![i] = @ + 1]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, callbackFired, waiters, esc_vars, drainer_vars>>

\* Source yields next item with existing waiters - deferred publish.
SourceYieldDeferred ==
  /\ storeCount > 0
  /\ ~hasTail
  /\ escPhase = "idle"
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
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars,
                 taskDone, activations, callbackFired, waiters, esc_vars, drainer_vars>>

\* Executor stores item as tail after ExecuteItemAsync returns (async pipelineTask).
ExecSetTail ==
  \E i \in Item :
    /\ loc[i] = "Executing"
    /\ loc' = [loc EXCEPT ![i] = "InTail"]
    /\ tailWaiter' = i
    /\ hasTail' = TRUE
    /\ UNCHANGED <<publish_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, esc_vars, drainer_vars>>

(* ===========================================================================
   CommitTailWaiter: the central handshake.

   Routes to one of three storage paths based on store state:
     - Slot path: pre-escalation, slot empty. Zero-alloc commit, wire slot callback.
     - Escalation entry: pre-escalation, slot occupied. Enters a multi-step
       escalation (PublishQueue → CASSlot → MoveSlot → EnqueueNew). During the
       intermediate phases other actions (notably the slot callback) can fire and
       observe the mixed state where escalated = TRUE but the slot is still
       occupied or its contents are still in slot fields. This is what splits the
       original atomic ExecCommitTail_X queue path.
     - Queue path: post-escalation. Just enqueue, wire queue callback. Loosened-CAS
       optimization: no slot CAS here because the slot is definitively empty
       (invariant PostEscalationSlotEmpty).
   =========================================================================== *)

\* CommitTailWaiter, executor wins the Exchange race (alreadyActivated = false).
\* Handles sync-success, slot path, and post-escalation enqueue (loosened CAS - no slot touch).
\* The first-escalation case (~escalated AND hasSlot) routes to ExecCommitTailEnterEscalation_X
\* below instead, where it splits into the multi-step PublishQueue → CAS → Move → Enqueue flow.
ExecCommitTailExecutorWins ==
  /\ hasTail
  /\ hasExecutingVisible  \* Exchange reads visible; TRUE → wins
  /\ escPhase = "idle"    \* no escalation in progress
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
            /\ storeCount' = storeCount
            /\ waiters' = waiters
            /\ completedCount' = completedCount
            /\ hasSlot' = hasSlot
            /\ slotItem' = slotItem
            /\ escalated' = escalated
          ELSE
            IF ~escalated /\ ~hasSlot
              THEN \* Slot path: zero-alloc, wire slot callback.
                /\ loc' = [loc EXCEPT ![i] = "InSlot"]
                /\ hasSlot' = TRUE
                /\ slotItem' = i
                /\ escalated' = escalated
                /\ waiters' = waiters
                /\ storeCount' = storeCount + 1
                /\ activations' = IF storeCount = 0
                                  THEN [activations EXCEPT ![i] = @ + 1]
                                  ELSE activations
                /\ completedCount' = completedCount
              ELSE \* Post-escalation queue path (escalated = TRUE).
                   \* Loosened CAS: no slot manipulation. Invariant PostEscalationSlotEmpty
                   \* says hasSlot stays FALSE here, so the original code's
                   \* Interlocked.Exchange(_hasSlot, 0) would have round-tripped 0 anyway.
                /\ escalated  \* precondition guard; first-escalation goes elsewhere
                /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
                /\ hasSlot' = hasSlot
                /\ slotItem' = slotItem
                /\ escalated' = escalated
                /\ waiters' = Append(waiters, i)
                /\ storeCount' = storeCount + 1
                /\ activations' = IF storeCount = 0
                                  THEN [activations EXCEPT ![i] = @ + 1]
                                  ELSE activations
                /\ completedCount' = completedCount
  /\ UNCHANGED <<adv_vars, taskDone, callbackFired, esc_vars, drainer_vars>>

\* CommitTailWaiter, advancer won (alreadyActivated = true). Executor skips _executingItem clear.
ExecCommitTailExecutorLoses ==
  /\ hasTail
  /\ ~hasExecutingVisible  \* executor's atomic Exchange reads visible; sees FALSE → loses
  /\ escPhase = "idle"
  /\ LET i == tailWaiter IN
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ IF i \in taskDone
          THEN
            /\ loc' = [loc EXCEPT ![i] = "Completed"]
            /\ activations' = activations
            /\ storeCount' = storeCount
            /\ waiters' = waiters
            /\ completedCount' = completedCount
            /\ hasSlot' = hasSlot
            /\ slotItem' = slotItem
            /\ escalated' = escalated
          ELSE
            IF ~escalated /\ ~hasSlot
              THEN \* Slot path. activated=true: no B-path activate.
                /\ loc' = [loc EXCEPT ![i] = "InSlot"]
                /\ hasSlot' = TRUE
                /\ slotItem' = i
                /\ escalated' = escalated
                /\ waiters' = waiters
                /\ storeCount' = storeCount + 1
                /\ activations' = activations
                /\ completedCount' = completedCount
              ELSE \* Post-escalation queue path (loosened CAS).
                /\ escalated
                /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
                /\ hasSlot' = hasSlot
                /\ slotItem' = slotItem
                /\ escalated' = escalated
                /\ waiters' = Append(waiters, i)
                /\ storeCount' = storeCount + 1
                /\ activations' = activations
                /\ completedCount' = completedCount
  /\ UNCHANGED <<publish_vars, adv_vars, taskDone, callbackFired, esc_vars, drainer_vars>>

(* ===========================================================================
   First-escalation entry + multi-step escalation flow.

   Step 1 (PublishQueue): publish _queue (escalated' = TRUE). hasSlot and slot
   contents are untouched. The new tail item moves from InTail to InEscalation
   and into escTail.

   Step 2 (CASSlot): Interlocked.Exchange(_hasSlot, 0). escSlotClaimed' records
   whether the executor's CAS won (slot still occupied) or lost (slot callback
   already drained it pre-publish).

   Step 3 (MoveSlot): if CAS won, append the slot item to waiters head and
   clear the slot fields. The slot item's loc transitions from InSlot to InWaiters.

   Step 4 (EnqueueNew): append escTail to waiters tail, increment count,
   loc[escTail] → InWaitersPending. Clears the escalation state.

   Slot callbacks fire from the mixed states between Step 1 and Step 3 (see
   SlotCallbackBailsDuringEscalation).
   =========================================================================== *)

\* Step 1: PublishQueue, executor-wins variant.
ExecCommitTailPublishQueueExecutorWins ==
  /\ hasTail
  /\ hasExecutingVisible
  /\ escPhase = "idle"
  /\ ~escalated
  /\ hasSlot
  /\ LET i == tailWaiter IN
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ escalated' = TRUE
       /\ escTail' = i
       /\ escPhase' = "publish_done"
       /\ escSlotClaimed' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InEscalation"]
  /\ UNCHANGED <<adv_vars, counters, hasSlot, slotItem, waiters,
                 taskDone, activations, callbackFired, drainer_vars>>

\* Step 1: PublishQueue, executor-loses variant.
ExecCommitTailPublishQueueExecutorLoses ==
  /\ hasTail
  /\ ~hasExecutingVisible
  /\ escPhase = "idle"
  /\ ~escalated
  /\ hasSlot
  /\ LET i == tailWaiter IN
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ escalated' = TRUE
       /\ escTail' = i
       /\ escPhase' = "publish_done"
       /\ escSlotClaimed' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InEscalation"]
  /\ UNCHANGED <<publish_vars, adv_vars, counters, hasSlot, slotItem, waiters,
                 taskDone, activations, callbackFired, drainer_vars>>

\* Step 2: CASSlot. Atomic Interlocked.Exchange(_hasSlot, 0). The race outcome with the
\* slot callback's pre-publish CAS is captured by reading hasSlot here: if the callback
\* drained the slot before the executor reached publish, hasSlot is FALSE and escSlotClaimed
\* records the loss; otherwise the executor wins and the slot fields are still populated.
ExecEscalationCASSlot ==
  /\ escPhase = "publish_done"
  /\ escSlotClaimed' = hasSlot
  /\ hasSlot' = FALSE
  /\ escPhase' = "cas_done"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters,
                 slotItem, escalated, waiters, escTail,
                 loc, taskDone, activations, callbackFired, drainer_vars>>

\* Step 3: MoveSlot. If the CAS won, append the slot item to waiters head and clear the slot
\* fields. The slot item's loc transitions from InSlot to InWaiters (its callback was wired
\* at slot-commit time and stays wired through the move).
ExecEscalationMoveSlot ==
  /\ escPhase = "cas_done"
  /\ escPhase' = "move_done"
  /\ IF escSlotClaimed
       THEN
         /\ waiters' = Append(waiters, slotItem)
         /\ loc' = [loc EXCEPT ![slotItem] = "InWaiters"]
         /\ slotItem' = NoItem
       ELSE
         /\ waiters' = waiters
         /\ loc' = loc
         /\ slotItem' = slotItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters,
                 hasSlot, escalated, escTail, escSlotClaimed,
                 taskDone, activations, callbackFired, drainer_vars>>

\* Step 4: EnqueueNew. Append escTail to waiters tail; increment count; loc → InWaitersPending.
\* Clears escalation state, returns to phase "idle". Activations follow the same rule as the
\* original atomic transition (storeCount = 0 was the wasEmpty trigger).
ExecEscalationEnqueueNew ==
  /\ escPhase = "move_done"
  /\ LET i == escTail IN
       /\ waiters' = Append(waiters, i)
       /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
       /\ storeCount' = storeCount + 1
       /\ activations' = IF storeCount = 0
                         THEN [activations EXCEPT ![i] = @ + 1]
                         ELSE activations
       /\ escPhase' = "idle"
       /\ escTail' = NoItem
       /\ escSlotClaimed' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, completedCount,
                 hasSlot, slotItem, escalated,
                 taskDone, callbackFired, drainer_vars>>

(* Inline-callback race in CommitWaiter queue path: at the post-Enqueue check, if task is
   already done the executor fires the wired callback inline; otherwise it registers it for
   later. Modeled by leaving the item in InWaitersPending and resolving via the three
   transitions below. (Slot path is handled by SlotCallback_ transitions directly, no pending
   window.) *)

ExecutorRegistersCallback ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i \notin taskDone
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, esc_vars, drainer_vars>>

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
    /\ drainerActive' = TRUE
    /\ UNCHANGED <<publish_vars, tail_vars, slot_vars, waiters,
                   taskDone, activations, storeCount, esc_vars>>

ExecutorInlineCallbackBailsOut ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i \in taskDone
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ callbackFired' = callbackFired \cup {i}
    /\ completedCount' = completedCount + 1
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                   taskDone, activations, storeCount, esc_vars, drainer_vars>>

(* ===========================================================================
   Callbacks (OnWaiterTaskCompleted / OnCommittedTaskCompleted).

   Slot callbacks (item still in InSlot when task completes) have two outcomes:
     - Drain inline: slot CAS wins, processes item directly.
     - Bail out: advancer already held, just bump completedCount.

   The slot callback also handles the post-escalation case where the slot was
   moved to queue head before the task completed: it falls through to the
   standard queue advancer drain. In the spec that's captured by the slot item
   being relocated to "InWaiters" by escalation, so its callback then fires via
   CallbackBecomesAdvancer / CallbackBailsOut just like a queue waiter.

   Queue callbacks (item in InWaiters) behave as in the pre-slot model.
   =========================================================================== *)

\* Slot callback fires while mid-escalation - between Step 1 (publish) and the move's
\* loc-transition in Step 3. The callback sees IsEscalated = TRUE and dispatches to
\* DrainReadyWaiters, which finds the queue empty (slot not yet moved). It bumps count,
\* the do-while TryReclaim also finds empty, and the callback exits. Net effect: count
\* incremented, callback marked fired, no other state changes. The stranded count must be
\* drained later by AdvancerReclaim (after move completes and the slot item is in queue head).
\* This is the transition that exposes the mixed-state race window the atomic original
\* spec collapsed away.
SlotCallbackBailsDuringEscalation ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InSlot"
    /\ slotItem = i
    /\ escalated  \* queue published, so callback's IsEscalated check returns TRUE
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ callbackFired' = callbackFired \cup {i}
    /\ completedCount' = completedCount + 1
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                   storeCount, loc, taskDone, activations, esc_vars, drainer_vars>>

\* Slot callback fires pre-escalation: bump + TryAcquire + CAS-claim slot + decrement + complete
\* item + count==0 C-path all in one atomic transition (matches real code's intra-method
\* atomicity inside DrainSlotInline). Does NOT release the advancer - that's SlotDrainerRelease.
\* The split (drain atomic, release separate) is what lets the spec visit the post-drain
\* pre-release window in which SlotCallbackBailsOut can fire and strand a count.
SlotCallbackDrains ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InSlot"
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ hasSlot
    /\ slotItem = i
    /\ ~escalated
    /\ callbackFired' = callbackFired \cup {i}
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    /\ drainerActive' = TRUE
    /\ hasSlot' = FALSE
    /\ slotItem' = NoItem
    /\ storeCount' = storeCount - 1
    /\ loc' = [loc EXCEPT ![i] = "Completed"]
    /\ completedCount' = completedCount  \* atomic bump+decrement: net zero
    /\ IF hasExecutingVisible /\ executingItemVisible # NoItem
         THEN
           /\ activations' = [activations EXCEPT ![executingItemVisible] = @ + 1]
           /\ hasExecuting' = FALSE
           /\ hasExecutingVisible' = FALSE
           /\ executingItem' = NoItem
           /\ executingItemVisible' = NoItem
         ELSE
           /\ activations' = activations
           /\ UNCHANGED publish_vars
    /\ UNCHANGED <<tail_vars, escalated, waiters, taskDone, esc_vars>>

\* Drainer's release step. Mirrors DrainSlotInline's _advancing.Release() tail. Between
\* SlotCallbackDrains and this release, the slot is empty but the advancer is still held.
\* A follow-up commit can land a new item in the slot, and SlotCallbackBailsOut fires if
\* that item's task is done. The race window the failing test exercises.
SlotDrainerRelease ==
  /\ advancing
  /\ drainerActive
  /\ ~escalated
  /\ Len(waiters) = 0
  /\ IF WeakAdvancerRelease
       THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
       ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
  /\ drainerActive' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars>>

\* The race the originally-atomic SlotCallbackDrains hid: a follow-up slot callback fires
\* while the previous drain still holds the advancer (between SlotDrainerDrains and
\* SlotDrainerRelease). TryAcquire fails, the callback bumps completedCount and exits
\* without draining its slot item. Pre-fix, no mechanism reclaims this stranded count in
\* slot mode (DrainSlotInline has no do-while equivalent of DrainReadyWaiters' TryReclaim).
\* TLC will surface this as a liveness violation: the stranded slot item never reaches
\* "Completed". Real code's fix is to signal OnDepthReachedZero after _advancing.Release(),
\* so the WaitForEmptyAsync awaiter can't resume early and commit the new slot item that
\* gets stranded here. That fix lives at the depth/idle-TCS layer, which this spec does
\* not model - the spec stops vouching for liveness it can't actually verify.
SlotCallbackBailsOut ==
  /\ ~EmptySignalDeferred  \* fix-toggle: TRUE blocks this transition (post-fix code shape)
  /\ \E i \in Item :
       /\ i \in taskDone
       /\ loc[i] = "InSlot"
       /\ slotItem = i
       /\ ~escalated
       /\ i \notin callbackFired
       /\ advancingVisible
       /\ callbackFired' = callbackFired \cup {i}
       /\ completedCount' = completedCount + 1
       /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                      storeCount, loc, taskDone, activations, esc_vars, drainer_vars>>

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
    /\ drainerActive' = TRUE
    /\ UNCHANGED <<publish_vars, tail_vars, slot_vars, waiters,
                   loc, taskDone, activations, storeCount, esc_vars>>

CallbackBailsOut ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InWaiters"
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ completedCount' = completedCount + 1
    /\ callbackFired' = callbackFired \cup {i}
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                   loc, taskDone, activations, storeCount, esc_vars, drainer_vars>>

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
       /\ storeCount' = storeCount - 1
       /\ completedCount' = completedCount - 1
       /\ \* D-path activate the next head, if any.
          activations' = IF Len(waiters) > 1
                         THEN [activations EXCEPT ![Head(Tail(waiters))] = @ + 1]
                         ELSE activations
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars,
                 taskDone, callbackFired, esc_vars, drainer_vars>>

\* Advancer at count=0, executes C-path activation under lock.
AdvancerCPath ==
  /\ advancing
  /\ storeCount = 0
  /\ hasExecutingVisible
  /\ Len(waiters) = 0
  /\ ~hasSlot
  /\ executingItemVisible # NoItem
  /\ LET exec == executingItemVisible IN
       /\ activations' = [activations EXCEPT ![exec] = @ + 1]
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, callbackFired, esc_vars, drainer_vars>>

\* Advancer release. Toggle picks Volatile.Write vs Exchange.
\* Aligned with DrainReadyWaiters' release: fires when queue is empty or head not done.
\* No slot clause: SlotCallbackDrains is atomic (acquire-drain-C-path-release), so the
\* advancer is never observed held between transitions in any state where hasSlot = TRUE.
AdvancerRelease ==
  /\ advancing
  /\ \/ Len(waiters) = 0
     \/ /\ Len(waiters) > 0
        /\ Head(waiters) \notin taskDone
  /\ IF WeakAdvancerRelease
       THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
       ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, drainer_vars>>

\* TryReclaimAdvancerForWork's acquire: Interlocked.Exchange (seq-cst).
\* Gated on drainerActive: real code's TryReclaim only fires from inside DrainReadyWaiters'
\* do-while loop. Outside an active drainer chain, no transition reclaims stranded counts -
\* they wait until the next entry to DrainReadyWaiters (a new callback fire or, with
\* NudgeEnabled, the explicit nudge).
AdvancerReclaim ==
  /\ drainerActive
  /\ ~advancingVisible
  /\ completedCount > 0
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, drainer_vars>>

\* The drainer chain exits when, post-release, the do-while's TryReclaim preconditions don't
\* match (queue empty or head not done). Clears drainerActive. Real code's do-while exits
\* and the calling method returns; subsequent stranded counts wait for a new callback entry.
DrainerChainExit ==
  /\ drainerActive
  /\ ~advancing
  /\ ~(completedCount > 0 /\ Len(waiters) > 0 /\ Head(waiters) \in taskDone)
  /\ drainerActive' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars>>

\* Post-escalation nudge: code's
\*   if (!isSlot && _waiterCompletedCount > 0 && _advancing.TryAcquire()) DrainReadyWaiters();
\* at the end of CommitWaiter when the escalation path was taken. Fires only when
\* NudgeEnabled = TRUE; toggle the constant to compare worlds with and without it. When
\* enabled, it provides an explicit reentry into the drainer chain after escalation; when
\* disabled, stranded counts must wait for the next callback fire to be drained.
\*
\* Precondition models the runtime check: just exited escalation (escPhase = "idle"), there
\* IS a stranded count (completedCount > 0), and TryAcquire succeeds (~advancingVisible).
\* The body matches a successful TryAcquire + DrainReadyWaiters entry: claim advancer,
\* mark drainerActive so subsequent AdvancerReclaim transitions can fire.
ExecPostEscalationNudge ==
  /\ NudgeEnabled
  /\ escPhase = "idle"
  /\ escalated      \* nudge is only meaningful after escalation has happened in this run
  /\ ~advancingVisible
  /\ completedCount > 0
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainerActive' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars>>

\* Visibility-propagation transitions for the *Visible shadow fields. Only fire when the
\* corresponding relaxed toggle is on; otherwise local and visible are always equal.
PropagateAdvancing ==
  /\ advancing # advancingVisible
  /\ PropagateOk(advancing, advancingVisible')
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, advancing, callbackFired, esc_vars, drainer_vars>>

PropagateHasExecuting ==
  /\ hasExecuting # hasExecutingVisible
  /\ PropagateOk(hasExecuting, hasExecutingVisible')
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, executingItem, executingItemVisible, hasExecuting, callbackFired, esc_vars, drainer_vars>>

PropagateExecutingItem ==
  /\ executingItem # executingItemVisible
  /\ PropagateOk(executingItem, executingItemVisible')
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, executingItem, hasExecuting, hasExecutingVisible, callbackFired, esc_vars, drainer_vars>>

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
  \/ ExecCommitTailPublishQueueExecutorWins
  \/ ExecCommitTailPublishQueueExecutorLoses
  \/ ExecEscalationCASSlot
  \/ ExecEscalationMoveSlot
  \/ ExecEscalationEnqueueNew
  \/ ExecutorRegistersCallback
  \/ ExecutorInlineCallbackBecomesAdvancer
  \/ ExecutorInlineCallbackBailsOut
  \/ SlotCallbackDrains
  \/ SlotDrainerRelease
  \/ SlotCallbackBailsOut
  \/ SlotCallbackBailsDuringEscalation
  \/ CallbackBecomesAdvancer
  \/ CallbackBailsOut
  \/ AdvancerDrainHead
  \/ AdvancerCPath
  \/ AdvancerRelease
  \/ AdvancerReclaim
  \/ DrainerChainExit
  \/ ExecPostEscalationNudge
  \/ PropagateAdvancing
  \/ PropagateHasExecuting
  \/ PropagateExecutingItem

Spec == Init /\ [][Next]_vars
        \* Fairness: assume the executor and advancer chain make progress.
        /\ WF_vars(SourceYieldInline \/ SourceYieldDeferred)
        /\ WF_vars(ExecSetTail)
        /\ WF_vars(ExecCommitTailExecutorWins \/ ExecCommitTailExecutorLoses)
        /\ WF_vars(ExecCommitTailPublishQueueExecutorWins \/ ExecCommitTailPublishQueueExecutorLoses)
        /\ WF_vars(ExecEscalationCASSlot)
        /\ WF_vars(ExecEscalationMoveSlot)
        /\ WF_vars(ExecEscalationEnqueueNew)
        /\ WF_vars(SlotCallbackBailsDuringEscalation)
        /\ WF_vars(AdvancerDrainHead)
        /\ WF_vars(AdvancerCPath)
        /\ WF_vars(AdvancerRelease)
        /\ WF_vars(AdvancerReclaim)
        /\ WF_vars(DrainerChainExit)
        \* Nudge fairness: enabled iff NudgeEnabled constant is TRUE. If FALSE, the action
        \* never fires (its NudgeEnabled precondition is FALSE), so fairness over it is vacuous;
        \* we still include WF for symmetry. The interesting test is whether liveness holds.
        /\ WF_vars(ExecPostEscalationNudge)
        /\ WF_vars(SlotCallbackDrains)
        /\ WF_vars(SlotDrainerRelease)
        /\ WF_vars(SlotCallbackBailsOut)
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
  /\ storeCount \in 0..NumItems
  /\ completedCount \in -NumItems..NumItems
  /\ hasSlot \in BOOLEAN
  /\ slotItem \in Item \cup {NoItem}
  /\ escalated \in BOOLEAN
  /\ escPhase \in {"idle", "publish_done", "cas_done", "move_done"}
  /\ escTail \in Item \cup {NoItem}
  /\ escSlotClaimed \in BOOLEAN
  /\ drainerActive \in BOOLEAN

\* WaiterStore count = (slot contribution) + (queue length). Use slotItem # NoItem rather than
\* hasSlot for the slot contribution: during the cas_done phase the executor has CAS'd hasSlot
\* to FALSE but hasn't yet cleared the slot fields (MoveSlot does that), so the slot item is
\* still counted in storeCount until MoveSlot transfers it to waiters. The new tail
\* (escTail) contributes only after EnqueueNew (Step 4) lands it in waiters.
CountConsistency ==
  storeCount = (IF slotItem # NoItem THEN 1 ELSE 0) + Len(waiters)

\* No item activated more than twice (1 expected, 2 caught the double-activation bug).
NoTripleActivation ==
  \A i \in Item : activations[i] <= 2

\* Items in waiters are exactly those with loc in {InWaitersPending, InWaiters}.
WaitersConsistent ==
  /\ \A i \in Item : (loc[i] \in {"InWaitersPending", "InWaiters"}) <=> (\E k \in 1..Len(waiters) : waiters[k] = i)

\* Escalation phase invariants. escTail is exactly the item in loc = "InEscalation"
\* (if any), and escPhase tracks which step we're at.
EscalationConsistent ==
  /\ (escPhase = "idle") <=> (escTail = NoItem)
  /\ (escTail # NoItem) => (loc[escTail] = "InEscalation")
  /\ (escPhase # "idle") => escalated
  /\ \A i \in Item : (loc[i] = "InEscalation") => (escTail = i)

\* Slot occupancy consistency. The slotItem field and the InSlot location always agree
\* on the identity (clauses 1+2). The hasSlot flag agrees with both, EXCEPT during the
\* mid-escalation "cas_done" / "move_done" phases where the executor's
\* Interlocked.Exchange(_hasSlot, 0) has fired (hasSlot = FALSE) but the slot fields
\* haven't yet been cleared - the slot is logically claimed but physically still populated
\* until the MoveSlot step. The escPhase = "idle" gating on clause 3 admits that window.
SlotConsistent ==
  LET inSlotExists == \E i \in Item : loc[i] = "InSlot"
  IN  /\ inSlotExists <=> (slotItem # NoItem)
      /\ slotItem # NoItem => loc[slotItem] = "InSlot"
      /\ escPhase = "idle" => (hasSlot <=> inSlotExists)

\* THE invariant justifying the loosened CAS in EscalateAndEnqueue: outside of mid-escalation
\* (escPhase = "idle"), the slot is definitively empty once we've escalated. No code path
\* refills the slot post-escalation (TryClaimSlotForNew short-circuits on Volatile.Read(_queue)
\* is null), so the CAS on _hasSlot in any subsequent EscalateAndEnqueue call would round-trip
\* 0 and is safely skipped.
\*
\* The escPhase = "idle" gating is necessary because the first escalation's intermediate phases
\* visibly violate this (queue published, slot still occupied) between PublishQueue and CASSlot.
\* That's the race window the split exposes; the optimized code DOESN'T rely on the invariant
\* during those intermediate phases (the executor itself is the only thing executing
\* EscalateAndEnqueue and it's currently inside the first call - no subsequent call observes
\* the mixed state).
PostEscalationSlotEmpty ==
  (escalated /\ escPhase = "idle") => ~hasSlot

\* Combined safety invariants. Single name keeps the .cfg simple and makes adding a new
\* invariant a one-file change rather than a two-file change.
Invariants ==
  /\ TypeOK
  /\ CountConsistency
  /\ NoTripleActivation
  /\ WaitersConsistent
  /\ SlotConsistent
  /\ PostEscalationSlotEmpty
  /\ EscalationConsistent

(* ===========================================================================
   Liveness
   =========================================================================== *)

\* Every item that gets yielded and has its task completed should eventually
\* reach Completed. (For items whose task is never completed, no expectation.)
\*
\* Holds under EmptySignalDeferred = TRUE (default, models post-fix code).
\* Violated under EmptySignalDeferred = FALSE (models pre-fix code): TLC produces a
\* SlotCallbackBailsOut counterexample where a slot waiter committed during the previous
\* drainer's post-drain pre-release window has its callback bail TryAcquire, strand a
\* completedCount bump, and never get drained.
EventuallyCompleted ==
  \A i \in Item :
    (loc[i] \in {"Executing", "InTail", "InSlot", "InEscalation", "InWaitersPending", "InWaiters"} /\ i \in taskDone)
      ~> (loc[i] = "Completed")

(* ===========================================================================
   Things to add as the model evolves
   ===========================================================================

   1. Inline OnWaiterTaskCompleted from CommitWaiter when wasEmpty && task done.
      Currently the model rolls that into ExecCommitTail_; should be a separate
      transition to expose the race window.

   2. RecoverCommittedTailWaiterAsync path - the executor's "task already
      faulted at commit time" branch. This was the source of the most recent
      test bug (CompleteAsync_DuringActiveRecovery_DrainsCleanly).

   3. Multiple concurrent callback firings - the model fires one at a time;
      real callbacks can interleave with each other and with the advancer.

   4. Source-side gating and pacing - the source's MoveNextAsync can defer
      yielding for reasons the pipeline doesn't see (backpressure, transaction
      state, connection state). Modeling this as a SourceGate predicate that
      blocks SourceYield_ would let us verify properties like "the pipeline
      makes progress whenever the source is willing to yield."

   5. Recovery flow (RecoverWaiter, _waiterRecoveryItem) - significant added
      state but covers a real bug class.

   6. Escalation-vs-slot-callback CAS race - both EscalateAndEnqueue and the
      slot callback do Exchange(_hasSlot, 0). Currently the spec models the
      executor's path explicitly (CAS branch in ExecCommitTail_) and the
      callback's via SlotCallbackDrains; the race is captured by interleaving
      but the "executor lost the CAS to a concurrent slot callback that
      already completed the item" branch could be made more explicit.
*)

=============================================================================
