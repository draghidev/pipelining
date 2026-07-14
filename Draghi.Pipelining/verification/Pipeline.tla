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

EXTENDS Integers, Sequences, FiniteSets, TLC, WeakMemory, WaiterStore

\* NumItems and the store state (slot tier, queue tier, count, escalation in-flight
\* locals) live in WaiterStore.tla - the spec's module boundary mirrors the code's
\* (WaiterStore.cs owns storage and the escalation protocol; Pipeline orchestrates
\* roles against it). EXTENDS shares the variable namespace, so reads reference store
\* variables directly (as the code's plain/volatile loads do) and writes go through
\* the Store* operators mirroring the .cs API.

CONSTANTS
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
  NudgeEnabled,              \* TRUE: the post-escalation nudge transition is in the spec with WF
                             \* fairness (matches current code). FALSE: nudge is absent, liveness
                             \* must hold via callback-driven drainer chains alone. Toggle to verify
                             \* whether the nudge is load-bearing or a latency optimization.
  SlotReclaimEnabled,        \* TRUE: DrainSlotInline loops post-release to reclaim a waiter
                             \* whose callback TryAcquire-bailed against our held advancer
                             \* (mirrors the queue drain's do-while). FALSE models shipped code,
                             \* where the slot-mode strand is unrecovered: field signature is
                             \* "Operation timed out waiting for activation" (~2/20 contended
                             \* Slon.Tests runs) - the next activation never comes until the
                             \* heartbeat activation timeout rescues the flow. Expect
                             \* EventuallyCompleted to FAIL with FALSE, hold with TRUE.
  ConsumeBeforeRepublish,    \* The consume-ordering contract under design (June 2026). The slot
                             \* drain consumes the waiter task (GetResult - the release point for
                             \* per-item resources such as Slon's shared pipelined-read promise
                             \* tenure) either BEFORE the count decrement republishes the pipeline
                             \* position (TRUE, the proposed contract) or AFTER it (FALSE, the
                             \* code as shipped). FALSE must falsify NoTenureClash: the executor's
                             \* Count==0 inline-activation gate dispatches a successor against a
                             \* still-held tenure (Slon's observed ThrowAlreadyStarted at
                             \* CommandFlow.DispatchPipelinedRead). TRUE must satisfy it.
  SlotChainActivation        \* The slot-mode D-path under design (June 2026, lost-activation
                             \* round 2). The queue drain partitions activation responsibility on
                             \* the value its atomic DecrementCount returns: count > 0 means a
                             \* successor's commit landed before the decrement, observed a
                             \* non-empty count (wasEmpty = false), and skipped self-activation -
                             \* so the DRAINER activates the new head (Pipeline.cs DrainReadyWaiters'
                             \* else-arm). DrainSlotInline discards that return value: it has only
                             \* the count==0 C-path arm, and its lock block clears a consumed
                             \* publish without activating when Count > 0. A successor committed
                             \* into the freed slot during the claim window (widened by the
                             \* consume-before-republish hoist: GetResult sits between claim and
                             \* decrement) is therefore activated by NOBODY. Field signature:
                             \* suite hang, stuck flow with _pendingActivationControl = null
                             \* (ActivateHeadItem never called), dumps reclaim_hang_72057+.
                             \* FALSE models shipped code: EventuallyActivated must FAIL.
                             \* TRUE models the fix: the drain-complete step branches on the
                             \* captured decrement value (drainRemaining) - > 0 activates the new
                             \* head (slot occupant; when a first escalation is relocating it to
                             \* the queue, the obligation is handed to the escalating commit via
                             \* the pendingHeadActivation flag dance rather than waiting out the
                             \* move - no thread ever waits on another thread's progress); = 0
                             \* runs the C-path lock block, restructured to leave the publish in
                             \* place when Count > 0 (the old clear-without-activate branch was
                             \* itself a loss: the cleared publish's item had no remaining
                             \* activator in slot mode). EventuallyActivated must HOLD.

VARIABLES
  loc,                  \* [Item -> Location] - per-item bucket.
  taskDone,             \* SUBSET Item - items whose pipelineTask SetResult has fired.
  activations,          \* [Item -> Nat] - per-item activation counter.
  callbackFired,        \* SUBSET Item - items whose completion callback has fired.
  failed,               \* SUBSET Item - items whose ExecuteItemAsync threw (bounded
                        \* per-item once: recovery-on-recovery is in the "things to add"
                        \* backlog, and bounding keeps activations[i] under the existing
                        \* NoTripleActivation bound for the in-scope recovery cycle).

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

  \* Completed-but-not-drained tally (Pipeline._waiterCompletedCount; the store's count
  \* and tiers live in WaiterStore.tla).
  completedCount,

  \* TRUE while a drainer chain (DrainReadyWaiters' do-while loop) is in progress: set when
  \* a callback acquires advancer or the nudge fires, kept TRUE through AdvancerRelease
  \* (the do-while continues), cleared by DrainerChainExit when reclaim preconditions don't
  \* match (the do-while exits naturally). Real code's TryReclaimAdvancerForWork only runs
  \* inside this loop; gating AdvancerReclaim on the flag mirrors that constraint.
  drainerActive,

  \* Slot-drain phase tracking. The previously-atomic SlotCallbackDrains hid the windows
  \* between TryClaimSlotForDrain (slot freed for commits), DecrementCount (position
  \* republished to the executor's Count==0 gate), and CompleteWaiter. Splitting exposes:
  \* (a) a commit CAS-ing into the freed slot before the decrement lands (the code's stale
  \* Debug.Assert(count == 0), see assertFailed), and (b) the consume-vs-republish ordering
  \* (see tenure). "idle" | "claimed" (slot claimed, count not yet decremented) | "counted"
  \* (count decremented, CompleteWaiter not yet run).
  drainPhase,
  drainItem,        \* the Item the slot drainer claimed, NoItem outside a drain.
  drainRemaining,   \* the value the slot drain's atomic DecrementCount returned (captured at
                    \* SlotDrainCount, consumed by the SlotDrainComplete* branch choice). The
                    \* single linearization point that partitions activation responsibility
                    \* between the drainer (> 0: successor committed before the decrement,
                    \* skipped self-activation) and the commit (= 0: any later commit observes
                    \* count 1 = wasEmpty and self-activates). 0 outside a drain.
  pendingHeadActivation,
                    \* The chain-arm-to-escalator activation handoff (Pipeline._pendingHeadActivation,
                    \* Interlocked.Exchange exactly-once claim). Set by the drainer when its chain
                    \* obligation meets an in-flight first escalation (the head is being relocated
                    \* slot -> queue and cannot be named); consumed by whichever side's Exchange
                    \* wins: the drainer's one-shot post-publish re-peek (head became visible) or
                    \* the escalating commit's post-enqueue compensation check (slotWasMoved gate).
                    \* Replaces a spin on the escalation's claim->move window: no thread waits on
                    \* another thread's progress.

  \* Per-item resource tenure (abstraction of Slon's per-protocol shared pipelined-read
  \* promise). Acquired by an inline-dispatched item at SourceYieldInline (the real
  \* dispatch's TryStart); released when the item's waiter task is CONSUMED (GetResult ->
  \* Reset): at the slot drain (timing per ConsumeBeforeRepublish), the queue drain head,
  \* the sync-success commit, or item failure. Deferred-dispatched items are deliberately
  \* NOT modeled: the deferred path's bridge consumes at SetResult time, before any waiter
  \* bookkeeping, and is contract-safe by construction.
  tenure,
  tenureClash,      \* TRUE when an inline dispatch found the tenure still held
                    \* (ThrowAlreadyStarted in the real code). Bug witness.

  \* TRUE when the slot drain's post-decrement count was non-zero - the code's
  \* Debug.Assert(count == 0) firing (observed as exit-134 in Slon.Tests runs 12/25,
  \* June 2026). Bug witness for the stale invariant.
  assertFailed

\* Variable groupings - used as `UNCHANGED group_name` in action bodies for compactness.
publish_vars == <<executingItem, executingItemVisible, hasExecuting, hasExecutingVisible>>
tail_vars    == <<tailWaiter, hasTail>>
adv_vars     == <<advancing, advancingVisible>>
counters     == <<storeCount, completedCount>>
\* slot_vars / esc_vars / store_vars come from WaiterStore.tla.
drainer_vars == <<drainerActive>>
item_vars    == <<loc, taskDone, activations, callbackFired, failed>>
\* June 2026 additions (slot-drain split + tenure). Actions that predate them and don't
\* interact get UNCHANGED aux_vars applied at the Next relation rather than per-body.
aux_vars     == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                  tenure, tenureClash, assertFailed>>

vars == <<loc, taskDone, activations, callbackFired, failed,
          executingItem, executingItemVisible, hasExecuting, hasExecutingVisible,
          tailWaiter, hasTail, advancing, advancingVisible, storeCount, completedCount,
          hasSlot, slotItem, escalated, waiters,
          escPhase, escTail, escSlotClaimed, escMoved, drainerActive,
          drainPhase, drainItem, drainRemaining, pendingHeadActivation,
          tenure, tenureClash, assertFailed>>

\* Item / NoItem come from WaiterStore.tla.
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
\* Recovering: ExecuteItemAsync threw for this item. ClearExecutingItem has run, the failed
\* item is waiting for RecoverItem to install its substitute. Modelled as a re-entry of the
\* same item identity rather than a fresh item slot - the substitute's job is to occupy the
\* failed item's pipeline position with the right activation timing, and any fidelity loss
\* on the substitute being a "different item" is outweighed by keeping NumItems bounded.
\* Draining: claimed out of the slot by the drainer (TryClaimSlotForDrain won), CompleteWaiter
\* not yet run. The slot fields are already empty - a successor commit can reuse the slot
\* while this item is mid-drain.
Locations == {"Nowhere", "Executing", "InTail", "InSlot", "InEscalation",
              "InWaitersPending", "InWaiters", "Recovering", "Draining", "Completed"}

(* ===========================================================================
   Init
   =========================================================================== *)

Init ==
  /\ StoreInit
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
  /\ completedCount = 0
  /\ callbackFired = {}
  /\ drainerActive = FALSE
  /\ failed = {}
  /\ drainPhase = "idle"
  /\ drainItem = NoItem
  /\ drainRemaining = 0
  /\ pendingHeadActivation = FALSE
  /\ tenure = NoItem
  /\ tenureClash = FALSE
  /\ assertFailed = FALSE

(* ===========================================================================
   External actions: task completion
   =========================================================================== *)

\* Test thread sets pipelineTask result, activated item. This is the variant liveness leans
\* on (WF in Spec): an activated flow's completion is an obligation under fair scheduling.
\*
\* Split from the unactivated variant (June 2026): activation is AT-MOST-ONCE, not a
\* completion prerequisite - cancellation or a flow needing no read I/O can complete before
\* (or instead of) ever being activated. But modeling ALL completion as unconditionally
\* available AND fair (the pre-split spec) made every lost activation invisible: the lost
\* item still "completed" externally, so EventuallyCompleted held while the real pipeline
\* hung (lost-activation round 2, dumps reclaim_hang_72057+). The split keeps both truths:
\* unactivated completion is POSSIBLE (CompleteTaskUnactivated, no fairness), activated
\* completion is GUARANTEED (this action, WF).
CompleteTask(i) ==
  /\ i \notin taskDone
  /\ activations[i] > 0
  /\ loc[i] \in {"Executing", "InTail", "InSlot", "InEscalation", "InWaitersPending", "InWaiters"}
  /\ taskDone' = taskDone \cup {i}
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 loc, activations, callbackFired, failed, waiters, esc_vars, drainer_vars>>

\* Completion without activation: cancellation, or a flow whose pipeline task settles from
\* its write phase with no read I/O. Deliberately NO fairness over this action - it may
\* happen, it never must, so no liveness argument can lean on it. Behaviors where it never
\* fires are exactly the ones that expose a lost activation as a hang; behaviors where it
\* does fire exercise the drain/commit guards for completed-but-never-activated waiters
\* (the IsCompleted skip in CommitWaiter and DrainSlotInline's chain arm, the reclaim
\* pickup behind it).
CompleteTaskUnactivated(i) ==
  /\ i \notin taskDone
  /\ activations[i] = 0
  /\ loc[i] \in {"Executing", "InTail", "InSlot", "InEscalation", "InWaitersPending", "InWaiters"}
  /\ taskDone' = taskDone \cup {i}
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 loc, activations, callbackFired, failed, waiters, esc_vars, drainer_vars>>

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
  /\ \A i \in Item : loc[i] # "Recovering" \* pending recovery must install before next yield
  /\ tenure = NoItem  \* the dispatch's TryStart; the held case is SourceYieldInlineClash
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"  \* item not yet yielded by source
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       /\ activations' = [activations EXCEPT ![i] = @ + 1]
       /\ tenure' = i
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, callbackFired, failed, waiters, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenureClash, assertFailed>>

\* Bug witness: the executor's Count==0 gate passed but the previous inline tenure is still
\* held - the dispatch's TryStart lands on a started shared promise. Real-world signature:
\* InvalidOperationException("The async method is already executing") from
\* CommandFlow.DispatchPipelinedRead, ~7/50 Slon.Tests suite runs pre-contract. The state
\* freezes the witness (no further modeling of the ensuing recovery; NoTenureClash flags it).
SourceYieldInlineClash ==
  /\ storeCount = 0
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ \A i \in Item : loc[i] # "Executing"
  /\ \A i \in Item : loc[i] # "Recovering"
  /\ tenure # NoItem
  /\ \E i \in Item : loc[i] = "Nowhere"
  /\ tenureClash' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, callbackFired, failed, waiters, esc_vars, drainer_vars,
                 loc, activations, drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                 tenure, assertFailed>>

\* Source yields next item with existing waiters - deferred publish.
SourceYieldDeferred ==
  /\ storeCount > 0
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ \A i \in Item : loc[i] # "Executing"
  /\ \A i \in Item : loc[i] # "Recovering"
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
                 taskDone, activations, callbackFired, failed, waiters, esc_vars, drainer_vars>>

\* Executor stores item as tail after ExecuteItemAsync returns (async pipelineTask).
ExecSetTail ==
  \E i \in Item :
    /\ loc[i] = "Executing"
    /\ loc' = [loc EXCEPT ![i] = "InTail"]
    /\ tailWaiter' = i
    /\ hasTail' = TRUE
    /\ UNCHANGED <<publish_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

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
  \* The executor's Exchange observes its OWN prior publish (store-to-load forwarding:
  \* same-thread program order), so it reads the writer-local hasExecuting, NOT the lagged
  \* hasExecutingVisible shadow. The shadow models remote store-buffer lag and only gates
  \* the ADVANCER's observations. Gating this on the shadow let the executor spuriously
  \* "lose" against its own unpropagated write and overwrite a still-pending publish on the
  \* next yield - a hardware-impossible trace the activation-gated liveness exposed.
  /\ hasExecuting  \* Exchange reads own write; TRUE → wins
  /\ escPhase = "idle"    \* no escalation in progress
  /\ LET i == tailWaiter IN
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       \* Sync-success consumes the task inline (CommitTailWaiter's GetResult) - tenure
       \* releases with it. Executor-local and ordered, so no contract concern here.
       /\ tenure' = IF i \in taskDone /\ tenure = i THEN NoItem ELSE tenure
       /\ IF i \in taskDone
          THEN \* sync-success: CompleteWaiter inline, store untouched.
            /\ loc' = [loc EXCEPT ![i] = "Completed"]
            /\ activations' = activations
            /\ completedCount' = completedCount
            /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters, storeCount>>
          ELSE
            IF ~escalated /\ ~hasSlot
              THEN \* Slot path: zero-alloc, wire slot callback.
                /\ StoreSlotCommit(i)
                /\ loc' = [loc EXCEPT ![i] = "InSlot"]
                /\ activations' = IF storeCount = 0
                                  THEN [activations EXCEPT ![i] = @ + 1]
                                  ELSE activations
                /\ completedCount' = completedCount
              ELSE \* Post-escalation queue path (escalated guard inside the operator).
                   \* Loosened CAS: no slot manipulation (PostEscalationSlotEmpty).
                /\ StoreQueueEnqueue(i)
                /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
                /\ activations' = IF storeCount = 0
                                  THEN [activations EXCEPT ![i] = @ + 1]
                                  ELSE activations
                /\ completedCount' = completedCount
  /\ UNCHANGED <<adv_vars, taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenureClash, assertFailed>>

\* CommitTailWaiter, advancer won (alreadyActivated = true). Executor skips _executingItem clear.
ExecCommitTailExecutorLoses ==
  /\ hasTail
  \* Own-thread observation (see ExecCommitTailExecutorWins): FALSE here means either the
  \* item was never published (inline activation) or an advancer's Exchange consumed the
  \* publish - both genuinely alreadyActivated. The lagged shadow must not produce this.
  /\ ~hasExecuting
  /\ escPhase = "idle"
  /\ LET i == tailWaiter IN
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ tenure' = IF i \in taskDone /\ tenure = i THEN NoItem ELSE tenure
       /\ IF i \in taskDone
          THEN
            /\ loc' = [loc EXCEPT ![i] = "Completed"]
            /\ activations' = activations
            /\ completedCount' = completedCount
            /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters, storeCount>>
          ELSE
            IF ~escalated /\ ~hasSlot
              THEN \* Slot path. activated=true: no B-path activate.
                /\ StoreSlotCommit(i)
                /\ loc' = [loc EXCEPT ![i] = "InSlot"]
                /\ activations' = activations
                /\ completedCount' = completedCount
              ELSE \* Post-escalation queue path (loosened CAS, escalated guard in the operator).
                /\ StoreQueueEnqueue(i)
                /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
                /\ activations' = activations
                /\ completedCount' = completedCount
  /\ UNCHANGED <<publish_vars, adv_vars, taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenureClash, assertFailed>>

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
  /\ hasExecuting  \* own-thread observation, see ExecCommitTailExecutorWins
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
       /\ StoreEscalatePublish(i)
       /\ loc' = [loc EXCEPT ![i] = "InEscalation"]
  /\ UNCHANGED <<adv_vars, completedCount,
                 taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 1: PublishQueue, executor-loses variant.
ExecCommitTailPublishQueueExecutorLoses ==
  /\ hasTail
  /\ ~hasExecuting  \* own-thread observation, see ExecCommitTailExecutorLoses
  /\ escPhase = "idle"
  /\ ~escalated
  /\ hasSlot
  /\ LET i == tailWaiter IN
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ StoreEscalatePublish(i)
       /\ loc' = [loc EXCEPT ![i] = "InEscalation"]
  /\ UNCHANGED <<publish_vars, adv_vars, completedCount,
                 taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 2: CASSlot. Atomic Interlocked.Exchange(_hasSlot, 0). The race outcome with the
\* slot callback's pre-publish CAS is captured by reading hasSlot here: if the callback
\* drained the slot before the executor reached publish, hasSlot is FALSE and escSlotClaimed
\* records the loss; otherwise the executor wins and the slot fields are still populated.
ExecEscalationCASSlot ==
  /\ StoreEscalateClaimSlot
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, completedCount,
                 loc, taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 3: MoveSlot. If the CAS won, append the slot item to waiters head and clear the slot
\* fields. The slot item's loc transitions from InSlot to InWaiters (its callback was wired
\* at slot-commit time and stays wired through the move).
ExecEscalationMoveSlot ==
  /\ StoreEscalateMove
  /\ loc' = IF escSlotClaimed THEN [loc EXCEPT ![slotItem] = "InWaiters"] ELSE loc
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, completedCount,
                 taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 4: EnqueueNew. Append escTail to waiters tail; increment count; loc → InWaitersPending.
\* Activations follow the same rule as the original atomic transition (storeCount = 0 was
\* the wasEmpty trigger). With the chain fix, a first escalation that moved the slot
\* (slotWasMoved) proceeds to the compensation check (CommitWaiter's post-escalation
\* `slotWasMoved && Exchange(_pendingHeadActivation, false)`); otherwise back to "idle".
ExecEscalationEnqueueNew ==
  /\ LET i == escTail IN
       /\ StoreEscalateEnqueue(IF SlotChainActivation /\ escSlotClaimed THEN "compensate" ELSE "idle")
       /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
       /\ activations' = IF storeCount = 0
                         THEN [activations EXCEPT ![i] = @ + 1]
                         ELSE activations
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, completedCount,
                 taskDone, callbackFired, failed, drainer_vars>>

\* The escalating commit's compensation check, FIX only (CommitWaiter, right where the
\* slotWasMoved nudge already lives): consume the handoff flag if set and activate the moved
\* head - the escalator holds its own copy of the moved (item, task) pair, so no queue
\* consumer op is needed (the executor is the SPSC producer and may not peek). The taskDone
\* skip mirrors the code's IsCompleted guard; a completed moved head is drained by the
\* existing slotWasMoved nudge immediately after this check.
ExecEscalationCompensate ==
  /\ StoreTakeMoved
  /\ LET target == escMoved IN  \* the code's local moved-pair copy, NOT the current head
       IF pendingHeadActivation
         THEN
           /\ pendingHeadActivation' = FALSE
           /\ activations' = IF target \in taskDone
                             THEN activations
                             ELSE [activations EXCEPT ![target] = @ + 1]
         ELSE
           /\ pendingHeadActivation' = pendingHeadActivation
           /\ activations' = activations
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, completedCount,
                 loc, taskDone, callbackFired, failed, drainer_vars,
                 drainPhase, drainItem, drainRemaining, tenure, tenureClash, assertFailed>>

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
                   taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

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
                   taskDone, activations, storeCount, esc_vars, failed>>

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
                   taskDone, activations, storeCount, esc_vars, drainer_vars, failed>>

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
                   storeCount, loc, taskDone, activations, esc_vars, drainer_vars, failed>>

\* Slot drain, step 1 of 3: callback bump + TryAcquire + CAS-claim slot. Previously fused
\* with the decrement and CompleteWaiter into one atomic SlotCallbackDrains - an atomicity
\* the real DrainSlotInline does NOT have. The fusion hid two real interleavings (observed
\* June 2026, Slon.Tests):
\*   (a) a successor commit CAS-ing into the freed slot between this claim and the count
\*       decrement - the code's Debug.Assert(count == 0) is FALSE under it (exit-134 aborts
\*       on shipped code; see assertFailed / DrainCountAssertHolds);
\*   (b) the consume-vs-republish ordering for per-item resources (see tenure).
\* completedCount net change across the three steps is -1 here + the callback's earlier +1
\* having happened in the same real method; modeled as the fused bump+decrement = net zero,
\* preserved by doing nothing to completedCount in this action (matches the old atomic
\* action's accounting).
\*
\* Consume timing: ConsumeBeforeRepublish = TRUE folds the consume (tenure release) into
\* the claim step - the proposed contract, release happens-before the count republish.
\* FALSE (shipped code) defers the release to SlotDrainComplete, opening the clash window.
SlotDrainClaim ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InSlot"
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ hasSlot
    /\ slotItem = i
    /\ ~escalated
    /\ drainPhase = "idle"
    /\ callbackFired' = callbackFired \cup {i}
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    /\ drainerActive' = TRUE
    /\ StoreClaimSlotForDrain
    /\ loc' = [loc EXCEPT ![i] = "Draining"]
    /\ drainPhase' = "claimed"
    /\ drainItem' = i
    /\ tenure' = IF ConsumeBeforeRepublish /\ tenure = i THEN NoItem ELSE tenure
    /\ UNCHANGED <<publish_vars, tail_vars, escalated, waiters, counters,
                   taskDone, activations, esc_vars, failed, drainRemaining, pendingHeadActivation,
                   tenureClash, assertFailed>>

\* Slot drain, step 2 of 3: DecrementCount - the position republish. The executor's Count==0
\* inline-activation gate (SourceYieldInline's storeCount = 0) can pass the instant this
\* lands. The shipped code asserts the post-decrement count is 0; a commit that landed in
\* the freed slot during the claimed window makes it 1 - assertFailed records the firing.
SlotDrainCount ==
  /\ drainPhase = "claimed"
  /\ StoreDecrementCount
  /\ assertFailed' = IF storeCount - 1 # 0 THEN TRUE ELSE assertFailed
  \* The atomic decrement's return value, captured for the SlotDrainComplete* branch choice
  \* (the queue drain's `var count = _waiters.DecrementCount()` partition; shipped
  \* DrainSlotInline discards it - see SlotChainActivation).
  /\ drainRemaining' = storeCount - 1
  /\ drainPhase' = "counted"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters, completedCount,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, pendingHeadActivation, tenure, tenureClash>>

\* Slot drain, step 3 of 3, SHIPPED code (~SlotChainActivation): CompleteWaiter + the
\* lock-guarded C-path. The lock block consumes the publish whenever held; it re-checks the
\* count under the lock and SKIPS activation when Count > 0 (the real code's
\* `else _executingItem = default` clear-without-activate branch). In queue mode that skip is
\* sound: every head-dequeue re-runs the D-path, so the cleared publish's item is activated
\* when its position comes up. Slot mode has NO recurring D-path - the skip is a permanent
\* loss, and it pairs with the commit side's wasEmpty=false skip (the successor that
\* committed during the claim window saw the claimed-but-uncounted position) to form the
\* double-skip: both sides defer, the activation decision evaporates. Field signature:
\* suite hang, flow with _pendingActivationControl = null. EventuallyActivated FAILS here.
SlotDrainCompleteLegacy ==
  /\ ~SlotChainActivation
  /\ drainPhase = "counted"
  /\ LET i == drainItem IN
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF ~ConsumeBeforeRepublish /\ tenure = i THEN NoItem ELSE tenure
       /\ IF hasExecutingVisible /\ executingItemVisible # NoItem
            THEN
              /\ activations' = IF storeCount = 0
                                THEN [activations EXCEPT ![executingItemVisible] = @ + 1]
                                ELSE activations
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
            ELSE
              /\ activations' = activations
              /\ UNCHANGED publish_vars
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, tenureClash, assertFailed>>

\* Slot drain, step 3 of 3, FIX, chain arm, slot-visible case (drainRemaining > 0,
\* ~escalated): a successor's commit landed before our decrement, so it observed count >= 2
\* (the claimed-but-uncounted position inflates it), took wasEmpty = false, and skipped
\* self-activation. The counter partition makes the drainer its designated activator - the
\* slot-mode D-path. The publish handshake is left UNTOUCHED: the published item (if any)
\* is behind the new head and gets activated by the chain (the new head's own drain) or the
\* C-path at count 0.
\*
\* Peek safety (TryPeekSlotForActivation, not separately modeled): the commit writes slot
\* fields BEFORE its count increment, and we observed that increment via our atomic
\* decrement, so the fields are fully published to us; the peek's post-barrier IsEscalated
\* check detects a racing escalation's claim/clear (queue published before the claim) and
\* routes to the handoff action below.
SlotDrainCompleteChainSlot ==
  /\ SlotChainActivation
  /\ drainPhase = "counted"
  /\ drainRemaining > 0
  /\ ~escalated
  /\ slotItem # NoItem
  /\ LET i == drainItem
         target == slotItem
     IN
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF ~ConsumeBeforeRepublish /\ tenure = i THEN NoItem ELSE tenure
       \* The code's IsCompleted skip (mirrors CommitWaiter's guard): a waiter whose task
       \* already settled - possible without activation via CompleteTaskUnactivated - is
       \* never activated; its callback fired and bailed against our held latch, and the
       \* post-release reclaim (SlotDrainerReclaim) drains it.
       /\ activations' = IF target \in taskDone
                         THEN activations
                         ELSE [activations EXCEPT ![target] = @ + 1]
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, tenureClash, assertFailed>>

\* Chain arm, escalation-raced case: the peek saw IsEscalated - a first escalation is (or
\* finished) relocating the head slot -> queue, so the head cannot be named from the slot
\* fields. Publish the activation obligation (Interlocked.Exchange(_pendingHeadActivation,
\* true), a full fence) and move to the one-shot re-peek (handoff resolution below). The
\* drained item's completion bookkeeping happens here; no activation yet.
SlotDrainCompleteChainHandoff ==
  /\ SlotChainActivation
  /\ drainPhase = "counted"
  /\ drainRemaining > 0
  /\ escalated
  /\ LET i == drainItem IN
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF ~ConsumeBeforeRepublish /\ tenure = i THEN NoItem ELSE tenure
  /\ pendingHeadActivation' = TRUE
  /\ drainPhase' = "handoff"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 tenureClash, assertFailed>>

\* Handoff resolution, drainer reclaims: the one-shot re-peek found the head visible (the
\* escalation's move landed), so the drainer claims its own obligation back (Exchange wins)
\* and activates. Exactly-once with the escalator's compensation check is the flag claim.
SlotDrainHandoffReclaim ==
  /\ drainPhase = "handoff"
  /\ pendingHeadActivation
  /\ Len(waiters) > 0
  /\ LET target == Head(waiters) IN
       activations' = IF target \in taskDone
                      THEN activations
                      ELSE [activations EXCEPT ![target] = @ + 1]
  /\ pendingHeadActivation' = FALSE
  /\ drainPhase' = "idle"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, tenure, tenureClash, assertFailed>>

\* Handoff resolution, drainer trusts the escalator: the re-peek found no visible head (the
\* move hasn't landed). The flag's full fence makes this safe: had the escalator's
\* compensation check already run, its enqueue would be ordered before our peek in the
\* flag's total order and the head would be visible - so an invisible head means the check
\* is still ahead and will consume the obligation. Also covers the flag having been consumed
\* mid-dance (the escalator's check won the Exchange between our publish and our re-peek).
SlotDrainHandoffTrust ==
  /\ drainPhase = "handoff"
  /\ \/ ~pendingHeadActivation
     \/ Len(waiters) = 0
  /\ drainPhase' = "idle"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure, tenureClash,
                 assertFailed>>

\* Slot drain, step 3 of 3, FIX, C-path arm (drainRemaining = 0): no successor preceded our
\* decrement, so any LATER commit observes count 1 = wasEmpty and self-activates - the
\* drainer only handles the deferred-published item. Restructured lock block: the count is
\* re-checked FIRST and the publish is consumed ONLY at Count = 0. When the count grew since
\* our decrement, the successor that raised it self-activated and its own drain continues
\* the chain; consuming the publish here (the legacy clear branch) would orphan the
\* published item in slot mode. The Count-then-Exchange TOCTOU is benign: a commit's
\* Exchange precedes its count increment, so whichever Exchange wins owns the activation
\* decision exactly once - this atomic action models that linearization.
SlotDrainCompleteCPath ==
  /\ SlotChainActivation
  /\ drainPhase = "counted"
  /\ drainRemaining = 0
  /\ LET i == drainItem IN
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF ~ConsumeBeforeRepublish /\ tenure = i THEN NoItem ELSE tenure
       /\ IF storeCount = 0 /\ hasExecutingVisible /\ executingItemVisible # NoItem
            THEN
              /\ activations' = [activations EXCEPT ![executingItemVisible] = @ + 1]
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
            ELSE
              /\ activations' = activations
              /\ UNCHANGED publish_vars
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, tenureClash, assertFailed>>

\* Drainer's release step. Mirrors DrainSlotInline's _advancing.Release() tail. Between
\* SlotCallbackDrains and this release, the slot is empty but the advancer is still held.
\* A follow-up commit can land a new item in the slot, and SlotCallbackBailsOut fires if
\* that item's task is done. The race window the failing test exercises.
SlotDrainerRelease ==
  /\ advancing
  /\ drainerActive
  /\ ~escalated
  /\ drainPhase = "idle"  \* the drain's three steps complete before the release
  /\ Len(waiters) = 0
  /\ IF WeakAdvancerRelease
       THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
       ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
  \* drainerActive stays TRUE: the slot drainer's method continues past the release into
  \* its reclaim check (mirroring the queue drain's do-while). DrainerChainExit clears it
  \* when no reclaim precondition holds.
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The slot-mode reclaim (the strand fix under design): post-release, the drainer re-checks
\* for a waiter whose callback bailed against its hold (counted in completedCount, callback
\* one-shot spent) and re-acquires to drain it. Fused TryAcquire + TryClaimSlotForDrain,
\* entering the standard three-step drain at "claimed". Mirrors TryReclaimAdvancerForWork.
SlotDrainerReclaim ==
  /\ SlotReclaimEnabled
  /\ drainerActive
  /\ ~advancingVisible
  /\ completedCount > 0
  /\ ~escalated
  /\ drainPhase = "idle"
  /\ \E i \in Item :
       /\ hasSlot
       /\ slotItem = i
       /\ i \in taskDone
       /\ i \in callbackFired  \* the bailed waiter: callback spent, undrained
       /\ advancing' = TRUE
       /\ advancingVisible' = TRUE
       /\ completedCount' = completedCount - 1
       /\ StoreClaimSlotForDrain
       /\ loc' = [loc EXCEPT ![i] = "Draining"]
       /\ drainPhase' = "claimed"
       /\ drainItem' = i
       /\ tenure' = IF ConsumeBeforeRepublish /\ tenure = i THEN NoItem ELSE tenure
  /\ UNCHANGED <<publish_vars, tail_vars, escalated, waiters, storeCount,
                 taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainRemaining, pendingHeadActivation, tenureClash, assertFailed>>

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
\* June 2026 correction: this bail is NOT blocked by the empty-signal fix. The fix only
\* closed the WaitForEmptyAsync-driven commit window; the EXECUTOR's pipelining commits a
\* successor slot waiter during the drainer's hold regardless (and the consume-before-
\* republish hoist widened the hold). Field-reproduced as the lost-activation timeout.
\* The recovery is SlotDrainerReclaim (gated on SlotReclaimEnabled), not this gate.
SlotCallbackBailsOut ==
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
                      storeCount, loc, taskDone, activations, esc_vars, drainer_vars, failed>>

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
                   loc, taskDone, activations, storeCount, esc_vars, failed>>

CallbackBailsOut ==
  \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InWaiters"
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ completedCount' = completedCount + 1
    /\ callbackFired' = callbackFired \cup {i}
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                   loc, taskDone, activations, storeCount, esc_vars, drainer_vars, failed>>

(* ===========================================================================
   Advancer actions
   =========================================================================== *)

\* Advancer drains the head of _waiters if its task is completed.
\* Models DrainReadyWaiters inner loop: TryPeek + TryDequeue + Decrement + CompleteWaiter.
AdvancerDrainHead ==
  /\ advancing
  /\ drainPhase = "idle"  \* the latch holder is one thread in one method: not mid-slot-drain
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  /\ LET i == Head(waiters) IN
       /\ StoreDequeueHead
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ completedCount' = completedCount - 1
       \* The queue drain consumes at dequeue, before its DecrementCount - already the
       \* contract ordering, no toggle. Tenure (if this head ever held it: a slot-tier item
       \* moved to queue head by escalation) releases with the consume.
       /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
       /\ \* D-path activate the next head, if any.
          activations' = IF Len(waiters) > 1
                         THEN [activations EXCEPT ![Head(Tail(waiters))] = @ + 1]
                         ELSE activations
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenureClash, assertFailed>>

\* Advancer at count=0, executes C-path activation under lock.
AdvancerCPath ==
  /\ advancing
  /\ drainPhase = "idle"  \* see AdvancerDrainHead
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
                 loc, taskDone, callbackFired, failed, esc_vars, drainer_vars>>

\* Advancer release. Toggle picks Volatile.Write vs Exchange.
\* Aligned with DrainReadyWaiters' release: fires when queue is empty or head not done.
\* No slot clause: SlotCallbackDrains is atomic (acquire-drain-C-path-release), so the
\* advancer is never observed held between transitions in any state where hasSlot = TRUE.
AdvancerRelease ==
  /\ advancing
  /\ drainPhase = "idle"  \* see AdvancerDrainHead; SlotDrainerRelease covers the slot drain
  /\ \/ Len(waiters) = 0
     \/ /\ Len(waiters) > 0
        /\ Head(waiters) \notin taskDone
  /\ IF WeakAdvancerRelease
       THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
       ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

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
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The drainer chain exits when, post-release, the do-while's TryReclaim preconditions don't
\* match (queue empty or head not done). Clears drainerActive. Real code's do-while exits
\* and the calling method returns; subsequent stranded counts wait for a new callback entry.
DrainerChainExit ==
  /\ drainerActive
  /\ ~advancing
  /\ ~(completedCount > 0 /\ Len(waiters) > 0 /\ Head(waiters) \in taskDone)
  \* Slot-mode reclaim precondition must also not hold (the slot drainer's do-while keeps
  \* going while a bailed slot waiter is reclaimable).
  /\ ~(SlotReclaimEnabled /\ completedCount > 0 /\ ~escalated /\ hasSlot
       /\ slotItem \in taskDone /\ slotItem \in callbackFired)
  /\ drainerActive' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

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
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

\* Visibility-propagation transitions for the *Visible shadow fields. Only fire when the
\* corresponding relaxed toggle is on; otherwise local and visible are always equal.
PropagateAdvancing ==
  /\ advancing # advancingVisible
  /\ PropagateOk(advancing, advancingVisible')
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, advancing, callbackFired, failed, esc_vars, drainer_vars>>

PropagateHasExecuting ==
  /\ hasExecuting # hasExecutingVisible
  /\ PropagateOk(hasExecuting, hasExecutingVisible')
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, executingItem, executingItemVisible, hasExecuting, callbackFired, failed, esc_vars, drainer_vars>>

PropagateExecutingItem ==
  /\ executingItem # executingItemVisible
  /\ PropagateOk(executingItem, executingItemVisible')
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, executingItem, hasExecuting, hasExecutingVisible, callbackFired, failed, esc_vars, drainer_vars>>

(* ===========================================================================
   Recovery: ExecuteItemAsync threw, RecoverItem installs a substitute.

   First-cut model:
     - ExecItemFailure mirrors the executor's catch + ClearExecutingItem(activated) at
       Pipeline.cs:257. The item transitions from Executing to Recovering and the
       publish handshake is rolled back to match the post-Clear state.
     - RecoverItem_Wins / RecoverItem_Loses mirror the fixed RecoverItem. The
       activation gate is `_waiters.Count is 0` (storeCount = 0 in the model). The
       wins path inline-activates the substitute (activations[i] += 1); the loses path
       publishes via _hasExecutingItem for the advancer C-path. From here the
       substitute proceeds through the normal Executing → InTail / InSlot / ...
       transitions, so no other actions need bespoke recovery awareness.
     - The substitute reuses the failed item's identity. Activations on the substitute
       accumulate on the same counter; NoTripleActivation (<=2) still holds for a
       single recovery cycle (Failed item's activation cleared on transition into
       Recovering would shrink the model further but isn't needed for the bug class).
     - Not yet modelled: trailing-task recovery (RecoverTrailingFailure), recovery
       chained on recovery, the policy refusing recovery (TryRecoverItemFailure
       returns false), and concurrent failure during the slot/escalation phases. The
       focus is the bug we hit: ExecuteItemTask failure interleaved with a previous
       waiter's in-flight pipeline task.
   =========================================================================== *)

\* Executor's ExecuteItemAsync threw. Mirrors Pipeline.cs:255-258 - catch + ClearExecutingItem.
\* If the failed item was inline-activated (storeCount was 0 at SourceYieldInline time), no
\* publish state to roll back. If deferred-published (SourceYieldDeferred set _executingItem
\* and _hasExecutingItem), Clear's Interlocked.Exchange on _hasExecutingItem races with the
\* advancer's C-path acquire; we model the "Exchange won, executor clears" branch (the
\* "advancer won, lock-sync" branch would be a separate transition if the C-path race needs
\* to be visible to liveness, but the activation-gating bug is reachable via the simpler path).
ExecItemFailure ==
  \E i \in Item :
    /\ loc[i] = "Executing"
    /\ i \notin failed  \* bounded: each item fails at most once (see VARIABLES comment)
    /\ failed' = failed \cup {i}
    /\ loc' = [loc EXCEPT ![i] = "Recovering"]
    \* A sync throw out of ExecuteAuto releases any tenure the dispatch acquired (the
    \* builder's sync-faulted path round-trips TryGetVoidTask, clearing _started).
    /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
    /\ IF hasExecuting /\ executingItem = i  \* own-thread observation of the executor's publish
         THEN \* Deferred-published failed item: roll back the publish.
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
         ELSE UNCHANGED publish_vars
    /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, esc_vars, drainer_vars,
                   drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenureClash, assertFailed>>

\* RecoverItem wins-path: no prior waiter in flight (_waiters.Count is 0), inline-activate
\* the substitute. Mirrors the post-fix code at the new RecoverItem in Pipeline.cs. The
\* substitute reuses the failed item's identity; its activation count goes up by one.
RecoverItemWins ==
  \E i \in Item :
    /\ loc[i] = "Recovering"
    \* BUG-VERIFICATION TEMP: storeCount = 0 guard removed to model pre-fix code.
    \* /\ storeCount = 0
    /\ ~hasTail
    /\ escPhase = "idle"
    /\ loc' = [loc EXCEPT ![i] = "Executing"]
    /\ activations' = [activations EXCEPT ![i] = @ + 1]
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, callbackFired, failed, esc_vars, drainer_vars>>

\* RecoverItem loses-path: a prior waiter is still in flight, defer activation. The
\* substitute publishes via _executingItem / _hasExecutingItem so the advancer's C-path
\* picks it up when the prior pipeline task completes. Mirrors SourceYieldDeferred's
\* publish handshake. Pre-fix code's ActivateHeadItem on this path would overwrite the
\* prior reader's binding and is the bug NoSimultaneousActiveReader catches.
RecoverItemLoses ==
  \E i \in Item :
    /\ loc[i] = "Recovering"
    /\ storeCount > 0
    /\ ~hasTail
    /\ escPhase = "idle"
    /\ loc' = [loc EXCEPT ![i] = "Executing"]
    /\ IF IsReferenceT
         THEN FencedWriteOk(executingItem', executingItemVisible', i)
         ELSE WeakWriteOk(executingItem', executingItemVisible', i, executingItemVisible)
    /\ IF WeakHasExecutingPublish
         THEN WeakWriteOk(hasExecuting', hasExecutingVisible', TRUE, hasExecutingVisible)
         ELSE FencedWriteOk(hasExecuting', hasExecutingVisible', TRUE)
    /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

(* ===========================================================================
   Next-state relation
   =========================================================================== *)

\* Actions predating the June 2026 aux variables (drain split + tenure) and not interacting
\* with them are wrapped with UNCHANGED aux_vars via the W-suffixed forms below, used by
\* BOTH Next and the fairness conjuncts (liveness evaluates fairness actions independently
\* of Next, so the wrap must live in the named action, not at the relation). Actions that DO
\* interact (yield/dispatch, commits' sync-success consume, the drain steps, queue drain
\* head, item failure) handle aux explicitly in their bodies.
CompleteTaskW(i) == CompleteTask(i) /\ UNCHANGED aux_vars
CompleteTaskUnactivatedW(i) == CompleteTaskUnactivated(i) /\ UNCHANGED aux_vars
SourceYieldDeferredW == SourceYieldDeferred /\ UNCHANGED aux_vars
ExecSetTailW == ExecSetTail /\ UNCHANGED aux_vars
RecoverItemWinsW == RecoverItemWins /\ UNCHANGED aux_vars
RecoverItemLosesW == RecoverItemLoses /\ UNCHANGED aux_vars
ExecCommitTailPublishQueueExecutorWinsW == ExecCommitTailPublishQueueExecutorWins /\ UNCHANGED aux_vars
ExecCommitTailPublishQueueExecutorLosesW == ExecCommitTailPublishQueueExecutorLoses /\ UNCHANGED aux_vars
ExecEscalationCASSlotW == ExecEscalationCASSlot /\ UNCHANGED aux_vars
ExecEscalationMoveSlotW == ExecEscalationMoveSlot /\ UNCHANGED aux_vars
ExecEscalationEnqueueNewW == ExecEscalationEnqueueNew /\ UNCHANGED aux_vars
ExecutorRegistersCallbackW == ExecutorRegistersCallback /\ UNCHANGED aux_vars
ExecutorInlineCallbackBecomesAdvancerW == ExecutorInlineCallbackBecomesAdvancer /\ UNCHANGED aux_vars
ExecutorInlineCallbackBailsOutW == ExecutorInlineCallbackBailsOut /\ UNCHANGED aux_vars
SlotDrainerReleaseW == SlotDrainerRelease /\ UNCHANGED aux_vars
SlotCallbackBailsOutW == SlotCallbackBailsOut /\ UNCHANGED aux_vars
SlotCallbackBailsDuringEscalationW == SlotCallbackBailsDuringEscalation /\ UNCHANGED aux_vars
CallbackBecomesAdvancerW == CallbackBecomesAdvancer /\ UNCHANGED aux_vars
CallbackBailsOutW == CallbackBailsOut /\ UNCHANGED aux_vars
AdvancerCPathW == AdvancerCPath /\ UNCHANGED aux_vars
AdvancerReleaseW == AdvancerRelease /\ UNCHANGED aux_vars
AdvancerReclaimW == AdvancerReclaim /\ UNCHANGED aux_vars
DrainerChainExitW == DrainerChainExit /\ UNCHANGED aux_vars
ExecPostEscalationNudgeW == ExecPostEscalationNudge /\ UNCHANGED aux_vars
PropagateAdvancingW == PropagateAdvancing /\ UNCHANGED aux_vars
PropagateHasExecutingW == PropagateHasExecuting /\ UNCHANGED aux_vars
PropagateExecutingItemW == PropagateExecutingItem /\ UNCHANGED aux_vars

Next ==
  \/ \E i \in Item : CompleteTaskW(i)
  \/ \E i \in Item : CompleteTaskUnactivatedW(i)  \* possible, never obligated - no WF below
  \/ SourceYieldInline
  \/ SourceYieldInlineClash
  \/ SourceYieldDeferredW
  \/ ExecSetTailW
  \/ ExecItemFailure
  \/ RecoverItemWinsW
  \/ RecoverItemLosesW
  \/ ExecCommitTailExecutorWins
  \/ ExecCommitTailExecutorLoses
  \/ ExecCommitTailPublishQueueExecutorWinsW
  \/ ExecCommitTailPublishQueueExecutorLosesW
  \/ ExecEscalationCASSlotW
  \/ ExecEscalationMoveSlotW
  \/ ExecEscalationEnqueueNewW
  \/ ExecutorRegistersCallbackW
  \/ ExecutorInlineCallbackBecomesAdvancerW
  \/ ExecutorInlineCallbackBailsOutW
  \/ SlotDrainClaim
  \/ SlotDrainCount
  \/ SlotDrainCompleteLegacy
  \/ SlotDrainCompleteChainSlot
  \/ SlotDrainCompleteChainHandoff
  \/ SlotDrainHandoffReclaim
  \/ SlotDrainHandoffTrust
  \/ SlotDrainCompleteCPath
  \/ ExecEscalationCompensate
  \/ SlotDrainerReclaim
  \/ SlotDrainerReleaseW
  \/ SlotCallbackBailsOutW
  \/ SlotCallbackBailsDuringEscalationW
  \/ CallbackBecomesAdvancerW
  \/ CallbackBailsOutW
  \/ AdvancerDrainHead
  \/ AdvancerCPathW
  \/ AdvancerReleaseW
  \/ AdvancerReclaimW
  \/ DrainerChainExitW
  \/ ExecPostEscalationNudgeW
  \/ PropagateAdvancingW
  \/ PropagateHasExecutingW
  \/ PropagateExecutingItemW

Spec == Init /\ [][Next]_vars
        \* Fairness: assume the executor and advancer chain make progress.
        \* W-suffixed forms keep aux_vars constrained; liveness evaluates these actions
        \* independently of Next.
        /\ WF_vars(SourceYieldInline \/ SourceYieldDeferredW)
        /\ WF_vars(ExecSetTailW)
        \* Recovery is reactive to failure (no WF on ExecItemFailure - failure is a
        \* possibility, not an obligation). Once Recovering, the substitute must eventually
        \* be installed via one of the two paths; WF on the disjunction so liveness still
        \* holds when the gate's storeCount value toggles between branches.
        /\ WF_vars(RecoverItemWinsW \/ RecoverItemLosesW)
        /\ WF_vars(ExecCommitTailExecutorWins \/ ExecCommitTailExecutorLoses)
        /\ WF_vars(ExecCommitTailPublishQueueExecutorWinsW \/ ExecCommitTailPublishQueueExecutorLosesW)
        /\ WF_vars(ExecEscalationCASSlotW)
        /\ WF_vars(ExecEscalationMoveSlotW)
        /\ WF_vars(ExecEscalationEnqueueNewW)
        /\ WF_vars(SlotCallbackBailsDuringEscalationW)
        /\ WF_vars(AdvancerDrainHead)
        /\ WF_vars(AdvancerCPathW)
        /\ WF_vars(AdvancerReleaseW)
        /\ WF_vars(AdvancerReclaimW)
        /\ WF_vars(DrainerChainExitW)
        \* Nudge fairness: enabled iff NudgeEnabled constant is TRUE. If FALSE, the action
        \* never fires (its NudgeEnabled precondition is FALSE), so fairness over it is vacuous;
        \* we still include WF for symmetry. The interesting test is whether liveness holds.
        /\ WF_vars(ExecPostEscalationNudgeW)
        /\ WF_vars(SlotDrainClaim)
        /\ WF_vars(SlotDrainCount)
        \* One WF over the disjunction: the complete variants are mutually exclusive
        \* (constant gate + drainRemaining partition + escalated branch) - the disjunction
        \* keeps fairness anchored on "the drain's step 3 eventually runs" rather than
        \* per-variant. Same for the handoff resolution pair (the one-shot re-peek).
        /\ WF_vars(SlotDrainCompleteLegacy \/ SlotDrainCompleteChainSlot
                   \/ SlotDrainCompleteChainHandoff \/ SlotDrainCompleteCPath)
        /\ WF_vars(SlotDrainHandoffReclaim \/ SlotDrainHandoffTrust)
        /\ WF_vars(ExecEscalationCompensate)
        /\ WF_vars(SlotDrainerReclaim)
        /\ WF_vars(SlotDrainerReleaseW)
        /\ WF_vars(SlotCallbackBailsOutW)
        /\ WF_vars(CallbackBecomesAdvancerW)
        /\ WF_vars(CallbackBailsOutW)
        /\ WF_vars(ExecutorRegistersCallbackW)
        /\ WF_vars(ExecutorInlineCallbackBecomesAdvancerW)
        /\ WF_vars(ExecutorInlineCallbackBailsOutW)
        \* Hardware: pending writes do propagate eventually (cache coherence).
        /\ WF_vars(PropagateAdvancingW)
        /\ WF_vars(PropagateHasExecutingW)
        /\ WF_vars(PropagateExecutingItemW)
        \* External: assume every ACTIVATED task eventually completes. Deliberately NO
        \* fairness over CompleteTaskUnactivatedW - unactivated completion (cancellation,
        \* no-read-I/O flows) is a possibility liveness must tolerate, never an escape
        \* hatch it may rely on.
        /\ \A i \in Item : WF_vars(CompleteTaskW(i))

(* ===========================================================================
   Invariants
   =========================================================================== *)

\* Every item is in exactly one logical location.
TypeOK ==
  /\ StoreTypeOK
  /\ loc \in [Item -> Locations]
  /\ activations \in [Item -> 0..5]  \* bounded for TLC; recovery cycle adds up to 2 more
  /\ completedCount \in -NumItems..NumItems
  /\ drainerActive \in BOOLEAN
  /\ failed \subseteq Item
  /\ drainPhase \in {"idle", "claimed", "counted", "handoff"}
  /\ drainItem \in Item \cup {NoItem}
  /\ drainRemaining \in 0..NumItems
  /\ pendingHeadActivation \in BOOLEAN
  /\ tenure \in Item \cup {NoItem}
  /\ tenureClash \in BOOLEAN
  /\ assertFailed \in BOOLEAN

\* WaiterStore count = (slot contribution) + (queue length). Use slotItem # NoItem rather than
\* hasSlot for the slot contribution: during the cas_done phase the executor has CAS'd hasSlot
\* to FALSE but hasn't yet cleared the slot fields (MoveSlot does that), so the slot item is
\* still counted in storeCount until MoveSlot transfers it to waiters. The new tail
\* (escTail) contributes only after EnqueueNew (Step 4) lands it in waiters.
\* The drainPhase = "claimed" term is the June 2026 correction: TryClaimSlotForDrain empties
\* the slot BEFORE DecrementCount lands, so a claimed-but-not-yet-counted drain contributes 1
\* to storeCount with no slot/queue residency. The code-level echo of the uncorrected
\* invariant was DrainSlotInline's Debug.Assert(count == 0), which a successor commit into
\* the freed slot falsifies (see DrainCountAssertHolds).
CountConsistency ==
  storeCount = (IF slotItem # NoItem THEN 1 ELSE 0) + Len(waiters)
               + (IF drainPhase = "claimed" THEN 1 ELSE 0)

\* Drain-phase bookkeeping coherence. Note mid-drain commits are LEGAL: a successor can
\* reuse the freed slot and even trigger first escalation while the drainer is between its
\* claim and its decrement - the only ownership constraint is the advancer latch itself.
DrainConsistent ==
  /\ (drainPhase \in {"claimed", "counted"}) <=> (drainItem # NoItem)
  /\ drainItem # NoItem => loc[drainItem] = "Draining"
  /\ \A i \in Item : (loc[i] = "Draining") => (drainItem = i)
  /\ drainPhase # "idle" => (advancing /\ drainerActive)
  /\ drainPhase # "counted" => drainRemaining = 0
  \* The handoff flag only exists in escalated runs (the drainer publishes it exactly when
  \* its chain obligation met an escalation).
  /\ pendingHeadActivation => escalated

\* No item activated more than twice in a normal pipeline pass (1 expected, 2 caught the
\* double-activation bug). Recovery adds another full activation cycle for the substitute
\* (which can also race with the advancer); the bound is bumped by 2 for items that have
\* been through ExecItemFailure.
NoTripleActivation ==
  \A i \in Item : activations[i] <= 2 + (IF i \in failed THEN 1 ELSE 0)

\* Items in waiters are exactly those with loc in {InWaitersPending, InWaiters}.
WaitersConsistent ==
  /\ \A i \in Item : (loc[i] \in {"InWaitersPending", "InWaiters"}) <=> (\E k \in 1..Len(waiters) : waiters[k] = i)

\* Escalation phase invariants. escTail is exactly the item in loc = "InEscalation"
\* (if any), and escPhase tracks which step we're at.
EscalationConsistent ==
  /\ StoreEscalationConsistent
  /\ (escTail # NoItem) => (loc[escTail] = "InEscalation")
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

\* PostEscalationSlotEmpty (the loosened-CAS justification) lives in WaiterStore.tla.

\* Pipeline contract: at most one item is "inline-activated" without going through the
\* deferred-publish + advancer C-path. Inline-activated means SourceYieldInline /
\* RecoverItemWins fired and the item hasn't yet completed. The bug we fixed had
\* RecoverItem always inline-activating, so when a substitute fired while a prior waiter's
\* pipeline task was in flight, two items overlapped as inline-activated readers.
\*
\* "Inline-activated" in the model = "Executing" + ~hasExecutingVisible (the deferred-
\* publish handshake is OFF, so activation happened inline). The prior waiter in flight
\* lives in InTail / InSlot / InWaiters and has its own activation; if RecoverItemWins
\* fires while waiters exist, two items can be simultaneously "active readers" at the
\* protocol layer.
NoSimultaneousActiveReader ==
  LET inlineActive == { i \in Item :
        loc[i] = "Executing" /\ ~hasExecutingVisible /\ ~hasExecuting }
  IN  Cardinality(inlineActive) <= 1

\* Combined safety invariants. Single name keeps the .cfg simple and makes adding a new
\* invariant a one-file change rather than a two-file change.
Invariants ==
  /\ TypeOK
  /\ CountConsistency
  /\ DrainConsistent
  /\ NoTripleActivation
  /\ WaitersConsistent
  /\ SlotConsistent
  /\ PostEscalationSlotEmpty
  /\ EscalationConsistent
  /\ NoSimultaneousActiveReader

\* ============================================================================
\* Bug witnesses (checked separately from Invariants so violations are attributable;
\* both are EXPECTED to be violated in the configurations noted)
\* ============================================================================

\* The shipped code's Debug.Assert(count == 0) in DrainSlotInline. EXPECTED FALSE under any
\* configuration: a successor commit can CAS into the freed slot between the drain's claim
\* and its decrement (observed as exit-134 aborts in Slon.Tests on shipped code, ~2/50 runs
\* once protocol-side timing shifted; latent since the slot tier landed). The fix is to
\* delete the assertion - the store's _hasSlot CAS is the ownership contract, not the count.
DrainCountAssertHolds ==
  ~assertFailed

\* The consume-before-republish contract. EXPECTED FALSE with ConsumeBeforeRepublish = FALSE
\* (shipped ordering: consume after DecrementCount; Slon's ThrowAlreadyStarted at
\* DispatchPipelinedRead, ~7/50 suite runs). MUST hold with ConsumeBeforeRepublish = TRUE -
\* that is the design target this spec evolution exists to validate.
NoTenureClash ==
  ~tenureClash

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
    (loc[i] \in {"Executing", "InTail", "InSlot", "InEscalation", "InWaitersPending", "InWaiters", "Draining"} /\ i \in taskDone)
      ~> (loc[i] = "Completed")

\* Every yielded item is eventually activated (or has already left the pipeline). The property
\* EventuallyCompleted cannot see lost activations: with completion gated on activation (see
\* CompleteTask), a never-activated item is never taskDone, so EventuallyCompleted's premise
\* never holds and the hang is vacuously "live". This property targets the activation decision
\* itself - the lost-activation double-skip (June 2026, dumps reclaim_hang_72057+) shows up
\* only here. The Completed disjunct future-proofs against completion paths that legitimately
\* bypass activation (none modeled today; aborts/cancellation will be).
\*
\* Expected to FAIL with SlotChainActivation = FALSE (shipped DrainSlotInline, see
\* Pipeline_LostActivationWitness.cfg) and HOLD with TRUE (Pipeline_Contract.cfg).
EventuallyActivated ==
  \A i \in Item :
    (loc[i] # "Nowhere") ~> (activations[i] > 0 \/ loc[i] = "Completed")

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
      state but covers a real bug class. First-cut landed: ExecItemFailure +
      RecoverItemWins / RecoverItemLoses cover the ExecuteItemTask failure
      path with the post-fix activation gating. Still to add:
        - RecoverTrailingFailure (trailing-task-failed on an already-committed
          waiter, plus the _activationLock dance the real code does).
        - RecoverCommittedTailWaiterAsync (CommitTailWaiter observing a
          pre-faulted pipelineTask).
        - Policy-refuses-recovery branch (TryRecoverItemFailure returns false).
        - Recovery on top of recovery (substitute's own failure).
        - Mid-escalation failure (Executing throw while escPhase != idle).

   6. Escalation-vs-slot-callback CAS race - both EscalateAndEnqueue and the
      slot callback do Exchange(_hasSlot, 0). Currently the spec models the
      executor's path explicitly (CAS branch in ExecCommitTail_) and the
      callback's via SlotCallbackDrains; the race is captured by interleaving
      but the "executor lost the CAS to a concurrent slot callback that
      already completed the item" branch could be made more explicit.

   7. Slot field writes vs the _hasSlot flag (June 2026 find, unconfirmed in
      field): SlotDrainClaim fuses Exchange+field-reads+clears and the slot
      commit fuses CAS+field-writes+increment into atomic actions, hiding the
      instruction-scale window where TryClaimSlotForDrain's reads/clears race
      a successor commit's writes inside the just-freed slot (torn (item,task)
      pair, or commit fields wiped under _hasSlot=1). Split the field accesses
      from the flag ops to expose it, then design the fix (read-before-claim
      with post-Exchange validation, or a version stamp).

   8. The queue lock-block's clear-at-Count>0 branch (DrainReadyWaiters) is
      not modeled (AdvancerCPath only fires at storeCount=0). It is believed
      sound in queue mode (the recurring D-path re-activates the cleared
      publish's item when its position comes up) but that belief is exactly
      the kind that slot mode falsified - model it and let EventuallyActivated
      adjudicate.
*)

=============================================================================
