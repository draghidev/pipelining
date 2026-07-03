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
     - ActivatedAtMostOnce: activations[i] <= 1 (non-failed) / <= 2 (failed, identity-reused
       substitute). Tightened June 2026 from the stale double-activation-hunt slack.
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
  SlotChainActivation,       \* The slot-mode D-path under design (June 2026, lost-activation
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
  SplitCountCommit,          \* FIDELITY toggle (June 2026, count-skew round). The code's queue
                             \* commit is TWO steps: `queue.Enqueue(entry); Interlocked.Increment
                             \* (ref _count)` (TryEscalateOrEnqueue, both the steady-state path
                             \* and the first escalation's tail). The model historically FUSED
                             \* them (StoreQueueEnqueue / StoreEscalateEnqueue), hiding the window
                             \* where the entry is visible and consumable while the count excludes
                             \* it. Field-proven consequence (dump crash_22130.dmp + Debug assert,
                             \* DeferredActivationUnderSustainedLoad_Stress): a drain consumes the
                             \* uncounted entry (its task completed at commit time - e.g. the
                             \* tail's CompletePipelineTask racing its own commit), DecrementCount
                             \* returns -1, and the C/D partition built for {0, >0} misroutes:
                             \* loud face = D-arm peeks an empty queue and activates default(T)
                             \* (NRE, exit 134, NoNullActivation); quiet face = C-path skipped at
                             \* -1 AND the late increment returns 0 so the committer's wasEmpty
                             \* (count == 1) is FALSE - both sides skip activation, the deferred
                             \* publish's item strands (the 2s field timeout). TRUE models the
                             \* real split (VERIFIED June 2026: StoreCountNonNegative and
                             \* NoNullActivation violated, 15-state trace - see
                             \* Pipeline_CountSkewWitness.cfg; EventuallyActivated does NOT
                             \* counterexample because AdvancerCPath is modeled as a standing
                             \* fair action while the code's C-path check runs once per drain
                             \* pass - the quiet face needs that granularity modeled, fix-round
                             \* backlog). FALSE keeps the legacy fused fiction so the
                             \* pre-existing green configs stay meaningful.
  SkewTolerantPartition,     \* The count-skew FIX (June 2026, designed against
                             \* SplitCountCommit = TRUE). The drain's C/D activation partition
                             \* treats the decremented count's `<= 0` as the C-path case instead
                             \* of `== 0`. Soundness rests on BoundedCountSkew (single producer
                             \* => skew bound 1 => decrement bottoms at -1) plus the implication
                             \* that a negative count means the in-flight entry was already
                             \* completed-and-consumed (the drain only consumes completed heads),
                             \* so the producer's existing wasEmpty/IsCompleted skips are correct
                             \* and the only live responsibility is the deferred publish - which
                             \* the C-path claims. The producer side needs NO change. Under the
                             \* fix, count > 0 implies a peekable head (under-promise direction),
                             \* so the D-arm's checked peek becomes a canary (NoNullActivation
                             \* must HOLD). FALSE models shipped code (the witness). TRUE is the
                             \* design target (Pipeline_Contract.cfg, with SplitCountCommit on).
  PassOnceDrain,             \* FIDELITY toggle (June 2026, bail/recheck strand round). The code's
                             \* DrainReadyWaiters is a PASS with a program counter:
                             \*   do { clear signal; while (head ready) drain; Release;
                             \*   } while (signal-read && TryReclaim);
                             \* and the post-release signal recheck runs ONCE per pass - a missed
                             \* rendezvous exits the chain forever. The legacy model composes the
                             \* drain from STANDING fair actions (AdvancerRelease / AdvancerReclaim
                             \* / DrainerChainExit guarded on the live state), which lets fairness
                             \* re-evaluate the exit decision - structurally hiding pass-once lost
                             \* wakes (field dump park_hang.dmp: drainSignal TRUE, latch free,
                             \* completed entry in queue, consumer cache never refreshed; the 2s
                             \* tail-completed strand at ~1/40k stress iterations). TRUE replaces
                             \* the standing trio with explicit pass steps (qDrainPhase) so TLC
                             \* explores the recheck race the hand-proofs keep failing to close.
                             \* FALSE keeps the legacy composition for the pre-existing configs.
  SignalConservation,        \* The bail/recheck strand FIX (June 2026, designed against
                             \* PassOnceDrain = TRUE). Round-3 counterexample (recheck_strand_
                             \* trace3.txt): a pass consumes the signal, truthfully finds nothing
                             \* peekable (the signaled work is still MATERIALIZING - the
                             \* escalation's slot->queue move, or the split commit's enqueue/
                             \* increment window), and exits through the once-only recheck -
                             \* destroying the token every later rescue (the nudge, the reclaim)
                             \* keys on. The rule: signals are CONSERVED - a pass that consumed
                             \* one and drained nothing restores it at its release point. The
                             \* restored token survives the pass's own exit (the recheck/reclaim
                             \* reads don't consume) and the materializer's tail check finds it:
                             \* the escalation's one-shot nudge, or the split commit's post-
                             \* increment inline callback. TRUE must flip the round-3 witness
                             \* green; FALSE models shipped code (expect the strand).
  CountGatedConservation,    \* Refinement of SignalConservation (June 2026, post-livelock): the
                             \* empty-handed restore fires only when storeCount > 0 - evidence of
                             \* committed work (the materializing mid-move/mid-commit cases all
                             \* carry a count). Without the gate, restores manufacture DANGLING
                             \* tokens (flag TRUE, zero work, nothing scheduled to clear them),
                             \* which both feed probe traffic into the reclaim's transient hold
                             \* (the field strand's fuel) and livelock any unconditional reclaim
                             \* retry (the iter-14 regression). With it, a post-release flag is
                             \* always real: a bail or a materializer.
  PendingWordLatch           \* THE transient-hold repair, FINAL (June 2026, v5 - after FOUR
                             \* TLC-refuted two-cell protocols: naive retry livelocked, bounded
                             \* retry left the recursion, serve-attempt and the uniform T-cycle
                             \* each leaked at their own last release). The root flaw was never
                             \* the protocol: latch and signal in SEPARATE CELLS make every
                             \* release/recheck a two-location Dekker rendezvous, and a bail can
                             \* always land between any pair of reads. v5 moves the bail's
                             \* deposit INTO THE LATCH WORD - Drepper's mutex tri-state
                             \* (free / held / held+pending) WITHOUT the kernel half: pending
                             \* does not mean parked threads to wake, it means deposited work
                             \* the RELEASER inherits (obligation transfer, no waiting anywhere).
                             \* TryAcquire's failure atomically ORs pending (same word, cannot
                             \* be missed); Release exchanges the whole word and READS pending
                             \* atomically with releasing - the window class is unrepresentable,
                             \* not handled. A releaser seeing pending re-acquires and continues
                             \* the pass (losing the re-acquire is benign: the winner's
                             \* exhaustive pass serves all visible work). drainSignal keeps ONLY
                             \* the materializing-token role (count-gated conservation, served
                             \* by the materializer's own tail checks). TRUE = the fix; FALSE =
                             \* shipped two-cell code (the strand witness).
  ,
  SplitSlotFieldOps          \* Fidelity toggle (June 2026, backlog #7): the slot tier's field
                             \* ops as the code's real instruction sequences. Commit is
                             \* CAS-then-fields-then-increment (the license bit precedes the
                             \* data - the inverse of the SPSC fields-then-index discipline);
                             \* claim is Exchange-then-read-then-clear. The fused operators hid
                             \* the windows: a claim Exchange winning between a successor
                             \* commit's CAS and its field writes reads the PREVIOUS tenure's
                             \* cleared fields (default claim - the NRE face), or reads the
                             \* successor's half-landed pair (torn claim), or its clear WIPES
                             \* the successor's written fields under _hasSlot = 1 (the lost-
                             \* item face). Torn reads per se are expected (SPSC tears the
                             \* same way) - the property under test is that every claim
                             \* returning TRUE is LICENSED: ordered after the fields of the
                             \* pair it returns. The slot callback path is licensed by
                             \* construction (fields -> wiring -> fire -> claim); the suspect
                             \* is the reclaim path, whose stale-token gate orders nothing.
                             \* NoDefaultSlotClaim is the witness invariant.
  ,
  TriStateSlotClaim          \* THE backlog #7 fix (June 2026, Nino's call - the slot gets the
                             \* same medicine as the latch: a third word state). _hasSlot
                             \* becomes 0 empty / 1 occupied / 2 consuming, with the SPSC
                             \* data-then-license discipline on the commit side:
                             \*   commit: fields first (flag-0 fields are executor-exclusive:
                             \*     claims never write fields under this protocol), then
                             \*     publish 1 (plain volatile - the slot CAS is DELETED, a
                             \*     perf refund; flag 1 PROVES fields complete);
                             \*   claim: peek the task under stable flag 1 (a 1->0->1 cycle
                             \*     is impossible: the only droppers are the latch-serialized
                             \*     claimer and the escalation, which drops permanently) -
                             \*     LIVE task = bail with NO state change (no claim, no
                             \*     un-claim, no GetResult block); completed = CAS 1->2,
                             \*     consume (read + clear + release 2->0) under exclusive
                             \*     ownership - no ghosts, no torn reads;
                             \*   escalation: move-claim becomes CAS(1->0), quiescent-only -
                             \*     a consuming 2 reads as not-moved (the occupant completes
                             \*     under the latch and exits, FIFO preserved). The Exchange
                             \*     it replaces could zero state 2 and strand the occupant.
                             \* State 2 is modeled as hasSlot /\ drainPhase = "cl_own" (the
                             \* word's third state rides the drainer's PC; no new variable).
                             \* Meaningful only under SplitSlotFieldOps. TRUE = the fix
                             \* (Contract); FALSE = shipped two-phase ops
                             \* (Pipeline_SlotTearWitness.cfg pins the tear).
  ,
  ModelCPathClear            \* Fidelity toggle (June 2026, backlog #8): the queue C-path lock
                             \* block's clear-at-Count>0 branch (DrainReadyWaiters ~1243). When
                             \* the advancer WINS the deferred-publish Exchange but re-reads
                             \* Count>0 under the lock (a successor committed since its decrement),
                             \* the code CLEARS _executingItem WITHOUT activating, on the belief
                             \* that the D-path activates the item as a queue head. That is the
                             \* slot double-skip's shape: winning the Exchange means the executor's
                             \* own commit saw alreadyActivated and did NOT self-activate, so the
                             \* clear leaves NEITHER side activating - sound ONLY if the D-path
                             \* always covers it. AdvancerCPath models only the Len=0 activate
                             \* arm; this toggle adds the Count>0 clear arm and lets
                             \* EventuallyActivated adjudicate. FALSE = the 9 verified configs
                             \* (the publish persists until the queue drains, then AdvancerCPath
                             \* claims it - behaviorally unaffected). TRUE = Pipeline_CPathClear-
                             \* Witness.cfg, the experiment.
  ,
  CPathLeaveFix              \* The backlog #8 FIX (June 2026, confirmed bug). At the C-path lock
                             \* with Count>0 (a successor raced in since the <=0 decrement), the
                             \* shipped code CLEARS the won publish without activating - and the
                             \* tightened witness proved that strands the published item (the
                             \* executor's commit saw alreadyActivated and deferred; neither side
                             \* activates; the item hangs when the successor drains before it
                             \* re-queues with a predecessor). The fix LEAVES the publish at
                             \* Count>0 (does not Exchange it away) - the executor's own
                             \* CommitTailWaiter Exchange then reclaims it and activates (wasEmpty
                             \* self-activate, or a later C-path/D-path once it queues behind the
                             \* successor). Code: check Count BEFORE the Exchange, only consume +
                             \* activate at Count<=0. Mirrors slot mode's lost-activation fix (the
                             \* clear-at-Count>0 was the bug there too). Meaningful only under
                             \* ModelCPathClear. TRUE = Pipeline_CPathFixWitness.cfg (expect
                             \* green); FALSE = Pipeline_CPathClearWitness.cfg (pins the bug).
  ,
  SplitCallbackOps          \* Fidelity toggle (June 2026, backlog #3 race granularity): the
                             \* queue callback's `_drainSignal = true` plain store as a step
                             \* SEPARATE from its `TryAcquireOrFlagPending`. The fused
                             \* CallbackBecomesAdvancer/BailsOut collapse set + acquire + the
                             \* pass-top clear into one atomic action, so the model can show a
                             \* drainSignal value the real interleaving never has (one callback's
                             \* set wiped by another's pass-top clear, the wake carried only by
                             \* the deposit). This un-fuses the set so two concurrent callbacks'
                             \* sets, and a concurrent advancer's clear, interleave against the
                             \* transient (callbackSignaled tracks "set, not yet acquired"). The
                             \* manual enumeration says every set is deposit-paired or the
                             \* materializing-token (both verified) - this converts that argument
                             \* to a checked fact. Extended (June 2026 audit round) to the SLOT
                             \* callback: CallbackSetSignal's InSlot arm + the claim entries'
                             \* signaled precondition + CallbackAcquireLose (the slot bail's
                             \* deposit window) + SlotCallbackSpent (occupant reclaimed between
                             \* set and acquire). Same round: CallbackAcquireLose had been
                             \* unsatisfiable since written (callbackSignaled double-bound in
                             \* its UNCHANGED), so the deposit-on-lose interleaving had never
                             \* actually been explored - now live for both arms.
                             \* FALSE = the 9 configs (fused, unaffected); TRUE
                             \* = Pipeline_SplitCallbackWitness.cfg (expect green, or a surprise).
  ,
  CompleteBeforeCount        \* DrainSlotInline's Complete-vs-Decrement ordering (June 2026,
                             \* activated-slot race round). The fault path's RecoverWaiter has
                             \* always run BEFORE DecrementCount (Pipeline.cs:1132-1138 audit
                             \* reorder); the non-fault path historically ran DecrementCount
                             \* BEFORE CompleteWaiterDeferred and only re-aligned with the
                             \* June 2026 ReorderFix. Decrement-first opens a window in which
                             \* a concurrent ActivateHeadItem (executor's deferred-publish
                             \* branch, a successor commit's wasEmpty inline-activate, ...)
                             \* writes activatedSlot = NEW; the subsequent unconditional
                             \* clear in CompleteWaiterDeferred then stomps NEW = NoItem, and
                             \* NEW's body's first decoder read sees NoItem and NREs
                             \* (Slon's PgDecoder.CurrentExecutionControl, Stress_SequentialReads_
                             \* NoRecovery ~1/17 Pg-suite runs). TRUE models the shipped fix
                             \* (Complete fires while count is still > 0, so no concurrent
                             \* activator can fire - the clear lands on an empty window).
                             \* FALSE reproduces the witness: NoStompedActivation must HOLD
                             \* with TRUE, must FAIL with FALSE.
  ,
  SplitCPathExchange         \* The read-turn round (July 2026). TRUE splits the queue C-path's
                             \* lock block into its two real instructions: the Count<=0 read
                             \* (arm) and the pending Exchange + activate (fire), with the
                             \* executor free to interleave between them. The window is real:
                             \* the advancer's Count read can go STALE while it is preempted
                             \* inside the lock - the executor commits the next waiter
                             \* (count 0->1, wasEmpty self-activates it, pre-fix lock-free)
                             \* AND dispatches+publishes its successor; the woken advancer's
                             \* Exchange then consumes that FRESH publish, activating a second
                             \* flow while the wasEmpty one is live. Two flows on the
                             \* single-tenant read baton = Slon's "async method is already
                             \* executing" (ConcurrentSyncAndAsync_RecordingReads, ~1/35 soak
                             \* loops, July 2026). FALSE = fused (the verified configs,
                             \* unaffected). ReadTurnMutex must FAIL with TRUE (witness of the
                             \* production bug) until the single-reader gate is modeled.
  ,
  SingleActivationGate       \* The gate round (July 2026). TRUE models the SHIPPED _liveActivation
                             \* occupancy bit (Pipeline.cs ~2003): set TRUE in ActivateHeadItem -
                             \* EVERY activation path - and cleared FALSE in CompleteWaiterDeferred
                             \* at every retirement (just before _policy.CompleteItem). It GATES
                             \* exactly TWO activation sites: (1) CommitWaiter's wasEmpty
                             \* self-activate (activate the sole committed waiter only when
                             \* !_liveActivation; else leave it committed-unactivated on the claim
                             \* "the advancer activates it in FIFO order when the live reader
                             \* retires"), and (2) the advancer's queue C-path (`_waiters.Count is
                             \* 0 && !_liveActivation && Volatile.Read(pending)`; on a gate-skip
                             \* the pending flag is LEFT in place on the claim "this C-path
                             \* re-fires in the same advancer pass"). BOTH coverage claims are
                             \* UNVERIFIED - whether a gate-skip can strand an activation
                             \* (EventuallyActivated) is what this toggle exists to adjudicate
                             \* (the live suspect for a rare field strand). The slot C-path
                             \* (SlotDrainCompleteCPath), the D-path arms, the slot-chain arms,
                             \* dispatch-inline, and the recovery activations are NOT gated - the
                             \* code doesn't gate them. FALSE = ungated (pre-fix shape); the
                             \* liveActivation variable is threaded but inert, so every prior
                             \* config is behaviorally unaffected.
  ,
  PeekGatedCPathLicense      \* The double-act FIX, load-bearing half (July 2026, gate round -
                             \* Pipeline_GateDoubleActWitness.cfg's chain). TRUE = the queue
                             \* C-path license requires the queue to be empty BY RESIDENCY, not
                             \* merely count 0: closes the enqueue-before-increment under-promise
                             \* window (SplitCountCommit) that lets the C-path fire past a
                             \* resident completed-unactivated head - the out-of-FIFO fire that
                             \* seeds the same-flow double activation. Queue C-path only (the
                             \* fused AdvancerCPath, where the atomic guard already reads
                             \* residency, and the split AdvancerCPathFire, where the residency
                             \* peek joins the fire's fresh under-lock reads). A declined license
                             \* LEAVES the pending publish for the re-fire, exactly like the
                             \* gate skip (AdvancerCPathFirePeekDecline exits the lock phase).
                             \* At-most-once is delivered by STRUCTURE (single license + FIFO),
                             \* never by IsCompleted checks - this conjunct carries the whole
                             \* correctness weight of the fix. FALSE = shipped count-0 license
                             \* (every prior config unaffected).
  ,
  QueueDPathCompletedGuard   \* The double-act fix, dispatch-saver half (July 2026). TRUE = the
                             \* queue D-arm (AdvancerDrainHead / DrainHeadRecovers activating the
                             \* peeked next head after a retirement) SKIPS activation when the
                             \* head's task is already completed, mirroring the slot D-path's
                             \* guard (SlotDrainCompleteChainSlot's IsCompleted skip and
                             \* SlotDrainHandoffReclaim's taskDone arm); the drain loop's own
                             \* next iteration dequeues the completed head. Completed-at-commit
                             \* items are drain-only by design (their tasks can complete long
                             \* before activation reaches them, e.g. timeouts) - but this check
                             \* is a check-then-activate TOCTOU against a monotonic-but-
                             \* asynchronous completion, so it must NOT be load-bearing for
                             \* at-most-once (Pipeline_GateDoubleActFixNoGuard.cfg adjudicates
                             \* that). FALSE = shipped unconditional D-arm activate (every prior
                             \* config unaffected).
  ,
  RecoverySplit             \* The recovery round (June 2026, backlog #2/#5). FALSE = legacy
                             \* identity-reuse: the substitute reuses the failed item's identity
                             \* (RecoverItemWins/Loses re-dispatch loc[i]), so a failed item
                             \* activates twice and the binding (recovery completes failed) is
                             \* invisible (one item completing itself). TRUE = distinct identity:
                             \* the failed item parks in "Recovering" carrying a completion
                             \* obligation; a fresh substitute j (recoveryOf[j]=i) flows the
                             \* normal lifecycle; BindingDischarge completes the failed item when
                             \* the substitute completes - so the binding is checkable, the
                             \* activation bound is uniform <=1, and recovery-on-recovery /
                             \* policy-refuse / the trailing-fault injection points get modeled.
                             \* FALSE = the 9 configs (unaffected); TRUE = the recovery witness.
  ,
  LockFreeIdleDispatch       \* The lock-elision round (July 2026). TRUE = the ESCALATED dispatch
                             \* (Pipeline.cs ~390-421: once _activationLock exists, every dispatch
                             \* serializes its "Count is 0 -> activate the head inline" decision
                             \* under the lock, publishing _executingItemActivationPending then
                             \* claiming it back at Count 0) gains an ELISION arm: when its guard
                             \*   storeCount = 0 (the clamped public Count read; modeled <= 0,
                             \*     the -1 face reads 0 through the clamp)
                             \*   /\ ~latchHeld  (the advancer latch word - advancingVisible, the
                             \*     remote read; acquires are interlocked so a FALSE read is never
                             \*     stale-free, only a TRUE read can be stale-held = conservative)
                             \*   /\ ~pending    (the executor's own _executingItemActivationPending
                             \*     read - hasExecuting, own-thread store-to-load forwarding)
                             \* holds - all read at elision time - it activates the head inline
                             \* WITHOUT the lock and without publishing pending, exactly like the
                             \* never-escalated fork (~376-388). When the guard fails it falls to
                             \* the existing locked arm unchanged (SourceYieldInline /
                             \* SourceYieldDeferred stay enabled as-is). Rationale under
                             \* adjudication: the C-path and all drain-time activation decisions
                             \* run with the latch held end to end, so "latch free" should mean no
                             \* concurrent activation decision exists; sequential-shape dispatches
                             \* would skip ~1 of their ~3 per-item lock acquires. The DANGER is
                             \* the check-then-act window (SplitIdleDispatchGuard below).
                             \* FALSE = shipped locked dispatch (every prior config unaffected).
  ,
  SplitIdleDispatchGuard     \* FIDELITY toggle, meaningful only under LockFreeIdleDispatch.
                             \* FALSE = FUSED: the elision guard's three reads and the activation
                             \* are one atomic action (SourceYieldElide) - adjudicates the pure
                             \* LOGIC (is the idle observation sound when reads are truth?).
                             \* TRUE = SPLIT: the guard reads are one action (SourceYieldElideArm,
                             \* parking the picked item in the DispatchArmed location) and the
                             \* activation a following action (SourceYieldElideAct, NO re-reads),
                             \* with every other thread free to interleave between them - the
                             \* latch can be acquired, counts can change. This is the shape
                             \* SplitCPathExchange used for the C-path lock block, and it found
                             \* the production bug there; whether the elision guard is stable as
                             \* a check-then-act is exactly what this level adjudicates
                             \* (Pipeline_IdleDispatchSplit.cfg).
  ,
  CPathPendingPreCheck       \* The C-path lock-elision round (July 2026). TRUE = the three
                             \* C-path claim sites (the queue drain's count-0 C-path, the slot
                             \* drain's C-path, and the recovery rejoin's copy - the last folds
                             \* into the slot C-path in this model) READ the deferred-publish
                             \* pending flag (hasExecutingVisible, a Volatile.Read) OUTSIDE the
                             \* _activationLock and SKIP the lock phase entirely when it reads
                             \* FALSE. The lock body is UNCHANGED when entered (all existing
                             \* under-lock checks - the count re-read, the Exchange, the gate -
                             \* run as before). Rationale under adjudication: the pending flag
                             \* is usually FALSE after a drain empties the store, so the
                             \* unconditional lock acquire is pure overhead; a stale-FALSE
                             \* pre-check that skips a publish landing just after the read is
                             \* claimed harmless because the publisher (the executor's dispatch
                             \* or commit) always returns to claim its own publish - its locked
                             \* arm re-reads Count and self-activates at count 0, or leaves it
                             \* for the CommitTailWaiter reclaim at count > 0. The DANGER is the
                             \* stale-read window (SplitCPathPreCheck below): whether a
                             \* pre-check that captured FALSE just before a publisher set pending
                             \* can strand that activation. FALSE = shipped unconditional lock
                             \* entry (every prior config unaffected). Modeled only in the
                             \* ModelCPathClear regime (the C-path is its own "cpath_lock" phase);
                             \* the pre-check gates entry to that phase.
  ,
  SplitCPathPreCheck         \* FIDELITY toggle, meaningful only under CPathPendingPreCheck.
                             \* FALSE = FUSED: the pre-check read + the skip/enter decision are
                             \* one atomic action against the TRUE pending value (no stale
                             \* window). Adjudicates the pure logic; a FUSED skip only fires when
                             \* pending is genuinely FALSE at that instant, so it coincides with
                             \* the existing model's lock-exit-on-no-publish (green baseline).
                             \* TRUE = SPLIT: the pre-check read is its OWN action (capturing the
                             \* pending value into the drain's PC), every other thread free to
                             \* interleave, then the skip/enter acts on the CAPTURED value with
                             \* NO re-read - so a stale FALSE (pending set by a concurrent
                             \* publisher between the read and the skip) still skips the lock,
                             \* LEAVING the fresh publish for the publisher's own rescue. This is
                             \* the shape SplitCPathExchange / SplitIdleDispatchGuard used, and it
                             \* found the production bug in the first; whether the pre-check is
                             \* stable as a check-then-act is exactly what this level adjudicates
                             \* (Pipeline_CPathPreCheckSplit.cfg). EventuallyActivated is THE
                             \* verdict: a stranded activation here refutes the elision.
  ,
  SplitCommitSelfActivate    \* FIDELITY toggle (July 2026, the commit self-activate round).
                             \* CommitWaiter's wasEmpty self-activate is, in the code, TWO
                             \* instructions with a lock boundary between them:
                             \*   wasEmpty = _waiters.Enqueue(item);       // increment; returns wasEmpty
                             \*   if (wasEmpty && !waiterTask.IsCompleted) // OUTSIDE _activationLock
                             \*     lock (_activationLock)
                             \*       if (!_liveActivation) ActivateHeadItem(item);  // gate + act
                             \* FALSE = FUSED: the shipped modeling (ExecCommitQueueCountWins /
                             \* ExecCommitSlotCountWins) reads wasEmpty (pre-increment count 0),
                             \* the IsCompleted check (i \notin taskDone) and the _liveActivation
                             \* gate all ATOMICALLY at the count step - every prior config is
                             \* behaviorally unaffected. TRUE = SPLIT: the increment + the
                             \* wasEmpty/IsCompleted capture are ONE action (ExecCommit*CountArm,
                             \* parking the commit in the "*_selfact_fire"/"*_selfact_done" PC on
                             \* escPhase, verdict CAPTURED, escTail preserved as the item handle),
                             \* and the lock/gate/activate a FOLLOWING action (ExecCommit*CountAct*)
                             \* acting on the CAPTURED IsCompleted verdict with NO re-read (the
                             \* gate IS re-read fresh - it is under the lock in the code). Every
                             \* OTHER thread interleaves between them: in particular the item's
                             \* task completes (CompleteTask*), its callback acquires the advancer
                             \* latch, the drain dequeues the item (its enqueue preceded the check)
                             \* and RETIRES it - CompleteWaiterDeferred's liveActivation clear is a
                             \* plain drain-side write, NOT under _activationLock - and THEN the
                             \* commit's activation lands on the already-retired item. The suspect:
                             \* an activation with no future retirement, liveActivation stuck TRUE.
                             \* The queue arm mirrors ExecCommitQueueCountWins (no activatedSlot
                             \* write); the slot arm mirrors ExecCommitSlotCountWins (writes
                             \* activatedSlot, runs ReadTurnMonitor) - each SPLIT arm's effects are
                             \* effect-identical to its FUSED twin, only time-separated, so any
                             \* violation is attributable to the TOCTOU window alone.
                             \* FALSE = every prior config unaffected; TRUE =
                             \* Pipeline_CommitSelfActWitness.cfg (the adjudication).
  ,
  CommitSelfActRecheck       \* Candidate FIX, meaningful only under SplitCommitSelfActivate.
                             \* FALSE = shipped: the activate step acts on the CAPTURED IsCompleted
                             \* verdict, no re-read (the code has no re-check inside the lock).
                             \* TRUE = the activate step RE-READS taskDone at act time (an
                             \* IsCompleted re-read inside the lock, immediately before
                             \* ActivateHeadItem, skipping the activation when the task completed
                             \* meanwhile). This SHRINKS but need not CLOSE the window: retirement
                             \* still does not take the lock, and the task can complete AFTER the
                             \* re-check but before the activate - TLC adjudicates whether the
                             \* residual window is real at this granularity
                             \* (Pipeline_CommitSelfActFix.cfg). FALSE = every prior config
                             \* unaffected (the re-read only exists under the split arms).
  ,
  SplitCommitSelfActRecheck  \* Candidate FIX granularity toggle (July 2026, the residual round).
                             \* Requires SplitCommitSelfActivate /\ CommitSelfActRecheck. The fused
                             \* CommitSelfActRecheck models the in-lock IsCompleted re-read and the
                             \* ActivateHeadItem as ONE atomic action (ExecCommit*CountActFire) - it
                             \* came back GREEN, but the fusion HIDES a residual: in code the re-read
                             \* (Pipeline.cs:1134) and the activate (Pipeline.cs:1137) are SEPARATE
                             \* instructions inside the SAME lock hold (the lock is taken at
                             \* Pipeline.cs:1127), and the drain's retirement (CompleteWaiterDeferred,
                             \* Pipeline.cs:1087 - a lock-FREE Volatile.Write) can land BETWEEN them if
                             \* the committing thread is preempted mid-lock.
                             \* FALSE = the fused ActFire stands (re-read fused with activate).
                             \* TRUE = SPLIT the ActFire into RECHECK and FIRE:
                             \*   RECHECK (ExecCommit*Recheck): re-read taskDone under the lock, park
                             \*     the verdict onto a new escPhase ("q_selfact_dofire"/"slot_selfact
                             \*     _dofire" when the item is not done, straight to idle when it is).
                             \*   FIRE (ExecCommit*Fire): the activate, consuming the parked verdict;
                             \*     the gate (GateOpen) is re-read fresh. On a grant, park
                             \*     "*_selfact_verify" (under CommitSelfActVerify) or return to idle.
                             \* The drain's retirement CLEAR (liveActivation' = FALSE) and the task's
                             \* completion (CompleteTask*) take NO lock and stay FREE to interleave
                             \* between RECHECK and FIRE - that is the residual window.
                             \* ENCODING of the single lock hold: RECHECK, FIRE and VERIFY act from
                             \* escPhase \in {"q_selfact_fire","q_selfact_dofire","q_selfact_verify",
                             \* "slot_selfact_fire","slot_selfact_dofire","slot_selfact_verify"} - that
                             \* phase-window IS the _activationLock hold (SelfActLockHeld). Every OTHER
                             \* action that models an _activationLock-taking region (the commit gate for
                             \* other items, the C-path license family, any gated / drain / recovery
                             \* ActivateHeadItem) is forbidden from GRANTING while the window is open,
                             \* enforced UNIFORMLY by NoForeignGrant (no liveActivation FALSE -> TRUE
                             \* transition outside the self-act family during SelfActLockHeld) rather
                             \* than per-action guards. The lock-FREE retirement clears (liveActivation
                             \* FALSE -> FALSE / TRUE -> FALSE) and every non-granting action stay
                             \* enabled. FALSE = SelfActLockHeld is never TRUE, NoForeignGrant is
                             \* vacuous, every prior config bit-identical. TRUE =
                             \* Pipeline_CommitSelfActResidualWitness.cfg (expected liveness RED: the
                             \* completion + retirement land between RECHECK and FIRE, the stale "fire"
                             \* verdict activates the retired item, liveActivation stuck TRUE).
  ,
  CommitSelfActVerify        \* Candidate STRONGER FIX, requires SplitCommitSelfActRecheck.
                             \* FALSE = the residual stands (FIRE returns to idle right after the
                             \* activate, leaving no window to catch a grant on an item the drain
                             \* retired between RECHECK and FIRE).
                             \* TRUE = after FIRE a third action VERIFY (ExecCommit*Verify) runs INSIDE
                             \* the same lock hold, before the phase returns to idle (reached only when
                             \* FIRE actually GRANTED - "*_selfact_verify" is set only on doAct): it
                             \* re-reads taskDone; if the item is now done, release the just-planted
                             \* turn - liveActivation' = FALSE (a task-done item must not keep the
                             \* reader turn; its callback drains it). The slot arm's activatedSlot
                             \* RESTORE, however, fires ONLY when the item was actually RETIRED
                             \* (loc = "Completed") - that is the genuine stuck-grant case - and then
                             \* mirrors the drain's clear (activatedSlot' = DepthZeroClear when the slot
                             \* still points at the item). A task-done-but-RESIDENT item's grant is
                             \* transient-legit (the drain retires it the normal way); nulling
                             \* activatedSlot there would STOMP a live slot occupant (NoNullWhileLive).
                             \* Item not done -> passthrough. MODEL FINDING (this round): keying the
                             \* slot restore on mere completion over-fires; it must key on retirement.
                             \* RATIONALE: every re-grant path serializes on the lock, so inside the
                             \* hold only the drain's lock-free clear can interleave; a double-clear is
                             \* idempotent and a self-clear of a retired item's grant restores exactly
                             \* the drain-clear state. The SPURIOUS policy activation (activations[i]
                             \* bumped at FIRE for an already-completed item) is ACCEPTED - VERIFY does
                             \* NOT undo it. FALSE = every prior config unaffected. TRUE =
                             \* Pipeline_CommitSelfActVerifyFix.cfg (expected GREEN).
  ,
  CommitSelfActVerifyOnTaken \* PORTABILITY variant of the VERIFY trigger, meaningful only under
                             \* CommitSelfActVerify. The retirement-keyed trip (loc = "Completed")
                             \* has no clean committer-observable in code: the retirement's side
                             \* effects (depth-0 slot clear, liveActivation clear) are not
                             \* attributable to OUR item. What the committer CAN legally read under
                             \* the lock is "our item has left the store":
                             \*   slot arm  = the slot-state word + item identity (a Volatile
                             \*               snapshot is claim-safe from any thread) - modeled as
                             \*               slotItem # i;
                             \*   queue arm = the producer-side first==last emptiness read (the
                             \*               committer IS the SPSC producer; the drain dequeues
                             \*               before completing) - modeled as waiters = <<>>.
                             \* FALSE = the retirement-keyed VERIFY (the previous green shape).
                             \* TRUE = BOTH the liveActivation self-clear AND the slot arm's
                             \* activatedSlot restore key on "task-done AND taken" instead. This
                             \* trips EARLIER: it also fires in the taken-but-not-yet-completed
                             \* window (the drain has claimed/dequeued the item; its GetResult /
                             \* CompleteWaiterDeferred / clear are still pending). The hand argument
                             \* under adjudication: (1) a taken item is no longer resident, so the
                             \* NoNullWhileLive stomp face is gone; (2) the drain's own clear still
                             \* runs after ours - double clear idempotent, no re-grant inside our
                             \* hold; (3) grants serialize on the lock we hold. The suspect: the
                             \* drain's clear is NOT inside our hold - after we release, a successor
                             \* grant can land through the (now-open) gate BEFORE the drain's
                             \* pending clear, which then stomps the fresh grant. TLC adjudicates
                             \* (Pipeline_CommitSelfActVerifyOnTakenFix.cfg). FALSE = every prior
                             \* config unaffected.
                             \* VERDICT (July 2026): REFUTED - NoNullWhileLive, 33-state trace. Not
                             \* via the successor-grant suspect (that face is UNREACHABLE: the store
                             \* still COUNTS a claimed-but-not-yet-counted item, so every grant path
                             \* - wasEmpty, C-path - stays closed through the taken window; probe
                             \* OnTakenSuccessorGrantWindowUnreached HOLDS). The real face: hand
                             \* argument (1) is false - "taken" is NOT "no longer resident" for the
                             \* PIPELINE. In the claimed/dequeued-but-uncounted window the depth is
                             \* still >= 1, and the restore nulls the activated slot mid-tenure (the
                             \* _activatedItem NRE; the depth-0 clear discipline). The sound
                             \* committer-observable is done /\ taken /\ COUNT RE-READ = 0:
                             \* VerifyCountZeroImpliesRetired proves that conjunction implies
                             \* loc = "Completed" over this variant's full state space.
  ,
  SplitRecoveryInlineAct     \* FIDELITY toggle (July 2026, the recovery inline-activate round).
                             \* The execute-phase fault's ClearExecutingItem claim race and the
                             \* recovery twins' ungated inline-activate. In code the fault (the
                             \* throw) does NOT touch the deferred publish - the teardown belongs to
                             \* ClearExecutingItem (Pipeline.cs 2214-2250), a LATER step whose
                             \* in-lock claim races the advancer's C-path claim+activate; the LOST
                             \* arm (2242-2244) leaves _executingItem populated because the advancer
                             \* activated it - possibly AFTER the item already faulted (the fault is
                             \* not lock-visible). Recovery (RecoverItem ~672-687 and
                             \* RecoverCommittedTailWaiterAsync ~936-950) then republishes the
                             \* substitute and inline-activates it on Count==0 with NO lock and NO
                             \* _liveActivation gate - the only ungated activation sites in the file.
                             \* FALSE = the shipped modeling: ExecItemFailure / ExecCommitTailRecovers
                             \* FUSE the fault with the publish rollback (the claim race's
                             \* executor-wins branch only, as their comments admit), so the C-path
                             \* can never observe a faulted publish and the post-mortem grant is
                             \* UNREACHABLE. TRUE = the fault leaves the publish INTACT; the
                             \* executor's won-claim is its own following action (ExecFaultClearWon),
                             \* the advancer's lost arm needs no new action - AdvancerCPathFire
                             \* consuming the still-up publish of the now-Recovering item IS the
                             \* post-mortem grant (the code's claim+activate under the lock, blind
                             \* to the fault); and RecoverInstallWins/Loses + RecoverRefuse
                             \* ("Recovering" face) are gated on claim-resolved (the publish no
                             \* longer holds the parked item), matching the code's ordering -
                             \* TryRecoverItemFailure runs strictly after ClearExecutingItem
                             \* returned. A substitute's own fault (RecoverOnRecoveryFails) stays
                             \* fused - the chain is single-level by construction and the
                             \* second-level claim race is out of the Phase A bound. FALSE = every
                             \* prior config unaffected. TRUE =
                             \* Pipeline_RecoveryPostMortemWitness.cfg (the adjudication; control =
                             \* Pipeline_RecoveryCPathBaseline.cfg with FALSE).
  ,
  QueueTrailingFault         \* Injection gate for DrainHeadRecovers (the Phase C queue trailing
                             \* fault). TRUE = the injection as Phase C modeled it - which is now a
                             \* KNOWN WIRING INFIDELITY (July 2026, found by the recovery
                             \* inline-activate round's baseline control): DrainHeadRecovers parks
                             \* the faulted queue head at "Recovering" (the EXECUTOR-side,
                             \* enqueue-behind lifecycle: RecoverInstallWins/Loses commit the
                             \* substitute to the store tail) and runs the next-head activation
                             \* partition AT the fault. The code does neither: DrainReadyWaiters'
                             \* catch (Pipeline.cs ~1534) routes to RecoverWaiter (~1765), which
                             \* activates the substitute UNCONDITIONALLY IN PLACE (~1788) and
                             \* executes it on the advancer chain - the substitute never enqueues;
                             \* the DecrementCount AND the partition are DEFERRED past recovery
                             \* (the advance=false return; AdvanceAndDrainRecovery rejoins) - the
                             \* recovering item HOLDS its count credit. Under SingleActivationGate
                             \* the mis-wiring manufactures a FALSE DEADLOCK: the fault-parked
                             \* head's turn stays live, a committed waiter's wasEmpty self-activate
                             \* gate-skips against it, the enqueue-behind substitute lands BEHIND
                             \* that waiter, and the turn's clear waits on the substitute - a cycle
                             \* the code cannot form precisely BECAUSE the in-place activation
                             \* bypasses the gate (the substitution inherits the turn by design;
                             \* that ungated activate is load-bearing for gate liveness).
                             \* 33-state lasso: Pipeline_RecoveryCPathBaseline run of 2026-07-03
                             \* pre-gate. GREEN pre-gate rounds are unaffected by the infidelity
                             \* (nothing gates on the turn there), so TRUE preserves every prior
                             \* config bit-for-bit. FALSE = injection excluded (the recovery
                             \* inline-activate round's configs, which adjudicate the
                             \* EXECUTOR-side twins - ExecItemFailure and ExecCommitTailRecovers
                             \* remain live). The faithful repair - park "RecoveringInline",
                             \* dequeue WITHOUT decrement (a new store op + a CountConsistency
                             \* credit term), defer the partition to a queue rejoin action - is
                             \* QUEUED, not in this round.
  ,
  ConsumableTaskTokens       \* FIDELITY toggle (July 2026, the consumable-token round). The
                             \* spec's `taskDone` is a PERSISTENT, freely re-readable set - a
                             \* fidelity gap: the code's waiter task is a ValueTask over a pooled
                             \* IValueTaskSource, a SINGLE-CONSUMPTION token. It is readable
                             \* (IsCompleted / GetStatus / OnCompleted) only until someone CONSUMES
                             \* it (GetResult); consumption ends the token's lifetime and the
                             \* pooled promise box is re-rented by a later flow (version bump), so
                             \* any read from a STALE holder after consumption observes a recycled
                             \* source and the version guard THROWS (Slon MRVTSC.GetStatus
                             \* token-validation, the pump-fault convicted UPDATE 8, 2026-07-04).
                             \* TRUE turns on the token lifecycle Live -> Completed -> Consumed:
                             \* `tokenConsumed` marks an item at the drain's GetResult step (any
                             \* read after that is the error; Recycled is modeled identical to
                             \* Consumed), and every COMMIT-side POST-PUBLICATION task-state read
                             \* (ExecCommitQueue/SlotCountArm's out-of-lock IsCompleted =
                             \* Pipeline.cs:1136, the in-lock recheck = 1152, the verify done-leg =
                             \* 1169, the callback-wiring IsCompleted = 1185 - the convicted
                             \* thrower) trips `staleTokenRead` when it reads a Consumed token.
                             \* The CONSUMER side (the drain claim paths under the advancer latch,
                             \* WaiterStore's TryClaimSlotForDrain IsCompleted read) is
                             \* protocol-protected - the claim confers EXCLUSIVE consumption, so
                             \* those reads are legal PRE-CLAIM by construction and never trip.
                             \* Meaningful only under the split-commit fidelity (SplitCommitSelfActivate
                             \* /\ SplitCountCommit): the fused arms read task state atomically with
                             \* the count and model no post-publication window. FALSE = tokenConsumed
                             \* stays {} and staleTokenRead stays FALSE (both constant), so every
                             \* prior config is behaviorally unaffected and state-count identical.
                             \* Witness: Pipeline_CommitTokenWitness.cfg (NoStaleTokenRead RED).
  ,
  CommitOwnershipRestructure \* The FIX to adjudicate (July 2026, UPDATE 8 direction B). Meaningful
                             \* only under ConsumableTaskTokens /\ the split-commit fidelity.
                             \* Redesigns CommitWaiter so it NEVER touches the waiter task after
                             \* publication:
                             \*   (1) capture-once PRE-publication: `wasCompleted =
                             \*       waiterTask.IsCompleted` read BEFORE TryEscalateOrEnqueue, in
                             \*       the exclusive-ownership window (the item is still InTail, no
                             \*       drain can have claimed it) - always token-legal. Modeled as
                             \*       commitWasCompleted' captured at the VisibleWins/Loses publish.
                             \*   (2) callback wiring moves BEFORE publication (RestructureWireBeforePublish
                             \*       models the resulting late-visible-deposit face).
                             \*   (3) all POST-publication decisions key on STORE WORDS only: the
                             \*       ARM's wasEmpty self-activate uses the captured wasCompleted
                             \*       (not a fresh taskDone read); the RECHECK drops its task read;
                             \*       the VERIFY drops its task-done leg and keys on
                             \*       storeCount = 0 /\ waiters = <<>> (count re-read + taken - the
                             \*       observables the OnTaken/CountZero round proved imply
                             \*       retirement under complete-before-decrement).
                             \* TRUE: the post-publication commit reads take NO token, so
                             \* NoStaleTokenRead holds BY CONSTRUCTION; the adjudication is whether
                             \* the store-word decisions still deliver ActivatedAtMostOnce /
                             \* ReadTurnMutex / EventuallyActivated. FALSE = the shipped protocol
                             \* (the witness). Green target: Pipeline_CommitOwnershipFix.cfg.
  ,
  RestructureWireBeforePublish \* The late-visible-deposit face (July 2026), meaningful only under
                             \* CommitOwnershipRestructure. Moving the callback wiring before the
                             \* enqueue means the completion callback can FIRE (set the drain
                             \* signal, trigger a pass) while the item is NOT YET VISIBLE in the
                             \* store - the drain peeks nothing. TRUE injects that face: at the
                             \* publish step, if the item's task is already done at capture
                             \* (commitWasCompleted), the pre-wired callback's signal is raised
                             \* BEFORE the enqueue lands (cbWiredPrePublish tracks the deposit),
                             \* so the machinery that must rescue it (the commit's post-publication
                             \* drainSignal nudge re-check + PendingWordLatch deposit +
                             \* count-gated SignalConservation) is exercised against a genuinely
                             \* early signal. FALSE = the callback's signal rides the ordinary
                             \* post-publish path (no early window). Non-vacuity probes:
                             \* Pipeline_CommitTokenLateVisible.cfg.

VARIABLES
  loc,                  \* [Item -> Location] - per-item bucket.
  taskDone,             \* SUBSET Item - items whose pipelineTask SetResult has fired.
  activations,          \* [Item -> Nat] - per-item activation counter.
  callbackFired,        \* SUBSET Item - items whose completion callback has fired.
  failed,               \* SUBSET Item - items whose ExecuteItemAsync threw (bounded
                        \* per-item once: recovery-on-recovery is in the "things to add"
                        \* backlog, and bounding keeps activations[i] under the
                        \* ActivatedAtMostOnce bound for the in-scope recovery cycle).

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

  \* Completions-since-pass-start dirty flag (June 2026 retype of the old
  \* _waiterCompletedCount tally: every consumer read it as a boolean, the magnitude was
  \* unused, and the counter could transiently under-run when a drain consumed a
  \* visible-but-uncounted entry whose inline callback had not yet bumped it). Callbacks
  \* that bail set it; acquisitions (pass starts: callback-wins, reclaims, the nudge)
  \* consume it; the post-release recheck closes the lost-wake window - the IOQueue-style
  \* clear-fence-recheck gate.
  drainSignal,

  \* The queue drain pass's program counter (PassOnceDrain only; "none" otherwise).
  \* "pass" = inside the do-body (signal cleared, draining heads); "recheck" = released,
  \* about to read the signal ONCE; "reclaim" = signal seen, attempting TryReclaim.
  qDrainPhase,

  \* Whether the current pass dequeued anything since it consumed the signal (PassOnceDrain;
  \* reset at every pass/sub-pass entry clear). The SignalConservation restore keys on it.
  qDrainedAny,

  \* The latch word's pending bit (PendingWordLatch). Updated ATOMICALLY with the latch in
  \* the same action - that single-word atomicity IS the design. Set by a failed acquire's
  \* deposit; consumed by the release that reads it.
  advancingPending,

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
  \* Reset): at the slot drain (bundled with Complete, the shipped path), the queue drain head,
  \* the sync-success commit, or item failure. Deferred-dispatched items are deliberately
  \* NOT modeled: the deferred path's bridge consumes at SetResult time, before any waiter
  \* bookkeeping, and is contract-safe by construction.
  tenure,

  \* TRUE when the slot drain's post-decrement count was non-zero - the code's
  \* Debug.Assert(count == 0) firing (observed as exit-134 in Slon.Tests runs 12/25,
  \* June 2026). Bug witness for the stale invariant.
  assertFailed,

  \* TRUE when the queue drain's D-arm fired with nothing peekable - the code's unchecked
  \* `_waiters.TryPeek(out var nextItem)` at DrainReadyWaiters ~1124 activating default(T):
  \* the NullReferenceException / exit-134 abort proven June 2026 (crash_22130.dmp,
  \* DeferredActivationUnderSustainedLoad_Stress). Reachable only when the decremented
  \* count skews against the queue (SplitCountCommit). Bug witness.
  nullActivation,

  \* SUBSET Item - items whose queue callback has executed its plain `_drainSignal = true`
  \* store but not yet its `TryAcquireOrFlagPending` (SplitCallbackOps, backlog #3). The
  \* set-before-acquire window the fused CallbackBecomesAdvancer/BailsOut collapses - exposed
  \* so a concurrent callback's set and a concurrent advancer's pass-top clear can interleave
  \* against it. Empty unless SplitCallbackOps.
  callbackSignaled,

  \* [Item -> Item \cup {NoItem}] - the recovery binding (RecoverySplit, backlog #2/#5). Maps a
  \* substitute item to the failed item it must complete on behalf of. 
  \* NoItem = not a substitute. A distinct substitute identity
  \* (vs the legacy identity-reuse) makes the binding discharge modelable and the activation
  \* bound uniform (each item dispatched once). NoItem for all unless RecoverySplit.
  recoveryOf,

  \* The shared `_activatedItem` field (Pipeline.cs:90). A single Item-or-NoItem identity
  \* the policy/consumer reads to find the currently-activated item (Slon's PgDecoder
  \* reads it via `Control.ActivatedFlow` to attribute incoming messages). Written by
  \* ActivateHeadItem (set to the activating item) and CompleteWaiterDeferred (cleared).
  \* Modeled here to capture the bug class where a concurrent ActivateHeadItem on path B
  \* writes activatedSlot=NEW between path A's DecrementCount and path A's
  \* CompleteWaiterDeferred clear - A's unconditional clear then stomps NEW's publication
  \* and a consumer reading the slot before B re-publishes observes NoItem (Slon's
  \* PgDecoder NRE on CurrentExecutionControl, June 2026). The ReorderFix (Complete-before-
  \* Decrement, mirroring the recovery-fault path's audit reorder) closes the window
  \* structurally: the clear lands while count > 0, no concurrent activator can fire.
  activatedSlot,

  \* TRUE if the slot drain's clear ever wiped activatedSlot while it pointed at an
  \* item that hadn't yet been completed - the stomp face of the bug. Bug witness for
  \* NoStompedActivation. Set in SlotDrainCompleteClear and never reset within a run.
  slotStomped,

  \* TRUE if an ACTIVATION ever moved activatedSlot off a still-live reader onto a new item -
  \* two flows holding the one read turn at once. slotStomped catches a CLEAR stomping a live
  \* reader; this catches an ACTIVATION doing it (the deferred-publish's "overrides any racing
  \* write", the C-path/wasEmpty self-activate). The root is a single invariant - one read turn
  \* at a time; a violation surfaces in several variations, no one of them canonical:
  \* ReadState.ReadPromise's single-tenant TryStart throwing "async method already executing",
  \* the shared PgDecoder re-armed under a live read, the _activatedItem slot stomped to NoItem
  \* (the NRE). All faces of the same two-turn collision the _liveReaderActive single-reader gate
  \* closes (Pipeline.cs, June 2026). Bug witness for ReadTurnMutex; set by ReadTurnMonitor,
  \* conjoined into every action that writes activatedSlot (mirrors slotStomped/activatedSlot's
  \* own UNCHANGED partition), never reset within a run.
  readTurnStomped,

\* The _liveActivation occupancy bit (SingleActivationGate; Pipeline.cs ~2003). TRUE while an
\* activation is live: set by every ActivateHeadItem-equivalent (any action that bumps
\* activations), cleared at every retirement (the CompleteWaiterDeferred-equivalents - the
\* waiter/recovery completion transitions; the fault parks skip it, as the code's catch routes
\* to RecoverWaiter instead of CompleteWaiterDeferred). Threaded through every action so the
\* prior configs stay well-formed; READ only under SingleActivationGate, at the two gated sites.
  liveActivation,

\* Consumable-token lifecycle (ConsumableTaskTokens). A waiter task's single-consumption
\* ValueTask: readable until a drain's GetResult CONSUMES it, after which any stale-holder read
\* observes a recycled promise box (version bump) and throws. tokenConsumed is the set of items
\* whose task has been GetResult'd (a drain reached its consume point); Recycled is modeled
\* identical to Consumed (any read after Consumed is the error). Empty unless ConsumableTaskTokens.
  tokenConsumed,

\* Witness (ConsumableTaskTokens): a COMMIT-side POST-PUBLICATION action read a Consumed token's
\* state (the shipped protocol's out-of-lock IsCompleted / in-lock recheck / verify done-leg /
\* callback-wiring IsCompleted - Pipeline.cs 1136/1152/1169/1185). Bug witness for NoStaleTokenRead;
\* set true and never reset. FALSE unless ConsumableTaskTokens, so prior configs are state-identical.
  staleTokenRead,

\* The restructure's PRE-publication IsCompleted capture (CommitOwnershipRestructure). Read in the
\* exclusive-ownership window (item still InTail, before TryEscalateOrEnqueue) and consumed by the
\* post-publication ARM/VERIFY decisions in place of a fresh taskDone read. One commit in flight
\* (the executor is sequential), so a single boolean suffices. FALSE unless CommitOwnershipRestructure.
  commitWasCompleted,

\* Late-visible-deposit bookkeeping (RestructureWireBeforePublish). TRUE while a pre-wired callback
\* raised the drain signal BEFORE its item became visible in the store (the enqueue not yet landed),
\* so the drain it triggered peeked nothing - the window the commit's post-publish nudge + the
\* PendingWordLatch deposit + count-gated conservation must rescue. FALSE unless the face toggle.
  cbWiredPrePublish

\* Variable groupings - used as `UNCHANGED group_name` in action bodies for compactness.
token_vars   == <<tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
publish_vars == <<executingItem, executingItemVisible, hasExecuting, hasExecutingVisible>>
tail_vars    == <<tailWaiter, hasTail>>
adv_vars     == <<advancing, advancingVisible>>
counters     == <<storeCount, drainSignal>>
\* slot_vars / esc_vars / store_vars come from WaiterStore.tla.
drainer_vars == <<drainerActive>>
item_vars    == <<loc, taskDone, activations, callbackFired, failed>>
\* June 2026 additions (slot-drain split + tenure). Actions that predate them and don't
\* interact get UNCHANGED aux_vars applied at the Next relation rather than per-body.
\* aux_vars: things actions don't touch (default UNCHANGED via the W-wrappers). activatedSlot
\* is included here so non-activation actions UNCHANGE it without ceremony; the W-wrappers
\* for activation actions (which DO write activatedSlot) use aux_vars_act instead, which
\* drops activatedSlot from the UNCHANGED bundle so the body's write isn't shadowed.
\* Token-FREE aux copies (aux_vars0 / aux_vars_act0) for the commit token-WRITERS: those actions
\* fully specify token_vars in their bodies (the stale-read trip, the pre-publication capture), so
\* their wrappers must NOT re-constrain tokens. Every OTHER wrapper uses the token-FULL bundles.
aux_vars0    == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, activatedSlot, slotStomped,
                  tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
                  callbackSignaled, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
aux_vars_act0   == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, slotStomped,
                     tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
                     callbackSignaled, slotStomped, recoveryOf>>
aux_vars     == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, activatedSlot, slotStomped,
                  tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
                  callbackSignaled, slotStomped, recoveryOf, readTurnStomped, liveActivation,
                  tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
\* aux minus the queue-drain PC, for wrappers of actions that WRITE qDrainPhase
\* (the advancer-acquire entries under PassOnceDrain).
aux_nophase  == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                  tenure, assertFailed, nullActivation, advancingPending,
                  callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation,
                  tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
\* aux minus the PC and the latch pending bit, for the release steps (they write both).
aux_relmin   == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, activatedSlot, slotStomped,
                  tenure, assertFailed, nullActivation, callbackSignaled, slotStomped, recoveryOf, readTurnStomped, liveActivation,
                  tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
\* aux minus only the latch pending bit, for the bail wrappers (failed acquire deposits).
aux_nopend   == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, activatedSlot, slotStomped,
                  tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny,
                  callbackSignaled, slotStomped, recoveryOf, readTurnStomped, liveActivation,
                  tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
\* aux_vars variants for actions that WRITE activatedSlot (any ActivateHeadItem-equivalent
\* and any CompleteWaiterDeferred-equivalent slot clear). Identical to the matching aux_*
\* but DROPS activatedSlot so the W-wrapper doesn't conflict with the body's write.
\* NOTE: the _act bundles stay token-FREE (like they are activatedSlot/readTurnStomped/
\* liveActivation-free): the activation-writing actions manage token vars explicitly at the
\* wrapper level (the commit arms write them; every other _act wrapper adds UNCHANGED token_vars).
aux_vars_act    == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, slotStomped,
                     tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
                     callbackSignaled, slotStomped, recoveryOf,
                     tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
aux_nophase_act == <<drainPhase, drainItem, drainRemaining, pendingHeadActivation, slotStomped,
                     tenure, assertFailed, nullActivation, advancingPending,
                     callbackSignaled, slotStomped, recoveryOf,
                     tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>

vars == <<loc, taskDone, activations, callbackFired, failed,
          executingItem, executingItemVisible, hasExecuting, hasExecutingVisible,
          tailWaiter, hasTail, advancing, advancingVisible, storeCount, drainSignal,
          hasSlot, slotItem, escalated, waiters,
          escPhase, escTail, escSlotClaimed, escMoved, drainerActive,
          drainPhase, drainItem, drainRemaining, pendingHeadActivation, activatedSlot, slotStomped,
          tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
          callbackSignaled, slotStomped, recoveryOf, readTurnStomped, liveActivation,
          tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>

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
\* InSlotPending: the slot commit's CAS landed but the field writes haven't (SplitSlotFieldOps;
\* the slot-tier mirror of InWaitersPending). The slot is claimable in this state - a claim
\* Exchange here reads the previous tenure's cleared fields (NoDefaultSlotClaim's window).
\* DispatchArmed: the split elision window (SplitIdleDispatchGuard): the executor has read the
\* elision guard (count/latch/pending) and vouched for idle, but has not yet run ActivateHeadItem.
\* Executor PC state riding loc - the item is in the executor's hand, in no store tier, its task
\* cannot complete (ExecuteItemAsync has not run). Unreachable unless LockFreeIdleDispatch /\
\* SplitIdleDispatchGuard.
\* RecoveringInline: a DRAIN-side park (the drain's GetResult faulted on a committed waiter;
\* RecoverWaiter on the advancer thread). Distinct from "Recovering" (executor-side parks)
\* because the two recovery lifecycles diverge in code: the executor-side substitute mirrors
\* the main loop (deferred publish / tail / CommitTailWaiter, Pipeline.cs RecoverItem ~536),
\* while the drain-side substitute is activated unconditionally and completes IN PLACE under
\* the held advancer latch (RecoverWaiter ~1418 -> CompleteRecoveryWaiter) - it NEVER enters
\* the store. Funneling drain-side substitutes through the shared tail/commit/store actions
\* let TLC manufacture code-impossible double activations (the inline-activated substitute
\* re-activated by the store's wasEmpty/D-path arms once it queued behind a live successor).
Locations == {"Nowhere", "Executing", "InTail", "InSlot", "InSlotPending", "InEscalation",
              "InWaitersPending", "InWaiters", "Recovering", "RecoveringInline", "Draining",
              "DispatchArmed", "Completed"}

\* depth-0 slot clear (the shipped CompleteWaiterDeferred): the slot returns to NoItem only when THIS
\* completion empties the pipeline (every other item already Completed). Otherwise the slot keeps its
\* (stale-but-safe) value - never nulled while a later item is still live. S = the item(s) completed
\* by this action. Replaces the pre-fix identity-clear, which nulled the owner the instant it
\* completed (violating NoNullWhileLive when a later item was still pending).
DepthZeroClear(S) == IF (\A j \in Item \ S : loc[j] = "Completed") THEN NoItem ELSE activatedSlot

\* The _liveActivation gate's read (SingleActivationGate): TRUE when an activation may be
\* granted at a gated site - either the gate is unmodeled (pre-fix configs) or no activation
\* is live. Used ONLY by the two gated sites (CommitWaiter's wasEmpty self-activate and the
\* advancer's queue C-path); every other activation path ignores it, as the code does.
GateOpen == ~SingleActivationGate \/ ~liveActivation

\* Consumable-token helpers (ConsumableTaskTokens). Gated on the toggle so both stay CONSTANT
\* (tokenConsumed = {}, staleTokenRead = FALSE) in every prior config - the state count is preserved.
\* ConsumeToken marks item i's waiter task Consumed at a drain's GetResult (the token's lifetime
\* ends; Recycled is modeled identical). CommitTokenTrip fires the witness when a COMMIT-side
\* POST-PUBLICATION read (SHIPPED protocol only - the restructure never reads task state after
\* publication) observes item i already Consumed (Pipeline.cs 1136/1152/1169/1185, the same
\* single-consumption ValueTask read class; 1185's callback-wiring IsCompleted is the convicted thrower).
ConsumeToken(i) == tokenConsumed' = IF ConsumableTaskTokens THEN tokenConsumed \cup {i} ELSE tokenConsumed
CommitTokenTrip(i) ==
  staleTokenRead' = IF ConsumableTaskTokens /\ ~CommitOwnershipRestructure /\ i \in tokenConsumed
                    THEN TRUE ELSE staleTokenRead

\* Read-turn mutex witness. Conjoined into every action that WRITES activatedSlot (the same
\* actions that drop it from their UNCHANGED - the _act W-forms and the in-body activators/
\* clearers); non-writers UNCHANGE readTurnStomped via the aux bundles, exactly as they do
\* activatedSlot. Reading activatedSlot' generically means one conjunct covers every arm and
\* every conditional activatedSlot' the action may take. Sets the witness TRUE when an
\* ACTIVATION moves the slot off a still-live reader onto a new item - two flows holding the
\* single-tenant read baton (ReadState.ReadPromise) at once, and the shared PgDecoder re-armed
\* under a live read. slotStomped catches the mirror face (a CLEAR landing on a live item); the
\* live-reader predicate is shared (loc \notin {Completed, Nowhere}). A legitimate handoff
\* activates only after the predecessor retired (loc = Completed), so it does not trip. Note
\* tenure is NOT this resource: tenure = activation-occupancy from the EXECUTOR's single
\* dispatch (set on inline-dispatch / recovery-install, released at consume), so the advancer/
\* drain activations (C-path, wasEmpty, drain-head) never touch it - the read turn as a
\* cross-path shared resource was never modeled at all. That is the fidelity gap this fills.
ReadTurnMonitor ==
  readTurnStomped' =
    IF /\ activatedSlot \in Item          \* slot held a real item...
       /\ activatedSlot' \in Item         \* ...and the step is an ACTIVATION (fresh real item), not a clear
       /\ activatedSlot' # activatedSlot  \* ...moved to a DIFFERENT item
       \* ...whose predecessor was still a live reader of the SHARED tenure. Excludes Completed/
       \* Nowhere (as slotStomped does) AND Recovering/RecoveringInline: a faulted item does not
       \* hold a SECOND tenure - its substitute takes over that SAME read tenure in place (the
       \* faulted identity is replaced and disappears, RecoverInstall*/RecoverItem*), so installing
       \* the substitute over a Recovering slot is a handoff of the one tenure, not two readers.
       /\ loc[activatedSlot] \notin {"Completed", "Nowhere", "Recovering", "RecoveringInline"}
       \* ...AND the predecessor is not being RETIRED in this same step. A single-path handoff
       \* (AdvancerDrainHead completes the drained head AND activates the next head atomically)
       \* moves the slot off a pre-state-live item that is Completed by the very same step - the
       \* turn ends as the next begins, one reader throughout. The stomp is TWO turns coexisting:
       \* the displaced item must STILL be live after the step (post-state loc also live).
       /\ loc'[activatedSlot] \notin {"Completed", "Nowhere", "Recovering", "RecoveringInline"}
    THEN TRUE
    ELSE readTurnStomped

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
  /\ drainSignal = FALSE
  /\ qDrainPhase = "none"
  /\ qDrainedAny = FALSE
  /\ advancingPending = FALSE
  /\ callbackFired = {}
  /\ callbackSignaled = {}
  /\ recoveryOf = [i \in Item |-> NoItem]
  /\ drainerActive = FALSE
  /\ failed = {}
  /\ drainPhase = "idle"
  /\ drainItem = NoItem
  /\ drainRemaining = 0
  /\ pendingHeadActivation = FALSE
  /\ tenure = NoItem
  /\ assertFailed = FALSE
  /\ nullActivation = FALSE
  /\ activatedSlot = NoItem
  /\ slotStomped = FALSE
  /\ readTurnStomped = FALSE
  /\ liveActivation = FALSE
  /\ tokenConsumed = {}
  /\ staleTokenRead = FALSE
  /\ commitWasCompleted = FALSE
  /\ cbWiredPrePublish = FALSE

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

\* RecoverySplit reserves the TOP identity (NumItems) as the substitute reservoir: the source
\* cannot yield it, so a recovery substitute always draws a fresh identity the workload did not
\* consume (otherwise TLC drains the shared pool with normal yields and a later failure has no
\* substitute - a model resource artifact, not a real strand). Phase A reserves one (single
\* failure). Under ~RecoverySplit every identity is a source slot (the 9 configs unchanged).
SubSlot == NumItems
SourceSlot(i) == (~RecoverySplit) \/ (i # SubSlot)

\* Source yields next item with no existing waiters - inline activation.
\* The source's MoveNextAsync is the wait; when it returns, we're already in the
\* executor with an item ready to dispatch. No queue, no wake signal.
SourceYieldInline ==
  /\ storeCount = 0
  /\ ~hasTail  \* tail must be committed before next yield
  /\ escPhase = "idle"  \* executor only yields next item after current commit completes
  /\ \A i \in Item : loc[i] # "Executing"  \* executor is sequential
  /\ \A i \in Item : loc[i] # "Recovering" \* pending recovery must install before next yield
  /\ \A i \in Item : loc[i] # "DispatchArmed"  \* executor is mid-elision (one thread, one PC)
  /\ tenure = NoItem  \* the dispatch's TryStart
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"  \* item not yet yielded by source
       /\ SourceSlot(i)       \* not the reserved substitute identity (RecoverySplit)
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       /\ activations' = [activations EXCEPT ![i] = @ + 1]
       /\ tenure' = i
       /\ activatedSlot' = i  \* ActivateHeadItem writes _activatedItem = item
       /\ liveActivation' = TRUE  \* ActivateHeadItem grants the activation turn
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, callbackFired, failed, waiters, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Source yields next item with existing waiters - deferred publish.
SourceYieldDeferred ==
  /\ storeCount > 0
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ \A i \in Item : loc[i] # "Executing"
  /\ \A i \in Item : loc[i] # "Recovering"
  /\ \A i \in Item : loc[i] # "DispatchArmed"  \* executor is mid-elision (one thread, one PC)
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"
       /\ SourceSlot(i)
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

(* ===========================================================================
   Idle-regime lock elision (LockFreeIdleDispatch): the escalated dispatch's
   proposed lock-free arm. The shipped escalated dispatch (Pipeline.cs ~390-421)
   is modeled by SourceYieldInline (count 0: publish pending + claim it back +
   activate, net inline - fused as one atomic action, honest because the lock
   serializes it against the C-path lock block) and SourceYieldDeferred
   (count > 0: deferred publish). The elision arm reads
     count = 0 (clamped public read: storeCount <= 0; at dispatch the
       executor-side skew windows are closed - escPhase = "idle" - so <= 0
       coincides with = 0, stated clamped for fidelity)
     /\ ~advancingVisible (the latch word, remote read; acquires are
       interlocked so FALSE is never stale-free)
     /\ ~hasExecuting (pending, own-thread read)
   and on success activates the head inline with NO lock and NO pending
   publish. Guard failure falls to the locked arm - SourceYieldInline /
   SourceYieldDeferred stay enabled unchanged (the model keeps the locked
   count-0 dispatch enabled even where the guard holds: an over-approximation
   that only adds already-verified behaviors).
   =========================================================================== *)

\* FUSED fidelity level (~SplitIdleDispatchGuard): guard reads + activation as one atomic
\* action. Adjudicates the pure logic. Note the guard is strictly stronger than
\* SourceYieldInline's (same dispatch state, extra latch/pending conjuncts + escalated), so
\* this action's behaviors are a SUBSET of the verified locked dispatch's - fused green is
\* the expected baseline, not evidence about the check-then-act window.
SourceYieldElide ==
  /\ LockFreeIdleDispatch
  /\ ~SplitIdleDispatchGuard
  /\ escalated              \* the elision arm is the ESCALATED fork's; pre-escalation
                            \* dispatch is already lock-free (SourceYieldInline as-is)
  /\ storeCount <= 0        \* the clamped public Count read (see block comment)
  /\ ~advancingVisible      \* latch not held (remote read of the latch word)
  /\ ~hasExecuting          \* pending not set (own-thread read)
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ \A i \in Item : loc[i] # "Executing"
  /\ \A i \in Item : loc[i] # "Recovering"
  /\ \A i \in Item : loc[i] # "DispatchArmed"
  /\ tenure = NoItem
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"
       /\ SourceSlot(i)
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       /\ activations' = [activations EXCEPT ![i] = @ + 1]
       /\ tenure' = i
       /\ activatedSlot' = i  \* ActivateHeadItem writes _activatedItem = item
       /\ liveActivation' = TRUE  \* ActivateHeadItem grants the activation turn
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, callbackFired, failed, waiters, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* SPLIT fidelity level, step 1 (SplitIdleDispatchGuard): the guard's three reads as their
\* own action - the check half of the check-then-act. The picked item parks in
\* DispatchArmed (executor PC riding loc; blocks every other executor dispatch step, as
\* one thread's program order does). Every OTHER thread interleaves freely before the act:
\* a callback can acquire the latch, a drainer's post-release tail can reclaim, counts can
\* move - whatever work exists to move them.
SourceYieldElideArm ==
  /\ LockFreeIdleDispatch
  /\ SplitIdleDispatchGuard
  /\ escalated
  /\ storeCount <= 0        \* guard read 1: the clamped public Count
  /\ ~advancingVisible      \* guard read 2: the latch word
  /\ ~hasExecuting          \* guard read 3: pending
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ \A i \in Item : loc[i] # "Executing"
  /\ \A i \in Item : loc[i] # "Recovering"
  /\ \A i \in Item : loc[i] # "DispatchArmed"
  /\ tenure = NoItem
  /\ \E i \in Item :
       /\ loc[i] = "Nowhere"
       /\ SourceSlot(i)
       /\ loc' = [loc EXCEPT ![i] = "DispatchArmed"]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, activations, callbackFired, failed, waiters, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
                 callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* SPLIT fidelity level, step 2: the act. NO re-reads - the arm's guard is the only
\* license, possibly stale by now. Activates inline exactly as the fused arm does. WF in
\* Spec: this is the executor's unconditional next program step once armed.
SourceYieldElideAct ==
  /\ \E i \in Item :
       /\ loc[i] = "DispatchArmed"
       /\ loc' = [loc EXCEPT ![i] = "Executing"]
       /\ activations' = [activations EXCEPT ![i] = @ + 1]
       /\ tenure' = i
       /\ activatedSlot' = i
       /\ liveActivation' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars,
                 taskDone, callbackFired, failed, waiters, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Executor stores item as tail after ExecuteItemAsync returns (async pipelineTask).
\* ~hasTail: _tailWaiter is a single executor-owned cell, written only after the previous
\* tail's CommitTailWaiter consumed it (top-of-loop ordering). Vacuous while only one item
\* can be Executing; load-bearing under RecoverySplit, where a drain-side park's substitute
\* and the executor's own item are concurrently Executing - without it a second SetTail
\* would overwrite (orphan) an uncommitted tail, a code-impossible state.
ExecSetTail ==
  /\ ~hasTail
  /\ \E i \in Item :
    /\ loc[i] = "Executing"
    \* Drain-side substitutes (bound to a RecoveringInline park) live the IN-PLACE lifecycle
    \* (RecoverWaiter executes and completes them on the advancer; they never become the
    \* executor's tail). Executor-side substitutes (bound to "Recovering") DO re-enter the
    \* normal tail flow (Pipeline.cs RecoverItem ~536) and are admitted here.
    /\ (IF recoveryOf[i] = NoItem THEN TRUE ELSE loc[recoveryOf[i]] # "RecoveringInline")
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
            /\ activatedSlot' = DepthZeroClear({i})
            /\ liveActivation' = FALSE  \* CompleteWaiterDeferred releases the turn at retirement
            /\ drainSignal' = drainSignal
            /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters, storeCount>>
          ELSE
            IF ~escalated /\ ~hasSlot
              THEN \* Slot path: zero-alloc, wire slot callback. Fused fiction only; the
                   \* split triple (ExecCommitSlotCAS*/Fields/Count*) covers this case
                   \* under SplitSlotFieldOps.
                   \* wasEmpty self-activate: GATED under SingleActivationGate - a live
                   \* activation defers to the advancer's FIFO; the commit still lands.
                /\ ~SplitSlotFieldOps
                /\ StoreSlotCommit(i)
                /\ loc' = [loc EXCEPT ![i] = "InSlot"]
                /\ activations' = IF storeCount = 0 /\ GateOpen
                                  THEN [activations EXCEPT ![i] = @ + 1]
                                  ELSE activations
                /\ activatedSlot' = IF storeCount = 0 /\ GateOpen THEN i ELSE activatedSlot
                /\ liveActivation' = IF storeCount = 0 /\ GateOpen THEN TRUE ELSE liveActivation
                /\ drainSignal' = drainSignal
              ELSE \* Post-escalation queue path (escalated guard inside the operator).
                   \* Loosened CAS: no slot manipulation (PostEscalationSlotEmpty).
                   \* Fused fiction only; the split pair (ExecCommitQueueVisible*/Count*)
                   \* covers this case under SplitCountCommit.
                   \* wasEmpty self-activate: GATED (see the slot arm).
                /\ ~SplitCountCommit
                /\ StoreQueueEnqueue(i)
                /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
                /\ activations' = IF storeCount = 0 /\ GateOpen
                                  THEN [activations EXCEPT ![i] = @ + 1]
                                  ELSE activations
                /\ activatedSlot' = IF storeCount = 0 /\ GateOpen THEN i ELSE activatedSlot
                /\ liveActivation' = IF storeCount = 0 /\ GateOpen THEN TRUE ELSE liveActivation
                /\ drainSignal' = drainSignal
  /\ UNCHANGED <<adv_vars, taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

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
            /\ activatedSlot' = DepthZeroClear({i})
            /\ liveActivation' = FALSE  \* CompleteWaiterDeferred releases the turn at retirement
            /\ drainSignal' = drainSignal
            /\ UNCHANGED <<hasSlot, slotItem, escalated, waiters, storeCount>>
          ELSE
            IF ~escalated /\ ~hasSlot
              THEN \* Slot path. activated=true: no B-path activate. Fused fiction only
                   \* (see ExecCommitTailExecutorWins's slot arm).
                /\ ~SplitSlotFieldOps
                /\ StoreSlotCommit(i)
                /\ loc' = [loc EXCEPT ![i] = "InSlot"]
                /\ activations' = activations
                /\ activatedSlot' = activatedSlot
                /\ liveActivation' = liveActivation
                /\ drainSignal' = drainSignal
              ELSE \* Post-escalation queue path (loosened CAS, escalated guard in the operator).
                   \* Fused fiction only (see ExecCommitTailExecutorWins's queue arm).
                /\ ~SplitCountCommit
                /\ StoreQueueEnqueue(i)
                /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
                /\ activations' = activations
                /\ activatedSlot' = activatedSlot
                /\ liveActivation' = liveActivation
                /\ drainSignal' = drainSignal
  /\ UNCHANGED <<publish_vars, adv_vars, taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Pre-faulted commit (RecoverySplit, Phase D). CommitTailWaiter observes the tail's pipeline
\* task already SETTLED and FAULTED at commit time - the code's
\*   `if (task.IsCompleted) { if (IsCompletedSuccessfully) {...complete...}
\*    return RecoverCommittedTailWaiterAsync(item, task).AsTask(); }`
\* branch (Pipeline.cs ~698-707). The recovery is awaited INLINE on the executor's logical
\* thread (NOT via the advancer - that preserves the SPSC producer role on _waiters and the
\* commit ordering), so the faulted tail parks in "Recovering" and the executor's next yield
\* stays blocked until the substitute resolves (the SourceYield* Recovering guard IS the
\* inline await). The store is untouched: the fault is caught before CommitWaiter ever runs.
\* The _hasExecutingItem Exchange precedes the IsCompleted check in code, so the publish
\* consume mirrors the Wins/Loses pair in one action: a won Exchange (own-thread hasExecuting
\* observation) clears the publish, a lost one leaves it. The fault is the nondeterministic
\* alternative to the sync-success arm of ExecCommitTailExecutorWins/Loses (a settled tail
\* either succeeded -> those, or faulted -> here); the parked item then resolves via
\* RecoverInstallWins/Loses / RecoverRefuse. RecoverCommittedTailWaiterAsync's GetResult
\* consumed the task, so any tenure the tail held releases here (sync-success's twin rule).
ExecCommitTailRecovers ==
  /\ RecoverySplit
  /\ failed = {}                 \* single-failure bound (shared with the other injection points)
  /\ hasTail
  /\ escPhase = "idle"
  /\ LET i == tailWaiter IN
       /\ i \in taskDone          \* settled at commit; the fault face of the sync arm
       /\ recoveryOf[i] = NoItem  \* first-level (a substitute's guarded tail completes directly)
       /\ loc' = [loc EXCEPT ![i] = "Recovering"]
       /\ failed' = failed \cup {i}
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
       \* NOT split under SplitRecoveryInlineAct: this twin's claim race resolves BEFORE the
       \* fault park in program order (CommitTailWaiter's in-lock claim precedes the
       \* IsCompleted check - Pipeline.cs ~863-882), so the fused consume is already
       \* faithful; a C-path grant of the settled tail lands PRE-park (while InTail) and is
       \* explored as-is. Only the execute-phase twin (ExecItemFailure), where the fault
       \* precedes ClearExecutingItem, gets the split.
       /\ (IF hasExecuting  \* own-thread observation: the Exchange preceded the IsCompleted check
            THEN /\ hasExecuting' = FALSE
                 /\ hasExecutingVisible' = FALSE
                 /\ executingItem' = NoItem
                 /\ executingItemVisible' = NoItem
            ELSE UNCHANGED publish_vars)
  /\ UNCHANGED <<adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending,
                 callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

(* ===========================================================================
   Truthful queue commit (SplitCountCommit): TryEscalateOrEnqueue's post-
   escalation path as the TWO steps the code actually performs -
   `queue.Enqueue(entry)` (visible/consumable immediately) then
   `Interlocked.Increment(ref _count)`. A drain consuming the entry between
   them is the count-skew window (June 2026, dump crash_22130.dmp): the
   consumer's DecrementCount returns -1 and the C/D activation partition,
   built for {0, >0}, misroutes on both sides.
   =========================================================================== *)

\* Step 1, executor won the publish Exchange (alreadyActivated = false). The Exchange claim
\* precedes the enqueue in code (CommitTailWaiter claims before calling CommitWaiter), so
\* the publish clears here; the activation variant rides the phase token to the count step.
ExecCommitQueueVisibleWins ==
  /\ SplitCountCommit
  /\ hasTail
  /\ hasExecuting  \* Exchange reads own write; TRUE = wins (see ExecCommitTailExecutorWins)
  /\ LET i == tailWaiter IN
       /\ i \notin taskDone  \* sync-success never reaches the store; stays on the fused action
       /\ StoreQueueEnqueueVisible(i, "q_enq_act")
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
       \* CommitOwnershipRestructure: capture-once PRE-publication (Pipeline.cs restructured
       \* 1125-ish: `wasCompleted = waiterTask.IsCompleted` BEFORE TryEscalateOrEnqueue). The item
       \* is still exclusively owned (InTail pre-state; no drain has claimed it), so this read is
       \* always token-legal. The VisibleWins guard already routes a done-at-commit item to the
       \* fused sync path, so on the split path the captured value is FALSE - faithful: the
       \* restructure's whole point is that a mid-flight completion is NOT re-observed.
       /\ commitWasCompleted' = IF CommitOwnershipRestructure THEN (i \in taskDone) ELSE commitWasCompleted
       \* Late-visible-deposit face (RestructureWireBeforePublish): with the callback wired
       \* BEFORE this enqueue, its completion signal can already be up when the item becomes
       \* visible, and the pre-wired callback's own drain pass peeked NOTHING (the item was still
       \* InTail). Model the conserved deposit: the drain signal is raised and the window flag set
       \* as the item lands resident-but-un-drained. The rescue is the commit's own post-publish
       \* nudge (Pipeline.cs:1217 re-reads _drainSignal after TryEscalateOrEnqueue and drains) plus
       \* count-gated SignalConservation - EventuallyCompleted/Activated must still hold.
       /\ drainSignal' = IF RestructureWireBeforePublish THEN TRUE ELSE drainSignal
       /\ cbWiredPrePublish' = IF RestructureWireBeforePublish THEN TRUE ELSE cbWiredPrePublish
  /\ UNCHANGED <<adv_vars, taskDone, activations, callbackFired, failed,
                 drainer_vars, tokenConsumed, staleTokenRead>>

\* Step 1, executor lost (alreadyActivated = true): no publish touch, no activation later.
ExecCommitQueueVisibleLoses ==
  /\ SplitCountCommit
  /\ hasTail
  /\ ~hasExecuting
  /\ LET i == tailWaiter IN
       /\ i \notin taskDone
       /\ StoreQueueEnqueueVisible(i, "q_enq_noact")
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
       /\ commitWasCompleted' = IF CommitOwnershipRestructure THEN (i \in taskDone) ELSE commitWasCompleted
  /\ UNCHANGED <<publish_vars, adv_vars, drainSignal, taskDone, activations,
                 callbackFired, failed, drainer_vars, tokenConsumed, staleTokenRead, cbWiredPrePublish>>

\* Step 2: the increment. The wasEmpty partition (post-increment count == 1, i.e.
\* pre-increment 0) and the activation's IsCompleted skip both live HERE, where the code
\* reads them - the entry's task completing between the steps changes both answers. The
\* quiet face of the count-skew: at a skewed pre-increment count of -1 the post value is 0,
\* wasEmpty is FALSE, and the committer skips an activation no drain will perform either.
ExecCommitQueueCountWins ==
  /\ ~SplitCommitSelfActivate  \* FUSED: the split arm (ARM/ACT) replaces this under the toggle
  /\ escPhase = "q_enq_act"
  /\ LET i == escTail IN
       \* wasEmpty self-activate, split-commit form: GATED under SingleActivationGate
       \* (the code's `wasEmpty && !IsCompleted && !_liveActivation` under _activationLock).
       /\ activations' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen
                         THEN [activations EXCEPT ![i] = @ + 1]
                         ELSE activations
       /\ liveActivation' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen
                            THEN TRUE ELSE liveActivation
  /\ StoreCommitCount("idle")
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars>>

(* ===========================================================================
   SplitCommitSelfActivate: ExecCommitQueueCountWins as its two real instructions.

   ARM  = _waiters.Enqueue's Interlocked.Increment (StoreCommitCount inlined so escTail
          survives as the item handle) fused with the CAPTURE of `wasEmpty` (pre-increment
          storeCount = 0) and the OUT-OF-LOCK IsCompleted check (i \notin taskDone). The
          verdict rides escPhase: "q_selfact_fire" (both held -> the lock will be taken) or
          "q_selfact_done" (either failed -> the code takes no lock, no activation). The item
          stays InWaitersPending, now COUNTED; every other thread is free to run.

   ACT  = the lock/gate/activate. The IsCompleted verdict is the CAPTURED one (no re-read);
          the gate (GateOpen) IS re-read fresh, matching `!_liveActivation` under the lock.
          CommitSelfActRecheck adds an IsCompleted re-read at act time. Queue arm: no
          activatedSlot write (mirrors ExecCommitQueueCountWins exactly).
   =========================================================================== *)

\* ARM: the increment + the wasEmpty/IsCompleted capture, verdict onto escPhase, escTail kept.
ExecCommitQueueCountArm ==
  /\ SplitCommitSelfActivate
  /\ escPhase = "q_enq_act"
  /\ LET i == escTail IN
       \* wasEmpty && !IsCompleted. SHIPPED: fresh post-publication `i \notin taskDone`
       \* (Pipeline.cs:1136 out-of-lock IsCompleted) - a POST-PUBLICATION token read, tripped
       \* by CommitTokenTrip when the item was drain-Consumed. RESTRUCTURE: the pre-publication
       \* captured verdict `~commitWasCompleted` (no token read - the exclusive-window capture).
       LET notDone == IF CommitOwnershipRestructure THEN ~commitWasCompleted ELSE i \notin taskDone IN
       /\ storeCount' = storeCount + 1                          \* Interlocked.Increment
       /\ escPhase' = IF storeCount = 0 /\ notDone
                      THEN "q_selfact_fire" ELSE "q_selfact_done"
       /\ CommitTokenTrip(i)
  /\ escTail' = escTail                                          \* item handle preserved
  /\ escSlotClaimed' = FALSE
  /\ escMoved' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, tokenConsumed, commitWasCompleted, cbWiredPrePublish>>

\* ACT, fire face: captured wasEmpty && !IsCompleted. The lock body re-reads the gate fresh
\* (GateOpen) and, under CommitSelfActRecheck, re-reads IsCompleted; on a live gate (or a
\* re-checked completed task) it leaves the item committed-unactivated, exactly as the code's
\* `if (!_liveActivation)` / IsCompleted skip does. NO activatedSlot write (queue mirror).
ExecCommitQueueCountActFire ==
  /\ ~SplitCommitSelfActRecheck  \* SPLIT: RECHECK/FIRE(/VERIFY) replace the fused re-read+activate
  /\ escPhase = "q_selfact_fire"
  /\ LET i == escTail IN
       \* SHIPPED: the in-lock recheck (Pipeline.cs:1152) re-reads `i \notin taskDone` under
       \* CommitSelfActRecheck - a POST-PUBLICATION token read. RESTRUCTURE: dropped (gate only).
       LET doAct == GateOpen /\ (CommitOwnershipRestructure \/ ~CommitSelfActRecheck \/ i \notin taskDone) IN
       /\ activations' = IF doAct THEN [activations EXCEPT ![i] = @ + 1] ELSE activations
       /\ liveActivation' = IF doAct THEN TRUE ELSE liveActivation
       \* trip only when the recheck read actually exists (shipped CommitSelfActRecheck)
       /\ staleTokenRead' = IF ConsumableTaskTokens /\ ~CommitOwnershipRestructure /\ CommitSelfActRecheck /\ i \in tokenConsumed
                            THEN TRUE ELSE staleTokenRead
  /\ escPhase' = "idle"
  /\ escTail' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved,
                 tokenConsumed, commitWasCompleted, cbWiredPrePublish>>

(* ===========================================================================
   SplitCommitSelfActRecheck, queue arm: the fused ActFire as the two (RECHECK, FIRE) or three
   (RECHECK, FIRE, VERIFY) instructions the code performs inside the ONE _activationLock hold
   (Pipeline.cs 1127-1144). The lock is taken at 1127; escPhase in the *_selfact_{fire,dofire,
   verify} window IS that hold (SelfActLockHeld / NoForeignGrant enforce mutual exclusion).
   =========================================================================== *)

\* RECHECK (queue): Pipeline.cs:1134 `!waiterTask.IsCompleted` re-read as its own in-lock step;
\* park "q_selfact_dofire" when not done, else go idle (the code skips the activate). The gate
\* re-read is deferred to FIRE (matching the fresh `!_liveActivation` read at act time).
ExecCommitQueueRecheck ==
  /\ SplitCommitSelfActRecheck
  /\ escPhase = "q_selfact_fire"
  \* SHIPPED: `escTail \notin taskDone` re-read (Pipeline.cs:1152) - POST-PUBLICATION token read.
  \* RESTRUCTURE: the in-lock recheck is DROPPED; the phase advances to FIRE unconditionally and
  \* FIRE decides on the gate + store words alone (no token read here).
  /\ LET i == escTail
         goFire == IF CommitOwnershipRestructure THEN TRUE ELSE i \notin taskDone IN
       /\ IF goFire
            THEN /\ escPhase' = "q_selfact_dofire"
                 /\ escTail'  = escTail
            ELSE /\ escPhase' = "idle"
                 /\ escTail'  = NoItem
       /\ CommitTokenTrip(i)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved,
                 tokenConsumed, commitWasCompleted, cbWiredPrePublish>>

\* FIRE (queue): Pipeline.cs:1137 ActivateHeadItem, a SEPARATE in-lock instruction. Consumes the
\* CAPTURED (recheck) not-done verdict; the gate (GateOpen) is re-read fresh. The completion +
\* off-lock retirement can land between RECHECK and FIRE - the stale verdict then fires on the
\* retired item. On a grant, park "q_selfact_verify" (CommitSelfActVerify) else return to idle.
ExecCommitQueueFire ==
  /\ escPhase = "q_selfact_dofire"
  /\ LET i == escTail IN
       LET doAct == GateOpen IN
       /\ activations' = IF doAct THEN [activations EXCEPT ![i] = @ + 1] ELSE activations
       /\ liveActivation' = IF doAct THEN TRUE ELSE liveActivation
       /\ escPhase' = IF doAct /\ CommitSelfActVerify THEN "q_selfact_verify" ELSE "idle"
       /\ escTail' = IF doAct /\ CommitSelfActVerify THEN escTail ELSE NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved,
                 tokenConsumed, staleTokenRead, commitWasCompleted, cbWiredPrePublish>>

\* VERIFY (queue): still inside the lock, reached only when FIRE granted. Re-read taskDone; if the
\* item is now done the grant is stuck (its retirement ran off-lock between RECHECK and FIRE), so
\* self-clear it - liveActivation' = FALSE, idempotent with the drain's clear. Not done ->
\* passthrough (a legitimately live activation). The spurious activations[i] bump is ACCEPTED.
\* CommitSelfActVerifyOnTaken: the trip keys on task-done AND taken (the producer-side first==last
\* emptiness read - the drain dequeues before completing, so emptiness proves our sole enqueue
\* left the store), the committer-observable the code actually has; trips earlier than the
\* retirement-keyed shape (also fires with the dequeue done but the drain's own clear pending).
ExecCommitQueueVerify ==
  /\ escPhase = "q_selfact_verify"
  /\ LET i == escTail
         \* SHIPPED: reads `i \in taskDone` (Pipeline.cs:1169 verify done-leg) - POST-PUBLICATION
         \* token read, tripped by staleTokenRead when Consumed. RESTRUCTURE: drops the task-done
         \* leg entirely and keys on STORE WORDS - storeCount = 0 (the count re-read) AND
         \* waiters = <<>> (producer-side taken). VerifyCountZeroImpliesRetired /
         \* RestructureVerifyStoreWordsImplyRetired adjudicate whether that two-way conjunction
         \* implies the item is retired without the task-done leg.
         trip == IF CommitOwnershipRestructure
                   THEN storeCount = 0 /\ waiters = <<>>
                   ELSE i \in taskDone /\ (~CommitSelfActVerifyOnTaken \/ waiters = <<>>)
     IN /\ liveActivation' = IF trip THEN FALSE ELSE liveActivation
        /\ staleTokenRead' = IF ConsumableTaskTokens /\ ~CommitOwnershipRestructure /\ i \in tokenConsumed
                             THEN TRUE ELSE staleTokenRead
  /\ escPhase' = "idle"
  /\ escTail' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved,
                 tokenConsumed, commitWasCompleted, cbWiredPrePublish>>

\* ACT, done face: captured verdict was don't-fire (not wasEmpty, or task already done at ARM).
\* The code takes no lock and does not activate; the commit simply returns.
ExecCommitQueueCountActDone ==
  /\ escPhase = "q_selfact_done"
  \* The unconditional callback-wiring read (Pipeline.cs:1185 `if (waiterTask.IsCompleted)`) - THE
  \* convicted thrower - runs at every commit exit, including this non-self-activating one. SHIPPED:
  \* a POST-PUBLICATION token read on `escTail`, tripped when Consumed. RESTRUCTURE: moved
  \* pre-publication (the callback was wired before TryEscalateOrEnqueue), so no read here.
  /\ CommitTokenTrip(escTail)
  /\ escPhase' = "idle"
  /\ escTail' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved,
                 tokenConsumed, commitWasCompleted, cbWiredPrePublish>>

ExecCommitQueueCountLoses ==
  /\ escPhase = "q_enq_noact"
  /\ StoreCommitCount("idle")
  \* The callback-wiring read (Pipeline.cs:1185) on the alreadyActivated commit path.
  /\ CommitTokenTrip(escTail)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 tokenConsumed, commitWasCompleted, cbWiredPrePublish>>

(* ===========================================================================
   Truthful slot commit (SplitSlotFieldOps): TryEscalateOrEnqueue's pre-
   escalation slot path as the THREE steps the code performs - the _hasSlot
   CAS (the slot is CLAIMABLE from here while the fields hold the previous
   tenure's residue: the license bit precedes the data), the field writes,
   then Interlocked.Increment. Backlog #7's commit half; the claim half is
   the SlotClaim* steps below.
   =========================================================================== *)

\* Step 1, executor-wins variant (publish consumption mirrors ExecCommitQueueVisibleWins).
ExecCommitSlotCASWins ==
  /\ SplitSlotFieldOps
  /\ ~TriStateSlotClaim  \* shipped license-before-data ordering; the Write variants are the fix
  /\ hasTail
  /\ hasExecuting  \* Exchange reads own write; TRUE = wins (see ExecCommitTailExecutorWins)
  /\ LET i == tailWaiter IN
       /\ i \notin taskDone  \* sync-success never reaches the store; stays on the fused action
       /\ StoreSlotCommitCAS(i, "slot_cas_act")
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InSlotPending"]
  /\ UNCHANGED <<adv_vars, drainSignal, taskDone, activations, callbackFired, failed,
                 drainer_vars>>

\* Step 1, executor lost (alreadyActivated = true): no publish touch, no activation later.
ExecCommitSlotCASLoses ==
  /\ SplitSlotFieldOps
  /\ ~TriStateSlotClaim
  /\ hasTail
  /\ ~hasExecuting
  /\ LET i == tailWaiter IN
       /\ i \notin taskDone
       /\ StoreSlotCommitCAS(i, "slot_cas_noact")
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InSlotPending"]
  /\ UNCHANGED <<publish_vars, adv_vars, drainSignal, taskDone, activations,
                 callbackFired, failed, drainer_vars>>

\* Step 2: the field writes land; the pair is now truthfully present (InSlot).
ExecCommitSlotFields ==
  /\ StoreSlotCommitFields
  /\ loc' = [loc EXCEPT ![escTail] = "InSlot"]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, taskDone, activations,
                 callbackFired, failed, drainer_vars>>

(* TriStateSlotClaim commit: data, then license. Fields land first (flag-0 fields are
   executor-exclusive: claims never write them under the tri-state protocol), the publish
   makes flag 1 PROVE field completeness, and the count rides the existing slot_f step. *)

\* Step 1, executor-wins variant: the field writes, publish consumption as in the CAS form.
ExecCommitSlotWriteWins ==
  /\ SplitSlotFieldOps
  /\ TriStateSlotClaim
  /\ hasTail
  /\ hasExecuting  \* Exchange reads own write; TRUE = wins (see ExecCommitTailExecutorWins)
  /\ LET i == tailWaiter IN
       /\ i \notin taskDone  \* sync-success never reaches the store; stays on the fused action
       /\ StoreSlotCommitFieldsFirst(i, "slot_w_act")
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InSlotPending"]
  /\ UNCHANGED <<adv_vars, drainSignal, taskDone, activations, callbackFired, failed,
                 drainer_vars>>

ExecCommitSlotWriteLoses ==
  /\ SplitSlotFieldOps
  /\ TriStateSlotClaim
  /\ hasTail
  /\ ~hasExecuting
  /\ LET i == tailWaiter IN
       /\ i \notin taskDone
       /\ StoreSlotCommitFieldsFirst(i, "slot_w_noact")
       /\ tailWaiter' = NoItem
       /\ hasTail' = FALSE
       /\ loc' = [loc EXCEPT ![i] = "InSlotPending"]
  /\ UNCHANGED <<publish_vars, adv_vars, drainSignal, taskDone, activations,
                 callbackFired, failed, drainer_vars>>

\* Step 2: the publish - Volatile.Write(_slotState, 1), no CAS. The pair is now claimable
\* AND complete, in that order.
ExecCommitSlotPublish ==
  /\ StoreSlotCommitPublish
  /\ loc' = [loc EXCEPT ![escTail] = "InSlot"]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, taskDone, activations,
                 callbackFired, failed, drainer_vars>>

\* Step 3: the increment + the wasEmpty activation with its IsCompleted skip (the code
\* reads both AFTER the count lands - CommitWaiter's `if (wasEmpty && !waiterTask
\* .IsCompleted)` - so a task completing mid-commit changes both answers).
ExecCommitSlotCountWins ==
  /\ ~SplitCommitSelfActivate  \* FUSED: the split arm (ARM/ACT) replaces this under the toggle
  /\ escPhase = "slot_f_act"
  /\ LET i == escTail IN
       \* wasEmpty self-activate, split-slot form: GATED under SingleActivationGate.
       /\ activations' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen
                         THEN [activations EXCEPT ![i] = @ + 1]
                         ELSE activations
       /\ activatedSlot' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen THEN i ELSE activatedSlot
       /\ liveActivation' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen
                            THEN TRUE ELSE liveActivation
  /\ StoreCommitCount("idle")
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars>>

(* ===========================================================================
   SplitCommitSelfActivate, slot arm: ExecCommitSlotCountWins as its two instructions.
   Same shape as the queue arm, but the ACT writes activatedSlot and runs ReadTurnMonitor
   (ActivateHeadItem's _activatedItem = item), mirroring ExecCommitSlotCountWins exactly.
   The slot item lives InSlot (counted after ARM); its slot callback + DrainSlotInline can
   claim and RETIRE it in the ARM->ACT window (loc -> Completed, liveActivation cleared off
   the lock), and the ACT then activates the retired slot occupant.
   =========================================================================== *)

\* ARM: the increment + the wasEmpty/IsCompleted capture, verdict onto escPhase, escTail kept.
ExecCommitSlotCountArm ==
  /\ SplitCommitSelfActivate
  /\ escPhase = "slot_f_act"
  /\ LET i == escTail IN
       /\ storeCount' = storeCount + 1
       /\ escPhase' = IF storeCount = 0 /\ i \notin taskDone
                      THEN "slot_selfact_fire" ELSE "slot_selfact_done"
  /\ escTail' = escTail
  /\ escSlotClaimed' = FALSE
  /\ escMoved' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters>>

\* ACT, fire face: gate re-read fresh, IsCompleted the captured verdict (re-read under
\* CommitSelfActRecheck). Writes activatedSlot on activate (ActivateHeadItem), ReadTurnMonitor
\* on the write. On a closed gate / re-checked completed task: no activate, commit returns.
ExecCommitSlotCountActFire ==
  /\ ~SplitCommitSelfActRecheck  \* SPLIT: RECHECK/FIRE(/VERIFY) replace the fused re-read+activate
  /\ escPhase = "slot_selfact_fire"
  /\ LET i == escTail IN
       LET doAct == GateOpen /\ (~CommitSelfActRecheck \/ i \notin taskDone) IN
       /\ activations' = IF doAct THEN [activations EXCEPT ![i] = @ + 1] ELSE activations
       /\ activatedSlot' = IF doAct THEN i ELSE activatedSlot
       /\ liveActivation' = IF doAct THEN TRUE ELSE liveActivation
  /\ escPhase' = "idle"
  /\ escTail' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp

(* ===========================================================================
   SplitCommitSelfActRecheck, slot arm: same three-instruction split as the queue arm, but FIRE
   writes activatedSlot (ActivateHeadItem's _activatedItem = item) and runs ReadTurnMonitor, and
   VERIFY restores the activated slot on a self-clear. One _activationLock hold (Pipeline.cs
   1127-1144); the slot occupant can be claimed + retired by DrainSlotInline off-lock in the
   RECHECK->FIRE window.
   =========================================================================== *)

\* RECHECK (slot): re-read taskDone under the lock; park "slot_selfact_dofire" when not done,
\* else go idle. No activatedSlot write (the gate + activate are FIRE's).
ExecCommitSlotRecheck ==
  /\ SplitCommitSelfActRecheck
  /\ escPhase = "slot_selfact_fire"
  /\ IF escTail \notin taskDone
       THEN /\ escPhase' = "slot_selfact_dofire"
            /\ escTail'  = escTail
       ELSE /\ escPhase' = "idle"
            /\ escTail'  = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved>>

\* FIRE (slot): the activate; gate re-read fresh; writes activatedSlot + ReadTurnMonitor, mirroring
\* the fused slot ActFire. On a grant, park "slot_selfact_verify" (CommitSelfActVerify) else idle.
ExecCommitSlotFire ==
  /\ escPhase = "slot_selfact_dofire"
  /\ LET i == escTail IN
       LET doAct == GateOpen IN
       /\ activations' = IF doAct THEN [activations EXCEPT ![i] = @ + 1] ELSE activations
       /\ activatedSlot' = IF doAct THEN i ELSE activatedSlot
       /\ liveActivation' = IF doAct THEN TRUE ELSE liveActivation
       /\ escPhase' = IF doAct /\ CommitSelfActVerify THEN "slot_selfact_verify" ELSE "idle"
       /\ escTail' = IF doAct /\ CommitSelfActVerify THEN escTail ELSE NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp

\* VERIFY (slot): still in the lock, reached only when FIRE granted. Re-read taskDone and release
\* the just-planted turn (liveActivation' = FALSE) when done - the item's completion callback drains
\* it, so it must not keep the reader turn (RECHECK's own skip-completed logic). The activated-slot
\* RESTORE, however, must fire ONLY when the item was actually RETIRED (loc = "Completed") - THAT is
\* the stuck-grant case (no future retirement will clear it), and the restore then mirrors exactly
\* the drain's clear (DepthZeroClear, guarded on the slot still pointing at the item). When the item
\* is task-done but STILL RESIDENT (loc = "InSlot"), the grant is transient-legit (the drain will
\* retire it and clear the slot the normal way) - nulling activatedSlot here would STOMP a live slot
\* occupant (NoNullWhileLive: the _activatedItem NRE). Model finding: re-reading taskDone alone
\* over-fires the slot restore; the restore must key on retirement, not mere completion.
\* CommitSelfActVerifyOnTaken: retirement has no committer-observable in code, so BOTH the turn
\* release and the restore key on task-done AND taken (the Volatile slot-word snapshot no longer
\* holding our item - slotItem # i); trips earlier (also in the claimed/dequeued-but-not-yet-
\* completed drain window, loc = "Draining"). TLC adjudicates the early trip's soundness.
ExecCommitSlotVerify ==
  /\ escPhase = "slot_selfact_verify"
  /\ LET i == escTail
         taken   == slotItem # i  \* the slot word + identity snapshot: our item left the store
         trip    == i \in taskDone /\ (~CommitSelfActVerifyOnTaken \/ taken)
         restore == i \in taskDone /\ activatedSlot = i
                    /\ (IF CommitSelfActVerifyOnTaken THEN taken ELSE loc[i] = "Completed")
     IN /\ liveActivation' = IF trip THEN FALSE ELSE liveActivation
        /\ activatedSlot'  = IF restore THEN DepthZeroClear({i}) ELSE activatedSlot
  /\ escPhase' = "idle"
  /\ escTail' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved>>
  /\ ReadTurnMonitor  \* self-clear writes activatedSlot; witness the read-turn stomp

\* ACT, done face: captured don't-fire. No lock, no activation; the commit returns.
ExecCommitSlotCountActDone ==
  /\ escPhase = "slot_selfact_done"
  /\ escPhase' = "idle"
  /\ escTail' = NoItem
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars,
                 hasSlot, slotItem, escalated, waiters, escSlotClaimed, escMoved>>

ExecCommitSlotCountLoses ==
  /\ escPhase = "slot_f_noact"
  /\ StoreCommitCount("idle")
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 activations, callbackFired, failed, drainer_vars>>

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
  /\ UNCHANGED <<adv_vars, drainSignal,
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
  /\ UNCHANGED <<publish_vars, adv_vars, drainSignal,
                 taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 2: CASSlot. Atomic Interlocked.Exchange(_hasSlot, 0). The race outcome with the
\* slot callback's pre-publish CAS is captured by reading hasSlot here: if the callback
\* drained the slot before the executor reached publish, hasSlot is FALSE and escSlotClaimed
\* records the loss; otherwise the executor wins and the slot fields are still populated.
ExecEscalationCASSlot ==
  /\ ~TriStateSlotClaim  \* Exchange semantics: zeroes ANY state - under the tri-state word
                         \* that would steal a consuming drainer's release (see the Tri variant)
  /\ StoreEscalateClaimSlot
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal,
                 loc, taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 2 under TriStateSlotClaim: CAS(1 -> 0), NOT Exchange - the escalation may only take
\* a QUIESCENT occupied slot. A consuming word (2: the drainer between its own CAS 1 -> 2 and
\* its release - modeled as hasSlot /\ drainPhase = "cl_own") reads as not-moved: the occupant
\* is completing RIGHT NOW under the advancer latch and exits the pipeline without ever
\* entering the queue, so FIFO is preserved and the drainer's release (2 -> 0) proceeds
\* untouched. The Exchange here was the corner that strands a live occupant: zeroing state 2
\* leaves the un-claimable pair fieldside with no queue route. lives in Pipeline.tla (not the
\* store module) because the owned state rides the drainer's PC.
ExecEscalationCASSlotTri ==
  /\ TriStateSlotClaim
  /\ escPhase = "publish_done"
  /\ LET quiescent == hasSlot /\ drainPhase # "cl_own" IN
       /\ escSlotClaimed' = quiescent
       /\ hasSlot' = (IF quiescent THEN FALSE ELSE hasSlot)
  /\ escPhase' = "cas_done"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, slotItem, escalated, waiters,
                 storeCount, escTail, escMoved,
                 loc, taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 3: MoveSlot. If the CAS won, append the slot item to waiters head and clear the slot
\* fields. The slot item's loc transitions from InSlot to InWaiters (its callback was wired
\* at slot-commit time and stays wired through the move).
ExecEscalationMoveSlot ==
  /\ StoreEscalateMove
  /\ loc' = IF escSlotClaimed THEN [loc EXCEPT ![slotItem] = "InWaiters"] ELSE loc
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal,
                 taskDone, activations, callbackFired, failed, drainer_vars>>

\* Step 4: EnqueueNew. Append escTail to waiters tail; increment count; loc → InWaitersPending.
\* Activations follow the same rule as the original atomic transition (storeCount = 0 was
\* the wasEmpty trigger). With the chain fix, a first escalation that moved the slot
\* (slotWasMoved) proceeds to the compensation check (CommitWaiter's post-escalation
\* `slotWasMoved && Exchange(_pendingHeadActivation, false)`); otherwise back to "idle".
ExecEscalationEnqueueNew ==
  /\ ~SplitCountCommit  \* fused fiction; the EnqueueTail/CommitCount pair is the truth
  /\ LET i == escTail IN
       /\ StoreEscalateEnqueue(IF SlotChainActivation /\ escSlotClaimed THEN "compensate" ELSE "idle")
       /\ loc' = [loc EXCEPT ![i] = "InWaitersPending"]
       \* wasEmpty self-activate, first-escalation form: GATED under SingleActivationGate.
       /\ activations' = IF storeCount = 0 /\ GateOpen
                         THEN [activations EXCEPT ![i] = @ + 1]
                         ELSE activations
       /\ liveActivation' = IF storeCount = 0 /\ GateOpen THEN TRUE ELSE liveActivation
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal,
                 taskDone, callbackFired, failed, drainer_vars>>

\* CommitWaiter's post-escalation nudge as the code has it: a ONE-SHOT check at the tail of
\* the first-escalation commit (split path; escPhase "nudge_check" routed from the count
\* step / compensation). The legacy standing ExecPostEscalationNudge (gated ~SplitCountCommit)
\* approximates this for the fused path - but a standing nudge is an unfaithful rescue: it
\* re-fires whenever the signal is set, where the code's check runs once and never returns
\* (the mid-escalation bail of SlotCallbackBailsDuringEscalation is exactly what it exists
\* to catch - and the only thing it can catch).
ExecEscalationNudgeStep ==
  /\ escPhase = "nudge_check"
  /\ IF NudgeEnabled /\ escSlotClaimed /\ drainSignal /\ ~advancingVisible
       THEN /\ advancing' = TRUE
            /\ advancingVisible' = TRUE
            /\ drainerActive' = TRUE
            /\ drainSignal' = FALSE  \* DrainReadyWaiters' do-top clear
            /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
            /\ qDrainedAny' = (IF PassOnceDrain THEN FALSE ELSE qDrainedAny)
            /\ advancingPending' = advancingPending
       ELSE /\ advancing' = advancing
            /\ advancingVisible' = advancingVisible
            /\ drainerActive' = drainerActive
            /\ drainSignal' = drainSignal
            /\ qDrainPhase' = qDrainPhase
            /\ qDrainedAny' = qDrainedAny
            \* PendingWordLatch: the nudge's TryAcquire is the same word primitive as every
            \* other acquirer - attempted (signal seen) and lost (holder present) deposits
            \* the obligation in the word. THE Contract v5 round-1 counterexample: a nudge
            \* bailing against the reclaim's transient rmiss hold, no deposit, strand.
            /\ advancingPending' = (IF PendingWordLatch /\ NudgeEnabled /\ escSlotClaimed
                                       /\ drainSignal /\ advancingVisible
                                    THEN TRUE ELSE advancingPending)
  /\ escPhase' = "idle"
  /\ escSlotClaimed' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, hasSlot, slotItem, escalated, waiters,
                 escTail, escMoved, loc, taskDone, activations, callbackFired, failed>>

\* Split Step 4a (SplitCountCommit): the tail append alone. loc moves to InWaitersPending
\* here (the entry is in waiters - WaitersConsistent); the count follows in 4b. Same
\* count-skew window as the steady-state pair.
ExecEscalationEnqueueTail ==
  /\ SplitCountCommit
  /\ StoreEscalateEnqueueTail
  /\ loc' = [loc EXCEPT ![escTail] = "InWaitersPending"]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal,
                 taskDone, activations, callbackFired, failed, drainer_vars>>

\* Split Step 4b: the increment, the wasEmpty activation (with the truthful IsCompleted
\* skip), and the compensate/idle routing the fused Step 4 performed.
ExecEscalationCommitCount ==
  /\ escPhase = "esc_enqueued"
  /\ LET i == escTail IN
       \* wasEmpty self-activate, split first-escalation form: GATED under SingleActivationGate.
       /\ activations' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen
                         THEN [activations EXCEPT ![i] = @ + 1]
                         ELSE activations
       /\ activatedSlot' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen THEN i ELSE activatedSlot
       /\ liveActivation' = IF storeCount = 0 /\ i \notin taskDone /\ GateOpen
                            THEN TRUE ELSE liveActivation
  /\ StoreCommitCount(IF SlotChainActivation /\ escSlotClaimed THEN "compensate" ELSE "nudge_check")
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal, loc, taskDone,
                 callbackFired, failed, drainer_vars>>

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
           /\ activatedSlot' = IF target \in taskDone THEN activatedSlot ELSE target
           /\ liveActivation' = IF target \in taskDone THEN liveActivation ELSE TRUE
         ELSE
           /\ pendingHeadActivation' = pendingHeadActivation
           /\ activations' = activations
           /\ activatedSlot' = activatedSlot
           /\ liveActivation' = liveActivation
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, drainSignal,
                 loc, taskDone, callbackFired, failed, drainer_vars,
                 drainPhase, drainItem, drainRemaining, tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

(* Inline-callback race in CommitWaiter queue path: at the post-Enqueue check, if task is
   already done the executor fires the wired callback inline; otherwise it registers it for
   later. Modeled by leaving the item in InWaitersPending and resolving via the three
   transitions below. (Slot path is handled by SlotCallback_ transitions directly, no pending
   window.) *)

ExecutorRegistersCallback ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i # escTail  \* registration runs after the commit's increment (split-commit phases)
    /\ i \notin taskDone
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

ExecutorInlineCallbackBecomesAdvancer ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i # escTail  \* the inline callback() call is post-increment (split-commit phases)
    /\ i \in taskDone
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ callbackFired' = callbackFired \cup {i}
    \* Set-then-acquire-then-pass-top-clear, fused: the winner's own signal (and any bailed
    \* sibling's) is consumed by the pass it is about to run exhaustively.
    /\ drainSignal' = FALSE
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    /\ drainerActive' = TRUE
    /\ UNCHANGED <<publish_vars, tail_vars, slot_vars, waiters,
                   taskDone, activations, storeCount, esc_vars, failed>>

ExecutorInlineCallbackBailsOut ==
  \E i \in Item :
    /\ loc[i] = "InWaitersPending"
    /\ i # escTail  \* post-increment, see ExecutorInlineCallbackBecomesAdvancer
    /\ i \in taskDone
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ loc' = [loc EXCEPT ![i] = "InWaiters"]
    /\ callbackFired' = callbackFired \cup {i}
    /\ drainSignal' = TRUE
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                   taskDone, activations, storeCount, esc_vars, drainer_vars, failed>>

(* ===========================================================================
   Callback (OnWaiterTaskCompleted).

   Slot callbacks (item still in InSlot when task completes) have two outcomes:
     - Drain inline: slot CAS wins, processes item directly.
     - Bail out: advancer already held, just set drainSignal.

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
  /\ ~SplitCallbackOps  \* fused set+acquire+empty-pass; CallbackSetSignal (slot arm) +
                        \* CallbackAcquireWin (escalated route) are the un-fusing
  /\ \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InSlot"
    /\ slotItem = i
    /\ escalated  \* queue published, so callback's IsEscalated check returns TRUE
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ callbackFired' = callbackFired \cup {i}
    /\ drainSignal' = TRUE
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
\* Signal accounting: the callback's set and the pass-start clear happen in the same real
\* method, so the fused action lands drainSignal = FALSE (the flag retype's equivalent of
\* the old counter's bump+decrement net zero).
\*
\* Consume timing: the consume (tenure release) is always bundled with Complete, mirroring
\* the shipped path. The clash window the old toggle exposed is structurally closed by
\* CompleteBeforeCount = TRUE (Complete runs before the count republish), so no concurrent
\* SourceYieldInline can observe a held tenure.
SlotDrainClaim ==
  /\ ~SplitSlotFieldOps  \* fused fiction; the SlotDrainClaimEntry + SlotClaim* steps are the truth
  /\ \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InSlot"
    \* Under SplitCallbackOps the set already ran (CallbackSetSignal's slot arm) and this
    \* action is acquire+claim only; fused, it covers set+acquire+claim in one step.
    /\ IF SplitCallbackOps THEN i \in callbackSignaled ELSE i \notin callbackFired
    /\ ~advancingVisible
    /\ hasSlot
    /\ slotItem = i
    /\ ~escalated
    /\ drainPhase = "idle"
    /\ callbackFired' = callbackFired \cup {i}
    /\ callbackSignaled' = callbackSignaled \ {i}
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    /\ drainerActive' = TRUE
    /\ StoreClaimSlotForDrain
    /\ loc' = [loc EXCEPT ![i] = "Draining"]
    /\ drainPhase' = "claimed"
    /\ drainItem' = i
    /\ tenure' = tenure
    \* Set-then-acquire-then-pass-top-clear, fused (slot pass start). Replaces the old
    \* counter's "bump + decrement = net zero" accounting. Under SplitCallbackOps the set
    \* happened earlier; the pass-start clear lands the same FALSE.
    /\ drainSignal' = FALSE
    /\ UNCHANGED <<publish_vars, tail_vars, escalated, waiters, storeCount,
                   taskDone, activations, esc_vars, failed, drainRemaining, pendingHeadActivation,
                   assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

(* ===========================================================================
   Truthful slot claim (SplitSlotFieldOps): the two entry routes (callback fire,
   stale-token reclaim) converge on TryClaimSlotForDrain's three real steps -
   Exchange, field read, field clear. The READ takes slotItem AS IT IS: the
   committed pair, the previous claim's NoItem, or a successor commit's writes
   racing in. NoDefaultSlotClaim pins the unlicensed default claim.
   =========================================================================== *)

\* Callback-path entry: the slot occupant's callback fires (one-shot consumed) and wins
\* the latch. Licensed for its OWN item by construction (fields -> count -> wiring ->
\* fire, all program-ordered on the executor; i # escTail excludes mid-commit fire) -
\* but the claim steps it routes to read the fields truthfully, so a successor commit
\* landing mid-claim is still in play.
SlotDrainClaimEntry ==
  /\ SplitSlotFieldOps
  /\ \E i \in Item :
       /\ i \in taskDone
       /\ loc[i] = "InSlot"
       \* Under SplitCallbackOps the set already ran (CallbackSetSignal's slot arm); this
       \* entry is the acquire onward. Fused, it covers set+acquire in one step.
       /\ IF SplitCallbackOps THEN i \in callbackSignaled ELSE i \notin callbackFired
       /\ i # escTail  \* wiring runs after the commit's count step
       /\ ~advancingVisible
       /\ ~escalated
       /\ drainPhase = "idle"
       /\ callbackFired' = callbackFired \cup {i}
       /\ callbackSignaled' = callbackSignaled \ {i}
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainerActive' = TRUE
  /\ drainSignal' = TRUE  \* set-then-acquire: the callback's plain store precedes the win;
                          \* the token stays visible until the post-claim clear (under
                          \* SplitCallbackOps the earlier set already landed this TRUE)
  /\ drainPhase' = "cl_acq"
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, esc_vars, failed, drainItem, drainRemaining,
                 pendingHeadActivation, tenure, assertFailed, nullActivation,
                 qDrainPhase, qDrainedAny, advancingPending, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Reclaim-path entry, the TRUTHFUL gate: drainSignal + the latch win. No identity, no
\* taskDone, no callbackFired knowledge - the fused SlotDrainerReclaim's
\* `slotItem = i /\ i \in taskDone /\ i \in callbackFired` license is fiction the code
\* never had. A stale token routes this entry into the claim steps against whatever the
\* slot holds, INCLUDING a successor commit between its CAS and its field writes.
SlotDrainerReclaimEntry ==
  /\ SplitSlotFieldOps
  /\ SlotReclaimEnabled
  /\ drainerActive
  /\ ~advancingVisible
  /\ drainSignal  \* read, NOT consumed: the code's clear sits after the claim succeeds
  /\ ~escalated
  /\ drainPhase = "idle"
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainPhase' = "cl_acq"
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Claim step 1: the Exchange. The flag transfers; the fields are whatever they are.
SlotClaimExchange ==
  /\ ~TriStateSlotClaim  \* shipped take-first protocol; the peek-gated steps are the fix
  /\ drainPhase = "cl_acq"
  /\ hasSlot
  /\ StoreClaimSlotExchange
  /\ drainPhase' = "cl_xchg"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, waiters,
                 loc, taskDone, activations, callbackFired, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Claim miss: the slot is empty (escalation or a prior claim owned it). Code: IsEscalated
\* reroutes into DrainReadyWaiters holding the latch (do-top clear and the queue drain's
\* own tail handles exit); otherwise the claim-fail arm RETURNS - release via
\* SlotClaimFailRelease ("cl_exit"), with NO reclaim tail check on this arm.
SlotClaimMiss ==
  /\ drainPhase = "cl_acq"
  /\ ~hasSlot
  /\ IF escalated
       THEN /\ qDrainPhase' = "pass"
            /\ drainSignal' = FALSE
            /\ qDrainedAny' = FALSE
            /\ drainPhase' = "idle"
       ELSE /\ drainPhase' = "cl_exit"
            /\ UNCHANGED <<qDrainPhase, drainSignal, qDrainedAny>>
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

(* TriStateSlotClaim claim: peek under the stable flag, claim only the completed, consume
   under exclusive ownership. The 1 -> 0 -> 1 cycle that would invalidate the peek is
   impossible: the only droppers are this latch-serialized claimer and the escalation,
   which drops permanently (post-escalation the slot never refills). *)

\* The peek, live verdict: the occupant's task is still running. Bail with NO state change -
\* no claim, no un-claim, no GetResult block. The occupant's wired callback drains it on
\* completion; the stale token that drove this probe costs exactly this one pass.
SlotClaimPeekLive ==
  /\ TriStateSlotClaim
  /\ drainPhase = "cl_acq"
  /\ hasSlot
  /\ slotItem \notin taskDone
  /\ drainPhase' = "cl_exit"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The peek, completed verdict: proceed toward the claim CAS. taskDone is monotonic, so the
\* verdict cannot go stale; the FLAG can (escalation takes the pair) - the own-win/lose pair
\* below is the CAS.
SlotClaimPeekDone ==
  /\ TriStateSlotClaim
  /\ drainPhase = "cl_acq"
  /\ hasSlot
  /\ slotItem \in taskDone
  /\ drainPhase' = "cl_peeked"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The claim CAS (1 -> 2) won: the consuming state begins. The word stays non-zero (hasSlot
\* TRUE + this PC = state 2), so commits route to escalation and the escalation's
\* quiescent-only CAS skips us.
SlotClaimOwnWin ==
  /\ drainPhase = "cl_peeked"
  /\ hasSlot
  /\ drainPhase' = "cl_own"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The claim CAS lost: the escalation took the pair between our peek and our CAS (the only
\* possible taker). The occupant is, or is about to be, the queue head - reroute into the
\* queue drain holding the latch, exactly the claim-fail IsEscalated arm.
SlotClaimOwnLose ==
  /\ drainPhase = "cl_peeked"
  /\ ~hasSlot
  /\ escalated  \* the only dropper besides ourselves; TLC checks this guard is total
  /\ qDrainPhase' = "pass"
  /\ drainSignal' = FALSE
  /\ qDrainedAny' = FALSE
  /\ drainPhase' = "idle"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The consume: read + clear + release (2 -> 0) + the post-claim signal clear. Fused as one
\* step because state 2 admits no interfering field access BY GUARD - commits need word 0,
\* the escalation skips 2, claimers are latch-serialized - and those guards are exact word
\* tests, so the fusion hides nothing observable.
SlotClaimConsume ==
  /\ drainPhase = "cl_own"
  /\ drainItem' = slotItem
  /\ slotItem' = NoItem
  /\ hasSlot' = FALSE
  /\ drainSignal' = FALSE  \* Interlocked.Exchange(_drainSignal, false) after the claim wins
  /\ loc' = [loc EXCEPT ![slotItem] = "Draining"]
  /\ tenure' = tenure
  /\ drainPhase' = "claimed"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, escalated, waiters,
                 taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainRemaining, pendingHeadActivation,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

(* The claim-fail exit: the code's fail arm releases the latch and RETURNS - no reclaim
   tail check (that belongs to the successful-drain path). The v5 pending arm applies: a
   deposit consumed at this release obligates one re-claim attempt (the code's
   `if (serveDeposit && TryAcquireOrFlagPending()) continue;`). *)

SlotClaimFailRelease ==
  /\ drainPhase = "cl_exit"
  /\ advancing
  /\ qDrainPhase = "none"
  /\ IF PendingWordLatch /\ advancingPending
       THEN /\ IF WeakAdvancerRelease
                 THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
                 ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
            /\ advancingPending' = FALSE
            /\ drainPhase' = "cl_pendReacq"
            /\ drainerActive' = drainerActive
       ELSE /\ IF WeakAdvancerRelease
                 THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
                 ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
            /\ advancingPending' = advancingPending
            /\ drainPhase' = "idle"
            /\ drainerActive' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

SlotClaimPendReacqWin ==
  /\ drainPhase = "cl_pendReacq"
  /\ ~advancingVisible
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainPhase' = "cl_acq"  \* the code's `continue`: back to the claim
  /\ drainerActive' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

SlotClaimPendReacqLose ==
  /\ drainPhase = "cl_pendReacq"
  /\ advancingVisible
  /\ drainPhase' = "idle"
  /\ drainerActive' = FALSE
  \* The re-acquire is the uniform word primitive: losing re-deposits on the winner.
  /\ advancingPending' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Claim step 2: the field read, TRUTHFUL. drainItem takes slotItem as it is at this
\* instant - NoItem when the claim won against a commit's CAS before its field writes
\* (the previous tenure's clear still in the cell), or the successor's identity when its
\* writes landed mid-claim (the hijack: the drainer processes the successor, the item it
\* was woken for strands).
SlotClaimRead ==
  /\ drainPhase = "cl_xchg"
  /\ drainItem' = slotItem
  /\ drainPhase' = "cl_read"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Claim step 3: the field clear (unconditional, like the code - if a successor's CAS
\* re-occupied the flag and its writes landed, this WIPES them under _hasSlot = 1), plus
\* the post-claim pass-top signal clear and the drain bookkeeping. Gated on a non-default
\* read so the model stays well-formed; the default claim halts at "cl_read" where
\* NoDefaultSlotClaim pins it.
SlotClaimClear ==
  /\ drainPhase = "cl_read"
  /\ drainItem # NoItem
  /\ StoreClaimSlotClear
  /\ drainSignal' = FALSE  \* Interlocked.Exchange(_drainSignal, false) after the claim wins
  /\ loc' = [loc EXCEPT ![drainItem] = "Draining"]
  /\ tenure' = tenure
  /\ drainPhase' = "claimed"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, hasSlot, escalated, waiters,
                 taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Pipeline.cs:1148-1163 audit reorder, ReorderFix shipped June 2026: CompleteWaiterDeferred
\* fires BEFORE DecrementCount, mirroring the recovery-fault path's own audit reorder. Active
\* only under CompleteBeforeCount=TRUE; transitions drainPhase "claimed" -> "completed" so
\* SlotDrainCount picks up from there. Does loc[i] -> "Completed", tenure release, and the
\* unconditional activatedSlot clear that the code's `_activatedItem = default!` performs.
\* Because the clear lands BEFORE the count drop publishes the freed position to the inline-
\* activation gate, no concurrent activator can have written activatedSlot = NEW yet; the
\* clear is therefore stomp-free by construction.
SlotDrainCompleteClear ==
  /\ CompleteBeforeCount
  /\ drainPhase = "claimed"
  /\ drainItem \in taskDone
  /\ \A i \in Item : loc[i] # "RecoveringInline"
  /\ LET i == drainItem IN
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
       \* No stomp check here: SlotDrainCompleteClear runs BEFORE SlotDrainCount under the
       \* shipped ReorderFix, so storeCount has not been decremented yet and no concurrent
       \* activator can have fired since the drain started. The clear is structurally safe.
       \* The separate "Complete clear stomps an already-existing non-drainItem activation"
       \* class - reachable when an item completes without ever being activated (the slot
       \* still points at an older live item) - is orthogonal and out of scope here.
       /\ slotStomped' = slotStomped
       /\ activatedSlot' = DepthZeroClear({drainItem})  \* depth-0 clear; only nulls when this empties the pipeline
       /\ liveActivation' = FALSE  \* CompleteWaiterDeferred releases the turn at retirement
  /\ drainPhase' = "completed"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters, drainSignal, storeCount,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, assertFailed,
                 nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot (DepthZeroClear); witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Slot drain, step 2 of 3: DecrementCount - the position republish. The executor's Count==0
\* inline-activation gate (SourceYieldInline's storeCount = 0) can pass the instant this
\* lands. The shipped code asserts the post-decrement count is 0; a commit that landed in
\* the freed slot during the claimed window makes it 1 - assertFailed records the firing.
\*
\* Phase precondition is gated on CompleteBeforeCount:
\*   TRUE  (shipped fix): SlotDrainCompleteClear has already fired at "claimed" and the
\*         phase advanced to "completed"; Count runs from there.
\*   FALSE (pre-fix): Count runs first at "claimed", and SlotDrainComplete* runs later
\*         at "counted" - opening the race window where a concurrent ActivateHeadItem
\*         (gated on storeCount = 0) can write activatedSlot = NEW before the clear stomps it.
SlotDrainCount ==
  /\ drainPhase = (IF CompleteBeforeCount THEN "completed" ELSE "claimed")
  \* The drain's GetResult blocks until the claimed task completes. Vacuous under the
  \* fused claim (its guard required taskDone); load-bearing under SplitSlotFieldOps,
  \* where a hijack claim can hold a successor whose task is still running.
  /\ drainItem \in taskDone
  \* A drain-side recovery park (SlotHeadRecovers) suspends the drain HERE: the thread's PC
  \* is inside RecoverWaiter until the park resolves (refuse, or substitute completion +
  \* binding discharge), and only then does the decrement run - the code's advance = true /
  \* AdvanceAndDrainRecovery rejoin. Mid-recovery firing would be another thread inside the
  \* held advancer latch - code-impossible.
  /\ \A i \in Item : loc[i] # "RecoveringInline"
  /\ StoreDecrementCount
  /\ assertFailed' = IF storeCount - 1 # 0 THEN TRUE ELSE assertFailed
  \* The atomic decrement's return value, captured for the SlotDrainComplete* branch choice
  \* (the queue drain's `var count = _waiters.DecrementCount()` partition; shipped
  \* DrainSlotInline discards it - see SlotChainActivation).
  /\ drainRemaining' = storeCount - 1
  /\ drainPhase' = "counted"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters, drainSignal,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, pendingHeadActivation, tenure, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

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
       \* Under CompleteBeforeCount=TRUE the Complete half (loc + tenure release + slot
       \* clear) already ran in SlotDrainCompleteClear at the "claimed" phase; this action
       \* is activation-only. Under FALSE it does both halves atomically (the pre-fix
       \* code's shape) - and that unconditional clear is where the stomp lives.
       /\ loc' = IF CompleteBeforeCount THEN loc ELSE [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF CompleteBeforeCount THEN tenure
                    ELSE (IF tenure = i THEN NoItem ELSE tenure)
       /\ IF hasExecutingVisible /\ executingItemVisible # NoItem
            THEN
              /\ activations' = IF storeCount = 0
                                THEN [activations EXCEPT ![executingItemVisible] = @ + 1]
                                ELSE activations
              /\ slotStomped' = IF ~CompleteBeforeCount /\ storeCount # 0
                                   /\ activatedSlot # NoItem /\ activatedSlot # i
                                   /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                                THEN TRUE ELSE slotStomped
              /\ activatedSlot' = IF storeCount = 0 THEN executingItemVisible
                                  ELSE (IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem)
              \* Code order: the retirement clear ran (in Clear under the fix, here pre-fix),
              \* then the C-path activation re-grants - a step that activates ends TRUE.
              /\ liveActivation' = IF storeCount = 0 THEN TRUE
                                   ELSE (IF CompleteBeforeCount THEN liveActivation ELSE FALSE)
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
            ELSE
              /\ activations' = activations
              \* Under FALSE this is the bug-shaped unconditional clear matching
              \* `_activatedItem = default!`; stomps if it lands on a successor's publish.
              /\ slotStomped' = IF ~CompleteBeforeCount
                                   /\ activatedSlot # NoItem /\ activatedSlot # i
                                   /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                                THEN TRUE ELSE slotStomped
              /\ activatedSlot' = IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem
              /\ liveActivation' = IF CompleteBeforeCount THEN liveActivation ELSE FALSE
              /\ UNCHANGED publish_vars
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

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
       \* Under CompleteBeforeCount=TRUE the loc/tenure/clear half is already done; this
       \* action is activation-only. Under FALSE both halves run here.
       /\ loc' = IF CompleteBeforeCount THEN loc ELSE [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF CompleteBeforeCount THEN tenure
                    ELSE (IF tenure = i THEN NoItem ELSE tenure)
       \* The code's IsCompleted skip (mirrors CommitWaiter's guard): a waiter whose task
       \* already settled - possible without activation via CompleteTaskUnactivated - is
       \* never activated; its callback fired and bailed against our held latch, and the
       \* post-release reclaim (SlotDrainerReclaim) drains it.
       /\ activations' = IF target \in taskDone
                         THEN activations
                         ELSE [activations EXCEPT ![target] = @ + 1]
       /\ slotStomped' = IF ~CompleteBeforeCount /\ target \in taskDone
                            /\ activatedSlot # NoItem /\ activatedSlot # i
                            /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                         THEN TRUE ELSE slotStomped
       /\ activatedSlot' = IF target \in taskDone
                           THEN (IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem)
                           ELSE target
       \* Retire-then-activate: the chain activation re-grants the turn; the IsCompleted-skip
       \* arm only completes (clear ran in Clear under the fix, here pre-fix).
       /\ liveActivation' = IF target \in taskDone
                            THEN (IF CompleteBeforeCount THEN liveActivation ELSE FALSE)
                            ELSE TRUE
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

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
       /\ loc' = IF CompleteBeforeCount THEN loc ELSE [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF CompleteBeforeCount THEN tenure
                    ELSE (IF tenure = i THEN NoItem ELSE tenure)
       /\ slotStomped' = IF ~CompleteBeforeCount
                            /\ activatedSlot # NoItem /\ activatedSlot # i
                            /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                         THEN TRUE ELSE slotStomped
       /\ activatedSlot' = IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem
       /\ liveActivation' = IF CompleteBeforeCount THEN liveActivation ELSE FALSE
  /\ pendingHeadActivation' = TRUE
  /\ drainPhase' = "handoff"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Handoff resolution, drainer reclaims: the one-shot re-peek found the head visible (the
\* escalation's move landed), so the drainer claims its own obligation back (Exchange wins)
\* and activates. Exactly-once with the escalator's compensation check is the flag claim.
SlotDrainHandoffReclaim ==
  /\ drainPhase = "handoff"
  /\ pendingHeadActivation
  /\ Len(waiters) > 0
  /\ LET target == Head(waiters) IN
       /\ activations' = IF target \in taskDone
                         THEN activations
                         ELSE [activations EXCEPT ![target] = @ + 1]
       /\ activatedSlot' = IF target \in taskDone THEN activatedSlot ELSE target
       /\ liveActivation' = IF target \in taskDone THEN liveActivation ELSE TRUE
  /\ pendingHeadActivation' = FALSE
  /\ drainPhase' = "idle"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

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
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

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
  \* Under the FUSED pre-check (CPathPendingPreCheck /\ ~SplitCPathPreCheck) this action IS
  \* the pre-check: it reads hasExecutingVisible atomically with the activate/skip decision,
  \* exactly the fused semantics - no separate action needed. Only the SPLIT pre-check
  \* intercepts (the SlotCPathPre* actions below); this guard hands the "counted" <=0 case
  \* to them. Both toggles FALSE => vacuously TRUE (every prior config unaffected).
  /\ ~(CPathPendingPreCheck /\ SplitCPathPreCheck)
  /\ drainPhase = "counted"
  \* <= 0 under the skew fix, mirroring DrainSlotInline's shipped `count <= 0` partition
  \* and AdvancerCPath: a -1 means the consumed pair was the in-flight commit (already
  \* completed), leaving the deferred publish as the only live responsibility.
  /\ (IF SkewTolerantPartition THEN drainRemaining <= 0 ELSE drainRemaining = 0)
  /\ LET i == drainItem IN
       \* Under CompleteBeforeCount=TRUE the loc/tenure/clear half is already done; this
       \* action is activation-only. Under FALSE both halves run here.
       /\ loc' = IF CompleteBeforeCount THEN loc ELSE [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF CompleteBeforeCount THEN tenure
                    ELSE (IF tenure = i THEN NoItem ELSE tenure)
       /\ IF (IF SkewTolerantPartition THEN storeCount <= 0 ELSE storeCount = 0)
             /\ hasExecutingVisible /\ executingItemVisible # NoItem
            THEN
              /\ activations' = [activations EXCEPT ![executingItemVisible] = @ + 1]
              /\ activatedSlot' = executingItemVisible  \* ActivateHeadItem publishes the deferred-published item
              \* The SLOT C-path is deliberately NOT gated on liveActivation - the code's
              \* DrainSlotInline C-path carries no _liveActivation check (only the QUEUE
              \* C-path in DrainReadyWaiters does). Activation re-grants the turn.
              /\ liveActivation' = TRUE
              /\ slotStomped' = slotStomped  \* the deferred-publish activation overrides any racing write
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
            ELSE
              /\ activations' = activations
              \* Under FALSE this is the bug-shaped unconditional clear matching
              \* `_activatedItem = default!`; stomps if it lands on a successor's publish.
              /\ slotStomped' = IF ~CompleteBeforeCount
                                   /\ activatedSlot # NoItem /\ activatedSlot # i
                                   /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                                THEN TRUE ELSE slotStomped
              /\ activatedSlot' = IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem
              /\ liveActivation' = IF CompleteBeforeCount THEN liveActivation ELSE FALSE
              /\ UNCHANGED publish_vars
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

(* ===========================================================================
   SPLIT slot C-path pending pre-check (CPathPendingPreCheck /\ SplitCPathPreCheck).
   The slot-drain twin of the queue AdvancerCPathPre* trio. The FUSED level needs
   NO action: SlotDrainCompleteCPath already reads hasExecutingVisible atomically
   with its activate/skip decision (that IS the fused pre-check). Only the SPLIT
   level intercepts the "counted" <=0 C-path case (SlotDrainCompleteCPath's
   exclusion guard hands it here). The read captures pending into drainPhase
   ("cnt_pt"/"cnt_pf"); the act/skip runs a following step on the captured value.
   The recovery rejoin's C-arm folds into SlotDrainCompleteCPath in this model, so
   it inherits the same pre-check (RecoverySplit = FALSE in the pre-check configs,
   so that path is dormant here regardless).
   =========================================================================== *)

\* SPLIT read, TRUE face: capture pending = TRUE. The act (SlotCPathPreActTrue) re-reads
\* pending/count FRESH under the lock, so a captured-TRUE-then-vanished publish misses cleanly.
SlotCPathPreReadTrue ==
  /\ SlotChainActivation
  /\ CPathPendingPreCheck
  /\ SplitCPathPreCheck
  /\ drainPhase = "counted"
  /\ (IF SkewTolerantPartition THEN drainRemaining <= 0 ELSE drainRemaining = 0)
  /\ hasExecutingVisible
  /\ drainPhase' = "cnt_pt"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation,
                 qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot,
                 slotStomped, recoveryOf, tenure, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* SPLIT read, FALSE face: capture pending = FALSE. SlotCPathPreSkipFalse then fires on this
\* STALE capture even if a publisher sets pending TRUE before it runs - the stranding suspect.
SlotCPathPreReadFalse ==
  /\ SlotChainActivation
  /\ CPathPendingPreCheck
  /\ SplitCPathPreCheck
  /\ drainPhase = "counted"
  /\ (IF SkewTolerantPartition THEN drainRemaining <= 0 ELSE drainRemaining = 0)
  /\ ~hasExecutingVisible
  /\ drainPhase' = "cnt_pf"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation,
                 qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot,
                 slotStomped, recoveryOf, tenure, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* SPLIT act: captured TRUE -> enter the lock body unchanged. Duplicates SlotDrainCompleteCPath's
\* body verbatim (fresh storeCount / hasExecutingVisible reads under the lock), differing only in
\* the "cnt_pt" phase guard.
SlotCPathPreActTrue ==
  /\ drainPhase = "cnt_pt"
  /\ LET i == drainItem IN
       /\ loc' = IF CompleteBeforeCount THEN loc ELSE [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF CompleteBeforeCount THEN tenure
                    ELSE (IF tenure = i THEN NoItem ELSE tenure)
       /\ IF (IF SkewTolerantPartition THEN storeCount <= 0 ELSE storeCount = 0)
             /\ hasExecutingVisible /\ executingItemVisible # NoItem
            THEN
              /\ activations' = [activations EXCEPT ![executingItemVisible] = @ + 1]
              /\ activatedSlot' = executingItemVisible
              /\ liveActivation' = TRUE
              /\ slotStomped' = slotStomped
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
            ELSE
              /\ activations' = activations
              /\ slotStomped' = IF ~CompleteBeforeCount
                                   /\ activatedSlot # NoItem /\ activatedSlot # i
                                   /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                                THEN TRUE ELSE slotStomped
              /\ activatedSlot' = IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem
              /\ liveActivation' = IF CompleteBeforeCount THEN liveActivation ELSE FALSE
              /\ UNCHANGED publish_vars
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* SPLIT skip: captured FALSE -> skip the activation, NO re-read of pending. Runs only the
\* ELSE (retire) bookkeeping and returns to idle, LEAVING any publish that landed after the
\* stale read for the publisher's own rescue. Fires regardless of the current hasExecutingVisible.
SlotCPathPreSkipFalse ==
  /\ drainPhase = "cnt_pf"
  /\ LET i == drainItem IN
       /\ loc' = IF CompleteBeforeCount THEN loc ELSE [loc EXCEPT ![i] = "Completed"]
       /\ tenure' = IF CompleteBeforeCount THEN tenure
                    ELSE (IF tenure = i THEN NoItem ELSE tenure)
       /\ activations' = activations
       /\ slotStomped' = IF ~CompleteBeforeCount
                            /\ activatedSlot # NoItem /\ activatedSlot # i
                            /\ loc[activatedSlot] \notin {"Completed", "Nowhere"}
                         THEN TRUE ELSE slotStomped
       /\ activatedSlot' = IF CompleteBeforeCount THEN DepthZeroClear({i}) ELSE NoItem
       /\ liveActivation' = IF CompleteBeforeCount THEN liveActivation ELSE FALSE
       /\ UNCHANGED publish_vars
  /\ drainPhase' = "idle"
  /\ drainItem' = NoItem
  /\ drainRemaining' = 0
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Trailing read fault on a committed SLOT waiter (RecoverySplit, Phase D) - the slot-tier
\* twin of DrainHeadRecovers. DrainSlotInline consumed the claimed task and its GetResult
\* FAULTED (the try/catch at Pipeline.cs ~1013-1020); the catch routes to RecoverWaiter
\* instead of CompleteWaiterDeferred. Fires at drainPhase = "claimed" - the GetResult
\* linearization point (SlotDrainCount's drainItem \in taskDone guard IS the GetResult; this
\* is its nondeterministic fault face) - and parks the drain THERE: loc -> "RecoveringInline",
\* drainPhase/drainItem UNTOUCHED. The parked drain's PC sits inside RecoverWaiter; the
\* position's count is still HELD (no decrement yet), so the executor's Count==0 inline gate
\* cannot cede the head position to a fresh reader while the recovery owns it, and the
\* advancer latch release (drainPhase = "idle" gated) is structurally unreachable until the
\* rejoin below runs - both exactly the in-place recovery's ownership story. Resolution:
\*   - REFUSE (RecoverRefuse) / substitute completes (RecoverInstallInline ->
\*     RecoverInlineCompletes -> BindingDischarge): the parked item leaves
\*     "RecoveringInline", unblocking SlotDrainCount (its RecoveringInline gate), and the
\*     drain REJOINS the standard SlotDrainCount -> SlotDrainComplete* steps - the
\*     decrement + activation partition (chain / handoff / <=0 C-path arms) run ONCE,
\*     post-resolution. This is the code's advance = true rejoin (refuse/sync) and
\*     AdvanceAndDrainRecovery's post-recovery drain (install) in one shape, with no
\*     partition duplication. (The complete arms' loc update on the already-Completed
\*     drainItem is idempotent; DrainConsistent's Draining clause admits the recovery locs.)
\* No counted-occupant gate is needed (DrainHeadRecovers' storeCount > 0 twin): the old
\* "counted"-point firing had to exclude the hijack-claimed mid-commit pair because its -1
\* skew mis-partitioned the fused arms; here the partition rides the standard skew-tolerant
\* rejoin, and a hijacked settled pair faulting at GetResult is truthfully recoverable.
\* SlotChainActivation-gated: the recovery tree models the fixed drain (the legacy complete
\* has no recovery-routing twin; every RecoverySplit config runs the fixed tree).
\*
\* CODE FINDING (June 2026, this round): Pipeline.cs's DrainSlotInline decrements BEFORE the
\* fault decision (~1022 precedes the RecoverWaiter call at ~1043), unlike DrainReadyWaiters
\* (decrement at ~1229 AFTER the advance = true rejoin). That early decrement (a) opens the
\* executor's Count==0 inline-activation gate while the faulted position is still being
\* recovered - the unconditional ActivateHeadItem in RecoverWaiter (~1418) then makes a
\* SECOND active reader (NoSimultaneousActiveReader's class); and (b) double-decrements on
\* the install path, whose continuation runs AdvanceAndDrain's queue-flavored DecrementCount
\* (~1637) again - and whose >0 arm peeks the QUEUE, tripping the count>0-no-head
\* Debug.Assert on a slot-tier successor. This action models the INTENDED order (fault
\* decision before decrement, slot site mirroring the queue site); DrainSlotInline needs the
\* reorder before this floor is real in code.
SlotHeadRecovers ==
  /\ RecoverySplit
  /\ SlotChainActivation
  /\ failed = {}                 \* single-failure bound (shared with the other injection points)
  /\ drainPhase = "claimed"
  /\ LET i == drainItem IN
       /\ i \in taskDone          \* settled at GetResult; the fault face of SlotDrainCount's gate
       /\ recoveryOf[i] = NoItem  \* first-level (a substitute's guarded task completes directly)
       /\ loc' = [loc EXCEPT ![i] = "RecoveringInline"]
       /\ failed' = failed \cup {i}
       /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny,
                 advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Drainer's release step. Mirrors DrainSlotInline's _advancing.Release() tail. Between
\* SlotCallbackDrains and this release, the slot is empty but the advancer is still held.
\* A follow-up commit can land a new item in the slot, and SlotCallbackBailsOut fires if
\* that item's task is done. The race window the failing test exercises.
\* (A drain-side recovery park needs no extra gate here: it parks at drainPhase = "claimed",
\* so the "idle" guard already keeps the latch held until the post-recovery rejoin ran.)
SlotDrainerRelease ==
  /\ advancing
  /\ drainerActive
  /\ drainPhase = "idle"  \* the drain's three steps complete before the release
  \* The code's release is UNCONDITIONAL in the method flow. Under PassOnceDrain the slot
  \* method's hold is disambiguated from queue passes by qDrainPhase = "none" (queue passes
  \* own a phase); the legacy composition disambiguated by ~escalated /\ empty-queue - which
  \* under pass-once would deadlock the slot drainer when the executor escalates mid-hold
  \* (escalated is monotonic; the first hunt's artifact #2).
  /\ (IF PassOnceDrain
        THEN qDrainPhase = "none"
        ELSE ~escalated /\ Len(waiters) = 0)
  /\ IF WeakAdvancerRelease
       THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
       ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
  \* PendingWordLatch: the release Exchange wipes the whole word, so a deposit made during
  \* this hold is consumed HERE. The obligation must be SERVED, not delegated to the flag:
  \* the slot drain's own claim cleared drainSignal at consume time (SlotClaimConsume /
  \* SlotDrainClaim), so a deposit whose companion flag-set the claim ate would vanish if we
  \* relied on the flag-gated tail (the tri-state round's first liveness counterexample - a
  \* queue inline-callback bail's deposit consumed by a slot release that then read the
  \* self-cleared flag and exited, stranding the queue head). The consumed-deposit route
  \* re-acquires and continues, exactly the queue path's pendReacq and the code's
  \* `(deposit || drainSignal)` reclaim gate. The non-deposit path keeps the flag-gated tail
  \* (a bail that did NOT win the deposit re-set the flag, and the tail serves it).
  \* The serve route is taken under SplitSlotFieldOps, where the claim's finer steps let a
  \* bail's flag be eaten by a LATER consume step (the strand). Under the fused claim the
  \* atomic consume + the bail's flag re-set keep the flag-gated tail sound (verified green),
  \* and "cl_acq" has no fused continuation - so fused keeps the delegate-to-flag behavior.
  /\ IF PendingWordLatch /\ advancingPending /\ SplitSlotFieldOps
       THEN /\ advancingPending' = FALSE
            /\ drainPhase' = "sl_serve"
            /\ drainerActive' = drainerActive
       ELSE /\ advancingPending' = (IF PendingWordLatch THEN FALSE ELSE advancingPending)
            /\ drainPhase' = drainPhase
            /\ drainerActive' = drainerActive
  \* drainerActive stays TRUE: the slot drainer's method continues past the release into
  \* its reclaim check (mirroring the queue drain's do-while). DrainerChainExit clears it
  \* when no reclaim precondition holds.
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Pass-once slot-method tail, escalated arm: DrainSlotInline's post-release reclaim finds
\* the store escalated and reroutes into DrainReadyWaiters (a fresh queue pass with the
\* do-top clear). Code: `if (signal && TryAcquire) { if (IsEscalated) { DrainReadyWaiters();
\* return; } ... }`. Unmodeled before this round; the legacy composition's standing actions
\* blurred over it.
SlotDrainerTailRerouteEscalated ==
  /\ PassOnceDrain
  /\ drainerActive
  /\ qDrainPhase = "none"
  /\ drainPhase = "idle"
  /\ ~advancingVisible
  /\ drainSignal
  /\ escalated
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainSignal' = FALSE  \* DrainReadyWaiters' do-top clear
  /\ qDrainPhase' = "pass"
  /\ qDrainedAny' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Pass-once slot-method exit: the post-release check found no signal, or lost the acquire
\* to a concurrent holder (who owns the chain from here). The method returns.
SlotDrainerTailExit ==
  /\ PassOnceDrain
  /\ drainerActive
  /\ qDrainPhase = "none"
  /\ drainPhase = "idle"
  /\ ~advancing
  /\ \/ ~drainSignal
     \/ advancingVisible  \* TryAcquire would lose; the winner's pass covers
  /\ drainerActive' = FALSE
  \* PendingWordLatch: a lose-exit (signal seen, stale-visible holder) deposits like every
  \* contended acquirer. Dead under fenced release (visible = actual, so ~advancing excludes
  \* the holder case); live under WeakAdvancerRelease.
  /\ advancingPending' = (IF PendingWordLatch /\ drainSignal /\ advancingVisible
                          THEN TRUE ELSE advancingPending)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

(* Consumed-deposit serve (SlotDrainerRelease's "sl_serve" route): the slot release wiped
   a deposit, so the drainer is obligated regardless of the flag. Re-acquire and continue -
   escalated reroutes into the queue drain, non-escalated re-enters the slot claim loop,
   a lost re-acquire re-deposits on the winner. The code's
   `if (deposit && TryAcquireOrFlagPending()) { IsEscalated ? DrainReadyWaiters() : continue; }`. *)

SlotServeReacqEscalated ==
  /\ drainPhase = "sl_serve"
  /\ ~advancingVisible
  /\ escalated
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainSignal' = FALSE  \* DrainReadyWaiters' do-top clear
  /\ qDrainPhase' = "pass"
  /\ qDrainedAny' = FALSE
  /\ drainPhase' = "idle"
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

SlotServeReacqSlot ==
  /\ drainPhase = "sl_serve"
  /\ ~advancingVisible
  /\ ~escalated
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainPhase' = "cl_acq"  \* re-enter the truthful slot claim (peek-gated under TriState)
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

SlotServeReacqLose ==
  /\ drainPhase = "sl_serve"
  /\ advancingVisible
  /\ drainPhase' = "idle"
  /\ drainerActive' = FALSE
  /\ advancingPending' = TRUE  \* lost re-acquire re-deposits on the winner (uniform rule)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars,
                 drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The slot-mode reclaim (the strand fix under design): post-release, the drainer re-checks
\* for a waiter whose callback bailed against its hold (recorded in drainSignal, callback
\* one-shot spent) and re-acquires to drain it. Fused TryAcquire + TryClaimSlotForDrain,
\* entering the standard three-step drain at "claimed". Mirrors TryReclaimAdvancerForWork.
SlotDrainerReclaim ==
  /\ ~SplitSlotFieldOps  \* fused fiction; SlotDrainerReclaimEntry + SlotClaim* are the truth
  /\ SlotReclaimEnabled
  /\ drainerActive
  /\ ~advancingVisible
  /\ drainSignal
  /\ ~escalated
  /\ drainPhase = "idle"
  /\ \E i \in Item :
       /\ hasSlot
       /\ slotItem = i
       /\ i \in taskDone
       /\ i \in callbackFired  \* the bailed waiter: callback spent, undrained
       /\ advancing' = TRUE
       /\ advancingVisible' = TRUE
       \* Re-acquire = sub-pass start: the signal is consumed; anything fired during the
       \* reclaimed drain re-sets it and the post-release recheck catches it.
       /\ drainSignal' = FALSE
       /\ StoreClaimSlotForDrain
       /\ loc' = [loc EXCEPT ![i] = "Draining"]
       /\ drainPhase' = "claimed"
       /\ drainItem' = i
       /\ tenure' = tenure
  /\ UNCHANGED <<publish_vars, tail_vars, escalated, waiters, storeCount,
                 taskDone, activations, callbackFired, esc_vars, failed, drainer_vars,
                 drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The race the originally-atomic SlotCallbackDrains hid: a follow-up slot callback fires
\* while the previous drain still holds the advancer (between SlotDrainerDrains and
\* SlotDrainerRelease). TryAcquire fails, the callback sets drainSignal and exits
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
  /\ ~SplitCallbackOps  \* fused set+failed-acquire+deposit; CallbackSetSignal (slot arm) +
                        \* CallbackAcquireLose are the un-fusing
  /\ \E i \in Item :
       /\ i \in taskDone
       /\ loc[i] = "InSlot"
       /\ slotItem = i
       /\ ~escalated
       /\ i \notin callbackFired
       /\ advancingVisible
       /\ callbackFired' = callbackFired \cup {i}
       /\ drainSignal' = TRUE
       /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                      storeCount, loc, taskDone, activations, esc_vars, drainer_vars, failed>>

\* Callback for an item in _waiters whose task completed. Reads `advancingVisible` via Exchange.
CallbackBecomesAdvancer ==
  /\ ~SplitCallbackOps  \* fused set+acquire+clear; the CallbackSetSignal/Acquire* steps are the un-fusing
  /\ \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InWaiters"
    /\ i \notin callbackFired
    /\ ~advancingVisible
    /\ advancing' = TRUE
    /\ advancingVisible' = TRUE
    \* Set-then-acquire-then-pass-top-clear, fused (see ExecutorInlineCallbackBecomesAdvancer).
    /\ drainSignal' = FALSE
    /\ callbackFired' = callbackFired \cup {i}
    /\ drainerActive' = TRUE
    /\ UNCHANGED <<publish_vars, tail_vars, slot_vars, waiters,
                   loc, taskDone, activations, storeCount, esc_vars, failed>>

CallbackBailsOut ==
  /\ ~SplitCallbackOps
  /\ \E i \in Item :
    /\ i \in taskDone
    /\ loc[i] = "InWaiters"
    /\ i \notin callbackFired
    /\ advancingVisible
    /\ drainSignal' = TRUE
    /\ callbackFired' = callbackFired \cup {i}
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters,
                   loc, taskDone, activations, storeCount, esc_vars, drainer_vars, failed>>

(* Un-fused queue callback (SplitCallbackOps, backlog #3). The real OnWaiterTaskCompleted is
   `_drainSignal = true; if (!TryAcquireOrFlagPending()) return; DrainReadyWaiters();`. Three
   steps, here split so concurrent callbacks and the advancer's pass-top clear interleave
   against the set-before-acquire transient. *)

\* Step 1: the plain `_drainSignal = true` store. Marks i signaled; the acquire follows.
\* Self-contained (no W-wrapper): callbackSignaled now lives in the aux tuples, so wrapping
\* would double-bind it.
CallbackSetSignal ==
  /\ SplitCallbackOps
  /\ \E i \in Item :
       /\ i \in taskDone
       \* InWaiters: queued-entry callback's store. InSlot occupant: slot-tier callback's
       \* store - the SLOT bail's set, un-fused from its acquire (the audit's last granularity
       \* item). The escTail exclusion mirrors SlotDrainClaimEntry (wiring runs after the
       \* commit's count step). A mid-move transient (loc still InSlot, slotItem already
       \* cleared) defers the set one step to the InWaiters arm - no reader distinguishes the
       \* plain store's timing within that window.
       /\ \/ loc[i] = "InWaiters"
          \/ (loc[i] = "InSlot" /\ slotItem = i /\ i # escTail)
       /\ i \notin callbackFired
       /\ i \notin callbackSignaled
       /\ callbackSignaled' = callbackSignaled \cup {i}
       /\ drainSignal' = TRUE
       /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars, waiters, storeCount,
                      esc_vars, drainer_vars, loc, taskDone, activations, callbackFired, failed,
                      drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                      tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny,
                      advancingPending, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Step 2, won: TryAcquireOrFlagPending won (advancer free). Become the advancer; the do-top
\* Exchange clears the signal (which a concurrent sibling's set may since have re-raised - that
\* re-raise is the sibling's, carried by ITS deposit when it later loses).
CallbackAcquireWin ==
  /\ SplitCallbackOps
  /\ escalated  \* the IsEscalated route: acquire -> DrainReadyWaiters. Queue items are
                \* escalated by construction; a PRE-escalation slot occupant's win routes
                \* through SlotDrainClaim/SlotDrainClaimEntry's signaled precondition instead
                \* (acquire -> DrainSlotInline), and a mid-escalation occupant lands here
                \* draining nothing - the un-fused SlotCallbackBailsDuringEscalation.
  /\ \E i \in callbackSignaled :
       /\ i \notin callbackFired
       /\ ~advancingVisible
       /\ advancing' = TRUE
       /\ advancingVisible' = TRUE
       /\ drainSignal' = FALSE
       /\ drainerActive' = TRUE
       /\ callbackFired' = callbackFired \cup {i}
       /\ callbackSignaled' = callbackSignaled \ {i}
       /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
       /\ qDrainedAny' = (IF PassOnceDrain THEN FALSE ELSE qDrainedAny)
       /\ UNCHANGED <<publish_vars, tail_vars, slot_vars, waiters, storeCount,
                      esc_vars, loc, taskDone, activations, failed,
                      drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                      tenure, assertFailed, nullActivation, advancingPending, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Step 2, lost: the advancer was held. Deposit the obligation in the latch word (the holder's
\* release serves it); the signal stays set (this callback's own store, not yet cleared).
CallbackAcquireLose ==
  /\ SplitCallbackOps
  /\ \E i \in callbackSignaled :
       /\ i \notin callbackFired
       /\ advancingVisible
       /\ callbackFired' = callbackFired \cup {i}
       /\ callbackSignaled' = callbackSignaled \ {i}
       /\ advancingPending' = (IF PendingWordLatch THEN TRUE ELSE advancingPending)
       \* (June 2026 audit round: callbackSignaled previously ALSO sat in this UNCHANGED
       \* tuple, contradicting the assignment above - the action was unsatisfiable and the
       \* deposit-on-lose interleaving was never explored; the witness config passed with
       \* the lose arm dead because the held latch eventually releases and the win arm
       \* covers liveness. Slot-bail coverage relies on this arm, so it is now live.)
       /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                      esc_vars, drainer_vars, loc, taskDone, activations, failed,
                      drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                      tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* The signaled slot occupant was drained out from under its own callback: the holder's
\* reclaim took it between the set and the acquire (reachable precisely because the
\* reclaim's gate is the signal token our set raised). The callback's acquire then wins
\* against a slot that no longer holds its item - DrainSlotInline's claim-fail branch
\* releases and exits, net zero. The release's deposit-serve is approximated as
\* not-consumed: the deposit stays for the next acquirer, service is delayed never lost
\* (fairness on the reclaim/callback entries delivers it) - trading the finer serve
\* interleaving for state space. The lose face (latch held) is CallbackAcquireLose, which
\* carries no loc guard. The escalated faces route through CallbackAcquireWin.
SlotCallbackSpent ==
  /\ SplitCallbackOps
  /\ \E i \in callbackSignaled :
       /\ i \notin callbackFired
       /\ ~escalated
       /\ loc[i] # "InSlot"
       /\ ~advancingVisible
       /\ callbackFired' = callbackFired \cup {i}
       /\ callbackSignaled' = callbackSignaled \ {i}
       /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                      esc_vars, drainer_vars, loc, taskDone, activations, failed,
                      drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                      tenure, assertFailed, nullActivation, qDrainPhase, qDrainedAny,
                      advancingPending, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

(* ===========================================================================
   Advancer actions
   =========================================================================== *)

\* Advancer drains the head of _waiters if its task is completed.
\* Models DrainReadyWaiters inner loop: TryPeek + TryDequeue + Decrement + CompleteWaiter.
AdvancerDrainHead ==
  /\ advancing
  /\ drainPhase = "idle"  \* the latch holder is one thread in one method: not mid-slot-drain
  /\ PassOnceDrain => qDrainPhase = "pass"  \* pass-once: the inner while runs inside the do-body
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  /\ LET i == Head(waiters)
         \* The code's `var count = _waiters.DecrementCount()` - the partition input is the
         \* COUNT, not the queue length. Under the fused commit they coincide (newCount = 0
         \* iff Len(waiters) = 1 here); under SplitCountCommit a visible-but-uncounted entry
         \* skews them and newCount can go NEGATIVE (StoreCountNonNegative).
         newCount == storeCount - 1 IN
       /\ StoreDequeueHead
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       \* CONSUMPTION POINT: the drain's `item.WaiterTask.GetAwaiter().GetResult()` (Pipeline.cs
       \* ~1529) - the single consumption that ends the token's lifetime. A concurrent committer
       \* still mid-self-act for THIS item now holds a stale ValueTask; its next post-publication
       \* read (ARM/recheck/verify/wiring) hits a Consumed token. This is the drain claim path
       \* under the advancer latch: the claim confers exclusive consumption (legal here).
       /\ ConsumeToken(i)
       \* This pass satisfied at least one signal's worth of work (SignalConservation's input).
       /\ qDrainedAny' = (IF PassOnceDrain THEN TRUE ELSE qDrainedAny)
       \* No per-item signal bookkeeping: the dirty flag was cleared at pass start and the
       \* pass drains exhaustively (the counter's per-dequeue decrement - and its transient
       \* negative when consuming a visible-but-uncounted entry whose inline callback hasn't
       \* fired yet - is retired with the retype).
       \* The queue drain consumes at dequeue, before its DecrementCount - already the
       \* contract ordering, no toggle. Tenure (if this head ever held it: a slot-tier item
       \* moved to queue head by escalation) releases with the consume.
       /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
       /\ UNCHANGED drainSignal  \* pass-scoped flag: cleared at acquisition, not per item
       /\ \* The partition. Shipped (~SkewTolerantPartition): `count is 0` takes the C-path
          \* lock block (separate actions); anything else - INCLUDING a skewed -1 - takes
          \* the D-arm, whose TryPeek return is UNCHECKED (Pipeline.cs ~1124): with an empty
          \* remainder it activates default(T), the proven NRE/exit-134 (nullActivation).
          \* Fix (SkewTolerantPartition): `count <= 0` routes to the C-path (correct per the
          \* BoundedCountSkew argument: a negative count's in-flight entry is already
          \* consumed); count > 0 implies a peekable head in the under-promise direction, so
          \* the D-arm's checked peek is a canary - the null arm stays as the detector and
          \* NoNullActivation must HOLD.
          \* activatedSlot mirrors the activation partition: clear if we just completed an
          \* item that was the slot occupant; set to the new head on the D-arm.
          IF (IF SkewTolerantPartition THEN newCount <= 0 ELSE newCount = 0)
            THEN /\ activations' = activations
                 /\ activatedSlot' = DepthZeroClear({i})
                 /\ liveActivation' = FALSE  \* retire-only: CompleteWaiterDeferred's clear
                 /\ nullActivation' = nullActivation
          ELSE IF Len(waiters) > 1
            \* QueueDPathCompletedGuard: the D-arm's IsCompleted skip, mirroring the slot
            \* D-path (SlotDrainCompleteChainSlot / SlotDrainHandoffReclaim). A completed
            \* head is drain-only - ActivateHeadItem is skipped, the retirement clear still
            \* runs (retire-only effects, same as the C-branch), and the obligation is
            \* discharged by this very pass's next iteration: the head is in taskDone and
            \* qDrainPhase stays "pass", so AdvancerDrainHead (WF) dequeues it next.
            \* Dispatch-saver ONLY - never load-bearing for at-most-once (a check-then-
            \* activate TOCTOU; Pipeline_GateDoubleActFixNoGuard.cfg holds without it).
            THEN IF QueueDPathCompletedGuard /\ Head(Tail(waiters)) \in taskDone
                   THEN /\ activations' = activations
                        /\ activatedSlot' = DepthZeroClear({i})
                        /\ liveActivation' = FALSE  \* retire-only: the activate is skipped
                        /\ nullActivation' = nullActivation
                   ELSE /\ activations' = [activations EXCEPT ![Head(Tail(waiters))] = @ + 1]
                        /\ activatedSlot' = Head(Tail(waiters))
                        \* Retire + D-arm activate in one step: code order is clear-at-retirement
                        \* then ActivateHeadItem's set - the step ends TRUE. The D-path is NOT
                        \* gated on liveActivation (the code doesn't gate it).
                        /\ liveActivation' = TRUE
                        /\ nullActivation' = nullActivation
            ELSE /\ activations' = activations
                 /\ activatedSlot' = DepthZeroClear({i})
                 /\ liveActivation' = FALSE  \* retire-only (the null-arm canary)
                 /\ nullActivation' = TRUE
       \* Backlog #8 (ModelCPathClear): a <=0 decrement enters the C-path LOCK block as a
       \* distinct phase, so the lock's storeCount re-read (which can differ from newCount -
       \* a successor committing in between is the whole point) is a separate step. Without
       \* the toggle, qDrainPhase stays "pass" (the lock-activate folds into AdvancerCPath, as
       \* the 9 verified configs have it). A >0 decrement is the D-path and never enters the lock.
       \* CPathPendingPreCheck routes to the pre-check phase "cpath_pre" instead of "cpath_lock":
       \* the pending flag is read OUTSIDE the lock and the lock ("cpath_lock") is entered only
       \* when it reads TRUE (see the AdvancerCPathPre* actions).
       /\ qDrainPhase' = (IF ModelCPathClear
                             /\ (IF SkewTolerantPartition THEN newCount <= 0 ELSE newCount = 0)
                          THEN (IF CPathPendingPreCheck THEN "cpath_pre" ELSE "cpath_lock")
                          ELSE qDrainPhase)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, advancingPending, callbackSignaled, slotStomped, recoveryOf,
                 staleTokenRead, commitWasCompleted, cbWiredPrePublish>>
  /\ ReadTurnMonitor  \* writes activatedSlot (D-arm activate / clear); witness the read-turn stomp

\* Trailing read fault on a committed queue waiter (RecoverySplit, backlog #2/#5 Phase C). The
\* drainer reaches a settled head whose pipeline task FAULTED (not completed): the code's
\* DrainReadyWaiters does `task.GetResult()` in a try/catch and routes the catch to RecoverWaiter.
\* The recovery-routing twin of AdvancerDrainHead - SAME store consumption (StoreDequeueHead) and
\* SAME next-head activation partition (the rest of the queue continues normally), but the faulted
\* head goes to "Recovering" (failed) instead of "Completed". The fault is a nondeterministic
\* alternative at drain time (a settled head either succeeded -> AdvancerDrainHead, or faulted ->
\* here); recovery then resolves the parked item via RecoverInstall / RecoverRefuse - the floor
\* must catch a trailing fault, not let it hose the wire. Phase C one-failure bound (failed = {}).
\* KNOWN WIRING INFIDELITY under SingleActivationGate - see the QueueTrailingFault constant:
\* the code's queue trailing fault recovers IN PLACE (RecoverWaiter, unconditional activate,
\* count credit held); this action's executor-side park + at-fault partition manufacture a
\* false gate deadlock. Kept as-is (gated) until the faithful rewiring round.
DrainHeadRecovers ==
  /\ RecoverySplit
  /\ QueueTrailingFault          \* injection gate; see the constant's comment
  /\ failed = {}                 \* Phase C single-failure bound (NumItems=3 substitute reservoir)
  /\ advancing
  /\ drainPhase = "idle"
  /\ (PassOnceDrain => qDrainPhase = "pass")
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  \* A trailing READ fault is on a fully-COMMITTED waiter (its commit landed, its read then
  \* faulted during drain), so the head is counted: storeCount > 0 keeps the decrement >= 0.
  \* This excludes the visible-but-uncounted split-commit mid-state (where StoreDequeueHead
  \* would skew the count to -1 with no in-flight commit to justify it - that faulting is not
  \* a real trailing fault, the item is not even fully committed).
  /\ storeCount > 0
  /\ LET i == Head(waiters)
         newCount == storeCount - 1 IN
       /\ recoveryOf[i] = NoItem  \* first-level (a substitute trailing-fault is a later sub-case)
       /\ StoreDequeueHead        \* the drain consumed the head before GetResult faulted
       /\ loc' = [loc EXCEPT ![i] = "Recovering"]
       /\ failed' = failed \cup {i}
       /\ qDrainedAny' = (IF PassOnceDrain THEN TRUE ELSE qDrainedAny)
       /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
       /\ UNCHANGED drainSignal
       \* The next-head activation partition, identical to AdvancerDrainHead: the faulted head
       \* leaving is an ordinary dequeue for the REST of the queue. activatedSlot mirrors:
       \* clear if slot pointed at the faulted i; set to the new head on the D-arm.
       \* liveActivation: the fault park does NOT run CompleteWaiterDeferred (the catch routes
       \* to RecoverWaiter), so the faulted head's clear happens later at its recovery
       \* completion - non-activating arms leave the bit; the D-arm's activation sets it.
       /\ IF (IF SkewTolerantPartition THEN newCount <= 0 ELSE newCount = 0)
            THEN /\ activations' = activations
                 /\ activatedSlot' = DepthZeroClear({i})
                 /\ liveActivation' = liveActivation
                 /\ nullActivation' = nullActivation
          ELSE IF Len(waiters) > 1
            \* QueueDPathCompletedGuard: same IsCompleted skip as AdvancerDrainHead's D-arm.
            \* The fault park leaves liveActivation as the non-activating arms here do (no
            \* CompleteWaiterDeferred ran); the skipped completed head is dequeued by the
            \* pass's own next AdvancerDrainHead iteration.
            THEN IF QueueDPathCompletedGuard /\ Head(Tail(waiters)) \in taskDone
                   THEN /\ activations' = activations
                        /\ activatedSlot' = DepthZeroClear({i})
                        /\ liveActivation' = liveActivation
                        /\ nullActivation' = nullActivation
                   ELSE /\ activations' = [activations EXCEPT ![Head(Tail(waiters))] = @ + 1]
                        /\ activatedSlot' = Head(Tail(waiters))
                        /\ liveActivation' = TRUE
                        /\ nullActivation' = nullActivation
            ELSE /\ activations' = activations
                 /\ activatedSlot' = DepthZeroClear({i})
                 /\ liveActivation' = liveActivation
                 /\ nullActivation' = TRUE
       /\ qDrainPhase' = (IF ModelCPathClear
                             /\ (IF SkewTolerantPartition THEN newCount <= 0 ELSE newCount = 0)
                          THEN (IF CPathPendingPreCheck THEN "cpath_pre" ELSE "cpath_lock")
                          ELSE qDrainPhase)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, slot_vars,
                 taskDone, callbackFired, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, advancingPending, callbackSignaled, slotStomped, recoveryOf>>
  /\ ReadTurnMonitor  \* writes activatedSlot (D-arm activate / clear); witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Advancer at count=0, executes C-path activation under lock.
AdvancerCPath ==
  /\ ~SplitCPathExchange  \* fused fiction; the split trio below is the truthful shape
  /\ advancing
  /\ drainPhase = "idle"  \* see AdvancerDrainHead
  \* Under ModelCPathClear the C-path lock is its own phase ("cpath_lock", set by the <=0
  \* decrement); without the toggle it folds into the "pass" do-body as the 9 configs have it.
  /\ PassOnceDrain => qDrainPhase = (IF ModelCPathClear THEN "cpath_lock" ELSE "pass")
  \* The fix widens the C-path to count <= 0: a negative count means the in-flight commit's
  \* entry was already completed-and-consumed (BoundedCountSkew bounds it at -1), so the
  \* deferred publish is the only live responsibility and this claim is correct. Shipped
  \* code's == 0 is the quiet-face skip.
  /\ (IF SkewTolerantPartition THEN storeCount <= 0 ELSE storeCount = 0)
  \* The QUEUE C-path is gated site (2): `_waiters.Count is 0 && !_liveActivation &&
  \* Volatile.Read(pending)`. Fused form: the gate in the guard suffices - the alternative
  \* to firing is simply not firing (the publish and pending stay; the lock-phase exit is
  \* AdvancerCPathLockExit, whose activate-arm negation mirrors this gate).
  /\ GateOpen
  /\ hasExecutingVisible
  \* PeekGatedCPathLicense: the residency half of the license. The fused form has always
  \* read Len(waiters) = 0 atomically with the count, so the peek gate is inherently
  \* satisfied here - the toggle's conjunct is stated for uniformity with the split fire
  \* but adds nothing (the under-promise window only opens when the Count read and the
  \* Exchange are separate instructions, i.e. SplitCPathExchange).
  /\ Len(waiters) = 0
  /\ (PeekGatedCPathLicense => Len(waiters) = 0)
  /\ ~hasSlot
  /\ executingItemVisible # NoItem
  /\ LET exec == executingItemVisible IN
       /\ activations' = [activations EXCEPT ![exec] = @ + 1]
       /\ activatedSlot' = exec  \* the C-path lock-block activation (Pipeline.cs:1281)
       /\ liveActivation' = TRUE  \* ActivateHeadItem grants the turn
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)  \* exit the lock, continue the do-body
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, callbackFired, failed, esc_vars, drainer_vars>>

\* SplitCPathExchange trio: the queue C-path lock block as its two real instructions. The code's
\*   if (_waiters.Count <= 0 && Interlocked.Exchange(ref pending, false)) ActivateHeadItem(...)
\* short-circuits Count-read THEN Exchange - separate instructions, and the thread can be
\* preempted between them WHILE HOLDING the lock. The lock excludes only other lock-takers
\* (the executor's dispatch lock block); the executor's commit-increment, its (pre-fix
\* lock-free) wasEmpty self-activate, and its next dispatch's publish+pending all land outside
\* the lock, so they walk straight through the armed window. The Exchange then consumes
\* whatever publish exists AT FIRE TIME - possibly a publish that did not exist when the Count
\* read vouched for an empty store. July 2026 witness trace (ConcurrentSyncAndAsync):
\* holder [-10,-3,-5(c0d1),25(c1d1),1], orphan [-10,20(c1d2),1,-3].
AdvancerCPathArm ==
  /\ SplitCPathExchange
  /\ advancing
  /\ drainPhase = "idle"
  /\ PassOnceDrain => qDrainPhase = (IF ModelCPathClear THEN "cpath_lock" ELSE "pass")
  /\ (IF SkewTolerantPartition THEN storeCount <= 0 ELSE storeCount = 0)  \* the Count read - stale-able
  \* PeekGatedCPathLicense at the ARM is inherently satisfied (the arm already reads
  \* Len(waiters) = 0) and deliberately NOT the fix: the arm's reads can go stale while the
  \* thread is preempted inside the lock - the witness's completed-unactivated head commits
  \* AFTER the arm. The load-bearing residency peek is the FIRE's (fresh, under-lock).
  /\ Len(waiters) = 0
  /\ (PeekGatedCPathLicense => Len(waiters) = 0)
  /\ ~hasSlot
  /\ qDrainPhase' = "cpath_armed"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Fire: the Exchange + activate. NO re-read of count/waiters/slot - the arm's read is the only
\* license, and it may be stale. Consumes the publish present NOW (executor steps may have
\* interleaved since the arm).
AdvancerCPathFire ==
  /\ SplitCPathExchange
  /\ advancing
  /\ qDrainPhase = "cpath_armed"
  \* Gated site (2), split form: the code reads _liveActivation under the lock AT FIRE TIME
  \* (a fresh read, after the arm's possibly-stale Count read). Gate closed = the fire is
  \* skipped and the publish/pending are LEFT (AdvancerCPathFireSkip below).
  /\ GateOpen
  \* PeekGatedCPathLicense: the residency half of the license, read FRESH at fire time
  \* alongside the gate's under-lock _liveActivation read. The arm's Count read vouched for
  \* an empty store, but a commit's enqueue precedes its increment (SplitCountCommit) and
  \* both land lock-free - a completed-unactivated head can be RESIDENT here while the
  \* count still reads 0. Firing past it activates the deferred-published item out of FIFO
  \* order, seeding the same-flow double activation (Pipeline_GateDoubleActWitness.cfg).
  \* Under the toggle the fire declines instead (AdvancerCPathFirePeekDecline below).
  /\ (PeekGatedCPathLicense => Len(waiters) = 0)
  /\ hasExecutingVisible
  /\ executingItemVisible # NoItem
  /\ LET exec == executingItemVisible IN
       /\ activations' = [activations EXCEPT ![exec] = @ + 1]
       /\ activatedSlot' = exec
       /\ liveActivation' = TRUE
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, callbackFired, failed, esc_vars, drainer_vars>>

\* Peek-decline (PeekGatedCPathLicense): the fire's fresh residency read found a resident
\* queue entry - possibly one whose count increment has not landed - so the license is
\* declined and the fire does NOT consume the publish. The pending flag and the publish are
\* LEFT in place for the re-fire, exactly like the gate skip: the resident head is drained
\* (or activated) in FIFO order by the pass's own loop, and the C-path re-arms once the
\* store is empty by residency. Exits the lock phase back to the do-body so the armed
\* window cannot deadlock the drainer; carries the fire family's fairness.
AdvancerCPathFirePeekDecline ==
  /\ SplitCPathExchange
  /\ PeekGatedCPathLicense
  /\ advancing
  /\ qDrainPhase = "cpath_armed"
  /\ Len(waiters) > 0
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Gate-skip (SingleActivationGate): the under-lock _liveActivation read found a live
\* activation, so the fire does NOT consume the publish - the pending flag is LEFT in place
\* (the code's short-circuit never reaches the Exchange) on the coverage claim "the live
\* reader's retirement clears _liveActivation and this C-path re-fires in the same advancer
\* pass". The lock phase exits back to the do-body; whether that claim always holds is
\* exactly what EventuallyActivated adjudicates under SingleActivationGate = TRUE.
AdvancerCPathFireSkip ==
  /\ SplitCPathExchange
  /\ SingleActivationGate
  /\ advancing
  /\ qDrainPhase = "cpath_armed"
  /\ liveActivation
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Miss: the Exchange found no publish (pending false). Exit the lock empty-handed.
AdvancerCPathFireMiss ==
  /\ SplitCPathExchange
  /\ advancing
  /\ qDrainPhase = "cpath_armed"
  /\ ~hasExecutingVisible
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The queue C-path lock block's clear-at-Count>0 arm (backlog #8, ModelCPathClear). The
\* advancer reached the terminal C-path lock (the drain loop's while has exited - no ready
\* head) and WON the deferred-publish Exchange (hasExecuting was TRUE), but the under-lock
\* re-read sees storeCount>0: a successor committed since the decrement that brought it here.
\* The code (DrainReadyWaiters ~1243) CLEARS _executingItem WITHOUT activating, trusting the
\* D-path to activate the item as a queue head. Winning the Exchange means the executor's
\* own commit of this item saw alreadyActivated and did NOT self-activate (the Loses path),
\* so the clear leaves NEITHER side activating - the slot double-skip's exact shape. The
\* cleared item is "Executing"; it commits behind the successor and its activation rides on a
\* later D-path. EventuallyActivated adjudicates whether that coverage always holds.
\* (Over-approximate in timing - it may fire before a specific <=0 decrement is marked - but
\* the activation-coverage question is timing-independent: either the D-path covers the
\* cleared item or it does not, regardless of exactly when the publish was cleared.)
AdvancerCPathClearAtCount ==
  /\ ModelCPathClear
  /\ ~CPathLeaveFix             \* the BUG: clears the won publish. The fix LEAVES it (below).
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_lock"  \* reached the lock via a genuine <=0 decrement
  /\ hasExecutingVisible        \* Exchange(hasExecuting) won - a publish to act on
  /\ executingItemVisible # NoItem
  /\ storeCount > 0             \* the under-lock re-read: a successor committed since the decrement
  /\ hasExecuting' = FALSE
  /\ hasExecutingVisible' = FALSE
  /\ executingItem' = NoItem
  /\ executingItemVisible' = NoItem
  /\ qDrainPhase' = "pass"      \* exit the lock, the do-body's while continues
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The backlog #8 FIX (CPathLeaveFix): at the C-path lock with Count>0, do NOT consume the
\* publish - LEAVE hasExecuting/executingItem intact and exit the lock. The published item is
\* still owned by the executor's CommitTailWaiter Exchange, which reclaims it and activates
\* (wasEmpty self-activate when it commits into the drained queue, or a later C-path/D-path
\* when it queues behind the live successor). Code: re-order so the Count<=0 check gates the
\* Exchange - only consume+activate at Count<=0, leave the publish otherwise.
AdvancerCPathLeaveAtCount ==
  /\ ModelCPathClear
  /\ CPathLeaveFix
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_lock"
  /\ hasExecutingVisible
  /\ executingItemVisible # NoItem
  /\ storeCount > 0
  /\ qDrainPhase' = "pass"      \* exit the lock; publish_vars deliberately untouched
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The C-path lock found nothing to do: no publish (the Exchange returned false), or a
\* residual storeCount/slot shape neither the activate nor the clear arm matches. Exit the
\* lock back into the do-body. Keeps the cpath_lock phase from deadlocking the drainer.
AdvancerCPathLockExit ==
  /\ ModelCPathClear
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_lock"
  \* Neither the activate arm (storeCount<=0 /\ Len=0 /\ ~hasSlot /\ publish /\ gate open -
  \* the GateOpen conjunct mirrors AdvancerCPath's guard: a gate-closed skip exits the lock
  \* here, publish and pending LEFT, as the code's short-circuit does) nor the clear arm
  \* (storeCount>0 /\ publish) is enabled.
  /\ ~( (IF SkewTolerantPartition THEN storeCount <= 0 ELSE storeCount = 0)
        /\ Len(waiters) = 0 /\ ~hasSlot /\ hasExecutingVisible /\ executingItemVisible # NoItem
        /\ GateOpen )
  /\ ~( storeCount > 0 /\ hasExecutingVisible /\ executingItemVisible # NoItem )
  /\ qDrainPhase' = "pass"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

(* ===========================================================================
   Queue C-path pending pre-check (CPathPendingPreCheck). The proposed elision:
   read the deferred-publish pending flag (hasExecutingVisible, a Volatile.Read)
   OUTSIDE the lock, and enter the lock ("cpath_lock") only when it reads TRUE;
   skip the whole lock phase (-> "pass") when it reads FALSE. The lock body is
   unchanged when entered - the AdvancerCPath* trio re-reads pending/count under
   the lock as before. AdvancerDrainHead's <=0 decrement routes to "cpath_pre"
   instead of "cpath_lock" under the toggle.

   FUSED (~SplitCPathPreCheck): the read + the enter/skip decision are one atomic
   action, so a FALSE skip only fires when pending is genuinely FALSE now - it
   coincides with the existing lock-exit-on-no-publish (green baseline).

   SPLIT (SplitCPathPreCheck): the read captures the pending value into the drain
   PC ("cpath_pre_t"/"cpath_pre_f"), other threads interleave, then the enter/skip
   acts on the CAPTURED value with NO re-read. A stale FALSE (pending set by a
   publisher between the read and the skip) still skips the lock, LEAVING the fresh
   publish - the adjudication that matters. EventuallyActivated is the verdict.
   =========================================================================== *)

\* FUSED read-and-enter: pending reads TRUE atomically -> enter the lock body unchanged.
AdvancerCPathPreEnter ==
  /\ CPathPendingPreCheck
  /\ ~SplitCPathPreCheck
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_pre"
  /\ hasExecutingVisible        \* the outside Volatile.Read, atomic with the decision
  /\ qDrainPhase' = "cpath_lock"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* FUSED read-and-skip: pending reads FALSE atomically -> skip the lock (like LockExit's
\* no-publish case; the publish, if any, is genuinely absent at this instant).
AdvancerCPathPreSkip ==
  /\ CPathPendingPreCheck
  /\ ~SplitCPathPreCheck
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_pre"
  /\ ~hasExecutingVisible
  /\ qDrainPhase' = "pass"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* SPLIT read, TRUE face: capture pending = TRUE into the PC. The enter is a following step;
\* a captured TRUE only means "the lock will be entered" - the lock body re-reads pending
\* fresh, so a captured-TRUE-then-vanished publish misses harmlessly (AdvancerCPathFireMiss).
AdvancerCPathPreReadTrue ==
  /\ CPathPendingPreCheck
  /\ SplitCPathPreCheck
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_pre"
  /\ hasExecutingVisible
  /\ qDrainPhase' = "cpath_pre_t"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* SPLIT read, FALSE face: capture pending = FALSE into the PC. The skip is a following step -
\* between here and the skip a publisher can set pending TRUE, and the skip still fires on the
\* STALE captured FALSE. This is the whole point of the split level.
AdvancerCPathPreReadFalse ==
  /\ CPathPendingPreCheck
  /\ SplitCPathPreCheck
  /\ advancing
  /\ drainPhase = "idle"
  /\ qDrainPhase = "cpath_pre"
  /\ ~hasExecutingVisible
  /\ qDrainPhase' = "cpath_pre_f"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* SPLIT enter: captured TRUE -> enter the lock body unchanged (fresh under-lock reads).
AdvancerCPathPreEnterSplit ==
  /\ CPathPendingPreCheck
  /\ SplitCPathPreCheck
  /\ advancing
  /\ qDrainPhase = "cpath_pre_t"
  /\ qDrainPhase' = "cpath_lock"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* SPLIT skip: captured FALSE -> skip the lock phase back to the do-body, NO re-read. Fires
\* even if hasExecutingVisible is TRUE by now: the publish landed after our stale read and is
\* LEFT for the publisher's own rescue (its commit's wasEmpty self-activate at count 0, or the
\* CommitTailWaiter reclaim at count > 0). If that rescue ever fails, EventuallyActivated
\* reports the strand - THE finding.
AdvancerCPathPreSkipSplit ==
  /\ CPathPendingPreCheck
  /\ SplitCPathPreCheck
  /\ advancing
  /\ qDrainPhase = "cpath_pre_f"
  /\ qDrainPhase' = "pass"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Advancer release. Toggle picks Volatile.Write vs Exchange.
\* Aligned with DrainReadyWaiters' release: fires when queue is empty or head not done.
\* No slot clause: SlotCallbackDrains is atomic (acquire-drain-C-path-release), so the
\* advancer is never observed held between transitions in any state where hasSlot = TRUE.
AdvancerRelease ==
  /\ ~PassOnceDrain  \* legacy standing release; QDrainReleaseStep is the pass-once truth
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
  /\ ~PassOnceDrain  \* legacy standing reclaim; the QDrainRecheck/Reclaim steps are the truth
  /\ drainerActive
  /\ ~advancingVisible
  /\ drainSignal
  /\ Len(waiters) > 0
  /\ Head(waiters) \in taskDone
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  \* Re-acquire = sub-pass start, signal consumed (see SlotDrainerReclaim).
  /\ drainSignal' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

(* ===========================================================================
   Pass-once drain steps (PassOnceDrain = TRUE): DrainReadyWaiters' real shape.
     do { Exchange(signal, FALSE); while (head ready) drain; Release();
     } while (signal && TryReclaim());
   The acquire entries (CallbackBecomesAdvancer / ExecutorInlineCallbackBecomesAdvancer /
   ExecPostEscalationNudge wrappers) set qDrainPhase = "pass" alongside their signal clear.
   AdvancerDrainHead and AdvancerCPath carry the qDrainPhase = "pass" gate. The steps below
   model the release-recheck-reclaim tail of the do-while, each a separate atomic step so
   TLC explores the rendezvous the standing composition re-evaluates away (field dump
   park_hang.dmp: signal TRUE, latch free, completed head stranded).
   =========================================================================== *)

\* The inner while found no ready head: Release, then the ONE-SHOT signal recheck is next.
QDrainReleaseStep ==
  /\ PassOnceDrain
  /\ qDrainPhase = "pass"
  /\ advancing
  /\ drainPhase = "idle"
  /\ \/ Len(waiters) = 0
     \/ /\ Len(waiters) > 0
        /\ Head(waiters) \notin taskDone
  \* The release is the latch-word Exchange: it reads the pending bit ATOMICALLY with
  \* releasing (PendingWordLatch). Pending set = a bail deposited during our hold - we are
  \* obligated: consume the bit and route to the re-acquire (the Exchange-then-CAS window
  \* is modeled by the pendReacq arms; losing the re-acquire is benign, the winner's
  \* exhaustive pass serves all visible work).
  /\ IF PendingWordLatch /\ advancingPending
       THEN /\ IF WeakAdvancerRelease
                 THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
                 ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
            /\ advancingPending' = FALSE
            /\ qDrainPhase' = "pendReacq"
       ELSE /\ IF WeakAdvancerRelease
                 THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
                 ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
            /\ advancingPending' = advancingPending
            /\ qDrainPhase' = "recheck"
  \* SignalConservation: a pass that consumed the signal and dequeued NOTHING did not
  \* satisfy it - the signaled work is still materializing (mid-move / mid-commit). Restore
  \* the token at the release so the materializer's tail check (the one-shot nudge, the
  \* post-increment inline callback) finds it. The pass's own recheck/reclaim reads do not
  \* consume, so the restored token survives this pass's exit.
  /\ drainSignal' = (IF SignalConservation /\ ~qDrainedAny
                        /\ (~CountGatedConservation \/ storeCount > 0)
                     THEN TRUE ELSE drainSignal)
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Recheck read the signal as set: proceed to TryReclaim.
QDrainRecheckSeen ==
  /\ qDrainPhase = "recheck"
  /\ drainSignal
  /\ qDrainPhase' = "reclaim"
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Recheck read the signal as clear: the do-while exits and the method returns. THE pass-once
\* exit - if a wake was needed and this read raced it, nothing re-evaluates.
QDrainRecheckMissed ==
  /\ qDrainPhase = "recheck"
  /\ ~drainSignal
  /\ qDrainPhase' = "none"
  /\ drainerActive' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

\* TryReclaimAdvancerForWork, UN-FUSED (June 2026, the field strand's home). The original
\* fused acquire+peek+release gave the transient hold ZERO WIDTH in-model - no bail could
\* land against it, which is why four hunt rounds missed the mechanism the event ring then
\* caught on tape: a callback's one-shot wake bails against the reclaim's hold, and the
\* miss path releases and exits with no post-release rendezvous. Four explicit steps now:
\* acquire ("rhold"), peek snapshot ("rhit"/"rmiss" - the verdict is a READ AT THAT MOMENT,
\* carried in the phase while the world moves on), then enter-pass or miss-release. Bail
\* actions fire whenever advancingVisible, so they land inside "rhold"/"rmiss" exactly as
\* on the tape.

\* TryAcquire won: the transient hold begins.
QDrainReclaimAcquire ==
  /\ qDrainPhase = "reclaim"
  /\ ~advancingVisible
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ qDrainPhase' = "rhold"
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The peek: a snapshot of drainable-work-NOW, recorded in the phase. The world (commits,
\* moves, completions, bails) keeps moving after this read - that staleness window is the
\* mechanism.
QDrainReclaimPeek ==
  /\ qDrainPhase = "rhold"
  /\ qDrainPhase' = (IF Len(waiters) > 0 /\ Head(waiters) \in taskDone THEN "rhit" ELSE "rmiss")
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Peek saw work: TryReclaim returns true holding the latch; the do-while loops back into
\* the pass (with its do-top signal clear).
QDrainReclaimEnterPass ==
  /\ qDrainPhase = "rhit"
  /\ qDrainPhase' = "pass"
  /\ drainSignal' = FALSE  \* the do-top Exchange on loop re-entry
  /\ qDrainedAny' = FALSE  \* new sub-pass
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* Peek saw nothing: release. Two-cell shipped code (~PendingWordLatch) returns false with
\* no post-release rendezvous (THE hole: a bail that landed after the peek snapshot is
\* released into a world where its token will never be read - the field strand). With the
\* pending word, the release Exchange reads the deposit atomically and serves it.
QDrainReclaimMissRelease ==
  /\ qDrainPhase = "rmiss"
  /\ IF PendingWordLatch /\ advancingPending
       THEN /\ IF WeakAdvancerRelease
                 THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
                 ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
            /\ advancingPending' = FALSE
            /\ qDrainPhase' = "pendReacq"
            /\ drainerActive' = drainerActive
       ELSE /\ IF WeakAdvancerRelease
                 THEN WeakWriteOk(advancing', advancingVisible', FALSE, advancingVisible)
                 ELSE FencedWriteOk(advancing', advancingVisible', FALSE)
            /\ advancingPending' = advancingPending
            /\ qDrainPhase' = "none"
            /\ drainerActive' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

(* The PendingWordLatch serve route: the release Exchange consumed the pending bit and
   the releaser is obligated. The Exchange-then-CAS window from the code is the pendReacq
   phase: re-acquire wins continue the pass (with its do-top clear), losses delegate to
   the winner's exhaustive pass. *)

QDrainPendReacqWin ==
  /\ qDrainPhase = "pendReacq"
  /\ ~advancingVisible
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ qDrainPhase' = "pass"
  /\ drainSignal' = FALSE  \* the continued pass's do-top Exchange
  /\ qDrainedAny' = FALSE
  /\ drainerActive' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters,
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

QDrainPendReacqLose ==
  /\ qDrainPhase = "pendReacq"
  /\ advancingVisible
  /\ qDrainPhase' = "none"
  /\ drainerActive' = FALSE
  \* The re-acquire CAS is the same word primitive: losing it re-deposits the obligation on
  \* the winner (who may itself be a stale-verdict reclaim hold - its release then serves).
  /\ advancingPending' = TRUE
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars>>

\* TryReclaim's TryAcquire lost to a concurrent acquirer: return false, the do-while exits.
\* The winner owns the chain (its own pass + recheck).
QDrainReclaimLose ==
  /\ qDrainPhase = "reclaim"
  /\ advancingVisible
  /\ qDrainPhase' = "none"
  \* PendingWordLatch: TryReclaim's failed acquire deposits like every contended acquirer -
  \* the holder we lost to may be a stale-verdict rmiss hold whose release must learn of us.
  /\ advancingPending' = (IF PendingWordLatch THEN TRUE ELSE advancingPending)
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters, qDrainedAny,
                 loc, taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* The drainer chain exits when, post-release, the do-while's TryReclaim preconditions don't
\* match (queue empty or head not done). Clears drainerActive. Real code's do-while exits
\* and the calling method returns; subsequent stranded counts wait for a new callback entry.
DrainerChainExit ==
  /\ ~PassOnceDrain  \* legacy; pass-once exits via QDrainRecheckMissed / QDrainReclaim arms
  /\ drainerActive
  /\ ~advancing
  /\ ~(drainSignal /\ Len(waiters) > 0 /\ Head(waiters) \in taskDone)
  \* Slot-mode reclaim precondition must also not hold (the slot drainer's do-while keeps
  \* going while a bailed slot waiter is reclaimable).
  /\ ~(SlotReclaimEnabled /\ drainSignal /\ ~escalated /\ hasSlot
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
\* IS a stranded signal (drainSignal), and TryAcquire succeeds (~advancingVisible).
\* The body matches a successful TryAcquire + DrainReadyWaiters entry: claim advancer,
\* mark drainerActive so subsequent AdvancerReclaim transitions can fire.
ExecPostEscalationNudge ==
  /\ ~SplitCountCommit  \* fused-path approximation; the split path runs ExecEscalationNudgeStep
  /\ NudgeEnabled
  /\ escPhase = "idle"
  /\ escalated      \* nudge is only meaningful after escalation has happened in this run
  /\ ~advancingVisible
  /\ drainSignal
  /\ advancing' = TRUE
  /\ advancingVisible' = TRUE
  /\ drainerActive' = TRUE
  \* Acquire = pass start, signal consumed (the nudge enters DrainReadyWaiters).
  /\ drainSignal' = FALSE
  /\ UNCHANGED <<publish_vars, tail_vars, storeCount, slot_vars, waiters,
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
       accumulate on the same counter; ActivatedAtMostOnce (<=2 for failed: original
       dispatch + substitute dispatch) holds for a single recovery cycle. The planned
       recovery-split (a distinct substitute identity) collapses this to a uniform <=1 and
       lets the bound CATCH a recovery double-activation instead of tolerating it.
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
    \* Phase A bounds recovery to ONE failure per run: a substitute draws a fresh identity from
    \* the NumItems pool, so a 2nd failure (needing failed + substitute = 2 more identities)
    \* exhausts NumItems=3 - a model resource artifact, not a real strand (the system creates
    \* recovery flows on demand). Multi-failure / recovery-on-recovery is Phase B (a substitute
    \* identity space). failed = {} also subsumes "i is not itself a substitute".
    /\ (RecoverySplit => failed = {})
    /\ failed' = failed \cup {i}
    /\ loc' = [loc EXCEPT ![i] = "Recovering"]
    \* A sync throw out of ExecuteAuto releases any tenure the dispatch acquired (the
    \* builder's sync-faulted path round-trips TryGetVoidTask, clearing _started).
    /\ tenure' = IF tenure = i THEN NoItem ELSE tenure
    \* SplitRecoveryInlineAct: the throw does NOT touch the publish - the rollback is
    \* ClearExecutingItem's, a LATER step (ExecFaultClearWon), racing the C-path's claim.
    \* Fused (~toggle): the shipped executor-wins-only modeling.
    /\ IF hasExecuting /\ executingItem = i /\ ~SplitRecoveryInlineAct
         THEN \* Deferred-published failed item: roll back the publish.
              /\ hasExecuting' = FALSE
              /\ hasExecutingVisible' = FALSE
              /\ executingItem' = NoItem
              /\ executingItemVisible' = NoItem
         ELSE UNCHANGED publish_vars
    /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, activations, callbackFired, esc_vars, drainer_vars,
                   drainPhase, drainItem, drainRemaining, pendingHeadActivation, assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, activatedSlot, slotStomped, recoveryOf, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* SplitRecoveryInlineAct: the executor's ClearExecutingItem won-claim as its own step
\* (Pipeline.cs 2230-2241): after the fault parked the item, the in-lock claim finds the
\* pending flag still set (the advancer's C-path did not consume it) and tears the publish
\* down. The LOST arm needs no action of its own - AdvancerCPathFire consuming the still-up
\* publish IS the advancer winning (and, the fault being lock-invisible, IS the post-mortem
\* grant when it lands after the park). Recovery (RecoverInstall*/RecoverRefuse) is gated on
\* the claim having resolved either way - the code's program order: TryRecoverItemFailure
\* runs strictly after ClearExecutingItem returned.
ExecFaultClearWon ==
  /\ SplitRecoveryInlineAct
  /\ \E i \in Item :
       /\ loc[i] = "Recovering"
       /\ recoveryOf[i] = NoItem  \* first-level (the substitute's own fault stays fused)
       /\ hasExecuting /\ executingItem = i  \* the publish still holds the parked item: claim won
       /\ hasExecuting' = FALSE
       /\ hasExecutingVisible' = FALSE
       /\ executingItem' = NoItem
       /\ executingItemVisible' = NoItem
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters, loc,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars>>

\* RecoverItem wins-path: no prior waiter in flight (_waiters.Count is 0), inline-activate
\* the substitute. Mirrors the post-fix code at the new RecoverItem in Pipeline.cs. The
\* substitute reuses the failed item's identity; its activation count goes up by one.
RecoverItemWins ==
  /\ ~RecoverySplit  \* legacy identity-reuse; RecoverInstall* is the distinct-identity split
  /\ \E i \in Item :
    /\ loc[i] = "Recovering"
    /\ storeCount = 0  \* the shipped guard, restored (was TEMP-disabled for pre-fix bug repro)
    /\ ~hasTail
    /\ escPhase = "idle"
    /\ loc' = [loc EXCEPT ![i] = "Executing"]
    /\ activations' = [activations EXCEPT ![i] = @ + 1]
    /\ activatedSlot' = i  \* substitute takes over the slot
    /\ liveActivation' = TRUE  \* recovery activation is NOT gated (the code doesn't)
    /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                   taskDone, callbackFired, failed, esc_vars, drainer_vars>>

\* RecoverItem loses-path: a prior waiter is still in flight, defer activation. The
\* substitute publishes via _executingItem / _hasExecutingItem so the advancer's C-path
\* picks it up when the prior pipeline task completes. Mirrors SourceYieldDeferred's
\* publish handshake. Pre-fix code's ActivateHeadItem on this path would overwrite the
\* prior reader's binding and is the bug NoSimultaneousActiveReader catches.
RecoverItemLoses ==
  /\ ~RecoverySplit
  /\ \E i \in Item :
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
   Distinct-identity recovery (RecoverySplit, backlog #2/#5)

   The failed item parks in "Recovering" carrying a completion obligation; a
   FRESH substitute j is installed bound to it (recoveryOf[j] = i) and flows the
   NORMAL lifecycle.
   Distinct identity makes the activation bound uniform <= 1 and the binding
   discharge checkable. Phase A models the first-level Executing-throw failure;
   recovery-on-recovery, policy-refuse, and the other failure-injection points
   (trailing fault on a committed waiter, pre-faulted at commit, mid-escalation)
   are later phases.
   =========================================================================== *)

\* Install, wins-path: no prior waiter (storeCount = 0), inline-dispatch the substitute.
\* Mirrors SourceYieldInline for a fresh item, plus the binding.
RecoverInstallWins ==
  /\ RecoverySplit
  /\ storeCount = 0
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ tenure = NoItem  \* the substitute dispatch's TryStart (see SourceYieldInline)
  /\ \E i \in Item :
       /\ loc[i] = "Recovering"
       /\ recoveryOf[i] = NoItem  \* i is a first-level failure (Phase A)
       \* SplitRecoveryInlineAct: recovery runs strictly after ClearExecutingItem resolved the
       \* claim (won: ExecFaultClearWon consumed the publish; lost: the C-path fire did).
       /\ (SplitRecoveryInlineAct => ~(hasExecuting /\ executingItem = i))
       /\ LET j == SubSlot IN     \* the reserved substitute identity
            /\ loc[j] = "Nowhere"
            /\ recoveryOf[j] = NoItem
            /\ j \notin failed
            /\ loc' = [loc EXCEPT ![j] = "Executing"]
            /\ activations' = [activations EXCEPT ![j] = @ + 1]
            /\ activatedSlot' = j
            /\ liveActivation' = TRUE  \* recovery activation is NOT gated
            /\ recoveryOf' = [recoveryOf EXCEPT ![j] = i]
            /\ tenure' = j
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped>>
  /\ ReadTurnMonitor  \* substitute activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Install, loses-path: a prior waiter is in flight (storeCount > 0), publish the substitute
\* via the deferred handshake for the advancer C-path. Mirrors SourceYieldDeferred / the legacy
\* RecoverItemLoses.
RecoverInstallLoses ==
  /\ RecoverySplit
  /\ storeCount > 0
  /\ ~hasTail
  /\ escPhase = "idle"
  /\ \E i \in Item :
       /\ loc[i] = "Recovering"
       /\ recoveryOf[i] = NoItem
       \* SplitRecoveryInlineAct: claim-resolved gate, as RecoverInstallWins - also protects
       \* the republish below from clobbering a pending (unresolved) publish of i.
       /\ (SplitRecoveryInlineAct => ~(hasExecuting /\ executingItem = i))
       /\ LET j == SubSlot IN
            /\ loc[j] = "Nowhere"
            /\ recoveryOf[j] = NoItem
            /\ j \notin failed
            /\ loc' = [loc EXCEPT ![j] = "Executing"]
            /\ recoveryOf' = [recoveryOf EXCEPT ![j] = i]
            /\ IF IsReferenceT
                 THEN FencedWriteOk(executingItem', executingItemVisible', j)
                 ELSE WeakWriteOk(executingItem', executingItemVisible', j, executingItemVisible)
            /\ IF WeakHasExecutingPublish
                 THEN WeakWriteOk(hasExecuting', hasExecutingVisible', TRUE, hasExecutingVisible)
                 ELSE FencedWriteOk(hasExecuting', hasExecutingVisible', TRUE)
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenure, activatedSlot,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped, readTurnStomped, liveActivation>>
  /\ UNCHANGED token_vars

\* Install for a DRAIN-side park (RecoveringInline; Phase C/D's RecoverWaiter, Pipeline.cs
\* ~1410-1443): the policy accepted, the substitute is activated UNCONDITIONALLY
\* (ActivateHeadItem at ~1418 - no count gate, no deferred-publish branch: the faulted head's
\* position is vacant by construction and the recovery item takes it over immediately) and
\* executes IN PLACE on the advancer thread, under the advancer latch the parked drain still
\* holds (advance = false return). It never publishes, never becomes the tail, never enters
\* the store - completion is RecoverInlineCompletes below. NO tenure interaction: the
\* substitute is a distinct recovery flow with its own promise, 
\* not a dispatch on the pipeline's shared promise - the executor's
\* TryStart tenure word is untouched.
RecoverInstallInline ==
  /\ RecoverySplit
  /\ \E i \in Item :
       /\ loc[i] = "RecoveringInline"
       /\ recoveryOf[i] = NoItem  \* first-level (a substitute's own fault is never re-recovered)
       /\ LET j == SubSlot IN
            /\ loc[j] = "Nowhere"
            /\ recoveryOf[j] = NoItem
            /\ j \notin failed
            /\ loc' = [loc EXCEPT ![j] = "Executing"]
            /\ activations' = [activations EXCEPT ![j] = @ + 1]
            /\ activatedSlot' = j
            /\ liveActivation' = TRUE  \* RecoverWaiter's unconditional ActivateHeadItem (~1418)
            /\ recoveryOf' = [recoveryOf EXCEPT ![j] = i]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped>>
  /\ ReadTurnMonitor  \* substitute activation writes activatedSlot; witness the read-turn stomp
  /\ UNCHANGED token_vars

\* In-place completion of a drain-side substitute: its pipeline task settled and the recovery
\* continuation runs CompleteRecoveryWaiter (Pipeline.cs ~1548/~1571) - the item completes at
\* the parked drain's position, store untouched (it was never committed). The bound failed
\* item discharges via BindingDischarge, which unparks the drain's rejoin (SlotDrainCount's
\* RecoveringInline gate) - the model's AdvanceAndDrainRecovery. The faulted-substitute
\* alternative is RecoverOnRecoveryFails (shared with the executor-side lifecycle). No
\* tenure interaction (see RecoverInstallInline).
RecoverInlineCompletes ==
  /\ RecoverySplit
  /\ \E j \in Item :
       /\ loc[j] = "Executing"
       /\ recoveryOf[j] # NoItem
       /\ loc[recoveryOf[j]] = "RecoveringInline"  \* drain-side substitute (in-place lifecycle)
       /\ j \in taskDone   \* the recovery continuation fired; GetResult observed (no fault face here)
       /\ j \notin failed
       /\ loc' = [loc EXCEPT ![j] = "Completed"]
       /\ activatedSlot' = DepthZeroClear({j})
       /\ liveActivation' = FALSE  \* CompleteRecoveryWaiter -> CompleteWaiterDeferred's clear
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars, recoveryOf,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped>>
  /\ ReadTurnMonitor  \* writes activatedSlot (DepthZeroClear); witness the read-turn stomp
  /\ UNCHANGED token_vars

\* The binding discharge: the substitute reached "Completed" (its normal lifecycle), so the
\* failed item it was bound to completes too (CompleteItem completes the bound failed flow with
\* the failure exception). A separate fair step - the window between the substitute's completion
\* and the discharge is the code's sequence inside CompleteItem. Covers both park flavors
\* (executor-side "Recovering" and drain-side "RecoveringInline").
BindingDischarge ==
  /\ RecoverySplit
  /\ \E j \in Item :
       /\ recoveryOf[j] # NoItem
       /\ loc[j] = "Completed"
       /\ LET i == recoveryOf[j] IN
            /\ loc[i] \in {"Recovering", "RecoveringInline"}
            /\ loc' = [loc EXCEPT ![i] = "Completed"]
            /\ activatedSlot' = DepthZeroClear({i})
            /\ liveActivation' = FALSE  \* the discharge completes via CompleteWaiterDeferred
       /\ recoveryOf' = [recoveryOf EXCEPT ![j] = NoItem]
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped>>
  /\ ReadTurnMonitor  \* writes activatedSlot (DepthZeroClear); witness the read-turn stomp
  /\ UNCHANGED token_vars

(* ===========================================================================
   Recovery Phase B (June 2026): policy-refuse + recovery-on-recovery.
   =========================================================================== *)

\* Policy refuses to recover (TryRecoverItemFailure returns false): the failed item completes
\* DIRECTLY with its own exception, no substitute installed. The other branch from a parked
\* first-level Recovering item (RecoverInstall installs a substitute); TLC explores both - the
\* policy decision is nondeterministic. RecoveryCompletes still holds (Recovering ~> Completed
\* via this direct route). Covers both park flavors: executor-side ("Recovering",
\* CompleteWaiter at RecoverItem/RecoverCommittedTailWaiterAsync) and drain-side
\* ("RecoveringInline", CompleteWaiterDeferred at RecoverWaiter ~1413 - the advance = true
\* return whose partition the park action already ran).
RecoverRefuse ==
  /\ RecoverySplit
  /\ \E i \in Item :
       /\ loc[i] \in {"Recovering", "RecoveringInline"}
       /\ recoveryOf[i] = NoItem  \* first-level
       \* SplitRecoveryInlineAct: the refuse is the other face of the SAME TryRecoverItemFailure
       \* call, so it carries the same claim-resolved ordering (executor-side park only; the
       \* RecoveringInline park has no deferred-publish race).
       /\ (SplitRecoveryInlineAct /\ loc[i] = "Recovering" => ~(hasExecuting /\ executingItem = i))
       \* Refuse and install are the two faces of ONE TryRecoverItemFailure call: once a
       \* substitute is bound to this park, the refuse face is spent. (Without this guard a
       \* post-install refuse double-resolves the park - completing the failed item while
       \* the substitute still runs - stranding the in-place substitute's completion guard
       \* and the binding discharge.)
       /\ ~\E j \in Item : recoveryOf[j] = i
       /\ loc' = [loc EXCEPT ![i] = "Completed"]
       /\ activatedSlot' = DepthZeroClear({i})
       /\ liveActivation' = FALSE  \* refuse completes directly via CompleteWaiterDeferred
  /\ UNCHANGED <<publish_vars, tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, failed, esc_vars, drainer_vars, recoveryOf,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation, tenure,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped>>
  /\ ReadTurnMonitor  \* writes activatedSlot (DepthZeroClear); witness the read-turn stomp
  /\ UNCHANGED token_vars

\* Recovery-on-recovery: a SUBSTITUTE j (recoveryOf[j] = k) itself fails while Executing. The
\* policy STRUCTURALLY refuses to recover a recovery item, so j completes
\* DIRECTLY (with both exceptions), AND the bound failed item k discharges too (CompleteItem with
\* the AggregateException). No new substitute - the chain is single-level by construction. This is
\* the "recovery fails one level up" path the code's try/finally must discharge; the model checks
\* that both j and k complete (no strand). j failing later (committed-then-trailing-fault) is
\* Phase C.
RecoverOnRecoveryFails ==
  /\ RecoverySplit
  /\ \E j \in Item :
       /\ loc[j] = "Executing"
       /\ recoveryOf[j] # NoItem  \* j is a substitute
       /\ j \notin failed
       /\ failed' = failed \cup {j}
       /\ LET k == recoveryOf[j] IN
            /\ loc' = [loc EXCEPT ![j] = "Completed", ![k] = "Completed"]  \* both direct
            /\ activatedSlot' = DepthZeroClear({j, k})
            /\ liveActivation' = FALSE  \* both complete directly via the retirement clear
       /\ recoveryOf' = [recoveryOf EXCEPT ![j] = NoItem]
       /\ tenure' = IF tenure = j THEN NoItem ELSE tenure
       \* Roll back j's publish if it was deferred (RecoverInstallLoses), mirroring ExecItemFailure.
       /\ IF hasExecuting /\ executingItem = j
            THEN /\ hasExecuting' = FALSE
                 /\ hasExecutingVisible' = FALSE
                 /\ executingItem' = NoItem
                 /\ executingItemVisible' = NoItem
            ELSE UNCHANGED publish_vars
  /\ UNCHANGED <<tail_vars, adv_vars, counters, slot_vars, waiters,
                 taskDone, activations, callbackFired, esc_vars, drainer_vars,
                 drainPhase, drainItem, drainRemaining, pendingHeadActivation,
                 assertFailed, nullActivation, qDrainPhase, qDrainedAny, advancingPending, callbackSignaled, slotStomped>>
  /\ ReadTurnMonitor  \* writes activatedSlot (DepthZeroClear); witness the read-turn stomp
  /\ UNCHANGED token_vars

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
RecoverItemWinsW == RecoverItemWins /\ UNCHANGED aux_vars_act /\ ReadTurnMonitor  \* writes activatedSlot
RecoverItemLosesW == RecoverItemLoses /\ UNCHANGED aux_vars
ExecFaultClearWonW == ExecFaultClearWon /\ UNCHANGED aux_vars
\* Queue token-WRITERS use the token-FREE aux0 variants (their bodies specify token_vars).
ExecCommitQueueVisibleWinsW == ExecCommitQueueVisibleWins /\ UNCHANGED aux_vars0
ExecCommitSlotCASWinsW == ExecCommitSlotCASWins /\ UNCHANGED aux_vars
ExecCommitSlotCASLosesW == ExecCommitSlotCASLoses /\ UNCHANGED aux_vars
ExecCommitSlotFieldsW == ExecCommitSlotFields /\ UNCHANGED aux_vars
ExecCommitSlotCountWinsW == ExecCommitSlotCountWins /\ UNCHANGED aux_vars_act /\ ReadTurnMonitor  \* writes activatedSlot
ExecCommitSlotCountLosesW == ExecCommitSlotCountLoses /\ UNCHANGED aux_vars
ExecCommitSlotWriteWinsW == ExecCommitSlotWriteWins /\ UNCHANGED aux_vars
ExecCommitSlotWriteLosesW == ExecCommitSlotWriteLoses /\ UNCHANGED aux_vars
ExecCommitSlotPublishW == ExecCommitSlotPublish /\ UNCHANGED aux_vars
ExecEscalationCASSlotTriW == ExecEscalationCASSlotTri /\ UNCHANGED aux_vars
ExecCommitQueueVisibleLosesW == ExecCommitQueueVisibleLoses /\ UNCHANGED aux_vars0
\* Writes liveActivation (the gated wasEmpty self-activate) but not activatedSlot: the
\* _act bundle (which carries neither) plus explicit UNCHANGED on the two it doesn't touch.
ExecCommitQueueCountWinsW == ExecCommitQueueCountWins /\ UNCHANGED aux_vars_act
  /\ UNCHANGED <<activatedSlot, readTurnStomped>>
ExecCommitQueueCountLosesW == ExecCommitQueueCountLoses /\ UNCHANGED aux_vars0
\* SplitCommitSelfActivate split arms. ARM/ActDone are plain PC steps (no activation) -> full
\* aux_vars. ActFire writes liveActivation (queue: not activatedSlot; slot: activatedSlot +
\* ReadTurnMonitor in the body), same bundle shape as their fused twins. Queue token-writers
\* use aux_vars0/aux_vars_act0 (token-free); bodies specify token_vars.
ExecCommitQueueCountArmW == ExecCommitQueueCountArm /\ UNCHANGED aux_vars0
ExecCommitQueueCountActFireW == ExecCommitQueueCountActFire /\ UNCHANGED aux_vars_act0
  /\ UNCHANGED <<activatedSlot, readTurnStomped>>
ExecCommitQueueCountActDoneW == ExecCommitQueueCountActDone /\ UNCHANGED aux_vars0
\* Slot self-act arms are token-NEUTRAL in this round (the conviction/restructure are modeled on
\* the QUEUE self-act path - the escalated/queue commit the convicted stack came from); they carry
\* UNCHANGED token_vars so SelfActLockFamily admits them fully specified.
ExecCommitSlotCountArmW == ExecCommitSlotCountArm /\ UNCHANGED aux_vars /\ UNCHANGED token_vars
ExecCommitSlotCountActFireW == ExecCommitSlotCountActFire /\ UNCHANGED aux_vars_act /\ UNCHANGED token_vars
ExecCommitSlotCountActDoneW == ExecCommitSlotCountActDone /\ UNCHANGED aux_vars /\ UNCHANGED token_vars
\* SplitCommitSelfActRecheck split arms. RECHECK is a plain PC step (no activation) -> aux_vars.
\* FIRE/VERIFY write liveActivation (queue: not activatedSlot; slot: activatedSlot + ReadTurnMonitor),
\* same bundle shape as the fused ActFire twins.
ExecCommitQueueRecheckW == ExecCommitQueueRecheck /\ UNCHANGED aux_vars0
ExecCommitQueueFireW == ExecCommitQueueFire /\ UNCHANGED aux_vars_act
  /\ UNCHANGED <<activatedSlot, readTurnStomped>>
ExecCommitQueueVerifyW == ExecCommitQueueVerify /\ UNCHANGED aux_vars_act0
  /\ UNCHANGED <<activatedSlot, readTurnStomped>>
ExecCommitSlotRecheckW == ExecCommitSlotRecheck /\ UNCHANGED aux_vars /\ UNCHANGED token_vars
ExecCommitSlotFireW == ExecCommitSlotFire /\ UNCHANGED aux_vars_act /\ UNCHANGED token_vars
ExecCommitSlotVerifyW == ExecCommitSlotVerify /\ UNCHANGED aux_vars_act /\ UNCHANGED token_vars
ExecEscalationEnqueueTailW == ExecEscalationEnqueueTail /\ UNCHANGED aux_vars
ExecEscalationCommitCountW == ExecEscalationCommitCount /\ UNCHANGED aux_vars_act /\ ReadTurnMonitor  \* writes activatedSlot
ExecCommitTailPublishQueueExecutorWinsW == ExecCommitTailPublishQueueExecutorWins /\ UNCHANGED aux_vars
ExecCommitTailPublishQueueExecutorLosesW == ExecCommitTailPublishQueueExecutorLoses /\ UNCHANGED aux_vars
ExecEscalationCASSlotW == ExecEscalationCASSlot /\ UNCHANGED aux_vars
ExecEscalationMoveSlotW == ExecEscalationMoveSlot /\ UNCHANGED aux_vars
\* Writes liveActivation (gated wasEmpty, fused escalation form) but not activatedSlot -
\* same bundle shape as ExecCommitQueueCountWinsW.
ExecEscalationEnqueueNewW == ExecEscalationEnqueueNew /\ UNCHANGED aux_vars_act
  /\ UNCHANGED <<activatedSlot, readTurnStomped>>
ExecutorRegistersCallbackW == ExecutorRegistersCallback /\ UNCHANGED aux_vars
ExecutorInlineCallbackBecomesAdvancerW == ExecutorInlineCallbackBecomesAdvancer /\ UNCHANGED aux_nophase
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ qDrainedAny' = (IF PassOnceDrain THEN FALSE ELSE qDrainedAny)
ExecutorInlineCallbackBailsOutW == ExecutorInlineCallbackBailsOut /\ UNCHANGED aux_nopend
  /\ advancingPending' = (IF PendingWordLatch THEN TRUE ELSE advancingPending)
SlotDrainerReleaseW == SlotDrainerRelease
SlotCallbackBailsOutW == SlotCallbackBailsOut /\ UNCHANGED aux_nopend
  /\ advancingPending' = (IF PendingWordLatch THEN TRUE ELSE advancingPending)
SlotCallbackBailsDuringEscalationW == SlotCallbackBailsDuringEscalation /\ UNCHANGED aux_vars
\* Acquire entries write the pass PC under PassOnceDrain (they enter DrainReadyWaiters,
\* whose do-top clear their actions already fuse).
CallbackBecomesAdvancerW == CallbackBecomesAdvancer /\ UNCHANGED aux_nophase
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ qDrainedAny' = (IF PassOnceDrain THEN FALSE ELSE qDrainedAny)
\* Bails deposit the pending bit atomically with their failed acquire (the one-word property).
CallbackBailsOutW == CallbackBailsOut /\ UNCHANGED aux_nopend
  /\ advancingPending' = (IF PendingWordLatch THEN TRUE ELSE advancingPending)
AdvancerCPathW == AdvancerCPath /\ UNCHANGED aux_nophase_act /\ ReadTurnMonitor  \* writes activatedSlot
AdvancerCPathArmW == AdvancerCPathArm /\ UNCHANGED aux_nophase
AdvancerCPathFireW == AdvancerCPathFire /\ UNCHANGED aux_nophase_act /\ ReadTurnMonitor  \* writes activatedSlot
AdvancerCPathFireMissW == AdvancerCPathFireMiss /\ UNCHANGED aux_nophase
AdvancerCPathFireSkipW == AdvancerCPathFireSkip /\ UNCHANGED aux_nophase
AdvancerCPathFirePeekDeclineW == AdvancerCPathFirePeekDecline /\ UNCHANGED aux_nophase
AdvancerCPathClearAtCountW == AdvancerCPathClearAtCount /\ UNCHANGED aux_nophase
AdvancerCPathLeaveAtCountW == AdvancerCPathLeaveAtCount /\ UNCHANGED aux_nophase
AdvancerCPathLockExitW == AdvancerCPathLockExit /\ UNCHANGED aux_nophase
\* Queue C-path pending pre-check wrappers (CPathPendingPreCheck). Pure qDrainPhase steps,
\* so aux_nophase (aux minus the queue-drain PC) covers the rest.
AdvancerCPathPreEnterW == AdvancerCPathPreEnter /\ UNCHANGED aux_nophase
AdvancerCPathPreSkipW == AdvancerCPathPreSkip /\ UNCHANGED aux_nophase
AdvancerCPathPreReadTrueW == AdvancerCPathPreReadTrue /\ UNCHANGED aux_nophase
AdvancerCPathPreReadFalseW == AdvancerCPathPreReadFalse /\ UNCHANGED aux_nophase
AdvancerCPathPreEnterSplitW == AdvancerCPathPreEnterSplit /\ UNCHANGED aux_nophase
AdvancerCPathPreSkipSplitW == AdvancerCPathPreSkipSplit /\ UNCHANGED aux_nophase
AdvancerReleaseW == AdvancerRelease /\ UNCHANGED aux_vars
AdvancerReclaimW == AdvancerReclaim /\ UNCHANGED aux_vars
DrainerChainExitW == DrainerChainExit /\ UNCHANGED aux_vars
ExecPostEscalationNudgeW == ExecPostEscalationNudge /\ UNCHANGED aux_nophase
  /\ qDrainPhase' = (IF PassOnceDrain THEN "pass" ELSE qDrainPhase)
  /\ qDrainedAny' = (IF PassOnceDrain THEN FALSE ELSE qDrainedAny)
ExecEscalationNudgeStepW == ExecEscalationNudgeStep /\ UNCHANGED aux_relmin
SlotDrainerTailRerouteEscalatedW == SlotDrainerTailRerouteEscalated /\ UNCHANGED aux_nophase
SlotDrainerTailExitW == SlotDrainerTailExit /\ UNCHANGED aux_nopend
QDrainReleaseStepW == QDrainReleaseStep /\ UNCHANGED aux_relmin
QDrainRecheckSeenW == QDrainRecheckSeen /\ UNCHANGED aux_nophase
QDrainRecheckMissedW == QDrainRecheckMissed /\ UNCHANGED aux_nophase
QDrainReclaimAcquireW == QDrainReclaimAcquire /\ UNCHANGED aux_nophase
QDrainReclaimPeekW == QDrainReclaimPeek /\ UNCHANGED aux_nophase
QDrainReclaimEnterPassW == QDrainReclaimEnterPass /\ UNCHANGED aux_nophase
QDrainReclaimMissReleaseW == QDrainReclaimMissRelease /\ UNCHANGED aux_relmin
QDrainReclaimLoseW == QDrainReclaimLose /\ UNCHANGED aux_relmin
QDrainPendReacqWinW == QDrainPendReacqWin /\ UNCHANGED aux_nophase
QDrainPendReacqLoseW == QDrainPendReacqLose /\ UNCHANGED aux_relmin
PropagateAdvancingW == PropagateAdvancing /\ UNCHANGED aux_vars
PropagateHasExecutingW == PropagateHasExecuting /\ UNCHANGED aux_vars
PropagateExecutingItemW == PropagateExecutingItem /\ UNCHANGED aux_vars

\* The self-act lock hold (SplitCommitSelfActRecheck): RECHECK, FIRE and VERIFY act from this
\* phase window, which models the single _activationLock hold (Pipeline.cs 1127-1144). Vacuous
\* whenever SplitCommitSelfActRecheck = FALSE (the multi-step phases never persist).
SelfActLockHeld ==
  /\ SplitCommitSelfActRecheck
  /\ escPhase \in {"q_selfact_fire", "q_selfact_dofire", "q_selfact_verify",
                   "slot_selfact_fire", "slot_selfact_dofire", "slot_selfact_verify"}

\* Mutual exclusion of _activationLock, encoded as a TRANSITION constraint: while the committer
\* holds the lock (SelfActLockHeld) no OTHER thread may GRANT (drive liveActivation FALSE -> TRUE
\* via an ActivateHeadItem - the commit gate for other items, the C-path license family, any
\* gated / drain / recovery activation). The lock-FREE retirement clears (TRUE -> FALSE) and every
\* non-granting action stay enabled; the self-act family itself is exempt (it IS the lock holder,
\* re-admitted at Next's top level). Vacuous whenever SplitCommitSelfActRecheck = FALSE, so every
\* prior config is bit-identical.
NoForeignGrant == ~SelfActLockHeld \/ ~(liveActivation = FALSE /\ liveActivation' = TRUE)

\* The lock-holder's own continuation - exempt from NoForeignGrant so FIRE may grant under the hold.
SelfActLockFamily ==
  \/ ExecCommitQueueCountArmW \/ ExecCommitQueueCountActFireW \/ ExecCommitQueueCountActDoneW
  \/ ExecCommitQueueRecheckW  \/ ExecCommitQueueFireW         \/ ExecCommitQueueVerifyW
  \/ ExecCommitSlotCountArmW  \/ ExecCommitSlotCountActFireW  \/ ExecCommitSlotCountActDoneW
  \/ ExecCommitSlotRecheckW   \/ ExecCommitSlotFireW          \/ ExecCommitSlotVerifyW

NextBody ==
  \/ \E i \in Item : CompleteTaskW(i)
  \/ \E i \in Item : CompleteTaskUnactivatedW(i)  \* possible, never obligated - no WF below
  \/ SourceYieldInline
  \/ SourceYieldDeferredW
  \/ SourceYieldElide
  \/ SourceYieldElideArm
  \/ SourceYieldElideAct
  \/ ExecSetTailW
  \/ ExecItemFailure
  \/ ExecFaultClearWonW
  \/ RecoverItemWinsW
  \/ RecoverItemLosesW
  \/ RecoverInstallWins
  \/ RecoverInstallLoses
  \/ RecoverInstallInline
  \/ RecoverInlineCompletes
  \/ BindingDischarge
  \/ RecoverRefuse
  \/ RecoverOnRecoveryFails
  \/ ExecCommitTailExecutorWins
  \/ ExecCommitTailExecutorLoses
  \/ ExecCommitTailRecovers
  \/ ExecCommitQueueVisibleWinsW
  \/ ExecCommitQueueVisibleLosesW
  \/ ExecCommitQueueCountWinsW
  \/ ExecCommitQueueCountLosesW
  \/ ExecCommitQueueCountArmW
  \/ ExecCommitQueueCountActFireW
  \/ ExecCommitQueueCountActDoneW
  \/ ExecCommitSlotCASWinsW
  \/ ExecCommitSlotCASLosesW
  \/ ExecCommitSlotFieldsW
  \/ ExecCommitSlotCountWinsW
  \/ ExecCommitSlotCountLosesW
  \/ ExecCommitSlotCountArmW
  \/ ExecCommitSlotCountActFireW
  \/ ExecCommitSlotCountActDoneW
  \/ ExecCommitSlotWriteWinsW
  \/ ExecCommitSlotWriteLosesW
  \/ ExecCommitSlotPublishW
  \/ ExecEscalationEnqueueTailW
  \/ ExecEscalationCommitCountW
  \/ ExecEscalationNudgeStepW
  \/ ExecCommitTailPublishQueueExecutorWinsW
  \/ ExecCommitTailPublishQueueExecutorLosesW
  \/ ExecEscalationCASSlotW
  \/ ExecEscalationCASSlotTriW
  \/ ExecEscalationMoveSlotW
  \/ ExecEscalationEnqueueNewW
  \/ ExecutorRegistersCallbackW
  \/ ExecutorInlineCallbackBecomesAdvancerW
  \/ ExecutorInlineCallbackBailsOutW
  \/ SlotDrainClaim
  \/ SlotDrainClaimEntry
  \/ SlotDrainerReclaimEntry
  \/ SlotClaimExchange
  \/ SlotClaimMiss
  \/ SlotClaimRead
  \/ SlotClaimClear
  \/ SlotClaimPeekLive
  \/ SlotClaimPeekDone
  \/ SlotClaimOwnWin
  \/ SlotClaimOwnLose
  \/ SlotClaimConsume
  \/ SlotClaimFailRelease
  \/ SlotClaimPendReacqWin
  \/ SlotClaimPendReacqLose
  \/ SlotServeReacqEscalated
  \/ SlotServeReacqSlot
  \/ SlotServeReacqLose
  \/ SlotDrainCompleteClear
  \/ SlotDrainCount
  \/ SlotDrainCompleteLegacy
  \/ SlotDrainCompleteChainSlot
  \/ SlotDrainCompleteChainHandoff
  \/ SlotDrainHandoffReclaim
  \/ SlotDrainHandoffTrust
  \/ SlotDrainCompleteCPath
  \/ SlotCPathPreReadTrue
  \/ SlotCPathPreReadFalse
  \/ SlotCPathPreActTrue
  \/ SlotCPathPreSkipFalse
  \/ SlotHeadRecovers
  \/ ExecEscalationCompensate
  \/ SlotDrainerReclaim
  \/ SlotDrainerReleaseW
  \/ SlotCallbackBailsOutW
  \/ SlotCallbackBailsDuringEscalationW
  \/ CallbackBecomesAdvancerW
  \/ CallbackBailsOutW
  \/ CallbackSetSignal
  \/ CallbackAcquireWin
  \/ CallbackAcquireLose
  \/ SlotCallbackSpent
  \/ AdvancerDrainHead
  \/ DrainHeadRecovers
  \/ AdvancerCPathW
  \/ AdvancerCPathArmW
  \/ AdvancerCPathFireW
  \/ AdvancerCPathFireMissW
  \/ AdvancerCPathFireSkipW
  \/ AdvancerCPathFirePeekDeclineW
  \/ AdvancerCPathClearAtCountW
  \/ AdvancerCPathLeaveAtCountW
  \/ AdvancerCPathLockExitW
  \/ AdvancerCPathPreEnterW
  \/ AdvancerCPathPreSkipW
  \/ AdvancerCPathPreReadTrueW
  \/ AdvancerCPathPreReadFalseW
  \/ AdvancerCPathPreEnterSplitW
  \/ AdvancerCPathPreSkipSplitW
  \/ AdvancerReleaseW
  \/ AdvancerReclaimW
  \/ QDrainReleaseStepW
  \/ QDrainRecheckSeenW
  \/ QDrainRecheckMissedW
  \/ QDrainReclaimAcquireW
  \/ QDrainReclaimPeekW
  \/ QDrainReclaimEnterPassW
  \/ QDrainReclaimMissReleaseW
  \/ QDrainReclaimLoseW
  \/ QDrainPendReacqWinW
  \/ QDrainPendReacqLoseW
  \/ SlotDrainerTailRerouteEscalatedW
  \/ SlotDrainerTailExitW
  \/ DrainerChainExitW
  \/ ExecPostEscalationNudgeW
  \/ PropagateAdvancingW
  \/ PropagateHasExecutingW
  \/ PropagateExecutingItemW

\* The self-act lock family is re-admitted at top level (exempt from NoForeignGrant so FIRE may
\* grant); every other action runs under NoForeignGrant (no foreign grant while the lock is held).
\* When SplitCommitSelfActRecheck = FALSE, NoForeignGrant is vacuous and SelfActLockFamily is a
\* subset of NextBody, so Next reduces EXACTLY to the prior NextBody disjunction.
\* Consumable-token round: token_vars are folded into the aux bundles (so W-wrappers carry them
\* in BOTH Next and the WF_vars fairness conjuncts) and into each raw action's UNCHANGED; the
\* commit token-writers use token-free aux variants (aux_vars0 / aux_vars_act0) and specify
\* token_vars in their bodies. So Next needs no token wrap - it reduces to the prior relation, and
\* when the toggles are FALSE token_vars stay constant so the state count is preserved.
Next ==
  \/ SelfActLockFamily
  \/ (NextBody /\ NoForeignGrant)

Spec == Init /\ [][Next]_vars
        \* Fairness: assume the executor and advancer chain make progress.
        \* W-suffixed forms keep aux_vars constrained; liveness evaluates these actions
        \* independently of Next.
        /\ WF_vars(SourceYieldInline \/ SourceYieldDeferredW)
        \* The elision act is the executor's unconditional next program step once armed
        \* (the armed state disables every other dispatch action, so without this WF a
        \* behavior could stutter armed forever and fake a strand). The ARM carries no
        \* fairness - eliding is a possibility, never an obligation (the locked arm's WF
        \* above covers dispatch progress); same for the fused elide. Vacuous when
        \* LockFreeIdleDispatch = FALSE (the act is never enabled).
        /\ WF_vars(SourceYieldElideAct)
        /\ WF_vars(ExecSetTailW)
        \* Recovery is reactive to failure (no WF on ExecItemFailure - failure is a
        \* possibility, not an obligation). Once Recovering, the substitute must eventually
        \* be installed via one of the two paths; WF on the disjunction so liveness still
        \* holds when the gate's storeCount value toggles between branches.
        /\ WF_vars(RecoverItemWinsW \/ RecoverItemLosesW)
        \* Distinct-identity recovery: a parked failed item must RESOLVE (install a substitute
        \* OR the policy refuses and it completes directly), and a completed substitute must
        \* discharge its binding. Both are obligations. RecoverOnRecoveryFails carries NO WF -
        \* a substitute's own failure is a possibility, not an obligation (like ExecItemFailure).
        /\ WF_vars(RecoverInstallWins \/ RecoverInstallLoses \/ RecoverInstallInline
                   \/ RecoverRefuse)
        \* SplitRecoveryInlineAct: the won-claim is the executor's unconditional program step
        \* after the fault (ClearExecutingItem always runs before TryRecoverItemFailure);
        \* without WF the claim could stutter unresolved forever and fake a recovery strand.
        \* Vacuous when the toggle is FALSE (the action is never enabled).
        /\ WF_vars(ExecFaultClearWonW)
        \* The drain-side substitute's completion is the recovery continuation running
        \* CompleteRecoveryWaiter - an obligation once its task settles (taskDone via the
        \* fair CompleteTask).
        /\ WF_vars(RecoverInlineCompletes)
        /\ WF_vars(BindingDischarge)
        \* One fairness anchor over every commit-entry variant: the commit's first step
        \* eventually fires whichever route (fused, queue split, slot CAS-first, slot
        \* write-first) the toggles and the store state enable.
        /\ WF_vars(ExecCommitTailExecutorWins \/ ExecCommitTailExecutorLoses
                   \/ ExecCommitQueueVisibleWinsW \/ ExecCommitQueueVisibleLosesW
                   \/ ExecCommitSlotCASWinsW \/ ExecCommitSlotCASLosesW
                   \/ ExecCommitSlotWriteWinsW \/ ExecCommitSlotWriteLosesW)
        \* The split commit's count step is the executor finishing TryEscalateOrEnqueue -
        \* an unconditional program step once the enqueue landed.
        /\ WF_vars(ExecCommitQueueCountWinsW \/ ExecCommitQueueCountLosesW)
        \* SplitCommitSelfActivate arms: ARM then ACT are the executor's unconditional next
        \* program steps once the enqueue landed (same fairness the fused count step carries).
        \* SplitCommitSelfActRecheck adds RECHECK/FIRE/VERIFY as further unconditional in-lock
        \* program steps; folded into the same per-arm WF disjunction so the commit always
        \* progresses out of the self-act phase window back to idle.
        /\ WF_vars(ExecCommitQueueCountArmW \/ ExecCommitQueueCountActFireW
                     \/ ExecCommitQueueCountActDoneW
                     \/ ExecCommitQueueRecheckW \/ ExecCommitQueueFireW
                     \/ ExecCommitQueueVerifyW)
        /\ WF_vars(ExecCommitSlotCountArmW \/ ExecCommitSlotCountActFireW
                     \/ ExecCommitSlotCountActDoneW
                     \/ ExecCommitSlotRecheckW \/ ExecCommitSlotFireW
                     \/ ExecCommitSlotVerifyW)
        /\ WF_vars(ExecEscalationEnqueueTailW)
        /\ WF_vars(ExecEscalationCommitCountW)
        /\ WF_vars(ExecEscalationNudgeStepW)
        /\ WF_vars(ExecCommitTailPublishQueueExecutorWinsW \/ ExecCommitTailPublishQueueExecutorLosesW)
        /\ WF_vars(ExecEscalationCASSlotW \/ ExecEscalationCASSlotTriW)
        /\ WF_vars(ExecEscalationMoveSlotW)
        /\ WF_vars(ExecEscalationEnqueueNewW)
        /\ WF_vars(SlotCallbackBailsDuringEscalationW)
        /\ WF_vars(AdvancerDrainHead)
        \* AdvancerCPath / the lock-exit are the do-body's terminal lock step - WF so the
        \* drainer makes progress out of "cpath_lock" (one of activate / exit must fire when
        \* the clear does not). No WF over AdvancerCPathClearAtCount: the clear is a
        \* POSSIBILITY (it competes with the executor's own-commit Exchange and the activate
        \* arm), like ExecItemFailure. TLC still explores behaviors where it fires (it is in
        \* Next); the downstream D-path actions carry the fairness that must recover the
        \* cleared item, and EventuallyActivated reports the stranding if that coverage fails.
        \* The leave arm (the fix) is a program step out of the lock, so it carries WF like
        \* the activate/exit - the drainer must progress out of "cpath_lock".
        /\ WF_vars(AdvancerCPathW \/ AdvancerCPathLockExitW \/ AdvancerCPathLeaveAtCountW)
        \* The fire-skip is the gated fire's program-step alternative out of "cpath_armed"
        \* (the code's short-circuit falls through and the pass continues) - it carries the
        \* same fairness so the drainer exits the armed phase under a closed gate. The
        \* peek-decline (PeekGatedCPathLicense) is the third program-step alternative:
        \* fire / miss / gate-skip / peek-decline partition the armed states, so the
        \* disjunction stays total and the drainer always exits "cpath_armed".
        /\ WF_vars(AdvancerCPathArmW \/ AdvancerCPathFireW \/ AdvancerCPathFireMissW
                   \/ AdvancerCPathFireSkipW \/ AdvancerCPathFirePeekDeclineW)
        \* The queue C-path pending pre-check steps (CPathPendingPreCheck): the drainer's
        \* straight-line program steps out of "cpath_pre" / "cpath_pre_t" / "cpath_pre_f".
        \* The read faces partition on hasExecutingVisible, and enter/skip partition their
        \* captured phases, so the disjunction is total - the drainer always exits the pre
        \* phase (into "cpath_lock" or "pass"). Vacuous when CPathPendingPreCheck = FALSE.
        /\ WF_vars(AdvancerCPathPreEnterW \/ AdvancerCPathPreSkipW
                   \/ AdvancerCPathPreReadTrueW \/ AdvancerCPathPreReadFalseW
                   \/ AdvancerCPathPreEnterSplitW \/ AdvancerCPathPreSkipSplitW)
        /\ WF_vars(AdvancerReleaseW)
        /\ WF_vars(AdvancerReclaimW)
        \* Pass-once drain steps: the drainer thread's straight-line program steps.
        /\ WF_vars(QDrainReleaseStepW)
        /\ WF_vars(QDrainRecheckSeenW \/ QDrainRecheckMissedW)
        /\ WF_vars(QDrainReclaimAcquireW \/ QDrainReclaimLoseW)
        /\ WF_vars(QDrainReclaimPeekW)
        /\ WF_vars(QDrainReclaimEnterPassW \/ QDrainReclaimMissReleaseW)
        \* The pendReacq re-acquire CAS: an unconditional program step after the release
        \* Exchange consumed the deposit - one of the two arms must fire.
        /\ WF_vars(QDrainPendReacqWinW \/ QDrainPendReacqLoseW)
        /\ WF_vars(SlotDrainerTailRerouteEscalatedW \/ SlotDrainerTailExitW)
        /\ WF_vars(DrainerChainExitW)
        \* Nudge fairness: enabled iff NudgeEnabled constant is TRUE. If FALSE, the action
        \* never fires (its NudgeEnabled precondition is FALSE), so fairness over it is vacuous;
        \* we still include WF for symmetry. The interesting test is whether liveness holds.
        /\ WF_vars(ExecPostEscalationNudgeW)
        /\ WF_vars(SlotDrainClaim)
        \* Truthful slot commit steps (SplitSlotFieldOps): the executor's straight-line
        \* program continuation after the entry lands (fields after the CAS-first entry;
        \* publish after the write-first entry).
        /\ WF_vars(ExecCommitSlotFieldsW \/ ExecCommitSlotPublishW)
        /\ WF_vars(ExecCommitSlotCountWinsW \/ ExecCommitSlotCountLosesW)
        \* Truthful slot claim steps: the drainer's straight-line continuation after an
        \* entry routes to "cl_acq". The cl_acq dispatch partitions on hasSlot/taskDone
        \* (Exchange or peek verdicts vs miss); the later steps are unconditional program
        \* steps within their phases. The entries themselves follow SlotDrainClaim /
        \* SlotDrainerReclaim's fairness pattern.
        /\ WF_vars(SlotDrainClaimEntry)
        /\ WF_vars(SlotDrainerReclaimEntry)
        /\ WF_vars(SlotClaimExchange \/ SlotClaimMiss
                   \/ SlotClaimPeekDone \/ SlotClaimPeekLive)
        /\ WF_vars(SlotClaimRead)
        /\ WF_vars(SlotClaimClear)
        /\ WF_vars(SlotClaimOwnWin \/ SlotClaimOwnLose)
        /\ WF_vars(SlotClaimConsume)
        /\ WF_vars(SlotClaimFailRelease)
        \* The claim-fail pendReacq CAS: one of the two arms must fire post-release.
        /\ WF_vars(SlotClaimPendReacqWin \/ SlotClaimPendReacqLose)
        \* The consumed-deposit serve: the slot release's obligation re-acquire. One arm
        \* must fire (escalated reroute / slot re-enter / lost re-deposit).
        /\ WF_vars(SlotServeReacqEscalated \/ SlotServeReacqSlot \/ SlotServeReacqLose)
        \* SlotDrainCompleteClear under CompleteBeforeCount=TRUE runs FIRST (claimed ->
        \* completed); SlotDrainCount runs second. Under FALSE it doesn't fire at all
        \* (its guard requires CompleteBeforeCount). WF on the disjunction with
        \* SlotDrainCount keeps fairness anchored on "the drain's bookkeeping advances."
        /\ WF_vars(SlotDrainCompleteClear \/ SlotDrainCount)
        \* One WF over the disjunction: the complete variants are mutually exclusive
        \* (constant gate + drainRemaining partition + escalated branch) - the disjunction
        \* keeps fairness anchored on "the drain's step 3 eventually runs" rather than
        \* per-variant. Same for the handoff resolution pair (the one-shot re-peek).
        /\ WF_vars(SlotDrainCompleteLegacy \/ SlotDrainCompleteChainSlot
                   \/ SlotDrainCompleteChainHandoff \/ SlotDrainCompleteCPath)
        \* SPLIT slot C-path pre-check (CPathPendingPreCheck /\ SplitCPathPreCheck): the read
        \* faces are the drain's step-3 entry when they intercept the "counted" <=0 case (they
        \* partition on hasExecutingVisible, so one always fires); the act/skip are the
        \* straight-line continuation out of "cnt_pt"/"cnt_pf". Vacuous when the toggles are off.
        /\ WF_vars(SlotCPathPreReadTrue \/ SlotCPathPreReadFalse)
        /\ WF_vars(SlotCPathPreActTrue \/ SlotCPathPreSkipFalse)
        /\ WF_vars(SlotDrainHandoffReclaim \/ SlotDrainHandoffTrust)
        /\ WF_vars(ExecEscalationCompensate)
        /\ WF_vars(SlotDrainerReclaim)
        /\ WF_vars(SlotDrainerReleaseW)
        /\ WF_vars(SlotCallbackBailsOutW)
        /\ WF_vars(CallbackBecomesAdvancerW)
        /\ WF_vars(CallbackBailsOutW)
        \* Un-fused callbacks (SplitCallbackOps), queue AND slot arms: the set is the
        \* callback's first program step (WF so a ready item's callback eventually runs),
        \* and the acquire follows it - via the escalated route (AcquireWin/Lose), the
        \* slot claim entries (SlotDrainClaim/SlotDrainClaimEntry's signaled precondition),
        \* or the spent cleanup when the occupant was reclaimed out from under it.
        /\ WF_vars(CallbackSetSignal)
        /\ WF_vars(CallbackAcquireWin \/ CallbackAcquireLose \/ SlotCallbackSpent)
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
  /\ drainSignal \in BOOLEAN
  /\ qDrainPhase \in {"none", "pass", "recheck", "reclaim", "rhold", "rhit", "rmiss",
                      "pendReacq", "cpath_lock", "cpath_armed",
                      "cpath_pre", "cpath_pre_t", "cpath_pre_f"}
  /\ qDrainedAny \in BOOLEAN
  /\ advancingPending \in BOOLEAN
  /\ drainerActive \in BOOLEAN
  /\ failed \subseteq Item
  /\ drainPhase \in {"idle", "claimed", "completed", "counted", "handoff", "cl_acq", "cl_xchg", "cl_read",
                     "cl_peeked", "cl_own", "cl_exit", "cl_pendReacq", "sl_serve",
                     "cnt_pt", "cnt_pf"}
  /\ activatedSlot \in Item \cup {NoItem}
  /\ slotStomped \in BOOLEAN
  /\ readTurnStomped \in BOOLEAN
  /\ liveActivation \in BOOLEAN
  /\ tokenConsumed \subseteq Item
  /\ staleTokenRead \in BOOLEAN
  /\ commitWasCompleted \in BOOLEAN
  /\ cbWiredPrePublish \in BOOLEAN
  /\ drainItem \in Item \cup {NoItem}
  \* -1 floor: the slot claim can consume a committed-but-uncounted pair under the split
  \* commit (the same single-producer skew bound as BoundedCountSkew; the code's
  \* Debug.Assert(count >= -1) in DrainSlotInline).
  /\ drainRemaining \in -1..NumItems
  /\ pendingHeadActivation \in BOOLEAN
  /\ tenure \in Item \cup {NoItem}
  /\ assertFailed \in BOOLEAN
  /\ nullActivation \in BOOLEAN
  /\ callbackSignaled \subseteq Item
  /\ recoveryOf \in [Item -> Item \cup {NoItem}]

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
\* The escPhase enqueued-term is the June 2026 count-skew correction: under SplitCountCommit
\* an entry is appended to waiters one step before its increment lands, so a mid-commit
\* state carries one visible-but-uncounted entry. Mirrors the drainPhase = "claimed" term's
\* shape (counted-but-not-resident vs here resident-but-not-counted).
\* The slot_f term mirrors the queue enqueued-term: under SplitSlotFieldOps the slot pair
\* is resident (fields written) one step before its increment lands.
\* The claim-in-flight terms (SplitSlotFieldOps): between the claim's Exchange and its
\* clear, the claimed position's pair is a GHOST in the cell - the claim owns it (the +1),
\* and the cell's residue must not double-count. Only a successor commit's landed fields
\* (escPhase slot_f*) are a real occupant again. Built so the equation balances through
\* every healthy interleaving and breaks exactly at corruption: the clear wiping a
\* successor's landed pair desyncs count from residency PERMANENTLY - that violation is a
\* face of backlog #7, not a bookkeeping artifact.
CountConsistency ==
  LET claimInFlight == drainPhase \in {"cl_xchg", "cl_read"}
      \* In-window, the cell's residue is the ghost unless a successor re-CASed the flag
      \* (hasSlot back to TRUE - the claim's Exchange dropped it) AND its fields landed
      \* (past the slot_cas phases, where the cell still shows the ghost under the new
      \* owner's flag).
      slotTerm == IF slotItem # NoItem
                     /\ (~claimInFlight
                         \/ (hasSlot /\ escPhase \notin {"slot_cas_act", "slot_cas_noact"}))
                  THEN 1 ELSE 0
  IN storeCount = slotTerm + Len(waiters)
               \* "completed" is the new phase between SlotDrainCompleteClear and
               \* SlotDrainCount under CompleteBeforeCount=TRUE: drainItem is loc-Completed
               \* but the store still counts it (decrement happens next at SlotDrainCount).
               + (IF drainPhase \in {"claimed", "completed"} THEN 1 ELSE 0)
               + (IF claimInFlight THEN 1 ELSE 0)
               - (IF escPhase \in {"esc_enqueued", "q_enq_act", "q_enq_noact",
                                   "slot_f_act", "slot_f_noact",
                                   "slot_w_act", "slot_w_noact"} THEN 1 ELSE 0)

\* Drain-phase bookkeeping coherence. Note mid-drain commits are LEGAL: a successor can
\* reuse the freed slot and even trigger first escalation while the drainer is between its
\* claim and its decrement - the only ownership constraint is the advancer latch itself.
DrainConsistent ==
  \* Split from the fused <=> so the truthful claim's read phase fits: at "cl_read"
  \* drainItem holds whatever the read returned - the committed pair (forward direction
  \* must not require it) or NoItem (NoDefaultSlotClaim's subject, not this invariant's).
  \* cnt_pt / cnt_pf are the SPLIT slot C-path pre-check phases (CPathPendingPreCheck): a
  \* continuation of "counted" that still owns drainItem and the captured drainRemaining while
  \* the pending read/act interleaves - treated like "counted" throughout this invariant.
  /\ (drainPhase \in {"claimed", "completed", "counted", "cnt_pt", "cnt_pf"}) => (drainItem # NoItem)
  /\ (drainItem # NoItem) => (drainPhase \in {"claimed", "completed", "counted", "cl_read", "cnt_pt", "cnt_pf"})
  \* The recovery admission: a drain-side fault park (SlotHeadRecovers) re-locates the
  \* claimed item to RecoveringInline while the drain PC stays parked at "claimed", and the
  \* post-resolution rejoin runs the count/complete steps with the item already Completed
  \* (refuse / discharge ran first). Outside recovery the original clause is unchanged.
  \* Under CompleteBeforeCount=TRUE the "completed" phase also has the item already
  \* Completed (SlotDrainCompleteClear ran first).
  /\ (drainItem # NoItem /\ drainPhase # "cl_read") =>
       \/ loc[drainItem] = "Draining"
       \/ (drainPhase \in {"completed", "counted", "cnt_pt", "cnt_pf"} /\ loc[drainItem] = "Completed")
       \/ (RecoverySplit /\ drainItem \in failed
           /\ loc[drainItem] \in {"RecoveringInline", "Completed"})
  /\ \A i \in Item : (loc[i] = "Draining") => (drainItem = i)
  \* cl_pendReacq / sl_serve are the drainer phases that hold the method alive PAST a release
  \* (the obligation re-acquire windows: claim-fail exit and the main release's deposit serve).
  /\ drainPhase \notin {"idle", "cl_pendReacq", "sl_serve"} => (advancing /\ drainerActive)
  /\ drainPhase \in {"cl_pendReacq", "sl_serve"} => drainerActive
  /\ drainPhase \notin {"counted", "cnt_pt", "cnt_pf"} => drainRemaining = 0
  \* The slot decrement's -1 floor (claimed-but-uncounted, BoundedCountSkew's bound) is
  \* reachable once the split commit lets a claim land before the increment.
  /\ drainRemaining >= -1
  \* The handoff flag only exists in escalated runs (the drainer publishes it exactly when
  \* its chain obligation met an escalation).
  /\ pendingHeadActivation => escalated

\* Each command is dispatched-as-reader EXACTLY ONCE - inline (SourceYieldInline), C-path
\* (AdvancerCPath / SlotDrainCompleteCPath when the drain reaches the deferred publish), or
\* D-path (AdvancerDrainHead activating the next head); the _hasExecutingItem Exchange picks
\* exactly one of inline/C-path. So a non-failed item's bound is 1. A failed item activates a
\* second time ONLY because the recovery substitute reuses its identity (the original failed
\* dispatch + the substitute's dispatch) - bound 2. TIGHTENED June 2026 from the old
\* `<= 2 + (failed ? 1 : 0)`: that slack was stale from the double-activation hunt (it tolerated
\* the very double the lost-activation rounds since eliminated). Verified tight over the full
\* Contract tree. A double-activation of a normal item - or a triple via recovery - now
\* VIOLATES this instead of hiding under the slack; the recovery-split (distinct substitute
\* identity) will collapse even the failed case to 1, making the bound a uniform `<= 1`.
ActivatedAtMostOnce ==
  \A i \in Item : activations[i] <= (IF ~RecoverySplit /\ i \in failed THEN 2 ELSE 1)

\* Items in waiters are exactly those with loc in {InWaitersPending, InWaiters}.
WaitersConsistent ==
  /\ \A i \in Item : (loc[i] \in {"InWaitersPending", "InWaiters"}) <=> (\E k \in 1..Len(waiters) : waiters[k] = i)

\* Escalation phase invariants. escTail is exactly the item in loc = "InEscalation"
\* (if any), and escPhase tracks which step we're at.
EscalationConsistent ==
  /\ StoreEscalationConsistent
  \* Mid-escalation phases keep the tail in InEscalation; the split-commit enqueued phases
  \* have it appended to waiters already (InWaitersPending per WaitersConsistent) with only
  \* its count outstanding. Registration/inline-callback actions exclude the mid-commit
  \* item (i # escTail guards), so it cannot move to InWaiters - but the DRAIN can consume
  \* the visible-but-uncounted entry and complete it mid-commit (that consumability is the
  \* count-skew bug itself), so Completed is admitted.
  /\ (escTail # NoItem /\ escPhase \in {"publish_done", "cas_done", "move_done"})
       => (loc[escTail] = "InEscalation")
  \* SplitCommitSelfActivate queue PCs: the increment landed at ARM, item is InWaitersPending
  \* (counted now) or already drained to Completed in the ARM->ACT window.
  /\ (escPhase \in {"esc_enqueued", "q_enq_act", "q_enq_noact",
                    "q_selfact_fire", "q_selfact_done",
                    "q_selfact_dofire", "q_selfact_verify"})  \* SplitCommitSelfActRecheck sub-phases
       => (loc[escTail] \in {"InWaitersPending", "Completed"})
  \* Slot-split commit phases: CAS landed, fields pending = InSlotPending (CAS-first form);
  \* fields landed, flag pending = InSlotPending (TriState write-first form); fields landed,
  \* count pending = InSlot - or already hijack-claimed (Draining) or even completed by a
  \* stale-token reclaim that won the mid-commit window (the same consumability class the
  \* queue's Completed admission covers). RecoveringInline is that class's FAULT face: the
  \* hijack claim's GetResult faulted and SlotHeadRecovers parked the mid-commit pair.
  /\ (escPhase \in {"slot_cas_act", "slot_cas_noact", "slot_w_act", "slot_w_noact"})
       => (loc[escTail] = "InSlotPending")
  \* SplitCommitSelfActivate slot PCs join slot_f_*: increment landed at ARM, the slot occupant
  \* is InSlot (counted) or claimed/completed by DrainSlotInline in the ARM->ACT window.
  /\ (escPhase \in {"slot_f_act", "slot_f_noact",
                    "slot_selfact_fire", "slot_selfact_done",
                    "slot_selfact_dofire", "slot_selfact_verify"})  \* SplitCommitSelfActRecheck sub-phases
       => (loc[escTail] \in {"InSlot", "Draining", "Completed", "RecoveringInline"})
  /\ \A i \in Item : (loc[i] = "InEscalation") => (escTail = i)

\* Slot occupancy consistency. The slotItem field and the InSlot location always agree
\* on the identity (clauses 1+2). The hasSlot flag agrees with both, EXCEPT during the
\* mid-escalation "cas_done" / "move_done" phases where the executor's
\* Interlocked.Exchange(_hasSlot, 0) has fired (hasSlot = FALSE) but the slot fields
\* haven't yet been cleared - the slot is logically claimed but physically still populated
\* until the MoveSlot step. The escPhase = "idle" gating on clause 3 admits that window.
SlotConsistent ==
  LET inSlotExists == \E i \in Item : loc[i] = "InSlot"
      \* The TriState commit's data-then-license window: fields landed, flag pending.
      midWrite == escPhase \in {"slot_w_act", "slot_w_noact"}
  IN  /\ (slotItem # NoItem) <=> (inSlotExists \/ midWrite)
      /\ slotItem # NoItem => loc[slotItem] = (IF midWrite THEN "InSlotPending" ELSE "InSlot")
      \* The truthful claim's Exchange-to-clear window (cl_xchg/cl_read) legitimately holds
      \* hasSlot = FALSE with the fields still populated - the same shape as the escalation's
      \* cas_done window, drainer-side. The TriState consuming state (cl_own) needs NO
      \* exception: the word stays non-zero and the occupant stays InSlot until the consume
      \* flips both - preserving this invariant is the design's point.
      /\ (escPhase = "idle" /\ drainPhase \notin {"cl_xchg", "cl_read"})
           => (hasSlot <=> inSlotExists)

\* PostEscalationSlotEmpty (the loosened-CAS justification) lives in WaiterStore.tla, but
\* the TriState consuming hold can legitimately outlive an escalation that skipped it (the
\* quiescent-only CAS reads 2 as not-moved and proceeds; the drainer's release follows).
\* This quiescent form replaces it in Invariants; it reduces to the original whenever
\* cl_own is unreachable (every non-TriState configuration).
PostEscalationSlotQuiescent ==
  (escalated /\ escPhase = "idle" /\ drainPhase # "cl_own") => ~hasSlot

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

\* Slot consistency: activatedSlot only points at items that haven't completed yet.
\* EXPECTED to hold across all toggle settings; orthogonal to the stomp witness below.
ActivatedSlotConsistent ==
  (activatedSlot = NoItem)
  \/ loc[activatedSlot] \notin {"Completed", "Nowhere"}

\* No-stomp invariant: the slot-drain Complete clear must NOT wipe a successor's
\* activatedSlot publication. EXPECTED to HOLD under CompleteBeforeCount = TRUE
\* (the shipped ReorderFix - clear lands while count > 0, no concurrent activator
\* can fire) and EXPECTED to FAIL under CompleteBeforeCount = FALSE (the pre-fix
\* shape - clear lands after the count drop opens the inline-activation gate, so
\* a successor can have written activatedSlot = NEW and the unconditional clear
\* stomps it; Slon's PgDecoder NRE on CurrentExecutionControl, June 2026).
NoStompedActivation == ~slotStomped

\* No stale-token read: no COMMIT-side action ever read a Consumed waiter task's state
\* (ConsumableTaskTokens). The single-consumption ValueTask is readable only until a drain's
\* GetResult consumes it; a stale post-publication read (Pipeline.cs 1136 out-of-lock IsCompleted,
\* 1152 in-lock recheck, 1169 verify done-leg, 1185 callback-wiring IsCompleted - the convicted
\* thrower) observes a recycled promise box and throws (Slon MRVTSC.GetStatus token-validation).
\* EXPECTED FALSE under the shipped commit protocol (ConsumableTaskTokens=TRUE,
\* CommitOwnershipRestructure=FALSE - Pipeline_CommitTokenWitness.cfg) and HOLD under the
\* ownership restructure (both TRUE - Pipeline_CommitOwnershipFix.cfg), where every
\* post-publication decision keys on the pre-publication capture + store words alone.
NoStaleTokenRead == ~staleTokenRead

\* Adjudication probe for the restructure's VERIFY trigger (Pipeline_CommitOwnershipFix.cfg and the
\* store-words probe). Under the restructure the VERIFY cannot read task-done; it keys on
\* storeCount = 0 /\ waiters = <<>> (count re-read + producer-side taken). CLAIM: that two-way
\* store-word conjunction, at a restructure VERIFY pre-state, already implies the item is RETIRED
\* (loc = "Completed") - so the dropped task-done leg (and the pre-publication wasCompleted capture)
\* need NOT join it. HOLDS => store words suffice; VIOLATED => the capture/done must join = the
\* finding. Rests on complete-before-decrement (CompleteBeforeCount): a zero count re-read after
\* our own +1 proves the drain's clear (loc -> Completed) already landed. Meaningful only under
\* CommitOwnershipRestructure; trivially TRUE otherwise (the restructure VERIFY phase is unreached).
RestructureVerifyStoreWordsImplyRetired ==
  ( /\ CommitOwnershipRestructure
    /\ escPhase \in {"q_selfact_verify", "slot_selfact_verify"}
    /\ storeCount = 0
    /\ (IF escPhase = "q_selfact_verify" THEN waiters = <<>> ELSE slotItem # escTail) )
  => loc[escTail] = "Completed"

\* Non-vacuity probe for RestructureVerifyStoreWordsImplyRetired: CLAIMS the restructure VERIFY
\* trip antecedent (a verify pre-state with storeCount = 0 /\ waiters = <<>>) is never reached.
\* EXPECTED VIOLATED under the restructure = the store-word VERIFY trip genuinely fires (the drain
\* retired our just-granted item between FIRE and VERIFY), so RestructureVerifyStoreWordsImplyRetired
\* is a non-vacuous implication and the restructure's store-word verify does real work.
RestructureVerifyTripUnreached ==
  ~( /\ CommitOwnershipRestructure
     /\ escPhase = "q_selfact_verify"
     /\ storeCount = 0
     /\ waiters = <<>> )

\* Non-vacuity probe for the late-visible-deposit face (RestructureWireBeforePublish,
\* Pipeline_CommitTokenLateVisible.cfg). CLAIMS the early-deposit window is NEVER entered - no
\* reachable state where a pre-wired callback raised the drain signal before its item became
\* visible (cbWiredPrePublish). EXPECTED VIOLATED under the face toggle: the violation is the
\* coverage proof that TLC explored callback-before-visible interleavings, so the GREEN
\* NoStaleTokenRead + liveness verdict on that config is not vacuous. Trivially TRUE when the
\* toggle is FALSE (the window is never entered).
LateVisibleWindowUnreached == ~cbWiredPrePublish

\* Read-turn mutex: at most one flow holds the read turn at a time. NoStompedActivation catches
\* a CLEAR wiping a live reader (the NoItem NRE face); this catches an ACTIVATION granting the
\* turn to a second flow while the first still holds it. One root, several variations (none
\* canonical): ReadState.ReadPromise's single-tenant TryStart throwing "async method already
\* executing", the shared decoder re-armed under a live read, the slot stomped to NoItem - all
\* closed by the _liveReaderActive single-reader gate (Pipeline.cs, June 2026). tenure is NOT this
\* resource - it is activation-occupancy from the EXECUTOR's single dispatch, maintained only on
\* the inline-dispatch and recovery-install paths; the slot-commit, queue-commit, C-path and
\* wasEmpty self-activate paths bump activations / write activatedSlot without touching it, so the
\* read turn as a cross-path shared resource was never modeled and a collision among THEM was
\* invisible - the fidelity gap this witness fills.
\* EXPECTED FALSE with SingleReaderLock = FALSE (the shipped pre-lock shape, all other fixes on -
\* Pipeline_ReadTurnWitness.cfg) and HOLD with SingleReaderLock = TRUE.
ReadTurnMutex == ~readTurnStomped

\* Non-vacuity probe for CommitSelfActVerify (Pipeline_CommitSelfActVerifyProbe.cfg). CLAIMS the
\* VERIFY self-clear branch never fires - i.e. no reachable state parks at a *_selfact_verify phase
\* with the item ALREADY done (the exact pre-state of a VERIFY that clears a stuck grant). Expected
\* to be VIOLATED under the verify fix: a violation proves the fix's trip branch is genuinely
\* explored (the fix is doing real work), so the GREEN Pipeline_CommitSelfActVerifyFix.cfg run is
\* not vacuously green. Meaningless (trivially TRUE) when CommitSelfActVerify = FALSE.
VerifyTripBranchUnreached ==
  ~(escPhase \in {"q_selfact_verify", "slot_selfact_verify"} /\ escTail \in taskDone)

\* Non-vacuity probe for CommitSelfActVerifyOnTaken (Pipeline_CommitSelfActVerifyOnTakenProbe.cfg).
\* CLAIMS the EARLY-trip branch never fires - no reachable VERIFY pre-state where the item is
\* task-done AND taken (queue: producer-side emptiness; slot: the word no longer holds it) but NOT
\* yet Completed - i.e. the drain is between its take and its CompleteWaiterDeferred clear. Expected
\* VIOLATED under the OnTaken fix: a violation proves the early trigger genuinely fires in the
\* window the retirement-keyed shape cannot observe. Trivially TRUE when the toggle is FALSE.
VerifyOnTakenEarlyTripUnreached ==
  ~( /\ escPhase \in {"q_selfact_verify", "slot_selfact_verify"}
     /\ escTail \in taskDone
     /\ loc[escTail] # "Completed"
     /\ (IF escPhase = "q_selfact_verify" THEN waiters = <<>> ELSE slotItem # escTail) )

\* Interleaving-coverage probe for the OnTaken variant's suspect window (the c-check,
\* Pipeline_CommitSelfActOnTakenGrantProbe.cfg). CLAIMS TLC never reaches a state where a NEW grant
\* is live (liveActivation TRUE for a not-done item j) while some done item i sits taken-but-not-
\* retired (loc = "Draining": the drain's take landed, its CompleteWaiterDeferred clear still
\* pending). Expected VIOLATED under the OnTaken fix: the violation is the existence proof that
\* our-clear -> successor-grant -> (pending) drain-clear interleavings are explored, so the main
\* run's verdict on that window is meaningful. Trivially TRUE when the toggle is FALSE only if
\* unreachable there - run it ONLY against the OnTaken config.
OnTakenSuccessorGrantWindowUnreached ==
  ~( \E i, j \in Item :
       /\ i # j
       /\ loc[i] = "Draining" /\ i \in taskDone /\ activations[i] > 0
       /\ loc[j] # "Completed" /\ j \notin taskDone /\ activations[j] > 0
       /\ liveActivation )

\* Port-observable adjudication (Pipeline_CommitSelfActCountZeroProbe.cfg). The OnTaken trigger is
\* unsound because "taken" fires in the claimed/dequeued-but-not-yet-counted drain window, where the
\* store still counts the item (CountConsistency's claimed-phase +1) - the restore then nulls the
\* activated slot at depth >= 1. CLAIM: conjoining the committer's count re-read (Volatile, under
\* the lock; the committer is the producer, so a zero count with our own commit's +1 landed means
\* the drain consumed AND counted our item) restores soundness - at a VERIFY pre-state, task-done
\* AND taken AND storeCount = 0 TOGETHER imply the item is genuinely RETIRED (loc = "Completed"):
\* under CompleteBeforeCount the drain's Clear (loc -> Completed + the liveActivation release) runs
\* BEFORE its DecrementCount, so a zero count proves the clear already landed. Expected to HOLD
\* over the OnTaken variant's full state space - then "done /\ taken /\ count == 0" is the
\* committer-observable retirement proxy the port must use.
VerifyCountZeroImpliesRetired ==
  ( /\ escPhase \in {"q_selfact_verify", "slot_selfact_verify"}
    /\ escTail \in taskDone
    /\ (IF escPhase = "q_selfact_verify" THEN waiters = <<>> ELSE slotItem # escTail)
    /\ storeCount = 0 )
  => loc[escTail] = "Completed"

\* Diagnostic (activatedSlot-INDEPENDENT): count items that have been ACTIVATED and are still
\* live, straight off the activations counter. A queued-but-not-yet-head waiter is activations=0
\* (activation happens when it becomes the reader), so under a correct single-reader discipline
\* this is <= 1 at all times - the read turn counted without the slot's last-writer-wins blur.
\* Used to discriminate the two hypotheses for why ReadTurnMutex holds on the shipped model:
\* (a) the turn genuinely stays single, vs (b) the two-reader race (site-20 C-path grants M while
\* site-25 wasEmpty-commit would grant N) is simply not REACHED. If this also holds, the model
\* never has two live activated items at once - the race is unreached, a fidelity gap, and the
\* lock's necessity cannot yet be adjudicated here.
LiveActivated == { i \in Item :
      activations[i] > 0 /\ loc[i] \notin {"Completed", "Nowhere", "Recovering", "RecoveringInline"} }
AtMostOneLiveActivated == Cardinality(LiveActivated) <= 1

\* Reachability PROBE for the recovery inline-activate round
\* (Pipeline_RecoveryPostMortemProbe.cfg). NOT a defect witness: recovery ALWAYS SUBSTITUTES -
\* the substitute inherits the failed item's pipeline position INCLUDING its activation state,
\* so a second ActivateHeadItem over a faulted predecessor's grant is the intended semantics
\* (a naive "no two grants for one position without intervening retirement" invariant would
\* flag intended behavior; any exemption wide enough to admit the sanctioned substitution also
\* admits the audited interleaving - the two are state-indistinguishable at grant time). This
\* action property CLAIMS no step ever grants an item already parked Recovering/RecoveringInline:
\* the C-path's POST-MORTEM activation (its claim beat ClearExecutingItem's, the activate landed
\* after the fault park - AdvancerCPathFire consuming the still-up publish of a faulted item,
\* reachable only under SplitRecoveryInlineAct). Expected VIOLATED in the probe config - the
\* violation is the COVERAGE PROOF that the audit's interleaving is explored, which is what makes
\* the adjudication run's green verdict (ReadTurnMutex + AtMostOneLiveActivated + liveness over
\* the same space) meaningful. Meaningful only under RecoverySplit: the legacy RecoverItemWins
\* re-activates the Recovering identity by design (identity reuse), a false positive here.
PostMortemGrantUnreached ==
  [][ \A i \in Item : (activations'[i] > activations[i])
        => loc[i] \notin {"Recovering", "RecoveringInline"} ]_vars

\* Never null while live: once an activation has happened and the pipeline is not empty (some item
\* not yet Completed - including not-yet-yielded items, which are enqueued/depth-counted), the slot
\* must name an item, never NoItem. This is the shipped depth-0 clear's contract: the decoder reads
\* the slot during an active read, so a transient NoItem while live is the production NRE; a stale
\* non-null reference is safe. depth-0 clears only when the pipeline drains empty, so it HOLDS this.
\* The pre-fix identity-clear (the model's diverged shape) and the unconditional clear both VIOLATE
\* it - they null the slot while a later item is still pending.
NoNullWhileLive ==
  ((\E i \in Item : activations[i] > 0) /\ (\E i \in Item : loc[i] # "Completed"))
    => (activatedSlot # NoItem)

\* Combined safety invariants. Single name keeps the .cfg simple and makes adding a new
\* invariant a one-file change rather than a two-file change.
Invariants ==
  /\ TypeOK
  /\ CountConsistency
  /\ DrainConsistent
  /\ ActivatedAtMostOnce
  /\ WaitersConsistent
  /\ SlotConsistent
  /\ PostEscalationSlotQuiescent
  /\ EscalationConsistent
  /\ NoSimultaneousActiveReader
  \* NoOverstay (ActivatedSlotConsistent) is intentionally NOT asserted: depth-0's stale window
  \* (slot keeps a Completed item's reference until the pipeline drains empty) is documented-safe.
  /\ NoNullWhileLive
  /\ NoStompedActivation

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

\* The queue drain's D-arm never fires with nothing to peek (the unchecked
\* `_waiters.TryPeek` at DrainReadyWaiters ~1124 activating default(T) - the proven
\* NRE/exit-134, June 2026, dump crash_22130.dmp). EXPECTED FALSE with SplitCountCommit =
\* TRUE against the shipped partition (Pipeline_CountSkewWitness.cfg): a drain consumes a
\* visible-but-uncounted entry, DecrementCount returns -1, and the `count is 0` test
\* misroutes to the D-arm over an empty queue. StoreCountNonNegative (WaiterStore.tla) is
\* the companion witness for the skewed count itself. The quiet face (both commit and drain
\* sides skip the deferred publish's activation - the 2s field completion timeout) is NOT
\* yet TLC-visible: AdvancerCPath is a standing fair action where the code checks once per
\* drain pass (see Pipeline_CountSkewWitness.cfg). Both witnesses must hold once the
\* partition is redesigned against the split. Until then the unchecked TryPeek sites
\* (~1124 and the recovery advance ~1436) stay as known sharp edges.
NoNullActivation ==
  ~nullActivation

\* Every slot claim that returns TRUE is LICENSED: the pair it read is the pair some
\* commit fully published. The "cl_read" state with drainItem = NoItem is the unlicensed
\* default claim - a claim Exchange that won between a successor commit's _hasSlot CAS and
\* its field writes, reading the previous tenure's cleared fields (backlog #7's NRE face:
\* the code would GetResult a default ValueTask - which completes successfully - and
\* CompleteWaiterDeferred(default(T))). EXPECTED FALSE under SplitSlotFieldOps = TRUE
\* against the shipped claim protocol (Pipeline_SlotTearWitness.cfg) IF the stale-token
\* reclaim window is reachable; the companion faces are SlotConsistent (the wipe / the
\* stranded hijack victim) and EventuallyCompleted (the lost item). Torn reads as such are
\* expected - SPSC tears the same way - the property is that no claim ACTS on one.
NoDefaultSlotClaim ==
  (drainPhase = "cl_read") => (drainItem # NoItem)

\* Mid-escalation Executing-fault coverage (Phase D finding, TLC-adjudicated - the first cut
\* of this invariant claimed the overlap unreachable OUTRIGHT and TLC refuted it in 5s).
\* Two regimes:
\*
\* PRE-FAILURE (failed = {}): the overlap "an item is Executing while escPhase # idle" is
\* structurally unreachable - the executor is one thread (escalation and every split-commit
\* phase are synchronous segments of CommitTailWaiter, complete before the next
\* ExecuteItemAsync can throw) and every Executing-entry action (SourceYield*,
\* RecoverInstall*, RecoverItem*) is gated escPhase = "idle" /\ ~hasTail. So the FIRST
\* failure - ExecItemFailure's whole domain under the one-failure bound, and every
\* first-level injection point - can never itself occur mid-escalation. THIS invariant pins
\* that half.
\*
\* POST-FAILURE: the overlap is REAL and exercised. A drain-side park (DrainHeadRecovers /
\* SlotHeadRecovers fires on the advancer thread) leaves the executor's own item mid-
\* lifecycle; the substitute install then puts TWO items in "Executing" - truthful to the
\* code, where RecoverWaiter's substitute executes on the DRAINER thread concurrently with
\* the executor - and one of them commits while the other still executes (the slot-tier
\* shape: the executor's own item commits its slot pair while the IN-PLACE substitute is
\* Executing under the held latch). RecoverOnRecoveryFails (escPhase-agnostic, like
\* ExecItemFailure) is enabled across that overlap, so the substitute's failure DURING an
\* in-flight commit/escalation is explored, and its routing is adjudicated by the standing
\* invariants + RecoveryCompletes. No additional action needed: the escPhase-agnostic
\* failure actions cover the overlap that exists, and this invariant proves empty the
\* overlap that doesn't. (The first cut of this round let the drain-side substitute reach
\* the executor's tail/commit - believed "an over-approximation that adds interleavings,
\* none excluded". TLC refuted the "none excluded" half: the substitute's commit stole the
\* executor item's deferred publish (the Exchange-wins guard presumes the publish is the
\* committer's OWN) and the wasEmpty arm double-activated it - ActivatedAtMostOnce, 19
\* states. Hence the RecoveringInline split: drain-side substitutes complete in place, as
\* the code does.)
ExecutorQuiescentMidCommitPreFailure ==
  (escPhase # "idle" /\ failed = {}) => (\A i \in Item : loc[i] # "Executing")

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
\* drainSignal bump, and never get drained.
EventuallyCompleted ==
  \A i \in Item :
    (loc[i] \in {"Executing", "InTail", "InSlot", "InSlotPending", "InEscalation", "InWaitersPending", "InWaiters", "Draining"} /\ i \in taskDone)
      ~> (loc[i] = "Completed")

\* The recovery binding's liveness (RecoverySplit, backlog #2/#5): a failed item parked in
\* "Recovering" must eventually complete - the lifetime-alignment guarantee that
\* CompleteItem completes the bound failed flow. A failed item is NEVER
\* taskDone (it threw, never completed its task), so EventuallyCompleted's premise cannot cover
\* it; this is the property that catches a stranded failed flow (the original hang's shape, and
\* the "recovery fails one level up" hazard). Vacuous under ~RecoverySplit (the legacy
\* identity-reuse re-dispatches the failed item, which then completes via the normal paths).
RecoveryCompletes ==
  \A i \in Item : (loc[i] \in {"Recovering", "RecoveringInline"}) ~> (loc[i] = "Completed")

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

   1. [DONE - SplitCountCommit round] Inline OnWaiterTaskCompleted from
      CommitWaiter when wasEmpty && task done. No longer rolled into
      ExecCommitTail_: under SplitCountCommit the committed waiter enters the
      store (InWaitersPending) and the inline callback resolves as a separate
      transition (ExecutorInlineCallbackBecomesAdvancer / BailsOut). The count
      step's `i \notin taskDone` guard matches the code's `!IsCompleted`
      self-activate gate. Covered in every config that sets SplitCountCommit
      (Contract included).

   2. [DONE June 2026] RecoverCommittedTailWaiterAsync path - the executor's
      "task already faulted at commit time" branch. This was the source of the
      most recent test bug (CompleteAsync_DuringActiveRecovery_DrainsCleanly).
      Modeled as ExecCommitTailRecovers (see backlog #5's Phase D entry,
      including the unconditional-activation code note).

   3. [DONE June 2026 - VERIFIED SOUND] Multiple concurrent callback firings.
      SplitCallbackOps un-fuses the queue callback's `_drainSignal = true` store
      from its TryAcquireOrFlagPending (callbackSignaled tracks the set-before-
      acquire transient), so two concurrent callbacks' stores and the advancer's
      pass-top clear interleave against it - the case the fused
      CallbackBecomesAdvancer/BailsOut collapsed. Pipeline_SplitCallbackWitness.cfg
      is GREEN over 2.08M distinct states (depth 52): the PendingWordLatch deposit
      carries the wake even when a sibling's pass-top clear wipes the signal mid-
      window. The manual enumeration (every signal-set is deposit-paired or the
      conservation-covered materializing-token) is thereby a checked fact. The
      fused model is kept canonical in Contract (verified-equivalent, smaller
      state space); the witness pins the equivalence. No code change - this round
      confirms the existing fused behavior is correct.

   4. Source-side gating and pacing - the source's MoveNextAsync can defer
      yielding for reasons the pipeline doesn't see (backpressure, transaction
      state, connection state). Modeling this as a SourceGate predicate that
      blocks SourceYield_ would let us verify properties like "the pipeline
      makes progress whenever the source is willing to yield."

   5. Recovery flow (RecoverWaiter) - DISTINCT-IDENTITY RECOVERY landed June 2026
      (RecoverySplit), the substitute-is-a-normal-item-plus-a-binding design:
        - recoveryOf[substitute] = failed item (the binding); BindingDischarge
          completes the failed item when the substitute completes; the failed
          item parks in "Recovering" carrying the obligation. RecoveryCompletes
          (Recovering ~> Completed) is the lifetime-alignment liveness guard.
        - The split makes ActivatedAtMostOnce UNIFORM <= 1 (vs the old
          identity-reuse +1 slack), so a recovery double-activation now violates.
        - [DONE] Phase A: first-level Executing-throw (the common user-in-loop
          case - bind validation / converter fault). RecoverInstallWins/Loses.
        - [DONE] Phase B: policy-refuses (RecoverRefuse, completes directly) +
          recovery-on-recovery (RecoverOnRecoveryFails, substitute's own failure -
          both it and the bound failed item complete directly, single-level by
          construction - the "fails one level up" hazard, verified discharged).
        - [DONE] Phase C: trailing READ fault on a committed queue waiter
          (DrainHeadRecovers, the recovery-routing twin of AdvancerDrainHead - the
          wire-fault case; gated storeCount > 0 = a counted waiter).
        - [DONE] Phase D: trailing READ fault on a committed SLOT waiter
          (SlotHeadRecovers). Landed as the IN-PLACE lifecycle, not the shared
          one: drain-side parks go to "RecoveringInline" and resolve via
          RecoverInstallInline (unconditional activation, advancer thread) +
          RecoverInlineCompletes (completes at the parked position, store
          untouched) - funneling the substitute through the shared tail/commit
          actions let TLC double-activate it (publish steal at the commit
          Exchange; D-path re-activation once queued behind a successor), both
          code-impossible. The park fires at drainPhase = "claimed" (the
          GetResult point, BEFORE the decrement) and leaves drainPhase/drainItem
          parked; the post-resolution rejoin IS the standard SlotDrainCount ->
          SlotDrainComplete* steps (= the partition + AdvanceAndDrainRecovery),
          gated on no RecoveringInline. TWO CODE FINDINGS pinned in the action
          comment, BOTH FIXED (June 2026 audit round): DrainSlotInline decremented
          BEFORE the fault decision (~1022) - (a) opened the executor's Count==0
          inline gate mid-recovery (second active reader vs RecoverWaiter's
          unconditional ActivateHeadItem), (b) double-decremented via
          AdvanceAndDrain's queue-flavored rejoin (whose >0 arm also queue-peeks;
          a slot successor tripped its assert). The code now matches the model's
          order (fault decision at "claimed", decrement only on advance; the
          recovery rejoin runs the slot partition + DrainSlotInline re-entry via
          AdvanceAndDrain's IsEscalated branch). Guarded by
          PipelineRecoveryTests.SlotRecoveryPark_* (fail on the old order).
        - [DONE] Phase D: pre-faulted commit (ExecCommitTailRecovers - the
          RecoverCommittedTailWaiterAsync branch: CommitTailWaiter observes a
          settled-and-faulted tail task and recovers executor-inline; publish
          consume covers both Exchange outcomes; store untouched). Resolves via
          RecoverInstallWins/Loses (count-gated activation). CODE NOTE, FIXED
          (June 2026 audit round): RecoverCommittedTailWaiterAsync activated
          UNCONDITIONALLY (~749) even with prior waiters in flight - the
          eager-activation shape RecoverItem's count gate (~506) exists to
          prevent. The code now installs count-gated (republish + deferred
          publish at Count > 0, matching the model); guarded by
          PipelineRecoveryTests.CommittedTailFaultsAtCommit_PriorWaiterInFlight_*.
          Coverage: fires (98 transitions in the
          witness) but adds 0 distinct states - its park state coincides with
          ExecItemFailure firing on a task-settled Executing item (the executor
          loop's sync pipeline-task-fault branch, ~359), a real distinct code
          route that converges in the model.
        - [DONE - FINDING, no action needed] mid-escalation failure (Executing
          throw while escPhase != idle): split verdict, TLC-adjudicated. PRE-
          failure the overlap is structurally unreachable (executor sequencing;
          pinned by ExecutorQuiescentMidCommitPreFailure - whose unconditional
          first cut TLC REFUTED, which is the point of checking). POST-failure
          it is real and exercised: a drain-side park's substitute executes
          concurrently with the executor (two Executing items, truthful to
          RecoverWaiter's drainer-thread substitute) and the escPhase-agnostic
          RecoverOnRecoveryFails covers its failure during an in-flight commit;
          the refutation trace doubles as the joint-reachability witness
          (substitute Executing at escPhase = "slot_w_act"). See the invariant's
          comment.
        - Still to add: the partial-write-mid-frame case (if the encoder can
          fault after emitting - needs a design decision);
          multi-failure (needs more reserved substitute identities; the single
          SubSlot reservoir + failed = {} bound is Phase A/B/C/D scope). Reserved
          substitute identity (SubSlot = NumItems) + one-failure bound keep the
          NumItems = 3 pool from being drained out from under recovery.
        - Payoff target: confirm refusal-completes-directly so the code's
          Debug.Assert can become a normal path.

   6. Escalation-vs-slot-callback CAS race - both EscalateAndEnqueue and the
      slot callback do Exchange(_hasSlot, 0). Currently the spec models the
      executor's path explicitly (CAS branch in ExecCommitTail_) and the
      callback's via SlotCallbackDrains; the race is captured by interleaving
      but the "executor lost the CAS to a concurrent slot callback that
      already completed the item" branch could be made more explicit.

   7. [DONE June 2026] Slot field writes vs the flag. SplitSlotFieldOps exposed
      the tear (Pipeline_SlotTearWitness.cfg, 15-state trace - the reachable
      route was the licensed callback path, not the suspected stale-token
      reclaim); TriStateSlotClaim fixed it (the _slotState 0/1/2 word,
      data-then-license commit, peek-gated claim, quiescent-only escalation
      CAS). Both toggles TRUE in Pipeline_Contract.cfg (green); WaiterStoreTests
      + WaiterStoreConcurrencyTests guard the code (proven to fail on the
      two-state version). A second liveness strand surfaced and was fixed in
      the same round: the slot release delegated a consumed deposit to the
      flag-driven tail, but the claim's own consume cleared that flag - the
      release now serves its own deposit (SlotServeReacq*; code: DrainSlotInline
      ORs the deposit into the reclaim gate).

   8. [DONE June 2026 - WAS A REAL BUG] The queue lock-block's clear-at-Count>0
      branch. ModelCPathClear added the faithful lock sequencing (a "cpath_lock"
      phase set only by a genuine <=0 decrement); the tightened witness
      (Pipeline_CPathClearWitness.cfg) PROVED the shipped clear strands the
      published item - the slot double-skip in the queue (winning the publish
      Exchange means the executor's own commit defers, so the clear leaves
      neither side activating; the item hangs when the successor drains before
      it re-queues with a predecessor). The "believed sound" reasoning was wrong,
      exactly as it was for slot mode. FIX (CPathLeaveFix): leave the publish at
      Count>0 for the executor's commit to reclaim and activate; code is the
      `Count<=0 && Exchange` short-circuit with the else-clear deleted, both the
      DrainReadyWaiters and AdvanceAndDrain sites. Pipeline_CPathFixWitness.cfg +
      Contract (both toggles TRUE) green; the clear witness pins the bug.
*)

=============================================================================
