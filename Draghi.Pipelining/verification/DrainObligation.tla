---------------------------- MODULE DrainObligation ----------------------------
(*
Queue-mode drain obligation transfer, at instruction granularity.

THE QUESTION. A field strand (June/July 2026, ~1/2400): an async item's waiter
task SETTLED but its drain never ran - the completion obligation evaporated
somewhere in the callback / latch / drain dance and the FIFO queue stranded
forever. Pipeline.tla holds on its shipped config yet reality strands, so the
big spec fuses the guilty windows. This module splits them: steady-state QUEUE
mode only, every latch op, signal op, queue op, and count op of the drain
protocol as its own step. Entries complete adversarially; the protocol must
retire every completed entry. A liveness violation (or a deadlock - detection
stays ON) here is a real-bug candidate trace.

CODE MAPPED (Pipeline.cs @ working tree, July 2026):
  OnWaiterTaskCompleted   ~1166-1191  (CbFire / CbAcq / CbBail)
  DrainReadyWaiters       ~1427-1631  (QPass / QDrain / Retire / QDecr and
                                       the QConserve, QRel, QRecheck,
                                       QReclaim action families)
  CommitWaiter            ~1081-1160  (CommitEnq / CommitInc; queue-only fast
                                       path of WaiterStore.TryEscalateOrEnqueue,
                                       WaiterStore.cs ~105-107)
  Latch (PendingWordLatch) Latch.cs 52-75
  WaiterStore.DecrementCount / Count clamp  WaiterStore.cs 47, 165-170
  SingleProducerSingleConsumerQueue.cs (the CONSUMER CACHED VIEW extension):
    Enqueue 101-120, EnqueueSlow 125-156, TryPeek 188-203,
    TryDequeue 161-183, TryDequeueSlow 212-259.

CONSUMER CACHED VIEW EXTENSION (2026-07-02). The base module trusted the SPSC
queue as an atomic FIFO. A real captured permanent strand (DrainTrace tape:
serving pass read count=9, TryPeek+IsCompleted drained nothing, conserve
restored, reclaim peeked and missed again, miss-break; item 5 resident and
completed at A[1] forever) put the queue's CONSUMER VIEW itself under
adjudication. Behind CONSTANT ConsumerCachedView the queue becomes the faithful
consumer state machine of SingleProducerSingleConsumerQueue: per-segment
producer index (`_last`), consumer-owned `_first` / `_lastCopy`, the fast path
hit iff first # lastCopy on the CACHED lastCopy, the slow path's acquire
refresh of lastCopy from true `_last`, the segment hop iff `_next` non-null
(PLAIN read) and first = currentLast, and the final first = last empty verdict.

  ENCODING: indices are UNBOUNDED SEQUENCE NUMBERS, not mod-C values. The
  wrap-at-full is still fully present: the producer's room check
  (pa - first < SegCapacity - 1, the (last+1)&(len-1) == firstCopy full test)
  bounds residency at SegCapacity - 1, and under that window two sequence
  numbers are congruent mod C iff they are EQUAL (0 <= x - first <= C-1 for
  every value x compared against first or lastCopy), so every mod-C index
  comparison in the code collapses to sequence-number equality without losing
  the wrap ABA: "the location holds value 1 again after wrapping" is exactly
  "a stale read returns sequence 1 while true last is at sequence C+1", which
  the read model expresses directly. Segment A has capacity SegCapacity;
  overflow goes to segment B via the EnqueueSlow path (preset B._last =
  B._lastCopy = 1 with the first item at B[0], publication via the
  Volatile.Write of A._next). B is modeled unbounded (real B = 2C; at model
  sizes B can never fill, so its own wrap/split is unreachable in reality too).

  THE READ MODEL (the critical fidelity question). What may the acquire read
  of `_last` return? Happens-before is tracked per THREAD (per callback agent)
  as a visibility floor flrA/flrB (plus sawN for `_next`), raised by:
    - the agent's OWN reads (same-location read-read coherence - never broken
      by any candidate lowering bug, fences or not);
    - in ChainSound mode ONLY, every synchronization edge the thread passes:
        * the callback wiring (UnsafeOnCompleted after the entry's count
          increment): wireA/wireB snapshot taken at CommitInc, applied at
          CbFire (an inline completed-at-commit callback actually carries MORE
          - full producer truth - so wire-only is the adversarial lower bound;
          any violation trace must be hand-checked against this);
        * every latch-word RMW (Latch.cs: all ops full-fence interlocked on
          one word, so they form a totally ordered release chain: each
          acquire adopts latchPkg, each op merges the agent's own knowledge);
        * the count-word RMW chain (CommitInc Interlocked.Increment /
          DecrementCount Interlocked.Decrement) via countPkg;
        * the count ACQUIRE READ: WaiterStore.Count is Volatile.Read
          (WaiterStore.cs 47) of a value written by full-fence RMWs, so
          QConserveRead harvests countPkg (this is a REAL edge: it forbids
          the reclaim peek from being staler than the conserve count read);
        * the _activationLock edge (lockPkg): producer merges at a wasEmpty
          commit of a not-yet-completed entry (CommitWaiter 1100-1122),
          consumer harvests at a drained-verdict decrement (the C-path lock,
          1493). Modeled as ALWAYS taken when its guard holds - maximal
          producer-side edges, so a surviving violation is robust.
    A read of A._last returns any sequence value in
      Max(lcA, flr) .. true pa      (ChainSound: flr includes the edges;
                                     chain-broken: flr is own-reads only)
    lastCopy itself lower-bounds every read in BOTH modes: it is the
    consumer's own last accepted read of the location (coherence), and it
    also keeps a below-lastCopy regression - which no coherent hardware
    produces - from fabricating garbage peeks of cleared slots.
    The PLAIN `_next` read may return null while published (staleness) or the
    fresh value at any time; it is FORCED non-null once the agent saw it
    (coherence) or, in ChainSound mode, once its floor proves B exists
    (flrB > 0: any HB edge from at/after the publish).
  ChainSound = TRUE  : reads are bounded below by the full happens-before
                       chain above (faithful sound memory model, INCLUDING
                       legal staleness between edges - NOT reads-return-truth).
  ChainSound = FALSE : the acquire is broken; reads may return any value the
                       location held at/after the thread's own last read of it
                       (adversarial staleness, coherence only).

  EDGES DELIBERATELY NOT HARVESTED (adversarial cuts - hand-check violation
  traces against these):
    - the signal word: CbFire's `_drainSignal = true` and the conserve restore
      are PLAIN stores; a plain store heads no release sequence, so the QPass
      Exchange and the recheck's Volatile.Read synchronize with nothing there;
    - DrainTrace instrumentation reads (QPass logs _waiters.Count - env-gated
      tracer, absent from untraced builds; correctness must not lean on it);
    - the activation partition's fences (the C-path's Volatile.Read of
      _executingItemActivationPending, ActivateHeadItem's dispatch RMWs) - the
      activation partition is cut (see below); on candidate traces the
      publish those reads acquire predates the commits whose visibility is in
      question;
    - the depth-word RMW chain (CompleteWaiterDeferred's decrement): a depth
      increment happens at pipeline dispatch, BEFORE the item's own waiter
      commit, so its package cannot carry the enqueue whose visibility the
      strand needs;
    - waiter-task state words: completers are environmental, they carry no
      producer knowledge.

  FUSIONS (per-site justification):
    - enqueue data write + Volatile.Write(_last) publish: the consumer can
      only trust data at indices below a value it read from _last, and that
      acquire/release pair is exactly what the read model expresses, so the
      pre-publish half-written state is invisible by construction;
    - a peek's multiple reads into ONE action instant: every constituent read
      may return values down to the thread floor, so any real multi-instant
      interleaving (producer commits landing between the reads) maps to the
      fused action at the LAST instant with the earlier reads expressed as
      staleness - including the TOCTOU family where currentLast is read
      before a burst of enqueues and _next after (this is why the cached-view
      model must not read "truth only": the SC-level split-read interleavings
      live in the staleness ranges);
    - peek + dequeue in QDrain: under the latch nobody else consumes, and the
      dequeue's reads are floored at the peek's reads (same thread), which
      forces the dequeue to follow the peek to the SAME entry on every path
      (fast-after-fast, fast-after-refresh, refresh-then-fast after a
      final-read hit, fast on B after a hop); the code's discarded TryDequeue
      result (Pipeline.cs 1441) can therefore never diverge from the retired
      item, and the line-255 freshness refresh is reachable only through the
      final-hit dequeue path (modeled: the s3 refresh in QDrain's final-hit).
  PRODUCER-SIDE SIMPLIFICATIONS: the producer's acquire read of _first
  (EnqueueSlow 132) is modeled as returning truth - producer-side staleness
  only splits segments EARLIER (allocates B while A has room), a layout
  variation, not a consumer-visibility mechanism; B is unbounded (above).

  ReclaimMissRetry (fix toggle): one FENCED re-peek before the reclaim's
  final break (a new "reclaimRetry" step after QReclaimPeekMiss). The retry
  is modeled WITH full-fence semantics AS SPECIFIED FOR THIS PROBE: its reads
  return true last (the thread's floors are raised to producer truth and the
  walk proceeds on fresh values). A weaker retry (fence without freshness)
  would re-miss under unbounded staleness; this config intentionally probes
  the strongest reading. Note what the retry can NEVER fix: a wrong segment
  hop already committed `_head = B`; segment A's residents are unreachable to
  any later read, fenced or not.

DELIBERATE CUTS (scope):
  - Steady-state queue mode only: the store is already escalated at Init
    (queue allocated, slot permanently empty). No slot tier, no escalation
    move, no slotWasMoved nudge / moved-pair compensation (CommitNudge,
    CommitMoved), no TakeMovedSlotPair.
  - NO ACTIVATION. The activation partition after DecrementCount (C-path lock
    body, D-path ActivateHeadItem, _liveActivation,
    _executingItemActivationPending) is skipped entirely, EXCEPT the
    _activationLock synchronization edge itself (lockPkg, above), which the
    cached-view read model needs. Activation consumes none of the
    drain-obligation state; what the cut can mask is a lost activation, a
    different failure mode.
  - No recovery (RecoverWaiter's advance=false path), no cancellation/shutdown
    (the QRecheck cancellation gate is dropped; the shutdown regime has its
    own spec, PipelineShutdown.tla), no DrainOnCompletionAsync sweep, no
    SignalDrainWakeupIfWaiting, no depth/idle signal (emptyReached).
  - Sequential consistency for everything EXCEPT the queue consumer's view
    when ConsumerCachedView = TRUE: the count value itself, the signal, the
    latch word stay SC (their fence arguments live in the base header and
    WeakMemory.tla); only the queue's _last/_next reads get the weak
    treatment, which is the adjudicated mechanism.
  - Waiter-task consume (GetAwaiter().GetResult()) is fused into Retire: it
    touches no shared protocol state (task sources are per-entry).

FIDELITY POINTS KEPT (the splits this module exists for):
  - Split callback: `_drainSignal = true` (CbFire) and
    `_advancing.TryAcquireOrFlagPending()` (CbAcq/CbBail) are separate steps.
  - Commit split: queue.Enqueue (CommitEnq) and Interlocked.Increment(_count)
    (CommitInc) are separate steps - the count UNDER-PROMISES the queue.
    The callback is wired (line 1125-1128) only AFTER TryEscalateOrEnqueue
    returned, i.e. after the increment, so CbFire is gated on the entry's
    increment having landed (cNext > e).
  - Signal conservation: the pass-local drainedAny verdict (QPeekMiss branch),
    the clamped count read (QConserveRead), and the conditional restore store
    (QConserveStore) are separate steps.
  - DecrementCount is its own step (QDecr), AFTER Retire, so its ordering
    against commit-side increments (the -1 skew face, WaiterStore.cs 155-170)
    is fully interleavable. The clamp lives in ClampedCount.
  - Release-and-check (QRel / QRelDeposit) and the deposit-serve re-acquire
    (QRelServeWin/Lose) are separate steps.
  - The reclaim's transient hold: acquire (QReclaimAcq/QReclaimBail), peek
    verdict (QReclaimHit/QReclaimPeekMiss), the miss release
    (QReclaimMissRel/QReclaimMissRelDeposit), and the stale-verdict re-acquire
    (QReclaimMissReacqWin/Lose) are all separate steps.

MODELING JUDGMENT CALLS (potential transcription gaps - review these first):
  - Peek + IsCompleted fused into one read instant: under the latch the head
    the view designates cannot be dequeued by anyone else and completion is
    monotonic, so the two reads collapse to the later instant.
  - drainedAny / conserveCount / cur are thread-locals; cleared when dead
    purely as state-space hygiene.
  - One committing thread (the executor is the store's single producer),
    committing entries in FIFO order. Inline completed-at-commit callback
    invocation is subsumed by the any-time-after schedule (and carries MORE
    visibility than the modeled wire floor - see read model note).
  - Each entry's callback fires exactly once (UnsafeOnCompleted one-shot).
  - Completions are adversarial: Complete(e) may fire at any time, including
    before the entry's own commit, and carries no fairness. Every protocol
    step IS weakly fair.

PROPERTIES:
  TypeOK, ViewCoherent (consumer-view sanity incl. the residency window that
  licenses the sequence-number encoding), LatchOwnership, CountSkewFloor,
  RetiredNotResident, RetiredImpliesCompleted.
  EventuallyDrained - THE question: once an entry and its whole FIFO prefix
  have completed, the entry is eventually retired.
  Deadlock detection stays ON: the only lawful terminal state is the explicit
  Done self-loop.

CONFIGS:
  DrainObligation.cfg                    - ConsumerCachedView = FALSE: the
                                           base truthful-peek model (queue as
                                           atomic FIFO), shipped shape.
  DrainObligation_NoConserveWitness.cfg  - base model, restore disabled probe.
  DrainObligation_CachedViewSound.cfg    - CachedView + ChainSound: can the
                                           captured strand shape occur under a
                                           SOUND happens-before chain?
  DrainObligation_ChainBrokenWitness.cfg - CachedView + broken chain: the
                                           witness for the captured tape.
  DrainObligation_SoundRetryProbe.cfg    - ChainSound + ReclaimMissRetry.
  DrainObligation_BrokenRetryProbe.cfg   - broken chain + ReclaimMissRetry.
  DrainObligation_HopGuardFix.cfg        - CachedViewSound + FreshHopGuard:
                                           the SHIPPED FIX, red-before-green.
  DrainObligation_HopGuardFixBroken.cfg  - the fix against the broken chain:
                                           scopes what the fix does NOT claim.

VERDICT (TLC 2.19, 2026-07-02; NumEntries = 3, SegCapacity = 2 unless noted):
  Base configs (ConsumerCachedView = FALSE): GREEN, unchanged conclusions
  (steady-state queue-mode obligation transfer exonerated with truthful
  peeks; conserve restore not load-bearing in pure queue mode). 13,120 /
  12,592 distinct states (the segment encoding grew the old 6,328 count).
  CachedViewSound: VIOLATION - EventuallyDrained falsified WITHOUT any broken
  acquire. The trace is the WRONG SEGMENT HOP: a drain pass's TryDequeueSlow
  reads currentLast while A is momentarily empty (truthfully!), the producer
  then fills A and publishes B, and the resumed guard
  `_next != null && _first == currentLast` (SingleProducerSingleConsumerQueue
  line 232) compares the fresh _next against the STALE LOCAL currentLast,
  hops `_head` to B past resident A entries, stranding them unreachably. The
  BCL original re-reads volatile _last at this guard; Draghi's single-read
  optimization (line 217-218 comment) removed the freshness this guard - not
  just the refresh assignment - depended on. This is an SC-level TOCTOU
  (legal on x86, no weak memory needed), fully consistent with the captured
  tape via an OS preemption inside the sub-nanosecond window.
  The sound-mode counterexample IS the tape end-to-end: hop, then the serving
  pass fast-peeks B's uncompleted head and drains nothing, conserve restores,
  release/recheck/reclaim re-peeks and re-misses, miss-break - signal set,
  latch free, every callback spent, the completed entry resident forever.
  30-state CE at N=3/C=2; confirmed at the tape's literal geometry N=5/C=4
  (34-state CE: A = <<1,2,3,4>> wrapped, B = <<5>>, first = lastCopy = 1,
  completed entry 2 stranded at A[1]; 5,887,570 distinct states explored).
  ChainBrokenWitness: VIOLATION - one broken read strands without the hop
  too. TLC's shortest face is a stale-NULL _next (the consumer never follows
  into the published B whose head completed); the tape's stale-equal _last
  face is also in this config's behavior set (the sound CE's schedule needs
  no harvest to be admissible). Pins that the capture is one broken/stale
  read away in several distinct ways.
  Retry probes: the fenced retry closes NEITHER config. The wrong hop is a
  committed _head advance no read freshness recovers (SoundRetryProbe:
  deadlock, entry stranded in A with everything completed), and the dominant
  strand exits never even reach the reclaim: a pass that drained SOMETHING
  (drainedAny) skips conserve, so its stale-miss exit leaves the token
  consumed and the recheck breaks without a reclaim - the retry sits at the
  wrong site. Details and state counts in the run report.
  HopGuardFix (FreshHopGuard = TRUE, the SHIPPED fix - both hop guards
  re-read _last via Volatile.Read in the hop condition): GREEN over the FULL
  state space, 64,513 distinct states, 0 left on queue, deadlock detection
  on, EventuallyDrained holds. Red-before-green closed at the source: the
  fresh emptiness check either licenses a stable hop (A permanently empty)
  or raises the reader's floor so the final read hits the resident head.
  All FreshHopGuard = FALSE reruns unchanged (base/NoConserve green at
  13,120 / 12,592; CachedViewSound still RED - same wrong-hop strand,
  surfaced as a deadlock this run; both witnesses and both retry probes
  still red).
  HopGuardFixBroken: VIOLATION (20-state CE) - the residual under a BROKEN
  chain. headSeg stays "A" (no hop, the fix holds); the strand is the pure
  stale-read face: the completed resident head is verdicted empty by
  stale-equal _last / stale-null _next reads in both the pass and the
  reclaim, miss-break with the signal set. Scopes the fix's guarantee to
  sound lowerings: only a genuinely broken acquire reproduces the tape shape
  past the fix, which is exactly what the (b)/(c) pair adjudicated.
*)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    NumEntries,             \* number of committed waiters
    ConserveRestoreEnabled, \* TRUE = shipped signal-conservation restore
    ConsumerCachedView,     \* TRUE = faithful SPSC consumer view; FALSE = truthful peeks
    ChainSound,             \* TRUE = reads bounded by the happens-before chain
    ReclaimMissRetry,       \* TRUE = fenced re-peek before the reclaim miss-break
    FreshHopGuard,          \* TRUE = the hop guard's emptiness side re-reads TRUE
                            \* last at hop time (a Volatile.Read inside the hop
                            \* condition - the SHIPPED FIX in TryDequeueSlow and
                            \* TryDequeueIf's slow path) instead of comparing the
                            \* pass-captured currentLast local. The re-read is a
                            \* genuine acquire and raises the reader's floor for
                            \* the location. Hop-on-fresh-equal is stable: _next
                            \* published means the producer never writes this
                            \* segment's _last again, so fresh first = last is
                            \* PERMANENT emptiness.
    SegCapacity             \* segment A capacity C (usable C-1 before split)

ASSUME SegCapacity >= 2

Entries == 1..NumEntries

VARIABLES
    \* Shared protocol state.
    count,      \* WaiterStore._count (raw, may dip to -1; reads clamp)
    signal,     \* _drainSignal
    latch,      \* the PendingWordLatch word: "free" | "held" | "heldPending"
    \* Ground truth.
    completed,  \* [Entries -> BOOLEAN] waiter task settled
    retired,    \* [Entries -> BOOLEAN] CompleteWaiterDeferred ran
    \* Committer thread (the executor, single producer).
    cNext,      \* next entry to commit (entries commit in id order)
    cPc,        \* "enq" | "inc" | "done"
    \* Per-entry callback agents.
    cb,         \* [Entries -> pc]
    da,         \* [Entries -> BOOLEAN] pass-local drainedAny
    cc,         \* [Entries -> 0..NumEntries] pass-local conserveCount read
    cur,        \* [Entries -> 0..NumEntries] dequeued-entry-in-hand (0 = none)
    \* The SPSC queue, segment/sequence-number encoding (see header).
    segAq,      \* Seq(Entries): entries enqueued to segment A, in order
    segBq,      \* Seq(Entries): entries enqueued to segment B, in order
    consA,      \* consumer A._first as a sequence number (entries dequeued from A)
    consB,      \* consumer B._first as a sequence number
    lcA,        \* consumer A._lastCopy as a sequence number
    lcB,        \* consumer B._lastCopy (preset 1 by EnqueueSlow)
    headSeg,    \* consumer _head: "A" | "B"
    \* Per-thread visibility floors (cached-view read model).
    flrA,       \* [Entries -> Nat] floor on reads of A._last
    flrB,       \* [Entries -> Nat] floor on reads of B._last
    sawN,       \* [Entries -> BOOLEAN] thread saw A._next non-null
    latchPkg,   \* producer-progress package released through the latch word
    countPkg,   \* package released through the count word RMW chain
    lockPkg,    \* package released through _activationLock
    wireA,      \* [Entries -> Nat] pa snapshot at the entry's CommitInc (wiring)
    wireB       \* [Entries -> Nat] pb snapshot at the entry's CommitInc

vars == <<count, signal, latch, completed, retired, cNext, cPc, cb, da, cc, cur,
          segAq, segBq, consA, consB, lcA, lcB, headSeg,
          flrA, flrB, sawN, latchPkg, countPkg, lockPkg, wireA, wireB>>

qVars   == <<segAq, segBq, consA, consB, lcA, lcB, headSeg>>
visVars == <<flrA, flrB, sawN, latchPkg, countPkg, lockPkg, wireA, wireB>>

CbPcs == {"idle", "acq", "pass", "peek", "retire", "decr", "consRead",
          "consStore", "rel", "relServe", "recheck", "reclaimAcq",
          "reclaimPeek", "reclaimRetry", "reclaimMissRel", "reclaimMissReacq",
          "done"}

HeldPcs == {"pass", "peek", "retire", "decr", "consRead", "consStore", "rel",
            "reclaimPeek", "reclaimRetry", "reclaimMissRel"}

ClampedCount == IF count > 0 THEN count ELSE 0   \* WaiterStore.Count (line 47)

MaxN(x, y) == IF x >= y THEN x ELSE y
ZeroPkg == [a |-> 0, b |-> 0]
JoinP(p, q) == [a |-> MaxN(p.a, q.a), b |-> MaxN(p.b, q.b)]

pa == Len(segAq)                 \* true A._last as a sequence number
pb == Len(segBq)                 \* true B._last as a sequence number
NextPub == pb > 0                \* A._next published (fused with B creation)
TruthPkg == [a |-> pa, b |-> pb]
AgentPkg(e) == [a |-> flrA[e], b |-> flrB[e]]

\* Ground-truth FIFO residency (A entries strictly precede B entries: the
\* producer never returns to A once B exists).
TrueQueue == SubSeq(segAq, consA + 1, pa) \o SubSeq(segBq, consB + 1, pb)

(* ---------------------------------------------------------------------------
   The read model. ReadSetA/B: values an acquire read of the segment's _last
   may return for agent e. NextReadSet: values the PLAIN _next read may return.
   With ConsumerCachedView = FALSE all reads return truth (base model).
   ------------------------------------------------------------------------ *)

ReadSetA(e) == IF ~ConsumerCachedView THEN {pa} ELSE MaxN(lcA, flrA[e])..pa
ReadSetB(e) == IF ~ConsumerCachedView THEN {pb} ELSE MaxN(lcB, flrB[e])..pb

NextReadSet(e) ==
    IF ~NextPub THEN {FALSE}
    ELSE IF ~ConsumerCachedView THEN {TRUE}
    ELSE IF sawN[e] \/ (ChainSound /\ flrB[e] > 0) THEN {TRUE}
    ELSE {TRUE, FALSE}

\* Floor bumps: own-read coherence in every cached mode; frozen when truthful.
FlrABump(e, v) == IF ConsumerCachedView THEN [flrA EXCEPT ![e] = MaxN(@, v)] ELSE flrA
FlrBBump(e, v) == IF ConsumerCachedView THEN [flrB EXCEPT ![e] = MaxN(@, v)] ELSE flrB
SawNBump(e)    == IF ConsumerCachedView THEN [sawN EXCEPT ![e] = TRUE] ELSE sawN

TypeOK ==
    /\ count \in -1..NumEntries
    /\ signal \in BOOLEAN
    /\ latch \in {"free", "held", "heldPending"}
    /\ completed \in [Entries -> BOOLEAN]
    /\ retired \in [Entries -> BOOLEAN]
    /\ cNext \in 1..(NumEntries + 1)
    /\ cPc \in {"enq", "inc", "done"}
    /\ cb \in [Entries -> CbPcs]
    /\ da \in [Entries -> BOOLEAN]
    /\ cc \in [Entries -> 0..NumEntries]
    /\ cur \in [Entries -> 0..NumEntries]
    /\ segAq \in Seq(Entries) /\ segBq \in Seq(Entries)
    /\ pa + pb <= NumEntries
    /\ consA \in 0..pa /\ consB \in 0..pb
    /\ lcA \in 0..pa /\ lcB \in 1..MaxN(pb, 1)
    /\ headSeg \in {"A", "B"}
    /\ flrA \in [Entries -> 0..NumEntries] /\ flrB \in [Entries -> 0..NumEntries]
    /\ sawN \in [Entries -> BOOLEAN]
    /\ latchPkg \in [a: 0..NumEntries, b: 0..NumEntries]
    /\ countPkg \in [a: 0..NumEntries, b: 0..NumEntries]
    /\ lockPkg \in [a: 0..NumEntries, b: 0..NumEntries]
    /\ wireA \in [Entries -> 0..NumEntries] /\ wireB \in [Entries -> 0..NumEntries]

\* Consumer-view sanity. The residency window (pa - consA <= C-1) is what
\* licenses the sequence-number encoding of the mod-C comparisons.
ViewCoherent ==
    /\ consA <= lcA
    /\ consB <= lcB \/ pb = 0
    /\ pa - consA <= SegCapacity - 1
    /\ headSeg = "B" => pb >= 1
    /\ pb = 0 => (consB = 0 /\ headSeg = "A" /\ lcB = 1)
    /\ \A e \in Entries : flrA[e] <= pa /\ flrB[e] <= pb
    /\ latchPkg.a <= pa /\ latchPkg.b <= pb
    /\ countPkg.a <= pa /\ countPkg.b <= pb
    /\ lockPkg.a <= pa /\ lockPkg.b <= pb

Init ==
    /\ count = 0
    /\ signal = FALSE
    /\ latch = "free"
    /\ completed = [e \in Entries |-> FALSE]
    /\ retired = [e \in Entries |-> FALSE]
    /\ cNext = 1
    /\ cPc = IF NumEntries = 0 THEN "done" ELSE "enq"
    /\ cb = [e \in Entries |-> "idle"]
    /\ da = [e \in Entries |-> FALSE]
    /\ cc = [e \in Entries |-> 0]
    /\ cur = [e \in Entries |-> 0]
    /\ segAq = <<>> /\ segBq = <<>>
    /\ consA = 0 /\ consB = 0
    /\ lcA = 0 /\ lcB = 1
    /\ headSeg = "A"
    /\ flrA = [e \in Entries |-> 0] /\ flrB = [e \in Entries |-> 0]
    /\ sawN = [e \in Entries |-> FALSE]
    /\ latchPkg = ZeroPkg /\ countPkg = ZeroPkg /\ lockPkg = ZeroPkg
    /\ wireA = [e \in Entries |-> 0] /\ wireB = [e \in Entries |-> 0]

(* ---------------------------------------------------------------------------
   Synchronization-edge helpers (cached-view + sound chain only).
   ------------------------------------------------------------------------ *)

\* Every latch op is a full-fence RMW on one word (Latch.cs): release own
\* knowledge into the chain, acquire everything already there.
LatchHarvest(e) ==
    IF ConsumerCachedView /\ ChainSound
    THEN /\ latchPkg' = JoinP(latchPkg, AgentPkg(e))
         /\ flrA' = [flrA EXCEPT ![e] = MaxN(@, latchPkg.a)]
         /\ flrB' = [flrB EXCEPT ![e] = MaxN(@, latchPkg.b)]
    ELSE UNCHANGED <<latchPkg, flrA, flrB>>

\* Latch actions never touch the other visibility state or the queue view.
LatchRest == UNCHANGED <<sawN, countPkg, lockPkg, wireA, wireB>> /\ UNCHANGED qVars

(* ---------------------------------------------------------------------------
   ENVIRONMENT: adversarial waiter-task settlement.
   ------------------------------------------------------------------------ *)

Complete(e) ==
    /\ ~completed[e]
    /\ completed' = [completed EXCEPT ![e] = TRUE]
    /\ UNCHANGED <<count, signal, latch, retired, cNext, cPc, cb, da, cc, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED visVars

(* ---------------------------------------------------------------------------
   COMMITTER (executor thread). queue.Enqueue routes per the real producer
   tree: segment A while it has room under the full check (wrap-at-full lives
   here: room iff pa - first < C-1), else EnqueueSlow allocates B (preset
   B._last = B._lastCopy = 1, first item at B[0]) and publishes A._next; once
   B exists every enqueue lands in B. Producer _firstCopy refresh fused
   truthful (see header). Then Interlocked.Increment(_count) - separate step,
   the count under-promises the queue.
   ------------------------------------------------------------------------ *)

CommitEnq ==
    /\ cPc = "enq"
    /\ IF pb > 0
       THEN segBq' = Append(segBq, cNext) /\ UNCHANGED segAq
       ELSE IF pa - consA < SegCapacity - 1
            THEN segAq' = Append(segAq, cNext) /\ UNCHANGED segBq
            ELSE segBq' = Append(segBq, cNext) /\ UNCHANGED segAq
    /\ cPc' = "inc"
    /\ UNCHANGED <<count, signal, latch, completed, retired, cNext, cb, da, cc, cur>>
    /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg>> /\ UNCHANGED visVars

\* Increment + the wiring snapshot (callback continuation starts with the
\* producer knowledge at wiring) + the count-word release + the wasEmpty
\* _activationLock release (CommitWaiter 1100-1122, taken when the sole
\* committed waiter is not yet completed; modeled always-taken - max edges).
CommitInc ==
    /\ cPc = "inc"
    /\ count' = count + 1
    /\ IF ConsumerCachedView /\ ChainSound
       THEN /\ wireA' = [wireA EXCEPT ![cNext] = pa]
            /\ wireB' = [wireB EXCEPT ![cNext] = pb]
            /\ countPkg' = JoinP(countPkg, TruthPkg)
            /\ lockPkg' = IF count = 0 /\ ~completed[cNext]
                          THEN JoinP(lockPkg, TruthPkg) ELSE lockPkg
       ELSE UNCHANGED <<wireA, wireB, countPkg, lockPkg>>
    /\ cNext' = cNext + 1
    /\ cPc' = IF cNext = NumEntries THEN "done" ELSE "enq"
    /\ UNCHANGED <<signal, latch, completed, retired, cb, da, cc, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED <<flrA, flrB, sawN, latchPkg>>

(* ---------------------------------------------------------------------------
   CALLBACK: OnWaiterTaskCompleted (Pipeline.cs 1166-1191).
   ------------------------------------------------------------------------ *)

\* Line 1171: `_drainSignal = true`. The agent thread starts with the wiring
\* visibility (UnsafeOnCompleted registered after the entry's increment).
CbFire(e) ==
    /\ cb[e] = "idle"
    /\ completed[e]
    /\ cNext > e
    /\ signal' = TRUE
    /\ cb' = [cb EXCEPT ![e] = "acq"]
    /\ IF ConsumerCachedView /\ ChainSound
       THEN /\ flrA' = [flrA EXCEPT ![e] = MaxN(@, wireA[e])]
            /\ flrB' = [flrB EXCEPT ![e] = MaxN(@, wireB[e])]
       ELSE UNCHANGED <<flrA, flrB>>
    /\ UNCHANGED <<count, latch, completed, retired, cNext, cPc, da, cc, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED <<sawN, latchPkg, countPkg, lockPkg, wireA, wireB>>

\* Line 1179: TryAcquireOrFlagPending wins -> run DrainReadyWaiters.
CbAcq(e) ==
    /\ cb[e] = "acq"
    /\ latch = "free"
    /\ latch' = "held"
    /\ cb' = [cb EXCEPT ![e] = "pass"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

\* Line 1179-1183: acquire loses -> obligation DEPOSITED in the latch word.
CbBail(e) ==
    /\ cb[e] = "acq"
    /\ latch # "free"
    /\ latch' = "heldPending"
    /\ cb' = [cb EXCEPT ![e] = "done"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

(* ---------------------------------------------------------------------------
   DRAIN: DrainReadyWaiters (Pipeline.cs 1427-1631).
   ------------------------------------------------------------------------ *)

\* Line 1435: pass start, Interlocked.Exchange(ref _drainSignal, false). RMW
\* on the SIGNAL word: its other writers are plain stores heading no release
\* sequence, so no visibility is harvested here (see header).
QPass(e) ==
    /\ cb[e] = "pass"
    /\ signal' = FALSE
    /\ da' = [da EXCEPT ![e] = FALSE]
    /\ cb' = [cb EXCEPT ![e] = "peek"]
    /\ UNCHANGED <<count, latch, completed, retired, cNext, cPc, cc, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED visVars

(* The peek machine, TryPeek/TryDequeue(Slow) decision tree on the CACHED
   view (truthful when ConsumerCachedView = FALSE - the ReadSets collapse):
     fast hit          : first # lastCopy (cached!) -> head = seg[first]
     slow refresh      : read s1 of true _last; s1 # lastCopy -> refresh, hit
     hop               : s1 = lastCopy /\ _next read non-null /\ first = s1
                         -> _head = B (IRREVERSIBLE), then B's preset view
     final verdict     : no hop; read s2 >= s1; s2 = first -> empty,
                         s2 > first -> hit WITHOUT refreshing lastCopy
   QDrain additionally fuses the dequeue (same-entry proof in header); its
   final-hit shape is the only one reaching the line-253/255 slow-tail
   dequeue, whose freshness refresh is the s3 read. *)

\* Line 1439-1443: peek hit on a completed head -> TryDequeue, drainedAny.
QDrain(e) ==
    /\ cb[e] = "peek"
    /\ \/ /\ headSeg = "A"
          /\ \/ \* fast hit (first # lastCopy on the cached copy)
                /\ consA # lcA
                /\ completed[segAq[consA + 1]]
                /\ cur' = [cur EXCEPT ![e] = segAq[consA + 1]]
                /\ consA' = consA + 1
                /\ UNCHANGED <<consB, lcA, lcB, headSeg, flrA, flrB, sawN>>
             \/ \* slow refresh hit (s1 # lastCopy -> lastCopy = s1, retry hits)
                /\ consA = lcA
                /\ \E s1 \in ReadSetA(e) :
                     /\ s1 # lcA
                     /\ completed[segAq[consA + 1]]
                     /\ lcA' = s1
                     /\ flrA' = FlrABump(e, s1)
                     /\ cur' = [cur EXCEPT ![e] = segAq[consA + 1]]
                     /\ consA' = consA + 1
                /\ UNCHANGED <<consB, lcB, headSeg, flrB, sawN>>
             \/ \* hop: s1 = lastCopy, _next non-null, emptiness side passes
                \* (unfixed: first = s1 the stale LOCAL; FreshHopGuard: first =
                \* TRUE last, and the re-read raises the floor) -> _head = B
                /\ consA = lcA
                /\ lcA \in ReadSetA(e)
                /\ TRUE \in NextReadSet(e)
                /\ FreshHopGuard => consA = pa
                /\ flrA' = IF FreshHopGuard THEN FlrABump(e, pa) ELSE flrA
                /\ \E s2 \in ReadSetB(e) :
                     /\ completed[segBq[1]]
                     /\ headSeg' = "B"
                     /\ consB' = 1
                     /\ flrB' = FlrBBump(e, s2)
                     /\ sawN' = SawNBump(e)
                     /\ cur' = [cur EXCEPT ![e] = segBq[1]]
                /\ UNCHANGED <<consA, lcA, lcB>>
             \/ \* FIX path: the hop check's fresh re-read finds A non-empty ->
                \* no hop, and the raised floor forces the final read (and the
                \* fused dequeue's slow-tail refresh) to truth -> hit A's head
                /\ FreshHopGuard
                /\ consA = lcA
                /\ lcA \in ReadSetA(e)
                /\ TRUE \in NextReadSet(e)
                /\ consA < pa
                /\ completed[segAq[consA + 1]]
                /\ lcA' = pa
                /\ flrA' = FlrABump(e, pa)
                /\ sawN' = SawNBump(e)
                /\ cur' = [cur EXCEPT ![e] = segAq[consA + 1]]
                /\ consA' = consA + 1
                /\ UNCHANGED <<consB, lcB, headSeg, flrB>>
             \/ \* final-read hit; the fused dequeue's slow tail refreshes (s3)
                /\ consA = lcA
                /\ lcA \in ReadSetA(e)
                /\ FALSE \in NextReadSet(e)
                /\ \E s2 \in ReadSetA(e) :
                     /\ s2 > consA
                     /\ completed[segAq[consA + 1]]
                     /\ lcA' = s2
                     /\ flrA' = FlrABump(e, s2)
                     /\ cur' = [cur EXCEPT ![e] = segAq[consA + 1]]
                     /\ consA' = consA + 1
                /\ UNCHANGED <<consB, lcB, headSeg, flrB, sawN>>
       \/ /\ headSeg = "B"
          /\ \/ /\ consB # lcB
                /\ completed[segBq[consB + 1]]
                /\ cur' = [cur EXCEPT ![e] = segBq[consB + 1]]
                /\ consB' = consB + 1
                /\ UNCHANGED <<consA, lcA, lcB, headSeg, flrA, flrB, sawN>>
             \/ /\ consB = lcB
                /\ \E s1 \in ReadSetB(e) :
                     /\ s1 # lcB
                     /\ completed[segBq[consB + 1]]
                     /\ lcB' = s1
                     /\ flrB' = FlrBBump(e, s1)
                     /\ cur' = [cur EXCEPT ![e] = segBq[consB + 1]]
                     /\ consB' = consB + 1
                /\ UNCHANGED <<consA, lcA, headSeg, flrA, sawN>>
             \/ /\ consB = lcB
                /\ lcB \in ReadSetB(e)
                /\ \E s2 \in ReadSetB(e) :
                     /\ s2 > consB
                     /\ completed[segBq[consB + 1]]
                     /\ lcB' = s2
                     /\ flrB' = FlrBBump(e, s2)
                     /\ cur' = [cur EXCEPT ![e] = segBq[consB + 1]]
                     /\ consB' = consB + 1
                /\ UNCHANGED <<consA, lcA, headSeg, flrA, sawN>>
    /\ da' = [da EXCEPT ![e] = TRUE]
    /\ cb' = [cb EXCEPT ![e] = "retire"]
    /\ UNCHANGED <<segAq, segBq, count, signal, latch, completed, retired,
                   cNext, cPc, cc, latchPkg, countPkg, lockPkg, wireA, wireB>>

\* Shared miss body: the peek's verdict is "no completed head" - either an
\* empty verdict or a hit on a not-(yet-)completed entry. Read side effects
\* (refresh, floors, THE HOP) persist even on a miss.
PeekMissCore(e) ==
    \/ /\ headSeg = "A"
       /\ \/ \* fast hit, head not completed
             /\ consA # lcA
             /\ ~completed[segAq[consA + 1]]
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, flrB, sawN>>
          \/ \* refresh hit, head not completed
             /\ consA = lcA
             /\ \E s1 \in ReadSetA(e) :
                  /\ s1 # lcA
                  /\ ~completed[segAq[consA + 1]]
                  /\ lcA' = s1
                  /\ flrA' = FlrABump(e, s1)
             /\ UNCHANGED <<consA, consB, lcB, headSeg, flrB, sawN>>
          \/ \* hop, B head not completed (the hop still commits!)
             /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ FreshHopGuard => consA = pa
             /\ flrA' = IF FreshHopGuard THEN FlrABump(e, pa) ELSE flrA
             /\ \E s2 \in ReadSetB(e) :
                  /\ ~completed[segBq[1]]
                  /\ headSeg' = "B"
                  /\ flrB' = FlrBBump(e, s2)
                  /\ sawN' = SawNBump(e)
             /\ UNCHANGED <<consA, consB, lcA, lcB>>
          \/ \* FIX path: fresh re-read at the hop check finds A non-empty ->
             \* no hop, final read forced to truth -> hit, head not completed
             /\ FreshHopGuard
             /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ consA < pa
             /\ ~completed[segAq[consA + 1]]
             /\ flrA' = FlrABump(e, pa)
             /\ sawN' = SawNBump(e)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrB>>
          \/ \* final-read hit, head not completed (peek does NOT refresh)
             /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ FALSE \in NextReadSet(e)
             /\ \E s2 \in ReadSetA(e) :
                  /\ s2 > consA
                  /\ ~completed[segAq[consA + 1]]
                  /\ flrA' = FlrABump(e, s2)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrB, sawN>>
          \/ \* empty verdict: s1 = lastCopy, no hop, s2 = first
             /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ FALSE \in NextReadSet(e)
             /\ consA \in ReadSetA(e)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, flrB, sawN>>
    \/ /\ headSeg = "B"
       /\ \/ /\ consB # lcB
             /\ ~completed[segBq[consB + 1]]
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, flrB, sawN>>
          \/ /\ consB = lcB
             /\ \E s1 \in ReadSetB(e) :
                  /\ s1 # lcB
                  /\ ~completed[segBq[consB + 1]]
                  /\ lcB' = s1
                  /\ flrB' = FlrBBump(e, s1)
             /\ UNCHANGED <<consA, consB, lcA, headSeg, flrA, sawN>>
          \/ /\ consB = lcB
             /\ lcB \in ReadSetB(e)
             /\ \E s2 \in ReadSetB(e) :
                  /\ s2 > consB
                  /\ ~completed[segBq[consB + 1]]
                  /\ flrB' = FlrBBump(e, s2)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, sawN>>
          \/ /\ consB = lcB
             /\ lcB \in ReadSetB(e)
             /\ consB \in ReadSetB(e)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, flrB, sawN>>

\* Shared hit body WITHOUT dequeue (the reclaim's peek-only sites).
PeekHitCore(e) ==
    \/ /\ headSeg = "A"
       /\ \/ /\ consA # lcA
             /\ completed[segAq[consA + 1]]
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, flrB, sawN>>
          \/ /\ consA = lcA
             /\ \E s1 \in ReadSetA(e) :
                  /\ s1 # lcA
                  /\ completed[segAq[consA + 1]]
                  /\ lcA' = s1
                  /\ flrA' = FlrABump(e, s1)
             /\ UNCHANGED <<consA, consB, lcB, headSeg, flrB, sawN>>
          \/ /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ FreshHopGuard => consA = pa
             /\ flrA' = IF FreshHopGuard THEN FlrABump(e, pa) ELSE flrA
             /\ \E s2 \in ReadSetB(e) :
                  /\ completed[segBq[1]]
                  /\ headSeg' = "B"
                  /\ flrB' = FlrBBump(e, s2)
                  /\ sawN' = SawNBump(e)
             /\ UNCHANGED <<consA, consB, lcA, lcB>>
          \/ \* FIX path: fresh re-read at the hop check finds A non-empty ->
             \* no hop, final read forced to truth -> completed hit
             /\ FreshHopGuard
             /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ consA < pa
             /\ completed[segAq[consA + 1]]
             /\ flrA' = FlrABump(e, pa)
             /\ sawN' = SawNBump(e)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrB>>
          \/ /\ consA = lcA
             /\ lcA \in ReadSetA(e)
             /\ FALSE \in NextReadSet(e)
             /\ \E s2 \in ReadSetA(e) :
                  /\ s2 > consA
                  /\ completed[segAq[consA + 1]]
                  /\ flrA' = FlrABump(e, s2)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrB, sawN>>
    \/ /\ headSeg = "B"
       /\ \/ /\ consB # lcB
             /\ completed[segBq[consB + 1]]
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, flrB, sawN>>
          \/ /\ consB = lcB
             /\ \E s1 \in ReadSetB(e) :
                  /\ s1 # lcB
                  /\ completed[segBq[consB + 1]]
                  /\ lcB' = s1
                  /\ flrB' = FlrBBump(e, s1)
             /\ UNCHANGED <<consA, consB, lcA, headSeg, flrA, sawN>>
          \/ /\ consB = lcB
             /\ lcB \in ReadSetB(e)
             /\ \E s2 \in ReadSetB(e) :
                  /\ s2 > consB
                  /\ completed[segBq[consB + 1]]
                  /\ flrB' = FlrBBump(e, s2)
             /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg, flrA, sawN>>

\* Line 1448-1464: consume + CompleteWaiterDeferred (fused).
Retire(e) ==
    /\ cb[e] = "retire"
    /\ retired' = [retired EXCEPT ![cur[e]] = TRUE]
    /\ cb' = [cb EXCEPT ![e] = "decr"]
    /\ UNCHANGED <<count, signal, latch, completed, cNext, cPc, da, cc, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED visVars

\* Line 1487: _waiters.DecrementCount() - the count-word RMW chain edge, plus
\* the C-path _activationLock edge when the verdict is drained (line 1493's
\* lock is unconditional on the TRUE branch).
QDecr(e) ==
    /\ cb[e] = "decr"
    /\ count' = count - 1
    /\ cur' = [cur EXCEPT ![e] = 0]
    /\ cb' = [cb EXCEPT ![e] = "peek"]
    /\ IF ConsumerCachedView /\ ChainSound
       THEN LET drained == count - 1 <= 0
                inPkg == JoinP(countPkg, IF drained THEN lockPkg ELSE ZeroPkg)
            IN /\ flrA' = [flrA EXCEPT ![e] = MaxN(@, inPkg.a)]
               /\ flrB' = [flrB EXCEPT ![e] = MaxN(@, inPkg.b)]
               /\ countPkg' = JoinP(countPkg, AgentPkg(e))
               /\ lockPkg' = IF drained THEN JoinP(lockPkg, AgentPkg(e)) ELSE lockPkg
       ELSE UNCHANGED <<flrA, flrB, countPkg, lockPkg>>
    /\ UNCHANGED <<signal, latch, completed, retired, cNext, cPc, da, cc>>
    /\ UNCHANGED qVars /\ UNCHANGED <<sawN, latchPkg, wireA, wireB>>

\* Line 1439 exit: no completed head at the read instant (see PeekMissCore for
\* the read side effects, including a committed hop).
QPeekMiss(e) ==
    /\ cb[e] = "peek"
    /\ PeekMissCore(e)
    /\ cb' = [cb EXCEPT ![e] = IF da[e] THEN "rel" ELSE "consRead"]
    /\ UNCHANGED <<segAq, segBq, count, signal, latch, completed, retired,
                   cNext, cPc, da, cc, cur, latchPkg, countPkg, lockPkg,
                   wireA, wireB>>

\* Line 1555: `var conserveCount = _waiters.Count` - Volatile.Read (acquire)
\* of a value written by full-fence RMWs: a REAL edge, harvests countPkg.
QConserveRead(e) ==
    /\ cb[e] = "consRead"
    /\ cc' = [cc EXCEPT ![e] = ClampedCount]
    /\ cb' = [cb EXCEPT ![e] = "consStore"]
    /\ IF ConsumerCachedView /\ ChainSound
       THEN /\ flrA' = [flrA EXCEPT ![e] = MaxN(@, countPkg.a)]
            /\ flrB' = [flrB EXCEPT ![e] = MaxN(@, countPkg.b)]
       ELSE UNCHANGED <<flrA, flrB>>
    /\ UNCHANGED <<count, signal, latch, completed, retired, cNext, cPc, da, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED <<sawN, latchPkg, countPkg, lockPkg, wireA, wireB>>

\* Line 1557-1560: conditional restore (plain store - releases nothing).
QConserveStore(e) ==
    /\ cb[e] = "consStore"
    /\ signal' = IF ConserveRestoreEnabled /\ cc[e] > 0 THEN TRUE ELSE signal
    /\ cc' = [cc EXCEPT ![e] = 0]
    /\ cb' = [cb EXCEPT ![e] = "rel"]
    /\ UNCHANGED <<count, latch, completed, retired, cNext, cPc, da, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED visVars

\* Line 1570: ReleaseAndCheckPending, no deposit.
QRel(e) ==
    /\ cb[e] = "rel"
    /\ latch = "held"
    /\ latch' = "free"
    /\ da' = [da EXCEPT ![e] = FALSE]
    /\ cb' = [cb EXCEPT ![e] = "recheck"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, cc, cur>>

\* Line 1570: the release consumed a deposit - the obligation is now OURS.
QRelDeposit(e) ==
    /\ cb[e] = "rel"
    /\ latch = "heldPending"
    /\ latch' = "free"
    /\ da' = [da EXCEPT ![e] = FALSE]
    /\ cb' = [cb EXCEPT ![e] = "relServe"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, cc, cur>>

\* Line 1572-1575: the deposit-serve re-acquire wins.
QRelServeWin(e) ==
    /\ cb[e] = "relServe"
    /\ latch = "free"
    /\ latch' = "held"
    /\ cb' = [cb EXCEPT ![e] = "pass"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

\* Line 1572-1576: the re-acquire loses - re-deposits on the winner.
QRelServeLose(e) ==
    /\ cb[e] = "relServe"
    /\ latch # "free"
    /\ latch' = "heldPending"
    /\ cb' = [cb EXCEPT ![e] = "done"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

\* Line 1583-1586: post-release recheck of _drainSignal (plain-store word: no
\* harvest, see header).
QRecheck(e) ==
    /\ cb[e] = "recheck"
    /\ cb' = [cb EXCEPT ![e] = IF signal THEN "reclaimAcq" ELSE "done"]
    /\ UNCHANGED <<count, signal, latch, completed, retired, cNext, cPc, da, cc, cur>>
    /\ UNCHANGED qVars /\ UNCHANGED visVars

\* Line 1607 (now 1638): TryReclaimAdvancerForWork's acquire wins.
QReclaimAcq(e) ==
    /\ cb[e] = "reclaimAcq"
    /\ latch = "free"
    /\ latch' = "held"
    /\ cb' = [cb EXCEPT ![e] = "reclaimPeek"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

\* The acquire loses -> obligation deposited on the winner.
QReclaimBail(e) ==
    /\ cb[e] = "reclaimAcq"
    /\ latch # "free"
    /\ latch' = "heldPending"
    /\ cb' = [cb EXCEPT ![e] = "done"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

\* Line 1645-1649: reclaim peek finds a completed head -> continue the pass.
QReclaimHit(e) ==
    /\ cb[e] = "reclaimPeek"
    /\ PeekHitCore(e)
    /\ cb' = [cb EXCEPT ![e] = "pass"]
    /\ UNCHANGED <<segAq, segBq, count, signal, latch, completed, retired,
                   cNext, cPc, da, cc, cur, latchPkg, countPkg, lockPkg,
                   wireA, wireB>>

\* Line 1645 miss verdict. With ReclaimMissRetry the fenced re-peek interposes
\* before the release; shipped shape goes straight to the miss release.
QReclaimPeekMiss(e) ==
    /\ cb[e] = "reclaimPeek"
    /\ PeekMissCore(e)
    /\ cb' = [cb EXCEPT ![e] = IF ReclaimMissRetry THEN "reclaimRetry" ELSE "reclaimMissRel"]
    /\ UNCHANGED <<segAq, segBq, count, signal, latch, completed, retired,
                   cNext, cPc, da, cc, cur, latchPkg, countPkg, lockPkg,
                   wireA, wireB>>

(* Fix probe: the fenced re-peek. Modeled with FULL-FENCE FRESHNESS: the
   retry's reads return true last (floors raised to producer truth), per the
   probe's specification - a weaker fence-only retry could re-miss under
   unbounded staleness. It still walks the REAL algorithm on the CURRENT
   view: if _head already hopped to B, segment A is unreachable - no read
   freshness recovers a committed wrong hop. *)

QReclaimRetryHit(e) ==
    /\ cb[e] = "reclaimRetry"
    /\ \/ /\ headSeg = "A"
          /\ consA < pa
          /\ completed[segAq[consA + 1]]
          /\ lcA' = IF consA # lcA THEN lcA ELSE pa
          /\ UNCHANGED <<consA, consB, lcB, headSeg>>
       \/ \* truthful hop: A drained and B published -> follow, hit B's head
          /\ headSeg = "A"
          /\ consA = pa
          /\ NextPub
          /\ consB < pb
          /\ completed[segBq[consB + 1]]
          /\ headSeg' = "B"
          /\ UNCHANGED <<consA, consB, lcA, lcB>>
       \/ /\ headSeg = "B"
          /\ consB < pb
          /\ completed[segBq[consB + 1]]
          /\ lcB' = IF consB # lcB THEN lcB ELSE pb
          /\ UNCHANGED <<consA, consB, lcA, headSeg>>
    /\ flrA' = FlrABump(e, pa)
    /\ flrB' = FlrBBump(e, pb)
    /\ sawN' = IF NextPub THEN SawNBump(e) ELSE sawN
    /\ cb' = [cb EXCEPT ![e] = "pass"]
    /\ UNCHANGED <<segAq, segBq, count, signal, latch, completed, retired,
                   cNext, cPc, da, cc, cur, latchPkg, countPkg, lockPkg,
                   wireA, wireB>>

QReclaimRetryMiss(e) ==
    /\ cb[e] = "reclaimRetry"
    /\ \/ \* head (on the current view, truthfully read) not completed
          /\ headSeg = "A"
          /\ consA < pa
          /\ ~completed[segAq[consA + 1]]
          /\ lcA' = IF consA # lcA THEN lcA ELSE pa
          /\ UNCHANGED <<consA, consB, lcB, headSeg>>
       \/ /\ headSeg = "A"
          /\ consA = pa
          /\ NextPub
          /\ consB < pb
          /\ ~completed[segBq[consB + 1]]
          /\ headSeg' = "B"
          /\ UNCHANGED <<consA, consB, lcA, lcB>>
       \/ \* truthfully empty (current view)
          /\ headSeg = "A"
          /\ consA = pa
          /\ ~NextPub
          /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg>>
       \/ /\ headSeg = "B"
          /\ consB < pb
          /\ ~completed[segBq[consB + 1]]
          /\ lcB' = IF consB # lcB THEN lcB ELSE pb
          /\ UNCHANGED <<consA, consB, lcA, headSeg>>
       \/ /\ headSeg = "B"
          /\ consB = pb
          /\ UNCHANGED <<consA, consB, lcA, lcB, headSeg>>
    /\ flrA' = FlrABump(e, pa)
    /\ flrB' = FlrBBump(e, pb)
    /\ sawN' = IF NextPub THEN SawNBump(e) ELSE sawN
    /\ cb' = [cb EXCEPT ![e] = "reclaimMissRel"]
    /\ UNCHANGED <<segAq, segBq, count, signal, latch, completed, retired,
                   cNext, cPc, da, cc, cur, latchPkg, countPkg, lockPkg,
                   wireA, wireB>>

\* Line 1651/1659-1660: the miss release, no deposit -> exit.
QReclaimMissRel(e) ==
    /\ cb[e] = "reclaimMissRel"
    /\ latch = "held"
    /\ latch' = "free"
    /\ cb' = [cb EXCEPT ![e] = "done"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

\* Line 1651-1657: a bail landed against the transient hold - serve it.
QReclaimMissRelDeposit(e) ==
    /\ cb[e] = "reclaimMissRel"
    /\ latch = "heldPending"
    /\ latch' = "free"
    /\ cb' = [cb EXCEPT ![e] = "reclaimMissReacq"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

QReclaimMissReacqWin(e) ==
    /\ cb[e] = "reclaimMissReacq"
    /\ latch = "free"
    /\ latch' = "held"
    /\ cb' = [cb EXCEPT ![e] = "pass"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

QReclaimMissReacqLose(e) ==
    /\ cb[e] = "reclaimMissReacq"
    /\ latch # "free"
    /\ latch' = "heldPending"
    /\ cb' = [cb EXCEPT ![e] = "done"]
    /\ LatchHarvest(e) /\ LatchRest
    /\ UNCHANGED <<count, signal, completed, retired, cNext, cPc, da, cc, cur>>

(* ---------------------------------------------------------------------------
   Composition.
   ------------------------------------------------------------------------ *)

AgentNext(e) ==
    \/ CbFire(e) \/ CbAcq(e) \/ CbBail(e)
    \/ QPass(e) \/ QDrain(e) \/ Retire(e) \/ QDecr(e) \/ QPeekMiss(e)
    \/ QConserveRead(e) \/ QConserveStore(e)
    \/ QRel(e) \/ QRelDeposit(e) \/ QRelServeWin(e) \/ QRelServeLose(e)
    \/ QRecheck(e)
    \/ QReclaimAcq(e) \/ QReclaimBail(e) \/ QReclaimHit(e) \/ QReclaimPeekMiss(e)
    \/ QReclaimRetryHit(e) \/ QReclaimRetryMiss(e)
    \/ QReclaimMissRel(e) \/ QReclaimMissRelDeposit(e)
    \/ QReclaimMissReacqWin(e) \/ QReclaimMissReacqLose(e)

CommitterNext == CommitEnq \/ CommitInc

Done ==
    /\ cPc = "done"
    /\ \A e \in Entries : retired[e] /\ cb[e] = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E e \in Entries : Complete(e) \/ AgentNext(e)
    \/ CommitterNext
    \/ Done

Fairness ==
    /\ WF_vars(CommitterNext)
    /\ \A e \in Entries : WF_vars(AgentNext(e))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ---------------------------------------------------------------------------
   Properties.
   ------------------------------------------------------------------------ *)

Holders == {e \in Entries : cb[e] \in HeldPcs}

LatchOwnership ==
    /\ Cardinality(Holders) <= 1
    /\ (latch = "free") <=> (Holders = {})

CountSkewFloor == count >= -1

\* No double-drain: a retired entry is never still resident.
RetiredNotResident == \A i \in DOMAIN TrueQueue : ~retired[TrueQueue[i]]

RetiredImpliesCompleted == \A e \in Entries : retired[e] => completed[e]

PrefixCompleted(e) == \A f \in 1..e : completed[f]

EventuallyDrained == \A e \in Entries : PrefixCompleted(e) ~> retired[e]

================================================================================
