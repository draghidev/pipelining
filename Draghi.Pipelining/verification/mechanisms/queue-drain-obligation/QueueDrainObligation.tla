---------------------------- MODULE QueueDrainObligation ----------------------------
(* Detailed queue-mode retirement-obligation model.

   The producer enqueues entries and increments the in-flight count in separate
   steps. Each completion callback publishes a drain signal, then either acquires
   the advance license or deposits a pending request. A licensed retirement pass
   observes, claims and retires FIFO heads; signal conservation, release, recheck
   and reclaim are modeled as separate actions.

   The SPSC queue is represented by two
   segments, consumer-owned cached producer indices, a possibly stale next-segment
   observation, and per-callback visibility floors. Synchronization limits
   reads to values allowed by the modeled synchronization chain.

   The consumer rereads the
   current segment producer index before advancing to the published next segment.
   Without it, a split read can skip newly resident entries in the old segment.

   Activation policy, recovery and shutdown are deliberately omitted. The checked
   question is whether every completed FIFO prefix is eventually retired.          *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    NumEntries,             \* number of in-flight items
    SegmentCapacity         \* segment A capacity C (usable C-1 before split)

ASSUME SegmentCapacity >= 2

Entries == 1..NumEntries

VARIABLES
    \* Shared protocol state.
    count,      \* InFlightStore._countAndLicense count field (reads clamp)
    signal,     \* _drainSignal
    advanceLicense,      \* the advance-license word: "free" | "held" | "heldPending"
    \* Ground truth.
    completed,  \* [Entries -> BOOLEAN] pipeline task settled
    retired,    \* [Entries -> BOOLEAN] RetireItemDeferred ran
    \* Committer thread (the executor, single producer).
    publisherNextEntry,      \* next entry to commit (entries commit in id order)
    publisherPc,        \* "enq" | "inc" | "done"
    \* Per-entry callback agents.
    callbackPc,         \* [Entries -> pc]
    passDrainedAny,         \* [Entries -> BOOLEAN] pass-local drainedAny
    conservationCount,         \* [Entries -> 0..NumEntries] pass-local conserveCount read
    claimedEntry,        \* [Entries -> 0..NumEntries] dequeued-entry-in-hand (0 = none)
    \* The SPSC queue, segment/sequence-number encoding (see header).
    segmentAItems,      \* Seq(Entries): entries enqueued to segment A, in order
    segmentBItems,      \* Seq(Entries): entries enqueued to segment B, in order
    segmentAConsumed,      \* consumer A._first as a sequence number (entries dequeued from A)
    segmentBConsumed,      \* consumer B._first as a sequence number
    segmentALastSeen,        \* consumer A._lastCopy as a sequence number
    segmentBLastSeen,        \* consumer B._lastCopy (preset 1 by EnqueueSlow)
    headSegment,    \* consumer _head: "A" | "B"
    \* Per-thread visibility floors (cached-view read model).
    segmentAReadFloor,       \* [Entries -> Nat] floor on reads of A._last
    segmentBReadFloor,       \* [Entries -> Nat] floor on reads of B._last
    sawNextSegment,       \* [Entries -> BOOLEAN] thread saw A._next non-null
    advanceLicenseVisibility,   \* producer-progress package released through the advanceLicense word
    countVisibility,   \* package released through the count word RMW chain
    lockVisibility,    \* package released through _activationLock
    segmentAPublicationWire,      \* [Entries -> Nat] segmentAPublished snapshot at the entry's PublisherIncrementCount (wiring)
    segmentBPublicationWire       \* [Entries -> Nat] segmentBPublished snapshot at the entry's PublisherIncrementCount

vars == <<count, signal, advanceLicense, completed, retired, publisherNextEntry, publisherPc, callbackPc, passDrainedAny, conservationCount, claimedEntry,
          segmentAItems, segmentBItems, segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment,
          segmentAReadFloor, segmentBReadFloor, sawNextSegment, advanceLicenseVisibility, countVisibility, lockVisibility, segmentAPublicationWire, segmentBPublicationWire>>

queueStateVars   == <<segmentAItems, segmentBItems, segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment>>
visibilityStateVars == <<segmentAReadFloor, segmentBReadFloor, sawNextSegment, advanceLicenseVisibility, countVisibility, lockVisibility, segmentAPublicationWire, segmentBPublicationWire>>

CallbackStates == {"idle", "acq", "pass", "peek", "retire", "decr", "consRead",
          "consStore", "rel", "relServe", "recheck", "reclaimAcq",
          "reclaimPeek", "reclaimMissRel", "reclaimMissReacq",
          "done"}

LicenseHeldStates == {"pass", "peek", "retire", "decr", "consRead", "consStore", "rel",
            "reclaimPeek", "reclaimMissRel"}

ClampedCount == IF count > 0 THEN count ELSE 0   \* InFlightStore.Count

MaxN(x, y) == IF x >= y THEN x ELSE y
EmptyVisibility == [a |-> 0, b |-> 0]
MergeVisibility(p, q) == [a |-> MaxN(p.a, q.a), b |-> MaxN(p.b, q.b)]

segmentAPublished == Len(segmentAItems)                 \* true A._last as a sequence number
segmentBPublished == Len(segmentBItems)                 \* true B._last as a sequence number
NextSegmentPublished == segmentBPublished > 0                \* A._next published (fused with B creation)
PublishedVisibility == [a |-> segmentAPublished, b |-> segmentBPublished]
AgentVisibility(e) == [a |-> segmentAReadFloor[e], b |-> segmentBReadFloor[e]]

\* Ground-truth FIFO residency (A entries strictly precede B entries: the
\* producer never returns to A once B exists).
TrueQueue == SubSeq(segmentAItems, segmentAConsumed + 1, segmentAPublished) \o SubSeq(segmentBItems, segmentBConsumed + 1, segmentBPublished)

(* ---------------------------------------------------------------------------
   The read model. ReadSetA/B: values an acquire read of the segment's _last
   may return for agent e. NextReadSet: values the plain _next read may return.
   Reads range from the agent's coherence floor to the published value.
   ------------------------------------------------------------------------ *)

ReadSetA(e) == MaxN(segmentALastSeen, segmentAReadFloor[e])..segmentAPublished
ReadSetB(e) == MaxN(segmentBLastSeen, segmentBReadFloor[e])..segmentBPublished

NextReadSet(e) ==
    IF ~NextSegmentPublished THEN {FALSE}
    ELSE IF sawNextSegment[e] \/ segmentBReadFloor[e] > 0 THEN {TRUE}
    ELSE {TRUE, FALSE}

RaiseSegmentAReadFloor(e, v) == [segmentAReadFloor EXCEPT ![e] = MaxN(@, v)]
RaiseSegmentBReadFloor(e, v) == [segmentBReadFloor EXCEPT ![e] = MaxN(@, v)]
RecordNextSegmentSeen(e)    == [sawNextSegment EXCEPT ![e] = TRUE]

TypeOK ==
    /\ count \in -1..NumEntries
    /\ signal \in BOOLEAN
    /\ advanceLicense \in {"free", "held", "heldPending"}
    /\ completed \in [Entries -> BOOLEAN]
    /\ retired \in [Entries -> BOOLEAN]
    /\ publisherNextEntry \in 1..(NumEntries + 1)
    /\ publisherPc \in {"enq", "inc", "done"}
    /\ callbackPc \in [Entries -> CallbackStates]
    /\ passDrainedAny \in [Entries -> BOOLEAN]
    /\ conservationCount \in [Entries -> 0..NumEntries]
    /\ claimedEntry \in [Entries -> 0..NumEntries]
    /\ segmentAItems \in Seq(Entries) /\ segmentBItems \in Seq(Entries)
    /\ segmentAPublished + segmentBPublished <= NumEntries
    /\ segmentAConsumed \in 0..segmentAPublished /\ segmentBConsumed \in 0..segmentBPublished
    /\ segmentALastSeen \in 0..segmentAPublished /\ segmentBLastSeen \in 1..MaxN(segmentBPublished, 1)
    /\ headSegment \in {"A", "B"}
    /\ segmentAReadFloor \in [Entries -> 0..NumEntries] /\ segmentBReadFloor \in [Entries -> 0..NumEntries]
    /\ sawNextSegment \in [Entries -> BOOLEAN]
    /\ advanceLicenseVisibility \in [a: 0..NumEntries, b: 0..NumEntries]
    /\ countVisibility \in [a: 0..NumEntries, b: 0..NumEntries]
    /\ lockVisibility \in [a: 0..NumEntries, b: 0..NumEntries]
    /\ segmentAPublicationWire \in [Entries -> 0..NumEntries] /\ segmentBPublicationWire \in [Entries -> 0..NumEntries]

\* Consumer-view sanity. The residency window (segmentAPublished - segmentAConsumed <= C-1) is what
\* licenses the sequence-number encoding of the mod-C comparisons.
ConsumerViewWithinPublishedBounds ==
    /\ segmentAConsumed <= segmentALastSeen
    /\ segmentBConsumed <= segmentBLastSeen \/ segmentBPublished = 0
    /\ segmentAPublished - segmentAConsumed <= SegmentCapacity - 1
    /\ headSegment = "B" => segmentBPublished >= 1
    /\ segmentBPublished = 0 => (segmentBConsumed = 0 /\ headSegment = "A" /\ segmentBLastSeen = 1)
    /\ \A e \in Entries : segmentAReadFloor[e] <= segmentAPublished /\ segmentBReadFloor[e] <= segmentBPublished
    /\ advanceLicenseVisibility.a <= segmentAPublished /\ advanceLicenseVisibility.b <= segmentBPublished
    /\ countVisibility.a <= segmentAPublished /\ countVisibility.b <= segmentBPublished
    /\ lockVisibility.a <= segmentAPublished /\ lockVisibility.b <= segmentBPublished

Init ==
    /\ count = 0
    /\ signal = FALSE
    /\ advanceLicense = "free"
    /\ completed = [e \in Entries |-> FALSE]
    /\ retired = [e \in Entries |-> FALSE]
    /\ publisherNextEntry = 1
    /\ publisherPc = IF NumEntries = 0 THEN "done" ELSE "enq"
    /\ callbackPc = [e \in Entries |-> "idle"]
    /\ passDrainedAny = [e \in Entries |-> FALSE]
    /\ conservationCount = [e \in Entries |-> 0]
    /\ claimedEntry = [e \in Entries |-> 0]
    /\ segmentAItems = <<>> /\ segmentBItems = <<>>
    /\ segmentAConsumed = 0 /\ segmentBConsumed = 0
    /\ segmentALastSeen = 0 /\ segmentBLastSeen = 1
    /\ headSegment = "A"
    /\ segmentAReadFloor = [e \in Entries |-> 0] /\ segmentBReadFloor = [e \in Entries |-> 0]
    /\ sawNextSegment = [e \in Entries |-> FALSE]
    /\ advanceLicenseVisibility = EmptyVisibility /\ countVisibility = EmptyVisibility /\ lockVisibility = EmptyVisibility
    /\ segmentAPublicationWire = [e \in Entries |-> 0] /\ segmentBPublicationWire = [e \in Entries |-> 0]

(* ---------------------------------------------------------------------------
   Synchronization-edge helpers.
   ------------------------------------------------------------------------ *)

\* Every advanceLicense operation is a full-fence RMW on the fused InFlightStore word: release local
\* knowledge into the chain, acquire everything already there.
ImportAdvanceLicenseVisibility(e) ==
    /\ advanceLicenseVisibility' = MergeVisibility(advanceLicenseVisibility, AgentVisibility(e))
    /\ segmentAReadFloor' = [segmentAReadFloor EXCEPT ![e] = MaxN(@, advanceLicenseVisibility.a)]
    /\ segmentBReadFloor' = [segmentBReadFloor EXCEPT ![e] = MaxN(@, advanceLicenseVisibility.b)]

\* Advance-license actions never touch the other visibility state or the queue view.
PreserveVisibility == UNCHANGED <<sawNextSegment, countVisibility, lockVisibility, segmentAPublicationWire, segmentBPublicationWire>> /\ UNCHANGED queueStateVars

(* ---------------------------------------------------------------------------
   Environment: adversarial pipeline-task settlement.
   ------------------------------------------------------------------------ *)

PublishCompletion(e) ==
    /\ ~completed[e]
    /\ completed' = [completed EXCEPT ![e] = TRUE]
    /\ UNCHANGED <<count, signal, advanceLicense, retired, publisherNextEntry, publisherPc, callbackPc, passDrainedAny, conservationCount, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED visibilityStateVars

(* ---------------------------------------------------------------------------
   Committer (executor thread). queue.Enqueue routes per the real producer
   tree: segment A while it has room under the full check (wrap-at-full lives
   here: room iff segmentAPublished - first < C-1), else EnqueueSlow allocates B (preset
   B._last = B._lastCopy = 1, first item at B[0]) and publishes A._next; once
   B exists every enqueue lands in B. Producer _firstCopy refresh fused
   truthful (see header). Then Interlocked.Increment(_count) - separate step,
   the count under-promises the queue.
   ------------------------------------------------------------------------ *)

PublisherEnqueueEntry ==
    /\ publisherPc = "enq"
    /\ IF segmentBPublished > 0
       THEN segmentBItems' = Append(segmentBItems, publisherNextEntry) /\ UNCHANGED segmentAItems
       ELSE IF segmentAPublished - segmentAConsumed < SegmentCapacity - 1
            THEN segmentAItems' = Append(segmentAItems, publisherNextEntry) /\ UNCHANGED segmentBItems
            ELSE segmentBItems' = Append(segmentBItems, publisherNextEntry) /\ UNCHANGED segmentAItems
    /\ publisherPc' = "inc"
    /\ UNCHANGED <<count, signal, advanceLicense, completed, retired, publisherNextEntry, callbackPc, passDrainedAny, conservationCount, claimedEntry>>
    /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment>> /\ UNCHANGED visibilityStateVars

\* Increment + the wiring snapshot (callback continuation starts with the
\* producer knowledge at wiring) + the count-word release + the wasEmpty
\* _activationLock release (CommitInFlightItem 1100-1122, taken when the sole
\* in-flight item is not yet completed; modeled always-taken - max edges).
PublisherIncrementCount ==
    /\ publisherPc = "inc"
    /\ count' = count + 1
    /\ segmentAPublicationWire' = [segmentAPublicationWire EXCEPT ![publisherNextEntry] = segmentAPublished]
    /\ segmentBPublicationWire' = [segmentBPublicationWire EXCEPT ![publisherNextEntry] = segmentBPublished]
    /\ countVisibility' = MergeVisibility(countVisibility, PublishedVisibility)
    /\ lockVisibility' = IF count = 0 /\ ~completed[publisherNextEntry]
                          THEN MergeVisibility(lockVisibility, PublishedVisibility) ELSE lockVisibility
    /\ publisherNextEntry' = publisherNextEntry + 1
    /\ publisherPc' = IF publisherNextEntry = NumEntries THEN "done" ELSE "enq"
    /\ UNCHANGED <<signal, advanceLicense, completed, retired, callbackPc, passDrainedAny, conservationCount, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED <<segmentAReadFloor, segmentBReadFloor, sawNextSegment, advanceLicenseVisibility>>

(* ---------------------------------------------------------------------------
   Callback: the registered head-completion callback (Pipeline.OnAdvanceCallback).
   ------------------------------------------------------------------------ *)

\* The callback publishes its pending-pass request. The agent thread starts with the wiring
\* visibility (UnsafeOnCompleted registered after the entry's increment).
CompletionCallbackPublishSignal(e) ==
    /\ callbackPc[e] = "idle"
    /\ completed[e]
    /\ publisherNextEntry > e
    /\ signal' = TRUE
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "acq"]
    /\ segmentAReadFloor' = [segmentAReadFloor EXCEPT ![e] = MaxN(@, segmentAPublicationWire[e])]
    /\ segmentBReadFloor' = [segmentBReadFloor EXCEPT ![e] = MaxN(@, segmentBPublicationWire[e])]
    /\ UNCHANGED <<count, advanceLicense, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED <<sawNextSegment, advanceLicenseVisibility, countVisibility, lockVisibility, segmentAPublicationWire, segmentBPublicationWire>>

\* InFlightStore.TryAcquire wins -> run the retirement pass.
CompletionCallbackAcquireLicense(e) ==
    /\ callbackPc[e] = "acq"
    /\ advanceLicense = "free"
    /\ advanceLicense' = "held"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "pass"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

\* A failed acquire deposits the obligation in the advance-license word.
CompletionCallbackDepositPending(e) ==
    /\ callbackPc[e] = "acq"
    /\ advanceLicense # "free"
    /\ advanceLicense' = "heldPending"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "done"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

(* ---------------------------------------------------------------------------
   Drain: the licensed FIFO retirement pass (Pipeline.Advance).
   ------------------------------------------------------------------------ *)

\* Pass start consumes the pending callback signal. The RMW
\* on the SIGNAL word: its other writers are plain stores heading no release
\* sequence, so no visibility is harvested here (see header).
DrainPassStart(e) ==
    /\ callbackPc[e] = "pass"
    /\ signal' = FALSE
    /\ passDrainedAny' = [passDrainedAny EXCEPT ![e] = FALSE]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "peek"]
    /\ UNCHANGED <<count, advanceLicense, completed, retired, publisherNextEntry, publisherPc, conservationCount, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED visibilityStateVars

(* The peek machine, TryPeek/TryDequeue(Slow) decision tree on the CACHED
   view:
     fast hit          : first # lastCopy (cached!) -> head = seg[first]
     slow refresh      : read s1 of true _last; s1 # lastCopy -> refresh, hit
     hop               : s1 = lastCopy /\ _next read non-null /\ first = s1
                         -> _head = B (IRREVERSIBLE), then B's preset view
     final verdict     : no hop; read s2 >= s1; s2 = first -> empty,
                         s2 > first -> hit WITHOUT refreshing lastCopy
   DrainPassReadHead additionally fuses the dequeue (same-entry proof in header); its
   final-hit shape is the only one reaching the line-253/255 slow-tail
   dequeue, whose freshness refresh is the s3 read. *)

\* A peek hit on a completed head claims it and records that the pass retired an item.
DrainPassReadHead(e) ==
    /\ callbackPc[e] = "peek"
    /\ \/ /\ headSegment = "A"
          /\ \/ \* fast hit (first # lastCopy on the cached copy)
                /\ segmentAConsumed # segmentALastSeen
                /\ completed[segmentAItems[segmentAConsumed + 1]]
                /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentAItems[segmentAConsumed + 1]]
                /\ segmentAConsumed' = segmentAConsumed + 1
                /\ UNCHANGED <<segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
             \/ \* slow refresh hit (s1 # lastCopy -> lastCopy = s1, retry hits)
                /\ segmentAConsumed = segmentALastSeen
                /\ \E s1 \in ReadSetA(e) :
                     /\ s1 # segmentALastSeen
                     /\ completed[segmentAItems[segmentAConsumed + 1]]
                     /\ segmentALastSeen' = s1
                     /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, s1)
                     /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentAItems[segmentAConsumed + 1]]
                     /\ segmentAConsumed' = segmentAConsumed + 1
                /\ UNCHANGED <<segmentBConsumed, segmentBLastSeen, headSegment, segmentBReadFloor, sawNextSegment>>
             \/ \* hop: s1 = lastCopy, _next non-null, and a fresh producer-index
                \* read confirms permanent emptiness before _head moves to B
                /\ segmentAConsumed = segmentALastSeen
                /\ segmentALastSeen \in ReadSetA(e)
                /\ TRUE \in NextReadSet(e)
                /\ segmentAConsumed = segmentAPublished
                /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, segmentAPublished)
                /\ \E s2 \in ReadSetB(e) :
                     /\ completed[segmentBItems[1]]
                     /\ headSegment' = "B"
                     /\ segmentBConsumed' = 1
                     /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s2)
                     /\ sawNextSegment' = RecordNextSegmentSeen(e)
                     /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentBItems[1]]
                /\ UNCHANGED <<segmentAConsumed, segmentALastSeen, segmentBLastSeen>>
             \/ \* the hop check's fresh re-read finds A non-empty ->
                \* no hop, and the raised floor forces the final read (and the
                \* fused dequeue's slow-tail refresh) to truth -> hit A's head
                /\ segmentAConsumed = segmentALastSeen
                /\ segmentALastSeen \in ReadSetA(e)
                /\ TRUE \in NextReadSet(e)
                /\ segmentAConsumed < segmentAPublished
                /\ completed[segmentAItems[segmentAConsumed + 1]]
                /\ segmentALastSeen' = segmentAPublished
                /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, segmentAPublished)
                /\ sawNextSegment' = RecordNextSegmentSeen(e)
                /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentAItems[segmentAConsumed + 1]]
                /\ segmentAConsumed' = segmentAConsumed + 1
                /\ UNCHANGED <<segmentBConsumed, segmentBLastSeen, headSegment, segmentBReadFloor>>
             \/ \* final-read hit; the fused dequeue's slow tail refreshes (s3)
                /\ segmentAConsumed = segmentALastSeen
                /\ segmentALastSeen \in ReadSetA(e)
                /\ FALSE \in NextReadSet(e)
                /\ \E s2 \in ReadSetA(e) :
                     /\ s2 > segmentAConsumed
                     /\ completed[segmentAItems[segmentAConsumed + 1]]
                     /\ segmentALastSeen' = s2
                     /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, s2)
                     /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentAItems[segmentAConsumed + 1]]
                     /\ segmentAConsumed' = segmentAConsumed + 1
                /\ UNCHANGED <<segmentBConsumed, segmentBLastSeen, headSegment, segmentBReadFloor, sawNextSegment>>
       \/ /\ headSegment = "B"
          /\ \/ /\ segmentBConsumed # segmentBLastSeen
                /\ completed[segmentBItems[segmentBConsumed + 1]]
                /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentBItems[segmentBConsumed + 1]]
                /\ segmentBConsumed' = segmentBConsumed + 1
                /\ UNCHANGED <<segmentAConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
             \/ /\ segmentBConsumed = segmentBLastSeen
                /\ \E s1 \in ReadSetB(e) :
                     /\ s1 # segmentBLastSeen
                     /\ completed[segmentBItems[segmentBConsumed + 1]]
                     /\ segmentBLastSeen' = s1
                     /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s1)
                     /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentBItems[segmentBConsumed + 1]]
                     /\ segmentBConsumed' = segmentBConsumed + 1
                /\ UNCHANGED <<segmentAConsumed, segmentALastSeen, headSegment, segmentAReadFloor, sawNextSegment>>
             \/ /\ segmentBConsumed = segmentBLastSeen
                /\ segmentBLastSeen \in ReadSetB(e)
                /\ \E s2 \in ReadSetB(e) :
                     /\ s2 > segmentBConsumed
                     /\ completed[segmentBItems[segmentBConsumed + 1]]
                     /\ segmentBLastSeen' = s2
                     /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s2)
                     /\ claimedEntry' = [claimedEntry EXCEPT ![e] = segmentBItems[segmentBConsumed + 1]]
                     /\ segmentBConsumed' = segmentBConsumed + 1
                /\ UNCHANGED <<segmentAConsumed, segmentALastSeen, headSegment, segmentAReadFloor, sawNextSegment>>
    /\ passDrainedAny' = [passDrainedAny EXCEPT ![e] = TRUE]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "retire"]
    /\ UNCHANGED <<segmentAItems, segmentBItems, count, signal, advanceLicense, completed, retired,
                   publisherNextEntry, publisherPc, conservationCount, advanceLicenseVisibility, countVisibility, lockVisibility, segmentAPublicationWire, segmentBPublicationWire>>

\* Shared miss body: the peek's verdict is "no completed head" - either an
\* empty verdict or a hit on a not-(yet-)completed entry. Read side effects
\* The refreshed indices, visibility floors and segment hop persist even on a miss.
HeadReadMiss(e) ==
    \/ /\ headSegment = "A"
       /\ \/ \* fast hit, head not completed
             /\ segmentAConsumed # segmentALastSeen
             /\ ~completed[segmentAItems[segmentAConsumed + 1]]
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
          \/ \* refresh hit, head not completed
             /\ segmentAConsumed = segmentALastSeen
             /\ \E s1 \in ReadSetA(e) :
                  /\ s1 # segmentALastSeen
                  /\ ~completed[segmentAItems[segmentAConsumed + 1]]
                  /\ segmentALastSeen' = s1
                  /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, s1)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentBLastSeen, headSegment, segmentBReadFloor, sawNextSegment>>
          \/ \* hop, B head not completed (the hop still commits!)
             /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ segmentAConsumed = segmentAPublished
             /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, segmentAPublished)
             /\ \E s2 \in ReadSetB(e) :
                  /\ ~completed[segmentBItems[1]]
                  /\ headSegment' = "B"
                  /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s2)
                  /\ sawNextSegment' = RecordNextSegmentSeen(e)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen>>
          \/ \* fresh re-read at the hop check finds A non-empty ->
             \* no hop, final read forced to truth -> hit, head not completed
             /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ segmentAConsumed < segmentAPublished
             /\ ~completed[segmentAItems[segmentAConsumed + 1]]
             /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, segmentAPublished)
             /\ sawNextSegment' = RecordNextSegmentSeen(e)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentBReadFloor>>
          \/ \* final-read hit, head not completed (peek does NOT refresh)
             /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ FALSE \in NextReadSet(e)
             /\ \E s2 \in ReadSetA(e) :
                  /\ s2 > segmentAConsumed
                  /\ ~completed[segmentAItems[segmentAConsumed + 1]]
                  /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, s2)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentBReadFloor, sawNextSegment>>
          \/ \* empty verdict: s1 = lastCopy, no hop, s2 = first
             /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ FALSE \in NextReadSet(e)
             /\ segmentAConsumed \in ReadSetA(e)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
    \/ /\ headSegment = "B"
       /\ \/ /\ segmentBConsumed # segmentBLastSeen
             /\ ~completed[segmentBItems[segmentBConsumed + 1]]
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
          \/ /\ segmentBConsumed = segmentBLastSeen
             /\ \E s1 \in ReadSetB(e) :
                  /\ s1 # segmentBLastSeen
                  /\ ~completed[segmentBItems[segmentBConsumed + 1]]
                  /\ segmentBLastSeen' = s1
                  /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s1)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, headSegment, segmentAReadFloor, sawNextSegment>>
          \/ /\ segmentBConsumed = segmentBLastSeen
             /\ segmentBLastSeen \in ReadSetB(e)
             /\ \E s2 \in ReadSetB(e) :
                  /\ s2 > segmentBConsumed
                  /\ ~completed[segmentBItems[segmentBConsumed + 1]]
                  /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s2)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, sawNextSegment>>
          \/ /\ segmentBConsumed = segmentBLastSeen
             /\ segmentBLastSeen \in ReadSetB(e)
             /\ segmentBConsumed \in ReadSetB(e)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>

\* Shared hit body WITHOUT dequeue (the reclaim's peek-only sites).
HeadReadHit(e) ==
    \/ /\ headSegment = "A"
       /\ \/ /\ segmentAConsumed # segmentALastSeen
             /\ completed[segmentAItems[segmentAConsumed + 1]]
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
          \/ /\ segmentAConsumed = segmentALastSeen
             /\ \E s1 \in ReadSetA(e) :
                  /\ s1 # segmentALastSeen
                  /\ completed[segmentAItems[segmentAConsumed + 1]]
                  /\ segmentALastSeen' = s1
                  /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, s1)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentBLastSeen, headSegment, segmentBReadFloor, sawNextSegment>>
          \/ /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ segmentAConsumed = segmentAPublished
             /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, segmentAPublished)
             /\ \E s2 \in ReadSetB(e) :
                  /\ completed[segmentBItems[1]]
                  /\ headSegment' = "B"
                  /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s2)
                  /\ sawNextSegment' = RecordNextSegmentSeen(e)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen>>
          \/ \* fresh re-read at the hop check finds A non-empty ->
             \* no hop, final read forced to truth -> completed hit
             /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ TRUE \in NextReadSet(e)
             /\ segmentAConsumed < segmentAPublished
             /\ completed[segmentAItems[segmentAConsumed + 1]]
             /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, segmentAPublished)
             /\ sawNextSegment' = RecordNextSegmentSeen(e)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentBReadFloor>>
          \/ /\ segmentAConsumed = segmentALastSeen
             /\ segmentALastSeen \in ReadSetA(e)
             /\ FALSE \in NextReadSet(e)
             /\ \E s2 \in ReadSetA(e) :
                  /\ s2 > segmentAConsumed
                  /\ completed[segmentAItems[segmentAConsumed + 1]]
                  /\ segmentAReadFloor' = RaiseSegmentAReadFloor(e, s2)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentBReadFloor, sawNextSegment>>
    \/ /\ headSegment = "B"
       /\ \/ /\ segmentBConsumed # segmentBLastSeen
             /\ completed[segmentBItems[segmentBConsumed + 1]]
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, segmentBReadFloor, sawNextSegment>>
          \/ /\ segmentBConsumed = segmentBLastSeen
             /\ \E s1 \in ReadSetB(e) :
                  /\ s1 # segmentBLastSeen
                  /\ completed[segmentBItems[segmentBConsumed + 1]]
                  /\ segmentBLastSeen' = s1
                  /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s1)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, headSegment, segmentAReadFloor, sawNextSegment>>
          \/ /\ segmentBConsumed = segmentBLastSeen
             /\ segmentBLastSeen \in ReadSetB(e)
             /\ \E s2 \in ReadSetB(e) :
                  /\ s2 > segmentBConsumed
                  /\ completed[segmentBItems[segmentBConsumed + 1]]
                  /\ segmentBReadFloor' = RaiseSegmentBReadFloor(e, s2)
             /\ UNCHANGED <<segmentAConsumed, segmentBConsumed, segmentALastSeen, segmentBLastSeen, headSegment, segmentAReadFloor, sawNextSegment>>

\* Consume and RetireItemDeferred are fused into this action.
RetireClaimedEntry(e) ==
    /\ callbackPc[e] = "retire"
    /\ retired' = [retired EXCEPT ![claimedEntry[e]] = TRUE]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "decr"]
    /\ UNCHANGED <<count, signal, advanceLicense, completed, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED visibilityStateVars

\* InFlightStore.DecrementCount supplies the count-word RMW chain edge, plus
\* the empty-edge activation-lock edge when the verdict is drained (the
\* lock is unconditional on the TRUE branch).
DrainPassDecrementCount(e) ==
    /\ callbackPc[e] = "decr"
    /\ count' = count - 1
    /\ claimedEntry' = [claimedEntry EXCEPT ![e] = 0]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "peek"]
    /\ LET drained == count - 1 <= 0
           inPkg == MergeVisibility(countVisibility, IF drained THEN lockVisibility ELSE EmptyVisibility)
       IN /\ segmentAReadFloor' = [segmentAReadFloor EXCEPT ![e] = MaxN(@, inPkg.a)]
          /\ segmentBReadFloor' = [segmentBReadFloor EXCEPT ![e] = MaxN(@, inPkg.b)]
          /\ countVisibility' = MergeVisibility(countVisibility, AgentVisibility(e))
          /\ lockVisibility' = IF drained THEN MergeVisibility(lockVisibility, AgentVisibility(e)) ELSE lockVisibility
    /\ UNCHANGED <<signal, advanceLicense, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED <<sawNextSegment, advanceLicenseVisibility, segmentAPublicationWire, segmentBPublicationWire>>

\* Exit when no completed head exists at the read instant (see HeadReadMiss for
\* the read side effects, including a committed hop).
DrainPassRecordHeadMiss(e) ==
    /\ callbackPc[e] = "peek"
    /\ HeadReadMiss(e)
    /\ callbackPc' = [callbackPc EXCEPT ![e] = IF passDrainedAny[e] THEN "rel" ELSE "consRead"]
    /\ UNCHANGED <<segmentAItems, segmentBItems, count, signal, advanceLicense, completed, retired,
                   publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry, advanceLicenseVisibility, countVisibility, lockVisibility,
                   segmentAPublicationWire, segmentBPublicationWire>>

\* Reading InFlightStore.Count conserves the observed count with acquire semantics
\* of a value written by full-fence RMWs: a REAL edge, harvests countVisibility.
DrainPassReadConservationCount(e) ==
    /\ callbackPc[e] = "consRead"
    /\ conservationCount' = [conservationCount EXCEPT ![e] = ClampedCount]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "consStore"]
    /\ segmentAReadFloor' = [segmentAReadFloor EXCEPT ![e] = MaxN(@, countVisibility.a)]
    /\ segmentBReadFloor' = [segmentBReadFloor EXCEPT ![e] = MaxN(@, countVisibility.b)]
    /\ UNCHANGED <<count, signal, advanceLicense, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED <<sawNextSegment, advanceLicenseVisibility, countVisibility, lockVisibility, segmentAPublicationWire, segmentBPublicationWire>>

\* Conditional signal restoration is a plain store and releases nothing.
DrainPassRestoreSignal(e) ==
    /\ callbackPc[e] = "consStore"
    /\ signal' = IF conservationCount[e] > 0 THEN TRUE ELSE signal
    /\ conservationCount' = [conservationCount EXCEPT ![e] = 0]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "rel"]
    /\ UNCHANGED <<count, advanceLicense, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED visibilityStateVars

\* ReleaseAndCheckPending finds no deposit.
DrainPassReleaseLicense(e) ==
    /\ callbackPc[e] = "rel"
    /\ advanceLicense = "held"
    /\ advanceLicense' = "free"
    /\ passDrainedAny' = [passDrainedAny EXCEPT ![e] = FALSE]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "recheck"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, conservationCount, claimedEntry>>

\* The release consumes a deposit, transferring the obligation to this pass.
DrainPassReleaseWithPending(e) ==
    /\ callbackPc[e] = "rel"
    /\ advanceLicense = "heldPending"
    /\ advanceLicense' = "free"
    /\ passDrainedAny' = [passDrainedAny EXCEPT ![e] = FALSE]
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "relServe"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, conservationCount, claimedEntry>>

\* The deposit-serve reacquire wins.
DrainPassServePendingAndReacquire(e) ==
    /\ callbackPc[e] = "relServe"
    /\ advanceLicense = "free"
    /\ advanceLicense' = "held"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "pass"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

\* A lost reacquire redeposits on the winner.
DrainPassServePendingButLose(e) ==
    /\ callbackPc[e] = "relServe"
    /\ advanceLicense # "free"
    /\ advanceLicense' = "heldPending"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "done"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

\* Post-release recheck of the drain signal (plain-store word: no
\* harvest, see header).
DrainPassRecheckSignal(e) ==
    /\ callbackPc[e] = "recheck"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = IF signal THEN "reclaimAcq" ELSE "done"]
    /\ UNCHANGED <<count, signal, advanceLicense, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>
    /\ UNCHANGED queueStateVars /\ UNCHANGED visibilityStateVars

\* The attempt to reclaim the advance license wins.
DrainPassReclaimLicense(e) ==
    /\ callbackPc[e] = "reclaimAcq"
    /\ advanceLicense = "free"
    /\ advanceLicense' = "held"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "reclaimPeek"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

\* The acquire loses -> obligation deposited on the winner.
DrainPassReclaimDeposit(e) ==
    /\ callbackPc[e] = "reclaimAcq"
    /\ advanceLicense # "free"
    /\ advanceLicense' = "heldPending"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "done"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

\* The reclaim peek finds a completed head and continues the pass.
DrainPassReclaimHeadHit(e) ==
    /\ callbackPc[e] = "reclaimPeek"
    /\ HeadReadHit(e)
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "pass"]
    /\ UNCHANGED <<segmentAItems, segmentBItems, count, signal, advanceLicense, completed, retired,
                   publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry, advanceLicenseVisibility, countVisibility, lockVisibility,
                   segmentAPublicationWire, segmentBPublicationWire>>

\* A reclaim-peek miss proceeds directly to the miss release.
DrainPassReclaimHeadMiss(e) ==
    /\ callbackPc[e] = "reclaimPeek"
    /\ HeadReadMiss(e)
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "reclaimMissRel"]
    /\ UNCHANGED <<segmentAItems, segmentBItems, count, signal, advanceLicense, completed, retired,
                   publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry, advanceLicenseVisibility, countVisibility, lockVisibility,
                   segmentAPublicationWire, segmentBPublicationWire>>

\* A miss release with no deposit exits.
DrainPassReleaseAfterReclaimMiss(e) ==
    /\ callbackPc[e] = "reclaimMissRel"
    /\ advanceLicense = "held"
    /\ advanceLicense' = "free"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "done"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

\* A bail landed against the transient hold, so serve it.
DrainPassReleaseMissWithPending(e) ==
    /\ callbackPc[e] = "reclaimMissRel"
    /\ advanceLicense = "heldPending"
    /\ advanceLicense' = "free"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "reclaimMissReacq"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

DrainPassReacquireAfterMiss(e) ==
    /\ callbackPc[e] = "reclaimMissReacq"
    /\ advanceLicense = "free"
    /\ advanceLicense' = "held"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "pass"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

DrainPassLoseReacquireAfterMiss(e) ==
    /\ callbackPc[e] = "reclaimMissReacq"
    /\ advanceLicense # "free"
    /\ advanceLicense' = "heldPending"
    /\ callbackPc' = [callbackPc EXCEPT ![e] = "done"]
    /\ ImportAdvanceLicenseVisibility(e) /\ PreserveVisibility
    /\ UNCHANGED <<count, signal, completed, retired, publisherNextEntry, publisherPc, passDrainedAny, conservationCount, claimedEntry>>

(* ---------------------------------------------------------------------------
   Composition.
   ------------------------------------------------------------------------ *)

CompletionAgentNext(e) ==
    \/ CompletionCallbackPublishSignal(e) \/ CompletionCallbackAcquireLicense(e) \/ CompletionCallbackDepositPending(e)
    \/ DrainPassStart(e) \/ DrainPassReadHead(e) \/ RetireClaimedEntry(e) \/ DrainPassDecrementCount(e) \/ DrainPassRecordHeadMiss(e)
    \/ DrainPassReadConservationCount(e) \/ DrainPassRestoreSignal(e)
    \/ DrainPassReleaseLicense(e) \/ DrainPassReleaseWithPending(e) \/ DrainPassServePendingAndReacquire(e) \/ DrainPassServePendingButLose(e)
    \/ DrainPassRecheckSignal(e)
    \/ DrainPassReclaimLicense(e) \/ DrainPassReclaimDeposit(e) \/ DrainPassReclaimHeadHit(e) \/ DrainPassReclaimHeadMiss(e)
    \/ DrainPassReleaseAfterReclaimMiss(e) \/ DrainPassReleaseMissWithPending(e)
    \/ DrainPassReacquireAfterMiss(e) \/ DrainPassLoseReacquireAfterMiss(e)

PublisherNext == PublisherEnqueueEntry \/ PublisherIncrementCount

Done ==
    /\ publisherPc = "done"
    /\ \A e \in Entries : retired[e] /\ callbackPc[e] = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E e \in Entries : PublishCompletion(e) \/ CompletionAgentNext(e)
    \/ PublisherNext
    \/ Done

Fairness ==
    /\ WF_vars(PublisherNext)
    /\ \A e \in Entries : WF_vars(CompletionAgentNext(e))

Spec == Init /\ [][Next]_vars /\ Fairness

(* ---------------------------------------------------------------------------
   Properties.
   ------------------------------------------------------------------------ *)

Holders == {e \in Entries : callbackPc[e] \in LicenseHeldStates}

AdvanceLicenseHasSingleOwner ==
    /\ Cardinality(Holders) <= 1
    /\ (advanceLicense = "free") <=> (Holders = {})

InFlightCountUnderrunIsAtMostOne == count >= -1

\* No double-drain: a retired entry is never still resident.
RetiredEntryNotResident == \A i \in DOMAIN TrueQueue : ~retired[TrueQueue[i]]

OnlyCompletedEntriesRetire == \A e \in Entries : retired[e] => completed[e]

PrefixCompleted(e) == \A f \in 1..e : completed[f]

CompletedPrefixesEventuallyRetire == \A e \in Entries : PrefixCompleted(e) ~> retired[e]

================================================================================
