--------------------------- MODULE InFlightStore ---------------------------
(* Storage component contract.

   Owns increment-first in-flight accounting, slot/overflow publication,
   leave-head escalation, queue-before-slot observation, FIFO head removal and
   the storage half of retirement. The queue contains overflow only; escalation
   never moves the existing slot head.

   The publisher's count increment and storage publication are separate actions.
   The claimer's first-tier and second-tier observations are also separate so
   publication and escalation may interleave between them.

   Completion dispatch and consumption belong to ItemTenure. Activation turns,
   the advance license and empty-edge handoff belong to ActivationGate. This
   component relies on one authorized claimer and monotonic completion flags. *)
EXTENDS Naturals, Sequences, TLC

CONSTANT N

ASSUME N \in Nat /\ N >= 2

NONE == 0

VARIABLES
  inFlightCount,          \* count arithmetic of the shared word (increment-first)
  slot,         \* NONE | item (the slot tier)
  queuePublished,     \* the queue tier is published (escalated)
  overflowQueue,            \* the escalated queue (overflow only - leave-head)
  completionPublished,         \* [1..N -> BOOLEAN] completion flags (env; ItemTenure rely)
  headTicket,   \* last retired ticket
  publisherPc, publisherItem,     \* committer: "incrementInFlightCount"|"publishCommittedItem"|"next" over items 1..N (single producer)
  claimerPc,          \* claimer: "idle"|"readFirstTier"|"readSecondTier"|"decideHead" (license rely: ONE claimer)
  claimerQueueHead, claimerSlotHead,  \* the two reads' snapshots
  faultFifo     \* an out-of-FIFO retirement happened (the July-9 face)

vars == <<inFlightCount, slot, queuePublished, overflowQueue, completionPublished, headTicket, publisherPc, publisherItem, claimerPc, claimerQueueHead, claimerSlotHead, faultFifo>>

Init ==
  /\ inFlightCount = 0 /\ slot = NONE /\ queuePublished = FALSE /\ overflowQueue = <<>>
  /\ completionPublished = [i \in 1..N |-> FALSE]
  /\ headTicket = 0
  /\ publisherPc = "incrementInFlightCount" /\ publisherItem = 1
  /\ claimerPc = "idle" /\ claimerQueueHead = NONE /\ claimerSlotHead = NONE
  /\ faultFifo = FALSE

QueueHead == IF overflowQueue = <<>> THEN NONE ELSE overflowQueue[1]

(* ------------------------------ committer -------------------------------- *)
\* Increment-first: the count may temporarily over-promise the store.
PublisherIncrementInFlightCount ==
  /\ publisherPc = "incrementInFlightCount" /\ publisherItem <= N
  /\ inFlightCount' = inFlightCount + 1
  /\ publisherPc' = "publishCommittedItem"
  /\ UNCHANGED <<slot, queuePublished, overflowQueue, completionPublished, headTicket, publisherItem, claimerPc, claimerQueueHead, claimerSlotHead, faultFifo>>

PublisherPublishCommittedItem ==
  /\ publisherPc = "publishCommittedItem"
  /\ IF slot = NONE /\ ~queuePublished
       THEN /\ slot' = publisherItem /\ UNCHANGED <<queuePublished, overflowQueue>>
       ELSE /\ queuePublished' = TRUE /\ overflowQueue' = Append(overflowQueue, publisherItem) /\ UNCHANGED slot
  /\ publisherPc' = "finishPublication"
  /\ UNCHANGED <<inFlightCount, completionPublished, headTicket, publisherItem, claimerPc, claimerQueueHead, claimerSlotHead, faultFifo>>

PublisherFinish ==
  /\ publisherPc = "finishPublication"
  /\ UNCHANGED inFlightCount
  /\ publisherItem' = publisherItem + 1
  /\ publisherPc' = IF publisherItem = N THEN "off" ELSE "incrementInFlightCount"
  /\ UNCHANGED <<slot, queuePublished, overflowQueue, completionPublished, headTicket, claimerPc, claimerQueueHead, claimerSlotHead, faultFifo>>

(* ----------------------------- completions ------------------------------- *)
PublishCompletion(i) ==
  /\ ~completionPublished[i]
  /\ (slot = i) \/ (\E j \in 1..Len(overflowQueue) : overflowQueue[j] = i)   \* published and resident
  /\ completionPublished' = [completionPublished EXCEPT ![i] = TRUE]
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, headTicket, publisherPc, publisherItem, claimerPc, claimerQueueHead, claimerSlotHead, faultFifo>>

(* ------------------------------- claimer --------------------------------- *)
\* The licensed claimer's TWO reads with a real boundary between them.
ClaimerStart ==
  /\ claimerPc = "idle"
  /\ headTicket < N
  /\ claimerPc' = "readFirstTier"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, completionPublished, headTicket, publisherPc, publisherItem, claimerQueueHead, claimerSlotHead, faultFifo>>

ClaimerReadFirstTier ==
  /\ claimerPc = "readFirstTier"
  /\ claimerQueueHead' = QueueHead /\ UNCHANGED claimerSlotHead
  /\ claimerPc' = "readSecondTier"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, completionPublished, headTicket, publisherPc, publisherItem, faultFifo>>

ClaimerReadSecondTier ==
  /\ claimerPc = "readSecondTier"
  /\ claimerSlotHead' = slot /\ UNCHANGED claimerQueueHead
  /\ claimerPc' = "decideHead"
  /\ UNCHANGED <<inFlightCount, slot, queuePublished, overflowQueue, completionPublished, headTicket, publisherPc, publisherItem, faultFifo>>

\* The decision + claim, per the snapshots. Slot-seen-occupied -> slot leg ONLY
\* (no queue fall-through: that would retire out of order). Slot-seen-empty ->
\* queue leg on the seen head. Claims act on TRUE state (exchange/dequeue are
\* atomic); the fault is retiring anything but headTicket+1.
ClaimerDecideHead ==
  /\ claimerPc = "decideHead"
  /\ IF claimerSlotHead # NONE
       THEN \* slot leg
            IF slot = claimerSlotHead /\ completionPublished[claimerSlotHead]
              THEN /\ slot' = NONE
                   /\ headTicket' = headTicket + 1
                   /\ faultFifo' = (faultFifo \/ (claimerSlotHead # headTicket + 1))
                   /\ inFlightCount' = inFlightCount - 1
                   /\ UNCHANGED <<queuePublished, overflowQueue>>
              ELSE /\ UNCHANGED <<slot, queuePublished, overflowQueue, headTicket, inFlightCount, faultFifo>>
       ELSE IF claimerQueueHead # NONE /\ QueueHead = claimerQueueHead /\ completionPublished[claimerQueueHead]
              THEN \* queue leg (the out-of-order face when the slot is resident)
                   /\ overflowQueue' = SubSeq(overflowQueue, 2, Len(overflowQueue))
                   /\ headTicket' = headTicket + 1
                   /\ faultFifo' = (faultFifo \/ (claimerQueueHead # headTicket + 1))
                   /\ inFlightCount' = inFlightCount - 1
                   /\ UNCHANGED <<slot, queuePublished>>
              ELSE /\ UNCHANGED <<slot, queuePublished, overflowQueue, headTicket, inFlightCount, faultFifo>>
  /\ claimerPc' = "idle"
  /\ claimerQueueHead' = NONE /\ claimerSlotHead' = NONE
  /\ UNCHANGED <<completionPublished, publisherPc, publisherItem>>

Finished ==
  /\ headTicket = N /\ claimerPc = "idle" /\ publisherPc = "off"
  /\ UNCHANGED vars

Next ==
  \/ PublisherIncrementInFlightCount \/ PublisherPublishCommittedItem \/ PublisherFinish
  \/ \E i \in 1..N : PublishCompletion(i)
  \/ ClaimerStart \/ ClaimerReadFirstTier \/ ClaimerReadSecondTier \/ ClaimerDecideHead
  \/ Finished

Spec == Init /\ [][Next]_vars

(* ------------------------------ properties ------------------------------- *)
\* FIFO retirement: nothing ever retires out of ticket order (the July-9 face).
ItemsClaimedInFifoOrder == ~faultFifo

\* Count coherence under increment-first: the count never under-runs residency.
InFlightCountNonNegative == inFlightCount >= 0

\* Over-promise direction: the count is always >= what is resident (visible).
CountNeverUnderstatesResidentItems ==
  inFlightCount >= (IF slot = NONE THEN 0 ELSE 1) + Len(overflowQueue)

\* Leave-head: the slot occupant is never displaced while unretired - the queue
\* only ever holds items committed AFTER the slot occupant.
SlotItemPrecedesOverflowQueue == (slot # NONE) => \A j \in 1..Len(overflowQueue) : overflowQueue[j] > slot
=============================================================================
