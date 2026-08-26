--------------------- MODULE PipelineSource ---------------------
(* Passive source contract used by the pipeline composition.

   The source owns an ordered, finite sequence of items. Delivery transfers one
   item to the executor exactly once; the executor may request the successor only
   after it has discharged the current item's trailing-execution obligation.

   Waiting, wake registration, synchronous takeover, cancellation, and concrete
   queue storage are source-implementation concerns. They refine whether a delivery is ready,
   not the ordering or ownership established here.                            *)
EXTENDS Naturals

CONSTANT N

FirstSourceItem == 1

SourceInit(currentItem) == currentItem = FirstSourceItem

SourceHasSuccessor(currentItem) == currentItem < N

SourceIsExhausted(currentItem) == currentItem = N

SourceDeliverSuccessor(currentItem) ==
  /\ SourceHasSuccessor(currentItem)
  /\ currentItem' = currentItem + 1

===============================================================
