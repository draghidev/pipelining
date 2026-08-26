# Verification architecture

The verification structure mirrors the intended code split. Each component owns one class of question; `Pipeline` owns the seams between them.

## PipelineSource

Owns source order and the transfer of an item to the executor:

- items are delivered in source order and at most once;
- exhaustion is monotonic;
- requesting the successor transfers ownership of that successor to the executor.

The canonical composition uses an always-ready bounded source, so this passive
contract needs no independent configuration. Concrete waiting, wake registration,
synchronous takeover, cancellation, and queue topology are source-implementation refinements,
not pipeline semantics.

## InFlightStore

Owns membership and order:

- increment-first in-flight accounting;
- slot and overflow-queue publication;
- leave-head escalation;
- queue-before-slot observation;
- FIFO head removal and count decrement.

It does not decide who may activate or claim an item. The shared count word contains storage arithmetic and advance-license bits in the implementation, but those meanings remain separately owned.

## ItemTenure

Owns when an individual item may be touched:

- two-phase completion publication and callback dispatch;
- callback registration and the delivery arm;
- completed-head claim exclusion while callback dispatch is live;
- completion consumption and retirement;
- item-keyed delivery state across head movement.

It does not choose the activation turn or represent storage tiers.

## ActivationGate

Owns the unique activation turn:

- empty-edge assignment under the edge lock;
- resident-head activation by a retirement pass;
- dispatcher provisional-turn claim;
- activation-hand-off publication and resolution;
- the empty-edge handoff between the dispatcher and the final retirement pass;
- shared-turn arbitration and activation-before-publication ordering.

The empty-edge handoff resolves a specific activation handoff at the resident-count transition to zero.

## ItemTaskGates

Owns the independent task conditions around `ExecuteSource`:

- `PipelineTask` gates item completion and ordered retirement;
- `TrailingExecutionTask` gates issue of the next source item;
- recovery waits before touching output still owned by trailing work;
- the tail item remains reachable until both obligations are resolved.

Neither task substitutes for the other.

## Pipeline

Composes the component contracts with:

- passive ordered source delivery;
- the advance license and pending deposit;
- direct and resident retirement paths;
- completion callback delivery;
- dispatcher and retirement-pass activation races;
- execution-side and pass-side recovery;
- generation-pinned recovery arbitration;
- weak visibility of the delivery-arm clear and activation handoff;
- empty-edge handoff ordering.

The composition reduces concepts per component, not transition count. Its purpose is to prove that individually sensible protocols still satisfy their guarantees when their instruction boundaries interleave.

## Terminology rule

Names describe the owned fact or transition. In particular:

- `PassResolveEmptyEdgeHandoff` names the zero-resident transition;
- `completionCallbackRegistered` names the live registration;
- `ExecutorFenceHandoffPublication` names the ordering edge;
- `ClaimerReadFirstTier` names the observed storage tier.
