# Fidelity boundaries

| Model | Establishes | Relies on | Deliberately omits |
|---|---|---|---|
| `Pipeline` | Ordered source delivery, activation, callback delivery, FIFO claim/retirement, empty-edge handoff, and recovery composition. | The two-task execution contract. | Source wake mechanics, public depth, shutdown wakeup, pipeline reuse, physical worker scheduling, and distinct substitute identity at one FIFO position. |
| `PipelineSource` | FIFO, at-most-once delivery, monotonic exhaustion, and source-to-executor ownership transfer. | The executor requests a successor only after discharging the current trailing-execution obligation. | Readiness, waiting, wake registration, cancellation, synchronous takeover, and concrete storage. |
| `ActivationGate` | Unique activation turn, edge-lock exclusion, dispatcher/empty-edge both-miss prevention. | Abstract item completion and serialized retirement claims. | Storage tiers, callback dispatch internals, and recovery. |
| `InFlightStore` | Increment/publication order, leave-head escalation, FIFO observation and removal. | One authorized claimer and monotonic completion. | Activation, callback delivery, and recovery. |
| `ItemTenure` | Two-phase completion, delivery-arm ordering, claim/dispatch exclusion, retirement. | Serialized claim attempts and abstract FIFO head identity. | Empty-edge arbitration and storage topology. |
| `ItemTaskGates` | Pipeline-task retirement gate, trailing-task successor gate, recovery/output tenure. | Atomic task-state publication. | Activation, store, ROB mechanics, and wakeups. |
| `QueueDrainObligation` | Detailed queue visibility, drain-obligation conservation, reclaim behavior, and eventual retirement of completed FIFO prefixes. | Its encoded SPSC visibility model. | Activation policy and shutdown. |
| `StoreEscalation` | Leave-head topology, slot-before-overflow order, activation safety, FIFO and exactly-once retirement. | Abstract task completion and fair execution. | Full advance-license protocol. |
| `DepthDrain` | Split depth-counter empty-wait arm and zero revalidation. | Atomic counter/TCS operations. | Item order and activation. |
| `WakeHandshake` | Symmetric store/load lost-wake theorem. | The chosen fence strength. | Feature-specific state outside the handshake. |

## Liveness

`Pipeline` intentionally has no fairness assumptions. Deadlock checks and safety invariants provide strong progress evidence but are not a fair-environment eventual-retirement proof. Focused mechanism models add fairness where the environment they model supplies it. Runtime liveness under real scheduling remains additionally covered by concurrency tests and soaks.

## Weak memory

The models represent only load-bearing weak-memory relations:

- local versus visible delivery-arm clear;
- activation-handoff publication versus the empty-edge count observation;
- symmetric store/load wake handshakes;
- selected SPSC cached-view reads.

They do not claim to model the complete ECMA/.NET or hardware memory model. A green result certifies the encoded ordering argument, not arbitrary compiler transformations.

## Identity

Most models identify items by FIFO position. A recovery substitute reuses the failed position with reset tenure state. This proves position and activation-turn inheritance but not arbitrary ABA over multiple concrete objects at the same position. That boundary should be extended with a tenure generation if concrete item-identity failures become a live concern.
