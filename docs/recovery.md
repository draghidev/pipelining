# Ordered substitution

Recovery is a `try`/`catch` at the pipeline-position level. When an item phase fails, Draghi
consults the policy before releasing that FIFO position. The policy may decline recovery or replace
the failed item with a substitute in the same position. Later items cannot pass until the position
retires.

## Outcomes

| Verdict | Failed item | Pipeline position |
|---|---|---|
| Recovery declined | Receives `CompleteItem` with its failure | Retires normally |
| Substitute succeeds | Is not completed by Draghi.Pipelining | Retires through the substitute |
| Substitute fails | Is not completed by Draghi.Pipelining | The substitute completes with its failure and the position retires without recursive recovery |

When substitution succeeds, the policy owns the failed item's remaining lifetime. It may already
have delivered its failure, own no resources, or need to remain retained until the substitute no
longer refers to it.

## Outstanding opposite phase

`PipelineItemFailureContext.Kind` identifies the failed boundary. When the opposite phase remains
live, `OutstandingPhaseTask` transfers that obligation to recovery:

- trailing-execution failure may carry the pipeline task
- pipeline-task failure may carry trailing execution

Failure on one side does not revoke the other side's resource ownership. Recovery must preserve or
await that work before touching the same resource. This allows inherited work and repair to remain
independently live when serializing them could prevent either phase from progressing.

## Substitute lifecycle

The substitute receives ordinary execution, activation, and completion at the inherited position.
It may be activated before execution when that position already owns the ordered frontier. A
substitute failure completes the substitute and retires the position. Recovery is not consulted
recursively.

Without structural substitution, equivalent recovery would have to wrap every item's outer
execution, trailing, and pipeline tasks to route failures, preserve the opposite phase, and
arbitrate replacement. Successful items would pay for that composition too. Draghi instead routes
an observed failure through the position's existing lifecycle machinery and creates a substitute
only on the failure path.

## Protocol-client example

Slon's PostgreSQL `ResyncRecoveryFlow` remains in the failed flow's response position while any
live read or write settles. When message boundaries remain trustworthy, it repairs recoverable
frontend framing, synchronizes the extended-query protocol, and rolls back the transaction and its
transaction-local session state. It then consumes the exact outstanding `ReadyForQuery` boundaries
until the decoder is aligned with the next flow's first response.

This centralizes PostgreSQL resynchronization in one substitute instead of requiring every flow to
implement and sequence its own repair path. That recovery boundary also lets Slon admit custom
flows alongside trusted ones: a recoverable custom-flow failure is repaired at its ordered
position before successors regain the decoder.

Normal Slon flows can return their read and write tasks directly without paying for recovery on
successful execution. When recovery is needed, it lets Slon repair the connection while keeping
the pipeline and its already in-flight successors active. Recoverable failures include an
unexpected exception during ordinary flow execution, a streaming parameter writer leaving a
partial frontend message, a custom flow failing after partial protocol progress, or even a
PostgreSQL pooler producing unexpected backend-message ordering on an edge path. Recovery remains
possible only while Slon can account for the framing and response boundaries. Otherwise
substitution is declined and the connection is condemned.
