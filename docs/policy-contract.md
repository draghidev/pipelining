# Policy contract

`IPipelinePolicy<T>` maps Draghi.Pipelining's structural lifecycle onto application items. A policy
is systems-level code: callbacks may arrive from executor, completion, scheduler, and recovery
paths, and several transitions are already irrevocable when the policy is called.

## Item construction and publication

Submission is the publication boundary. Before a source makes an item visible, the item must
construct every piece of state needed by both `ExecuteItemAsync` and `ActivateHeadItem`.

Activation may occur before or after `ExecuteItemAsync`, depending on whether the dispatched item is
already at the ordered frontier. `ExecuteItemAsync` must therefore not initialize state that
activation requires. A common shape is:

- construct both phase-completion sources before submission
- let execution start work and return those phase tasks
- let activation only grant or wake the ordered phase
- let completion publish the item's terminal result and reuse boundary

The included queue source safely publishes the item and all writes preceding `Enqueue`. A custom
source must provide an equivalent happens-before edge before `TryGetNext` returns the item. State
mutated after submission needs its own synchronization.

## Execution and callback concurrency

| Call | Concurrency contract |
|---|---|
| `ExecuteItemAsync(..., pipelineTaskRecovery: false)` | Serialized with other ordinary execution calls |
| `ExecuteItemAsync(..., pipelineTaskRecovery: true)` | Serialized with other pipeline-task recovery calls, but may overlap one ordinary execution call |
| `ActivateHeadItem` and `CompleteItem` for one item | Activation is optional and at most once; when present it precedes and never overlaps completion |
| Policy calls for different items | May overlap unless a more specific guarantee applies |
| `TryRecoverItemFailure` for `PipelineTask` failure | May overlap ordinary item execution |

`pipelineTaskRecovery` exists so a policy does not reuse executor-local single-pump state for the
one recovery path that may overlap the executor. Slon, for example, routes that call away from its
pooled execution promise.

## Three asynchronous boundaries

An execution produces three failure boundaries:

1. The outer `ExecuteItemAsync` result produces a `PipelineItemResult`.
2. `TrailingExecutionTask` gates dispatch of the next source item.
3. `PipelineTask` represents independently progressing work at the ordered frontier.

A synchronous exception from invoking `ExecuteItemAsync` and a fault from its outer `ValueTask`
have the same meaning: no phase tasks were produced. Draghi.Pipelining classifies both as
`ExecuteItemTask` failure and consults recovery.

The two phase tasks are separate consumption obligations. They must not refer to the same
single-consumption `IValueTaskSource` operation. Synchronously completed values and task-backed
values may safely be reused.

Completion of one phase does not revoke the other. `CompleteItem` runs only after both obligations
have been observed. When one phase fails while the other remains live, recovery receives the
opposite task through `OutstandingPhaseTask` where applicable.

## Cancellation and shutdown

The cancellation token passed to execution and recovery is the source enumeration's shutdown
signal. Policies must observe it and converge outstanding work. Ignoring it can prevent the executor
from leaving its loop and keep `CompleteAsync` pending indefinitely.

Cancellation does not automatically revoke application resources. Each item remains responsible
for making its phase tasks settle under its surrounding abort and cancellation contract.

## Nonthrowing lifecycle callbacks

`ExecuteItemAsync` is the exception to the general nonthrowing rule: its invocation or outer task
may fail normally and enter recovery.

The remaining lifecycle callbacks must not throw synchronously:

- `ActivateHeadItem` publishes or schedules a bounded wake. Operational failure belongs in the
  item's task machinery.
- `CompleteItem` performs terminal notification and cleanup after structural retirement.
- `TryRecoverItemFailure` returns `false` when it cannot provide a substitute.
- `PipelineScheduler.SubmitDetached` is a fire-and-forget primitive and must not throw.

A synchronous exception from one of these critical callbacks is a policy contract violation.
Draghi.Pipelining does not promise graceful recovery after a callback interrupts an already
published ownership transition.

## Struct policies

`TPolicy` acts as a concrete typeclass dictionary. A struct policy allows the JIT to specialize
policy calls while the item type remains free to use the representation best suited to storage and
atomic publication. Implement every interface slot explicitly. Default interface implementations
on a struct policy can introduce boxing and defeat that specialization.
