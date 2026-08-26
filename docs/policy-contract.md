# Policy contract

`IPipelinePolicy<T>` maps Draghi.Pipelining's lifecycle onto application items. Its callbacks may
arrive from execution, completion, scheduling, and recovery paths. Several transitions are already
irrevocable when the policy is called.

## Publication

Submission is the publication boundary. Before an item becomes visible, it must contain all state
needed by both `ExecuteItemAsync` and `ActivateHeadItem`. Activation may precede execution, so
execution must not initialize state required by activation.

A common item shape is:

- construct both phase-completion sources before submission
- let execution start work and return their tasks
- let activation grant or wake the ordered phase
- let completion publish the terminal result and reuse boundary

The included queue source publishes writes preceding `Enqueue`. A custom source must provide an
equivalent happens-before edge. Later mutations require their own synchronization.

## Callback concurrency

| Call | Contract |
|---|---|
| `ExecuteItemAsync(..., pipelineTaskRecovery: false)` | Serialized with ordinary execution calls |
| `ExecuteItemAsync(..., pipelineTaskRecovery: true)` | Serialized with recovery execution calls, but may overlap one ordinary execution call |
| `ActivateHeadItem` and `CompleteItem` for one item | Activation is optional and at most once. When present, it precedes and never overlaps completion |
| `OnIdle` | Runs after the `CompleteItem` that changes in-flight depth to zero. Source backlog may remain |
| Calls for different items | May overlap unless a stronger rule above applies |
| `TryRecoverItemFailure` after `PipelineTask` failure | May overlap ordinary item execution |

`pipelineTaskRecovery` lets a policy avoid reusing executor-local state for the recovery path that
may overlap ordinary execution.

## Asynchronous boundaries

An execution has three failure boundaries:

1. The outer `ExecuteItemAsync` result produces a `PipelineItemResult`.
2. `TrailingExecutionTask` gates dispatch of the next source item.
3. `PipelineTask` holds the independently progressing ordered position.

A synchronous exception from invoking `ExecuteItemAsync` and a fault from its outer `ValueTask`
both enter recovery as `ExecuteItemTask` failure because no phase tasks were produced.

The two phase tasks are separate consumption obligations and must not share one single-consumption
`IValueTaskSource` operation. Completion of one phase does not revoke the other. `CompleteItem`
runs only after both have settled. Recovery receives a still-live opposite phase through
`OutstandingPhaseTask` where applicable.

## Cancellation and shutdown

The token passed to execution and recovery is the source enumeration's shutdown signal. Policy
work must observe it and converge or `CompleteAsync` may remain pending. Cancellation does not
revoke application resources automatically.

## Synchronous exceptions

Invocation or outer-task failure from `ExecuteItemAsync` is supported and enters recovery. The
remaining lifecycle callbacks must not throw synchronously:

- `ActivateHeadItem`
- `CompleteItem`
- `OnIdle`
- `TryRecoverItemFailure`
- `PipelineScheduler.SubmitDetached`

Such an exception is a policy contract violation. Draghi.Pipelining cannot promise graceful
recovery after a callback interrupts an already published ownership transition.

## Struct policies

`TPolicy` acts as a concrete typeclass dictionary. A struct policy lets the JIT specialize policy
calls while leaving the item representation unconstrained. Implement every interface member
explicitly because a default interface implementation may box the policy.
