# Draghi.Pipelining

Draghi.Pipelining is a source-driven, backpressure-aware execution pipeline for operations whose
work can overlap while their externally visible effects remain ordered.

A motivating application is pipelined protocol processing. In a protocol client, one operation may
still be writing while an earlier one is reading, either direction may apply backpressure, and a
failure may require an ordered recovery operation. A protocol server commonly assigns the read and
write roles in the opposite direction. These are policy mappings, not roles fixed by the pipeline.
Draghi.Pipelining itself is not coupled to I/O. It supplies the timing and ownership chain that lets
work progress and settle out of order while activation and terminal notification cross an ordered
frontier. Items, execution, activation, completion, and recovery are supplied by the policy.

## When to use it

Use Draghi.Pipelining when items have independently progressing phases but must retain FIFO
ownership of an ordered resource. It is intended for cases where backpressure in one phase must not
prevent progress in the other, or where failure may require a substitute to replace the failed item
at its existing ordered position.

For example, a protocol client may block while writing request `B` because its peer cannot continue
consuming requests until the client reads response `A`. Draghi.Pipelining can activate `A`'s read
while `B`'s trailing write remains pending. It also supports synchronous callers driving directly
into their ordered handoff, so progress in the common single-request case does not depend on another
ThreadPool worker becoming available under starvation.

Use a channel or conventional asynchronous pump when each admitted wave has a real bound on encoded
bytes and retained resources, its send phase can finish while reception is paused, interactive work
may serialize processing, and uncertain failure may terminate the shared resource.

## The model

A pipeline has four participants:

- The **source** owns admission, pending-item storage, and the empty-to-ready wakeup.
- The **executor** pulls items from the source and invokes their execution phase in source order.
- The **in-flight sequence** retains dispatched items and advances its FIFO head when work completes.
- The **policy** maps those lifecycle transitions onto an application's items.

The source controls how the executor resumes after a missed pull. A wake may let the signaling
caller resume useful work inline, or may dispatch it through a `PipelineScheduler`.
For example, a synchronous caller can be allowed to drive execution directly into its handoff
instead of first taking a thread-pool round trip. Other resumptions can be dispatched through the
scheduler.

### Two-phase work

`ExecuteItemAsync` returns two independently progressing phase tasks:

| Item work | Protocol-client example | What it gates |
|---|---|---|
| `TrailingExecutionTask` | Finish writing the request | Dispatch of the next item |
| `PipelineTask` | Read and process the response | Terminal notification at the FIFO frontier, together with trailing settlement |

`ExecuteItemAsync` produces the two phase tasks. It may instead perform an entire sequential
roundtrip and return both tasks already completed, requiring no cross-phase coordination.

Those examples reverse naturally for a protocol server. More generally, the names describe ordering
and lifetime rather than application roles. A policy decides what work belongs to each phase.

The item is committed to the in-flight sequence before the pipeline awaits trailing execution. This
lets pipeline work begin and release capacity needed by trailing execution while that trailing work
remains incomplete.

`CompleteItem` does not run until both phase tasks have been observed. A pipeline task that settles
while trailing execution remains pending therefore cannot complete its position early.

### Queue-backed use

`Pipeline.Create(policy)` creates an `UnboundedPipeline<T, TPolicy>` backed by the included SPSC
source. Enqueueing and signaling are deliberately separate because signaling may synchronously run
the executor on the caller's thread. The source has one logical producer. Multiple submitting
threads must serialize `Enqueue`; signaling may happen after leaving that synchronization.

The following conceptual example shows the policy mapping. `Operation` owns the phase tasks and the
activation/completion state.

```csharp
using Draghi.Pipelining;

readonly struct ProtocolPolicy : IPipelinePolicy<Operation>
{
    public async ValueTask<PipelineItemResult> ExecuteItemAsync(
        Operation item,
        bool pipelineTaskRecovery,
        CancellationToken cancellationToken)
    {
        var phases = await item.StartAsync(cancellationToken);
        return new(
            trailingExecutionTask: phases.WriteTask,
            pipelineTask: phases.ReadTask);
    }

    public void ActivateHeadItem(Operation item, bool preferAsync = true)
        => item.GrantReaderTurn(preferAsync);

    public void CompleteItem(Operation item, Exception? exception)
        => item.Complete(exception);

    public void OnIdle() { }

    public bool TryRecoverItemFailure(
        in PipelineItemFailureContext context,
        Operation failedItem,
        CancellationToken cancellationToken,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Operation? substitute)
    {
        substitute = null;
        return false;
    }
}

var pipeline = Pipeline.Create<Operation, ProtocolPolicy>(new());

var enqueue = pipeline.Enqueue(operation);
enqueue.Signal();

await operation.Completion;
await pipeline.CompleteAsync();
```

Call `Enqueue` under producer synchronization if necessary, but call the returned `Signal` outside
that synchronization. Applications with different admission or storage requirements can provide a
custom source.

### Out-of-order engine analogy

Draghi.Pipelining can be understood as a software out-of-order engine. The executor dispatches work into an
in-flight sequence, independent item work may complete out of order, and the activation frontier
advances in FIFO order. The in-flight store plays a role similar to a reorder buffer: it permits
overlap behind the head while preserving the ordered point at which an item may take ownership of a
shared resource and complete.

The analogy is structural rather than literal. Policies define what execution and activation mean,
sources define admission, and recovery may replace a failed item in its existing position.

| Frontier | Advances when | Controls |
|---|---|---|
| Execution frontier | The current trailing task settles | When the next source item may execute |
| Ordered frontier | The FIFO head's pipeline task and trailing obligation have settled | Activation of the next live head and terminal notification |

### A concrete progression

Consider three items arriving in source order: `A`, `B`, then `C`.

1. `A` reaches an empty pipeline, is activated, and executes.
2. `A` enters the in-flight sequence before its trailing task is awaited.
3. When `A`'s trailing task settles, `B` executes. `C` can execute after `B`'s trailing task settles.
4. `B.PipelineTask` settles before `A.PipelineTask`, for example because `B` was cancelled. `B`
   remains in the in-flight sequence because `A` still owns the ordered frontier.
5. `A.PipelineTask` settles, so `CompleteItem(A)` runs.
6. The frontier reaches the already-settled `B`, so `CompleteItem(B)` runs without activation.
7. `C` is still live, so `ActivateHeadItem(C)` grants its ordered phase.
8. When `C.PipelineTask` settles, `CompleteItem(C)` runs.

The execution frontier therefore advances through trailing-task settlement, while the ordered
frontier advances through the in-flight sequence. Pipeline tasks may settle ahead of that ordered
frontier, but cannot move it out of FIFO order.

Activation moves directly between participants that already own progress. A completing head can
activate its successor, and a dispatch into an empty pipeline can activate its own item.
Draghi.Pipelining does not need an additional thread-pool work item just to move the frontier, as a
naive implementation might. The policy may still dispatch the activated item's own work.

### Activation and completion

Submission is the publication boundary for an item. Before the source makes it visible, the item
must construct all state needed by both `ExecuteItemAsync` and `ActivateHeadItem`.
`ExecuteItemAsync` must not initialize state that activation requires because either callback may
come first. Execution starts work or exposes the item's existing phase tasks. Activation only grants
or wakes the ordered phase.

Activation grants the FIFO head exclusive permission to use the policy-defined ordered resource. It
is a tenure-safe ownership handoff, not merely notification that the item is first in a queue. For
every item that is activated, Draghi.Pipelining guarantees:

- activation occurs at most once
- activation occurs before completion
- activation and completion do not overlap for that item

An item can finish before reaching the head, so activation is optional. This is what makes immediate
cancellation and pooled item reuse possible without a stale activation touching a later tenure.
Activation may occur before or after `ExecuteItemAsync` is called, depending on whether the item is
already at the ordered frontier when it is dispatched.

`CompleteItem` is the terminal lifecycle notification for an item. After the completion that makes
the in-flight sequence idle, `OnIdle` runs. This is an exact transition notification rather than a
stable emptiness observation: pending source work may remain, and `CompleteItem` may already have
published more work.

See [Policy contract](docs/policy-contract.md) for publication, callback concurrency, cancellation,
`ValueTask` consumption, and nonthrowing-callback requirements.

### Recovery

When an item phase fails, the pipeline calls `TryRecoverItemFailure`. A successful recovery returns a
substitute in `recoveryItem`. The substitute assumes the failed item's position, and the failed
item is not separately completed. It receives its own activation and completion callbacks under the
same lifecycle rules as any other item in that position.

Recovery is therefore an ordered ownership transplant, not a retry or generic exception callback.
The substitute inherits the failed position and responsibility for restoring its shared resource
before later items may pass it.

When the failed item's opposite phase remains live, `OutstandingPhaseTask` carries that obligation
into recovery. The substitute can sequence inherited work through one phase while repair progresses
through the other. See [Ordered substitution](docs/recovery.md) for exact outcomes, opposite-phase
ownership, and non-recursive recovery.

## Policy requirements

Implementing a policy is systems programming. Activation and execution may occur in either order,
calls for different items may overlap, returned `ValueTask` instances must obey single-consumption
rules, and outstanding work must converge during shutdown. Failure while invoking
`ExecuteItemAsync`, or while awaiting its outer task, enters ordinary recovery. By contrast,
`ActivateHeadItem`, `CompleteItem`, `OnIdle`, `TryRecoverItemFailure`, and `SubmitDetached` must not throw
synchronously. The [policy contract](docs/policy-contract.md) defines these rules.

## Cost model

The hot path is generic over the policy, source, and concrete source-enumerator types. Supplying
them as structs forces a specialized pipeline instantiation for that composition. Focused
microbenchmarks put the synchronous queue-backed enqueue, execute, activate, and complete cycle at
approximately the cost of one single-producer, single-consumer `Channel<T>` write/read cycle. The
comparison lives in `Draghi.Pipelining.Benchmarks`; scheduled paths additionally include the chosen
scheduler's dispatch cost.

That is a conservative unit-cost comparison, not a feature-equivalent baseline. A channel-based
approximation needs at least separate execution and ordered-progress paths, plus correlation, FIFO
activation and completion, and failure coordination around them. Draghi.Pipelining was designed so
that richer lifecycle does not impose a double-channel coordination tax on every successful item.

## Advanced customization

Custom sources can own admission, item storage, wake behavior, migration, and what happens to
pending undispatched items during shutdown. Their concrete struct enumerators retain the specialized
pull path. See
[Custom sources](docs/custom-sources.md) for the publication, miss-and-wake, completion, and instance
reuse contracts.

## Shutdown and reuse

`CompleteAsync` initiates shutdown and completes only after source execution, in-flight completion,
recovery continuations, and enumerator teardown are quiet. Repeated calls return the same run task.

The source owns its cancellation lifecycle. Its `CompletionToken` is passed into policy execution and
recovery so outstanding work can converge during shutdown.

A completed pipeline instance may be reused with a new policy and source after the preceding
completion task settles. The
[custom-source guide](docs/custom-sources.md#shutdown-and-instance-reuse) describes that lifecycle.

## Verification

The coordination protocols are accompanied by TLA+ models under
[`Draghi.Pipelining/verification`](Draghi.Pipelining/verification). They cover the end-to-end pipeline
and focused mechanisms including item tenure, empty-edge activation, depth draining, wake handshakes,
and in-flight-store advancement.

```text
cd Draghi.Pipelining/verification
./verify check
./verify run pipeline/Pipeline.cfg
```

The verification README documents model scope, fidelity boundaries, and the required TLC setup.
