# Draghi.Pipelining

Draghi.Pipelining is a source-driven, backpressure-aware execution pipeline for operations whose
work can overlap while their externally visible effects remain ordered.

A motivating application is protocol processing where a later request may still be writing while
an earlier response is read. Either direction may apply backpressure, and failure may require
ordered recovery. Draghi.Pipelining is not coupled to I/O. It supplies the timing and ownership
chain while the policy defines items, execution, activation, completion, and recovery.

## Status

Draghi.Pipelining targets .NET 10 and is intended for systems-library authors. Public types under
`Draghi.Pipelining.Internal` are experimental composition primitives and produce diagnostic
warnings when used directly.

## When to use it

Use Draghi.Pipelining when item phases progress independently but retain FIFO ownership of an
ordered resource. Backpressure in one phase must not stop the other, and recovery may need to
replace a failed item at its existing position.

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

The source controls how the executor resumes after a missed pull. A wake may resume useful work on
the signaling thread or dispatch it through a `PipelineScheduler`. This lets a synchronous caller
drive directly into its handoff without first taking a thread-pool round trip.

### Two-phase work

`ExecuteItemAsync` returns two independently progressing phase tasks:

| Item work | Protocol-client example | What it gates |
|---|---|---|
| `TrailingExecutionTask` | Finish writing the request | Dispatch of the next item |
| `PipelineTask` | Read and process the response | Terminal notification at the FIFO frontier, together with trailing settlement |

Both tasks may already be complete for a sequential round trip. The protocol roles may also be
reversed. Their names describe ordering and lifetime rather than fixed application work.

The item enters the in-flight sequence before trailing execution is awaited, allowing pipeline work
to release capacity needed by that trailing phase. `CompleteItem` waits until both obligations have
settled.

### Queue-backed use

`Pipeline.Create(policy)` creates an `UnboundedPipeline<T, TPolicy>` backed by the included SPSC
source. Its `EnqueueSignal` separates publication from the source wake so a producer can signal
after leaving its synchronization. If asynchronous continuations were disabled and the executor
has armed an empty-source wait, signaling resumes execution on the producer's thread. The source
has one logical producer, so multiple submitting threads must serialize `Enqueue`.

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
that synchronization. `EnqueueSignal` belongs to the included queue source. Custom sources define
their own submission and wake surface.

### Out-of-order engine analogy

Draghi.Pipelining can be understood as a software out-of-order engine. Work may settle out of order
behind the FIFO activation frontier, while the in-flight store preserves ordered ownership and
completion.

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

Pipeline tasks may settle ahead of the ordered frontier but cannot move it out of FIFO order. A
completing head activates its successor, while a dispatch into an empty pipeline activates itself.
No additional thread-pool work item is required merely to move the frontier.

### Activation and completion

Submission is the publication boundary. An item must already contain the state needed by
`ExecuteItemAsync` and `ActivateHeadItem` because either callback may come first. Execution starts
work or exposes existing phase tasks. Activation only grants or wakes the ordered phase.

Activation grants the FIFO head exclusive permission to use the policy-defined ordered resource. It
is a tenure-safe ownership handoff, not merely notification that the item is first in a queue. For
every item that is activated, Draghi.Pipelining guarantees:

- activation occurs at most once
- activation occurs before completion
- activation and completion do not overlap for that item

An item can finish before reaching the head, so activation is optional. This permits immediate
cancellation and pooled reuse without a stale activation touching a later tenure.

`CompleteItem` is the terminal lifecycle notification for an item. After the completion that makes
the in-flight sequence idle, `OnIdle` runs. This is an exact transition notification rather than a
stable emptiness observation: pending source work may remain, and `CompleteItem` may already have
published more work.

See [Policy contract](docs/policy-contract.md) for publication, callback concurrency, cancellation,
`ValueTask` consumption, and nonthrowing-callback requirements.

### Recovery

When a phase fails, `TryRecoverItemFailure` may return a substitute for the failed item's existing
position. Draghi completes only the substitute, which receives the ordinary activation and
completion lifecycle. This is an ordered ownership transplant, not a retry at the tail.

`OutstandingPhaseTask` carries a still-live opposite phase into recovery, allowing inherited work
and repair to remain independently live. See [Ordered substitution](docs/recovery.md) for exact
outcomes, ownership, and non-recursive recovery.

## Policy requirements

Implementing a policy is systems programming. Activation and execution may arrive in either order,
calls for different items may overlap, returned `ValueTask` instances obey single-consumption rules,
and outstanding work must converge during shutdown. Invocation or outer-task failure from
`ExecuteItemAsync` enters recovery. Other lifecycle callbacks must not throw synchronously. The
[policy contract](docs/policy-contract.md) defines the exact rules.

## Cost model

The hot path is generic over the policy, source, and concrete source-enumerator types. Supplying
them as structs forces a specialized pipeline instantiation for that composition. Focused
microbenchmarks put the synchronous queue-backed enqueue, execute, activate, and complete cycle at
approximately the cost of one single-producer, single-consumer `Channel<T>` write/read cycle. The
comparison lives in `Draghi.Pipelining.Benchmarks`. Scheduled paths also include the chosen
scheduler's dispatch cost.

This is a conservative unit-cost comparison, not a feature-equivalent baseline. A channel-based
approximation also needs separate execution and ordered-progress paths, correlation, FIFO
activation and completion, and failure coordination.

## Advanced customization

Custom sources can own admission, storage, wake behavior, migration, and undispatched shutdown
work while retaining a specialized struct-enumerator pull path. See
[Custom sources](docs/custom-sources.md) for their exact contract.

## Shutdown and reuse

`CompleteAsync` completes after source execution, in-flight work, recovery continuations, and
enumerator teardown are quiet. Repeated calls return the same run task. The source's
`CompletionToken` lets policy work converge during shutdown. A completed pipeline instance may then
be reused with a new policy and source. See [shutdown and instance reuse](docs/custom-sources.md#shutdown-and-instance-reuse).

## Build and test

```text
dotnet build Draghi.Pipelining.slnx -c Release
dotnet test --project Draghi.Pipelining.Tests/Draghi.Pipelining.Tests.csproj -c Release
```

The test suite uses Microsoft.Testing.Platform.

## Verification

The [`verification`](verification) directory contains TLA+ models for the end-to-end pipeline and
focused coordination mechanisms.

```text
cd verification
./verify check
./verify run pipeline/Pipeline.cfg
```

The verification README documents model scope, fidelity boundaries, and TLC setup.

## License

Copyright (c) 2026 Nino Floris. Licensed under the [MIT License](LICENSE).
