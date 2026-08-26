# Custom sources

Begin with the queue-backed `Pipeline.Create(policy)`. Implement
`IPipelineSource<T, TEnumerator>` only when admission, storage, wakeup, placement, migration, or
undispatched shutdown work requires different behavior.

## Pull contract

A source returns a concrete struct enumerator:

- `TryGetNext` returns a visible item or reports a current miss.
- `WaitForNextAsync` returns `true` to retry the pull and `false` after completion.
- `Complete` begins shutdown while permitting residual drain.
- `DisposeAsync` performs terminal cleanup.

Concrete `TSource` and `TEnumerator` arguments let the JIT specialize this path without boxing or
interface dispatch. `PipelineSourceAsyncEnumerable` provides `IAsyncEnumerable<T>` interoperation
when that matters more than specialization.

## Miss and wake handshake

The transition from a missed pull to waiting must arm and then recheck the source. An item arriving
between the miss and continuation registration must either cause an immediate retry or wake the
waiter. Checking an empty source and then registering a continuation creates a lost-wakeup window.

`SourceWakeEvent` implements this handshake for `UnboundedQueueSource<T>`. It holds the source check
through continuation registration and lets a signal resume the executor inline or through a
`PipelineScheduler`.

## Publication and depth

`TryGetNext` must observe all item initialization preceding submission. State mutated after
publication needs separate synchronization.

Pipeline depth begins when the executor dispatches an item, not when the source accepts it. Pending
items remain outside depth and may be reclaimed, disposed, or migrated according to source policy.

## Included queue source

`UnboundedQueueSource<T>` has one logical producer and one consumer. Multiple producers must
serialize `Enqueue`. It publishes the item immediately and returns a deferred signal. Invoke that
signal after leaving producer synchronization. If asynchronous continuations were disabled and the
executor has armed an empty-source wait, signaling resumes execution inline. This signal API is
specific to the included queue source. Custom sources define their own wake surface.

`Backlog` reports accepted but undispatched items. Together with pipeline `Depth`, it describes
outstanding work, although either observation may already be stale.

## Shutdown and instance reuse

The source enumerator owns the shutdown token supplied to execution and recovery. Its work must
converge after `Complete` so disposal and pipeline completion can become quiet.

After the preceding completion task settles, a completed
`Pipeline<T, TPolicy, TSource, TEnumerator>` instance may be passed back to `Pipeline.Create` with a
new policy and source. Reuse retains coordination allocations while resetting per-run state.
