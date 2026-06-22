using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Sources;

namespace Draghi.Pipelining;

/// <summary>
/// Per-item lifecycle contract for a pipeline. The pipeline consumes items from an
/// <see cref="IPipelineSource{T,TEnumerator}"/> and invokes these methods at each lifecycle
/// transition (execute, activate, complete, recover).
/// </summary>
/// <remarks>
/// Exception robustness: policy callbacks other than <see cref="ExecuteItemAsync"/> (i.e.,
/// <see cref="ActivateHeadItem"/>, <see cref="CompleteItem"/>, <see cref="TryRecoverItemFailure"/>)
/// MUST NOT throw. The framework does not wrap them in try/catch and an escaping exception will
/// propagate into whichever context invoked it (executor loop, advancer continuation, drain path),
/// leaving the pipeline in an undefined state. Same contract as <c>IThreadPoolWorkItem.Execute</c>
/// or async continuations: the runtime expects callback authors to handle their own exception
/// domains. Exceptions from <see cref="ExecuteItemAsync"/> and from the returned pipeline/trailing
/// tasks are first-class and flow through <see cref="TryRecoverItemFailure"/>. That is the
/// supported way to surface failure from an item.
/// <para>
/// Production-side concerns (when items arrive, how the executor suspends while waiting, dispatch
/// scheduling between enqueue and execute) live on the source, not the policy. The policy is
/// purely about what to do with each item as it flows through the pipeline.
/// </para>
/// </remarks>
public interface IPipelinePolicy<T>
{
    /// Runs the item's work and returns its pipeline and trailing execution tasks.
    /// <remarks>
    /// Cancellation contract: implementations MUST observe <paramref name="cancellationToken"/>.
    /// The token is the pipeline's shutdown signal. The executor awaits this call inline during
    /// the recovery paths (<c>RecoverItem</c>, <c>RecoverTrailingFailure</c>,
    /// <c>RecoverCommittedTailWaiterAsync</c>). If an implementation ignores the token, the
    /// executor cannot exit its main loop and the pipeline's completion task will never complete.
    /// Idiomatic handling is to either throw <see cref="OperationCanceledException"/> on
    /// cancellation or return a faulted task.
    /// <para>
    /// <paramref name="waiterExecution"/> identifies which side issued this dispatch. False is the
    /// EXECUTOR side: the pump's own dispatches and the recoveries driven inline on the executor
    /// thread (<c>RecoverItem</c>, <c>RecoverTrailingFailure</c>, <c>RecoverCommittedTailWaiterAsync</c>),
    /// all serialized by the executor loop. True is the WAITER side: the waiter-drain recovery
    /// (<c>RecoverWaiter</c>), dispatched off the advancer chain when a committed waiter's pipeline
    /// task faulted. The waiter side runs off the executor thread, so it can overlap an in-flight
    /// executor dispatch - and THAT cross-side overlap is the hazard this flag exists for.
    /// Waiter executions do not overlap EACH OTHER even though waiters complete out of order: the
    /// advancer latch serializes them. It is held across the recovery dispatch (a synchronous recovery
    /// inline; an async one hands the held flag to its completion continuation, which only re-drains
    /// via <c>AdvanceAndDrainRecovery</c> after the execute settles), so a second waiter faulting
    /// concurrently loses <c>TryAcquireOrFlagPending</c> and defers rather than dispatching. Hence at
    /// most one waiter execution is ever in flight, overlapping only the single executor dispatch.
    /// A policy that shares per-dispatch state across calls on the assumption of executor-loop
    /// serialization (e.g. a pooled builder/promise reused because one dispatch completes before the
    /// next starts) MUST NOT reuse that state for a waiter execution - it would collide with the
    /// concurrent executor dispatch on the shared state.
    /// </para>
    /// </remarks>
    ValueTask<PipelineItemResult> ExecuteItemAsync(T item, bool waiterExecution, CancellationToken cancellationToken);

    /// Signals the item that it is at the head of the pipeline.
    /// When <paramref name="preferAsync"/> is true, activation should be scheduled or guarded to avoid deep recursion.
    /// <remarks>
    /// Optional per-item: an item that completes outside the head-of-pipeline position (timeout,
    /// recovery, sync-deferred path) will not see a subsequent (stale) call for activation.
    /// When called, it is guaranteed to be at most once per item,
    /// strictly before <see cref="CompleteItem"/> for the same item, and never concurrently with it.
    /// <para>
    /// Pooling: the "will never" guarantee above is load-bearing. Without it, pooling would
    /// be structurally impossible. A stale Activate arriving after the item is returned to
    /// its pool would be indistinguishable from a legitimate Activate after the item is
    /// re-enqueued for a new tenure, and the item has no identity for "this tenure" to filter
    /// on. With the guarantee in place, an item that self-completes (timeout, cancellation,
    /// internal error) can be returned to its pool inside <see cref="CompleteItem"/> safely:
    /// no subsequent Activate will touch the returned object. No coordination with this
    /// method is required.
    /// </para>
    /// <para>
    /// Today this runs under the advancer latch, so slow inline work here (or in <see cref="CompleteItem"/>)
    /// pins the drain and defers other completions. Treat <c>preferAsync: true</c> as required:
    /// dispatch off-thread, no item-body work inline. The name stays "prefer" because the
    /// constraint is a framework limitation, not the contract's intent.
    /// </para>
    /// </remarks>
    void ActivateHeadItem(T item, bool preferAsync = true);

    /// Notifies the item of completion with an optional error. <paramref name="remainingDepth"/> is the pipeline depth after this item.
    void CompleteItem(T item, int remainingDepth, Exception? exception);

    /// Attempts to recover from a failed item. Returns a recovery item that supplants the failed item in the pipeline,
    /// or null if recovery is not possible. When non-null the pipeline will not complete the failed item.
    /// NOTE: struct policies should override this to avoid DIM boxing.
    bool TryRecoverItemFailure(in PipelineItemFailureContext context, T failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out T? recoveryItem)
    {
        recoveryItem = default;
        return false;
    }
}

/// <summary>
/// Returned by <see cref="IPipelinePolicy{T}.ExecuteItemAsync"/> to describe the work an item produces.
/// </summary>
/// <remarks>
/// The two tasks are returned together so the pipeline can enqueue the item as a waiter (keyed on
/// <see cref="PipelineTask"/>) before awaiting <see cref="TrailingExecutionTask"/>. This allows the
/// item to be activated and begin its pipelined phase concurrently with trailing execution, which is
/// critical when the transport can backpressure: if both sides block on I/O and neither can make progress,
/// folding the trailing task into <see cref="IPipelinePolicy{T}.ExecuteItemAsync"/> would prevent
/// enqueueing, prevent activation, and deadlock.
/// For example, a client pipeline whose execution phase writes commands and whose pipelined phase reads
/// responses: if writes block due to a full send buffer (e.g. TCP zero window), the pipeline must still be able
/// to start reading responses. Only by enqueuing the item before awaiting trailing writes can activation
/// of the response-reading phase proceed and unblock the connection.
/// <para>
/// Each result must carry distinct task instances. An <see cref="IValueTaskSource"/>-backed
/// <see cref="ValueTask"/> shared across items would be double-consumed by the pipeline (once
/// for the trailing/pipelined inspection on the executor, once on the waiter completion path),
/// corrupting the source's token and producing undefined behavior. Distinct <see cref="Task"/>-backed
/// or <see cref="ValueTask.CompletedTask"/>/<c>default</c> instances are always safe.
/// </para>
/// <para>
/// Both phases retain their ordinary liveness obligation when the other phase faults. In particular,
/// a trailing-task failure may be reported while the pipeline task still owns an external resource;
/// recovery cannot safely revoke that ownership. The outstanding phase must therefore eventually
/// settle itself, or be terminated through the item's surrounding cancellation/abort contract.
/// </para>
/// </remarks>
public readonly struct PipelineItemResult(ValueTask trailingExecutionTask, ValueTask pipelineTask)
{
    /// <summary>
    /// Remaining execution work (typically writes) that must complete before the next item can
    /// execute. The framework awaits this between iterations to maintain backpressure on the
    /// shared transport.
    /// </summary>
    /// <remarks>
    /// Items returning a non-default <see cref="TrailingExecutionTask"/> are routed through the
    /// tail-waiter path unconditionally. The framework guarantees that
    /// <see cref="IPipelinePolicy{T}.CompleteItem"/> for such an item will fire only after this
    /// task has been observed (success or fault), regardless of when <see cref="PipelineTask"/>
    /// completes. Policy authors can return a sync-complete pipeline task while their trailing
    /// task is still pending. The framework will not fire <c>CompleteItem</c> until trailing
    /// has finished. No internal composition required in the item.
    /// </remarks>
    public ValueTask TrailingExecutionTask { get; } = trailingExecutionTask;
    /// <summary>The item's pipelined-phase task. Completion signals the item is done in the pipeline.</summary>
    public ValueTask PipelineTask { get; } = pipelineTask;

    public PipelineItemResult(ValueTask pipelineTask)
        : this(default, pipelineTask) { }

    public static implicit operator PipelineItemResult(ValueTask pipelineTask) => new(pipelineTask);
}
