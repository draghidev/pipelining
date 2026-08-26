using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Sources;

namespace Draghi.Pipelining;

/// <summary>
/// Per-item lifecycle contract for a pipeline. The pipeline consumes items from an
/// <see cref="IPipelineSource{T,TEnumerator}"/> and invokes these methods at each lifecycle
/// transition (execute, activate, complete, idle, recover).
/// </summary>
/// <remarks>
/// <see cref="ExecuteItemAsync"/> may fail synchronously or through its returned task; both are
/// first-class item-execution failures and flow through recovery. The remaining policy callbacks
/// MUST NOT throw synchronously. They run at lifecycle transitions where a thrown exception cannot
/// reliably describe which policy effects occurred.
/// <para>
/// Production-side concerns (when items arrive, how the executor suspends while waiting, dispatch
/// scheduling between enqueue and execute) live on the source, not the policy. The policy is
/// purely about what to do with each item as it flows through the pipeline.
/// </para>
/// <para>
/// Unless a method documents a stronger guarantee, policy calls for different items may overlap.
/// Per-item activation and completion ordering is documented on <see cref="ActivateHeadItem"/>.
/// </para>
/// </remarks>
public interface IPipelinePolicy<T>
{
    /// Runs the item's work and returns its pipeline and trailing execution tasks.
    /// <remarks>
    /// Cancellation contract: implementations MUST observe <paramref name="cancellationToken"/>.
    /// The token is the pipeline's shutdown signal. The executor awaits this call inline during
    /// the recovery paths (<c>RecoverItem</c>, <c>RecoverTrailingFailure</c>,
    /// <c>RecoverCommittedPendingTailAsync</c>). If an implementation ignores the token, the
    /// executor cannot exit its main loop and the pipeline's completion task will never complete.
    /// Idiomatic handling is to either throw <see cref="OperationCanceledException"/> on
    /// cancellation or return a faulted task.
    /// <para>
    /// A synchronous exception thrown before this method returns its <see cref="ValueTask{TResult}"/>
    /// is supported and has the same failure classification as a fault from that outer task. No
    /// trailing or pipeline task has been established in that case.
    /// </para>
    /// <para>
    /// <paramref name="pipelineTaskRecovery"/> is true only when recovering a
    /// <see cref="PipelineItemFailureKind.PipelineTask"/> failure from the in-flight drain. Calls with
    /// <c>pipelineTaskRecovery: true</c> never overlap one another, but one may overlap an ordinary
    /// <c>pipelineTaskRecovery: false</c> execution. Calls with
    /// <c>pipelineTaskRecovery: false</c> are serialized with one another.
    /// A policy that shares per-dispatch state across calls on the assumption of executor-loop
    /// serialization (e.g. a pooled builder/promise reused because one dispatch completes before the
    /// next starts) MUST NOT reuse that state for pipeline-task recovery - it would collide with the
    /// concurrent executor dispatch on the shared state.
    /// </para>
    /// </remarks>
    ValueTask<PipelineItemResult> ExecuteItemAsync(T item, bool pipelineTaskRecovery, CancellationToken cancellationToken);

    /// Signals the item that it is at the head of the pipeline.
    /// When <paramref name="preferAsync"/> is true, activation should be scheduled or guarded to avoid deep recursion.
    /// <remarks>
    /// Optional per-item: an item that completes outside the head-of-pipeline position (timeout,
    /// recovery, sync-deferred path) will not see a subsequent (stale) call for activation.
    /// When called, it is guaranteed to be at most once per item,
    /// strictly before <see cref="CompleteItem"/> for the same item, and never concurrently with it.
    /// Activation may occur before or after <see cref="ExecuteItemAsync"/> is called. Policies must
    /// not assume an ordering between those two callbacks.
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
    /// A call with <c>preferAsync: true</c> occurs on a completion path and must return promptly;
    /// schedule item-body work rather than running it inline. Slow work here, or in
    /// <see cref="CompleteItem"/>, delays retirement and other completion callbacks.
    /// </para>
    /// <para>
    /// This method must not throw. The pipeline has already published the item as the activated head and
    /// transferred its activation turn before invoking the policy. A synchronous exception cannot
    /// roll that transition back or be treated as an ordinary item failure. Activation should only
    /// perform or schedule a nonthrowing wake. Operational failures belong to the item's task
    /// machinery. A scheduler used here must obey
    /// <see cref="PipelineScheduler.SubmitDetached(Action{object?}, object?, bool)"/>'s nonthrowing
    /// fire-and-forget contract.
    /// </para>
    /// </remarks>
    void ActivateHeadItem(T item, bool preferAsync = true);

    /// Notifies the item that its pipeline position has completed, with an optional error, exactly
    /// once. If recovery replaces an item, the failed item does not receive this callback; its
    /// substitute does.
    /// <remarks>
    /// Retirement is irrevocable before this callback runs. A synchronous exception violates the
    /// policy contract and propagates normally. Recovery handlers do not reinterpret it as an item
    /// failure or invoke this callback again for the same pipeline position.
    /// </remarks>
    void CompleteItem(T item, Exception? exception);

    /// Notifies the policy when retirement leaves no item in flight.
    /// <remarks>
    /// This callback runs immediately after <see cref="CompleteItem"/> for the item whose retirement
    /// reached zero. It is an exact edge notification, not a stable observation: source backlog may
    /// remain, and completion may synchronously publish more work. This method must not throw.
    /// </remarks>
    void OnIdle();

    /// Attempts to recover from a failed item. Returns a substitute that replaces the failed item
    /// in the same pipeline position, or false if recovery is not possible. When true, the pipeline
    /// does not call <see cref="CompleteItem"/> for <paramref name="failedItem"/>; the substitute
    /// assumes that lifecycle position. A consultation for
    /// <see cref="PipelineItemFailureKind.PipelineTask"/> may overlap ordinary item execution.
    bool TryRecoverItemFailure(in PipelineItemFailureContext context, T failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out T? recoveryItem);
}

/// <summary>
/// Returned by <see cref="IPipelinePolicy{T}.ExecuteItemAsync"/> to describe the work an item produces.
/// </summary>
/// <remarks>
/// The two tasks are returned together so the pipeline can commit the item in flight (keyed on
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
/// The two values must not refer to the same single-consumption
/// <see cref="IValueTaskSource"/> operation. Such an operation would be consumed twice (once
/// for the trailing/pipelined inspection on the executor, once on the in-flight completion path),
/// corrupting its token and producing undefined behavior. Task-backed values and synchronously
/// completed values may safely be reused.
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
    /// pending-tail path unconditionally. The framework guarantees that
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
