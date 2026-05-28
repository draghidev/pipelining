using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

/// <remarks>
/// Exception robustness: policy callbacks other than <see cref="ExecuteItemAsync"/> (i.e.,
/// <see cref="ActivateHeadItem"/>, <see cref="CompleteItem"/>, <see cref="TryRecoverItemFailure"/>,
/// <see cref="OnExecutionIdleAsync"/>, <see cref="YieldAfterFirstItem"/>) MUST NOT throw. The
/// framework does not wrap them in try/catch and an escaping exception will propagate into
/// whichever context invoked it (executor loop, advancer continuation, drain path), leaving the
/// pipeline in an undefined state. Same contract as <c>IThreadPoolWorkItem.Execute</c> or async
/// continuations: the runtime expects callback authors to handle their own exception domains.
/// Exceptions from <see cref="ExecuteItemAsync"/> and from the returned pipeline/trailing tasks
/// are first-class and flow through <see cref="TryRecoverItemFailure"/>; that is the supported
/// way to surface failure from an item.
/// </remarks>
public interface IPipelinePolicy<T>
{
    /// Runs the item's work and returns its pipeline and trailing execution tasks.
    /// <remarks>
    /// Cancellation contract: implementations MUST observe <paramref name="cancellationToken"/>.
    /// The token is the pipeline's shutdown signal (cancelled by <see cref="Pipeline{T,TPolicy}.CompleteAsync"/>).
    /// The executor awaits this call inline during the recovery flows
    /// (<c>RecoverItem</c>, <c>RecoverTrailingFailure</c>, <c>RecoverCommittedTailWaiterAsync</c>);
    /// if an implementation ignores the token, the executor cannot exit its main loop and
    /// <c>CompleteAsync</c>'s returned task will never complete. Idiomatic handling is to either
    /// throw <see cref="OperationCanceledException"/> on cancellation or return a faulted task.
    /// </remarks>
    ValueTask<PipelineItemResult> ExecuteItemAsync(T item, CancellationToken cancellationToken);

    /// Signals the item that it is at the head of the pipeline.
    /// When <paramref name="preferAsync"/> is true, activation should be scheduled or guarded to avoid deep recursion.
    /// <remarks>
    /// Optional per-item: an item that completes outside the head-of-pipeline flow (timeout,
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
    /// </remarks>
    void ActivateHeadItem(T item, bool preferAsync = true);

    /// Notifies the item of completion with an optional error. <paramref name="remainingDepth"/> is the pipeline depth after this item.
    void CompleteItem(T item, int remainingDepth, Exception? exception);

    /// Attempts to recover from a failed item. Returns a recovery item that supplants the failed item in the pipeline,
    /// or null if recovery is not possible. When non-null the pipeline will not complete the failed item.
    /// NOTE: struct policies should override this to avoid DIM boxing.
    bool TryRecoverItemFailure(PipelineItemFailureContext context, T failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out T? recoveryItem)
    {
        recoveryItem = default;
        return false;
    }

    /// The scheduler used for the execution loop.
    /// When null, falls back to ThreadPool.
    PipelineScheduler? ExecutionScheduler => null;

    /// When true, Enqueue dispatches to the scheduler. When false, runs the executor inline on the caller's thread.
    bool RunEnqueueAsynchronously => true;

    /// Called when the executor is idle, before waiting for more work.
    /// Waiters may still be active, use <see cref="Pipeline{T,TPolicy}.WaitForIdleAsync"/> for full pipeline idle.
    /// The executor awaits the returned task before parking. <paramref name="cancellationToken"/>
    /// is the pipeline's shutdown signal (same as <see cref="ExecuteItemAsync"/>); implementations
    /// must observe it so the executor can exit when <see cref="Pipeline{T,TPolicy}.CompleteAsync"/>
    /// is called.
    ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => default;

    /// Called after the executor processes the first item of a resumed batch, when there are more items.
    /// The policy can yield to the scheduler (e.g. to free a caller's thread) or return default to continue inline.
    /// Default: when RunEnqueueAsynchronously is false, yield to the current scheduler.
    /// NOTE: struct policies should override this to avoid DIM boxing.
    ValueTask YieldAfterFirstItem() => RunEnqueueAsynchronously ? default : DefaultYieldAsync();

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private static async ValueTask DefaultYieldAsync()
        => await PipelineScheduler.ThreadPool.ContinueOnAsync(forceYielding: true);
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
/// </remarks>
public readonly struct PipelineItemResult(ValueTask trailingExecutionTask, ValueTask pipelineTask)
{
    /// <summary>Remaining execution work that must complete before the next item can execute.</summary>
    public ValueTask TrailingExecutionTask { get; } = trailingExecutionTask;
    /// <summary>The item's pipelined-phase task. Completion signals the item is done in the pipeline.</summary>
    public ValueTask PipelineTask { get; } = pipelineTask;

    public PipelineItemResult(ValueTask pipelineTask)
        : this(default, pipelineTask) { }

    public static implicit operator PipelineItemResult(ValueTask pipelineTask) => new(pipelineTask);
}
