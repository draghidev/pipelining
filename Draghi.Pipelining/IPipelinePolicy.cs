using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

public interface IPipelinePolicy<T>
{
    /// Runs the item's work and returns its pipeline and trailing execution tasks.
    ValueTask<PipelineItemResult> ExecuteItemAsync(T item, CancellationToken cancellationToken);

    /// Signals the item that it is at the head of the pipeline.
    /// When <paramref name="schedule"/> is true, activation should be scheduled or guarded to avoid deep recursion.
    void ActivateHeadItem(T item, bool schedule = true);

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
