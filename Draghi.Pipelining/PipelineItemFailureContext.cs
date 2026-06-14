namespace Draghi.Pipelining;

public enum PipelineItemFailureKind
{
    /// <summary>The execute item task or operation completed with an error.</summary>
    ExecuteItemTask,
    /// <summary>The pipeline task completed synchronously with an error during execution.</summary>
    PipelineTask,
    /// <summary>The pipeline task completed with an error asynchronously while the item was a waiter.</summary>
    PipelineTaskWaiter,
    /// <summary>The trailing execution task completed with an error.</summary>
    TrailingExecutionTask,
}

public readonly struct PipelineItemFailureContext(PipelineItemFailureKind kind, Exception exception, ValueTask outstandingPhaseTask = default)
{
    public PipelineItemFailureKind Kind { get; } = kind;
    public Exception Exception { get; } = exception;
    /// The failed item's OTHER-phase task still owed to the framework. The pipeline chooses
    /// the underlying source per-construction-site: a raw ValueTask passed through preserves
    /// single-consume semantics (the recovery is the only awaiter), an AsTask-backed
    /// ValueTask permits idempotent multi-await for callers that want to inspect/observe
    /// non-destructively. The <see cref="Kind"/> differentiates whether the task is
    /// meaningful (so a non-nullable ValueTask with <c>default</c> as sentinel suffices —
    /// awaiting <c>default(ValueTask)</c> is a synchronously-completed no-op).
    ///
    /// Per-kind semantics: for <see cref="PipelineItemFailureKind.TrailingExecutionTask"/>
    /// it carries the still-pending PIPELINE task (the framework hasn't observed it yet,
    /// but the policy may need to wire its outcome into a substitute); for
    /// <see cref="PipelineItemFailureKind.PipelineTask"/> it carries the still-running
    /// TRAILING task (the failed item's trailing may still be in-flight when its pipeline
    /// task sync-faulted, and the substitute must sequence against it to avoid colliding
    /// on the shared output). Default for <see cref="PipelineItemFailureKind.ExecuteItemTask"/>
    /// (no PipelineItemResult returned) and <see cref="PipelineItemFailureKind.PipelineTaskWaiter"/>
    /// (failed item's trailing was awaited at commit time long before the drain-side fault).
    public ValueTask OutstandingPhaseTask { get; } = outstandingPhaseTask;
}
