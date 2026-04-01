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

public readonly struct PipelineItemFailureContext(PipelineItemFailureKind kind, Exception exception, Task? pipelineTask = null)
{
    public PipelineItemFailureKind Kind { get; } = kind;
    public Exception Exception { get; } = exception;
    /// The item's pipeline task, if still live at the time of failure. Converted to Task so it can
    /// be safely observed by both the policy and the pipeline.
    /// Meaningful when <see cref="Kind"/> is <see cref="PipelineItemFailureKind.TrailingExecutionTask"/>.
    public Task? PipelineTask { get; } = pipelineTask;
}
