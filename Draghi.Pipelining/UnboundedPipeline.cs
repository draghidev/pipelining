namespace Draghi.Pipelining;

/// <summary>
/// Bundles a <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}"/> with an
/// <see cref="UnboundedQueueSource{T}"/> and exposes <see cref="Enqueue"/> directly.
/// </summary>
/// <remarks>
/// For source-driven scenarios where the producer side isn't a simple SPSC queue (e.g. a source
/// with its own idle/wake handoff), use <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}"/>
/// directly with a custom <see cref="IPipelineSource{T,TEnumerator}"/> implementation.
/// </remarks>
public sealed class UnboundedPipeline<T, TPolicy>
    where TPolicy : IPipelinePolicy<T>
{
    /// <summary>The underlying source-driven pipeline.</summary>
    public Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator> Pipeline { get; }

    /// <summary>The bundled queue source. Exposed for advanced scenarios. Most callers use
    /// <see cref="Enqueue"/> on the wrapper directly.</summary>
    public UnboundedQueueSource<T> Source { get; }

    internal UnboundedPipeline(
        Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator> pipeline,
        UnboundedQueueSource<T> source)
    {
        Pipeline = pipeline;
        Source = source;
    }

    /// <summary>The source-level cancellation token (set when constructing the underlying
    /// <see cref="UnboundedQueueSource{T}"/>). Stable for this instance's lifetime and cancelled
    /// only by the externally supplied token; <see cref="CompleteAsync"/> does not cancel it.</summary>
    public CancellationToken SourceCancellationToken => Source.CancellationToken;

    /// <summary>Current in-flight count (dispatched - completed).</summary>
    public int Depth => Pipeline.Depth;

    /// <summary>Items enqueued but not yet dispatched (the source queue length). A gauge:
    /// <c>Depth + Backlog</c> is the total outstanding. Lock-free read, may be stale.</summary>
    public int Backlog => Source.Backlog;

    /// <summary>Enqueues an item for processing. See
    /// <see cref="UnboundedQueueSource{T}.Enqueue"/> for the deferred-execute semantics.</summary>
    public UnboundedQueueSource<T>.EnqueueResult Enqueue(T item) => Source.Enqueue(item);

    /// <summary>Completion of the current run. See
    /// <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}.Completion"/>.</summary>
    public Task Completion => Pipeline.Completion;

    /// <summary>Initiates pipeline shutdown. See <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}.CompleteAsync"/>.</summary>
    public ValueTask CompleteAsync(Exception? exception = null) => Pipeline.CompleteAsync(exception);

    // Depth and backlog can reach zero independently. Revalidate both after every depth-zero
    // notification and re-arm when backlog remains; a later retirement or executor park resolves it.
    internal async ValueTask WaitForEmptyAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var wait = Pipeline.WaitForEmptyAsync(Source.Backlog, cancellationToken);
            // The last backlogged item can dispatch and retire while the wait is being armed.
            // Recheck with a fresh snapshot so that zero transition cannot be missed.
            if (!wait.IsCompleted)
                Pipeline.RecheckEmpty(Source.Backlog);
            await wait.ConfigureAwait(false);
            if (Pipeline.IsEmpty(Source.Backlog))
                return;
        }
    }

    /// <summary>Returns an enumerator over items currently in the pipeline.</summary>
    public Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator>.Enumerator GetEnumerator()
        => Pipeline.GetEnumerator();
}
