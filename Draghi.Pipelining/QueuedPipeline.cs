namespace Draghi.Pipelining;

/// <summary>
/// Convenience wrapper bundling a <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}"/> with an
/// <see cref="UnboundedQueueSource{T}"/>. Mirrors the queue-flavored API that the pre-source-pivot
/// <c>Pipeline&lt;T,TPolicy&gt;</c> exposed: callers get <see cref="Enqueue"/> directly on the
/// wrapper without having to thread the source through their own code.
/// </summary>
/// <remarks>
/// For source-driven scenarios where the producer side isn't a simple SPSC queue (e.g. a source
/// with its own idle/wake handoff), use <see cref="Pipeline{T,TPolicy,TSource,TEnumerator}"/>
/// directly with a custom <see cref="IPipelineSource{T,TEnumerator}"/> implementation.
/// </remarks>
public sealed class QueuedPipeline<T, TPolicy>
    where TPolicy : IPipelinePolicy<T>
{
    /// <summary>The underlying source-driven pipeline.</summary>
    public Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator> Pipeline { get; }

    /// <summary>The bundled queue source. Exposed for advanced scenarios. Most callers use
    /// <see cref="Enqueue"/> on the wrapper directly.</summary>
    public UnboundedQueueSource<T> Source { get; }

    internal QueuedPipeline(
        Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator> pipeline,
        UnboundedQueueSource<T> source)
    {
        Pipeline = pipeline;
        Source = source;
    }

    /// <summary>The source-level cancellation token (set when constructing the underlying
    /// <see cref="UnboundedQueueSource{T}"/>). Stable for the QueuedPipeline's lifetime. Fires only
    /// when the externally-provided CT is cancelled. <see cref="CompleteAsync"/> does NOT fire it
    /// (CompleteAsync's returned task is the proxy for "pipeline ran to completion").</summary>
    public CancellationToken CompletionToken => Source.CancellationToken;

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

    // Awaits both halves of empty: in-flight (Depth) and backlog (source queue). The completer's
    // Depth==0 fire is backlog-blind (DepthState owns only the in-flight half), so with in-flight
    // Depth a transient zero between serial dispatches can fire while items still sit in the
    // source queue. Re-validate both halves after each fire and re-arm on a premature one: a
    // later completer's zero-crossing (each drained backlog item produces one) or the executor's
    // park-seam fire resolves the re-armed wait, and the final drain reads both halves zero.
    internal async ValueTask WaitForEmptyAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var wait = Pipeline.WaitForEmptyAsync(Source.Backlog, cancellationToken);
            // Stale-arm nudge: the arm's self-fire re-check inside GetIdleTask uses the backlog
            // snapshot from BEFORE its arm CAS; the last backlogged item can dispatch + complete
            // entirely inside that window (its zero-crossing precedes the arm), leaving the armed
            // wait with no future fire. Re-check with a FRESH snapshot after the arm.
            if (!wait.IsCompleted)
                Pipeline.RecheckEmpty(Source.Backlog);
            await wait.ConfigureAwait(false);
            // Order is the proof: an in-flight item is always in one of {Backlog, Depth, pulling}.
            // A false IsPulling read either precedes the executor's pull (then the Backlog read
            // above still saw the item) or follows its IncrementDepth (then the Depth RE-READ
            // below sees the item, or its genuine completion). No interleaving straddles all four.
            if (Source.Backlog is 0 && Pipeline.Depth is 0 && !Pipeline.IsPulling && Pipeline.Depth is 0)
                return;
        }
    }

    /// <summary>Returns an enumerator over items currently in the pipeline.</summary>
    public Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator>.Enumerator GetEnumerator()
        => Pipeline.GetEnumerator();
}
