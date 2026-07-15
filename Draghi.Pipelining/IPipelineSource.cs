namespace Draghi.Pipelining;

/// <summary>
/// Native source contract for source-driven pipelines. The pipeline drives the concrete struct
/// enumerator returned by <see cref="GetAsyncEnumerator"/> through its synchronous-pull and
/// asynchronous-wait seam. The source owns item storage, ordering, and idle/wake semantics.
/// </summary>
/// <remarks>
/// <para>
/// The enumerator is exposed as a concrete struct type via the <typeparamref name="TEnumerator"/>
/// generic parameter rather than an interface return. The JIT therefore sees the concrete type
/// and specializes <see cref="IPipelineEnumerator{T}.TryGetNext"/> and
/// <see cref="IPipelineEnumerator{T}.WaitForNextAsync"/> without boxing or interface dispatch.
/// </para>
/// <para>
/// Implementing this interface directly gives the fully-specialized hot path.
/// <see cref="PipelineSourceAsyncEnumerable{T,TSource,TEnumerator}"/> exposes a source to ordinary
/// <c>await foreach</c> consumers when interoperability is needed.
/// </para>
/// <para>
/// The optional token belongs to the caller constructing the enumeration. Pipeline shutdown is
/// initiated through <see cref="IPipelineEnumerator{T}.Complete"/>; the enumerator exposes the
/// resulting signal to policies through <see cref="IPipelineEnumerator{T}.CompletionToken"/>.
/// </para>
/// </remarks>
public interface IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    /// <summary>
    /// Returns the concrete struct enumerator driven by the pipeline. Called once per run when the
    /// executor loop starts.
    /// </summary>
    /// <param name="cancellationToken">Optional caller-provided cancellation for this enumeration.</param>
    /// <remarks>
    /// Depth is counted by the pipeline at DISPATCH (the executor's single-consumer pull), not by the
    /// source at enqueue, so there is no depth-increment hook to invoke. The source only owns storage,
    /// ordering, and idle/wake.
    /// </remarks>
    TEnumerator GetAsyncEnumerator(CancellationToken cancellationToken = default);
}
