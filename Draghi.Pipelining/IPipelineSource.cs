namespace Draghi.Pipelining;

/// <summary>
/// Native source contract for source-driven pipelines. The pipeline consumes items by walking
/// <see cref="GetAsyncEnumerator"/>'s returned struct enumerator via <c>await foreach</c>. The
/// source owns item storage, ordering, and idle/wake semantics. The pipeline just consumes.
/// </summary>
/// <remarks>
/// <para>
/// The enumerator is exposed as a concrete struct type via the <typeparamref name="TEnumerator"/>
/// generic parameter rather than the <see cref="IAsyncEnumerator{T}"/> interface. This is the
/// non-negotiable difference from <see cref="IAsyncEnumerable{T}"/>: calling
/// <c>GetAsyncEnumerator</c> through the BCL interface boxes a struct enumerator on every call,
/// because the return type is the interface. Here the JIT sees the concrete struct and
/// specializes <c>MoveNextAsync</c> and <c>Current</c> calls per-source without boxing or
/// interface dispatch.
/// </para>
/// <para>
/// Implementing this interface directly gives you the fully-specialized hot path. Adapter
/// sources (for <see cref="IAsyncEnumerable{T}"/>, <c>ChannelReader{T}</c>,
/// <see cref="IEnumerable{T}"/>) wrap legacy enumerables at the adapter boundary. The boxing
/// cost lands once at the adapter, not per-MoveNext.
/// </para>
/// <para>
/// Cancellation: the token passed to <see cref="GetAsyncEnumerator"/> is the pipeline's shutdown
/// signal. Sources should observe it so the enumerator's <c>MoveNextAsync</c> completes
/// (typically by returning false or throwing <see cref="OperationCanceledException"/>) when the
/// pipeline shuts down.
/// </para>
/// </remarks>
public interface IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    /// <summary>
    /// Returns a concrete struct enumerator the pipeline drives via <c>await foreach</c>. Called
    /// once per pipeline lifetime when the executor loop starts.
    /// </summary>
    /// <param name="cancellationToken">The pipeline's shutdown signal.</param>
    /// <remarks>
    /// Depth is counted by the pipeline at DISPATCH (the executor's single-consumer pull), not by the
    /// source at enqueue, so there is no depth-increment hook to invoke. The source only owns storage,
    /// ordering, and idle/wake.
    /// </remarks>
    TEnumerator GetAsyncEnumerator(CancellationToken cancellationToken = default);
}
