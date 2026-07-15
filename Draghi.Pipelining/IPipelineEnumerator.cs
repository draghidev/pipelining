using System.Diagnostics.CodeAnalysis;

namespace Draghi.Pipelining;

/// <summary>
/// Per-enumeration lifecycle holder for source-driven pipelines. The pipeline consumes items
/// through the <see cref="TryGetNext"/> / <see cref="WaitForNextAsync"/> pull seam and disposes via
/// <see cref="IAsyncDisposable"/>; it never routes through a <c>Current</c> / <c>MoveNextAsync</c>
/// surface. await-foreach compat lives in <see cref="PipelineSourceAsyncEnumerable{T, TSource, TEnumerator}"/>,
/// not on every implementation.
/// </summary>
/// <remarks>
/// The enumerator is the canonical authority for "is this enumeration shutting down." Pipeline
/// passes <see cref="CompletionToken"/> through to policy methods for cancellation observation, and
/// triggers shutdown by calling <see cref="IAsyncDisposable.DisposeAsync"/> on the enumerator (which
/// the source implementation handles by signaling its internal wake/completion mechanism).
/// </remarks>
public interface IPipelineEnumerator<T> : IAsyncDisposable
{
    /// <summary>The enumeration's shutdown signal, observable by policy methods.</summary>
    CancellationToken CompletionToken { get; }

    /// <summary>
    /// Signals the enumeration to stop accepting or producing new items. After Complete, the source resolves its
    /// residual (items enqueued but not yet dispatched) and then resolves completed. The
    /// disposition is the source's choice: drain the residual through the executor, or reclaim it
    /// in bulk for the producer to dispose or migrate. CompletionToken fires so policy methods
    /// observing it can short-circuit in-flight work.
    /// The enumerator remains usable for drain reads until DisposeAsync. Complete is the
    /// non-terminal "I'm winding down" signal, DisposeAsync is the terminal cleanup.
    /// </summary>
    void Complete();

    /// <summary>
    /// Synchronous pull: hands the next item out directly through <paramref name="item"/> when one
    /// is available (hence "get", not "move" - there is no Current store on the hot path). Returning
    /// false means "nothing available right now" - empty OR completed; <see cref="WaitForNextAsync"/>
    /// disambiguates.
    /// </summary>
    bool TryGetNext([MaybeNullWhen(false)] out T item);

    /// <summary>
    /// Waits until the pull should be retried. Awaiting the result yields true to retry
    /// <see cref="TryGetNext"/> (an item arrived, or may have) and false when the enumeration
    /// completed and drained. Implementations re-check availability under their wake
    /// synchronization before genuinely suspending, so a TryGetNext-miss racing an arrival
    /// resolves to an immediate retry rather than a lost wake.
    /// </summary>
    /// <remarks>
    /// This pair replaces the usual <c>MoveNextAsync</c> operation as the executor's pull
    /// seam: the miss path returns a concrete awaitable (see <see cref="WaitForNextAwaitable"/>)
    /// instead of a <see cref="ValueTask{TResult}"/>, which on the wait-per-item production shape
    /// skips the value-task-source dispatch stack entirely. Await-foreach compatibility is supplied
    /// by <see cref="PipelineSourceAsyncEnumerable{T,TSource,TEnumerator}"/>.
    /// </remarks>
    WaitForNextAwaitable WaitForNextAsync();
}
