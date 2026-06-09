namespace Draghi.Pipelining;

/// <summary>
/// Per-enumeration lifecycle holder for source-driven pipelines. Inherits the standard
/// <see cref="IAsyncEnumerator{T}"/> contract and adds <see cref="CompletionToken"/> so the
/// pipeline can read the enumeration's shutdown signal without owning its own CTS.
/// </summary>
/// <remarks>
/// The enumerator is the canonical authority for "is this enumeration shutting down." Pipeline
/// passes the token through to policy methods for cancellation observation, and triggers
/// shutdown by calling <see cref="IAsyncDisposable.DisposeAsync"/> on the enumerator (which the
/// source implementation handles by signaling its internal wake/completion mechanism).
/// </remarks>
public interface IPipelineEnumerator<T> : IAsyncEnumerator<T>
{
    /// <summary>The enumeration's shutdown signal, observable by policy methods.</summary>
    CancellationToken CompletionToken { get; }

    /// <summary>
    /// Signals the enumeration to stop producing items. After Complete, MoveNextAsync drains
    /// any remaining buffered items and then returns false. CompletionToken fires so policy
    /// methods observing it can short-circuit in-flight work.
    /// The enumerator remains usable for drain reads until DisposeAsync. Complete is the
    /// non-terminal "I'm winding down" signal, DisposeAsync is the terminal cleanup.
    /// </summary>
    void Complete();
}
