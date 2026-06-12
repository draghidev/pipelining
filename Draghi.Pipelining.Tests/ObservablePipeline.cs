namespace Draghi.Pipelining.Tests;

// Test-only convenience wrapper bundling a Pipeline over a TestObservableQueueSource<T>, mirroring
// QueuedPipeline<T,TPolicy>'s surface (Enqueue / Depth / CompleteAsync / WaitForEmptyAsync /
// GetEnumerator) so the wait-path tests can build pipelines with the re-homed idle/yield hooks without
// threading the source through their own code.
#pragma warning disable DRAGHI001
static class ObservablePipeline
{
    public static ObservablePipeline<T, TPolicy> Create<T, TPolicy>(
        TPolicy policy,
        bool runContinuationsAsynchronously = true,
        PipelineScheduler? scheduler = null,
        Func<CancellationToken, ValueTask>? onIdle = null,
        CancellationToken cancellationToken = default)
        where TPolicy : IPipelinePolicy<T>
    {
        var source = TestObservableQueueSource<T>.Create(runContinuationsAsynchronously, scheduler, onIdle, cancellationToken);
        var pipeline = Pipeline.Create<T, TPolicy, TestObservableQueueSource<T>, TestObservableQueueSource<T>.Enumerator>(policy, source);
        return new ObservablePipeline<T, TPolicy>(pipeline, source);
    }
}

sealed class ObservablePipeline<T, TPolicy>
    where TPolicy : IPipelinePolicy<T>
{
    public Pipeline<T, TPolicy, TestObservableQueueSource<T>, TestObservableQueueSource<T>.Enumerator> Pipeline { get; }
    public TestObservableQueueSource<T> Source { get; }

    internal ObservablePipeline(
        Pipeline<T, TPolicy, TestObservableQueueSource<T>, TestObservableQueueSource<T>.Enumerator> pipeline,
        TestObservableQueueSource<T> source)
    {
        Pipeline = pipeline;
        Source = source;
    }

    public CancellationToken CompletionToken => Source.CancellationToken;

    public int Depth => Pipeline.Depth;

    public TestObservableQueueSource<T>.EnqueueResult Enqueue(T item) => Source.Enqueue(item);

    public ValueTask CompleteAsync(Exception? exception = null) => Pipeline.CompleteAsync(exception);

    public ValueTask WaitForEmptyAsync(CancellationToken cancellationToken = default)
        => Pipeline.WaitForEmptyAsync(cancellationToken);

    public Pipeline<T, TPolicy, TestObservableQueueSource<T>, TestObservableQueueSource<T>.Enumerator>.Enumerator GetEnumerator()
        => Pipeline.GetEnumerator();
}
#pragma warning restore DRAGHI001
