namespace Draghi.Pipelining;

/// <summary>
/// <see cref="IAsyncEnumerable{T}"/> adapter over an <see cref="IPipelineSource{T, TEnumerator}"/>,
/// for await-foreach consumers. The pipeline executor drives the
/// <see cref="IPipelineEnumerator{T}.TryGetNext"/> / <see cref="IPipelineEnumerator{T}.WaitForNextAsync"/>
/// pull seam directly and never needs this. It exists so the await-foreach compat loop lives in one
/// place rather than being reimplemented on every source.
/// </summary>
public sealed class PipelineSourceAsyncEnumerable<T, TSource, TEnumerator>(TSource source) : IAsyncEnumerable<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    readonly TSource _source = source;

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new Enumerator(_source.CreateEnumerator(cancellationToken));

    sealed class Enumerator(TEnumerator inner) : IAsyncEnumerator<T>
    {
        TEnumerator _inner = inner;
        T _current = default!;

        public T Current => _current;

        public async ValueTask<bool> MoveNextAsync()
        {
            while (true)
            {
                if (_inner.TryGetNext(out var item))
                {
                    _current = item!;
                    return true;
                }

                if (!await _inner.WaitForNextAsync())
                {
                    _current = default!;
                    return false;
                }
            }
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
