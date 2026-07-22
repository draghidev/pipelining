using System.Diagnostics.CodeAnalysis;
using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

// Configurable throwing source for executor source/teardown-fault tests. Pre-loaded items only
// (no live enqueue, no wake signal), then a chosen failure at a chosen seam, so the executor's
// fault + finally-teardown paths can be driven deterministically:
//   - waitThrow: WaitForNextAsync throws synchronously once items have drained.
//   - gateWait : the empty wait parks on a gate the test faults (TriggerWaitThrow), so a fault can
//                land while a pre-loaded item is still in flight.
//   - disposeThrow: DisposeAsync throws (the teardown-masking case).
sealed class ThrowingSourceState<T>
{
    public readonly Queue<T> Items;
    public readonly Exception? WaitThrow;
    public readonly Exception? DisposeThrow;
    public readonly TaskCompletionSource<bool>? WaitGate;
    public readonly CancellationTokenSource Cts = new();

    public ThrowingSourceState(IEnumerable<T> items, Exception? waitThrow, Exception? disposeThrow, bool gateWait)
    {
        Items = new Queue<T>(items);
        WaitThrow = waitThrow;
        DisposeThrow = disposeThrow;
        WaitGate = gateWait ? new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously) : null;
    }
}

readonly struct ThrowingSource<T> : IPipelineSource<T, ThrowingSource<T>.Enumerator>
{
    readonly ThrowingSourceState<T> _state;
    ThrowingSource(ThrowingSourceState<T> state) => _state = state;

    public static ThrowingSource<T> Create(
        IEnumerable<T>? items = null, Exception? waitThrow = null, Exception? disposeThrow = null, bool gateWait = false)
        => new(new ThrowingSourceState<T>(items ?? Array.Empty<T>(), waitThrow, disposeThrow, gateWait));

    // Faults the gated empty-wait, landing a source fault while pre-loaded items are still in flight.
    public void TriggerWaitThrow(Exception ex) => _state.WaitGate!.SetException(ex);

    public Enumerator CreateEnumerator(CancellationToken cancellationToken = default)
    {
        // Depth is counted by the pipeline at dispatch (each TryGetNext success), so the source no
        // longer pre-increments per pre-loaded item.
        return new Enumerator(_state);
    }

    public struct Enumerator : IPipelineEnumerator<T>
    {
        readonly ThrowingSourceState<T> _state;
        internal Enumerator(ThrowingSourceState<T> state) => _state = state;

        public CancellationToken CompletionToken => _state.Cts.Token;
        public void Complete() => _state.Cts.Cancel();

        public bool TryGetNext([MaybeNullWhen(false)] out T item)
        {
            if (_state.Items.Count > 0)
            {
                item = _state.Items.Dequeue();
                return true;
            }
            item = default!;
            return false;
        }

        public WaitForNextAwaitable WaitForNextAsync()
        {
            if (_state.Items.Count > 0)
                return WaitForNextAwaitable.Retry;
            if (_state.WaitThrow is { } ex)
                throw ex;
            if (_state.WaitGate is { } gate)
                return WaitForNextAwaitable.FromTask(new ValueTask<bool>(gate.Task));
            return WaitForNextAwaitable.Completed;
        }

        public ValueTask DisposeAsync()
        {
            _state.Cts.Cancel();
            if (_state.DisposeThrow is { } ex)
                throw ex;
            return default;
        }
    }
}
