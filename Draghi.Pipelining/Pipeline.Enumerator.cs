using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

public sealed partial class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    public struct Enumerator
    {
        enum EnumerationPhase : byte
        {
            ExecutingItem,
            RecoveryItem,
            SnapshotStore,
            InitializeQueue,
            EnumerateQueue,
            PendingTail,
            Completed
        }

        readonly Pipeline<T, TPolicy, TSource, TEnumerator> _pipeline;
        SingleProducerSingleConsumerQueue<(T Item, ValueTask PipelineTask)>.Enumerator _inFlightEnumerator;
        // Read before the slot check, not at phase 2's own turn - see the read-order comment at its
        // use site (matches TryClaimCompletedHead/TryPeekHead's fix: read the queue reference before
        // the slot state, since an escalating commit writes them in that order and this enumerator
        // has no other synchronization pairing it with this store beyond these two reads).
        SingleProducerSingleConsumerQueue<(T Item, ValueTask PipelineTask)>? _queueSnapshot;
        EnumerationPhase _phase;

        internal Enumerator(Pipeline<T, TPolicy, TSource, TEnumerator> pipeline)
        {
            _pipeline = pipeline;
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            switch (_phase)
            {
                case EnumerationPhase.ExecutingItem:
                    _phase = EnumerationPhase.RecoveryItem;
                    // Visibility-only window: the in-flight item is held on _executingItem before
                    // being committed elsewhere. Without yielding it here, heartbeat-style
                    // consumers can't see the item during dispatch (waiting-body abort propagation
                    // needs this). Volatile.Read pairs with the executor's Volatile.Write on
                    // _executingItemVisible.
                    if (Volatile.Read(ref _pipeline._executingItemVisible) && _pipeline._executingItem is { } executing)
                    {
                        Current = executing;
                        return true;
                    }
                    goto case EnumerationPhase.RecoveryItem;
                case EnumerationPhase.RecoveryItem:
                    _phase = EnumerationPhase.SnapshotStore;
                    // The recovery owns the oldest live position but resides outside the in-flight
                    // store. The visibility flag publishes its identity to heartbeat-style users.
                    if (Volatile.Read(ref _pipeline._inFlightRecoveryVisible) && _pipeline._inFlightRecoveryItem is { } recovery)
                    {
                        Current = recovery;
                        return true;
                    }
                    goto case EnumerationPhase.SnapshotStore;
                case EnumerationPhase.SnapshotStore:
                    // SnapshotForEnumeration owns the safe read order internally (queue reference
                    // before slot state) - this call site just presents slot before queue in
                    // ENUMERATION order, which is a separate concern (leave-head FIFO presentation).
                    _pipeline._inFlight.SnapshotForEnumeration(out var slotItem, out var hasSlotItem, out _queueSnapshot);
                    _phase = EnumerationPhase.InitializeQueue;
                    if (hasSlotItem && slotItem is { } slot)
                    {
                        Current = slot;
                        return true;
                    }
                    goto case EnumerationPhase.InitializeQueue;
                case EnumerationPhase.InitializeQueue:
                    // Null queue means the pipeline never escalated. Skip to the tail phase.
                    var queue = _queueSnapshot;
                    if (queue is null)
                    {
                        _phase = EnumerationPhase.PendingTail;
                        goto case EnumerationPhase.PendingTail;
                    }
                    _inFlightEnumerator = new(queue);
                    _phase = EnumerationPhase.EnumerateQueue;
                    goto case EnumerationPhase.EnumerateQueue;
                case EnumerationPhase.EnumerateQueue:
                    while (_inFlightEnumerator.MoveNext())
                    {
                        if (_inFlightEnumerator.Current.Item is { } item)
                        {
                            Current = item;
                            return true;
                        }
                    }
                    _phase = EnumerationPhase.PendingTail;
                    goto case EnumerationPhase.PendingTail;
                case EnumerationPhase.PendingTail:
                    _phase = EnumerationPhase.Completed;
                    // Volatile.Read pairs with the executor's Volatile.Write on _hasPendingTail:
                    // if observed true, the prior _pendingTail / _pendingTailPipelineTask writes are visible.
                    // Consistent for any T that fits in a native word (refs, primitives, small
                    // structs). Larger structs can tear their own write regardless of fences.
                    if (Volatile.Read(ref _pipeline._hasPendingTail) && _pipeline._pendingTail is { } tail)
                    {
                        Current = tail;
                        return true;
                    }
                    return false;
                default:
                    return false;
            }
        }
    }

}
