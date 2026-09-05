using Draghi.Pipelining.Internal;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

public sealed partial class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    /// <summary>Conservatively observes the oldest candidates within a captured dispatch range.</summary>
    /// <remarks>
    /// Concurrent mutation may omit or duplicate items. Each candidate consumes one captured
    /// position, and no position beyond the frontier is produced. A concurrently recycled storage
    /// slot may contain a newer identity, which is why position validation at the use boundary is
    /// required. For value types, concurrently cleared queue slots may surface as <c>default(T)</c>,
    /// and non-atomic structs may tear; reference types are the reliable identity-observation form.
    /// A successful <see cref="MoveNext"/> does not pin <see cref="Current"/>. Tenure-sensitive
    /// consumers must validate <see cref="Position"/> with <see cref="EnumerationFrontier.IsRetired"/>
    /// under their own retirement/reuse synchronization before dereferencing the candidate.
    /// </remarks>
    [Experimental("DRAGHI001")]
    public struct FrontierEnumerator
    {
        enum EnumerationPhase : byte
        {
            RecoveryItem,
            Slot,
            Queue,
            PendingTail,
            ExecutingItem,
            Completed
        }

        readonly EnumerationFrontier _frontier;
        SingleProducerSingleConsumerQueue<(T Item, ValueTask PipelineTask)>.FrontierEnumerator _queue;
        T _recovery;
        T _slot;
        T _pendingTail;
        T _executing;
        bool _hasRecovery;
        bool _hasSlot;
        bool _hasPendingTail;
        bool _hasExecuting;
        uint _position;
        int _remaining;
        EnumerationPhase _phase;

        internal FrontierEnumerator(
            Pipeline<T, TPolicy, TSource, TEnumerator> pipeline,
            EnumerationFrontier frontier)
        {
            _frontier = frontier;
            _hasRecovery = Volatile.Read(ref pipeline._inFlightRecoveryVisible);
            _recovery = _hasRecovery ? pipeline._inFlightRecoveryItem : default!;

            pipeline._inFlight.SnapshotForEnumeration(
                out _slot, out _hasSlot, out var queueSnapshot);
            _queue = queueSnapshot is null ? default : queueSnapshot.GetFrontierEnumerator();

            _hasPendingTail = Volatile.Read(ref pipeline._hasPendingTail);
            _pendingTail = _hasPendingTail ? pipeline._pendingTail : default!;

            _hasExecuting = Volatile.Read(ref pipeline._executingItemVisible);
            _executing = _hasExecuting
                ? ReadSlot(ref pipeline._executingItem, ref pipeline._executingItemGeneration)
                : default!;

            _position = frontier.RetiredAtCapture;
            var capturedPositions = frontier.DispatchedThrough - frontier.RetiredAtCapture;
            var candidateLimit = (int)capturedPositions;
            var candidateCount = (_hasRecovery ? 1 : 0) + (_hasSlot ? 1 : 0);
            var countEnumerator = _queue;
            while (candidateCount < candidateLimit && countEnumerator.MoveNext(out _))
                candidateCount++;
            if (candidateCount < candidateLimit && _hasPendingTail)
                candidateCount++;
            if (candidateCount < candidateLimit && _hasExecuting)
                candidateCount++;

            // A post-frontier candidate can enter only after publishing its dispatch count. Reading
            // the count after every candidate snapshot therefore bounds how many newest candidates
            // must be excluded. Missing/raced captured positions only shorten the observation.
            var postFrontierDispatches = pipeline._depthState.DispatchedThrough
                - frontier.DispatchedThrough;
            _remaining = postFrontierDispatches > int.MaxValue || capturedPositions == 0
                ? 0
                : Math.Max(
                    0,
                    Math.Min(candidateLimit, candidateCount) - (int)postFrontierDispatches);
        }

        public T Current { get; private set; } = default!;
        /// <summary>The captured position associated with <see cref="Current"/> after a successful
        /// <see cref="MoveNext"/>.</summary>
        public uint Position => _position;
        /// <summary>The dispatch and retirement range governing this observation.</summary>
        public EnumerationFrontier Frontier => _frontier;

        /// <summary>Advances to the next conservative candidate in the captured range.</summary>
        public bool MoveNext()
        {
            if (!_frontier.IsCurrentRun)
            {
                _remaining = 0;
                Current = default!;
                return false;
            }
            while (_remaining != 0)
            {
                ChargeConcurrentRetirements();
                if (_remaining == 0)
                    break;
                if (!TryReadCandidate(out var item))
                    break;
                if (Consume(item))
                    return true;
            }

            _remaining = 0;
            Current = default!;
            return false;
        }

        void ChargeConcurrentRetirements()
        {
            var retiredDistance = unchecked((int)(_frontier.RetiredThrough - _position));
            if (retiredDistance <= 0)
                return;
            var retired = Math.Min(retiredDistance, _remaining);
            DiscardCandidates(retired);
            _position += (uint)retired;
            _remaining -= retired;
        }

        void DiscardCandidates(int count)
        {
            while (count-- != 0 && TryReadCandidate(out _)) { }
        }

        bool TryReadCandidate(out T item)
        {
            while (true)
            {
                switch (_phase)
                {
                    case EnumerationPhase.RecoveryItem:
                        _phase = EnumerationPhase.Slot;
                        if (_hasRecovery)
                        {
                            item = _recovery;
                            return true;
                        }
                        continue;
                    case EnumerationPhase.Slot:
                        _phase = EnumerationPhase.Queue;
                        if (_hasSlot)
                        {
                            item = _slot;
                            return true;
                        }
                        continue;
                    case EnumerationPhase.Queue:
                        if (_queue.MoveNext(out var entry))
                        {
                            item = entry.Item;
                            return true;
                        }
                        _phase = EnumerationPhase.PendingTail;
                        continue;
                    case EnumerationPhase.PendingTail:
                        _phase = EnumerationPhase.ExecutingItem;
                        if (_hasPendingTail)
                        {
                            item = _pendingTail;
                            return true;
                        }
                        continue;
                    case EnumerationPhase.ExecutingItem:
                        _phase = EnumerationPhase.Completed;
                        if (_hasExecuting)
                        {
                            item = _executing;
                            return true;
                        }
                        continue;
                    default:
                        item = default!;
                        return false;
                }
            }
        }

        bool Consume(T item)
        {
            _position++;
            _remaining--;
            if (item is null && RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                return false;
            Current = item;
            return true;
        }
    }

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
