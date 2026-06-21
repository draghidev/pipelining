// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Draghi.Pipelining.Internal;

/// <summary>A placeholder class for common padding constants and eventually routines.</summary>
static class PaddingHelpers
{
    /// <summary>A size greater than or equal to the size of the most common CPU cache lines.</summary>
    internal const int CACHE_LINE_SIZE = 128;
}

/// <summary>Padding structure used to minimize false sharing</summary>
[StructLayout(LayoutKind.Explicit, Size = PaddingHelpers.CACHE_LINE_SIZE - sizeof(int))]
struct PaddingFor32
{
}

/// <summary>
/// Provides a producer/consumer queue safe to be used by only one producer and one consumer concurrently.
/// </summary>
/// <typeparam name="T">Specifies the type of data contained in the queue.</typeparam>
/// <remarks>
/// Exposed under <see cref="Draghi.Pipelining.Internal"/> as a building block for callers
/// composing their own <see cref="IPipelineSource{T,TEnumerator}"/> implementations. The
/// single-producer / single-consumer contract is load-bearing. Using this from multiple producers
/// or consumers concurrently produces undefined results.
/// </remarks>
[Experimental("DRAGHI001")]
[DebuggerDisplay("Count = {Count}")]
[DebuggerTypeProxy(typeof(SingleProducerSingleConsumerQueue<>.SingleProducerSingleConsumerQueue_DebugView))]
public sealed class SingleProducerSingleConsumerQueue<T> : IEnumerable<T>
{
    // Design:
    //
    // SingleProducerSingleConsumerQueue (SPSCQueue) is a concurrent queue designed to be used
    // by one producer thread and one consumer thread. SPSCQueue does not work correctly when used by
    // multiple producer threads concurrently or multiple consumer threads concurrently.
    //
    // SPSCQueue is based on segments that behave like circular buffers. Each circular buffer is represented
    // as an array with two indexes: _first and _last. _first is the index of the array slot for the consumer
    // to read next, and _last is the slot for the producer to write next. The circular buffer is empty when
    // (_first == _last), and full when ((_last+1) % _array.Length == _first).
    //
    // Since _first is only ever modified by the consumer thread and _last by the producer, the two indices can
    // be updated without interlocked operations. As long as the queue size fits inside a single circular buffer,
    // enqueues and dequeues simply advance the corresponding indices around the circular buffer. If an enqueue finds
    // that there is no room in the existing buffer, however, a new circular buffer is allocated that is twice as big
    // as the old buffer. From then on, the producer will insert values into the new buffer. The consumer will first
    // empty out the old buffer and only then follow the producer into the new (larger) buffer.
    //
    // As described above, the enqueue operation on the fast path only modifies the _first field of the current segment.
    // However, it also needs to read _last in order to verify that there is room in the current segment. Similarly, the
    // dequeue operation on the fast path only needs to modify _last, but also needs to read _first to verify that the
    // queue is non-empty. This results in true cache line sharing between the producer and the consumer.
    //
    // The cache line sharing issue can be mitigating by having a possibly stale copy of _first that is owned by the producer,
    // and a possibly stale copy of _last that is owned by the consumer. So, the consumer state is described using
    // (_first, _lastCopy) and the producer state using (_firstCopy, _last). The consumer state is separated from
    // the producer state by padding, which allows fast-path enqueues and dequeues from hitting shared cache lines.
    // _lastCopy is the consumer's copy of _last. Whenever the consumer can tell that there is room in the buffer
    // simply by observing _lastCopy, the consumer thread does not need to read _last and thus encounter a cache miss. Only
    // when the buffer appears to be empty will the consumer refresh _lastCopy from _last. _firstCopy is used by the producer
    // in the same way to avoid reading _first on the hot path.

    /// <summary>The initial size to use for segments (in number of elements).</summary>
    // 8 (was 32): callers escalate to the queue only past a slot fast path and typically overlap a
    // few deep, so a small floor cuts the per-instance footprint (the 32-element array dominated the
    // ~700B-1.2KB minimum alongside the cache-line padding). Segments still double on demand
    // (EnqueueSlow) up to MaxSegmentSize, so deep producers are unaffected.
    const int InitialSegmentSize = 8; // must be a power of 2
    /// <summary>The maximum size to use for segments (in number of elements).</summary>
    const int MaxSegmentSize = 0x1000000; // this could be made as large as int.MaxValue / 2

    /// <summary>The head of the linked list of segments. Consumer-owned writes, observers use Volatile.Read.</summary>
    Segment _head;
    /// <summary>The tail of the linked list of segments. Producer-owned writes, observers use Volatile.Read.</summary>
    Segment _tail;

    /// <summary>Initializes the queue.</summary>
    public SingleProducerSingleConsumerQueue()
    {
        // Validate constants in ctor rather than in an explicit cctor that would cause perf degradation
        Debug.Assert(InitialSegmentSize > 0, "Initial segment size must be > 0.");
        Debug.Assert((InitialSegmentSize & (InitialSegmentSize - 1)) == 0, "Initial segment size must be a power of 2");
        Debug.Assert(InitialSegmentSize <= MaxSegmentSize, "Initial segment size should be <= maximum.");
        Debug.Assert(MaxSegmentSize < int.MaxValue / 2, "Max segment size * 2 must be < int.MaxValue, or else overflow could occur.");

        // Initialize the queue
        _head = _tail = new Segment(InitialSegmentSize);
    }

    /// <summary>Enqueues an item into the queue.</summary>
    /// <param name="item">The item to enqueue.</param>
    public void Enqueue(T item)
    {
        Segment segment = _tail; // producer-owned, plain read
        T[] array = segment._array;
        int last = segment._state._last; // producer-owned, plain read

        // Fast path: there's obviously room in the current segment
        int tail2 = (last + 1) & (array.Length - 1);
        if (tail2 != segment._state._firstCopy)
        {
            array[last] = item;
            // Release: publish data slot write before the _last update so consumer's acquire-load
            // on _last (slow-path refresh of _lastCopy) sees the data.
            Volatile.Write(ref segment._state._last, tail2);
            return;
        }

        // Slow path: there may not be room in the current segment.
        EnqueueSlow(item, ref segment);
    }

    /// <summary>Enqueues an item into the queue.</summary>
    /// <param name="item">The item to enqueue.</param>
    /// <param name="segment">The segment in which to first attempt to store the item.</param>
    void EnqueueSlow(T item, ref Segment segment)
    {
        Debug.Assert(segment != null, "Expected a non-null segment.");

        // Acquire-load on _first to see consumer's latest dequeue progress.
        // Single read (vs BCL's two volatile reads): the window between two reads is sub-nanosecond, so
        // the freshness benefit on assignment is dominated by the cost of a second ldar on ARM64.
        int currentFirst = Volatile.Read(ref segment._state._first);
        if (segment._state._firstCopy != currentFirst)
        {
            segment._state._firstCopy = currentFirst;
            Enqueue(item); // will only recur once for this enqueue operation
            return;
        }

        int newSegmentSize = Math.Min(_tail._array.Length * 2, MaxSegmentSize);
        Debug.Assert(newSegmentSize > 0, "The max size should always be small enough that we don't overflow.");

        var newSegment = new Segment(newSegmentSize);
        newSegment._array[0] = item;
        newSegment._state._last = 1;
        newSegment._state._lastCopy = 1;

        try { }
        finally
        {
            // Finally block to protect against corruption due to a thread abort between
            // setting _next and setting _tail (this is only relevant on .NET Framework).
            Volatile.Write(ref _tail._next, newSegment); // ensure segment not published until item is fully stored
            _tail = newSegment;
        }
    }

    /// <summary>Attempts to dequeue an item from the queue.</summary>
    /// <param name="result">The dequeued item.</param>
    /// <returns>true if an item could be dequeued. Otherwise, false.</returns>
    public bool TryDequeue([MaybeNullWhen(false)] out T result)
    {
        Segment segment = _head; // consumer-owned, plain read
        T[] array = segment._array;
        int first = segment._state._first; // consumer-owned: only this thread writes _first, so a plain read is correct. Data visibility rides the _last acquire/release pair, not _first.

        // Fast path: there's obviously data available in the current segment
        if (first != segment._state._lastCopy)
        {
            result = array[first];
            // Advancing _first publishes the slot back to the producer for reuse, so any store the
            // consumer makes to the slot must be ordered before it: a plain advance can become
            // visible first, and the producer's write into the reused slot then races the pending
            // slot store. Elements without references skip the slot clear entirely (the producer
            // overwrites the slot before publishing _last, and there is nothing for the GC to
            // release), which also removes the store needing ordering: the advance stays a plain
            // store. Elements with references must be cleared to release them, so there the
            // advance is a release store ordering the clear before the handoff.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                array[first] = default!; // Clear the slot to release the element
                Volatile.Write(ref segment._state._first, (first + 1) & (array.Length - 1));
            }
            else
            {
                // Plain store on _first: the producer reads it only to gauge free room, where a
                // stale value is conservative and safe.
                segment._state._first = (first + 1) & (array.Length - 1);
            }
            return true;
        }

        // Slow path: there may not be data available in the current segment
        return TryDequeueSlow(segment, array, peek: false, out result);
    }

    /// <summary>Attempts to peek at an item in the queue.</summary>
    /// <param name="result">The peeked item.</param>
    /// <returns>true if an item could be peeked. Otherwise, false.</returns>
    /// <remarks>
    /// Consumer-only, like every dequeue: the slow path refreshes _lastCopy and hops _head
    /// (consumer-owned writes), and even a read-only peek from another thread can observe an
    /// element torn by the consumer's concurrent slot clear. Single producer, single consumer,
    /// peeks included.
    /// </remarks>
    public bool TryPeek([MaybeNullWhen(false)] out T result)
    {
        Segment segment = _head; // consumer-owned, plain read
        T[] array = segment._array;
        int first = segment._state._first; // consumer-owned plain read (see TryDequeue)

        // Fast path: there's obviously data available in the current segment
        if (first != segment._state._lastCopy)
        {
            result = array[first];
            return true;
        }

        // Slow path: there may not be data available in the current segment
        return TryDequeueSlow(segment, array, peek: true, out result);
    }

    /// <summary>Attempts to dequeue or peek at an item from the queue.</summary>
    /// <param name="segment">The segment from which the item was dequeued.</param>
    /// <param name="array">The array from <paramref name="segment"/>.</param>
    /// <param name="peek">true if this is only a peek operation. False if the item should be dequeued.</param>
    /// <param name="result">The dequeued or peeked item.</param>
    /// <returns>true if an item could be dequeued or peeked. Otherwise, false.</returns>
    [MethodImpl(MethodImplOptions.NoInlining)]
    bool TryDequeueSlow(Segment segment, T[] array, bool peek, [MaybeNullWhen(false)] out T result)
    {
        Debug.Assert(segment != null, "Expected a non-null segment.");
        Debug.Assert(array != null, "Expected a non-null item array.");

        // Acquire-load on _last to see producer's published data writes.
        // Single read (vs BCL's two volatile reads): see EnqueueSlow for the cost reasoning.
        int currentLast = Volatile.Read(ref segment._state._last);
        if (currentLast != segment._state._lastCopy)
        {
            segment._state._lastCopy = currentLast;
            return peek ?
                TryPeek(out result) :
                TryDequeue(out result); // will only recur once for this operation
        }

        // The hop guard must not decide on the pass-captured currentLast: it can be arbitrarily
        // stale by the time _next is observed non-null (one preemption suffices), and hopping on
        // stale-equal advances _head irreversibly past every entry the producer added in between,
        // stranding them forever. The acquire sits on _next, pairing with EnqueueSlow's publish,
        // whose release ordered the segment's final _last store before it. The _last read below
        // is therefore the final value, not merely fresh at its own instant. Once _next is
        // published the producer never writes this segment again, so first == final-last means
        // permanently empty and the hop is safe.
        if (Volatile.Read(ref segment._next) is { } nextSegment && segment._state._first == Volatile.Read(ref segment._state._last)) // consumer-owned plain read of _first
        {
            segment = nextSegment;
            array = segment._array;
            _head = segment; // consumer-owned write
        }

        int first = segment._state._first; // consumer-owned plain read (see TryDequeue)
        int last = Volatile.Read(ref segment._state._last); // acquire on potentially-new segment

        if (first == last)
        {
            result = default;
            return false;
        }

        result = array[first];
        if (!peek)
        {
            // Clear-then-release ordering: see TryDequeue.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                array[first] = default!; // Clear the slot to release the element
                Volatile.Write(ref segment._state._first, (first + 1) & (segment._array.Length - 1));
            }
            else
            {
                segment._state._first = (first + 1) & (segment._array.Length - 1);
            }
            // Freshness-only refresh of _lastCopy to widen the fast-path window.
            segment._state._lastCopy = Volatile.Read(ref segment._state._last);
        }

        return true;
    }

    /// <summary>Attempts to dequeue an item from the queue.</summary>
    /// <param name="predicate">The predicate that must return true for the item to be dequeued.  If null, all items implicitly return true.</param>
    /// <param name="result">The dequeued item.</param>
    /// <returns>true if an item could be dequeued. Otherwise, false.</returns>
    public bool TryDequeueIf(Predicate<T>? predicate, [MaybeNullWhen(false)] out T result)
    {
        Segment segment = _head; // consumer-owned, plain read
        T[] array = segment._array;
        int first = segment._state._first; // consumer-owned plain read (see TryDequeue)

        // Fast path: there's obviously data available in the current segment
        if (first != segment._state._lastCopy)
        {
            result = array[first];
            if (predicate == null || predicate(result))
            {
                // Clear-vs-advance ordering: see TryDequeue.
                if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                {
                    array[first] = default!;
                    Volatile.Write(ref segment._state._first, (first + 1) & (array.Length - 1));
                }
                else
                {
                    segment._state._first = (first + 1) & (array.Length - 1);
                }
                return true;
            }

            result = default;
            return false;
        }

        // Slow path: there may not be data available in the current segment
        return TryDequeueIfSlow(predicate, segment, array, out result);
    }

    /// <summary>Attempts to dequeue an item from the queue.</summary>
    /// <param name="predicate">The predicate that must return true for the item to be dequeued.</param>
    /// <param name="array">The array from which the item was dequeued.</param>
    /// <param name="segment">The segment from which the item was dequeued.</param>
    /// <param name="result">The dequeued item.</param>
    /// <returns>true if an item could be dequeued. Otherwise, false.</returns>
    bool TryDequeueIfSlow(Predicate<T>? predicate, Segment segment, T[] array, [MaybeNullWhen(false)] out T result)
    {
        Debug.Assert(segment != null, "Expected a non-null segment.");
        Debug.Assert(array != null, "Expected a non-null item array.");

        // Acquire-load on _last to see producer's published data writes.
        // Single read (vs BCL's two volatile reads): see EnqueueSlow for the cost reasoning.
        int currentLast = Volatile.Read(ref segment._state._last);
        if (currentLast != segment._state._lastCopy)
        {
            segment._state._lastCopy = currentLast;
            return TryDequeueIf(predicate, out result); // will only recur once for this dequeue operation
        }

        // Acquire on _next plus fresh _last, the same guard as TryDequeueSlow: hopping on the
        // stale currentLast maroons entries added since it was captured, and the _next acquire
        // guarantees the _last read is the segment's final value.
        if (Volatile.Read(ref segment._next) is { } nextSegment && segment._state._first == Volatile.Read(ref segment._state._last)) // consumer-owned plain read of _first
        {
            segment = nextSegment;
            array = segment._array;
            _head = segment; // consumer-owned write
        }

        int first = segment._state._first; // consumer-owned plain read (see TryDequeue)
        int last = Volatile.Read(ref segment._state._last); // acquire on potentially-new segment

        if (first == last)
        {
            result = default;
            return false;
        }

        result = array[first];
        if (predicate == null || predicate(result))
        {
            // Clear-then-release ordering: see TryDequeue.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            {
                array[first] = default!; // Clear the slot to release the element
                Volatile.Write(ref segment._state._first, (first + 1) & (segment._array.Length - 1));
            }
            else
            {
                segment._state._first = (first + 1) & (segment._array.Length - 1);
            }
            // Freshness-only refresh of _lastCopy.
            segment._state._lastCopy = Volatile.Read(ref segment._state._last);
            return true;
        }

        result = default;
        return false;
    }

    public void Clear()
    {
        while (TryDequeue(out _)) ;
    }

    /// <summary>Gets whether the collection is currently empty.</summary>
    /// <remarks>WARNING: This should not be used concurrently without further vetting.</remarks>
    public bool IsEmpty
    {
        get
        {
            // This implementation is optimized for calls from the consumer.

            // Plain read of _head: reference reads are ordered via data-dependency through the load.
            Segment head = _head;
            // Acquire-load on _first, subsequent plain read of _lastCopy cannot be reordered past it.
            // Dequeues of elements without references store _first plain (see TryDequeue), so from
            // a thread other than the consumer this read is best-effort, per the remarks above; from
            // the consumer's own thread it is always fresh.
            var first = Volatile.Read(ref head._state._first);
            if (first != head._state._lastCopy)
            {
                return false;
            }

            // Acquire-load on _last to pair with producer's Volatile.Write release.
            if (first != Volatile.Read(ref head._state._last))
            {
                return false;
            }

            // Plain read of _next: data-dependent on `head`, reference assignment by producer is release per the .NET memory model.
            return head._next == null;
        }
    }

    /// <summary>Gets an enumerable for the collection.</summary>
    /// <remarks>This method is not safe to use concurrently with any other members that may mutate the collection.</remarks>
    IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

    /// <summary>Gets an enumerable for the collection.</summary>
    /// <remarks>This method is not safe to use concurrently with any other members that may mutate the collection.</remarks>
    IEnumerator IEnumerable.GetEnumerator() { return GetEnumerator(); }

    /// <summary>Returns a struct-based enumerator that walks the queue without allocating.</summary>
    /// <remarks>
    /// Not safe to use concurrently with mutations. Under concurrent use, individual reference-typed
    /// fields within elements are still read atomically, but composite value-type elements may be torn.
    /// Callers should null-check extracted references defensively.
    /// </remarks>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>A thread-safe way to get the number of items in the collection. May synchronize access by locking the provided synchronization object.</summary>
    /// <param name="syncObj">The sync object used to lock</param>
    /// <returns>The collection count</returns>
    public int GetCountSafe(object syncObj)
    {
        Debug.Assert(syncObj != null, "The syncObj parameter is null.");
        lock (syncObj)
        {
            return Count;
        }
    }

    /// <summary>Gets the number of items in the collection.</summary>
    /// <remarks>This method is not safe to use concurrently with any other members that may mutate the collection.</remarks>
    public int Count
    {
        get
        {
            int count = 0;
            // Reference reads of _head and _next are plain, data-dependency ordering covers them.
            // Value-type reads of _first/_last use Volatile.Read to pair with producer/consumer releases.
            for (Segment? segment = _head; segment != null; segment = segment._next)
            {
                int arraySize = segment._array.Length;
                int first, last;
                while (true) // Count is not meant to be used concurrently, but this helps to avoid issues if it is
                {
                    first = Volatile.Read(ref segment._state._first);
                    last = Volatile.Read(ref segment._state._last);
                    // Re-check _first to detect concurrent consumer advancement during the read, if it moved we retry.
                    if (first == Volatile.Read(ref segment._state._first))
                    {
                        break;
                    }
                }

                count += (last - first) & (arraySize - 1);
            }
            return count;
        }
    }

    /// <summary>A zero-allocation enumerator that walks the queue's segments.</summary>
    public struct Enumerator : IEnumerator<T>
    {
        Segment? _segment;
        int _position;
        int _last;

        internal Enumerator(SingleProducerSingleConsumerQueue<T> queue)
        {
            // Plain read of _head, reference assignment by producer is release per the .NET memory model,
            // and subsequent value-type reads use Volatile.Read to pair with the producer/consumer releases on _first/_last.
            _segment = queue._head;
            if (_segment is not null)
            {
                _position = Volatile.Read(ref _segment._state._first);
                _last = Volatile.Read(ref _segment._state._last);
            }
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            while (_segment is not null)
            {
                if (_position != _last)
                {
                    Current = _segment._array[_position];
                    _position = (_position + 1) & (_segment._array.Length - 1);
                    return true;
                }

                // Plain read of _next, data-dependency through the loaded reference orders subsequent value-type reads.
                _segment = _segment._next;
                if (_segment is not null)
                {
                    _position = Volatile.Read(ref _segment._state._first);
                    _last = Volatile.Read(ref _segment._state._last);
                }
            }
            return false;
        }

        void IEnumerator.Reset() => throw new NotSupportedException();
        object? IEnumerator.Current => Current;
        void IDisposable.Dispose() {}
    }

    /// <summary>A segment in the queue containing one or more items.</summary>
    [StructLayout(LayoutKind.Sequential)]
    sealed class Segment
    {
        /// <summary>The next segment in the linked list of segments.</summary>
        internal Segment? _next;
        /// <summary>The data stored in this segment.</summary>
        internal readonly T[] _array;
        /// <summary>Details about the segment.</summary>
        internal SegmentState _state; // separated out to enable StructLayout attribute to take effect

        /// <summary>Initializes the segment.</summary>
        /// <param name="size">The size to use for this segment.</param>
        internal Segment(int size)
        {
            Debug.Assert((size & (size - 1)) == 0, "Size must be a power of 2");
            _array = new T[size];
        }
    }

    /// <summary>Stores information about a segment.</summary>
    [StructLayout(LayoutKind.Sequential)] // enforce layout so that padding reduces false sharing
    struct SegmentState
    {
        /// <summary>Padding to reduce false sharing between the segment's array and _first.</summary>
        internal PaddingFor32 _pad0;

        /// <summary>The index of the current head in the segment. Consumer-owned: the consumer reads
        /// and writes it PLAIN on its hot path (no fence needed for its own field; data visibility
        /// rides the _last acquire/release pair). The producer's room-check and the best-effort
        /// observers (IsEmpty/Count) read it with Volatile.Read since they touch it cross-thread.</summary>
        internal int _first;
        /// <summary>A copy of the current tail index. Consumer-owned (and read by IsEmpty after acquire-load on _first).</summary>
        internal int _lastCopy;

        /// <summary>Padding to reduce false sharing between the first and last.</summary>
        internal PaddingFor32 _pad1;

        /// <summary>A copy of the current head index. Producer-owned.</summary>
        internal int _firstCopy;
        /// <summary>The index of the current tail in the segment. Producer uses Volatile.Write on publish, consumer uses Volatile.Read on slow-path refresh.</summary>
        internal int _last;

        /// <summary>Padding to reduce false sharing with the last and what's after the segment.</summary>
        internal PaddingFor32 _pad2;
    }

    /// <summary>Debugger type proxy for a SingleProducerSingleConsumerQueue of T.</summary>
    sealed class SingleProducerSingleConsumerQueue_DebugView
    {
        /// <summary>The queue being visualized.</summary>
        readonly SingleProducerSingleConsumerQueue<T> _queue;

        /// <summary>Initializes the debug view.</summary>
        /// <param name="queue">The queue being debugged.</param>
        public SingleProducerSingleConsumerQueue_DebugView(SingleProducerSingleConsumerQueue<T> queue)
        {
            Debug.Assert(queue != null, "Expected a non-null queue.");
            _queue = queue;
        }

        /// <summary>Gets the contents of the list.</summary>
        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        public T[] Items => new List<T>(_queue).ToArray();
    }
}
