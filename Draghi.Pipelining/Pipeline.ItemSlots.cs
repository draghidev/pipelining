using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

public sealed partial class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    // Mirrors ConcurrentDictionary's ECMA-335-based atomic-write test. Custom structs remain
    // tearable regardless of size because the JIT may decompose their copies.
    static readonly bool WriteAtomic = IsWriteAtomic();

    static bool IsWriteAtomic()
    {
        if (!typeof(T).IsValueType || typeof(T) == typeof(IntPtr) || typeof(T) == typeof(UIntPtr))
            return true;

        switch (Type.GetTypeCode(typeof(T)))
        {
            case TypeCode.Boolean:
            case TypeCode.Byte:
            case TypeCode.Char:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.SByte:
            case TypeCode.Single:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                return true;
            case TypeCode.Int64:
            case TypeCode.Double:
            case TypeCode.UInt64:
                return IntPtr.Size == 8;
            default:
                return false;
        }
    }

    // Atomic types use the zero-cost plain store; other structs use an odd/even seqlock bracket.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void PublishSlot(ref T slot, ref uint generation, T value)
    {
        if (!typeof(T).IsValueType || WriteAtomic)
        {
            slot = value;
        }
        else
        {
            Interlocked.Increment(ref generation); // odd: store in progress
            slot = value;
            Interlocked.Increment(ref generation); // even: store complete
        }
    }

    // A stale snapshot is permitted; a snapshot overlapping a struct write is retried.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static T ReadSlot(ref T slot, ref uint generation)
    {
        if (!typeof(T).IsValueType || WriteAtomic)
            return slot;

        var spin = new SpinWait();
        while (true)
        {
            var g1 = Volatile.Read(ref generation);
            if ((g1 & 1) == 0) // even: no write in progress at the sample point
            {
                var value = slot; // acquire on g1 orders this read after the generation sample
                Interlocked.MemoryBarrier(); // the copy must complete before the re-sample (LoadLoad)
                if (Volatile.Read(ref generation) == g1)
                    return value;
            }
            spin.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SetExecutingItem(T value) => PublishSlot(ref _executingItem, ref _executingItemGeneration, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SetActivatedItem(T value) => PublishSlot(ref _activatedItem, ref _activatedItemGeneration, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivateHeadItem(T item, bool preferAsync = true)
    {
        // Publish identity before activation. Turn ownership was established by the caller.
        SetActivatedItem(item);
        _policy.ActivateHeadItem(item, preferAsync);
    }

    /// Publishes a newly dispatched zero-edge owner under the same lock used by a delayed
    /// depth-zero clear. The policy callback remains outside the non-reentrant edge lock.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ActivateHeadItemAtZeroEdge(T item, bool preferAsync = true)
    {
        BeforeZeroEdgeSlotPublish?.Invoke();
        var edgeLock = _activationGate.EdgeLock;
        edgeLock.Enter();
        // Publication is only non-throwing stores/interlocked operations. Keep this bracket EH-free so
        // the zero-edge transition remains inlineable while still excluding the delayed clear.
        SetActivatedItem(item);
        edgeLock.Exit();
        _policy.ActivateHeadItem(item, preferAsync);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivatePublishedHeadItem(T item, bool preferAsync = true)
        => _policy.ActivateHeadItem(item, preferAsync);

    /// Resolves the item's activation handoff and clears executor visibility. True gives the caller
    /// completion ownership; false means the empty-edge pass owns activation and the item must enter
    /// the store. This uses plain consume because residents may legitimately hold the activation turn.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool ClearExecutingItem(bool wasActivated)
    {
        // Executor-owned; release the enumeration-visible flag.
        Volatile.Write(ref _executingItemVisible, false);
        var owned = wasActivated || _activationGate.TryTakeHandoff();
        // A winning empty-edge pass captured the item before consuming the handoff.
        SetExecutingItem(default!);
        return owned;
    }
}
