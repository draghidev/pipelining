using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// Targeted regression guard for the slot field/flag tear. The tear was a claim
/// winning between a commit's flag publish and its field writes, reading a stale/default or torn
/// pair. The tri-state word fixed it (data-then-license commit, peek-gated claim). These races
/// hammer exactly that window at the InFlightStore boundary, a far tighter loop than the full
/// pipeline stress, so a reintroduced tear surfaces fast as a default(0) or a lost/duplicated
/// marker. DRAGHI_STRESS_ITERATIONS overrides the iteration count.
[TestClass, DoNotParallelize]
public class InFlightStoreConcurrencyTests
{
    // SlotClaimRace is a probabilistic torn-claim hunter: one persistent claimer thread races the
    // committer on a reused slot for Iterations inner iterations (a deep sweep raises it via
    // DRAGHI_STRESS_ITERATIONS). (SlotEscalation_AllInterleavings is now deterministic - no count.)
    static int Iterations =>
        int.TryParse(Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 300_000;

    /// Slot-tier churn: the producer commits a marker then waits (via Count) for the claimer to
    /// drain it before committing the next, so the store never escalates and every iteration is a
    /// fresh slot tenancy. The claimer spins on TryClaimCompletedSlot, so it is mid-peek exactly
    /// when the producer publishes - the tear window. Markers are strictly sequential under the
    /// handshake, so a torn claim shows as a non-sequential or zero value.
    [TestMethod]
    public void SlotClaimRace_NoTornOrStaleClaims()
    {
        var box = new InFlightStoreBox<int>();
        var n = Iterations;
        Exception? failure = null;

        var claimer = new Thread(() =>
        {
            try
            {
                for (var expected = 1; expected <= n; expected++)
                {
                    int item;
                    while (!box.TryClaimCompletedSlot(out item, out _))
                        Thread.SpinWait(1);
                    Assert.AreNotEqual(0, item, "Torn/stale claim: read a default(T) mid-commit.");
                    Assert.AreEqual(expected, item, "Torn/stale claim: read the wrong tenancy's value.");
                    box.DecrementCount();
                }
            }
            catch (Exception ex) { failure = ex; }
        });
        claimer.Start();

        for (var marker = 1; marker <= n; marker++)
        {
            box.TryEscalateOrEnqueue(marker, default, out var isSlot);
            Assert.IsTrue(isSlot, "Handshake must keep every commit slot-tier (no escalation).");
            while (box.Count != 0 && failure is null)
                Thread.SpinWait(1);
            if (failure is not null)
                break;
        }

        claimer.Join();
        if (failure is not null)
            throw new AssertFailedException("Slot claim race failed.", failure);
    }

    /// Slot<->escalation<->claim correctness, DETERMINISTIC (was a 2000-round spin race, ~20ms alone
    /// but ~600ms in-suite because the round-handshake's spin escalates to Thread.Sleep once the
    /// process has accumulated sibling-test threads - and a true race wasn't buying coverage anyway).
    ///
    /// Each operation is internally atomic - the slot commit is data-then-license (fields then the
    /// release-store of Occupied, so any reader sees empty or a COMPLETE pair, never torn), the claim
    /// and escalation each pivot on one Interlocked CAS against Occupied. So EVERY observable outcome is
    /// reachable by ORDERING the calls; the spin only sampled these probabilistically. We drive each
    /// interleaving explicitly and assert no torn/default/lost/duplicated marker. (Weak-memory
    /// ordering, the actual concurrency hazard, is covered by the InFlightStore ordering units, not
    /// by this brute-force sampler.)
    [TestMethod]
    public void SlotEscalation_AllInterleavings_NoLostOrTornMarkers()
    {
        // Case 1: claim takes the slot BEFORE escalation (claimer wins the slot).
        {
            var box = new InFlightStoreBox<int>();
            box.TryEscalateOrEnqueue(1, default, out var isSlot);
            Assert.IsTrue(isSlot, "marker 1 must land in the slot on a fresh box.");
            Assert.IsTrue(box.TryClaimCompletedSlot(out var claimed, out _), "occupied slot must be claimable.");
            Assert.AreEqual(1, claimed, "claim must read the committed pair, never default/torn.");
            box.DecrementCount();
            // Slot now empty; further commits go to the queue (still pre-escalation slot path first).
            box.TryEscalateOrEnqueue(2, default, out var isSlot2);
            Assert.IsTrue(isSlot2, "after the slot drained, marker 2 re-occupies the (empty) slot.");
            Assert.IsTrue(box.TryClaimCompletedSlot(out var c2, out _)); Assert.AreEqual(2, c2); box.DecrementCount();
            Assert.AreEqual(0, box.Count);
        }

        // Case 2: LEAVE-HEAD escalation. marker1 -> slot; marker2's commit sees the slot occupied,
        // escalates, and enqueues ONLY marker2 - marker1 STAYS in the slot (never moved). The head
        // retires from the slot tier first, then the queue overflow, in FIFO order.
        {
            var box = new InFlightStoreBox<int>();
            box.TryEscalateOrEnqueue(1, default, out var s1);
            Assert.IsTrue(s1);
            box.TryEscalateOrEnqueue(2, default, out var s2);
            Assert.IsFalse(s2, "marker 2 escalates to the queue.");
            Assert.IsTrue(box.IsEscalated);
            Assert.IsTrue(box.TrySnapshotSlot(out var resident), "marker 1 must stay resident in the slot.");
            Assert.AreEqual(1, resident, "leave-head: the pre-escalation occupant is the slot-resident head.");
            // Unified head API: slot head first, then queue head.
            Assert.IsTrue(box.TryClaimCompletedHead(out var h1, out _)); Assert.AreEqual(1, h1); box.DecrementCount();
            Assert.IsTrue(box.TryClaimCompletedHead(out var h2, out _)); Assert.AreEqual(2, h2); box.DecrementCount();
            Assert.AreEqual(0, box.Count);
        }

        // Case 3: claim on an EMPTY slot returns false; on an ESCALATED box the slot occupant is
        // still resident (leave-head), so the slot claim SUCCEEDS for the head. Never a torn/default.
        {
            var box = new InFlightStoreBox<int>();
            Assert.IsFalse(box.TryClaimCompletedSlot(out _, out _), "empty box: claim returns false.");
            box.TryEscalateOrEnqueue(1, default, out _);
            box.TryEscalateOrEnqueue(2, default, out _); // escalates; marker 1 stays in the slot
            Assert.IsTrue(box.TryClaimCompletedSlot(out var slotHead, out _), "escalated box: slot occupant still claimable (leave-head).");
            Assert.AreEqual(1, slotHead);
            box.DecrementCount();
            Assert.IsFalse(box.TryClaimCompletedSlot(out _, out _), "slot now drained: claim returns false.");
            while (box.TryDequeue(out _)) box.DecrementCount();
            Assert.AreEqual(0, box.Count);
        }

        // Case 4: many fresh boxes through the full slot->escalation->drain cycle, asserting the marker
        // accounting holds every time (cheap deterministic loop; catches an accounting regression that a
        // single case might miss without paying for threads/spin).
        const int perBox = 3;
        for (var b = 0; b < 5_000; b++)
        {
            var box = new InFlightStoreBox<int>();
            var drained = new bool[perBox + 1];
            for (var marker = 1; marker <= perBox; marker++)
                box.TryEscalateOrEnqueue(marker, default, out _);
            // Drain via the unified head API: slot head first (leave-head resident), then the queue.
            while (box.TryClaimCompletedHead(out var head, out _)) { Record(drained, head); box.DecrementCount(); }
            for (var marker = 1; marker <= perBox; marker++)
                Assert.IsTrue(drained[marker], $"box {b}: marker {marker} lost or duplicated. finalCount={box.Count}");
            Assert.AreEqual(0, box.Count);
        }
    }

    static void Record(bool[] drained, int marker)
    {
        Assert.AreNotEqual(0, marker, "Torn/stale drain: read a default(T).");
        Assert.IsFalse(drained[marker], $"Duplicate drain of marker {marker}.");
        drained[marker] = true;
    }

    /// Segment-boundary soak for the queue consumer's hop guard: a slow-path segment hop must
    /// never advance past resident entries. Fresh small queues keep every iteration inside the
    /// initial segment's wrap and first growths, where the hop windows live, and the consumer
    /// runs the drain loop's peek-then-dequeue shape with jittered pacing on both sides so the
    /// catch-up points land at random offsets. Every marker must arrive exactly once, in order.
    /// The escape timeout converts a marooning into a loud failure instead of a hang.
    [TestMethod]
    public void QueueSegmentBoundaryHop_ManyCrossings_NoMarooningNoReorder()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 400;
        const int markers = 100;

        for (var iter = 0; iter < iterations; iter++)
        {
            var queue = new Internal.SingleProducerSingleConsumerQueue<int>();
            var producerSeed = 12345 + iter;
            var producer = new Thread(() =>
            {
                var rnd = new Random(producerSeed);
                for (var m = 0; m < markers; m++)
                {
                    queue.Enqueue(m);
                    if ((m & 7) == 7)
                        Thread.SpinWait(rnd.Next(128));
                }
            })
            { IsBackground = true };
            producer.Start();

            var consumerRnd = new Random(54321 + iter);
            var expected = 0;
            var escape = System.Diagnostics.Stopwatch.StartNew();
            var spin = new SpinWait();
            while (expected < markers)
            {
                if (queue.TryPeek(out var peeked))
                {
                    Assert.AreEqual(expected, peeked, $"iter {iter}: peek out of order, entries marooned or reordered");
                    Assert.IsTrue(queue.TryDequeue(out var dequeued), $"iter {iter}: dequeue failed after a successful peek");
                    Assert.AreEqual(expected, dequeued, $"iter {iter}: dequeue disagreed with peek");
                    expected++;
                    if ((expected & 15) == 0)
                        Thread.SpinWait(consumerRnd.Next(64));
                    spin = new SpinWait();
                }
                else
                {
                    if (escape.Elapsed > TimeSpan.FromSeconds(10))
                        Assert.Fail($"iter {iter}: consumer starved at marker {expected}/{markers}, entries marooned behind a wrong segment hop");
                    spin.SpinOnce();
                }
            }
            Assert.IsFalse(queue.TryPeek(out _), $"iter {iter}: queue not empty after all markers consumed");
            producer.Join();
        }
    }
}

/// Boxes the InFlightStore struct so racing threads share one instance (the producer commits, the
/// consumer claims/dequeues against the same storage).
sealed class InFlightStoreBox<T>
{
    // InFlightStore<T> is a struct with a custom parameterless constructor (allocates the edge lock);
    // a bare field declaration zero-inits instead of calling it (the 2026-07-08 struct-ctor trap:
    // then the count bias, now the lock).
    InFlightStore<T> _store = new();
    ItemTenure _tenure;
    public int Count => _store.Count;
    public bool IsEscalated => _store.IsEscalated;
    public int TryEscalateOrEnqueue(T item, ValueTask task, out bool isSlot)
    {
        // Increment-first commit pair (what CommitInFlightItem does around its edge/mid routing). Returns
        // the new count to keep this harness's callers' arithmetic unchanged.
        var prev = _store.IncrementCommitCount();
        _store.PublishCommitted(item, task, out isSlot);
        return prev + 1;
    }
    public bool TryClaimCompletedSlot(out T item, out ValueTask task) => _store.TryClaimCompletedSlot(ref _tenure, out item, out task, out _);
    public bool TryClaimCompletedHead(out T item, out ValueTask task) => _store.TryClaimCompletedHead(ref _tenure, out item, out task, out _, out _);
    public bool TrySnapshotSlot(out T item) => _store.TrySnapshotSlot(out item);
    public bool TryDequeue(out (T Item, ValueTask PipelineTask) entry) => _store.TryDequeue(out entry);
    public bool DecrementCount() => _store.DecrementCount();
}
