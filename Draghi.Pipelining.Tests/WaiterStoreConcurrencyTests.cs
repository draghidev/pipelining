using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// Targeted regression guard for the slot field/flag tear (backlog #7). The tear was a claim
/// winning between a commit's flag publish and its field writes, reading a stale/default or torn
/// pair. The tri-state word fixed it (data-then-license commit, peek-gated claim); these races
/// hammer exactly that window at the WaiterStore boundary - a far tighter loop than the full
/// pipeline stress, so a reintroduced tear surfaces fast as a default(0) or a lost/duplicated
/// marker. DRAGHI_STRESS_ITERATIONS overrides the iteration count.
[TestClass, DoNotParallelize]
public class WaiterStoreConcurrencyTests
{
    // SlotClaimRace is a probabilistic torn-claim hunter: one persistent claimer thread races the
    // committer on a reused slot for Iterations inner iterations (a deep sweep raises it via
    // DRAGHI_STRESS_ITERATIONS). (SlotEscalation_AllInterleavings is now deterministic - no count.)
    static int Iterations =>
        int.TryParse(Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 300_000;

    /// Slot-tier churn: the producer commits a marker then waits (via Count) for the claimer to
    /// drain it before committing the next, so the store never escalates and every iteration is a
    /// fresh slot tenancy. The claimer spins on TryClaimSlotForDrain, so it is mid-peek exactly
    /// when the producer publishes - the tear window. Markers are strictly sequential under the
    /// handshake, so a torn claim shows as a non-sequential or zero value.
    [TestMethod]
    public void SlotClaimRace_NoTornOrStaleClaims()
    {
        var box = new WaiterStoreBox<int>();
        var n = Iterations;
        Exception? failure = null;

        var claimer = new Thread(() =>
        {
            try
            {
                for (var expected = 1; expected <= n; expected++)
                {
                    int item;
                    while (!box.TryClaimSlotForDrain(out item, out _))
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
            box.TryEscalateOrEnqueue(marker, default, out var isSlot, out _);
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
    /// interleaving explicitly and assert no torn/default/lost/duplicated marker. (Weak-memory ordering
    /// - the actual concurrency hazard - is covered by WeakMemory.tla + the WaiterStore ordering units,
    /// not by this brute-force sampler.)
    [TestMethod]
    public void SlotEscalation_AllInterleavings_NoLostOrTornMarkers()
    {
        // Case 1: claim takes the slot BEFORE escalation (claimer wins the slot).
        {
            var box = new WaiterStoreBox<int>();
            box.TryEscalateOrEnqueue(1, default, out var isSlot, out _);
            Assert.IsTrue(isSlot, "marker 1 must land in the slot on a fresh box.");
            Assert.IsTrue(box.TryClaimSlotForDrain(out var claimed, out _), "occupied slot must be claimable.");
            Assert.AreEqual(1, claimed, "claim must read the committed pair, never default/torn.");
            box.DecrementCount();
            // Slot now empty; further commits go to the queue (still pre-escalation slot path first).
            box.TryEscalateOrEnqueue(2, default, out var isSlot2, out _);
            Assert.IsTrue(isSlot2, "after the slot drained, marker 2 re-occupies the (empty) slot.");
            Assert.IsTrue(box.TryClaimSlotForDrain(out var c2, out _)); Assert.AreEqual(2, c2); box.DecrementCount();
            Assert.AreEqual(0, box.Count);
        }

        // Case 2: escalation MOVES an occupied slot (escalation wins). marker1 -> slot; marker2's commit
        // sees the slot occupied, escalates, CAS-moves marker1 to the queue head (slotWasMoved). A
        // subsequent claim finds the slot empty; both drain from the queue in FIFO order.
        {
            var box = new WaiterStoreBox<int>();
            box.TryEscalateOrEnqueue(1, default, out var s1, out _);
            Assert.IsTrue(s1);
            box.TryEscalateOrEnqueue(2, default, out var s2, out var moved2);
            Assert.IsFalse(s2, "marker 2 escalates to the queue.");
            Assert.IsTrue(moved2, "the occupied slot's marker 1 must be moved to the queue head.");
            box.TakeMovedSlotPair(out var movedItem, out _);
            Assert.AreEqual(1, movedItem, "moved pair must be marker 1, never default/torn.");
            Assert.IsFalse(box.TryClaimSlotForDrain(out _, out _), "post-escalation the slot is empty: no claim.");
            Assert.IsTrue(box.TryDequeue(out var q1)); Assert.AreEqual(1, q1.Waiter); box.DecrementCount();
            Assert.IsTrue(box.TryDequeue(out var q2)); Assert.AreEqual(2, q2.Waiter); box.DecrementCount();
            Assert.AreEqual(0, box.Count);
        }

        // Case 3: claim on an EMPTY slot and on an ESCALATED (slot-empty) box returns false, never a
        // torn/default success.
        {
            var box = new WaiterStoreBox<int>();
            Assert.IsFalse(box.TryClaimSlotForDrain(out _, out _), "empty box: claim returns false.");
            box.TryEscalateOrEnqueue(1, default, out _, out _);
            box.TryEscalateOrEnqueue(2, default, out _, out var moved); // escalates, slot now empty
            Assert.IsTrue(moved);
            box.TakeMovedSlotPair(out _, out _);
            Assert.IsFalse(box.TryClaimSlotForDrain(out _, out _), "escalated box: slot empty, claim returns false.");
            while (box.TryDequeue(out _)) box.DecrementCount();
            Assert.AreEqual(0, box.Count);
        }

        // Case 4: many fresh boxes through the full slot->escalation->drain cycle, asserting the marker
        // accounting holds every time (cheap deterministic loop; catches an accounting regression that a
        // single case might miss without paying for threads/spin).
        const int perBox = 3;
        for (var b = 0; b < 5_000; b++)
        {
            var box = new WaiterStoreBox<int>();
            var drained = new bool[perBox + 1];
            for (var marker = 1; marker <= perBox; marker++)
                box.TryEscalateOrEnqueue(marker, default, out _, out var moved);
            // Drain: the slot first (if still occupied), then the queue.
            if (box.TryClaimSlotForDrain(out var slotItem, out _)) { Record(drained, slotItem); box.DecrementCount(); }
            while (box.TryDequeue(out var entry)) { Record(drained, entry.Waiter); box.DecrementCount(); }
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
}

/// Boxes the WaiterStore struct so racing threads share one instance (the producer commits, the
/// consumer claims/dequeues against the same storage).
sealed class WaiterStoreBox<T>
{
    WaiterStore<T> _store;
    public int Count => _store.Count;
    public int TryEscalateOrEnqueue(T item, ValueTask task, out bool isSlot, out bool slotWasMoved)
        => _store.TryEscalateOrEnqueue(item, task, out isSlot, out slotWasMoved);
    public bool TryClaimSlotForDrain(out T item, out ValueTask task) => _store.TryClaimSlotForDrain(out item, out task);
    public void TakeMovedSlotPair(out T item, out ValueTask task) => _store.TakeMovedSlotPair(out item, out task);
    public bool TryDequeue(out (T Waiter, ValueTask WaiterTask) entry) => _store.TryDequeue(out entry);
    public int DecrementCount() => _store.DecrementCount();
}
