using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// Tri-state slot word contract. The
/// fields and the occupancy flag tore under the old two-state word: a claim that won between a
/// commit's flag publish and its field writes read a stale/default or torn pair. These tests pin
/// the three behaviors the tri-state introduced (the data-then-license commit, the live-task
/// peek bail, and the licensed never-default claim) plus the quiescent escalation handoff.
[TestClass]
public class WaiterStoreTests
{
    static ValueTask Completed => default;            // a default ValueTask is completed-successfully
    static ValueTask Live => new(new TaskCompletionSource().Task);

    [TestMethod]
    public void Commit_ThenClaim_ReturnsCommittedPair()
    {
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(42, Completed, out var isSlot, out _);
        Assert.IsTrue(isSlot, "First commit should land in the slot.");

        Assert.IsTrue(store.TryClaimSlotForDrain(out var item, out _));
        Assert.AreEqual(42, item, "Claim must return the exact committed item, never a torn/default value.");
    }

    [TestMethod]
    public void Claim_OnEmptySlot_ReturnsFalse()
    {
        var store = new WaiterStore<int>();
        Assert.IsFalse(store.TryClaimSlotForDrain(out var item, out _));
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_OnLiveTask_BailsWithoutStateChange()
    {
        // THE live-task bail: a stale-token reclaim that lands on an occupied-but-still-running
        // slot must NOT claim it (that would block the drainer in GetResult and, pre-tri-state,
        // could strand the occupant). It bails with no state change; the occupant's own callback
        // drains it once the task completes. We model "callback fired" by claiming after.
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(7, Live, out _, out _);

        Assert.IsFalse(store.TryClaimSlotForDrain(out var item, out _),
            "A live-task slot must not be claimed.");
        Assert.AreEqual(0, item);
        Assert.IsTrue(store.TrySnapshotSlot(out var stillThere), "The bail must leave the occupant in place.");
        Assert.AreEqual(7, stillThere);
    }

    [TestMethod]
    public void Claim_OnCompletedTask_Succeeds()
    {
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(7, Completed, out _, out _);
        Assert.IsTrue(store.TryClaimSlotForDrain(out var item, out _));
        Assert.AreEqual(7, item);
    }

    [TestMethod]
    public void DoubleClaim_SecondReturnsFalse()
    {
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(7, Completed, out _, out _);
        Assert.IsTrue(store.TryClaimSlotForDrain(out _, out _));
        Assert.IsFalse(store.TryClaimSlotForDrain(out var item, out _), "The slot is consumed; a second claim must fail.");
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_AfterDrain_SlotIsReusable()
    {
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(1, Completed, out _, out _);
        Assert.IsTrue(store.TryClaimSlotForDrain(out _, out _));
        store.DecrementCount();

        store.TryEscalateOrEnqueue(2, Completed, out var isSlot, out _);
        Assert.IsTrue(isSlot, "After a slot drain the slot must be reusable (stays slot-tier, no escalation).");
        Assert.IsTrue(store.TryClaimSlotForDrain(out var item, out _));
        Assert.AreEqual(2, item);
    }

    [TestMethod]
    public void SecondCommit_Escalates_MovesOccupantToQueueHead()
    {
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(1, Completed, out var firstIsSlot, out _);
        Assert.IsTrue(firstIsSlot);

        store.TryEscalateOrEnqueue(2, Completed, out var secondIsSlot, out var slotWasMoved);
        Assert.IsFalse(secondIsSlot, "The second commit escalates to the queue.");
        Assert.IsTrue(slotWasMoved, "The quiescent occupant must move to the queue (CAS 1->0 won).");

        Assert.IsTrue(store.IsEscalated);
        Assert.IsTrue(store.TryDequeue(out var head));
        Assert.AreEqual(1, head.Waiter, "FIFO: the moved slot occupant is the queue head.");
        Assert.IsTrue(store.TryDequeue(out var tail));
        Assert.AreEqual(2, tail.Waiter);
    }

    [TestMethod]
    public void Claim_AfterEscalation_ReturnsFalse()
    {
        var store = new WaiterStore<int>();
        store.TryEscalateOrEnqueue(1, Completed, out _, out _);
        store.TryEscalateOrEnqueue(2, Completed, out _, out _);  // escalates, slot emptied by the move
        Assert.IsFalse(store.TryClaimSlotForDrain(out var item, out _),
            "Post-escalation the slot is empty; the claim must miss (the occupant is the queue head).");
        Assert.AreEqual(0, item);
    }
}
