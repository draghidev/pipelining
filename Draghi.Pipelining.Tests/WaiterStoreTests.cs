using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// Tri-state slot word contract. The
/// fields and the occupancy flag tore under the old two-state word: a claim that won between a
/// commit's flag publish and its field writes read a stale/default or torn pair. These tests pin
/// the three behaviors the tri-state introduced (the data-then-license commit, the live-task
/// peek bail, and the licensed never-default claim) plus the LEAVE-HEAD escalation contract (the
/// slot occupant stays in the slot; only the overflow is enqueued).
[TestClass]
public class WaiterStoreTests
{
    static ValueTask Completed => default;            // a default ValueTask is completed-successfully
    static ValueTask Live => new(new TaskCompletionSource().Task);

    /// The commit pair under increment-first: count, then publish (what CommitWaiter does around
    /// its edge/mid-chain routing). Returns the pre-increment count.
    static int Commit(ref WaiterStore<int> store, int item, ValueTask task, out bool isSlot)
    {
        var prev = store.IncrementCommitCount();
        store.PublishCommitted(item, task, out isSlot);
        return prev;
    }

    [TestMethod]
    public void Commit_ThenClaim_ReturnsCommittedPair()
    {
        var store = new WaiterStore<int>();
        Commit(ref store, 42, Completed, out var isSlot);
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
        Commit(ref store, 7, Live, out _);

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
        Commit(ref store, 7, Completed, out _);
        Assert.IsTrue(store.TryClaimSlotForDrain(out var item, out _));
        Assert.AreEqual(7, item);
    }

    [TestMethod]
    public void DoubleClaim_SecondReturnsFalse()
    {
        var store = new WaiterStore<int>();
        Commit(ref store, 7, Completed, out _);
        Assert.IsTrue(store.TryClaimSlotForDrain(out _, out _));
        Assert.IsFalse(store.TryClaimSlotForDrain(out var item, out _), "The slot is consumed; a second claim must fail.");
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_AfterDrain_SlotIsReusable()
    {
        var store = new WaiterStore<int>();
        Commit(ref store, 1, Completed, out _);
        Assert.IsTrue(store.TryClaimSlotForDrain(out _, out _));
        store.DecrementCount();

        Commit(ref store, 2, Completed, out var isSlot);
        Assert.IsTrue(isSlot, "After a slot drain the slot must be reusable (stays slot-tier, no escalation).");
        Assert.IsTrue(store.TryClaimSlotForDrain(out var item, out _));
        Assert.AreEqual(2, item);
    }

    [TestMethod]
    public void SecondCommit_Escalates_LeavesOccupantInSlot()
    {
        // LEAVE-HEAD: escalation publishes the queue and enqueues ONLY the overflow; the slot
        // occupant STAYS in the slot and retires through the slot tier (never moved).
        var store = new WaiterStore<int>();
        Commit(ref store, 1, Completed, out var firstIsSlot);
        Assert.IsTrue(firstIsSlot);

        Commit(ref store, 2, Completed, out var secondIsSlot);
        Assert.IsFalse(secondIsSlot, "The second commit escalates to the queue.");

        Assert.IsTrue(store.IsEscalated);
        // FIFO head is the slot occupant (item 1); the queue holds only the overflow (item 2).
        Assert.IsTrue(store.TrySnapshotSlot(out var slotHead), "The occupant must stay resident in the slot.");
        Assert.AreEqual(1, slotHead, "FIFO: the pre-escalation occupant is the slot-resident head.");
        Assert.IsTrue(store.TryDequeue(out var tail), "The queue holds the overflow.");
        Assert.AreEqual(2, tail.Waiter);
    }

    [TestMethod]
    public void ClaimCompletedHead_Escalated_DrainsSlotHeadBeforeQueue()
    {
        // The unified head API enforces the ordering contract: the slot-resident head retires
        // FIRST, then the queue head, and never a queue head while the slot is occupied.
        var store = new WaiterStore<int>();
        Commit(ref store, 1, Completed, out _);
        Commit(ref store, 2, Completed, out _); // escalate; item 1 stays in the slot.

        Assert.IsTrue(store.TryClaimCompletedHead(out var first, out _));
        Assert.AreEqual(1, first, "The slot-resident head must retire before the queue head.");
        store.DecrementCount();
        Assert.IsTrue(store.TryClaimCompletedHead(out var second, out _));
        Assert.AreEqual(2, second, "The queue head retires only after the slot is empty.");
        store.DecrementCount();
        Assert.IsFalse(store.TryClaimCompletedHead(out _, out _), "Store drained.");
    }

    [TestMethod]
    public void ClaimCompletedHead_LiveSlotHead_DoesNotJumpToCompletedQueueHead()
    {
        // Ordering contract: a slot head whose task is not yet completed blocks the queue head
        // (which IS completed) from retiring out of order.
        var store = new WaiterStore<int>();
        Commit(ref store, 1, Live, out _);      // slot head, not completed
        Commit(ref store, 2, Completed, out _); // queue head, completed

        Assert.IsFalse(store.TryClaimCompletedHead(out var item, out _),
            "A live slot head must block the completed queue head (no out-of-order retirement).");
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_AfterEscalation_SlotOccupantStillClaimable()
    {
        // LEAVE-HEAD: post-escalation the slot still holds the pre-escalation occupant, so the
        // slot-tier claim succeeds (the occupant retires through the slot tier).
        var store = new WaiterStore<int>();
        Commit(ref store, 1, Completed, out _);
        Commit(ref store, 2, Completed, out _);  // escalates; item 1 stays in the slot
        Assert.IsTrue(store.TryClaimSlotForDrain(out var item, out _),
            "Post-escalation the slot occupant is still resident and claimable (leave-head).");
        Assert.AreEqual(1, item);
    }

    /// Successor of the 2026-07-08 exec-word regression guard (CloseExecActivation never resetting).
    /// The gen-word machinery it pinned is deleted; the equivalent contract under the edge-lock
    /// protocol: a full deferral grant lifecycle (place -> census take -> sentinel assign ->
    /// release) leaves NO residue that blocks a later item's turn assignment or retirement.
    [TestMethod]
    public void ExecGrantLifecycle_LeavesNoResidueForLaterTurns()
    {
        var store = new WaiterStore<int>();

        // A full census-shaped grant lifecycle on a parked deferral. The census PEEKS the gen +
        // item, FAIL-IF-LIVE claims the turn as -gen, then GEN-PINS the consume, handing back the
        // deferral's OWN item (7).
        var gen = store.PlaceExecDeferred(7);
        Assert.IsTrue(store.ExecDeferredVisible);
        Assert.IsTrue(store.TryPeekExecDeferred(out var peekedGen, out var granted));
        Assert.AreEqual(gen, peekedGen);
        Assert.AreEqual(7, granted, "The census peeks the deferral's own item, not a stale capture.");
        Assert.IsTrue(store.TryClaimTurnGrant(peekedGen), "Fail-if-live turn claim must win on a free turn.");
        Assert.AreEqual(-peekedGen, store.Turn, "The census grant is identity-tagged -gen.");
        Assert.IsTrue(store.TryConsumeExecDeferredGen(peekedGen), "The gen-pinned consume must win the unrecycled deferral.");
        Assert.IsTrue(store.ReleaseTurn(-peekedGen), "The grant's completion releases exactly its -gen.");

        // A later, unrelated item commits at the edge and retires: the turn must assign and
        // release cleanly - no residue from the earlier grant.
        Assert.AreEqual(0, Commit(ref store, 1, Completed, out var isSlot));
        Assert.IsTrue(isSlot);
        Assert.IsFalse(store.AssignTurnAtCommit(0, store.HeadSeq), "A fresh edge assign must not read as inherited.");
        Assert.IsTrue(store.TryClaimCompletedHead(out var item, out _));
        Assert.AreEqual(1, item);
        Assert.IsTrue(store.ReleaseTurn(store.LastClaimedSeq), "The retiring owner's identity release must win.");
        Assert.IsTrue(store.DecrementCount(), "Single item: the decrement drains to zero.");
    }
}
