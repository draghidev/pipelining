using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// Tri-state slot word contract. The
/// fields and the occupancy flag tore under the old two-state word: a claim that won between a
/// commit's flag publish and its field writes read a stale/default or torn pair. These tests pin
/// the three behaviors the tri-state introduced (the data-then-license commit, the live-task
/// peek bail, and the licensed never-default claim) plus the LEAVE-HEAD escalation contract (the
/// slot occupant stays in the slot; only the overflow is enqueued).
[TestClass]
public class InFlightStoreTests
{
    static ValueTask Completed => default;            // a default ValueTask is completed-successfully
    static ValueTask Live => new(new TaskCompletionSource().Task);

    /// The commit pair under increment-first: count, then publish (what CommitInFlightItem does around
    /// its edge/mid-chain routing). Returns the pre-increment count.
    static int Commit(ref InFlightStore<int> store, int item, ValueTask task, out bool isSlot)
    {
        var prev = store.IncrementCommitCount();
        store.PublishCommitted(item, task, out isSlot);
        return prev;
    }

    [TestMethod]
    public void Commit_ThenClaim_ReturnsCommittedPair()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 42, Completed, out var isSlot);
        Assert.IsTrue(isSlot, "First commit should land in the slot.");

        Assert.IsTrue(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _));
        Assert.AreEqual(42, item, "Claim must return the exact committed item, never a torn/default value.");
    }

    [TestMethod]
    public void Claim_OnEmptySlot_ReturnsFalse()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Assert.IsFalse(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _));
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_OnLiveTask_BailsWithoutStateChange()
    {
        // THE live-task bail: a stale-token reclaim that lands on an occupied-but-still-running
        // slot must NOT claim it (that would block the drainer in GetResult and, pre-tri-state,
        // could strand the occupant). It bails with no state change; the occupant's own callback
        // drains it once the task completes. We model "callback fired" by claiming after.
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 7, Live, out _);

        Assert.IsFalse(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _),
            "A live-task slot must not be claimed.");
        Assert.AreEqual(0, item);
        Assert.IsTrue(store.TrySnapshotSlot(out var stillThere), "The bail must leave the occupant in place.");
        Assert.AreEqual(7, stillThere);
    }

    [TestMethod]
    public void Claim_OnCompletedTask_Succeeds()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 7, Completed, out _);
        Assert.IsTrue(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _));
        Assert.AreEqual(7, item);
    }

    [TestMethod]
    public void DoubleClaim_SecondReturnsFalse()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 7, Completed, out _);
        Assert.IsTrue(store.TryClaimCompletedSlot(ref tenure, out _, out _, out _));
        Assert.IsFalse(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _), "The slot is consumed; a second claim must fail.");
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_AfterDrain_SlotIsReusable()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 1, Completed, out _);
        Assert.IsTrue(store.TryClaimCompletedSlot(ref tenure, out _, out _, out _));
        store.DecrementCount();

        Commit(ref store, 2, Completed, out var isSlot);
        Assert.IsTrue(isSlot, "After a slot drain the slot must be reusable (stays slot-tier, no escalation).");
        Assert.IsTrue(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _));
        Assert.AreEqual(2, item);
    }

    [TestMethod]
    public void SecondCommit_Escalates_LeavesOccupantInSlot()
    {
        // LEAVE-HEAD: escalation publishes the queue and enqueues ONLY the overflow; the slot
        // occupant STAYS in the slot and retires through the slot tier (never moved).
        var store = new InFlightStore<int>();
        Commit(ref store, 1, Completed, out var firstIsSlot);
        Assert.IsTrue(firstIsSlot);

        Commit(ref store, 2, Completed, out var secondIsSlot);
        Assert.IsFalse(secondIsSlot, "The second commit escalates to the queue.");

        Assert.IsTrue(store.IsEscalated);
        // FIFO head is the slot occupant (item 1); the queue holds only the overflow (item 2).
        Assert.IsTrue(store.TrySnapshotSlot(out var slotHead), "The occupant must stay resident in the slot.");
        Assert.AreEqual(1, slotHead, "FIFO: the pre-escalation occupant is the slot-resident head.");
        Assert.IsTrue(store.TryDequeue(out var tail), "The queue holds the overflow.");
        Assert.AreEqual(2, tail.Item);
    }

    [TestMethod]
    public void ClaimCompletedHead_Escalated_DrainsSlotHeadBeforeQueue()
    {
        // The unified head API enforces the ordering contract: the slot-resident head retires
        // FIRST, then the queue head, and never a queue head while the slot is occupied.
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 1, Completed, out _);
        Commit(ref store, 2, Completed, out _); // escalate; item 1 stays in the slot.

        Assert.IsTrue(store.TryClaimCompletedHead(ref tenure, out var first, out _, out _, out _));
        Assert.AreEqual(1, first, "The slot-resident head must retire before the queue head.");
        store.DecrementCount();
        Assert.IsTrue(store.TryClaimCompletedHead(ref tenure, out var second, out _, out _, out _));
        Assert.AreEqual(2, second, "The queue head retires only after the slot is empty.");
        store.DecrementCount();
        Assert.IsFalse(store.TryClaimCompletedHead(ref tenure, out _, out _, out _, out _), "Store drained.");
    }

    [TestMethod]
    public void ClaimCompletedHead_LiveSlotHead_DoesNotJumpToCompletedQueueHead()
    {
        // Ordering contract: a slot head whose task is not yet completed blocks the queue head
        // (which IS completed) from retiring out of order.
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 1, Live, out _);      // slot head, not completed
        Commit(ref store, 2, Completed, out _); // queue head, completed

        Assert.IsFalse(store.TryClaimCompletedHead(ref tenure, out var item, out _, out _, out _),
            "A live slot head must block the completed queue head (no out-of-order retirement).");
        Assert.AreEqual(0, item);
    }

    [TestMethod]
    public void Claim_AfterEscalation_SlotOccupantStillClaimable()
    {
        // LEAVE-HEAD: post-escalation the slot still holds the pre-escalation occupant, so the
        // slot-tier claim succeeds (the occupant retires through the slot tier).
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        Commit(ref store, 1, Completed, out _);
        Commit(ref store, 2, Completed, out _);  // escalates; item 1 stays in the slot
        Assert.IsTrue(store.TryClaimCompletedSlot(ref tenure, out var item, out _, out _),
            "Post-escalation the slot occupant is still resident and claimable (leave-head).");
        Assert.AreEqual(1, item);
    }

    /// A full handoff lifecycle must leave no activation ownership behind for a later tenure.
    [TestMethod]
    public void ActivationGrantLifecycle_LeavesNoResidueForLaterTurns()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        var gate = new ActivationGate<int>();

        // The empty-edge pass peeks an exact placement, claims its turn, then consumes that same
        // generation.
        var gen = gate.PublishHandoff(7);
        Assert.IsTrue(gate.HasHandoff);
        Assert.IsTrue(gate.TryPeekHandoff(out var peekedGen, out var handedOff));
        Assert.AreEqual(gen, peekedGen);
        Assert.AreEqual(7, handedOff, "The empty-edge pass peeks the published item, not a stale capture.");
        Assert.IsTrue(gate.TryClaimProvisionalTurn(peekedGen), "Fail-if-live turn claim must win on a free turn.");
        Assert.AreEqual(-peekedGen, gate.Turn, "The provisional turn is identity-tagged -gen.");
        Assert.IsTrue(gate.TryTakeHandoff(peekedGen), "The generation-pinned consume must win the unrecycled handoff.");
        Assert.IsTrue(gate.Release(-peekedGen), "The turn's completion releases exactly its -gen.");

        // A later, unrelated item commits at the edge and retires: the turn must assign and
        // release cleanly, without residue from the earlier provisional turn.
        Assert.AreEqual(0, Commit(ref store, 1, Completed, out var isSlot));
        Assert.IsTrue(isSlot);
        Assert.IsFalse(gate.CommitTurn(0, tenure.HeadSequence), "A fresh edge assign must not read as inherited.");
        Assert.IsTrue(store.TryClaimCompletedHead(ref tenure, out var item, out _, out _, out _));
        Assert.AreEqual(1, item);
        Assert.IsTrue(gate.Release(tenure.LastClaimedSequence), "The retiring owner's identity release must win.");
        Assert.IsTrue(store.DecrementCount(), "Single item: the decrement drains to zero.");
    }
}
