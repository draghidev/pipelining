using System.Diagnostics;
using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

[TestClass]
public class ActivationGateTests
{
    [TestMethod]
    public void EmptyEdgeHandoff_IsGenerationPinned()
    {
        var gate = new ActivationGate<int>();
        var generation = gate.PublishHandoff(7);

        Assert.IsTrue(gate.TryPeekHandoff(out var observedGeneration, out var item));
        Assert.AreEqual(generation, observedGeneration);
        Assert.AreEqual(7, item);
        Assert.IsTrue(gate.TryClaimProvisionalTurn(generation));
        Assert.IsTrue(gate.TryTakeHandoff(generation));
        Assert.IsTrue(gate.TryReleaseTurn(-generation));
        Assert.IsFalse(gate.IsTurnOwned);
        Assert.IsFalse(gate.HasHandoff);
    }

    [TestMethod]
    public void RecycledHandoff_RejectsStaleGeneration()
    {
        var gate = new ActivationGate<int>();
        var first = gate.PublishHandoff(1);
        Assert.IsTrue(gate.TryTakeHandoff());
        var second = gate.PublishHandoff(2);

        Assert.IsFalse(gate.TryTakeHandoff(first));
        Assert.IsTrue(gate.TryPeekHandoff(out var observedGeneration, out var item));
        Assert.AreEqual(second, observedGeneration);
        Assert.AreEqual(2, item);
    }

    [TestMethod]
    public async Task ContendedOwnedCommit_WaitsForDoomedEmptyEdgeClaim()
    {
        var gate = new ActivationGate<int>();
        var edgeLock = gate.EdgeLock;

        var oldGeneration = gate.PublishHandoff(1);
        edgeLock.Enter();
        Assert.IsTrue(gate.TryClaimProvisionalTurn(oldGeneration));

        // The executor consumes after the empty-edge pass claimed but before its pinned consume.
        Assert.IsTrue(gate.TryTakeHandoff());
        var currentGeneration = gate.PublishHandoff(2);
        Assert.IsTrue(gate.TryTakeHandoff());

        var commitStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var commit = Task.Run(() =>
        {
            commitStarted.SetResult();
            edgeLock.Enter();
            try
            {
                return gate.CommitTurn(currentGeneration, sequence: 1);
            }
            finally
            {
                edgeLock.Exit();
            }
        });

        await commitStarted.Task;
        await Task.Delay(25);
        Assert.IsFalse(commit.IsCompleted, "The commit must not overwrite the pass's live old-generation claim.");

        Assert.IsTrue(gate.TryReleaseTurn(-oldGeneration));
        edgeLock.Exit();

        Assert.IsFalse(await commit, "The current generation was executor-owned, so commit assigns a fresh resident turn.");
        Assert.AreEqual(1, gate.TurnOwner);
        Assert.IsTrue(gate.TryReleaseTurn(1));
    }

    [TestMethod]
    public void RecoveryCommit_InheritsItsGrantedGeneration()
    {
        var store = new InFlightStore<int>();
        var tenure = new ItemTenure();
        var gate = new ActivationGate<int>();

        var recoveryGeneration = gate.PublishHandoff(7);
        Assert.IsTrue(gate.TryClaimProvisionalTurn(recoveryGeneration));
        Assert.IsTrue(gate.TryTakeHandoff(recoveryGeneration));

        Assert.AreEqual(0, store.IncrementCommitCount());
        Assert.IsTrue(gate.CommitTurn(recoveryGeneration, tenure.NextSequence),
            "The recovery commit must carry its generation so the provisional turn becomes resident ownership.");
        store.PublishCommitted(7, default, out _);

        Assert.IsTrue(store.TryClaimCompletedHead(ref tenure, out var item, out _, out var claimedSequence, out _));
        Assert.AreEqual(7, item);
        Assert.AreEqual(1, claimedSequence);
        Assert.IsTrue(gate.TryReleaseTurn(claimedSequence));
        Assert.IsTrue(store.DecrementCount());
    }

    [TestMethod]
    public void Reset_ClearsDirtyOwnershipWithoutReusingGeneration()
    {
        var gate = new ActivationGate<int>();
        var generation = gate.PublishHandoff(1);
        Assert.IsTrue(gate.TryClaimProvisionalTurn(generation));

        Assert.ThrowsExactly<UnreachableException>(gate.EnsureIdle);
        gate.Reset();

        gate.EnsureIdle();
        Assert.IsFalse(gate.IsTurnOwned);
        Assert.IsFalse(gate.HasHandoff);
        Assert.IsGreaterThan(generation, gate.PublishHandoff(2));
    }
}
