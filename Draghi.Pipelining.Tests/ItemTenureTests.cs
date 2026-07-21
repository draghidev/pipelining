using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

[TestClass]
public class ItemTenureTests
{
    [TestMethod]
    public void ClaimHead_AdvancesIdentityExactlyOncePerClaim()
    {
        var tenure = new ItemTenure();

        Assert.AreEqual(1, tenure.HeadSequence);
        Assert.AreEqual(1, tenure.ClaimHead());
        Assert.AreEqual(1, tenure.LastClaimedSequence);
        Assert.AreEqual(2, tenure.HeadSequence);
        Assert.AreEqual(2, tenure.ClaimHead());
    }

    [TestMethod]
    public void CompletionCallbackArm_BlocksOnlyItsHeadUntilDelivery()
    {
        var tenure = new ItemTenure();

        tenure.ArmCompletionCallback(tenure.HeadSequence);
        Assert.IsTrue(tenure.IsCompletionCallbackPendingForHead());

        tenure.MarkCompletionCallbackDelivered();
        Assert.IsFalse(tenure.IsCompletionCallbackPendingForHead());
    }
}
