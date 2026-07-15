using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

[TestClass]
public class ItemTenureTests
{
    [TestMethod]
    public void ClaimHead_AdvancesIdentityExactlyOncePerClaim()
    {
        var tenure = new ItemTenure();

        Assert.AreEqual(1, tenure.HeadSeq);
        Assert.AreEqual(1, tenure.ClaimHead());
        Assert.AreEqual(1, tenure.LastClaimedSeq);
        Assert.AreEqual(2, tenure.HeadSeq);
        Assert.AreEqual(2, tenure.ClaimHead());
    }

    [TestMethod]
    public void CompletionCallbackArm_BlocksOnlyItsHeadUntilDelivery()
    {
        var tenure = new ItemTenure();

        tenure.ArmCompletionCallback(tenure.HeadSeq);
        Assert.IsTrue(tenure.IsCompletionCallbackPendingForHead());

        tenure.MarkCompletionCallbackDelivered();
        Assert.IsFalse(tenure.IsCompletionCallbackPendingForHead());
    }
}
