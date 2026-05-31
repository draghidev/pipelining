using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

[TestClass]
public class LatchTests
{
    [TestMethod]
    public void FreshLatch_IsNotHeld()
    {
        var latch = new Latch();
        Assert.IsFalse(latch.IsHeld);
    }

    [TestMethod]
    public void TryAcquire_OnFreshLatch_Succeeds()
    {
        var latch = new Latch();
        Assert.IsTrue(latch.TryAcquire());
        Assert.IsTrue(latch.IsHeld);
    }

    [TestMethod]
    public void TryAcquire_OnHeldLatch_Fails()
    {
        var latch = new Latch();
        Assert.IsTrue(latch.TryAcquire());
        Assert.IsFalse(latch.TryAcquire(), "Second acquire on a held latch must return false.");
        Assert.IsTrue(latch.IsHeld);
    }

    [TestMethod]
    public void Release_ReturnsToUnheld()
    {
        var latch = new Latch();
        latch.TryAcquire();
        latch.Release();
        Assert.IsFalse(latch.IsHeld);
        Assert.IsTrue(latch.TryAcquire(), "Latch should be re-acquirable after release.");
    }

    [TestMethod]
    public void Release_OnUnheldLatch_IsNoOp()
    {
        var latch = new Latch();
        latch.Release();  // no exception, no state change
        Assert.IsFalse(latch.IsHeld);
        Assert.IsTrue(latch.TryAcquire());
    }

    /// Under concurrent acquire attempts, exactly one caller wins. The 99 losers see false. Their
    /// returns are immediate (no blocking).
    [TestMethod]
    public void ConcurrentTryAcquire_ExactlyOneWins()
    {
        const int contenders = 100;
        var box = new LatchBox();  // boxes the struct so threads share one instance
        var winners = 0;

        Parallel.For(0, contenders, _ =>
        {
            if (box.TryAcquire())
                Interlocked.Increment(ref winners);
        });

        Assert.AreEqual(1, winners, "Exactly one contender should acquire under concurrent TryAcquire.");
        Assert.IsTrue(box.IsHeld);
    }

}

/// Boxing wrapper so the struct field lives on the heap and threads share one instance.
sealed class LatchBox
{
    Latch _latch;
    public bool TryAcquire() => _latch.TryAcquire();
    public void Release() => _latch.Release();
    public bool IsHeld => _latch.IsHeld;
}
