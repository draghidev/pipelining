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
        Assert.IsFalse(latch.ReleaseAndCheckPending(), "Uncontended hold must release without a deposit.");
        Assert.IsFalse(latch.IsHeld);
        Assert.IsTrue(latch.TryAcquire(), "Latch should be re-acquirable after release.");
    }

    [TestMethod]
    public void TryAcquireOrFlagPending_OnFreshLatch_Acquires()
    {
        var latch = new Latch();
        Assert.IsTrue(latch.TryAcquireOrFlagPending());
        Assert.IsTrue(latch.IsHeld);
        Assert.IsFalse(latch.ReleaseAndCheckPending(), "No contender, no deposit.");
    }

    [TestMethod]
    public void TryAcquireOrFlagPending_OnHeldLatch_DepositsPending()
    {
        var latch = new Latch();
        Assert.IsTrue(latch.TryAcquire());
        Assert.IsFalse(latch.TryAcquireOrFlagPending(), "Contended acquire must report the deposit, not win.");
        Assert.IsTrue(latch.IsHeld, "Deposit must not free the latch.");
        Assert.IsTrue(latch.ReleaseAndCheckPending(), "The release must consume and report the deposit.");
        Assert.IsFalse(latch.IsHeld);
    }

    [TestMethod]
    public void ReleaseAndCheckPending_ConsumesTheDeposit()
    {
        var latch = new Latch();
        latch.TryAcquire();
        latch.TryAcquireOrFlagPending();
        Assert.IsTrue(latch.ReleaseAndCheckPending());
        // The serve re-acquire starts a fresh hold; the consumed deposit must not re-report.
        Assert.IsTrue(latch.TryAcquireOrFlagPending());
        Assert.IsFalse(latch.ReleaseAndCheckPending(), "A consumed deposit must not survive into the next hold.");
    }

    [TestMethod]
    public void PlainTryAcquire_OnHeldLatch_DoesNotDeposit()
    {
        var latch = new Latch();
        latch.TryAcquire();
        Assert.IsFalse(latch.TryAcquire());
        Assert.IsFalse(latch.ReleaseAndCheckPending(),
            "Plain TryAcquire is the off-protocol form and must not deposit.");
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

    /// Same shape with the depositing form: one winner, and because the winner holds until after
    /// the loop completes, every loser deposits - the winner's release must report it.
    [TestMethod]
    public void ConcurrentTryAcquireOrFlagPending_OneWinner_LosersDeposit()
    {
        const int contenders = 100;
        var box = new LatchBox();
        var winners = 0;

        Parallel.For(0, contenders, _ =>
        {
            if (box.TryAcquireOrFlagPending())
                Interlocked.Increment(ref winners);
        });

        Assert.AreEqual(1, winners);
        Assert.IsTrue(box.IsHeld);
        Assert.IsTrue(box.ReleaseAndCheckPending(),
            "Losers deposited against the winner's hold; the release must report the obligation.");
    }
}

/// Boxing wrapper so the struct field lives on the heap and threads share one instance.
sealed class LatchBox
{
    Latch _latch;
    public bool TryAcquire() => _latch.TryAcquire();
    public bool TryAcquireOrFlagPending() => _latch.TryAcquireOrFlagPending();
    public bool ReleaseAndCheckPending() => _latch.ReleaseAndCheckPending();
    public bool IsHeld => _latch.IsHeld;
}
