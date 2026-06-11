namespace Draghi.Pipelining.Tests;

[TestClass]
public class LatchConcurrencyTests
{
    /// Publish-then-acquire pattern with obligation transfer, mirroring the advancer claim in
    /// Pipeline.DrainReadyWaiters: a producer whose acquire loses deposits in the latch word,
    /// and the holder's release consumes the deposit and re-acquires to serve. Every publish
    /// must be drained by some caller WITHOUT any out-of-band recheck: unlike the old two-cell
    /// protocol (separate latch + pending flag, where strand prevention needed a post-release
    /// re-read of the flag and still had the lost-wake window against transient holds), the
    /// single word makes deposit-vs-release atomic. A stranded publish at steady state means
    /// the word primitives lost a deposit: suspect the CAS loop in TryAcquireOrFlagPending or
    /// the Exchange in ReleaseAndCheckPending.
    [TestMethod]
    public void PublishThenAcquireOrDeposit_NoStrandedPublishesUnderConcurrentProducers()
    {
        var box = new LatchBox();
        var pending = new int[1];  // array so closure captures the reference, not the slot
        var drained = new int[1];
        const int producers = 4;
        const int perProducer = 200_000;

        void Producer()
        {
            for (var i = 0; i < perProducer; i++)
            {
                Interlocked.Increment(ref pending[0]);
                if (!box.TryAcquireOrFlagPending())
                    continue;  // obligation deposited; the holder serves
                bool obligated;
                do
                {
                    Interlocked.Add(ref drained[0], Interlocked.Exchange(ref pending[0], 0));
                    obligated = box.ReleaseAndCheckPending();
                } while (obligated && box.TryAcquireOrFlagPending());
            }
        }

        var tasks = new Task[producers];
        for (var t = 0; t < producers; t++)
            tasks[t] = Task.Run(Producer);
        Task.WaitAll(tasks);

        Assert.IsFalse(box.IsHeld, "Latch should be free after all producers exit.");
        var finalPending = Volatile.Read(ref pending[0]);
        Assert.AreEqual(0, finalPending,
            $"Stranded publishes: pending={finalPending} remains after all producers exited.");
        Assert.AreEqual(producers * perProducer, drained[0]);
    }
}
