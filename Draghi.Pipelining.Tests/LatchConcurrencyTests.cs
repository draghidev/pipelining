namespace Draghi.Pipelining.Tests;

[TestClass]
public class LatchConcurrencyTests
{
    /// Publish-then-try-acquire pattern with a do-while reclaim drainer, mirroring the advancer
    /// claim in Pipeline.DrainReadyWaiters (Release, then re-read pending and re-acquire if work
    /// remains). Every publish must be drained by some caller. Strand prevention rests on BOTH
    /// pieces: Release's seq-cst fence AND the post-release re-check. A single-shot drainer (drop
    /// the do-while) strands publishes even with a correct Latch, via a fence-independent window
    /// (an increment landing after the holder's drain-Exchange but before its Release, on the
    /// incrementer's final iteration), so it cannot isolate the fence. With the re-check in place,
    /// a non-zero pending count at steady state means a producer's TryAcquire bailed on stale state,
    /// or the reread floated above the Release: suspect Latch.Release's seq-cst semantics.
    [TestMethod]
    public void PublishThenTryAcquire_NoStrandedPublishesUnderConcurrentProducers()
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
                if (!box.TryAcquire())
                    continue;
                do
                {
                    Interlocked.Add(ref drained[0], Interlocked.Exchange(ref pending[0], 0));
                    box.Release();
                } while (Volatile.Read(ref pending[0]) != 0 && box.TryAcquire());
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
