using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// The edge lock's two contracts: mutual exclusion (the counter tests would tear without it)
/// and no lost wake (the exit's RMW must observe the waiter bit; a plain store could strand a
/// parked rival with the lock free - the ping-pong wedges if that regresses).
[TestClass]
public class EdgeLockTests
{
    [TestMethod]
    public void MutualExclusion_ContendedCounterIsExact()
    {
        var edgeLock = new EdgeLock();
        var counter = 0;
        const int Threads = 8;
        const int PerThread = 200_000;
        var start = new ManualResetEventSlim(false);
        var workers = new Thread[Threads];
        for (var t = 0; t < Threads; t++)
        {
            workers[t] = new Thread(() =>
            {
                start.Wait();
                for (var i = 0; i < PerThread; i++)
                {
                    edgeLock.Enter();
                    counter++;
                    edgeLock.Exit();
                }
            });
            workers[t].Start();
        }
        start.Set();
        foreach (var w in workers)
            Assert.IsTrue(w.Join(TimeSpan.FromSeconds(60)), "worker wedged under contention");
        Assert.AreEqual(Threads * PerThread, counter);
    }

    [TestMethod]
    public void Exit_WakesParkedWaiter()
    {
        var edgeLock = new EdgeLock();
        edgeLock.Enter();
        var acquired = new ManualResetEventSlim(false);
        var contender = new Thread(() =>
        {
            edgeLock.Enter();
            acquired.Set();
            edgeLock.Exit();
        });
        contender.Start();
        // Let the contender reach the parked state (the wait handle allocates lazily on this
        // first contention). A fast-path win after the release is equally correct, so the only
        // hard assertions are exclusion-while-held and the eventual wake.
        Thread.Sleep(50);
        Assert.IsFalse(acquired.IsSet, "contender acquired while the lock was held");
        edgeLock.Exit();
        Assert.IsTrue(acquired.Wait(TimeSpan.FromSeconds(10)), "parked waiter never woke");
        Assert.IsTrue(contender.Join(TimeSpan.FromSeconds(10)));
    }

    [TestMethod]
    public void ParkedWake_HandsOffThroughManyCycles()
    {
        var edgeLock = new EdgeLock();
        var counter = 0;
        const int Cycles = 2_000;
        void Body()
        {
            for (var i = 0; i < Cycles; i++)
            {
                edgeLock.Enter();
                counter++;
                if (i % 64 == 0)
                    Thread.Yield();
                edgeLock.Exit();
            }
        }
        var a = new Thread(Body);
        var b = new Thread(Body);
        a.Start();
        b.Start();
        Assert.IsTrue(a.Join(TimeSpan.FromSeconds(30)) && b.Join(TimeSpan.FromSeconds(30)),
            "ping-pong wedged (lost wake)");
        Assert.AreEqual(2 * Cycles, counter);
    }

    [TestMethod]
    public async Task Contention_AcrossThreadPool()
    {
        var edgeLock = new EdgeLock();
        var counter = 0;
        var tasks = new Task[16];
        for (var t = 0; t < tasks.Length; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < 50_000; i++)
                {
                    edgeLock.Enter();
                    counter++;
                    edgeLock.Exit();
                }
            });
        }
        var all = Task.WhenAll(tasks);
        var done = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(60)));
        Assert.AreSame(all, done, "thread-pool contention wedged");
        await all;
        Assert.AreEqual(tasks.Length * 50_000, counter);
    }
}
