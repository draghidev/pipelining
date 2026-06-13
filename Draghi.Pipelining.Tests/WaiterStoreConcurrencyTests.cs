using Draghi.Pipelining;

namespace Draghi.Pipelining.Tests;

/// Targeted regression guard for the slot field/flag tear (backlog #7). The tear was a claim
/// winning between a commit's flag publish and its field writes, reading a stale/default or torn
/// pair. The tri-state word fixed it (data-then-license commit, peek-gated claim); these races
/// hammer exactly that window at the WaiterStore boundary - a far tighter loop than the full
/// pipeline stress, so a reintroduced tear surfaces fast as a default(0) or a lost/duplicated
/// marker. DRAGHI_STRESS_ITERATIONS overrides the iteration count.
[TestClass, DoNotParallelize]
public class WaiterStoreConcurrencyTests
{
    static int Iterations =>
        int.TryParse(Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 500_000;

    /// Slot-tier churn: the producer commits a marker then waits (via Count) for the claimer to
    /// drain it before committing the next, so the store never escalates and every iteration is a
    /// fresh slot tenancy. The claimer spins on TryClaimSlotForDrain, so it is mid-peek exactly
    /// when the producer publishes - the tear window. Markers are strictly sequential under the
    /// handshake, so a torn claim shows as a non-sequential or zero value.
    [TestMethod]
    public void SlotClaimRace_NoTornOrStaleClaims()
    {
        var box = new WaiterStoreBox<int>();
        var n = Iterations;
        Exception? failure = null;

        var claimer = new Thread(() =>
        {
            try
            {
                for (var expected = 1; expected <= n; expected++)
                {
                    int item;
                    while (!box.TryClaimSlotForDrain(out item, out _))
                        Thread.SpinWait(1);
                    Assert.AreNotEqual(0, item, "Torn/stale claim: read a default(T) mid-commit.");
                    Assert.AreEqual(expected, item, "Torn/stale claim: read the wrong tenancy's value.");
                    box.DecrementCount();
                }
            }
            catch (Exception ex) { failure = ex; }
        });
        claimer.Start();

        for (var marker = 1; marker <= n; marker++)
        {
            box.TryEscalateOrEnqueue(marker, default, out var isSlot, out _);
            Assert.IsTrue(isSlot, "Handshake must keep every commit slot-tier (no escalation).");
            while (box.Count != 0 && failure is null)
                Thread.SpinWait(1);
            if (failure is not null)
                break;
        }

        claimer.Join();
        if (failure is not null)
            throw new AssertFailedException("Slot claim race failed.", failure);
    }

    /// Escalation-vs-claim: each round is a fresh store driven through its single slot->queue
    /// transition while a claimer races both tiers. Exercises the quiescent-only escalation CAS
    /// (a consuming slot must read as not-moved, never stolen) and the licensed claim across the
    /// boundary. Every committed marker must be drained exactly once, never default.
    [TestMethod]
    public void SlotEscalationRace_NoLostOrDuplicatedItems()
    {
        const int perRound = 3;  // slot + two queue entries, forcing the slot->escalation move
        var rounds = Math.Max(1, Iterations / 50);

        for (var r = 0; r < rounds; r++)
        {
            var box = new WaiterStoreBox<int>();
            var drained = new bool[perRound + 1];
            var log = new List<string>(perRound);  // claimer-thread-only; visible after Join
            Exception? failure = null;

            var claimer = new Thread(() =>
            {
                try
                {
                    var count = 0;
                    while (count < perRound)
                    {
                        if (box.TryClaimSlotForDrain(out var slotItem, out _))
                        {
                            log.Add($"slot:{slotItem}");
                            Record(drained, slotItem);
                            box.DecrementCount();
                            count++;
                        }
                        else if (box.TryDequeue(out var entry))
                        {
                            log.Add($"queue:{entry.Waiter}");
                            Record(drained, entry.Waiter);
                            box.DecrementCount();
                            count++;
                        }
                        else
                        {
                            Thread.SpinWait(1);
                        }
                    }
                }
                catch (Exception ex) { failure = ex; }
            });
            claimer.Start();

            var commitLog = new List<string>(perRound);
            for (var marker = 1; marker <= perRound; marker++)
            {
                box.TryEscalateOrEnqueue(marker, default, out var isSlot, out var slotWasMoved);
                commitLog.Add($"{marker}:{(isSlot ? "slot" : "queue")}{(slotWasMoved ? "+moved" : "")}");
            }

            claimer.Join();
            var trace = $"commits=[{string.Join(",", commitLog)}] drains=[{string.Join(",", log)}] finalCount={box.Count}";
            if (failure is not null)
                throw new AssertFailedException($"Escalation race failed at round {r}. {trace}", failure);
            for (var marker = 1; marker <= perRound; marker++)
                Assert.IsTrue(drained[marker], $"Round {r}: marker {marker} was lost. {trace}");
        }
    }

    static void Record(bool[] drained, int marker)
    {
        Assert.AreNotEqual(0, marker, "Torn/stale drain: read a default(T).");
        Assert.IsFalse(drained[marker], $"Duplicate drain of marker {marker}.");
        drained[marker] = true;
    }
}

/// Boxes the WaiterStore struct so racing threads share one instance (the producer commits, the
/// consumer claims/dequeues against the same storage).
sealed class WaiterStoreBox<T>
{
    WaiterStore<T> _store;
    public int Count => _store.Count;
    public int TryEscalateOrEnqueue(T item, ValueTask task, out bool isSlot, out bool slotWasMoved)
        => _store.TryEscalateOrEnqueue(item, task, out isSlot, out slotWasMoved);
    public bool TryClaimSlotForDrain(out T item, out ValueTask task) => _store.TryClaimSlotForDrain(out item, out task);
    public bool TryDequeue(out (T Waiter, ValueTask WaiterTask) entry) => _store.TryDequeue(out entry);
    public int DecrementCount() => _store.DecrementCount();
}
