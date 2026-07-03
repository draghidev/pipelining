using Draghi.Pipelining.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Draghi.Pipelining.Tests;

/// <summary>
/// Stress tests targeting WakeSignal in isolation - no Pipeline, no Enumerator CTS chain, no
/// source. Mirrors the State.WaitForNextAsync arm/await/wake protocol with the minimum surface so a
/// failure pins the defect on WakeSignal itself (or refutes that hypothesis when the wider
/// PipelineConcurrencyTests stress fails but this one passes).
/// </summary>
[TestClass]
public class WakeSignalConcurrencyTests
{
    /// <summary>
    /// Races Complete() against an in-flight consumer that's mid-arm: AcquireLock, peek (empty),
    /// Arm, await. The hypothesis under test is the corruption shape where
    /// cancellation lands while the consumer's state
    /// machine is between Arm and OnCompleted, leaving _pending = TRUE with _waitContinuation
    /// null. A subsequent SignalCore claim dispatches a null continuation (NRE).
    ///
    /// Iterations via DRAGHI_STRESS_ITERATIONS (default 200). On a hit, the test fails with the
    /// captured NRE; on hang, it fails the 5s timeout.
    /// </summary>
    [TestMethod, DoNotParallelize]
    public async Task ArmRacingComplete_NoNullDispatch_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var iter = 0; iter < iterations; iter++)
        {
            var signal = new WakeSignal(runContinuationsAsynchronously: true, PipelineScheduler.ThreadPool);
            var queue = new int[1]; // single-slot pseudo-queue; "has item" = volatile read != 0
            var enqueued = 0;

            // Consumer mirrors the State.WaitForNextAsync shape: lock, peek, completion check, arm.
            var consumerTask = Task.Run(async () =>
            {
                while (true)
                {
                    signal.AcquireWakeLock();
                    if (Volatile.Read(ref enqueued) != 0)
                    {
                        Volatile.Write(ref enqueued, 0);
                        signal.ReleaseWakeLock();
                        return true;
                    }
                    if (signal.IsCompleted)
                    {
                        signal.ReleaseWakeLock();
                        return false;
                    }
                    await signal.Arm();
                }
            });

            // Two competing wakers: a producer that sets the slot + signals, and a canceller that
            // calls Complete(). Both eventually unblock the consumer. The race window is
            // arm-then-await: if Complete claims _pending while the consumer hasn't registered yet,
            // the dispatch path NREs.
            var spinTarget = (iter * 13) % 128;
            var producerTask = Task.Run(() =>
            {
                for (var s = 0; s < spinTarget; s++)
                    Thread.SpinWait(8);
                Volatile.Write(ref enqueued, 1);
                signal.Signal();
            });

            var cancellerTask = Task.Run(() =>
            {
                for (var s = 0; s < (spinTarget ^ 64); s++)
                    Thread.SpinWait(8);
                signal.Complete();
            });

            string? diagnosis = null;
            try
            {
                await Task.WhenAll(consumerTask, producerTask, cancellerTask)
                    .WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                diagnosis = $"iter {iter}: stuck - WakeSignal wedged. " +
                    $"consumer={consumerTask.Status} producer={producerTask.Status} canceller={cancellerTask.Status}";
            }
            catch (NullReferenceException ex)
            {
                diagnosis = $"iter {iter}: NRE - DispatchClaimed saw null _waitContinuation. " +
                    $"{ex.Message}\n{ex.StackTrace}";
            }
            catch (AggregateException ae)
            {
                diagnosis = $"iter {iter}: aggregate - {ae.Flatten().InnerException}";
            }

            if (diagnosis is not null)
                Assert.Fail(diagnosis);
        }
    }

    /// <summary>
    /// Tighter variant: no producer signal at all. Just consumer arms and canceller completes.
    /// Isolates the cancel-during-arm window from any concurrent Signal contention. If this hits
    /// the NRE, the bug is fully inside the (Arm, WaitOnCompleted, Complete) trio.
    /// </summary>
    [TestMethod, DoNotParallelize]
    public async Task ArmRacingCompleteOnly_NoNullDispatch_Stress()
    {
        var iterations = int.TryParse(
            Environment.GetEnvironmentVariable("DRAGHI_STRESS_ITERATIONS"), out var n) ? n : 200;

        for (var iter = 0; iter < iterations; iter++)
        {
            var signal = new WakeSignal(runContinuationsAsynchronously: true, PipelineScheduler.ThreadPool);

            var consumerTask = Task.Run(async () =>
            {
                while (true)
                {
                    signal.AcquireWakeLock();
                    if (signal.IsCompleted)
                    {
                        signal.ReleaseWakeLock();
                        return false;
                    }
                    await signal.Arm();
                }
            });

            var spinTarget = (iter * 17) % 256;
            var cancellerTask = Task.Run(() =>
            {
                for (var s = 0; s < spinTarget; s++)
                    Thread.SpinWait(4);
                signal.Complete();
            });

            string? diagnosis = null;
            try
            {
                await Task.WhenAll(consumerTask, cancellerTask)
                    .WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                diagnosis = $"iter {iter}: stuck - consumer={consumerTask.Status} canceller={cancellerTask.Status}";
            }
            catch (NullReferenceException ex)
            {
                diagnosis = $"iter {iter}: NRE on cancel-only path. {ex.Message}\n{ex.StackTrace}";
            }
            catch (AggregateException ae)
            {
                diagnosis = $"iter {iter}: aggregate - {ae.Flatten().InnerException}";
            }

            if (diagnosis is not null)
                Assert.Fail(diagnosis);
        }
    }
}
