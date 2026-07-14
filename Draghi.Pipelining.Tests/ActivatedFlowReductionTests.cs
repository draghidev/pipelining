using System.Diagnostics.CodeAnalysis;

namespace Draghi.Pipelining.Tests;

// Reduction for the abort observed in Slon at PgDecoder.cs:26
// (Debug.Assert(_control.ActivatedFlow is not null)). Hypothesis (per Agent 1):
//   The framework's per-item ordering (Activate(i)-before-Complete(i)) is preserved.
//   What is NOT preserved is cross-item ordering between Complete(A) and Activate(B)
//   when Activate(B) is dispatched to TP via UnsafeQueueUserWorkItem (matching the
//   protocol's Policy.ActivateHeadItem at PgClientProtocol.cs:496-502).
//
// We synthesise a policy that:
//   - holds a single `_activated` field analogous to PgClientProtocol.Control.ActivatedFlow.
//   - sets it in ActivateHeadItem.
//   - nulls it in CompleteItem.
//   - reads it (and asserts non-null) at a configurable check site.
// Then drives the pipeline with hundreds of items so the framework's CommitWaiter
// (Pipeline.cs:744) and DrainSlotInline (Pipeline.cs:856) preferAsync=true paths fire.
//
// Two variants: INLINE activation and TP-dispatched activation. If only the second
// reproduces the assert, the framework's contract is honoured and the bug is in the
// off-thread activation hop the protocol introduced.
[TestClass]
public class ActivatedFlowReductionTests
{
    sealed class SyntheticItem
    {
        public int Id { get; init; }
        // RunContinuationsAsynchronously: framework's CompleteItem continuation (registered
        // via UnsafeOnCompleted by the pipeline waiter machinery) gets TP-queued instead of
        // running inline on whoever called SetResult. This is what makes Complete(prev)
        // and Activate(next) end up on DIFFERENT threads in the real protocol where
        // pipeline-task completions and ActivateHeadItem work items each schedule onto TP
        // independently and race.
        public TaskCompletionSource PipelineTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ValueTask GetPipelineTask() => new(PipelineTcs.Task);

        // Tenure-reuse support: models PgClientFlow.Reset() + re-enqueue of the SAME instance
        // (MaintenanceFlow / pooled-flow pattern). Tenures of one item are strictly sequential
        // (the re-enqueue happens inside the previous tenure's completion), so plain fields.
        public int RemainingTenures;
        public Tenure CurrentTenure = new();
        public void Reset()
        {
            PipelineTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            CurrentTenure = new();
        }
    }

    // One activation cycle of an item. The read gate: a decoder read only executes during its
    // own tenure's drain, i.e. before that tenure's CompleteItem begins. A read that observes
    // a null slot while its tenure is still mid-drain is a REAL hazard; a read straddling its
    // own completion is suppressed. This is race-free where the old activeCount gate was not
    // (count could be sampled before a completion's decrement and the slot after that same
    // completion's null, yielding false positives).
    sealed class Tenure
    {
        public volatile bool Completed;
    }

    // Mutable indirection so the policy (copied by value into the pipeline) can re-enqueue
    // into the pipeline it belongs to.
    sealed class PipelineHolder
    {
        public QueuedPipeline<SyntheticItem, ProbePolicy>? Pipeline;
    }

    enum NullPolicy
    {
        AlwaysNull,           // baseline: ActivatedFlow = null in Complete unconditionally
        DepthZeroOnly,        // null only when remainingDepth == 0 (assignment)
        DepthZeroWithCas,     // null only at depth=0, via CAS that fails on overwrite
        NeverNull,            // never null - rely on next Activate to overwrite
    }

    sealed class ActivationProbe
    {
        SyntheticItem? _activated;
        public int Asserts;
        public int Activations;
        public int Completions;
        public int PendingReads;
        // Tenure-reuse diagnostics: how often the re-enqueued tenure's activation landed
        // inside the completion window vs timed out. All-timeouts means the framework
        // serialized activation behind CompleteItem and a pass proves nothing about the race.
        public int ActivationsLandedInWindow;
        public int WaitTimeouts;
        // Direct framework-level evidence: the null policy fired while the re-enqueued
        // tenure's activation had already landed - a live binding was nulled. Counted on the
        // completing thread (no read-timing coincidence needed); each one is a latent abort
        // in the real protocol where the live flow's next decoder read derefs the binding.
        public int LiveNulls;
        public NullPolicy Policy = NullPolicy.AlwaysNull;

        public void Activate(SyntheticItem item)
        {
            Interlocked.Increment(ref Activations);
            Volatile.Write(ref _activated, item);
        }

        // newTenureIsLive: the caller observed the re-enqueued tenure's activation land
        // before this call (same thread, no race). If the null policy still fires, it just
        // severed a live binding.
        public void Complete(SyntheticItem item, int remainingDepth, bool newTenureIsLive = false)
        {
            Interlocked.Increment(ref Completions);
            switch (Policy)
            {
                case NullPolicy.AlwaysNull:
                    Volatile.Write(ref _activated, null);
                    if (newTenureIsLive)
                        Interlocked.Increment(ref LiveNulls);
                    break;
                case NullPolicy.DepthZeroOnly:
                    if (remainingDepth is 0)
                    {
                        Volatile.Write(ref _activated, null);
                        if (newTenureIsLive)
                            Interlocked.Increment(ref LiveNulls);
                    }
                    break;
                case NullPolicy.DepthZeroWithCas:
                    if (remainingDepth is 0
                        && Interlocked.CompareExchange(ref _activated, null, item) == item
                        && newTenureIsLive)
                        Interlocked.Increment(ref LiveNulls);
                    break;
                case NullPolicy.NeverNull:
                    break;
            }
        }

        // The protocol's CurrentExecutionControl read - the assert site. A read executes on
        // behalf of a specific tenure and only asserts if the slot is null while that tenure
        // is still mid-drain. Mirrors the decoder: reads only happen during the activated
        // flow's drain, so a null observed then is a genuine binding loss.
        //
        // Read order matters: slot FIRST, own-tenure flag SECOND. Complete() sets the flag
        // before applying the null policy (both release-ordered), so a reader that observed
        // a null produced by its OWN completion must subsequently observe Completed==true
        // (acquire on the null read). A null with Completed==false is therefore a null
        // produced by a FOREIGN tenure's completion while this tenure is mid-drain - the
        // real hazard. Flag-first would straddle: flag sampled before the completion, slot
        // sampled after its null, false-positively blaming a clean ordering.
        public void Read(Tenure tenure)
        {
            if (Volatile.Read(ref _activated) is null && !tenure.Completed)
                Interlocked.Increment(ref Asserts);
            Interlocked.Decrement(ref PendingReads);
        }

        public void RegisterPendingRead() => Interlocked.Increment(ref PendingReads);
    }

    struct ProbePolicy : IPipelinePolicy<SyntheticItem>
    {
        readonly ActivationProbe _probe;
        readonly bool _activateOnTp;
        readonly PipelineHolder? _holder;
        readonly bool _completionActionFirst;

        public ProbePolicy(ActivationProbe probe, bool activateOnTp, PipelineHolder? holder = null, bool completionActionFirst = false)
        {
            _probe = probe;
            _activateOnTp = activateOnTp;
            _holder = holder;
            _completionActionFirst = completionActionFirst;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(SyntheticItem item, CancellationToken cancellationToken)
            => new(new PipelineItemResult(default, item.GetPipelineTask()));

        public void ActivateHeadItem(SyntheticItem item, bool preferAsync = true)
        {
            if (_activateOnTp && preferAsync)
            {
                _probe.RegisterPendingRead();
                ThreadPool.UnsafeQueueUserWorkItem(static state =>
                {
                    var (probe, item) = ((ActivationProbe, SyntheticItem))state!;
                    probe.Activate(item);
                    // Schedule the Read for "later" on a separate TP thread - do NOT
                    // block. The pipelineTcs.SetResult fires immediately, so the
                    // framework's CompleteItem chain races with this scheduled Read.
                    // Mirrors the real protocol where the decoder's batch await on the
                    // socket thread can fire its continuation (the Read at line 187)
                    // arbitrarily later, after other items' Completes have interleaved.
                    ThreadPool.UnsafeQueueUserWorkItem(static s =>
                    {
                        var (p, t) = ((ActivationProbe, Tenure))s!;
                        p.Read(t);
                    }, (probe, item.CurrentTenure), preferLocal: false);
                    item.PipelineTcs.TrySetResult();
                }, (_probe, item), preferLocal: true);
            }
            else if (_holder is not null)
            {
                // Tenure-reuse mode, inline activation (the executor's _waiters.Count==0 path
                // calls ActivateHeadItem with preferAsync:false, Pipeline.cs:237). Model the
                // real flow shape: the decoder keeps reading AFTER activation (async Read),
                // and the pipeline task completes from a socket continuation, never inline in
                // ExecuteItemAsync (async TrySetResult). A synchronous TrySetResult here would
                // take the executor's sync-completion shortcut and run CompleteItem on the
                // executor thread itself, serializing away the cross-thread race.
                _probe.Activate(item);
                // The real decoder reads many times across a drain; several scattered reads
                // sample the (potential) null window instead of a single coin flip per tenure.
                for (var r = 0; r < 4; r++)
                {
                    _probe.RegisterPendingRead();
                    ThreadPool.UnsafeQueueUserWorkItem(static s =>
                    {
                        var (p, t) = ((ActivationProbe, Tenure))s!;
                        p.Read(t);
                    }, (_probe, item.CurrentTenure), preferLocal: false);
                }
                ThreadPool.UnsafeQueueUserWorkItem(static s => ((SyntheticItem)s!).PipelineTcs.TrySetResult(), item, preferLocal: false);
            }
            else
            {
                _probe.Activate(item);
                _probe.Read(item.CurrentTenure);
                item.PipelineTcs.TrySetResult();
            }
        }

        public void CompleteItem(SyntheticItem item, int remainingDepth, Exception? exception)
        {
            // Mirrors Slon's Policy.CompleteItem ordering question. In the real protocol,
            // ExecutionControl.Complete() fires the flow's completion action (which for
            // MaintenanceFlow does Reset + re-enqueue of the SAME instance, synchronously)
            // and only THEN does Control.OnCompleted run its depth==0
            // CompareExchange(ref _activatedFlow, null, flow). If the re-enqueued tenure's
            // Activate(item) lands between those two, the CAS comparand matches the NEW
            // activation of the same instance (ABA) and nulls a live flow.
            // Mark the COMPLETING tenure's drain over at entry, before either ordering branch:
            // the real flow's reads end when its drain ends, which precedes the whole
            // CompleteItem chain. Capturing the token here matters - in completion-action-first mode the
            // completion action's Reset() replaces item.CurrentTenure, so marking later would
            // stamp the NEW tenure (mid-drain!) and suppress exactly the read that catches
            // the ABA.
            item.CurrentTenure.Completed = true;
            if (_completionActionFirst)
            {
                var newTenureIsLive = FireCompletionAction(item, remainingDepth);
                _probe.Complete(item, remainingDepth, newTenureIsLive);
            }
            else
            {
                _probe.Complete(item, remainingDepth);
                FireCompletionAction(item, remainingDepth);
            }
        }

        // Returns true if the re-enqueued tenure's activation was observed to land before
        // returning (i.e. a subsequent null policy application severs a live binding).
        bool FireCompletionAction(SyntheticItem item, int remainingDepth)
        {
            if (_holder is null || item.RemainingTenures <= 0)
                return false;
            item.RemainingTenures--;
            item.Reset();
            var activationsBefore = Volatile.Read(ref _probe.Activations);
            _holder.Pipeline!.Enqueue(item).Execute();
            // Only the depth==0 completion runs the null policy, so only there can the
            // re-enqueued tenure's activation race it - and only there CAN it activate
            // promptly (empty pipeline, executor takes the Count==0 inline path). Waiting at
            // depth>0 is pure timeout burn: the item can't reach head until the rest drains.
            // The wait also only exists to widen the PRE-null window, which the safe
            // (release-before-complete) ordering does not have - the null already happened
            // before this method ran. Skipping it there removes the suite's long pole
            // without weakening the regression property (the hazard variants keep it).
            if (remainingDepth is not 0 || !_completionActionFirst)
                return false;
            // Model the completing thread being preempted between the completion action and
            // Control.OnCompleted: give the re-enqueued tenure's activation a chance to land
            // first. This is an honest probe of the ordering contract - if the framework
            // serialized head activation behind CompleteItem, this wait would always time out
            // (see WaitTimeouts) and the test would pass regardless of null policy. Bounded so
            // a serializing framework can't deadlock the advancer latch held by our caller.
            // 2ms cap: when the activation CAN land in-window it lands in microseconds (the
            // executor inline-activates off the enqueue wake); anything longer is a timeout
            // by construction (deferred-publish ordering), so a larger cap only adds cost -
            // the safe-ordering variant pays it on every one of its thousands of tenures.
            var spinStart = Environment.TickCount64;
            while (Volatile.Read(ref _probe.Activations) == activationsBefore)
            {
                if (Environment.TickCount64 - spinStart >= 2)
                {
                    Interlocked.Increment(ref _probe.WaitTimeouts);
                    return false;
                }
                Thread.Yield();
            }
            Interlocked.Increment(ref _probe.ActivationsLandedInWindow);
            return true;
        }

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, SyntheticItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out SyntheticItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public ValueTask OnExecutionIdleAsync(CancellationToken cancellationToken) => default;

        public bool RunEnqueueAsynchronously => true;

        public ValueTask YieldAfterFirstItem() => default;
    }

    static async Task RunBatch(bool activateOnTp, int items, int batches, NullPolicy nullPolicy = NullPolicy.AlwaysNull, bool concurrentEnqueue = false)
    {
        for (var b = 0; b < batches; b++)
        {
            var probe = new ActivationProbe { Policy = nullPolicy };
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp));
            var all = new SyntheticItem[items];
            if (concurrentEnqueue)
            {
                // Stagger enqueues across multiple threads to interleave with the framework's
                // CompleteItem callbacks. Models the user-thread-Enqueue racing socket-thread-
                // Complete(depth=0) hazard. Mid-pipeline Enqueue can refill the pipeline
                // between a previous Complete and its OnCompleted nulling.
                var enqueueLock = new Lock();
                Parallel.For(0, items, i =>
                {
                    all[i] = new SyntheticItem { Id = i };
                    UnboundedQueueSource<SyntheticItem>.EnqueueResult enqueue;
                    lock (enqueueLock)
                        enqueue = pipeline.Enqueue(all[i]);
                    enqueue.Execute();
                });
            }
            else
            {
                for (var i = 0; i < items; i++)
                {
                    all[i] = new SyntheticItem { Id = i };
                    pipeline.Enqueue(all[i]).Execute();
                }
            }

            // Mirror the protocol's invariant: an item's pipelineTask completes only AFTER
            // its activation chain ran. In the real protocol, ExecutePipelined returns
            // (triggering the framework's CompleteItem) only after Activate fires Activate-
            // continuation -> ExecutePipelined chain. We replicate by setting the TCS from
            // INSIDE the ActivateHeadItem callback, so Complete(i) is causally after Activate(i).
            await pipeline.CompleteAsync();
            // Wait for any scheduled Reads to drain so we count nulls observed late too.
            while (Volatile.Read(ref probe.PendingReads) > 0)
                await Task.Yield();

            if (probe.Asserts > 0)
            {
                Assert.Fail($"activateOnTp={activateOnTp} policy={nullPolicy} concEnq={concurrentEnqueue} batch={b} items={items}: " +
                            $"asserts={probe.Asserts} activations={probe.Activations} completions={probe.Completions}");
            }
        }
    }

    // Tenure-reuse runner: a small number of items ping-pong through the pipeline, each
    // completion at depth==0 re-enqueueing the SAME (reset) instance from inside the
    // completion callback. completionActionFirst=true reproduces the hazardous ordering
    // (ExecutionControl.Complete -> completion action -> Control.OnCompleted) where the
    // depth-0 null policy is ABA-defeated by the re-activated same instance - those runs
    // EXPECT the repro and fail only if it no longer reproduces (lost harness sensitivity).
    // false models the fixed ordering Slon ships in Policy.CompleteItem (bookkeeping before
    // user-visible completion) and expects zero hazards.
    static async Task RunReuseBatch(int items, int batches, int tenures, NullPolicy nullPolicy, bool completionActionFirst, bool expectHazard)
    {
        var landedTotal = 0;
        var hazards = 0;
        for (var b = 0; b < batches; b++)
        {
            var probe = new ActivationProbe { Policy = nullPolicy };
            var holder = new PipelineHolder();
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp: true, holder, completionActionFirst));
            holder.Pipeline = pipeline;
            var expected = items * (1 + tenures);
            for (var i = 0; i < items; i++)
                pipeline.Enqueue(new SyntheticItem { Id = i, RemainingTenures = tenures }).Execute();

            // No-progress deadline instead of an unbounded spin: the latch-held wait in
            // FireCompletionAction can strand a waiter (its completion callback TryAcquire-
            // fails against the advancer latch our wait is holding; the slot-mode strand is
            // unrecoverable, see Pipeline.DrainSlotInline). A stranded batch can't finish -
            // abandon it (skip CompleteAsync, which would also hang) and move to a fresh
            // pipeline. Asserts and in-window landings accumulate across batches either way.
            var stranded = false;
            var lastCompletions = -1;
            var lastProgress = Environment.TickCount64;
            while (Volatile.Read(ref probe.Completions) < expected)
            {
                var completions = Volatile.Read(ref probe.Completions);
                if (completions != lastCompletions)
                {
                    lastCompletions = completions;
                    lastProgress = Environment.TickCount64;
                }
                else if (Environment.TickCount64 - lastProgress > 2_000)
                {
                    stranded = true;
                    break;
                }
                if (Volatile.Read(ref probe.Asserts) > 0 || Volatile.Read(ref probe.LiveNulls) > 0)
                    break;
                await Task.Yield();
            }

            if (!stranded && probe.Asserts == 0 && probe.LiveNulls == 0)
            {
                await pipeline.CompleteAsync();
                while (Volatile.Read(ref probe.PendingReads) > 0)
                    await Task.Yield();
            }

            hazards += probe.Asserts + probe.LiveNulls;
            if (!expectHazard && hazards > 0)
            {
                Assert.Fail($"live binding severed under supposedly-safe ordering. completionActionFirst={completionActionFirst} policy={nullPolicy} batch={b} items={items} tenures={tenures}: " +
                            $"liveNulls={probe.LiveNulls} asserts={probe.Asserts} activations={probe.Activations} completions={probe.Completions} " +
                            $"landedInWindow={probe.ActivationsLandedInWindow} waitTimeouts={probe.WaitTimeouts}");
            }
            landedTotal += probe.ActivationsLandedInWindow;
            if (expectHazard && hazards > 0)
                return;
        }

        // A hazard-expecting pass is only meaningful if the racing activation actually landed
        // inside the completion window at least some of the time. All-timeouts would mean the
        // framework serialized the two and the null policy was never under test. The safe
        // ordering skips the window machinery entirely (see FireCompletionAction), so no
        // landing requirement applies there.
        if (expectHazard)
        {
            Assert.AreNotEqual(0, landedTotal, "activation never landed inside the completion window; harness exercised nothing");
            Assert.Fail($"hazardous ordering no longer reproduces (landedInWindow={landedTotal}); harness sensitivity lost or framework ordering changed");
        }
    }

    // Reads are scheduled per-item on a separate TP work item (modelling the socket-
    // completion continuation that lands cross-thread). Read fires causally AFTER its
    // item's Activate, so the noise floor of "Read before any Activate" is gone -
    // any null observation now is a genuine cross-item race window.
    [TestMethod] public Task Sequential_AlwaysNull_Inline() => RunBatch(false, 200, 50, NullPolicy.AlwaysNull);
    [TestMethod] public Task Sequential_AlwaysNull_Tp() => RunBatch(true, 200, 50, NullPolicy.AlwaysNull);
    [TestMethod] public Task Sequential_DepthZeroOnly_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroOnly);
    [TestMethod] public Task Sequential_DepthZeroWithCas_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroWithCas);
    [TestMethod] public Task Sequential_NeverNull_Tp() => RunBatch(true, 200, 50, NullPolicy.NeverNull);

    // Concurrent enqueue exposes the user-thread-Enqueue racing framework
    // CompleteItem(depth=0) hazard - the harder race.
    [TestMethod] public Task ConcurrentEnqueue_AlwaysNull_Tp() => RunBatch(true, 200, 50, NullPolicy.AlwaysNull, concurrentEnqueue: true);
    [TestMethod] public Task ConcurrentEnqueue_DepthZeroOnly_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroOnly, concurrentEnqueue: true);
    [TestMethod] public Task ConcurrentEnqueue_DepthZeroWithCas_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroWithCas, concurrentEnqueue: true);
    [TestMethod] public Task ConcurrentEnqueue_NeverNull_Tp() => RunBatch(true, 200, 50, NullPolicy.NeverNull, concurrentEnqueue: true);

    // Tenure reuse (MaintenanceFlow pattern): the same instance is reset and re-enqueued
    // from inside its completion callback. With the completion-action-first ordering the
    // depth==0 null is ABA-defeated: the CAS compares against the same reference the new
    // tenure just activated, severing a live binding (the PgDecoder.cs:26 abort). The
    // CompleteBeforeRelease runs document that hazard (they EXPECT the repro - failing only
    // if it stops reproducing, i.e. lost harness sensitivity); ReleaseBeforeComplete is the
    // ordering Slon ships in Policy.CompleteItem and must stay hazard-free.
    [TestMethod] public Task TenureReuse_Cas_CompleteBeforeRelease_Hazardous() => RunReuseBatch(1, 20, 500, NullPolicy.DepthZeroWithCas, completionActionFirst: true, expectHazard: true);
    [TestMethod] public Task TenureReuse_Cas_ReleaseBeforeComplete() => RunReuseBatch(1, 10, 150, NullPolicy.DepthZeroWithCas, completionActionFirst: false, expectHazard: false);
    [TestMethod] public Task TenureReuse_DepthZeroOnly_CompleteBeforeRelease_Hazardous() => RunReuseBatch(1, 20, 500, NullPolicy.DepthZeroOnly, completionActionFirst: true, expectHazard: true);
}
