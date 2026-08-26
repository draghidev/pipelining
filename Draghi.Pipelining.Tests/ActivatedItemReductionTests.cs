using System.Diagnostics.CodeAnalysis;

namespace Draghi.Pipelining.Tests;

// Reduction for a cross-item activation/completion ordering hazard. The framework's per-item
// ordering (Activate(i)-before-Complete(i)) is preserved. What a consumer can still observe broken
// is cross-item ordering between Complete(A) and Activate(B) when Activate(B) is dispatched off the
// completing thread via UnsafeQueueUserWorkItem - the activated-item slot can read null in the gap.
//
// We synthesise a policy that:
//   - holds a single `_activated` field (the activated-item slot a consumer reads).
//   - sets it in ActivateHeadItem.
//   - nulls it in CompleteItem.
//   - reads it (and asserts non-null) at a configurable check site.
// Then drives the pipeline with hundreds of items so the framework's CommitInFlightItem and
// DrainSlotInline preferAsync=true paths fire.
//
// Two variants: INLINE activation and TP-dispatched activation. If only the second reproduces the
// assert, the framework's contract is honoured and the hazard is in the off-thread activation hop.
[TestClass]
public class ActivatedItemReductionTests
{
    sealed class SyntheticItem
    {
        public int Id { get; init; }
        // RunContinuationsAsynchronously: the framework's CompleteItem continuation (registered
        // via UnsafeOnCompleted by the pipeline waiter machinery) gets TP-queued instead of
        // running inline on whoever called SetResult. This is what makes Complete(prev) and
        // Activate(next) end up on DIFFERENT threads - pipeline-task completions and
        // ActivateHeadItem work items each schedule onto TP independently and race.
        public TaskCompletionSource PipelineTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ActivatedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ValueTask GetPipelineTask() => new(PipelineTcs.Task);

        // Tenure-reuse support: models Reset() + re-enqueue of the SAME instance (the pooled-item
        // pattern). Tenures of one item are strictly sequential (the re-enqueue happens inside the
        // previous tenure's completion), so plain fields.
        public int RemainingTenures;
        public Tenure CurrentTenure = new();
        public void Reset()
        {
            PipelineTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            ActivatedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            CurrentTenure = new();
        }
    }

    // One activation cycle of an item. The read gate: a read only executes during its own tenure's
    // drain, i.e. before that tenure's CompleteItem begins. A read that observes a null slot while
    // its tenure is still mid-drain is a REAL hazard; a read straddling its own completion is
    // suppressed. This is race-free where the old activeCount gate was not (count could be sampled
    // before a completion's decrement and the slot after that same completion's null, yielding
    // false positives).
    sealed class Tenure
    {
        public volatile bool Completed;
    }

    // Mutable indirection so the policy (copied by value into the pipeline) can re-enqueue
    // into the pipeline it belongs to.
    sealed class PipelineHolder
    {
        public UnboundedPipeline<SyntheticItem, ProbePolicy>? Pipeline;
    }

    enum NullPolicy
    {
        AlwaysNull,           // baseline: activated slot = null in Complete unconditionally
        DepthZeroOnly,        // null only on the exact idle edge (assignment)
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
        public NullPolicy Policy = NullPolicy.AlwaysNull;

        public void Activate(SyntheticItem item)
        {
            Interlocked.Increment(ref Activations);
            Volatile.Write(ref _activated, item);
        }

        public void Complete(SyntheticItem item, bool idle)
        {
            Interlocked.Increment(ref Completions);
            switch (Policy)
            {
                case NullPolicy.AlwaysNull:
                    Volatile.Write(ref _activated, null);
                    break;
                case NullPolicy.DepthZeroOnly:
                    if (idle)
                        Volatile.Write(ref _activated, null);
                    break;
                case NullPolicy.DepthZeroWithCas:
                    if (idle)
                        Interlocked.CompareExchange(ref _activated, null, item);
                    break;
                case NullPolicy.NeverNull:
                    break;
            }
        }

        // The consumer's activated-slot read - the assert site. A read executes on behalf of a
        // specific tenure and only asserts if the slot is null while that tenure is still mid-drain.
        // Reads only happen during the activated item's drain, so a null observed then is a genuine
        // binding loss.
        //
        // Read order matters: slot FIRST, own-tenure flag SECOND. Complete() sets the flag before
        // applying the null policy (both release-ordered), so a reader that observed a null produced
        // by its OWN completion must subsequently observe Completed==true (acquire on the null
        // read). A null with Completed==false is therefore a null produced by a FOREIGN tenure's
        // completion while this tenure is mid-drain - the real hazard. Flag-first would straddle:
        // flag sampled before the completion, slot sampled after its null, false-positively blaming
        // a clean ordering.
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
        readonly bool _holdRelease;
        readonly bool _verifyFrameworkSlot;

        public ProbePolicy(ActivationProbe probe, bool activateOnTp, PipelineHolder? holder = null,
            bool holdRelease = false, bool verifyFrameworkSlot = false)
        {
            _probe = probe;
            _activateOnTp = activateOnTp;
            _holder = holder;
            _holdRelease = holdRelease;
            _verifyFrameworkSlot = verifyFrameworkSlot;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(SyntheticItem item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
            => new(new PipelineItemResult(default, item.GetPipelineTask()));

        public void ActivateHeadItem(SyntheticItem item, bool preferAsync = true)
        {
            if (_verifyFrameworkSlot)
            {
                _probe.Activate(item);
                // This callback is the production BindDecoder boundary: the framework published
                // ActivatedItem immediately before entering it. Keep sampling briefly so a prior
                // retirement that already sampled zero can expose an unbracketed late clear.
                for (var i = 0; i < 64; i++)
                {
                    if (!ReferenceEquals(_holder!.Pipeline!.Pipeline.ActivatedItem, item))
                        Interlocked.Increment(ref _probe.Asserts);
                    Thread.Yield();
                }
                item.ActivatedTcs.TrySetResult();
                return;
            }
            if (_holdRelease)
            {
                // Activate now, then complete the pipeline task after a brief delay on a separate TP
                // thread. The delay lets the executor dispatch and activate SUBSEQUENT items before
                // this one completes, so depth climbs above 1 AND activation/completion interleave: an
                // earlier item completes while a later one is the live activated owner - the exact
                // live-later-owner stomp the production NRE came from. (Holding all then releasing all
                // would separate the two phases and never interleave them.) The assert site is the
                // test's continuous framework-slot sampler.
                _probe.Activate(item);
                // Delay on the timer wheel, NOT by blocking a TP worker: the ms-scale hold is
                // load-bearing (it keeps depth above 1 so completions interleave with later
                // activations, and it dwarfs the retirement lag the sampler's tight re-confirm
                // discriminates against - a us-scale hold lets depth touch 0 between items and
                // false-positives the sampler). What was NOT load-bearing was occupying a
                // thread for it: the old SpinWait's Sleep(1) escalation blocked dozens of TP
                // workers at once (60 in-flight items), serializing the batch behind threadpool
                // injection - ~11s for the 8x60 run. The timer keeps the hold and frees the pool.
                // 1ms: activations are single-tenure serialized, so the hold multiplies DIRECTLY
                // into wall time (items x batches x hold - 50ms made the test take ~24s). The
                // sampler no longer discriminates by tight timing (see its comment), so the hold
                // only needs to keep completions asynchronous and depth above 1.
                _ = Task.Delay(1).ContinueWith(
                    static (_, s) => ((SyntheticItem)s!).PipelineTcs.TrySetResult(), item,
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                return;
            }
            if (_activateOnTp && preferAsync)
            {
                _probe.RegisterPendingRead();
                ThreadPool.UnsafeQueueUserWorkItem(static state =>
                {
                    var (probe, item) = ((ActivationProbe, SyntheticItem))state!;
                    probe.Activate(item);
                    // Schedule the Read for "later" on a separate TP thread - do NOT block. The
                    // pipelineTcs.SetResult fires immediately, so the framework's CompleteItem chain
                    // races with this scheduled Read. The Read fires arbitrarily later, after other
                    // items' Completes may have interleaved.
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
                // Tenure-reuse mode, inline activation (the executor's _inFlight.Count==0 path calls
                // ActivateHeadItem with preferAsync:false). Model the item shape where reads continue
                // AFTER activation (async Read) and the pipeline task completes from an off-thread
                // continuation, never inline in ExecuteItemAsync (async TrySetResult). A synchronous
                // TrySetResult here would take the executor's sync-completion shortcut and run
                // CompleteItem on the executor thread itself, serializing away the cross-thread race.
                _probe.Activate(item);
                // A consumer reads many times across a drain; several scattered reads sample the
                // (potential) null window instead of a single coin flip per tenure.
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

        public void CompleteItem(SyntheticItem item, Exception? exception)
        {
            // Bookkeeping (Probe.Complete - the depth==0 CompareExchange) runs BEFORE the item's
            // completion action, so the re-enqueued tenure's Activate cannot ABA-defeat the
            // comparand. Mark the completing tenure's drain over at entry so the completion action's
            // Reset doesn't stamp the new tenure mid-drain.
            item.CurrentTenure.Completed = true;
            // The framework clears its activated slot only at the exact idle edge and does so
            // before CompleteItem, giving policies an item-safe discriminator without a depth count.
            _probe.Complete(item, _holder?.Pipeline?.Pipeline.ActivatedItem is null);
            FireCompletionAction(item);
        }

        void FireCompletionAction(SyntheticItem item)
        {
            if (_holder is null || item.RemainingTenures <= 0)
                return;
            item.RemainingTenures--;
            item.Reset();
            _holder.Pipeline!.Enqueue(item).Signal();
        }

        public void OnIdle() { }

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, SyntheticItem failedItem, CancellationToken cancellationToken, [NotNullWhen(true)] out SyntheticItem? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        public bool RunEnqueueAsynchronously => true;
    }

    static async Task RunBatch(bool activateOnTp, int items, int batches, NullPolicy nullPolicy = NullPolicy.AlwaysNull, bool concurrentEnqueue = false)
    {
        for (var b = 0; b < batches; b++)
        {
            var probe = new ActivationProbe { Policy = nullPolicy };
            var holder = new PipelineHolder();
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp, holder));
            holder.Pipeline = pipeline;
            var all = new SyntheticItem[items];
            if (concurrentEnqueue)
            {
                // Stagger enqueues across multiple threads to interleave with the framework's
                // CompleteItem callbacks. Models the user-thread-Enqueue racing the completion-
                // thread-Complete(depth=0) hazard. Mid-pipeline Enqueue can refill the pipeline
                // between a previous Complete and its OnCompleted nulling.
                var enqueueLock = new Lock();
                Parallel.For(0, items, i =>
                {
                    all[i] = new SyntheticItem { Id = i };
                    UnboundedQueueSource<SyntheticItem>.EnqueueSignal enqueue;
                    lock (enqueueLock)
                        enqueue = pipeline.Enqueue(all[i]);
                    enqueue.Signal();
                });
            }
            else
            {
                for (var i = 0; i < items; i++)
                {
                    all[i] = new SyntheticItem { Id = i };
                    pipeline.Enqueue(all[i]).Signal();
                }
            }

            // Maintain the invariant: an item's pipelineTask completes only AFTER its activation
            // chain ran. We replicate by setting the TCS from INSIDE the ActivateHeadItem callback,
            // so Complete(i) is causally after Activate(i).
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
    // completion callback. Verifies the shipped ordering (bookkeeping before user-visible
    // completion) stays hazard-free under tenure reuse.
    static async Task RunReuseBatch(int items, int batches, int tenures, NullPolicy nullPolicy)
    {
        for (var b = 0; b < batches; b++)
        {
            var probe = new ActivationProbe { Policy = nullPolicy };
            var holder = new PipelineHolder();
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp: true, holder));
            holder.Pipeline = pipeline;
            var expected = items * (1 + tenures);
            for (var i = 0; i < items; i++)
                pipeline.Enqueue(new SyntheticItem { Id = i, RemainingTenures = tenures }).Signal();

            while (Volatile.Read(ref probe.Completions) < expected)
            {
                if (Volatile.Read(ref probe.Asserts) > 0)
                    break;
                await Task.Yield();
            }

            if (probe.Asserts == 0)
            {
                await pipeline.CompleteAsync();
                while (Volatile.Read(ref probe.PendingReads) > 0)
                    await Task.Yield();
            }

            if (probe.Asserts > 0)
            {
                Assert.Fail($"live binding severed under supposedly-safe ordering. policy={nullPolicy} batch={b} items={items} tenures={tenures}: " +
                            $"asserts={probe.Asserts} activations={probe.Activations} completions={probe.Completions}");
            }
        }
    }

    // Reads are scheduled per-item on a separate TP work item (modelling a completion continuation
    // that lands cross-thread). Read fires causally AFTER its item's Activate, so the noise floor of
    // "Read before any Activate" is gone - any null observation now is a genuine cross-item race.
    [TestMethod] public Task Sequential_AlwaysNull_Inline() => RunBatch(false, 200, 50, NullPolicy.AlwaysNull);
    [TestMethod] public Task Sequential_AlwaysNull_Tp() => RunBatch(true, 200, 50, NullPolicy.AlwaysNull);
    [TestMethod] public Task Sequential_DepthZeroOnly_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroOnly);
    [TestMethod] public Task Sequential_DepthZeroWithCas_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroWithCas);
    [TestMethod] public Task Sequential_NeverNull_Tp() => RunBatch(true, 200, 50, NullPolicy.NeverNull);

    // Concurrent enqueue exposes the user-thread-Enqueue racing the framework
    // CompleteItem(depth=0) hazard - the harder race.
    [TestMethod] public Task ConcurrentEnqueue_AlwaysNull_Tp() => RunBatch(true, 200, 50, NullPolicy.AlwaysNull, concurrentEnqueue: true);
    [TestMethod] public Task ConcurrentEnqueue_DepthZeroOnly_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroOnly, concurrentEnqueue: true);
    [TestMethod] public Task ConcurrentEnqueue_DepthZeroWithCas_Tp() => RunBatch(true, 200, 50, NullPolicy.DepthZeroWithCas, concurrentEnqueue: true);
    [TestMethod] public Task ConcurrentEnqueue_NeverNull_Tp() => RunBatch(true, 200, 50, NullPolicy.NeverNull, concurrentEnqueue: true);

    // Tenure reuse (pooled-item pattern): the same instance is reset and re-enqueued from inside its
    // completion callback. With ReleaseBeforeComplete (bookkeeping before user-visible completion)
    // the depth==0 null sees the activated instance under its own tenure and never severs a live
    // binding. The historical CompleteBeforeRelease ordering was ABA-defeated; the activated-item /
    // decrement reorder structurally closes that window in the framework so the unsafe variant no
    // longer reproduces - the only remaining check here is that the shipped ordering stays
    // hazard-free.
    [TestMethod] public Task TenureReuse_Cas_ReleaseBeforeComplete() => RunReuseBatch(1, 10, 150, NullPolicy.DepthZeroWithCas);

    [TestMethod]
    public Task FrameworkSlot_NeverNullWhileLive_PipelinedBroad()
        => RunSlotNeverNullWhileLive(60, 8);

    // The framework's own _activatedItem slot (read by Slon via Control.ActivatedFlow), under
    // pipelined load with MANY items concurrently in-flight. The production contract: the sole reader
    // (PgDecoder.CurrentExecutionControl) reads the slot only while an activated flow's own body is
    // mid-decode. A null (or a foreign item) observed while the slot's live owner is still mid-drain
    // is the production NRE - NoStomp (an earlier item's completion nulling a live later owner's slot).
    // A null in the inter-item gap (a completion drove in-flight Depth to 0 while backlog remained) is
    // SAFE: no flow body reads there, and the depth-0 clear only fires for the last in-flight item.
    //
    // Two pieces make this reproduce + discriminate under in-flight Depth:
    //   - holdRelease: each item activates then self-completes its pipeline task after a brief delay,
    //     so the executor dispatches and activates the next item before this one completes - Depth
    //     climbs above 1 (real pipelining), not the Depth=1 of serial sync-completion, AND completions
    //     interleave with later activations across threads, the only regime where a stomp is reachable.
    //   - A continuous off-thread sampler (frequent sampling catches the brief transient stomp the
    //     original test caught) that brackets each suspect read against the LAST-SEEN owner's tenure:
    //     a hazard is null/foreign-slot while that owner is not yet Completed, re-confirmed by the
    //     owner staying incomplete across a bounded settle (a safe retirement lag flips Completed; the
    //     framework's clear-before-Completed order makes the raw sample ambiguous, the re-confirm
    //     resolves it). Depth is NOT used as the liveness proxy - under in-flight it dips to 0 between
    //     items and would false-positive the safe gap.
    static async Task RunSlotNeverNullWhileLive(int items, int batches)
    {
        for (var b = 0; b < batches; b++)
        {
            var probe = new ActivationProbe { Policy = NullPolicy.NeverNull };
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp: true, holdRelease: true));

            var violations = 0;
            var stop = false;
            var sampler = Task.Run(() =>
            {
                SyntheticItem? owner = null;
                while (!Volatile.Read(ref stop))
                {
                    var slot = pipeline.Pipeline.ActivatedItem;
                    if (slot is not null)
                    {
                        owner = slot;
                        continue;
                    }
                    if (owner is not { CurrentTenure.Completed: false })
                        continue;
                    // Slot is null with the last-seen owner not yet Completed. Two causes:
                    //   - SAFE retirement lag: the framework clears the slot a couple of instructions
                    //     BEFORE CompleteItem marks THIS owner Completed, so the flag flips almost
                    //     immediately.
                    //   - HAZARD (stomp): a FOREIGN item's completion nulled the slot while this owner
                    //     is genuinely live; this owner stays mid-drain until its own (much later)
                    //     completion.
                    // TIME cannot robustly separate the safe lag from a transient stomp: the lag
                    // (clear -> CompleteItem flag, a few instructions) stretches to preemption
                    // scale (ms+) under parallel-suite load, and any window that exceeds preemption
                    // forces a hold that makes the single-tenure-serialized test take tens of
                    // seconds. The transient face is the model's job. The runtime sampler asserts
                    // the crisp face only: after a suspect null the owner must COMPLETE within a
                    // generous bound, since a stranded owner is the unfakeable production shape.
                    var tenure = owner.CurrentTenure;
                    var deadline = System.Diagnostics.Stopwatch.StartNew();
                    var spin = new SpinWait();
                    var hazard = true;
                    while (deadline.ElapsedMilliseconds < 1000)
                    {
                        if (tenure.Completed)
                        {
                            hazard = false;
                            break;
                        }
                        spin.SpinOnce();
                    }
                    if (hazard)
                        Interlocked.Increment(ref violations);
                }
            });

            // Enqueue with the sampler already running. Each item activates then self-completes after a
            // brief delay (holdRelease), so depth climbs above 1 and completions interleave with later
            // activations across threads.
            for (var i = 0; i < items; i++)
                pipeline.Enqueue(new SyntheticItem { Id = i }).Signal();

            await pipeline.CompleteAsync();
            Volatile.Write(ref stop, true);
            await sampler;

            if (Volatile.Read(ref violations) > 0)
                Assert.Fail($"batch={b} items={items}: framework ActivatedItem slot read null while its live owner's " +
                            $"tenure was mid-drain {Volatile.Read(ref violations)}x - the production NRE.");
        }
    }

}
