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
// Then drives the pipeline with hundreds of items so the framework's CommitWaiter and
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
        public ValueTask GetPipelineTask() => new(PipelineTcs.Task);

        // Tenure-reuse support: models Reset() + re-enqueue of the SAME instance (the pooled-item
        // pattern). Tenures of one item are strictly sequential (the re-enqueue happens inside the
        // previous tenure's completion), so plain fields.
        public int RemainingTenures;
        public Tenure CurrentTenure = new();
        public void Reset()
        {
            PipelineTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
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
        public QueuedPipeline<SyntheticItem, ProbePolicy>? Pipeline;
    }

    enum NullPolicy
    {
        AlwaysNull,           // baseline: activated slot = null in Complete unconditionally
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
        public NullPolicy Policy = NullPolicy.AlwaysNull;

        public void Activate(SyntheticItem item)
        {
            Interlocked.Increment(ref Activations);
            Volatile.Write(ref _activated, item);
        }

        public void Complete(SyntheticItem item, int remainingDepth)
        {
            Interlocked.Increment(ref Completions);
            switch (Policy)
            {
                case NullPolicy.AlwaysNull:
                    Volatile.Write(ref _activated, null);
                    break;
                case NullPolicy.DepthZeroOnly:
                    if (remainingDepth is 0)
                        Volatile.Write(ref _activated, null);
                    break;
                case NullPolicy.DepthZeroWithCas:
                    if (remainingDepth is 0)
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

        public ProbePolicy(ActivationProbe probe, bool activateOnTp, PipelineHolder? holder = null)
        {
            _probe = probe;
            _activateOnTp = activateOnTp;
            _holder = holder;
        }

        public ValueTask<PipelineItemResult> ExecuteItemAsync(SyntheticItem item, bool waiterExecution, CancellationToken cancellationToken)
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
                // Tenure-reuse mode, inline activation (the executor's _waiters.Count==0 path calls
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

        public void CompleteItem(SyntheticItem item, int remainingDepth, Exception? exception)
        {
            // Bookkeeping (Probe.Complete - the depth==0 CompareExchange) runs BEFORE the item's
            // completion action, so the re-enqueued tenure's Activate cannot ABA-defeat the
            // comparand. Mark the completing tenure's drain over at entry so the completion action's
            // Reset doesn't stamp the new tenure mid-drain.
            item.CurrentTenure.Completed = true;
            _probe.Complete(item, remainingDepth);
            FireCompletionAction(item);
        }

        void FireCompletionAction(SyntheticItem item)
        {
            if (_holder is null || item.RemainingTenures <= 0)
                return;
            item.RemainingTenures--;
            item.Reset();
            _holder.Pipeline!.Enqueue(item).Execute();
        }

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
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
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
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
            holder.Pipeline = pipeline;
            var expected = items * (1 + tenures);
            for (var i = 0; i < items; i++)
                pipeline.Enqueue(new SyntheticItem { Id = i, RemainingTenures = tenures }).Execute();

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

    // The framework's own _activatedItem slot (read by Slon via Control.ActivatedFlow), under
    // pipelined TP-dispatch load. The contract: never transient-null while the pipeline is live - a
    // null read by the decoder mid-pipeline is the production NRE. We sample (ActivatedItem, Depth)
    // off-thread across a single non-refilling batch; once the slot has gone non-null (first
    // activation), it must never read null again while Depth > 0 - it may only return to default at
    // the depth-0 retirement terminal. depth-0 holds this (it clears only at depth 0); the
    // unconditional clear that shipped the NRE nulls the slot on every intermediate completion and
    // is caught here. Stash-validated: fails with the unconditional clear, passes with depth-0.
    static async Task RunSlotNeverNullWhileLive(int items, int batches)
    {
        for (var b = 0; b < batches; b++)
        {
            var probe = new ActivationProbe { Policy = NullPolicy.NeverNull };
            var pipeline = Pipeline.Create<SyntheticItem, ProbePolicy>(new(probe, activateOnTp: true));
            using var __pin = MstestWhenAllWorkaround.Pin(pipeline);
            for (var i = 0; i < items; i++)
                pipeline.Enqueue(new SyntheticItem { Id = i }).Execute();

            var violations = 0;
            var seen = false;
            var stop = false;
            var sampler = Task.Run(() =>
            {
                while (!Volatile.Read(ref stop))
                {
                    // Read slot BEFORE depth: if the slot is non-null we're done; if it's null we then
                    // read depth. A depth > 0 observed AFTER a null slot (with the slot still settling)
                    // is conservative - it can only over-report, and depth-0 never produces a null here
                    // mid-batch, so a clean run proves the property.
                    var slot = pipeline.Pipeline.ActivatedItem;
                    if (slot is not null)
                        seen = true;
                    else if (seen && pipeline.Depth > 0)
                        Interlocked.Increment(ref violations);
                }
            });

            await pipeline.CompleteAsync();
            Volatile.Write(ref stop, true);
            await sampler;

            if (Volatile.Read(ref violations) > 0)
                Assert.Fail($"batch={b} items={items}: framework ActivatedItem slot read null while live (Depth>0) " +
                            $"{Volatile.Read(ref violations)}x - a transient-null-while-live the decoder would NRE on.");
        }
    }

    [TestMethod] public Task FrameworkSlot_NeverNullWhileLive_Pipelined() => RunSlotNeverNullWhileLive(400, 40);
}
