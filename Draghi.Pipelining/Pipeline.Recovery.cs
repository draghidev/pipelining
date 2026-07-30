using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

public sealed partial class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    /// Recovers execution or pipeline-task failure. <paramref name="activated"/> distinguishes an
    /// executor-owned turn from an equal-looking provisional turn held by an empty-edge pass; only the latter
    /// requires waiting for that pass before touching the failed item or its substitute.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverItem(T item, bool activated, PipelineItemFailureContext context, long failedTurn, CancellationToken cancellationToken)
    {
        // A foreign provisional turn belongs to the matching empty-edge pass. Wait for activation to return.
        if (!activated && failedTurn != 0)
            WaitForRecoveryHandoff(-failedTurn, cancellationToken);

        T? recoveryCandidate;
        bool recovered;
        try
        {
            recovered = _policy.TryRecoverItemFailure(context, item, cancellationToken, out recoveryCandidate);
        }
        catch (Exception recoveryPolicyException)
        {
            // The executor is terminating, but the failed item still owns its activation turn and
            // depth tenure. Retire it normally before preserving the policy exception as the root.
            try
            {
                RetireItem(item, recoveryPolicyException, failedTurn);
            }
            catch
            {
                // Completion is cleanup for an already-terminal executor. Preserve the recovery
                // policy failure that caused termination rather than replacing it here.
            }
            throw;
        }

        if (!recovered)
        {
            RetireItem(item, context.Exception, failedTurn);
            return;
        }
        var recoveryItem = recoveryCandidate!;

        // Republish the substitute and mirror normal activation gating. Prior in-flight work keeps
        // activation deferred so the substitute cannot overlap its shared resource tenure.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _executingItemVisible, true);
        var recoveryActivated = false;
        long recoveryGeneration = 0;
        if (_inFlight.Count is 0)
        {
            // Inherit the failed position's provisional turn or claim a fresh one. A foreign turn
            // hands off the substitute. Decide under the edge lock and activate outside it.
            var candidateGeneration = failedTurn < 0 ? -failedTurn : _activationGate.NextGeneration();
            var recoveryLock = _activationGate.EdgeLock;
            recoveryLock.Enter();
            recoveryGeneration = _activationGate.ClaimOrInheritProvisionalTurn(candidateGeneration);
            recoveryLock.Exit();
            if (recoveryGeneration != 0)
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                recoveryActivated = true;
            }
            else
            {
                recoveryGeneration = _activationGate.PublishHandoff(recoveryItem);
            }
        }
        else
        {
            // Handoff publication orders the executor slot before an empty-edge consumer observes it.
            recoveryGeneration = _activationGate.PublishHandoff(recoveryItem);
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, pipelineTaskRecovery: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
                await result.TrailingExecutionTask.ConfigureAwait(false);

            if (result.PipelineTask.IsCompleted)
            {
                if (ClearExecutingItem(recoveryActivated))
                {
                    result.PipelineTask.GetAwaiter().GetResult();
                    RetireItem(recoveryItem, null, ownedTurn: GetActivationOwner(recoveryActivated, ownsTurn: true, recoveryGeneration));
                }
                else
                {
                    // A provisional turn claimed during recovery routes through the store.
                    CommitInFlightItem(recoveryItem, activateAtCommit: false, ownsTurn: false, turnGeneration: recoveryGeneration, GuardRecoveryTask(result.PipelineTask));
                }
            }
            else
            {
                // Re-enter the normal pending-tail lifecycle without exposing the item twice. Guard its
                // late task fault from recursively entering recovery.
                Volatile.Write(ref _executingItemVisible, false);
                _pendingTail = recoveryItem;
                _pendingTailTask = GuardRecoveryTask(result.PipelineTask);
                _pendingTailActivated = recoveryActivated;
                _pendingTailGeneration = recoveryGeneration;
                Volatile.Write(ref _hasPendingTail, true);
            }
        }
        catch (Exception recoveryEx)
        {
            // No pending tail to observe here: the only _pendingTail publish in the try
            // is in the trailing pending branch, which doesn't throw.
            var ownedEx = ClearExecutingItem(recoveryActivated);
            RetireItem(recoveryItem, recoveryEx,
                ownedTurn: GetActivationOwner(recoveryActivated, ownedEx, recoveryGeneration));
        }
    }

    /// Handles trailing execution task failures, including pending-tail recovery.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverTrailingFailure(T item, bool activated, long failedGeneration, Exception ex, CancellationToken cancellationToken)
    {
        // Preserve the pipeline task: the framework re-uses the materialized Task locally below
        // (no-recovery branch's CommitInFlightItem), so we want a stable handle that outlives this
        // method's locals. Wrap it back into a ValueTask for the context's per-construction-
        // site value - the recovery awaits the Task-backed ValueTask idempotently, the
        // framework's CommitInFlightItem takes the Task directly.
        var pipelineTask = _pendingTailTask.Preserve();
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.TrailingExecutionTask, ex, pipelineTask);
        T? recoveryCandidate;
        bool recovered;
        try
        {
            recovered = _policy.TryRecoverItemFailure(context, item, cancellationToken, out recoveryCandidate);
        }
        catch
        {
            _hasPendingTail = false;
            _pendingTailTask = default;
            _pendingTailActivated = false;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _pendingTail = default!;
            var ownsTurn = activated || _activationGate.TryTakeHandoff();
            CommitInFlightItem(item, activateAtCommit: ownsTurn && !activated, ownsTurn: ownsTurn, turnGeneration: failedGeneration, pipelineTask);
            throw;
        }

        if (!recovered)
        {
            // A pending pipeline task must enter the in-flight store rather than complete prematurely.
            // Items can handle their own interdependency between the two tasks if needed.
            _hasPendingTail = false;
            _pendingTailTask = default;
            _pendingTailActivated = false;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _pendingTail = default!;
            // Resolve the parked handoff before the commit (the commit invariant): the one-winner
            // take decides ownership; a loss commits under the empty-edge pass's provisional turn.
            // Plain consume (see ClearExecutingItem's doc) - residents may be live here.
            var ownsTurn = activated || _activationGate.TryTakeHandoff();
            CommitInFlightItem(item, activateAtCommit: ownsTurn && !activated, ownsTurn: ownsTurn, turnGeneration: failedGeneration, pipelineTask);
            return;
        }
        var recoveryItem = recoveryCandidate!;

        // Consume the failed item's handoff before replacing its published identity. Publishing the
        // substitute first could pair it with the failed item's still-live handoff. Recovery is
        // executor-serialized; this ordering protects the swap against lock-free claimants.
#if DEBUG
        Debug.Assert(Interlocked.Exchange(ref _recoverySwapGuard, 1) == 0,
            "Concurrent recovery swap - the executor-strand premise was violated (multi-strand recovery restructure?).");
#endif
        // Losing the consume means the empty-edge pass already captured and will activate the failed
        // item. Wait for that exact generation to resolve before activating its substitute.
        var recoverySwapWon = _activationGate.TryTakeHandoff();
        SetExecutingItem(recoveryItem);
        bool recoveryActivated;
        long recoveryGeneration;
        if (recoverySwapWon)
        {
            recoveryGeneration = _activationGate.PublishHandoff(recoveryItem);
            recoveryActivated = false;
        }
        else
        {
            // The substitute inherits the failed item's turn. A self-activated item has no handoff
            // observer to await; only a lost deferred consume requires the generation wait.
            if (!activated)
                WaitForRecoveryHandoff(failedGeneration, cancellationToken);
            recoveryGeneration = failedGeneration;
            ActivateHeadItem(recoveryItem, preferAsync: false);
            recoveryActivated = true;
        }
#if DEBUG
        Debug.Assert(Interlocked.Exchange(ref _recoverySwapGuard, 0) == 1);
#endif

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, pipelineTaskRecovery: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
            {
                try
                {
                    await result.TrailingExecutionTask.ConfigureAwait(false);
                }
                catch (Exception trailingEx)
                {
                    _hasPendingTail = false;
                    _pendingTailTask = default;
                    var ownedTrail = ClearExecutingItem(recoveryActivated);
                    RetireItem(recoveryItem, trailingEx,
                        ownedTurn: GetActivationOwner(recoveryActivated, ownedTrail, recoveryGeneration));
                    return;
                }
            }

            if (result.PipelineTask.IsCompleted)
            {
                _hasPendingTail = false;
                _pendingTailTask = default;
                if (ClearExecutingItem(recoveryActivated))
                {
                    Exception? pipelineEx = null;
                    try
                    {
                        result.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception e)
                    {
                        pipelineEx = e;
                    }
                    RetireItem(recoveryItem, pipelineEx,
                        ownedTurn: GetActivationOwner(recoveryActivated, ownsTurn: true, recoveryGeneration));
                }
                else
                {
                    // The empty-edge pass owns the provisional turn, so route through the store.
                    CommitInFlightItem(recoveryItem, activateAtCommit: false, ownsTurn: false, turnGeneration: recoveryGeneration, GuardRecoveryTask(result.PipelineTask));
                }
                return;
            }

            // Guarded for the same reason as RecoverItem's tail transition: the substitute's own
            // late fault completes it directly, never re-enters recovery.
            _pendingTail = recoveryItem;
            _pendingTailTask = GuardRecoveryTask(result.PipelineTask);
            _pendingTailActivated = recoveryActivated;
            _pendingTailGeneration = recoveryGeneration;
        }
        catch (Exception recoveryEx)
        {
            _hasPendingTail = false;
            _pendingTailTask = default;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _pendingTail = default!;
            var ownedRec = ClearExecutingItem(recoveryActivated);
            RetireItem(recoveryItem, recoveryEx,
                ownedTurn: GetActivationOwner(recoveryActivated, ownedRec, recoveryGeneration));
        }
    }

    /// Commits the pending tail (if any) to the in-flight store. Called by the executor at the
    /// start of each iteration and before going idle. Returns null when the commit completed
    /// synchronously (no tail, CommitInFlightItem, or sync-complete pipeline task), or a Task the
    /// caller must await for the trailing-recovery path. Returning Task? instead of ValueTask
    /// lets the caller skip the await ceremony entirely on the common sync paths via a null check.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Task? CommitPendingTail()
    {
        if (!_hasPendingTail)
            return null;

        var item = _pendingTail;
        var task = _pendingTailTask;
        _hasPendingTail = false;
        _pendingTailTask = default;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _pendingTail = default!;

        var tailActivated = _pendingTailActivated;
        _pendingTailActivated = false;
        var tailGeneration = _pendingTailGeneration;
        _pendingTailGeneration = 0;
        // Reclaim an executor-owned activation or an ungranted handoff. If the edge pass won, route
        // through the store so advance-license ordering places activation before retirement.
        var ownsTurn = tailActivated || _activationGate.TryTakeHandoff();
        // A winning edge pass captured the item before consuming its handoff, so either arm may clear it.
        SetExecutingItem(default!);

        if (task.IsCompleted && ownsTurn)
        {
            // Inline retirement may bypass the reorder buffer only when this item is the head.
            if (task.IsCompletedSuccessfully && _inFlight.Count is 0)
            {
                task.GetAwaiter().GetResult();
                RetireItem(item, null, ownedTurn: GetActivationOwner(tailActivated, ownsTurn: true, tailGeneration));
                return null;
            }

            if (!task.IsCompletedSuccessfully)
                return RecoverCommittedPendingTailAsync(item, task, tailActivated, tailGeneration).AsTask();
        }

        // An edge-owned fault also routes through the store and is surfaced by the advancer.
        CommitInFlightItem(item, activateAtCommit: ownsTurn && !tailActivated, ownsTurn: ownsTurn, turnGeneration: tailGeneration, task);
        return null;
    }

    // Awaited on the executor strand to preserve the store's single-producer contract and FIFO commit.
    // Entry requires a successfully reclaimed turn, so no empty-edge handoff remains to await.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverCommittedPendingTailAsync(T item, ValueTask task, bool tailActivated, long tailGeneration)
    {
        Exception exception;
        try
        {
            task.GetAwaiter().GetResult();
            exception = null!; // Unreachable.
        }
        catch (Exception ex)
        {
            exception = ex;
        }

        // A recovery item's own guarded tail faulting between SetTail and this commit (the
        // recovery substitutes re-enter the normal tail lifecycle; see GuardRecoveryTask).
        // Complete directly with the real fault - never re-recovered, never consulted.
        var failedTurn = GetActivationOwner(tailActivated, ownsTurn: true, tailGeneration);
        if (exception is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            RetireItem(item, recoveryFault.InnerException, failedTurn);
            return;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.ExecutionPipelineTask, exception);
        T? recoveryCandidate;
        bool recovered;
        try
        {
            recovered = _policy.TryRecoverItemFailure(context, item, _enumerator.CompletionToken, out recoveryCandidate);
        }
        catch (Exception recoveryPolicyException)
        {
            RetireItem(item, recoveryPolicyException, failedTurn);
            throw;
        }

        if (!recovered)
        {
            RetireItem(item, exception, failedTurn);
            return;
        }
        var recoveryItem = recoveryCandidate!;

        // Republish and gate activation on the count, mirroring RecoverItem: activating
        // unconditionally while prior items are in flight would put a second active reader on the
        // wire.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _executingItemVisible, true);
        var recoveryActivated = false;
        long recoveryGeneration = 0;
        if (_inFlight.Count is 0)
        {
            // Recovery substitutes inherit the failed provisional turn or claim a fresh one;
            // a live foreign turn requires a handoff (full argument at RecoverItem's twin arm).
            var candidateGeneration = failedTurn < 0 ? -failedTurn : _activationGate.NextGeneration();
            var recoveryLock = _activationGate.EdgeLock;
            recoveryLock.Enter();
            recoveryGeneration = _activationGate.ClaimOrInheritProvisionalTurn(candidateGeneration);
            recoveryLock.Exit();
            if (recoveryGeneration == 0)
            {
                recoveryGeneration = _activationGate.PublishHandoff(recoveryItem);
            }
            else
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                recoveryActivated = true;
            }
        }
        else
        {
            // Republish - see RecoverItem's twin: the empty-edge pass captures before its take.
            recoveryGeneration = _activationGate.PublishHandoff(recoveryItem);
        }

        PipelineItemResult result;
        try
        {
            result = await _policy.ExecuteItemAsync(recoveryItem, pipelineTaskRecovery: false, _enumerator.CompletionToken).ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
            var ownedRec = ClearExecutingItem(recoveryActivated);
            RetireItem(recoveryItem, recoveryEx,
                ownedTurn: GetActivationOwner(recoveryActivated, ownedRec, recoveryGeneration));
            return;
        }

        if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
        {
            try
            {
                await result.TrailingExecutionTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var ownedTrail = ClearExecutingItem(recoveryActivated);
                RetireItem(recoveryItem, ex,
                    ownedTurn: GetActivationOwner(recoveryActivated, ownedTrail, recoveryGeneration));
                return;
            }
        }

        var pipelineTask = result.PipelineTask;
        if (pipelineTask.IsCompleted)
        {
            // Inline completion, ownership-gated like every executor-side completion: a lost
            // A substitute owned by the empty-edge pass routes through the store instead.
            if (ClearExecutingItem(recoveryActivated))
            {
                Exception? pipelineException = null;
                try
                {
                    pipelineTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    pipelineException = ex;
                }
                RetireItem(recoveryItem, pipelineException,
                    ownedTurn: GetActivationOwner(recoveryActivated, ownsTurn: true, recoveryGeneration));
            }
            else
            {
                CommitInFlightItem(recoveryItem, activateAtCommit: false, ownsTurn: false, turnGeneration: recoveryGeneration, GuardRecoveryTask(pipelineTask));
            }
        }
        else if (_enumerator.CompletionToken.IsCancellationRequested)
        {
            // Pipeline shutdown while recovery's async work was in flight. CommitInFlightItem at this
            // point would leak: OnInFlightTaskCompleted bails on wake completion, and DrainOnCompletionAsync
            // has likely already passed. Complete directly so depth tracking and CompleteItem still fire.
            var ownedShut = ClearExecutingItem(recoveryActivated);
            RetireItem(recoveryItem, _completionException,
                ownedTurn: GetActivationOwner(recoveryActivated, ownedShut, recoveryGeneration));
        }
        else
        {
            // The recovery enters the store as an ordinary item; its identity travels in the task
            // (the guard wrapper rethrows late faults as RecoveryItemFaultException, see the marker
            // type), not in pipeline state - no fields, no value-T-unsound item comparisons.
            // Resolve the handoff before the commit (the commit invariant), one-winner.
            var ownsTurnAtCommit = recoveryActivated || _activationGate.TryTakeHandoff();
            SetExecutingItem(default!);
            Volatile.Write(ref _executingItemVisible, false);
            CommitInFlightItem(recoveryItem, activateAtCommit: ownsTurnAtCommit && !recoveryActivated, ownsTurn: ownsTurnAtCommit, turnGeneration: recoveryGeneration, GuardRecoveryTask(pipelineTask));
        }
    }

    /// Marks a recovery item's late task fault so normal tail/store handling completes it directly
    /// rather than recursively consulting recovery policy. Allocation occurs only on failure.
    static async ValueTask GuardRecoveryTask(ValueTask pipelineTask)
    {
        try
        {
            await pipelineTask.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new Pipeline.RecoveryItemFaultException(ex);
        }
    }

    /// <summary>
    /// Waits for the empty-edge activation inherited by a recovery to return. The caller only enters
    /// after losing a generation-pinned handoff consume, which proves that the matching empty-edge pass
    /// already consumed it and is at most one synchronous <see cref="IPipelinePolicy{T}.ActivateHeadItem"/>
    /// call away from publishing the resolution stamp. This is therefore a short rendezvous, not an
    /// open-ended wait. Cancellation prevents a misbehaving policy call from parking shutdown forever.
    /// </summary>
    void WaitForRecoveryHandoff(long generation, CancellationToken cancellationToken)
    {
        var spin = new SpinWait();
        while (!_activationGate.IsHandoffResolved(generation) && !cancellationToken.IsCancellationRequested)
            spin.SpinOnce();
    }

    // Retire an item that completes outside the advance (an owned inline completion, a recovery
    // substitute, a no-recovery fault, a shutdown bailout). Runs the retirement effects and fires
    // the depth-0 idle signal.
    void RetireItem(T item, Exception? exception, long ownedTurn)
    {
        var reachedZero = RetireItemDeferred(item, exception, ownedTurn);
        if (reachedZero)
            _depthState.OnDepthReachedZero();
    }

    /// Applies retirement while deferring the empty signal until the advance exits. This prevents an
    /// empty observer from resuming while retirement is still in progress.
    bool RetireItemDeferred(T item, Exception? exception, long ownedTurn)
    {
        var depth = _depthState.DecrementDepth();
        // Keep the zero comparison exact: <= would hide a double retirement and corrupt empty signaling.
        Debug.Assert(depth >= 0, "Pipeline depth under-ran: double completion for a single enqueue.");
        // In-order retirement makes depth zero the only safe clear: no activated owner remains. A
        // transient null between items is safe because activated work cannot run in that gap.
        if (depth is 0)
        {
            SetActivatedItem(default!);
        }
        // Owner-checked release cannot clear a successor's turn.
        if (ownedTurn != 0)
            _activationGate.Release(ownedTurn);
        _policy.CompleteItem(item, depth, exception);
        return depth is 0;
    }

    /// Commits a counted item after its activation handoff has resolved. A zero preceding count is
    /// the frontier: under the edge lock, assign or inherit its turn before publishing, then arm its
    /// completion. Mid-chain commits publish and arm without touching the held frontier turn.
    // A self-activated item or empty-edge pass owns the negative generation; a failed reclaim owns none.
    static long GetActivationOwner(bool activated, bool ownsTurn, long generation)
        => activated || !ownsTurn ? -generation : 0;

    void CommitInFlightItem(T item, bool activateAtCommit, bool ownsTurn, long turnGeneration, ValueTask pipelineTask)
    {
        Debug.Assert(!_activationGate.HasHandoff, "Commit with an unresolved activation handoff.");
        var prev = _inFlight.IncrementCommitCount();

        if (prev == 0)
        {
            // The edge guarantees a sole head; the executor still owns the item and its token
            // (unpublished), so the status read and the registration below are contract-clean.
            if (activateAtCommit && !pipelineTask.IsCompleted)
            {
                // An incomplete head activates while still exclusively unpublished. A completed head
                // needs only callback delivery to drive advancement.
                ActivateHeadItem(item, preferAsync: false);
            }
            // Sole head: no claim can run before our publish, so the ordinal read is stable.
            var sequence = _itemTenure.HeadSequence;
            if (ownsTurn && _inFlight.TryAcquireAdvanceIfFree())
            {
                // Hold advance ownership through turn assignment, publication, and callback attachment.
                _activationGate.CommitTurn(turnGeneration, sequence);
                _inFlight.PublishCommitted(item, pipelineTask, out _);
                RegisterAdvanceCallback(pipelineTask, sequence);
                if (_inFlight.ReleaseAdvance())
                    Advance();
                return;
            }
            // With a contended or edge-owned frontier, attach before publication, serialize assignment
            // with the edge pass, then acquire or deposit work for the current license holder.
            RegisterAdvanceCallback(pipelineTask, sequence);
            // A pass may still hold the edge after its generation became stale; serialize with its exit.
            var edgeLock = _activationGate.EdgeLock;
            edgeLock.Enter();
            try
            {
                _activationGate.CommitTurn(turnGeneration, sequence);
                _inFlight.PublishCommitted(item, pipelineTask, out _);
            }
            finally
            {
                edgeLock.Exit();
            }
            if (_inFlight.TryAcquireAdvanceOrRequest())
                Advance();
            return;
        }

        // Mid-chain publication uses acquire-or-deposit. A live advancer consumes the deposit on
        // release; otherwise this caller wins the license and drives. The count-word CAS also
        // supplies the StoreLoad edge between publication and the license decision.
        _inFlight.PublishCommitted(item, pipelineTask, out _);
        if (_inFlight.TryAcquireAdvanceOrRequest())
            Advance();
    }

    /// Returns whether advance may continue. A pending substitute stops it and resumes after retirement.
    /// <paramref name="emptyReached"/> defers depth-zero notification until the active advance exits.
    [MethodImpl(MethodImplOptions.NoInlining)]
    // The caller consumed the pipeline task (the consume-before-decrement ordering, see Advance) and hands
    // over the extracted exception.
    bool RecoverInFlightItem(T failedItem, Exception ex, long claimedSequence, out bool emptyReached)
    {
        emptyReached = false;

        // Recovery-item faults complete directly; policy is never asked to recover its own substitute.
        if (ex is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            emptyReached = RetireItemDeferred(failedItem, recoveryFault.InnerException, ownedTurn: claimedSequence);
            return true;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex);
        T? recoveryCandidate;
        bool recovered;
        try
        {
            recovered = _policy.TryRecoverItemFailure(context, failedItem, _enumerator.CompletionToken, out recoveryCandidate);
        }
        catch (Exception recoveryPolicyException)
        {
            emptyReached = RetireItemDeferred(failedItem, recoveryPolicyException, ownedTurn: claimedSequence);
            throw;
        }

        if (!recovered)
        {
            emptyReached = RetireItemDeferred(failedItem, ex, ownedTurn: claimedSequence);
            return true;
        }
        var recoveryItem = recoveryCandidate!;

        // Recover in place while this position retains its count credit, excluding a concurrent
        // frontier activation. Transfer the claimed tenure directly to the substitute; deferring it
        // behind the failed head would create a liveness cycle.
        _activationGate.AssignTurnForRecovery(claimedSequence);
        ActivateHeadItem(recoveryItem, preferAsync: true);
        _inFlightRecoverySequence = claimedSequence;

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            // The in-flight drain may run this recovery concurrently with the executor's next
            // dispatch, so a policy must not share executor-local per-dispatch state with it.
            executeTask = _policy.ExecuteItemAsync(recoveryItem, pipelineTaskRecovery: true, _enumerator.CompletionToken);
        }
        catch (Exception recoveryEx)
        {
            emptyReached = RetireItemDeferred(recoveryItem, recoveryEx, ownedTurn: claimedSequence);
            return true;
        }

        // Publish the recovery item so the bailout path can complete it on shutdown.
        // The recovery continuation always completes this item itself (via CompleteRecoveryItem
        // on the normal path or BailoutRecoveryOnShutdown on shutdown), so no atomic claim race
        // with teardown is needed; teardown just waits for the advance to drain.
        _inFlightRecoveryItem = recoveryItem;

        if (executeTask.IsCompletedSuccessfully)
        {
            return RecoverInFlightItemResult(recoveryItem, executeTask.Result, out emptyReached);
        }

        // Execute is async, hook continuation. The advance stops here; the continuation owns the position's
        // count credit and settles it exactly once (ResumeAdvanceAfterRecovery or the shutdown bailout).
        executeTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            // Bailout: wake signal completed while recovery's executeTask was in flight.
            if (_enumerator.CompletionToken.IsCancellationRequested)
            {
                try { executeTask.GetAwaiter().GetResult(); }
                catch { /* shutdown in progress, exception observed and discarded */ }
                BailoutRecoveryOnShutdown();
                return;
            }

            PipelineItemResult result;
            try
            {
                result = executeTask.GetAwaiter().GetResult();
            }
            catch (Exception recoveryEx)
            {
                CompleteRecoveryItem(recoveryItem, recoveryEx);
                ResumeAdvanceAfterRecovery();
                return;
            }

            if (!RecoverInFlightItemResult(recoveryItem, result, out var emptyReached))
                return; // pipeline task pending, continuation will resume

            ResumeAdvanceAfterRecovery(emptyReached);
        });
        return false;
    }

    /// Handles a completed recovery execution result. Returns true if the advancer should continue,
    /// false if the recovery's pipeline task is pending (occupying this position).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal on the sync return-true
    /// paths. The async continuation paths (RecoverInFlightItem / the trailing continuation) now capture this
    /// out-param and pass it to ResumeAdvanceAfterRecovery(emptyReached), which fires OnDepthReachedZero
    /// after the advancer release - so a recovery that retires the last in-flight item no longer drops
    /// the depth-0 idle signal (which previously hung a parked WaitForEmptyAsync).
    bool RecoverInFlightItemResult(T recoveryItem, PipelineItemResult result, out bool emptyReached)
    {
        emptyReached = false;
        // Await trailing execution, must observe the result.
        if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
        {
            if (result.TrailingExecutionTask.IsCompleted)
            {
                try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                catch (Exception ex)
                {
                    emptyReached = CompleteRecoveryItemDeferred(recoveryItem, ex);
                    return true;
                }
            }
            else
            {
                var pipelineTask = result.PipelineTask;
                result.TrailingExecutionTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
                {
                    Exception? trailingEx = null;
                    try { result.TrailingExecutionTask.GetAwaiter().GetResult(); }
                    catch (Exception ex) { trailingEx = ex; }

                    // Bailout: wake signal completed while recovery's trailing task was in flight.
                    if (_enumerator.CompletionToken.IsCancellationRequested)
                    {
                        BailoutRecoveryOnShutdown();
                        return;
                    }

                    if (trailingEx is not null)
                    {
                        CompleteRecoveryItem(recoveryItem, trailingEx);
                        ResumeAdvanceAfterRecovery();
                        return;
                    }

                    if (!RecoverInFlightPipelineTask(recoveryItem, pipelineTask, out var emptyReached))
                        return; // pipeline task pending, its continuation owns the count credit

                    ResumeAdvanceAfterRecovery(emptyReached);
                });
                return false;
            }
        }

        return RecoverInFlightPipelineTask(recoveryItem, result.PipelineTask, out emptyReached);
    }

    /// Handles the recovery item's pipeline task. Returns true if done, false if pending.
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal to the caller on the sync
    /// return-true path; callers capture it and pass it to ResumeAdvanceAfterRecovery (fires
    /// OnDepthReachedZero after the advancer release). The async return-false path completes via the
    /// NON-deferred CompleteRecoveryItem inside its own continuation, which fires OnDepthReachedZero
    /// inline - so the depth-0 signal is delivered on every path.
    bool RecoverInFlightPipelineTask(T recoveryItem, ValueTask pipelineTask, out bool emptyReached)
    {
        emptyReached = false;
        if (pipelineTask.IsCompleted)
        {
            Exception? pipelineException = null;
            try
            {
                pipelineTask.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                pipelineException = ex;
            }
            emptyReached = CompleteRecoveryItemDeferred(recoveryItem, pipelineException);
            return true;
        }

        // Pipeline task pending, hook continuation. The count credit stays with the continuation.
        pipelineTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(() =>
        {
            Exception? pipelineException = null;
            try
            {
                pipelineTask.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                pipelineException = ex;
            }
            if (_enumerator.CompletionToken.IsCancellationRequested)
            {
                // Bailout: wake signal completed while recovery's pipeline task was in flight.
                BailoutRecoveryOnShutdown();
                return;
            }

            CompleteRecoveryItem(recoveryItem, pipelineException);
            ResumeAdvanceAfterRecovery();
        });
        return false;
    }

    /// Shared shutdown bailout for recovery continuations. Completes the recovery item with the shutdown
    /// exception, settles its count credit, and signals teardown. The continuation always owns completion
    /// (no race with teardown) because teardown only waits for the advance to drain and does not compete for
    /// the recovery item.
    void BailoutRecoveryOnShutdown()
    {
        var recoveryItem = _inFlightRecoveryItem;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _inFlightRecoveryItem = default!;
        var emptyReached = RetireItemDeferred(
            recoveryItem, _completionException, ownedTurn: _inFlightRecoverySequence);
        // Shutdown still has to leave through the episode's advance license. This settles the retained
        // count credit and continues retiring successors already resident behind the recovery.
        ResumeAdvanceAfterRecovery(emptyReached);
    }

    /// Signals teardown after the advance drains. Volatile prevents the optional slot read from
    /// being cached or hoisted; .NET has no relaxed atomic load for this weaker requirement.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SignalDrainWakeupIfWaiting()
        => Volatile.Read(ref _advanceDrainWaiter)?.TrySetResult();

    /// Completes the recovery item on the normal (non-shutdown) recovery path. The continuation
    /// owns completion uncontested. Drain (DrainOnCompletionAsync) only waits for the advancer
    /// advance to drain via ResumeAdvanceAfterRecovery's exit signal.
    void CompleteRecoveryItem(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _inFlightRecoveryItem = default!;
        RetireItem(recoveryItem, exception, ownedTurn: _inFlightRecoverySequence);
    }

    /// Completes recovery while deferring the empty signal until the advance exits.
    bool CompleteRecoveryItemDeferred(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _inFlightRecoveryItem = default!;
        return RetireItemDeferred(recoveryItem, exception, ownedTurn: _inFlightRecoverySequence);
    }

}
