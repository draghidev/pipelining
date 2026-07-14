using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// <summary>
/// Source-driven pipelined request/response coordinator. Processes each item from the source
/// through the policy's lifecycle (execute/activate/complete/recover). The source's enumerator owns
/// cancellation and completion; <see cref="CompleteAsync"/> drives shutdown by signalling it.
/// </summary>
/// <remarks>
/// <see cref="CompleteAsync"/> and <see cref="GetEnumerator"/> are not thread-safe - serialize
/// external callers. <see cref="Depth"/> is a lock-free read safe from any thread, but like a
/// concurrent collection's count it is a snapshot that may be stale on return. Internally the class
/// IS concurrent: the execution loop runs on its own scheduler thread, the advancer fires on
/// threadpool continuations from waiter-task completions, and recovery spans async boundaries.
/// </remarks>
public sealed class Pipeline<T, TPolicy, TSource, TEnumerator>
    where TPolicy : IPipelinePolicy<T>
    where TSource : IPipelineSource<T, TEnumerator>
    where TEnumerator : struct, IPipelineEnumerator<T>
{
    TPolicy _policy;
    TSource _source;
    TEnumerator _enumerator;

    // Field-initialized to a completed sentinel so the first Initialize from the constructor sees a
    // "completed previous run" and proceeds. On run completion ExecuteSource swaps it back to
    // Task.CompletedTask, releasing the prior ExecuteSource task box.
    Task _executionTask = Task.CompletedTask;
    // Shutdown coordination over a single slot, claimable by either an external CompleteAsync
    // caller or by the executor's terminal cleanup (self-shutdown via external CT cancellation).
    // Slot values:
    //   null                  = None: no shutdown in progress.
    //   _shutdownInFlight     = a CompleteAsync caller is mid-call to _enumerator.Complete.
    //   _shutdownDone         = caller (or executor on the no-caller path) finished. The
    //                            terminal cleanup may dispose.
    //   <a TaskCompletionSource> = executor's lazily-installed TCS that the caller must signal.
    // Transitions use paired Interlocked CompareExchange / Exchange (full fences), so the
    // publish-and-signal handoff has no StoreLoad reorder hazard. Allocates one TCS per shutdown
    // when a caller is in flight, none when the executor wins the CAS.
    TaskCompletionSource? _shutdownSlot;
    // Identity-only sentinels. Never signaled; their Task is never observed. The Exchange-result
    // check uses ReferenceEquals to distinguish them from a real executor-published TCS.
    static readonly TaskCompletionSource _shutdownInFlight = new();
    static readonly TaskCompletionSource _shutdownDone = new();
    Pipeline.DepthState _depthState;
    Exception? _completionException;

    // Executor-owned, only touched by the execution loop.
    // Pending tail, the most recent waiter, held outside the queue so the executor can swap it
    // for a recovery item if the trailing task fails. Committed to the waiter queue on the next
    // loop iteration.
    T _tailWaiter = default!;
    bool _hasTailWaiter;
    ValueTask _tailWaiterTask;
    // Published by the execution loop for the enumerator/heartbeat surface. NOTE: the deferral's
    // grant identity does NOT ride this field - it rides WaiterStore._execDeferredItem, pinned to
    // the versioned _execWord, so the census grants exactly the consumed deferral's item (capture-
    // before-take off this field was unsound: a suspended census could read a stale _executingItem
    // while consuming a recycled deferral). The executor places the deferral (with its item) on the
    // versioned word; the census version-pin-consumes-and-grants, the executor race-back / reclaim
    // plain-consumes its own. See WaiterStore's exec-word section.
    T _executingItem = default!;
    // Seqlock generation for the public ExecutingItem getter under value-type T (see PublishSlot /
    // ReadSlot). Unused for reference T (single-word atomic). _executingItem has a sole writer (the
    // executor strand; recovery is awaited inline on that same strand), so the seqlock's single-writer
    // assumption holds.
    uint _executingItemGen;

    /// <summary>
    /// The item currently holding the executor's single-pump slot, or default if none. The
    /// pipeline assigns this before <see cref="IPipelinePolicy{T}.ExecuteItemAsync"/> is called
    /// and keeps it through the trailing task's tail (a tail-waiter item retains the slot until
    /// the next iteration's <see cref="CommitTailWaiter"/>); recovery republishes to the
    /// substitute when <see cref="IPipelinePolicy{T}.TryRecoverItemFailure"/> returns true, and
    /// leaves the slot cleared when it returns false.
    /// </summary>
    /// <remarks>
    /// The single-pump invariant (only one item holds this slot at a time, ever) is what makes the
    /// pipeline an item-sequencer: policies use this identity as the single source of truth so they
    /// can omit their own bookkeeping or locking primitive.
    /// </remarks>
    public T ExecutingItem => ReadSlot(ref _executingItem, ref _executingItemGen);

    /// The most recently activated item (set whenever the pipeline calls
    /// <see cref="IPipelinePolicy{T}.ActivateHeadItem"/>). The Activate-before-Complete in-order
    /// invariant means this identifies the item currently in its post-activation phase. Held across
    /// the brief window between completion of the prior item and activation of the next, never reset
    /// to default while the pipeline is live, so a completion-thread callback racing the activation
    /// thread always sees a valid reference rather than a transient null.
    T _activatedItem = default!;
    // Seqlock generation for the public ActivatedItem getter under value-type T. Unused for reference
    // T. The writers of _activatedItem (ActivateHeadItem, the depth-0 clear, the post-drain reset)
    // run on different strands but never overlap for a live slot, serialized by the in-order
    // retirement discipline + Complete-before-DecrementCount.
    uint _activatedItemGen;

    /// <summary>
    /// The most recently activated item, or default if no activation has occurred yet. Mirror of
    /// <see cref="ExecutingItem"/> for the post-activation phase: ActivatedItem identifies whichever
    /// item is currently in its post-activation phase under the in-order Activate-before-Complete
    /// discipline. Policies use this identity to omit their own current-item bookkeeping (read-channel
    /// or response-writer ownership) - the executor's in-order activation IS that bookkeeping.
    /// </summary>
    /// <remarks>
    /// Holds the prior activation's reference across the brief gap between Complete(prev) and
    /// Activate(next): a stale reference there is safe because in-flight processing only happens during
    /// an activated item's phase, when the read sees a usable identity.
    /// </remarks>
    public T ActivatedItem => ReadSlot(ref _activatedItem, ref _activatedItemGen);
    // Visibility-only flag for GetEnumerator. Set on dispatch in both branches (waiters=0 and
    // waiters>0) so heartbeat-style consumers can act on the in-flight item before it transitions
    // to a tail-waiter slot. Cleared on every transition that gives the enumerator another channel
    // to see the item (sync completion, tail-waiter committal, recovery). Kept separate from the
    // exec-word deferral so the advancer C-path's claim semantics on that carrier stay unchanged.
    bool _hasInFlightItem;

    // Cross-thread atomics, touched by the executor, advancer, enqueuer, and completion callbacks.
    // The store has a single inline slot (zero alloc) for the common one-pending-waiter case. The
    // SPSC queue inside it is lazy-allocated only on true overlap (a second waiter arrives while
    // the first is still pending). See WaiterStore<T>.
    WaiterStore<T> _waiters;

    T _waiterRecoveryItem = default!; // The item being recovered, for the bailout/completion paths to access.
    long _waiterRecoverySeq;          // The recovery position's claim ordinal - the turn identity its completion releases.
    bool _tailWaiterActivated;        // The tail item was activated on the executor strand (elision/race-back): its turn is TurnExec.
    long _tailWaiterGen;              // The tail item's deferral grant gen (0 if elision/never deferred): its census-grant identity for the commit inherit + owned-turn release.
    TaskCompletionSource? _drainWakeupTcs; // Set by DrainOnCompletionAsync while waiting for the advance chain to drain _waiters. Signaled by the advancer's count-0 exit (and recovery bailout) so teardown can re-check.

    // The single fixed head-continuation trampoline (the advance-fire). Registered on the current head's
    // task by the granting advancer/arm; its fire re-enters the advance. Replaces the eager per-item
    // completion callback: only the current head ever carries a continuation (single-continuation
    // invariant), so the runtime's own continuation slot is the lossless completion-side wake arbiter -
    // no latch, no drain signal, no deposit/serve machinery.
    readonly Action _onAdvanceFire;


    internal Pipeline(TPolicy policy, TSource source)
    {
        // Delegate references bind to `this` and don't change.
        _onAdvanceFire = OnAdvanceFire;
        // Explicit store construction: the count word is biased (+1) and a default-initialized
        // struct would read count -1 (see WaiterStore's ctor).
        _waiters = new();
        Initialize(policy, source);
    }

    /// Per-run init: sets policy + source, resets transient executor state, creates a fresh enumerator
    /// and execution task. Called from the constructor and by callers for flyweight reuse. Throws if the
    /// previous run hasn't fully completed (first call uses a completed sentinel).
    [MemberNotNull(nameof(_policy), nameof(_source))]
    internal void Initialize(TPolicy policy, TSource source)
    {
        if (!_executionTask.IsCompleted)
            throw new InvalidOperationException("Cannot re-initialize a pipeline whose previous run hasn't fully completed. Await CompleteAsync's returned task first.");

        _policy = policy;
        _source = source;
        _shutdownSlot = null;

        // Depth is counted at DISPATCH (the executor's single-consumer pull), not at enqueue, so the
        // source needs no depth-increment hook. Pipeline doesn't pass a CT here: source owns
        // its own cancellation lifecycle (caller-configured at source construction).
        _enumerator = _source.GetAsyncEnumerator();
        _executionTask = ExecuteSource();

        // Other per-run fields are left at default: field-initialized on first run, or zeroed by the
        // previous run's ExecuteSource-exit cleanup.
    }

#if DEBUG
    // Executor-strand tripwire for the recovery swap (RecoverTrailingFailure). Recovery runs on the
    // executor's logical strand, so no two recovery swaps overlap; this fails loud if a future
    // multi-strand recovery restructure breaks that premise. NOT a lock - the unpublish-first exec-word
    // ordering protects the swap against the lock-free claimants. Debug-only.
    int _recoverySwapGuard;
#endif


    /// <summary>Current in-flight count: items dispatched by the executor but not yet completed.
    /// Excludes the source backlog (enqueued but not yet dispatched); a queue-backed pipeline exposes
    /// that as <see cref="QueuedPipeline{T,TPolicy}.Backlog"/>, and <c>Depth + Backlog</c> is the total
    /// outstanding. Lock-free read, may be stale by the time the caller observes it. Use
    /// <see cref="WaitForEmptyAsync"/> to await empty (both halves zero).</summary>
    public int Depth => _depthState.Depth;

    /// Diagnostic readout of the store's two activation word cells, for test forensics.
    internal string DebugWordStates() => _waiters.DebugWordStates();

    /// <summary>Completion of the current run: completes when the run has fully torn down, faults
    /// when the run breaks, the same task <see cref="CompleteAsync"/> returns. Unlike CompleteAsync
    /// this does not initiate completion, so an embedder can observe a live pipeline. Between runs
    /// this reads as a completed task; a faulted run keeps its fault here until the next run
    /// starts.</summary>
    public Task Completion => _executionTask;

    // Brackets the executor's dequeue -> IncrementDepth window, during which the in-flight item is
    // in NEITHER gauge (left the source's backlog, not yet in Depth). Emptiness conclusions require
    // Backlog==0 && Depth==0 && !IsPulling && Depth==0 (re-read) IN THAT ORDER: a false bit read
    // either precedes the pull (the backlog check saw the item) or follows the count (the Depth
    // re-read sees it or its completion). Executor-owned single-writer; volatile for the cross-
    // thread emptiness readers. Kept OUT of Depth so the gauge stays honest (no phantom +1).
    bool _pulling;
    internal bool IsPulling => Volatile.Read(ref _pulling);

    /// <summary>
    /// Waits for the pipeline to be empty: both in-flight (<see cref="Depth"/>) and backlog
    /// (enqueued but not yet dispatched) at zero. Does not prevent new items from being yielded by
    /// the source.
    /// </summary>
    /// <remarks>
    /// Depth counts in-flight only (dispatched - completed), so the backlog half is invisible to the
    /// generic pipeline: the caller passes the current backlog snapshot from its source. A
    /// queue-backed wrapper (<see cref="QueuedPipeline{T,TPolicy}"/>) supplies its source's queue
    /// length. The completion fires from inside <see cref="IPipelinePolicy{T}.CompleteItem"/> or from
    /// the executor's genuine-suspend seam, so the executor may still be inside its inner drain loop
    /// and threadpool-resident advancer continuations may still be unwinding when this returns. For
    /// strict "fully quiet" GC-collectability semantics use <see cref="CompleteAsync"/>.
    /// </remarks>
    internal ValueTask WaitForEmptyAsync(int backlog, CancellationToken cancellationToken = default)
        => _depthState.GetIdleTask(backlog, cancellationToken);

    /// Post-arm idle re-check with a fresh backlog snapshot - see DepthState.RecheckIdle
    /// (closes the stale-backlog arm strand for callers that re-arm in a loop).
    internal void RecheckEmpty(int backlog) => _depthState.RecheckIdle(backlog);

    /// <summary>
    /// Initiates pipeline shutdown. First-writer wins: subsequent calls return the same execution task.
    /// The returned task completes when the executor loop has fully drained and exited.
    /// </summary>
    /// <param name="exception">
    /// Optional exception delivered to any items still in flight when shutdown drains them
    /// (via <see cref="IPipelinePolicy{T}.CompleteItem"/>'s exception parameter). Note: this exception is
    /// not propagated through the returned task. It only flows to items.
    /// </param>
    /// <remarks>
    /// Awaiting the returned task gives "fully quiet" semantics: all items are completed, the executor
    /// has exited, and the advancer chain (including in-flight recovery continuations) has unwound -
    /// drain coordinates with the advancer via an idle TCS so any remaining continuation work
    /// is observed before drain returns.
    /// </remarks>
    public ValueTask CompleteAsync(Exception? exception = null)
    {
        // First-claim wins (CAS null -> InFlight). Subsequent callers (or a late call after the
        // executor self-shut-down via external CT cancellation, which transitions the slot to Done
        // from the terminal cleanup) just return the executor task.
        if (Interlocked.CompareExchange(ref _shutdownSlot, _shutdownInFlight, null) != null)
            return new(_executionTask);

        _completionException = exception;
        // Complete() signals the enumeration to wind down: cancels CompletionToken (firing policy
        // CT for in-flight ExecuteItemAsync calls) and signals the source's wake/completion
        // mechanism via the registered callback. The enumerator stays usable for drain reads.
        // DisposeAsync runs at the end of the executor's main loop as the terminal cleanup.
        _enumerator.Complete();

        // Transition InFlight -> Done. The Exchange picks up whatever was in the slot - either the
        // InFlight sentinel we put there (executor hasn't shown up) or a TCS the executor
        // published while we were inside _enumerator.Complete. In the latter case we signal it so
        // the executor unblocks. The Exchange's full fence pairs with the executor's CASes (see
        // _shutdownSlot comment) - one of the two sides always observes the other's transition.
        var swapped = Interlocked.Exchange(ref _shutdownSlot, _shutdownDone);
        if (swapped is not null && !ReferenceEquals(swapped, _shutdownInFlight))
            swapped.TrySetResult();
        return new(_executionTask);
    }

    /// <summary>Returns an enumerator over all items currently in the pipeline, from oldest to newest.</summary>
    /// <remarks>
    /// Best-effort under concurrent mutation. Both the execution queue and the waiters queue may
    /// be mutated by the execution loop or the advancer, pausing enqueues alone is not sufficient.
    /// For reference types, null checks filter out cleared queue slots.
    /// For value types the enumerator may yield default(T) values from slots that were concurrently dequeued,
    /// or torn values for types that are not atomically writable.
    /// Use a reference type for T if you need more reliable enumeration.
    /// </remarks>
    public Enumerator GetEnumerator() => new(this);

    async Task ExecuteSource()
    {
        // Promoted out of the loop so the post-loop clear below can null them out.
        T item;
        PipelineItemResult itemResult;
        // Root cause of an executor break, captured (stack preserved) so the teardown drain/dispose
        // in the finally can't mask it, then rethrown after teardown so it faults _executionTask.
        ExceptionDispatchInfo? fault = null;
        try
        {
            // The CTS-cancelled check belongs to the source's MoveNextAsync, not here: even after
            // CompleteAsync fires, items admitted before Complete() must still be processed. The
            // source returns false once its queue is empty and its wake signal is completed.
            while (true)
            {
                // Commit the previous iteration's pending tail to the waiter queue BEFORE waiting
                // on MoveNextAsync. If the source has nothing more to yield, the tail would
                // otherwise sit in _tailWaiter forever with no UnsafeOnCompleted callback wired,
                // and a producer that completes its pipeline task would have nobody listening.
                // CommitTailWaiter is sync in all paths except trailing recovery, where it
                // returns a Task to await.
                var commitWork = CommitTailWaiter();
                if (commitWork is not null)
                {
                    // Cold suspension (trailing recovery): clear the per-item locals first, same
                    // retention rationale as the wait-path clear below.
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        item = default!;
                    itemResult = default;
                    await commitWork.ConfigureAwait(false);
                }

                // The pull seam: a backlogged item is taken synchronously by TryGetNext (lock-free
                // on the queue source, no ValueTask machinery, no Current store). A miss goes to
                // WaitForNextAsync, whose awaitable re-checks under the source's wake lock and
                // either resolves synchronously (retry/completed) or suspends on the bare-delegate
                // wait protocol - the wake invokes the executor's continuation directly, with no
                // value-task-source dispatch in between.
                //
                // _pulling brackets the dequeue -> IncrementDepth window: between the dequeue and
                // the increment the item is in NEITHER gauge (gone from the source's backlog, not
                // yet in Depth), so a concurrent WaitForEmptyAsync reading both gauges zero in that
                // window would return while an item was in the executor's hands
                // (NoTwoItemsActivatedConcurrently iter 2120). Emptiness conclusions therefore also
                // require !_pulling AND a Depth RE-READ after the bit read (see QueuedPipeline.
                // WaitForEmptyAsync): a false bit read either precedes this set (the backlog check
                // then still saw the item) or follows the clear below (the Depth re-read then sees
                // the increment or the item's genuine completion). Depth itself stays HONEST - a
                // first cut counted speculatively and rolled back on miss, but the transient +1
                // leaked into every Depth consumer and suppressed completer zero-fires; reverted.
                Volatile.Write(ref _pulling, true);
                if (!_enumerator.TryGetNext(out item!))
                {
                    Volatile.Write(ref _pulling, false);
                    // About to (possibly) suspend. item/itemResult are hoisted into the executor's
                    // state-machine box (they live across the in-iteration awaits), so without this
                    // clear the box keeps the last-processed item and its tasks GC-rooted across
                    // the whole idle period (the post-loop clear only runs on termination).
                    // Clearing only on the miss path keeps the backlogged loop free of these dead
                    // stores. Both are reassigned before any use, and the recovery `continue`
                    // paths route back through here too.
                    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                        item = default!;
                    itemResult = default;
                    // Same reasoning as the clear above, for WaiterStore's deferred-item field: no
                    // consume site clears it (see ClearStaleExecDeferredItem's doc), so a stale
                    // reference from an already-resolved deferral would otherwise sit GC-rooted for
                    // the whole idle period. Safe here specifically because this is the sole writer
                    // strand, about to suspend.
                    _waiters.ClearStaleExecDeferredItem();

                    // Drain-empty fire. The backlog half of "empty" (enqueued - dispatched == 0) is
                    // only observable to the single consumer, and only at the moment it commits to a
                    // genuine suspend: WaitForNextAsync re-checked the source empty under the wake
                    // lock and armed (Awaiter.IsCompleted == false). A synchronous resolve
                    // (retry/completed, IsCompleted == true) means an item may be present, so no fire.
                    // At a genuine arm with Depth 0, in-flight is also empty: fire the drain waiter.
                    // OnDepthReachedZero revalidates Depth and is idempotent against a racing completer.
                    // A producer that enqueues after the arm trips Signal, which claims this wait and
                    // wakes us, so there is no parked state with an item queued and unsignalled.
                    var wait = _enumerator.WaitForNextAsync();
                    var waitAwaiter = wait.GetAwaiter();
                    if (!waitAwaiter.IsCompleted && _depthState.Depth is 0)
                        _depthState.OnDepthReachedZero();
                    if (!await wait)
                        break;
                    continue;
                }

                // Count depth at DISPATCH: a source-yielded item just became in-flight. This is the
                // single-consumer chokepoint (the executor), so the increment is single-writer - no
                // producer race, no enqueue-side serialization. Recovery substitutes do NOT increment
                // here (they republish SetExecutingItem for the failed item's already-counted slot).
                // The _pulling clear comes AFTER the increment: the item must be in at least one of
                // {backlog, Depth, _pulling} at every instant (the emptiness predicate's invariant).
                _depthState.IncrementDepth();
                Volatile.Write(ref _pulling, false);

                // Publish _executingItem and the visibility flag before either activation path.
                // _hasInFlightItem signals GetEnumerator that the in-flight item is yieldable during
                // the dispatch window where it isn't tracked anywhere else.
                SetExecutingItem(item);
                Volatile.Write(ref _hasInFlightItem, true);

                var activated = false;
                // The grant generation for THIS item (its deferral's gen, or a freshly-minted gen
                // for the inline elision). It is the item's turn identity (-execGen) for the whole
                // dispatch: self-act, census grant, commit-convert, and completion-release all key
                // off it, so a committing item only ever inherits its own grant.
                long execGen = 0;
                // Idle-regime elision. At count 0 with no deferral published there is no concurrent
                // activation decider: the C-path needs the deferral (executor-owned, absent here); a
                // advancer exists only while retiring resident waiters (count > 0), so count 0 excludes a
                // mid-chain advancer; and a advancer at its idle-edge exit touches no activation state unless
                // it is granting the deferral, which the deferral check screens. Also covers the
                // pre-first-commit case (no advancer has ever run). A stale-nonzero count read only
                // declines, the conservative direction.
                if (_waiters.Count is 0 && !_waiters.ExecDeferredVisible)
                {
                    // Inline self-activation elision (no deferral). FAIL-IF-LIVE TurnExec claim: the
                    // gate excludes a resident, but a census may have granted a non-resident (turn
                    // live at count 0), so the turn - not the stale gate - is the authority. On a win
                    // activate inline; on a loss (census grant live) defer, and a later stop covers it.
                    var elideGen = _waiters.NextGrantGen();
                    if (_waiters.TryClaimTurnGrant(elideGen))
                    {
                        execGen = elideGen;
                        ActivateHeadItem(item, preferAsync: false);
                        activated = true;
                    }
                    else
                    {
                        execGen = _waiters.PlaceExecDeferred(item);
                    }
                }
                else
                {
                    // An advancer may be draining concurrently. Publish the deferral (its gen is the
                    // grant identity a census stamps and this item inherits at commit), full-fence
                    // StoreLoad, then the CLAIM-FIRST race-back (matches the census's own claim-then-
                    // consume order exactly - see TryReclaimExecDeferred's doc for why the opposite
                    // order double-bails and strands the item). A claim loss (a census granting this
                    // same gen) declines outright; this item routes as a resident for a later stop.
                    execGen = _waiters.PlaceExecDeferred(item);
                    Interlocked.MemoryBarrier();
                    if (_waiters.Count is 0 && _waiters.TryReclaimExecDeferred(execGen))
                    {
                        ActivateHeadItem(item, preferAsync: false);
                        activated = true;
                    }
                }

                try
                {
                    itemResult = await _policy.ExecuteItemAsync(item, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    var owned = ClearExecutingItem(activated);
                    var failedTurn = ExecOwnedTurn(activated, owned, execGen);
                    await RecoverItem(item, activated, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), failedTurn, _enumerator.CompletionToken).ConfigureAwait(false);
                    continue;
                }

                // Sync shortcut: only taken when both tasks are already observed successful at dispatch
                // time. Items with a non-default trailing task fall through to the tail-waiter path until
                // their trailing is also sync-complete (default(ValueTask) is success, so items without a
                // trailing keep the fast path). This is how the framework guarantees CompleteWaiter doesn't
                // fire before trailing is observed.
                //
                // Gated on _waiters.Count is 0 to keep retirement strictly in-order: the store plus the
                // head-gated drain is a reorder buffer (entries complete out of order, buffer, and retire
                // in program order from the head). This inline shortcut bypasses the ROB, so it's only
                // safe when the ROB is empty (Count is 0 = this item is the head). With an earlier entry
                // buffered, retiring inline would jump ahead of it, so we fall through to route through
                // the ROB instead. The shortcut is a perf optimization, not a correctness path - an
                // ordered transport never produces a later item sync-completing before an earlier one.
                if (_waiters.Count is 0
                    && itemResult.PipelineTask.IsCompletedSuccessfully && itemResult.TrailingExecutionTask.IsCompletedSuccessfully)
                {
                    if (ClearExecutingItem(activated))
                    {
                        // Owned completion: activated on this strand, or the reclaim won (no grant
                        // exists or ever can). The token and the turn are ours.
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                        CompleteWaiter(item, null, ownedTurn: ExecOwnedTurn(activated, owned: true, execGen));
                    }
                    else
                    {
                        // Reclaim LOST: a census grant is mid-flight or landed. NEVER inline-complete a
                        // lost item - route it through the store: the census advancer acts under the
                        // license, and the advance's claim needs that same license, so the activation is
                        // ordered before the retire (the deleted observe-rendezvous's happens-before,
                        // carried by license ordering instead of a spin).
                        CommitWaiter(item, sentinelHeld: true, own: false, grantGen: execGen, itemResult.PipelineTask);
                    }
                }
                else if (itemResult.PipelineTask.IsCompleted && !itemResult.PipelineTask.IsCompletedSuccessfully)
                {
                    // Pipeline task faulted synchronously. Recovery path. The failed item's
                    // TRAILING task may still be in-flight (the per-iteration await at the
                    // loop's bottom is skipped via `continue` below); pass it through the
                    // failure context so the substitute can sequence against it before
                    // touching the shared output - else recovery writes race the still-
                    // running trailing flush on the writer. Pass the ValueTask directly (no
                    // AsTask conversion): the recovery is the sole awaiter, single-consume
                    // is preserved by construction.
                    var ownedFault = ClearExecutingItem(activated);
                    var faultedTurn = ExecOwnedTurn(activated, ownedFault, execGen);
                    var outstandingTrailing = itemResult.TrailingExecutionTask;
                    try
                    {
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        await RecoverItem(item, activated, new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex, outstandingTrailing), faultedTurn, _enumerator.CompletionToken).ConfigureAwait(false);
                        continue;
                    }
                }
                else
                {
                    // Tail waiter path. Either the pipeline task is pending, or it's sync-complete but
                    // trailing is pending/faulted - either way completion gates on both. The framework's
                    // trailing-await below stalls the executor before the next CommitTailWaiter (which
                    // fires CompleteWaiter), so trailing is structurally observed before completion.
                    //
                    // Clear _hasInFlightItem BEFORE publishing _hasTailWaiter so a concurrent enumerator
                    // never sees both true at once - the reverse order would let a heartbeat-style
                    // enumerator yield the same item twice (phase 0 and phase 4) and fire the consumer's
                    // per-tick callback twice. _executingItem stays populated for the advancer C-path
                    // when waiters>0; CommitTailWaiter clears it after its reclaim.
                    Volatile.Write(ref _hasInFlightItem, false);
                    _tailWaiter = item;
                    _tailWaiterTask = itemResult.PipelineTask;
                    _tailWaiterActivated = activated;
                    _tailWaiterGen = execGen;
                    Volatile.Write(ref _hasTailWaiter, true);
                }

                if (itemResult.TrailingExecutionTask != default)
                {
                    try
                    {
                        await itemResult.TrailingExecutionTask.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (_hasTailWaiter)
                        {
                            await RecoverTrailingFailure(item, activated, execGen, ex, _enumerator.CompletionToken).ConfigureAwait(false);
                        }
                    }
                }
            }

            // Clear the locals so the executor's async state machine box does not
            // retain the last-processed item and its tasks across termination.
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                item = default!;
            itemResult = default;
            // No post-loop CommitTailWaiter needed: the top-of-loop commit already ran before the
            // MoveNextAsync that returned false.
        }
        catch (OperationCanceledException ex) when (ex.CancellationToken == _enumerator.CompletionToken
            && ex.CancellationToken.IsCancellationRequested)
        {
            // Sanctioned shutdown: a source may signal completion either by returning false from
            // WaitForNextAsync or, the idiomatic IAsyncEnumerator way, by throwing OCE carrying the
            // enumerator's own CompletionToken. Identity-matched so a foreign or untokened OCE is NOT
            // swallowed, AND the token must have actually fired: the legitimate idiom only throws after
            // observing cancellation, while a mistranslated cancellation carrying this token on an
            // un-fired run would otherwise read as shutdown and drop the pump out of a live loop -
            // the teardown then faults on the mid-flight state, skipping the enumerator dispose, and
            // the source wedges open. Not-fired falls through to the capture below and faults loudly
            // with the real origin stack.
        }
        catch (Exception ex)
        {
            // Any other throw from a source/policy/loop seam (TryGetNext, WaitForNextAsync, commit, a
            // recovery that itself escaped) breaks the pipeline. Capture the root cause now, before the
            // teardown drain/dispose runs, so a teardown fault can't mask which seam actually broke.
            // The fault surfaces on _executionTask, observable without initiating completion via
            // Completion, so an embedder can monitor a live pipeline and scope its own response.
            fault = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            // Teardown runs on EVERY exit - clean, sanctioned-shutdown, or fault - so a faulted loop
            // still drains in-flight items (decoupled from the source, they retire normally) and
            // disposes the enumerator, instead of leaking the run. Guarded so a teardown throw is folded
            // into the captured fault rather than replacing it.
            try
            {
                await DrainOnCompletionAsync();

                // Close the shutdown gate before disposing the enumerator. CAS null -> Done:
                //   - Wins (slot was null): no caller in flight; dispose immediately.
                //   - Loses with Done: caller already finished; dispose immediately.
                //   - Loses with InFlight: caller is mid-_enumerator.Complete; install a TCS via a second
                //     CAS over the InFlight sentinel. If that CAS wins, await the TCS - the caller's
                //     Exchange to Done picks our TCS up and signals it. If that CAS loses, the caller
                //     raced to Done between our two CASes, so skip the await.
                var prev = Interlocked.CompareExchange(ref _shutdownSlot, _shutdownDone, null);
                if (prev is not null && !ReferenceEquals(prev, _shutdownDone))
                {
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    if (ReferenceEquals(Interlocked.CompareExchange(ref _shutdownSlot, tcs, _shutdownInFlight), _shutdownInFlight))
                        await tcs.Task.ConfigureAwait(false);
                }
                await _enumerator.DisposeAsync().ConfigureAwait(false);

                // ExecuteSource has fully completed: all items drained, enumerator disposed, advancer
                // chain quiesced. Default the per-run reference-holding fields so a cached-but-idle
                // Pipeline shell doesn't hold them across an idle period. Trimmed to the safe minimum:
                // only reference-typed and reference-containing fields that could be promoted to gen1/2.
                // _shutdownSlot is already at Done from the gate-close above, so a racing CompleteAsync
                // first-call's CAS null -> InFlight fails and they return the execution task without
                // touching the defaulted fields.
                _completionException = null;
                _policy = default!;
                _source = default!;
                _enumerator = default;
                _tailWaiter = default!;
                _tailWaiterTask = default;
                // Public surface: cleared unconditionally so value-type T pipelines don't expose a
                // stale slot value post-shutdown.
                SetExecutingItem(default!);
                SetActivatedItem(default!);
                _waiters.Reset();
                _waiterRecoveryItem = default!;
                _drainWakeupTcs = null;
                // Reset the sentinel ONLY on a clean exit. On a fault, leave _executionTask pointing at
                // this faulting task so CompleteAsync (which returns it) surfaces the fault; rethrow below.
                // A settled async-Task box releases its state machine, so the faulted task roots no
                // per-run state (only a collectable Pipeline self-cycle) - no separate task needed.
                if (fault is null)
                    _executionTask = Task.CompletedTask;
            }
            catch (Exception teardownEx)
            {
                fault = fault is null
                    ? ExceptionDispatchInfo.Capture(teardownEx)
                    : ExceptionDispatchInfo.Capture(new AggregateException(fault.SourceException, teardownEx));
            }
        }

        // Rethrow the captured root (or root + teardown aggregate) with its original stack, faulting
        // _executionTask. No-op on the clean / sanctioned-shutdown paths.
        fault?.Throw();
    }

    /// Waits for the advance chain to drain remaining items after the execution loop exits. The advance is
    /// autonomous (head continuations fire and retire in FIFO order), so teardown does not compete - it
    /// waits for quiescence. Quiescence = count==0 AND no in-flight executor item: keying on count==0
    /// alone is insufficient for the deferred-executor population (a race-back consumes the exec word to
    /// None while the exec item still runs, non-resident with count 0), so the predicate keys on the
    /// executor's lifecycle (_hasInFlightItem), not the exec word. The advancer's count-0 exit signals
    /// _drainWakeupTcs to wake this loop.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        // Wait for _waiters to be empty (every committed item's task completed and the advance retired it)
        // AND no executor item in flight. The count condition makes teardown wait for in-flight pipeline
        // tasks to finish naturally - retiring items ourselves would race the body's still-running task
        // and tear down state it depends on.
        while (_waiters.Count > 0 || Volatile.Read(ref _hasInFlightItem))
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // Full fence, NOT Volatile.Write: this is the arm side of a Dekker-shaped arm-then-check
            // against the advancer's publish-then-signal. A release-only publish lets the re-check's loads
            // hoist ABOVE the store - the check reads a stale Count, the exiting advancer reads the
            // not-yet-visible null tcs, and the wakeup is lost with both sides convinced the other had it.
            // The signal side is fenced by the advancer's DecrementCount, so this is the matching half.
            Interlocked.Exchange(ref _drainWakeupTcs, tcs);
            // Re-check post-publish in case the last advancer exited while we were setting up.
            if (_waiters.Count == 0 && !Volatile.Read(ref _hasInFlightItem))
            {
                Volatile.Write(ref _drainWakeupTcs, null);
                break;
            }
            await tcs.Task.ConfigureAwait(false);
            Volatile.Write(ref _drainWakeupTcs, null);
        }
    }

    /// Handles execution-phase or pipeline-task failures, including recovery.
    /// Recovery items get the full async treatment since they're taking the place of the original item.
    /// <paramref name="activated"/> is the FAILED item's own dispatch-time activation status - it's
    /// what distinguishes "failedTurn is -gen because X was self-activated, no census ever involved"
    /// from "failedTurn is -gen because a census genuinely won X's grant" (ExecOwnedTurn collapses
    /// both to the same -gen encoding; only the caller, which computed `activated` before the fault,
    /// can still tell them apart). Only the latter needs WaitForCensusResolution before X or R is
    /// touched further - a self-activated X was never at risk of a racing census in the first place.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverItem(T item, bool activated, PipelineItemFailureContext context, long failedTurn, CancellationToken cancellationToken)
    {
        // A live failedTurn (-gen) that ISN'T from self-activation can only be a census's grant of
        // this exact gen (ExecOwnedTurn's only other output is TurnNone) - wait for that specific
        // census's ActivateHeadItem(X, ...) to have returned before touching X or R any further, so
        // whatever happens next (complete X directly below, or activate R past this point) is
        // ordered after X's real activation rather than racing it.
        if (!activated && failedTurn != WaiterStore<T>.TurnNone)
            WaitForCensusResolution(-failedTurn, cancellationToken);

        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            CompleteWaiter(item, context.Exception, failedTurn);
            return;
        }

        // Recovery item takes over. Republish _executingItem / _hasInFlightItem (the failed item's
        // ClearExecutingItem cleared them), then mirror the main loop's activation gating:
        // inline-activate only when no prior pipeline task is in flight, otherwise publish for
        // deferred activation. Activating eagerly while a prior waiter still owns the read channel
        // overwrites its activation state and races the in-flight decoder consumer.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _hasInFlightItem, true);
        var recoveryActivated = false;
        long recoveryGen = 0;
        if (_waiters.Count is 0)
        {
            // Recovery substitutes take over the failed item's position INCLUDING its turn: INHERIT
            // the failed item's grant in place (real activation, ordered-safe by the wait above when
            // it was a census's; vacuous-for-X when it was X's own) or FAIL-IF-LIVE claim a fresh
            // grant; a live FOREIGN turn (a census granting another non-resident) declines and the
            // substitute defers for a later stop. Under the edge lock like every count==0 grant; the
            // policy call runs outside the hold.
            var inheritGen = failedTurn < 0 ? -failedTurn : _waiters.NextGrantGen();
            var recoveryLock = _waiters.EdgeLock;
            recoveryLock.Enter();
            recoveryGen = _waiters.ClaimOrInherit(inheritGen);
            recoveryLock.Exit();
            if (recoveryGen != 0)
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                recoveryActivated = true;
            }
            else
            {
                recoveryGen = _waiters.PlaceExecDeferred(recoveryItem);
            }
        }
        else
        {
            // Republish for deferred activation. PlaceExecDeferred's Volatile.Write orders the
            // SetExecutingItem above before the word; the census acquire-reads and captures first.
            recoveryGen = _waiters.PlaceExecDeferred(recoveryItem);
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
                await result.TrailingExecutionTask.ConfigureAwait(false);

            if (result.PipelineTask.IsCompleted)
            {
                if (ClearExecutingItem(recoveryActivated))
                {
                    result.PipelineTask.GetAwaiter().GetResult();
                    CompleteWaiter(recoveryItem, null, ownedTurn: ExecOwnedTurn(recoveryActivated, owned: true, recoveryGen));
                }
                else
                {
                    // Census-granted mid-recovery: route through the store (guarded - a recovery's own
                    // late fault is never re-recovered), same lost-item rule as the main loop.
                    CommitWaiter(recoveryItem, sentinelHeld: true, own: false, grantGen: 0, GuardRecoveryTask(result.PipelineTask));
                }
            }
            else
            {
                // Tail-waiter transition. Mirror the main loop's _hasInFlightItem clear so the
                // enumerator doesn't double-yield the item across the dispatch window. The task
                // is guarded: the substitute re-enters the normal tail lifecycle, and its own
                // late fault must complete it directly rather than re-enter recovery.
                Volatile.Write(ref _hasInFlightItem, false);
                _tailWaiter = recoveryItem;
                _tailWaiterTask = GuardRecoveryTask(result.PipelineTask);
                _tailWaiterActivated = recoveryActivated;
                _tailWaiterGen = recoveryGen;
                Volatile.Write(ref _hasTailWaiter, true);
            }
        }
        catch (Exception recoveryEx)
        {
            // No in-flight tail-waiter to observe here: the only _tailWaiter publish in the try
            // is in the trailing pending branch, which doesn't throw.
            var ownedEx = ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx,
                ownedTurn: ExecOwnedTurn(recoveryActivated, ownedEx, recoveryGen));
        }
    }

    /// Handles trailing execution task failures, including tail waiter recovery.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverTrailingFailure(T item, bool activated, long failedGen, Exception ex, CancellationToken cancellationToken)
    {
        // Preserve the pipeline task: the framework re-uses the materialized Task locally below
        // (no-recovery branch's CommitWaiter), so we want a stable handle that outlives this
        // method's locals. Wrap it back into a ValueTask for the context's per-construction-
        // site carrier - the recovery awaits the Task-backed ValueTask idempotently, the
        // framework's CommitWaiter takes the Task directly.
        var pipelineTask = _tailWaiterTask.Preserve();
        var context = new PipelineItemFailureContext(PipelineItemFailureKind.TrailingExecutionTask, ex, pipelineTask);
        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            // Pipeline task may still be pending, enqueue as a waiter rather than completing prematurely.
            // Items can handle their own interdependency between the two tasks if needed.
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            _tailWaiterActivated = false;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            // Resolve the parked deferral before the commit (the commit invariant): the one-winner
            // consume decides ownership; a lost item commits sentinel-held (the census granted it).
            // PLAIN consume (see ClearExecutingItem's doc) - residents may be live here.
            var own = activated || _waiters.TryConsumeExecDeferred();
            CommitWaiter(item, sentinelHeld: !own || activated, own, grantGen: failedGen, pipelineTask);
            return;
        }

        // Swap the tail: replace _executingItem with the recovery item. LOCK-FREE UNPUBLISH-FIRST: the
        // failed item X's deferral is a genuinely multi-location transaction (identity field + exec-word
        // must flip atomically vs lock-free claimants, and a packed long can't carry generic-T
        // identity). A naive eager swap (SetExecutingItem before consume) would let a C-path read R's
        // new identity against X's still-live deferral. So UNPUBLISH FIRST - consume the deferral
        // (which fails against a C-path's ACTIVATING, giving the ordering) - THEN swap identity in the
        // now-owned window, THEN republish/activate. No recovery mutex: recovery runs on the executor's
        // logical strand (the Debug tripwire below asserts it), so recovery-vs-recovery is structurally
        // impossible, and the unpublish-first word ordering alone protects the swap against the
        // lock-free claimants.
#if DEBUG
        Debug.Assert(Interlocked.Exchange(ref _recoverySwapGuard, 1) == 0,
            "Concurrent recovery swap - the executor-strand premise was violated (multi-strand recovery restructure?).");
#endif
        // Unpublish: the one-winner consume resolves X's deferral. WON (still deferred) means no
        // grant exists or ever can - swap the identity and republish for R (the census that reads
        // the fresh deferral captures R). LOST means a census's gen-pinned consume of this exact gen
        // already won: its capture happened before its take, so its ActivateHeadItem(X, ...) call is
        // REAL, not vacuous - X's own activation genuinely fires. What makes activating R immediately
        // afterward safe is not that X's activation doesn't happen, but that it's ORDERED to happen
        // first: WaitForCensusResolution below blocks until that exact census's ActivateHeadItem call
        // has returned (WaiterStore.IsCensusResolved(failedGen)), so R's activation is guaranteed to
        // be a clean eclipse-transfer (Activate(X) happens-before Activate(R)), the same shape as a
        // self-activated item whose trailing later faults and gets eclipsed - never two unordered,
        // concurrently-live activations.
        var recoverySwapWon = _waiters.TryConsumeExecDeferred();
        SetExecutingItem(recoveryItem);
        bool recoveryActivated;
        long recoveryGen;
        if (recoverySwapWon)
        {
            recoveryGen = _waiters.PlaceExecDeferred(recoveryItem);
            recoveryActivated = false;
        }
        else
        {
            // R inherits X's grant in place: the turn already holds -failedGen (X's), and R takes it -
            // R releases/converts that same -recoveryGen. _executingItem stays populated: the
            // recovery's ExecuteItemAsync below needs it for write-phase gating.
            //
            // The consume can lose for two different reasons the code must not conflate: X was
            // self-activated at dispatch (elided/self-reclaimed - `activated` is true here, nothing
            // was ever deferred, so there is no census to wait for and failedGen names X's OWN turn,
            // never a stamped grant), or a census's gen-pinned consume of this exact gen already won
            // (`activated` is false - the only case WaitForCensusResolution's premise, "a census
            // exists for this gen", actually holds). Only the latter needs the wait.
            if (!activated)
                WaitForCensusResolution(failedGen, cancellationToken);
            recoveryGen = failedGen;
            ActivateHeadItem(recoveryItem, preferAsync: false);
            recoveryActivated = true;
        }
#if DEBUG
        Debug.Assert(Interlocked.Exchange(ref _recoverySwapGuard, 0) == 1);
#endif

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
            {
                try
                {
                    await result.TrailingExecutionTask.ConfigureAwait(false);
                }
                catch (Exception trailingEx)
                {
                    _hasTailWaiter = false;
                    _tailWaiterTask = default;
                    var ownedTrail = ClearExecutingItem(recoveryActivated);
                    CompleteWaiter(recoveryItem, trailingEx,
                        ownedTurn: ExecOwnedTurn(recoveryActivated, ownedTrail, recoveryGen));
                    return;
                }
            }

            if (result.PipelineTask.IsCompleted)
            {
                _hasTailWaiter = false;
                _tailWaiterTask = default;
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
                    CompleteWaiter(recoveryItem, pipelineEx,
                        ownedTurn: ExecOwnedTurn(recoveryActivated, owned: true, recoveryGen));
                }
                else
                {
                    // Census-granted mid-recovery: route through the store (guarded), the lost-item rule.
                    CommitWaiter(recoveryItem, sentinelHeld: true, own: false, grantGen: 0, GuardRecoveryTask(result.PipelineTask));
                }
                return;
            }

            // Guarded for the same reason as RecoverItem's tail transition: the substitute's own
            // late fault completes it directly, never re-enters recovery.
            _tailWaiter = recoveryItem;
            _tailWaiterTask = GuardRecoveryTask(result.PipelineTask);
            _tailWaiterActivated = recoveryActivated;
            _tailWaiterGen = recoveryGen;
        }
        catch (Exception recoveryEx)
        {
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            var ownedRec = ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx,
                ownedTurn: ExecOwnedTurn(recoveryActivated, ownedRec, recoveryGen));
        }
    }

    /// Commits the pending tail (if any) to the waiter queue. Called by the executor at the
    /// start of each iteration and before going idle. Returns null when the commit completed
    /// synchronously (no tail, EnqueueWaiter, or sync-complete pipeline task), or a Task the
    /// caller must await for the trailing-recovery path. Returning Task? instead of ValueTask
    /// lets the caller skip the await ceremony entirely on the common sync paths via a null check.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    Task? CommitTailWaiter()
    {
        if (!_hasTailWaiter)
            return null;

        var item = _tailWaiter;
        var task = _tailWaiterTask;
        _hasTailWaiter = false;
        _tailWaiterTask = default;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _tailWaiter = default!;

        var tailActivated = _tailWaiterActivated;
        _tailWaiterActivated = false;
        var tailGen = _tailWaiterGen;
        _tailWaiterGen = 0;
        // One-winner reclaim, no rendezvous. own = the completion and token are ours: activated on
        // our own strand (elision/race-back - no deferral was ever placed), or the parked deferral
        // was consumed un-granted. !own = the census granted (or is mid-grant): the item MUST route
        // through the store - the census advancer acts under the license and the advance's claim needs
        // that license, so the activation is ordered before the retire (the deleted rendezvous's
        // happens-before, carried by license ordering).
        var own = tailActivated || _waiters.TryConsumeExecDeferred();
        // Clearing is safe in both arms: the census captures _executingItem BEFORE its take, so a
        // lost reclaim means the capture already happened. Without this the item strands in
        // _executingItem across the whole idle period.
        SetExecutingItem(default!);

        if (task.IsCompleted && own)
        {
            // Head-gated like the sync shortcut: the store plus the head-gated drain is a reorder
            // buffer, and this inline completion bypasses it, so it is only safe when the ROB is
            // empty (Count is 0 = this item is the head). A completed-but-non-head tail waiter
            // falls through to the store as a drain-only commit and retires in FIFO order.
            if (task.IsCompletedSuccessfully && _waiters.Count is 0)
            {
                task.GetAwaiter().GetResult();
                CompleteWaiter(item, null, ownedTurn: ExecOwnedTurn(tailActivated, owned: true, tailGen));
                return null;
            }

            if (!task.IsCompletedSuccessfully)
                return RecoverCommittedTailWaiterAsync(item, task, tailActivated, tailGen).AsTask();
        }

        // A lost item's fault (rare: census-raced) routes through the store too; the advance's claim
        // surfaces it to the waiter-drain recovery (kind PipelineTaskWaiter instead of PipelineTask -
        // advisory drift, accepted).
        CommitWaiter(item, sentinelHeld: !own || tailActivated, own, grantGen: tailGen, task);
        return null;
    }

    // Awaited inline by the executor (via CommitTailWaiter) so the recovery happens on the
    // executor's logical thread. This preserves the SPSC contract on _waiters (executor is sole
    // producer), keeps pipeline ordering correct (recovery enqueues before subsequent items are
    // committed), and avoids the dual-producer hazard that a fire-and-forget continuation pattern
    // would create.
    //
    // OUT OF SCOPE for the census-race wait (WaitForCensusResolution): this method is only ever
    // reached from CommitTailWaiter's `task.IsCompleted && own` branch, where
    // `own = tailActivated || _waiters.TryConsumeExecDeferred()` - so `own` is provably true at
    // every entry to this method. A census can only ever win by beating the plain consume
    // (own=false); it can never be live here, so tailActivated/tailGen below always describe a
    // genuinely self-owned turn, never a census's.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverCommittedTailWaiterAsync(T item, ValueTask task, bool tailActivated, long tailGen)
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
        var failedTurn = ExecOwnedTurn(tailActivated, owned: true, tailGen);
        if (exception is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            CompleteWaiter(item, recoveryFault.InnerException, failedTurn);
            return;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, exception);
        if (!_policy.TryRecoverItemFailure(context, item, _enumerator.CompletionToken, out var recoveryItem))
        {
            CompleteWaiter(item, exception, failedTurn);
            return;
        }

        // Republish and gate activation on the count, mirroring RecoverItem: activating
        // unconditionally while prior waiters are in flight would put a second active reader on the
        // wire.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _hasInFlightItem, true);
        var recoveryActivated = false;
        long recoveryGen = 0;
        if (_waiters.Count is 0)
        {
            // Recovery substitutes - inherit the failed item's grant in place, or fail-if-live claim
            // a fresh one; a live foreign turn defers (full argument at RecoverItem's twin arm).
            var inheritGen = failedTurn < 0 ? -failedTurn : _waiters.NextGrantGen();
            var recoveryLock = _waiters.EdgeLock;
            recoveryLock.Enter();
            recoveryGen = _waiters.ClaimOrInherit(inheritGen);
            recoveryLock.Exit();
            if (recoveryGen == 0)
            {
                recoveryGen = _waiters.PlaceExecDeferred(recoveryItem);
            }
            else
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                recoveryActivated = true;
            }
        }
        else
        {
            // Republish - see RecoverItem's twin: the census captures before its take.
            recoveryGen = _waiters.PlaceExecDeferred(recoveryItem);
        }

        PipelineItemResult result;
        try
        {
            result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
            var ownedRec = ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx,
                ownedTurn: ExecOwnedTurn(recoveryActivated, ownedRec, recoveryGen));
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
                CompleteWaiter(recoveryItem, ex,
                    ownedTurn: ExecOwnedTurn(recoveryActivated, ownedTrail, recoveryGen));
                return;
            }
        }

        var pipelineTask = result.PipelineTask;
        if (pipelineTask.IsCompleted)
        {
            // Inline completion, ownership-gated like every executor-side completion: a lost
            // (census-granted) substitute routes through the store instead.
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
                CompleteWaiter(recoveryItem, pipelineException,
                    ownedTurn: ExecOwnedTurn(recoveryActivated, owned: true, recoveryGen));
            }
            else
            {
                CommitWaiter(recoveryItem, sentinelHeld: true, own: false, grantGen: 0, GuardRecoveryTask(pipelineTask));
            }
        }
        else if (_enumerator.CompletionToken.IsCancellationRequested)
        {
            // Pipeline shutdown while recovery's async work was in flight. EnqueueWaiter at this
            // point would leak: OnWaiterTaskCompleted bails on wake completion, and DrainOnCompletionAsync
            // has likely already passed. Complete directly so depth tracking and CompleteItem still fire.
            var ownedShut = ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, _completionException,
                ownedTurn: ExecOwnedTurn(recoveryActivated, ownedShut, recoveryGen));
        }
        else
        {
            // The recovery enters the store as an ordinary waiter; its identity travels in the TASK
            // (the guard wrapper rethrows late faults as RecoveryItemFaultException, see the marker
            // type), not in pipeline state - no fields, no value-T-unsound item comparisons.
            // Resolve the deferral before the commit (the commit invariant), one-winner.
            var ownCommit = recoveryActivated || _waiters.TryConsumeExecDeferred();
            SetExecutingItem(default!);
            Volatile.Write(ref _hasInFlightItem, false);
            CommitWaiter(recoveryItem, sentinelHeld: !ownCommit || recoveryActivated, ownCommit, grantGen: 0, GuardRecoveryTask(pipelineTask));
        }
    }

    /// Wraps a recovery item's pending pipeline task before it enters the normal tail/waiter
    /// lifecycle: any late fault is rethrown as the framework's marker so CommitTailWaiter's
    /// faulted-at-commit branch and RecoverWaiter complete the recovery directly instead of
    /// consulting the policy - a recovery's own failure is never re-recovered. Allocates only
    /// on the failure path (which already allocates recovery machinery).
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

    // Retire an item that completes OUTSIDE the advance (an owned inline completion, a recovery
    // substitute, a no-recovery fault, a shutdown bailout). Runs the retirement effects and fires
    // the depth-0 idle signal.
    void CompleteWaiter(T item, Exception? exception, long ownedTurn)
    {
        var reachedZero = CompleteWaiterDeferred(item, exception, ownedTurn);
        if (reachedZero)
            _depthState.OnDepthReachedZero();
    }

    /// The retirement effects (the advance's Drive step): decrement depth, clear the depth-0 ActivatedItem
    /// slot, release the reader turn, run the policy's CompleteItem. Skips the
    /// <see cref="DepthState.OnDepthReachedZero"/> signal so the advancer can fire it once at its exit,
    /// AFTER the count-0 publish (firing it mid-advance would resume external WaitForEmptyAsync awaiters
    /// while the advance is still retiring). Returns true iff depth reached 0.
    bool CompleteWaiterDeferred(T item, Exception? exception, long ownedTurn)
    {
        var depth = _depthState.DecrementDepth();
        // A negative depth means a double-decrement, which would feed garbage to the policy and let
        // the zero-signal comparison miss forever (a stranded WaitForEmptyAsync). The comparison stays
        // `is 0` deliberately: <= would mask the same corruption by double-signaling.
        Debug.Assert(depth >= 0, "Pipeline depth under-ran: double completion for a single enqueue.");
        // Release the ActivatedItem slot when this completion drains in-flight depth to 0. CompleteItem
        // is the single in-order retirement seam, and the slot retires off it like depth and the
        // policy's per-item resources. Correct and uniform across reference and value T:
        //   - No stomp. Completion is strictly head-ordered (the store drains head-first, the sync
        //     fast-path is gated on _waiters.Count is 0), so a completion that is NOT the slot's owner
        //     is an earlier item completing while the newest-activated owner is still live. The depth-0
        //     gate fires only for the LAST in-flight item, whose own body has already finished, so it
        //     never nulls a live owner's slot.
        //   - Null only outside an activated read. Under in-flight depth, depth 0 is also hit at every
        //     inter-item gap (an item sync-completes while the source still has backlog), so this DOES
        //     transiently null the slot between a completion and the next item's activation. That is
        //     safe: the sole reader (PgDecoder.CurrentExecutionControl) reads the slot only while an
        //     activated flow's own body is mid-decode, never in that gap, and ActivateHeadItem
        //     publishes the slot before dispatching the next body. The field's contract prefers a stale
        //     non-null reference in the gap over a null, which the depth-0 clear honors WITHIN an
        //     activated tenure (it never nulls a live owner) even though it nulls in the dead gap.
        //     (Identity or a per-item generation would clear promptly but reintroduce null-while-live.)
        //     Struct-T safe by construction - a plain counter, no reference identity to box.
        if (depth is 0)
        {
            SetActivatedItem(default!);
        }
        // Release the turn BY OWNER IDENTITY: the CAS-if-mine declines when this item never held it
        // (a never-activated head, an unactivated inline completion), so the old owner-blind
        // retire's whole "who may call me" discipline collapses into the identity comparison -
        // a release can never clear a foreign owner's turn.
        if (ownedTurn != WaiterStore<T>.TurnNone)
            _waiters.ReleaseTurn(ownedTurn);
        _policy.CompleteItem(item, depth, exception);
        return depth is 0;
    }

    /// Commits a waiter to the store (INCREMENT-FIRST, the edge-lock protocol; LockedWalk.tla). All
    /// call sites run on the executor's logical thread (the SPSC single-producer contract) with the
    /// item's deferral already RESOLVED (consumed, or granted - never parked; the Debug guard bites).
    /// prev==0 is THE EDGE: the item will be the sole head (the count is exact at zero), so this
    /// committer owns the frontier - activate-before-publish (policy on a never-activated own item,
    /// skipped when the task already completed: an own-token pre-publish read), the pre-publish
    /// frontier attach, then lock { assign-or-inherit the turn, THEN publish - assign-first is a
    /// hard in-hold ordering (LW2_pubfirst RED: the unlocked stop can see a published head before
    /// its assign and double-grant) } and the acquire-or-FLAG arm (the deposit covers a
    /// census-exiting holder whose exit never re-peeks after a pre-publish fire consumed the
    /// carrier - LW2 wedge). prev>0 is MID-CHAIN: bare publish, no turn, no attach (the stop is the
    /// frontier's mover), and a plain-read arm that never deposits (a mid-chain deposit would CAS +
    /// force a serve once per pipelined item against a continuously-held license).
    /// <paramref name="sentinelHeld"/>: the turn sentinel is (or is about to be) this item's - it
    /// was activated on the executor strand or a census granted it; the edge lock inherits and
    /// converts, and no policy call is owed here.
    // The turn identity an executor-processed item holds at its inline completion / recovery: a
    // same-strand self-act (TurnExec), or a census grant carrying the item's deferral gen (-gen),
    // or none. `owned` is the reclaim result (activated, or won its own consume); !owned means a
    // census granted this item, so its live turn is -gen and the completion must release exactly it.
    static long ExecOwnedTurn(bool activated, bool owned, long gen)
        => activated || !owned ? -gen : WaiterStore<T>.TurnNone;

    void CommitWaiter(T item, bool sentinelHeld, bool own, long grantGen, ValueTask waiterTask)
    {
        Debug.Assert(!_waiters.ExecDeferredVisible, "Commit with this item's deferral still parked - resolve (consume or route) before committing.");
        var prev = _waiters.IncrementCommitCount();

        if (prev == 0)
        {
            // THE EDGE. Sole head guaranteed; the executor still owns the item and its token
            // (unpublished), so the status read and the registration below are contract-clean.
            if (!sentinelHeld && !waiterTask.IsCompleted)
            {
                // ACTIVATE-BEFORE-PUBLISH: a never-activated incomplete head needs its policy call,
                // and here - pre-publication - it is exclusive by invisibility (the recovery
                // double-activate CE killed the post-release placement). A completed-at-commit item
                // needs no activation at all: the carrier and the arm drive its advance.
                ActivateHeadItem(item, preferAsync: false);
            }
            // Sole head: no claim can run before our publish, so the ordinal read is stable.
            var mySeq = _waiters.HeadSeq;
            if (own && _waiters.TryAcquireIfFree())
            {
                // THE LICENSED OWN-EDGE COMMIT (LW2_licedge GREEN incl. EWalkBounded): hold the
                // advance license across assign + publish + attach. No lock (LW2_noownedgelock:
                // no census can exist for an owned item), no probe advance, no Dekker fence (RMWs
                // on both sides), and the attach runs POST-publish safely - claims are license-
                // serialized and we hold it, the same argument as the stop's attach. A completed
                // task's inline fire (or any racing fire) DEPOSITS on us; the release-or-serve
                // consumes it and we advance our own SOLE head only - the executor-advancer
                // bound is the checked EWalkBounded invariant, not prose.
                _waiters.AssignTurnAtCommit(grantGen, mySeq);
                _waiters.PublishCommitted(item, waiterTask, out _);
                RegisterAdvanceFire(waiterTask, mySeq);
                if (_waiters.Release())
                    Advance();
                return;
            }
            // Contended (a census or a spurious probe holds the license) or census-raced (!own):
            // the ARMED shape - frontier attach pre-publish (an inline fire bails on the phantom
            // edge; the arm re-drives), assign+publish under the lock when a census can be
            // mid-hold, then acquire-or-FLAG (the deposit is load-bearing here - the holder's
            // exit consumes it).
            RegisterAdvanceFire(waiterTask, mySeq);
            if (own)
            {
                _waiters.AssignTurnAtCommit(grantGen, mySeq);
                _waiters.PublishCommitted(item, waiterTask, out _);
            }
            else
            {
                var edgeLock = _waiters.EdgeLock;
                edgeLock.Enter();
                _waiters.AssignTurnAtCommit(grantGen, mySeq);
                _waiters.PublishCommitted(item, waiterTask, out _);
                edgeLock.Exit();
            }
            if (_waiters.TryAcquire())
                Advance();
            return;
        }

        // MID-CHAIN: publish bare, then the certified committer arm - ACQUIRE-OR-FLAG (LW2
        // ECommitWordRead; nocommitterarm DEADLOCKs, so the flag is load-bearing). A held license
        // means an advancer is live: DEPOSIT, and its release-or-serve re-probes and drives this
        // item - the hand-off is lossless BY CONSTRUCTION. Winning the acquire means no advancer is
        // live (the prior one exited past our still-invisible increment via a phantom bail): we
        // drive. Both outcomes are count-word CASes, and the publish is program-order-before them,
        // so the CAS IS the StoreLoad fence - no separate barrier, and the earlier bare-read decline
        // (which dropped the deposit and stranded a commit racing an exiting advancer - the iter-1800
        // permanent-strand, a fidelity gap I introduced by "optimizing" the flag away) is gone.
        //
        // DOCTRINE NOTE (EWalkBounded): a win with Count > 1 advances a deep chain inline on the
        // executor. That is a latency-doctrine relaxation, NOT a safety issue - losslessness first;
        // dispatching the deep-win advance to the scheduler restores the bound without touching
        // correctness and is the tracked follow-up.
        _waiters.PublishCommitted(item, waiterTask, out _);
        if (_waiters.TryAcquire())
            Advance();
    }

    /// Register the fixed advance-fire trampoline on a task. IRREVOCABLE and at-most-once per task
    /// tenure (a second OnCompleted on the production MRVTSC task throws) - callers are the two
    /// frontier owners only: the edge committer (pre-publish, own token) and the stop
    /// (license-held, gated by its own turn assign). An already-completed task invokes inline.
    /// Arming precedes the registration: the inline-invoke case runs the fire (and its
    /// delivery mark, which reads the armed word) inside the UnsafeOnCompleted call itself.
    void RegisterAdvanceFire(ValueTask task, long seq)
    {
        _waiters.ArmFire(seq);
        task.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onAdvanceFire);
    }

    /// The fixed head-continuation trampoline (the advance-fire). A committed head's task completion invokes
    /// this. Co-fire safety lives entirely on the ATTACH side, not here: TryAttachHead registers the
    /// trampoline AT MOST ONCE per head-tenure ticket (a same-ticket rebind reads CellOnly and never
    /// calls UnsafeOnCompleted again, which the production MRVTSC task would throw on), so two binds
    /// on the same tenure (the arm's fallback bind and a granter's re-bind) never produce two live
    /// trampolines to begin with. This fire is just the advance license's wake-protocol acquirer (the
    /// latch, folded into the count word): acquire wins the advance, a loss DEPOSITS on the holder,
    /// whose release-and-serve owns the redrive.
    void OnAdvanceFire()
    {
        // Delivery mark FIRST: from this instant the completer's pair reads are behind us, so the
        // armed head is safely claimable. Must precede the acquire-or-deposit - a losing deposit
        // is consumed by a holder whose gate re-read is ordered after this write through the
        // count-word RMW chain (our CAS after the write, its serve CAS after ours).
        _waiters.MarkFireDelivered();
        if (!_waiters.TryAcquire())
        {
            return;
        }
        Advance();
    }

    /// The advance chain: retires completed heads in FIFO order and, on reaching an incomplete head,
    /// assigns it the turn, activates it, and registers the frontier trampoline so its completion
    /// re-enters here (the advancer chains the single continuation forward, stop by stop). SINGLE
    /// ADVANCER by the license. Entered by (a) a frontier fire (OnAdvanceFire), (b) a committer arm
    /// (edge acquire-or-flag / mid-chain acquire-if-idle), or (c) a recovery continuation resuming
    /// after retiring its substitute (ResumeAdvanceAfterRecovery).
    ///
    /// Model map (LockedWalk.tla): the claim+retire is AdvanceClaim's done arm; the incomplete head is
    /// WalkStop (unlocked - the count partition); the peek-miss with count>0 is the PHANTOM EDGE
    /// (WalkBailRelease + WalkRepeek: a committer between its increment and its publish - the
    /// two-sided protocol's advancer side); the count==0 probe runs the locked census.
    void Advance(bool emptyReached = false)
    {
        var holdsLicense = true;
        while (true)
        {
            // Claim the completed head out of the store (slot or queue - the store owns the FIFO
            // ordering contract). Claims are license-serialized, which is the token-read exclusion:
            // the claim's IsCompleted and the GetResult below can never race a rival consumer.
            // The license does NOT cover the head's own completer, whose two-phase dispatch is
            // mid-flight exactly when an armed head first turns claimable - the fire-delivery
            // gate (fireDeliveryPending) is that exclusion; see the store's gate doc.
            if (_waiters.TryClaimCompletedHead(out var claimedItem, out var claimedTask, out var claimedSeq, out var fireDeliveryPending))
            {
                // The claimed tenure's ordinal, out of the SAME atomic operation that advanced
                // _headTicket - not a separate LastClaimedSeq read afterward (see the 3-arg
                // overload's own doc). The turn identity this retirement releases (the release is
                // CAS-if-mine, so a head that never held the turn declines harmlessly).

                // Consume the task BEFORE DecrementCount so a successor dispatched the instant the count
                // drops finds per-item resources (reset in GetResult) released. GetResult CONSUMES the
                // token (the MRVTSC tenure contract): claimedTask is never touched again after this.
                Exception? taskException = null;
                try
                {
                    claimedTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    taskException = ex;
                }

                if (taskException is not null)
                {
                    if (!RecoverWaiter(claimedItem, taskException, claimedSeq, out var recEmpty))
                        // Recovery substitute took over this position; its continuation resumes the advance
                        // (ResumeAdvanceAfterRecovery) after retiring the substitute. The count credit stays.
                        return;
                    emptyReached |= recEmpty; // recovered inline (no substitute / sync-complete)
                }
                else
                {
                    // Retirement effects (Drive): complete BEFORE decrement so the depth-0 ActivatedItem
                    // clear runs while the store count still shuts the executor's Count==0 inline gate.
                    emptyReached |= CompleteWaiterDeferred(claimedItem, null, ownedTurn: claimedSeq);
                }

                if (AdvanceDecrement(ref emptyReached, out holdsLicense))
                    continue; // a successor may exist: the top of the loop peeks and drives it uniformly.
                break; // idle edge: AdvanceDecrement already resolved the license (held or released).
            }

            // A completed head whose registered advance-fire is undelivered: its completer is
            // mid-dispatch, and retiring it here would consume/reset the tenure under the
            // dispatcher's pair reads (the retire-vs-dispatch tear). Exit through the standard
            // release tail - NOT the peek path, whose IsCompleted continue would spin on the gate.
            // Lossless: the fire is guaranteed in flight, and its acquire-or-deposit against this
            // advance's release-or-serve re-drives the walk once the delivery mark is visible.
            if (fireDeliveryPending)
            {
                break;
            }

            // No completed head to claim.
            if (_waiters.TryPeekHead(out var peekedItem, out var peekedTask))
            {
                // License-covered status read: claims are the only token consumers and we hold the
                // license, so this can never land on a consumed token.
                if (peekedTask.IsCompleted)
                    continue; // completed since the claim miss: claim it inline.
                // Incomplete resident head: THE STOP - the frontier moves here. Turn assignment is
                // LOCK-FREE (the count partition: a resident head means count>=1, and every rival
                // assigner is in the count==0 family; TLC-certified, stop UNLOCKED). A live turn
                // means the head's own edge committer carried it (assign-first ordering: a published
                // head's turn precedes its publish) - exit bare, its carrier fires the advance.
                if (_waiters.Turn == WaiterStore<T>.TurnNone)
                {
                    var headSeq = _waiters.HeadSeq;
                    _waiters.AssignTurnAtStop(headSeq);
                    ActivateHeadItem(peekedItem, preferAsync: true);
                    // The frontier attach: license-held, once per tenure (gated by the None->assign
                    // above - a second stop on this head sees the live turn and exits).
                    RegisterAdvanceFire(peekedTask, headSeq);
                }
                break;
            }

            // Peek miss. count>0 = the PHANTOM EDGE: a committer counted but has not published yet
            // (increment-first over-promises). ONE still-licensed re-peek before giving up the
            // license at all: TryPeekHead's queue leg is a mutating consumer-side operation on the
            // SPSC queue (refreshes _lastCopy, hops _head on the slow path), not a pure read - the
            // queue's contract is single-consumer, peeks included. A prior release-then-repeek shape
            // touched it AFTER releasing the license, making this thread a second, unlicensed
            // consumer for the duration of that touch - unsafe by construction regardless of whether
            // a live overlap is ever caught. Holding the license one extra peek costs nothing: a
            // depositor's claim is never lost regardless of how long the current holder holds it.
            if (_waiters.Count > 0)
            {
                if (_waiters.TryPeekHead(out _, out _))
                {
                    continue; // still licensed throughout - the publish landed, re-probe from the top.
                }
                if (_waiters.Release())
                    continue; // a deposit landed during this hold: kept the license, re-probe.
                holdsLicense = false;
                break; // still invisible after both checks: the committer's own arm covers it.
            }

            // count==0 idle probe (a fire landed on an already-drained store): run the census - a
            // deferral may be parked with no other driver.
            RunIdleCensus();
            break;
        }

        // Every stop above either already resolved the license (the idle edge inside AdvanceDecrement, or
        // the phantom bail) or still holds it here - releasing it is this advance's own tail, the point
        // a rival fire can acquire and advance.
        if (AdvanceExit(emptyReached, releaseLicense: holdsLicense))
            Advance(emptyReached);
    }

    /// Decrement the store count. Returns true to keep looping (a successor may exist, resolved fresh
    /// at the top of Advance's loop), false to stop (the idle edge, already resolved below). DecrementCount
    /// is THE quiescence-visible publish and the LAST run-scoped write on the idle-edge path (the
    /// exit-ordering discipline): at the idle edge the advancer touches only the postdecr exec grant and
    /// then nothing else, so a teardown observer seeing count==0 is guaranteed the advancer will not touch
    /// reset run-scoped state.
    bool AdvanceDecrement(ref bool emptyReached, out bool holdsLicense)
    {
        // License state is EXPLICIT, never inferred from the signal flags: the idle-edge paths
        // release (in the edge CAS, or after the licensed census) and exit unlicensed; every
        // other stop still holds. Conflating this with emptyReached wedged the license held
        // forever after any serve (every subsequent fire deposited into a dead hold).
        holdsLicense = true;
        // The fold's decrement split (the DepositCount law): mid-advance, the advancer's count view is a
        // LOWER bound (commits only raise it; we are the only decrementer), so view > 1 proves the
        // edge is unreachable and the wait-free XADD stays. At the edge the decrement, the license
        // release, and the deposit consume are ONE CAS: no deposit -> the license drops HERE and
        // the idle-edge granters below run license-free; a deposit -> consume it, KEEP the license,
        // and re-probe after the granters (the serve never lets the license go).
        bool idle, serve = false;
        if (_waiters.Count > 1)
            idle = _waiters.DecrementCount();
        else
            idle = _waiters.DecrementCountAtEdge(out serve);
        if (!idle)
            return true; // a successor exists (count under-promises the store, never over-promises).
        // Idle edge: this decrement drained the count to zero. The census runs HERE - after the
        // decrement (the executor race-back Dekker closes: our decrement is the write its count-read
        // observes), LICENSED (the serve arm kept the license at the edge CAS; the released arm
        // re-acquires or flags, and the flag is complete coverage: the holder's serve re-probes and
        // ends at its OWN idle edge). The license is what orders the census's activation before any
        // advance claim can retire the granted item (the deleted rendezvous's happens-before). There is
        // NO resident granter and NO edge pin: the deferral grant is the census's whole job
        // (FoldedWalk probe-proved the resident granter redundant; the stale-edge/corpse-grant
        // machinery guarded generations that no longer exist).
        emptyReached = true;
        if (!serve && !_waiters.TryAcquire())
        {
            // Deposited on a live holder: its eventual edge census covers the grant.
            holdsLicense = false;
            return false;
        }
        RunIdleCensus();
        if (serve)
            return true; // the edge CAS consumed a deposit: licensed re-probe from the top.
        // Census done under the re-acquired license: release it - a deposit that landed during
        // the census re-probes (nothing between the census and this release can be dropped);
        // a clean release exits unlicensed.
        if (_waiters.Release())
            return true;
        holdsLicense = false;
        return false;
    }

    /// The exit tail (runs on every advance stop). The DecrementCount that published quiescence was the last
    /// run-scoped write; these two signals are the only post-exit actions and read no run-scoped state
    /// that teardown resets. The depth-0 idle signal is fired here (after the count publish) so an
    /// external WaitForEmptyAsync awaiter never resumes mid-advance; the teardown wake re-checks count.
    /// Returns TRUE when the license release consumed a deposit: the caller re-enters the advance
    /// (the serve keeps the license). The idle-edge path releases inside its decrement CAS and
    /// passes releaseLicense: false.
    bool AdvanceExit(bool emptyReached, bool releaseLicense = false)
    {
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalDrainWakeupIfWaiting();
        // The license release is LAST (after the signals): the release is the point a rival fire can
        // acquire and advance, and everything above is this advance's own tail.
        return releaseLicense && _waiters.Release();
    }

    /// The locked idle-edge census (license-held). The one cross-thread race the edge lock exists
    /// for: the census's take+assign vs the executor's cnt==0 self-assigns. VERSION-PINNED CAPTURE:
    /// the granted item comes from the deferral itself (TryCensusConsumeDeferred), read under the
    /// generation pin, so it is EXACTLY the consumed deferral's item. Capture-before-take (reading
    /// _executingItem before the consume) was unsound - a suspended census could consume a RECYCLED
    /// deferral while granting the stale captured item, activating it after its own retirement (the
    /// NoTwoItemsActivatedConcurrently collision, iter-7). The pin declines a recycle instead of
    /// granting stale; the executor's own reclaim covers the deferral the census declined. The
    /// policy call runs OUTSIDE the hold, under the license: if the granted item's completion races
    /// this activation, the reclaim loser routes it through the store, whose claim needs the license
    /// we hold - activation-before-retire by license ordering.
    void RunIdleCensus()
    {
        if (!_waiters.ExecDeferredVisible)
        {
            return;
        }
        T captured = default!;
        var censusLock = _waiters.EdgeLock;
        censusLock.Enter();
        // PEEK the deferral's gen, FAIL-IF-LIVE claim the turn as -gen (IDENTITY-TAGGED so a
        // committing item inherits only its own census grant - the iter-83 steal fix), then
        // GEN-PIN the consume to that exact deferral. Both the Count==0 idle-edge decision and the
        // turn can be STALE by now (the executor commits + self-acts lock-free); the turn CAS is
        // the authority (a live turn - a resident's ordinal, TurnExec, or another -g - declines),
        // and the gen-pinned consume declines a recycled deferral (the stale edge), releasing -gen.
        var granted = false;
        var grantGen = 0L; // only ever read below once granted is true, which proves it was assigned
        if (_waiters.Count == 0 && _waiters.TryPeekExecDeferred(out grantGen, out captured)
            && _waiters.TryClaimTurnGrant(grantGen))
        {
            granted = _waiters.TryConsumeExecDeferredGen(grantGen);
            if (!granted)
            {
                _waiters.ReleaseTurn(-grantGen);
                captured = default!;
            }
        }
        censusLock.Exit();
        if (!granted)
        {
            return;
        }
        ActivateHeadItem(captured, preferAsync: true);
        // Stamp AFTER the policy call returns, not before: a recovery that lost this exact gen's
        // consume (Pipeline.cs's RecoverItem/RecoverTrailingFailure) waits on this stamp specifically
        // so it never proceeds to activate its own substitute before this call has actually returned -
        // that ordering, not vacuity, is what makes the substitute's activation safe.
        _waiters.RecordCensusResolved(grantGen);
    }

    /// Spins until the census that won <paramref name="gen"/>'s grant has returned from its
    /// ActivateHeadItem call (WaiterStore.IsCensusResolved). Called only after a recovery's own
    /// TryConsumeExecDeferred(gen) has already lost - that loss proves a census's gen-pinned consume
    /// of exactly this gen succeeded, so the stamp for it is at most one synchronous policy call
    /// away and this is expected to resolve near-instantly, never on a gen nothing claimed. The
    /// cancellation check exists purely so a shutdown race can't turn this into a permanent park (see
    /// the park-point inventory) - on cancellation this returns without the stamp necessarily having
    /// landed, matching every other shutdown branch in this file that favors not hanging over strict
    /// ordering once the pipeline is tearing down anyway.
    void WaitForCensusResolution(long gen, CancellationToken cancellationToken)
    {
        var spin = new SpinWait();
        while (!_waiters.IsCensusResolved(gen) && !cancellationToken.IsCancellationRequested)
            spin.SpinOnce();
    }

    /// Resume the advance from a recovery continuation that has just retired its substitute
    /// (CompleteRecoveryWaiter ran the effects). The recovery held this position's count credit, so
    /// resume at DecrementCount (AdvanceDecrement) and continue retiring or exit.
    void ResumeAdvanceAfterRecovery(bool emptyReached = false)
    {
        // The recovery episode held the advance license across the substitute (the license is
        // episode-affine, not thread-affine); resume under it and release through the normal exits.
        if (AdvanceDecrement(ref emptyReached, out var holdsLicense))
            Advance(emptyReached);
        else if (AdvanceExit(emptyReached, releaseLicense: holdsLicense))
            Advance(emptyReached);
    }

    /// Returns true if the advance should continue (recovery completed inline, or no recovery), false if a
    /// recovery substitute is occupying this pipeline position (the advance stops; the recovery continuation
    /// resumes it via ResumeAdvanceAfterRecovery after retiring the substitute).
    /// <paramref name="emptyReached"/> propagates the deferred depth-0 signal up to the advance on the sync
    /// return-true paths so it fires at the advance's exit. The async return-false path fires the depth-0
    /// signal on EVERY branch: deferred completions thread emptyReached to ResumeAdvanceAfterRecovery
    /// (fires at the advance exit); the inline CompleteRecoveryWaiter branches fire OnDepthReachedZero inline.
    [MethodImpl(MethodImplOptions.NoInlining)]
    // The caller consumed the waiter task (the consume-before-decrement ordering, see Advance) and hands
    // over the extracted exception.
    bool RecoverWaiter(T failedItem, Exception ex, long claimedSeq, out bool emptyReached)
    {
        emptyReached = false;

        // A committed executor-side recovery faulting late, identified by the guard wrapper's
        // marker exception (see RecoverCommittedTailWaiterAsync): completed directly with the
        // real fault - recovery is the cleanup of last resort, its own failure means the wire
        // is gone and there is nothing a policy could substitute. This is what entitles
        // policies to assert they are never consulted about their own recovery items.
        // Turn release is identity-CAS'd on the claimed ordinal: declines when this head never
        // held it.
        if (ex is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            emptyReached = CompleteWaiterDeferred(failedItem, recoveryFault.InnerException, ownedTurn: claimedSeq);
            return true;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTaskWaiter, ex);
        if (!_policy.TryRecoverItemFailure(context, failedItem, _enumerator.CompletionToken, out var recoveryItem))
        {
            emptyReached = CompleteWaiterDeferred(failedItem, ex, ownedTurn: claimedSeq);
            return true;
        }

        // Recovery item takes over, activate it at the current pipeline position, IN PLACE - this
        // position still holds its count credit, so no commit can read prev==0 and self-activate
        // concurrently, and the census is count-gated away (the credit means count>=1) - which is
        // why this placement needs NO lock (LW2_nolockrecwalk GREEN, full-space). NOTE: do not add
        // a count gate here - the in-place activation is load-bearing for liveness. A gate-skip
        // would park this position's turn on the dead predecessor while the substitute enqueues
        // behind a head that never advances, wedging the gate, the substitute, and the drain in a
        // cycle none of them can break.
        // The turn transfers with the position: inherit-or-assign on the claimed ordinal (the old
        // spin-until-claimable dies - there is no transient rival left to wait out; the exiting
        // advancer's idle-edge claim was gen machinery). The substitute's completion releases it by
        // the same identity.
        _waiters.AssignTurnAtRecovery(claimedSeq);
        ActivateHeadItem(recoveryItem, preferAsync: true);
        _waiterRecoverySeq = claimedSeq;

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            // waiterExecution: this is the waiter-drain recovery (RecoverWaiter off the advance chain
            // via OnWaiterTaskCompleted). It can run on a non-pump thread, concurrently with the
            // executor's own next dispatch, so a policy must not share single-pump per-dispatch state.
            executeTask = _policy.ExecuteItemAsync(recoveryItem, waiterExecution: true, _enumerator.CompletionToken);
        }
        catch (Exception recoveryEx)
        {
            emptyReached = CompleteWaiterDeferred(recoveryItem, recoveryEx, ownedTurn: claimedSeq);
            return true;
        }

        // Publish the recovery item so the bailout path can complete it on shutdown.
        // The recovery continuation always completes this item itself (via CompleteRecoveryWaiter
        // on the normal path or BailoutRecoveryOnShutdown on shutdown), so no atomic claim race
        // with teardown is needed; teardown just waits for the advance to drain.
        _waiterRecoveryItem = recoveryItem;

        if (executeTask.IsCompletedSuccessfully)
        {
            return RecoverWaiterResult(recoveryItem, executeTask.Result, out emptyReached);
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
                CompleteRecoveryWaiter(recoveryItem, recoveryEx);
                ResumeAdvanceAfterRecovery();
                return;
            }

            if (!RecoverWaiterResult(recoveryItem, result, out var emptyReached))
                return; // pipeline task pending, continuation will resume

            ResumeAdvanceAfterRecovery(emptyReached);
        });
        return false;
    }

    /// Handles a completed recovery execution result. Returns true if the advancer should continue,
    /// false if the recovery's pipeline task is pending (occupying this position).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal on the sync return-true
    /// paths. The async continuation paths (RecoverWaiter / the trailing continuation) now capture this
    /// out-param and pass it to ResumeAdvanceAfterRecovery(emptyReached), which fires OnDepthReachedZero
    /// after the advancer release - so a recovery that retires the last in-flight item no longer drops
    /// the depth-0 idle signal (which previously hung a parked WaitForEmptyAsync).
    bool RecoverWaiterResult(T recoveryItem, PipelineItemResult result, out bool emptyReached)
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
                    emptyReached = CompleteRecoveryWaiterDeferred(recoveryItem, ex);
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
                        CompleteRecoveryWaiter(recoveryItem, trailingEx);
                        ResumeAdvanceAfterRecovery();
                        return;
                    }

                    if (!RecoverWaiterPipelineTask(recoveryItem, pipelineTask, out var emptyReached))
                        return; // pipeline task pending, its continuation owns the count credit

                    ResumeAdvanceAfterRecovery(emptyReached);
                });
                return false;
            }
        }

        return RecoverWaiterPipelineTask(recoveryItem, result.PipelineTask, out emptyReached);
    }

    /// Handles the recovery item's pipeline task. Returns true if done, false if pending.
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal to the caller on the sync
    /// return-true path; callers capture it and pass it to ResumeAdvanceAfterRecovery (fires
    /// OnDepthReachedZero after the advancer release). The async return-false path completes via the
    /// NON-deferred CompleteRecoveryWaiter inside its own continuation, which fires OnDepthReachedZero
    /// inline - so the depth-0 signal is delivered on every path.
    bool RecoverWaiterPipelineTask(T recoveryItem, ValueTask pipelineTask, out bool emptyReached)
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
            emptyReached = CompleteRecoveryWaiterDeferred(recoveryItem, pipelineException);
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

            CompleteRecoveryWaiter(recoveryItem, pipelineException);
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
        var recoveryItem = _waiterRecoveryItem;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        CompleteWaiter(recoveryItem, _completionException, ownedTurn: _waiterRecoverySeq);
        // The recovery park holds its position's count credit (the fault decision precedes the
        // decrement in the advance). Every park resolution settles it exactly once: the advance decrements in
        // its loop, the normal continuation decrements via ResumeAdvanceAfterRecovery, and this bailout
        // settles it here - without it the credit strands and DrainOnCompletionAsync's count wait never
        // falls to zero. No activation follows (completion ownership is the shutdown sweep's).
        _ = _waiters.DecrementCount();
        SignalDrainWakeupIfWaiting();
    }

    /// Signals any teardown that's waiting for the advance to drain. Null check is the
    /// common case (no one waiting), only allocates a Volatile.Read + null compare.
    /// Ordering: no hardware fence is required, cache coherence delivers the write eventually
    /// on any target. The only real requirement is to stop the JIT from hoisting or caching the
    /// load across calls (a relaxed atomic load would express this exactly). .NET lacks a
    /// relaxed-read primitive, so we use Volatile.Read and pay an unnecessary acquire fence
    /// on ARM as the cost of saying "actually emit the load."
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SignalDrainWakeupIfWaiting()
        => Volatile.Read(ref _drainWakeupTcs)?.TrySetResult();

    /// Completes the recovery item on the normal (non-shutdown) recovery path. The continuation
    /// owns completion uncontested. Drain (DrainOnCompletionAsync) only waits for the advancer
    /// advance to drain via ResumeAdvanceAfterRecovery's exit signal.
    void CompleteRecoveryWaiter(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        CompleteWaiter(recoveryItem, exception, ownedTurn: _waiterRecoverySeq);
    }

    /// <see cref="CompleteRecoveryWaiter"/> variant that returns the deferred-empty signal so the
    /// caller can fire <see cref="DepthState.OnDepthReachedZero"/> after releasing the advancer.
    /// Used by the sync recovery chain (<see cref="RecoverWaiter"/>, <see cref="RecoverWaiterResult"/>,
    /// <see cref="RecoverWaiterPipelineTask"/>) to avoid the same stranding window the slot drain fix
    /// closed: signalling OnDepthReachedZero mid-advance would let a WaitForEmptyAsync awaiter resume and
    /// observe a transient empty while the advance is still retiring. Fire it at the advance's exit instead.
    bool CompleteRecoveryWaiterDeferred(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        return CompleteWaiterDeferred(recoveryItem, exception, ownedTurn: _waiterRecoverySeq);
    }

    // Whether a store to a slot field of type T is atomic against a concurrent getter read, deciding
    // whether the seqlock below is needed. Mirrors ConcurrentDictionary's IsValueWriteAtomic
    // (ECMA-335 I.12.6.6): references and IntPtr are atomic, primitives up to 4 bytes always, 8-byte
    // primitives only on 64-bit, and EVERY custom struct is tearable. That last point is load-bearing:
    // the JIT may field-decompose a struct copy regardless of size, so a `sizeof <= nativeword` test is
    // NOT a safe substitute. Cached per instantiation - true for reference T (the seqlock vanishes),
    // false for the struct pipelines that need it.
    static readonly bool _writeAtomic = IsWriteAtomic();

    static bool IsWriteAtomic()
    {
        if (!typeof(T).IsValueType || typeof(T) == typeof(IntPtr) || typeof(T) == typeof(UIntPtr))
            return true;

        switch (Type.GetTypeCode(typeof(T)))
        {
            case TypeCode.Boolean:
            case TypeCode.Byte:
            case TypeCode.Char:
            case TypeCode.Int16:
            case TypeCode.Int32:
            case TypeCode.SByte:
            case TypeCode.Single:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
                return true;
            case TypeCode.Int64:
            case TypeCode.Double:
            case TypeCode.UInt64:
                return IntPtr.Size == 8;
            default:
                return false;
        }
    }

    // Publish a value to one of the public single-pump slots (_executingItem / _activatedItem).
    // When the store is atomic (_writeAtomic), a cross-thread getter read can never tear, so a plain
    // store suffices and the generation work is skipped. The !typeof(T).IsValueType guard JIT-folds, so
    // reference T short-circuits with zero cost.
    //
    // Otherwise T is a multi-word struct and a concurrent getter could observe a field-mismatched
    // ("torn") value. The seqlock generation closes that: bump it odd before the store and even after,
    // so ReadSlot detects any overlap and retries. A torn copy is harmless - reference fields are
    // atomically copied, so a torn struct still has every reference valid (just mismatched), never a
    // garbage pointer, and ReadSlot discards it on the generation recheck. Interlocked.Increment is a
    // full fence, so the store stays sandwiched in the odd/even bracket.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void PublishSlot(ref T slot, ref uint gen, T value)
    {
        if (!typeof(T).IsValueType || _writeAtomic)
        {
            slot = value;
        }
        else
        {
            Interlocked.Increment(ref gen); // odd: store in progress
            slot = value;
            Interlocked.Increment(ref gen); // even: store complete
        }
    }

    // Tear-free read of a public single-pump slot. Reference T: a plain atomic load. Value T: the
    // seqlock read - sample an even generation, copy the struct, re-sample; if the generation moved
    // the copy straddled a write (possibly torn, always GC-safe) so retry. Writes are item-paced
    // (wire-I/O-gated) so the loop effectively never spins, but correctness does not depend on that:
    // a snapshot is only returned when no write overlapped it. Stale-but-consistent is fine (the
    // slot's contract permits a stale reference); only tearing is excluded.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static T ReadSlot(ref T slot, ref uint gen)
    {
        if (!typeof(T).IsValueType || _writeAtomic)
            return slot;

        var spin = new SpinWait();
        while (true)
        {
            var g1 = Volatile.Read(ref gen);
            if ((g1 & 1) == 0) // even: no write in progress at the sample point
            {
                var value = slot; // acquire on g1 orders this read after the generation sample
                Interlocked.MemoryBarrier(); // the copy must complete before the re-sample (LoadLoad)
                if (Volatile.Read(ref gen) == g1)
                    return value;
            }
            spin.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SetExecutingItem(T value) => PublishSlot(ref _executingItem, ref _executingItemGen, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void SetActivatedItem(T value) => PublishSlot(ref _activatedItem, ref _activatedItemGen, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivateHeadItem(T item, bool preferAsync = true)
    {
        // Update the publicly-observed ActivatedItem slot before dispatching to the policy:
        // a same-thread inline activation sees the new value via the policy call's own
        // sequencing, and a TP-dispatched activation publishes via the dispatch fence.
        // The TURN is not touched here: every caller assigned (or inherited) it under its own
        // discipline - the edge lock, the license-held stop, or the recovery placement.
        SetActivatedItem(item);
        _policy.ActivateHeadItem(item, preferAsync);
    }

    /// Resolve the item's deferral and drop the executor-slot references. Returns TRUE when the
    /// completion is OWNED (activated on this strand, or the reclaim won the one-winner consume -
    /// no grant exists or ever can): the caller may inline-complete. FALSE = the census granted (or
    /// is mid-grant): the caller must route the item through the store (never inline-complete a
    /// lost item - the license orders the census's activation before the advance's retire; this is
    /// what replaced the ACTIVATING observe-rendezvous and its unbounded spin). PLAIN consume, not
    /// claim-first: this runs with no Count==0 guarantee (residents may legitimately hold the turn),
    /// so a fail-if-live turn claim here would misread a foreign resident as a foreign census grant -
    /// see WaiterStore.TryConsumeExecDeferred's doc for the known residual this leaves open.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool ClearExecutingItem(bool wasActivated)
    {
        // Single writer (the executor), so no RMW needed; the Volatile.Write carries the clear to
        // the enumerator's cross-thread read.
        Volatile.Write(ref _hasInFlightItem, false);
        var owned = wasActivated || _waiters.TryConsumeExecDeferred();
        // Clearing is safe in BOTH arms: the census captures _executingItem BEFORE its take, so a
        // lost reclaim means the capture already happened.
        SetExecutingItem(default!);
        return owned;
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    static void ThrowCompleted() => throw new InvalidOperationException("The pipeline has been completed.");


    public struct Enumerator
    {
        readonly Pipeline<T, TPolicy, TSource, TEnumerator> _pipeline;
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>.Enumerator _waitersEnumerator;
        // Read before the slot check, not at phase 2's own turn - see the read-order comment at its
        // use site (matches TryClaimCompletedHead/TryPeekHead's fix: read the queue reference before
        // the slot state, since an escalating commit writes them in that order and this enumerator
        // has no other synchronization pairing it with this store beyond these two reads).
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>? _queueSnapshot;
        // 0: executing item, 1: init queue waiters, 2: enumerate queue waiters, 3: slot waiter, 4: tail waiter, 5: done
        int _phase;

        internal Enumerator(Pipeline<T, TPolicy, TSource, TEnumerator> pipeline)
        {
            _pipeline = pipeline;
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            switch (_phase)
            {
                case 0:
                    _phase = 1;
                    // Visibility-only window: the in-flight item is held on _executingItem before
                    // being committed elsewhere. Without yielding it here, heartbeat-style
                    // consumers can't see the item during dispatch (waiting-body abort propagation
                    // needs this). Volatile.Read pairs with the executor's Volatile.Write on
                    // _hasInFlightItem.
                    if (Volatile.Read(ref _pipeline._hasInFlightItem) && _pipeline._executingItem is { } inFlight)
                    {
                        Current = inFlight;
                        return true;
                    }
                    goto case 1;
                case 1:
                    // SnapshotForEnumeration owns the safe read order internally (queue reference
                    // before slot state) - this call site just presents slot before queue in
                    // ENUMERATION order, which is a separate concern (leave-head FIFO presentation).
                    _pipeline._waiters.SnapshotForEnumeration(out var slotItem, out var hasSlotItem, out _queueSnapshot);
                    _phase = 2;
                    if (hasSlotItem && slotItem is { } slot)
                    {
                        Current = slot;
                        return true;
                    }
                    goto case 2;
                case 2:
                    // Null queue means the pipeline never escalated. Skip to the tail phase.
                    var queue = _queueSnapshot;
                    if (queue is null)
                    {
                        _phase = 4;
                        goto case 4;
                    }
                    _waitersEnumerator = new(queue);
                    _phase = 3;
                    goto case 3;
                case 3:
                    while (_waitersEnumerator.MoveNext())
                    {
                        if (_waitersEnumerator.Current.Waiter is { } waiter)
                        {
                            Current = waiter;
                            return true;
                        }
                    }
                    _phase = 4;
                    goto case 4;
                case 4:
                    _phase = 5;
                    // Volatile.Read pairs with the executor's Volatile.Write on _hasTailWaiter:
                    // if observed true, the prior _tailWaiter / _tailWaiterTask writes are visible.
                    // Consistent for any T that fits in a native word (refs, primitives, small
                    // structs). Larger structs can tear their own write regardless of fences.
                    if (Volatile.Read(ref _pipeline._hasTailWaiter) && _pipeline._tailWaiter is { } tail)
                    {
                        Current = tail;
                        return true;
                    }
                    return false;
                default:
                    return false;
            }
        }
    }

}

public static class Pipeline
{
    /// Marker carrying a committed recovery item's own fault through the waiter store's task plumbing.
    /// Recovery identity travels in the task rather than pipeline state: the guard wrapper (see
    /// RecoverCommittedTailWaiterAsync) rethrows the recovery's late fault as this type, and
    /// RecoverWaiter recognizes it and completes the item directly with the inner exception. A
    /// recovery's own failure is never re-recovered, and policies are never consulted about items they
    /// returned as recoveries.
    ///
    /// Exception-as-marker rather than a status enum on every store entry: the fault path is already
    /// exceptional and the drain already catches the task's exception, so the marker only retypes an
    /// in-flight throw. Revisit if store entries ever need more kinds than this one.
    internal sealed class RecoveryItemFaultException(Exception innerException)
        : Exception(null, innerException)
    {
        public new Exception InnerException => base.InnerException!;
    }

    /// <summary>Construct a source-driven pipeline against a caller-supplied source.</summary>
    /// <remarks>
    /// No CancellationToken parameter: the caller-supplied source owns its own cancellation
    /// lifecycle (either at source construction or by integrating their CT into the source's
    /// GetAsyncEnumerator implementation). Threading another CT through here would be redundant.
    /// <para>
    /// Flyweight reuse: pass a previously-completed <paramref name="instance"/> to rebind it
    /// against the new <paramref name="policy"/> + <paramref name="source"/> and restart it.
    /// The shell allocation (waiter queue, delegates, depth machinery) gets reused while
    /// the per-run state (enumerator, execution task) gets fresh.
    /// </para>
    /// <para>
    /// The natural call-site pattern when caller caches a nullable pipeline field:
    /// <code>_cachedPipeline = Pipeline.Create(policy, source, _cachedPipeline);</code>
    /// First call: <paramref name="instance"/> is null → fresh allocation.
    /// Subsequent calls: <paramref name="instance"/> is non-null → shell reuse.
    /// </para>
    /// <para>
    /// Throws if <paramref name="instance"/> is non-null but its previous run hasn't fully
    /// completed (caller must await the previous CompleteAsync's returned task first).
    /// </para>
    /// </remarks>
    public static Pipeline<T, TPolicy, TSource, TEnumerator> Create<T, TPolicy, TSource, TEnumerator>(TPolicy policy, TSource source, Pipeline<T, TPolicy, TSource, TEnumerator>? instance = null)
        where TPolicy : IPipelinePolicy<T>
        where TSource : IPipelineSource<T, TEnumerator>
        where TEnumerator : struct, IPipelineEnumerator<T>
    {
        if (instance is null)
            return new(policy, source);
        instance.Initialize(policy, source);
        return instance;
    }

    /// <summary>Construct a queue-backed pipeline. Returns a <see cref="QueuedPipeline{T,TPolicy}"/> that
    /// exposes <see cref="QueuedPipeline{T,TPolicy}.Enqueue"/> directly.</summary>
    /// <remarks>
    /// The cancellation token is passed to the internally-created source. Pipeline itself stays
    /// CT-free, and the source owns the cancellation lifecycle.
    /// </remarks>
    public static QueuedPipeline<T, TPolicy> Create<T, TPolicy>(TPolicy policy, bool runContinuationsAsynchronously = true, PipelineScheduler? scheduler = null, CancellationToken cancellationToken = default)
        where TPolicy : IPipelinePolicy<T>
    {
        var source = UnboundedQueueSource<T>.Create(runContinuationsAsynchronously, scheduler, cancellationToken);
        var pipeline = new Pipeline<T, TPolicy, UnboundedQueueSource<T>, UnboundedQueueSource<T>.Enumerator>(policy, source);
        return new QueuedPipeline<T, TPolicy>(pipeline, source);
    }

    /// Split-counter depth + drain-waiter (WaitForEmptyAsync) protocol. Depth is the difference of two
    /// monotonic totals rather than one shared word:
    ///   _enqueued - producer-owned. The SPSC contract serializes enqueues, so the increment is a plain
    ///     read + Volatile.Write (release), no RMW, on its own padded cache line (no producer/completer
    ///     line ping-pong).
    ///   _completed - completer-side Interlocked.Increment. The RMW stays because executor-inline
    ///     completions and advancer drains overlap, but the producer never touches this line.
    /// Counters are uint, wrap-safe while live depth stays below 2^31 (guarded in IncrementDepth via a
    /// producer-cached completed snapshot, so the guard adds no cross-line read per enqueue).
    ///
    /// Zero-crossing (drain): completers bump _completed (full-fence RMW), compute depth against their
    /// own bump, and on zero check the _drainTcs slot (the load cannot hoist above the RMW) and
    /// Exchange-take + fire. The armer publishes the TCS with a CAS - the publish IS the arm, and the
    /// full-fence RMW orders the subsequent depth re-check (a release publish would let the re-check's
    /// loads reorder above it on x64, a lost wake).
    ///
    /// Firing on a momentary zero while a producer races a new item in is the documented
    /// WaitForEmptyAsync semantics (idle convergence, not exactly-once). TrySetResult and the
    /// Exchange-clear keep completer/armer races idempotent, and at most one armed cycle is outstanding
    /// (single-caller API), so a clear+fire always targets the cycle that armed it. The momentary-zero
    /// license covers zeros at or after the arm: the armer's re-check fire qualifies by construction, a
    /// completer's fire is deferred and revalidates depth first (see OnDepthReachedZero).
    internal struct DepthState
    {
        // Ref-free explicit-layout blob so the producer counter's isolation survives the
        // runtime's auto layout of this ref-containing struct: wherever the blob lands,
        // Value sits >= 128 bytes (one Apple Silicon line) from the fields on either side.
        [StructLayout(LayoutKind.Explicit, Size = 256)]
        struct PaddedProducerCounter
        {
            [FieldOffset(128)] public uint Value;
            // Producer-owned stale snapshot of _completed for the overflow guard. Same line as
            // Value on purpose: only the producer reads/writes it.
            [FieldOffset(132)] public uint CompletedCache;
        }

        PaddedProducerCounter _enqueued;
        uint _completed;
        // The slot doubles as the arm signal: non-null means a drain waiter is armed. The
        // publish CAS is the full-fence RMW the Dekker pair needs, so no separate flag word.
        TaskCompletionSource? _drainTcs;

        /// <summary>Current depth. Lock-free.</summary>
        public int Depth
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Read completed BEFORE dispatched: an item is dispatch-counted (here, _enqueued) by
                // the executor strictly BEFORE it can complete (you cannot complete what was never
                // dispatched), so this order can never observe comp > disp (negative depth). The bump
                // and the eventual decrement order on the same logical item.
                var comp = Volatile.Read(ref _completed);
                var enq = Volatile.Read(ref _enqueued.Value);
                return (int)(enq - comp);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void IncrementDepth()
        {
            // Executor-owned (called at dispatch, the single-consumer pull) so the counter stays
            // single-writer; plain read + release store. Depth now means in-flight (dispatched -
            // completed), not enqueued - completed.
            var next = _enqueued.Value + 1;
            // Wrap guard against the producer-local stale snapshot: apparent live depth past
            // int.MaxValue forces a snapshot refresh (the only time the producer touches the
            // completer's line) and throws if the depth is genuinely at the limit.
            if (next - _enqueued.CompletedCache > (uint)int.MaxValue)
                RefreshCacheOrThrow(next);
            Volatile.Write(ref _enqueued.Value, next);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void RefreshCacheOrThrow(uint next)
        {
            var comp = Volatile.Read(ref _completed);
            if (next - comp > (uint)int.MaxValue)
                throw new InvalidOperationException("Pipeline depth overflow.");
            _enqueued.CompletedCache = comp;
        }


        /// <summary>Records a completion. Returns the new depth. Caller MUST invoke
        /// <see cref="OnDepthReachedZero"/> when the result is 0 to signal any pending drain waiter.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int DecrementDepth()
        {
            // Full-fence RMW; the enqueued read (and any subsequent _drainTcs read in
            // OnDepthReachedZero) is ordered after it. Depth is computed against OUR bump:
            // when completers race the final item, exactly the last bumper observes zero.
            var comp = Interlocked.Increment(ref _completed);
            var enq = Volatile.Read(ref _enqueued.Value);
            return (int)(enq - comp);
        }

        /// <summary>
        /// Returns a task that completes when the pipeline is empty (momentarily; see remarks on the
        /// containing type). Empty has two halves: in-flight (Depth) and backlog (enqueued but not
        /// dispatched). DepthState owns only the in-flight half, so the caller passes the current
        /// backlog snapshot; both must read zero for a synchronous empty. The armed TCS is fired by
        /// the executor at the genuine-suspend seam (its authoritative backlog == 0 observation) or by
        /// a completer crossing Depth to zero while the executor is already parked-empty. Publish-arm-
        /// recheck; single-caller API.
        /// </summary>
        public ValueTask GetIdleTask(int backlog, CancellationToken cancellationToken)
        {
            if (backlog is 0 && Depth is 0)
                return ValueTask.CompletedTask;

            var newTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // The publish IS the arm: the CAS is the full-fence RMW, so the re-check below is ordered
            // after it. A completer that hit zero before observing the publish skipped its fire, and
            // the re-check catches that case and self-signals. The backlog snapshot gates the self-
            // fire: a backlog seen non-empty at the call means an item is still queued, so the
            // executor's park-seam fire (not this synchronous path) resolves the wait when it drains.
            var tcs = Interlocked.CompareExchange(ref _drainTcs, newTcs, null) ?? newTcs;
            if (backlog is 0 && Depth is 0)
                SignalDrainWaiter();

            return new(tcs.Task.WaitAsync(cancellationToken));
        }

        /// <summary>
        /// Post-arm re-check with a FRESH backlog snapshot. GetIdleTask's own self-fire re-check
        /// uses the backlog parameter captured BEFORE its arm CAS; the last backlogged item can
        /// dispatch AND complete entirely inside that window (its zero-crossing finds nothing
        /// armed yet), leaving the armed wait with no future fire - the stale-backlog arm strand
        /// (caught by NoTwoItemsActivatedConcurrently iter 878, a 10s WaitForEmptyAsync timeout
        /// with every gauge zero). The caller re-reads its source's backlog AFTER the arm and
        /// calls this; the arm CAS is the publish that orders this re-check's Depth read after
        /// any completer's decrement that preceded its missed slot-read (same Dekker discipline
        /// as GetIdleTask's own re-check).
        /// </summary>
        public void RecheckIdle(int backlog)
        {
            if (backlog is 0 && Depth is 0)
                SignalDrainWaiter();
        }

        /// <summary>
        /// Called by a completer after <see cref="DecrementDepth"/> returns 0. Disarms and
        /// signals the pending drain waiter, if any - revalidating depth first, because the
        /// zero verdict is bump-instant and this call may have been DEFERRED (the drain paths
        /// signal after releasing the advancer): a producer and a NEW WaitForEmptyAsync arm
        /// can land in the deferral window, and a blind fire would wake that caller on a zero
        /// predating its arm (its entry check saw depth > 0).
        /// </summary>
        public void OnDepthReachedZero()
        {
            // The caller's bump was a full fence, so this load is ordered after it; a publish
            // that preceded the bump is visible here. A publish that FOLLOWS the bump may be
            // missed - that side's re-check covers it (see GetIdleTask).
            if (Volatile.Read(ref _drainTcs) is null)
                return;

            while (true)
            {
                var tcs = Interlocked.Exchange(ref _drainTcs, null);
                if (tcs is null)
                    return;

                // Fire only against a still-zero depth: the armer's momentary-zero contract
                // covers zeros that occur AT or AFTER its arm; a stale pre-arm zero does not.
                if (Depth is 0)
                {
                    tcs.TrySetResult();
                    return;
                }

                // Stale zero against a newer arm: hand the tcs back. The CAS can only fail
                // against a caller that cancelled its wait (the WaitAsync wrapper frees the
                // single-caller API early) and re-armed - the held tcs has no waiter and is
                // dropped; the new arm did its own publish-recheck.
                if (Interlocked.CompareExchange(ref _drainTcs, tcs, null) is not null)
                    return;

                // The put-back is itself an arm: a completer that hit zero while we held the
                // tcs read the slot as empty and skipped its fire. Re-check depth after the
                // put-back (same publish-then-recheck discipline as GetIdleTask) and loop
                // back to re-take if a fresh zero materialized.
                if (Depth is not 0)
                    return;
            }
        }

        void SignalDrainWaiter()
        {
            // Exchange-take makes the disarm exactly-once per armed cycle; the winner fires.
            // TrySetResult keeps the armer-vs-completer self-signal race idempotent.
            var tcs = Interlocked.Exchange(ref _drainTcs, null);
            tcs?.TrySetResult();
        }
    }
}
