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
    // Published by the execution loop for deferred activation. The bool flag is the handshake:
    // set (true) by the executor, claimed (false) under _activationLock. Claims are lock-serialized
    // (plain reads/writes inside the lock); the executor's publishes are lock-free release stores
    // (item written before the flag), so the cross-strand claimants (the advancer C-path family)
    // acquire-read the flag to order their _executingItem load after the flag observation - the
    // acquire half of the historical Interlocked.Exchange, kept as the only fence in the protocol.
    // Executor-strand claimants (CommitTailWaiter, ClearExecutingItem, the recovery reclaim) read
    // plain: the sole lock-free publisher is their own strand.
    T _executingItem = default!;
    bool _executingItemActivationPending;
    // Seqlock generation for the public ExecutingItem getter under value-type T (see PublishSlot /
    // ReadSlot). Unused for reference T (single-word atomic). _executingItem has a sole writer (the
    // executor strand; recovery is awaited inline, the advancer only reads it under _activationLock),
    // so the seqlock's single-writer assumption holds.
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
    // to see the item (sync completion, tail-waiter committal, recovery). Kept separate from
    // _executingItemActivationPending so the advancer C-path's claim semantics on that flag stay unchanged.
    bool _hasInFlightItem;
    // The chain-arm-to-escalator activation handoff. Set (true) by the slot drainer when its
    // chain obligation meets an in-flight first escalation (the head is being relocated to the
    // queue and cannot be named); claimed (false) via Interlocked.Exchange by whichever side
    // gets there first: the drainer's one-shot re-peek or the escalating commit's compensation
    // check. Self-clearing per dance; no per-run reset needed.
    bool _pendingHeadActivation;

    // Cross-thread atomics, touched by the executor, advancer, enqueuer, and completion callbacks.
    // The store has a single inline slot (zero alloc) for the common one-pending-waiter case. The
    // SPSC queue inside it is lazy-allocated only on true overlap (a second waiter arrives while
    // the first is still pending). See WaiterStore<T>.
    WaiterStore<T> _waiters;
    // Completions-since-pass-start dirty flag. Callbacks that bail set it (plain write, published by
    // their TryAcquire's full fence). Drain passes consume it with an Interlocked.Exchange at pass
    // start, and the post-release recheck closes the lost-wake window - the clear/fence/recheck gate.
    bool _drainSignal;

    Latch _advancing; // Held while a thread is currently the advancer. See Latch.cs for semantics.
    T _waiterRecoveryItem = default!; // The item being recovered, for the bailout/completion paths to access.
    TaskCompletionSource? _drainWakeupTcs; // Set by DrainOnCompletionAsync while waiting for the advancer chain to release and _waiters to empty. Signaled by callbacks at end-of-cycle so drain can re-check both conditions.

    // Activation.
    readonly Action _onWaiterTaskCompletedAction;

    internal Pipeline(TPolicy policy, TSource source)
    {
        // Delegate references bind to `this` and don't change.
        _onWaiterTaskCompletedAction = OnWaiterTaskCompleted;
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
        // source no longer needs a depth-increment hook. Pipeline doesn't pass a CT here: source owns
        // its own cancellation lifecycle (caller-configured at source construction).
        _enumerator = _source.GetAsyncEnumerator();
        _executionTask = ExecuteSource();

        // Other per-run fields are left at default: field-initialized on first run, or zeroed by the
        // previous run's ExecuteSource-exit cleanup.
    }

    // Off the hot path (advancer/recovery handoff), so OS handoff on contention is preferable.
    // Lazy-allocated alongside _waiters at first escalation. Stays null while no advancer can
    // exist (advancers require waiters), and the few lock sites that may run pre-escalation
    // null-guard.
    Lock? _activationLock;


    /// <summary>Current in-flight count: items dispatched by the executor but not yet completed.
    /// Excludes the source backlog (enqueued but not yet dispatched); a queue-backed pipeline exposes
    /// that as <see cref="QueuedPipeline{T,TPolicy}.Backlog"/>, and <c>Depth + Backlog</c> is the total
    /// outstanding. Lock-free read, may be stale by the time the caller observes it. Use
    /// <see cref="WaitForEmptyAsync"/> to await empty (both halves zero).</summary>
    public int Depth => _depthState.Depth;

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
                if (_activationLock is not { } dispatchLock)
                {
                    // No waiter has ever been committed (_activationLock is created at first commit,
                    // executor strand). With no queue there is no advancer, so the Count read races
                    // nobody: the historical lock-free inline-activate / deferred-publish fork is safe.
                    if (_waiters.Count is 0)
                    {
                        ActivateHeadItem(item, preferAsync: false);
                        activated = true;
                    }
                    else
                    {
                        // Plain: null _activationLock means no advancer has ever existed; the flag is
                        // executor-private until first escalation allocates the lock.
                        _executingItemActivationPending = true;
                    }
                }
                else
                {
                    // Idle-regime elision. At count 0 with the latch free and no pending publish
                    // there is no concurrent activation decider: the C-path needs the pending flag
                    // (executor-owned, false here), the D-path and callbacks need resident waiters,
                    // and any drain crossing count 0 with a live activation holds the latch end to
                    // end, so the latch read screens it. The elision publishes nothing, so no remote
                    // checker depends on a store from this arm and the volatile guard loads need no
                    // further ordering. A stale-held latch read only declines, the conservative
                    // direction.
                    if (_waiters.Count is 0 && !_advancing.IsHeld && !_executingItemActivationPending)
                    {
                        // preferAsync: true matching the locked arm. The elision changes the
                        // serialization only, never the dispatch mode.
                        ActivateHeadItem(item, preferAsync: true);
                        activated = true;
                    }
                    else
                    {
                        // An advancer may be draining concurrently. The dispatch-time "Count is 0 -> activate
                        // the head" decision must serialize against the advancer's drain-time activation
                        // through the same lock + _executingItemActivationPending claim the advancer's
                        // count<=0 branch uses. Otherwise the executor reads Count is 0 in the window after
                        // the advancer dequeued the last waiter but before it activated the deferred head,
                        // and both strands activate it (two items in the post-activation phase, which a
                        // policy with a single read channel cannot serve) or neither does (lost activation,
                        // count skew). Publish the claim first, then re-read Count and Exchange under the
                        // lock: exactly one of {executor, advancer} wins the flag and activates _executingItem.
                        // The claim+activate stays inside the lock so the executor's own ClearExecutingItem
                        // fence-acquire blocks until this ActivateHeadItem finishes (the advancer's TOCTOU close).
                        lock (dispatchLock)
                        {
                            // Pending is plain lock-protected state (no atomics): every claimant - this
                            // block, the advancer's C-path, CommitTailWaiter's reclaim - runs under
                            // _activationLock, so publish and claim are ordinary reads/writes here.
                            _executingItemActivationPending = true;
                            if (_waiters.Count is 0)
                            {
                                _executingItemActivationPending = false;
                                // preferAsync: true, matching the advancer's count<=0 branch. false would
                                // run an async flow's body inline UNDER the lock (the deferral exists to
                                // avoid exactly that - arbitrary continuations under _activationLock deadlock
                                // the advancer). preferAsync only defers async flows; a sync flow still
                                // activates inline, the same as the advancer already does here safely.
                                ActivateHeadItem(item, preferAsync: true);
                                activated = true;
                            }
                        }
                    }
                }

                try
                {
                    itemResult = await _policy.ExecuteItemAsync(item, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ClearExecutingItem(activated);
                    await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.ExecuteItemTask, ex), _enumerator.CompletionToken).ConfigureAwait(false);
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
                    itemResult.PipelineTask.GetAwaiter().GetResult();
                    ClearExecutingItem(activated);
                    CompleteWaiter(item, null);
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
                    ClearExecutingItem(activated);
                    var outstandingTrailing = itemResult.TrailingExecutionTask;
                    try
                    {
                        itemResult.PipelineTask.GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        await RecoverItem(item, new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, ex, outstandingTrailing), _enumerator.CompletionToken).ConfigureAwait(false);
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
                    // when waiters>0; CommitTailWaiter clears it under the lock.
                    Volatile.Write(ref _hasInFlightItem, false);
                    _tailWaiter = item;
                    _tailWaiterTask = itemResult.PipelineTask;
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
                            await RecoverTrailingFailure(item, activated, ex, _enumerator.CompletionToken).ConfigureAwait(false);
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
            // un-fired run would otherwise read as shutdown and walk the pump out of a live loop -
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
                _commitFireGate = CommitFireIdle;
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

    /// Drains remaining items after the execution loop exits. Waits for the advancer chain to
    /// quiesce via a TCS that DrainReadyWaiters / BailoutRecoveryOnShutdown signal on release of
    /// _advancing. Recovery continuations always complete their own items (via CompleteRecoveryWaiter
    /// or BailoutRecoveryOnShutdown), so drain doesn't compete, it just waits for the chain to
    /// finish. After advancer-idle, drains _waiters.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask DrainOnCompletionAsync()
    {
        // Wait for both the advancer chain to be idle (no callback mid-flight) and _waiters to be empty
        // (every committed item's waiter task completed and its callback drained it). Each callback
        // signals _drainWakeupTcs on release, waking this loop to re-check. The count condition makes
        // drain wait for in-flight pipeline tasks to finish naturally - dequeueing items ourselves would
        // race the body's still-running pipeline task and tear down state it depends on.
        while (_advancing.IsHeld || _waiters.Count > 0)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            // Full fence, NOT Volatile.Write: this is the arm side of a Dekker-shaped arm-then-check
            // against the callbacks' release-then-signal. A release-only publish lets the re-check's
            // loads hoist ABOVE the store - the check reads a stale IsHeld/Count, the releasing
            // callback reads the not-yet-visible null tcs, and the wakeup is lost with both sides
            // convinced the other had it. The signal side is already fenced by the latch release's
            // Interlocked.Exchange, so this is the matching half.
            Interlocked.Exchange(ref _drainWakeupTcs, tcs);
            // Re-check post-publish in case the last callback released while we were setting up.
            if (!_advancing.IsHeld && _waiters.Count == 0)
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
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverItem(T item, PipelineItemFailureContext context, CancellationToken cancellationToken)
    {
        if (!_policy.TryRecoverItemFailure(context, item, cancellationToken, out var recoveryItem))
        {
            CompleteWaiter(item, context.Exception);
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
        if (_waiters.Count is 0)
        {
            // Recovery substitutes: the recovery item takes over the failed item's pipeline position
            // INCLUDING its activation state, so this ungated activate may land while the
            // predecessor's grant is nominally live (a C-path that claimed the pending publish before
            // the fault and activated after it - ClearExecutingItem's lost arm). That grant is
            // vacuous by construction: the failed item is parked in recovery and never returns to
            // the wire, and the turn it held transfers here, released by this item's own retirement
            // or the binding discharge. No lock or gate: the pending word was consumed in-lock by
            // ClearExecutingItem before this runs, so no claimant can race a second activation onto
            // the substitute, and the count arms are mutually exclusive with the deferred publish.
            ActivateHeadItem(recoveryItem, preferAsync: false);
            recoveryActivated = true;
        }
        else
        {
            // Release publish (STLR): claims are lock-serialized, but the publish itself needs no
            // lock - a claimant that misses an in-buffer publish leaves it for a later pass or the
            // executor's own reclaim, same as the historical Exchange protocol. The release orders
            // the SetExecutingItem above before the flag; claimants pair with a Volatile.Read.
            Volatile.Write(ref _executingItemActivationPending, true);
        }

        try
        {
            var result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);

            if (!result.TrailingExecutionTask.IsCompletedSuccessfully)
                await result.TrailingExecutionTask.ConfigureAwait(false);

            if (result.PipelineTask.IsCompleted)
            {
                ClearExecutingItem(recoveryActivated);
                result.PipelineTask.GetAwaiter().GetResult();
                CompleteWaiter(recoveryItem, null);
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
                Volatile.Write(ref _hasTailWaiter, true);
            }
        }
        catch (Exception recoveryEx)
        {
            // No in-flight tail-waiter to observe here: the only _tailWaiter publish in the try
            // is in the trailing pending branch, which doesn't throw.
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
        }
    }

    /// Handles trailing execution task failures, including tail waiter recovery.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverTrailingFailure(T item, bool activated, Exception ex, CancellationToken cancellationToken)
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
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            CommitWaiter(item, activated, pipelineTask);
            return;
        }

        // Swap the tail: replace _executingItem with the recovery item under the activation lock
        // so the advancer's count==0 claim can't observe partial state and double-activate.
        // On inline activation also clear the published slots so a later advancer claim cannot
        // re-read recovery and activate it twice.
        // If _activationLock is still null we never escalated, so no advancer can be running and
        // the lock body runs uncontended without it.
        bool recoveryActivated;
        if (_activationLock is { } activationLock)
        {
            lock (activationLock)
            {
                SetExecutingItem(recoveryItem);
                // Plain lock-protected state: every claimant runs under this lock.
                recoveryActivated = !_executingItemActivationPending;
                _executingItemActivationPending = true;
                if (recoveryActivated)
                {
                    ActivateHeadItem(recoveryItem, preferAsync: false);
                    _executingItemActivationPending = false;
                    // _executingItem stays populated: the recovery's ExecuteItemAsync below needs it
                    // for write-phase gating (same C-path pattern).
                }
            }
        }
        else
        {
            SetExecutingItem(recoveryItem);
            // No-lock path: a null _activationLock means no escalation ever happened, so no
            // advancer exists to read this flag. Single strand, plain access.
            recoveryActivated = !_executingItemActivationPending;
            _executingItemActivationPending = true;
            if (recoveryActivated)
            {
                ActivateHeadItem(recoveryItem, preferAsync: false);
                _executingItemActivationPending = false;
            }
        }

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
                    ClearExecutingItem(recoveryActivated);
                    CompleteWaiter(recoveryItem, trailingEx);
                    return;
                }
            }

            if (result.PipelineTask.IsCompleted)
            {
                _hasTailWaiter = false;
                _tailWaiterTask = default;
                ClearExecutingItem(recoveryActivated);
                Exception? pipelineEx = null;
                try
                {
                    result.PipelineTask.GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    pipelineEx = e;
                }
                CompleteWaiter(recoveryItem, pipelineEx);
                return;
            }

            // Guarded for the same reason as RecoverItem's tail transition: the substitute's own
            // late fault completes it directly, never re-enters recovery.
            _tailWaiter = recoveryItem;
            _tailWaiterTask = GuardRecoveryTask(result.PipelineTask);
        }
        catch (Exception recoveryEx)
        {
            _hasTailWaiter = false;
            _tailWaiterTask = default;
            if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
                _tailWaiter = default!;
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
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

        // Reclaim the publish under the lock: replaces the old lock-free Exchange PLUS the empty
        // fence-acquire lock{} it needed (any in-progress advancer ActivateHeadItem runs inside its
        // own lock section, so entering ours means it finished - CompleteWaiter below cannot outrun
        // it). alreadyActivated covers both "advancer's C-path consumed the pending" and "never
        // published" (inline activation never set it). When _activationLock is still null no advancer
        // has ever run and pending is executor-private - plain access, nothing to synchronize with.
        bool alreadyActivated;
        if (_activationLock is { } activationLock)
        {
            lock (activationLock)
            {
                alreadyActivated = !_executingItemActivationPending;
                _executingItemActivationPending = false;
            }
        }
        else
        {
            alreadyActivated = !_executingItemActivationPending;
            _executingItemActivationPending = false;
        }
        // Clear unconditionally post-claim: if the advancer won, its ActivateHeadItem (which reads
        // _executingItem under the lock) completed before our lock section above; any future C-path
        // win requires a fresh publish that rewrites _executingItem first. Without this the item
        // strands in _executingItem across the whole idle period.
        SetExecutingItem(default!);

        if (task.IsCompleted)
        {
            if (task.IsCompletedSuccessfully)
            {
                task.GetAwaiter().GetResult();
                CompleteWaiter(item, null);
                return null;
            }

            return RecoverCommittedTailWaiterAsync(item, task).AsTask();
        }

        CommitWaiter(item, activated: alreadyActivated, task);
        return null;
    }

    // Awaited inline by the executor (via CommitTailWaiter) so the recovery happens on the
    // executor's logical thread. This preserves the SPSC contract on _waiters (executor is sole
    // producer), keeps pipeline ordering correct (recovery enqueues before subsequent items are
    // committed), and avoids the dual-producer hazard that a fire-and-forget continuation pattern
    // would create.
    [MethodImpl(MethodImplOptions.NoInlining)]
    async ValueTask RecoverCommittedTailWaiterAsync(T item, ValueTask task)
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
        if (exception is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            CompleteWaiter(item, recoveryFault.InnerException);
            return;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTask, exception);
        if (!_policy.TryRecoverItemFailure(context, item, _enumerator.CompletionToken, out var recoveryItem))
        {
            CompleteWaiter(item, exception);
            return;
        }

        // Republish and gate activation on the count, mirroring RecoverItem: activating
        // unconditionally while prior waiters are in flight would put a second active reader on the
        // wire.
        SetExecutingItem(recoveryItem);
        Volatile.Write(ref _hasInFlightItem, true);
        var recoveryActivated = false;
        if (_waiters.Count is 0)
        {
            // Recovery substitutes - activation state transfers with the position, so this ungated
            // activate over the predecessor's vacuous grant is the design, not a race. Full argument
            // at RecoverItem's twin arm.
            ActivateHeadItem(recoveryItem, preferAsync: false);
            recoveryActivated = true;
        }
        else
        {
            // Release publish (STLR), no lock - see RecoverItem's twin: claimants are
            // lock-serialized and miss-tolerant; the release orders the item before the flag.
            Volatile.Write(ref _executingItemActivationPending, true);
        }

        PipelineItemResult result;
        try
        {
            result = await _policy.ExecuteItemAsync(recoveryItem, waiterExecution: false, _enumerator.CompletionToken).ConfigureAwait(false);
        }
        catch (Exception recoveryEx)
        {
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, recoveryEx);
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
                ClearExecutingItem(recoveryActivated);
                CompleteWaiter(recoveryItem, ex);
                return;
            }
        }

        var pipelineTask = result.PipelineTask;
        if (pipelineTask.IsCompleted)
        {
            ClearExecutingItem(recoveryActivated);
            Exception? pipelineException = null;
            try
            {
                pipelineTask.GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                pipelineException = ex;
            }
            CompleteWaiter(recoveryItem, pipelineException);
        }
        else if (_enumerator.CompletionToken.IsCancellationRequested)
        {
            // Pipeline shutdown while recovery's async work was in flight. EnqueueWaiter at this
            // point would leak: OnWaiterTaskCompleted bails on wake completion, and DrainOnCompletionAsync
            // has likely already passed. Complete directly so depth tracking and CompleteItem still fire.
            ClearExecutingItem(recoveryActivated);
            CompleteWaiter(recoveryItem, _completionException);
        }
        else
        {
            // The recovery enters the store as an ordinary waiter; its identity travels in the TASK
            // (the guard wrapper rethrows late faults as RecoveryItemFaultException, see the marker
            // type), not in pipeline state - no fields, no value-T-unsound item comparisons.
            //
            // Reclaim the publish under the lock before the commit (this runs inside the executor's
            // commit await, so the tail slot is not an option). Mirrors CommitTailWaiter: entering
            // the lock means any advancer C-path ActivateHeadItem finished, so the commit below
            // cannot race it - the old Exchange + empty fence-acquire pair collapses into the claim.
            // Executor strand: the only lock-free publisher of the flag is this strand itself, so a
            // plain in-lock read is exact.
            bool alreadyActivated;
            if (_activationLock is { } activationLock)
            {
                lock (activationLock)
                {
                    alreadyActivated = !_executingItemActivationPending;
                    _executingItemActivationPending = false;
                }
            }
            else
            {
                alreadyActivated = !_executingItemActivationPending;
                _executingItemActivationPending = false;
            }
            // Unconditional post-claim clear (see CommitTailWaiter): without this the recovery item
            // strands in _executingItem across the idle period.
            SetExecutingItem(default!);
            Volatile.Write(ref _hasInFlightItem, false);
            CommitWaiter(recoveryItem, activated: alreadyActivated, GuardRecoveryTask(pipelineTask));
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

    void CompleteWaiter(T item, Exception? exception)
    {
        if (CompleteWaiterDeferred(item, exception))
            _depthState.OnDepthReachedZero();
    }

    /// Same as <see cref="CompleteWaiter"/> but skips the <see cref="DepthState.OnDepthReachedZero"/>
    /// signal so the caller can defer it (returns true iff depth reached 0). Drain paths holding the
    /// advancer latch or _activationLock use this: firing OnDepthReachedZero there would resume external
    /// WaitForEmptyAsync awaiters while internal sync is still held. Defer until the drainer releases.
    bool CompleteWaiterDeferred(T item, Exception? exception)
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
        // Release the activation turn: this item retired (its use of the policy's shared per-connection
        // resource ended before its waiter task completed), so a deferred next activation may now be
        // granted. Head-ordered retirement + one-live-activation makes an idempotent clear here correct.
        Volatile.Write(ref _liveActivation, false);
        DrainTrace.RecordItem(DrainTrace.Kind.Retire, item, depth, exception is null ? 0 : 1);
        _policy.CompleteItem(item, depth, exception);
        return depth is 0;
    }

    /// Commits a waiter to the store and coordinates activation with the execution loop. The
    /// store routes to the inline slot when empty (zero alloc), otherwise it escalates to the
    /// SPSC queue (allocates queue + lock on first overlap). All call sites run on the executor's
    /// logical thread, preserving the SPSC single-producer contract. The wired completion
    /// callback dispatches on _waiters.IsEscalated at fire time (slot occupant -> DrainSlotInline,
    /// queued or post-escalation moved-to-head -> DrainReadyWaiters).
    void CommitWaiter(T item, bool activated, ValueTask waiterTask)
    {
        // Allocate the activation lock on first commit (slot or queue): the count==0 handoff
        // serializes against the executor's deferred-publish + ClearExecutingItem fence-acquire,
        // and deferred-publish kicks in for any subsequent iter once Count > 0. One small Lock
        // allocation per Pipeline lifetime, amortized across all subsequent commits and runs.
        _activationLock ??= new Lock();

        // Capture and wire while this commit still exclusively owns the task. Publication hands
        // the pair to a store a concurrent drainer may claim and CONSUME at any moment, and
        // consumption ends the task token's lifetime: the pooled promise behind it is re-rented
        // by a later item, and any read of the stale token throws out of the pump. So the
        // completion status is read once here, the callback is registered here, and after the
        // publish below this method never touches waiterTask again - every later decision keys
        // on store words. The deferral gate keeps a completion that lands between this
        // registration and the publish from running its drain pass against a store that does not
        // yet contain the item (the pass would consume the drain signal, and the count-gated
        // conservation cannot restore at count zero); the post-publish replay runs it once the
        // item is visible.
        var wasCompletedAtCommit = waiterTask.IsCompleted;
        if (!wasCompletedAtCommit)
        {
            Volatile.Write(ref _commitFireGate, CommitFireArmed);
            waiterTask.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onWaiterTaskCompletedAction);
        }

        var count = _waiters.TryEscalateOrEnqueue(item, waiterTask, out _, out var slotWasMoved);
        var wasEmpty = count is 1;
        DrainTrace.RecordItem(DrainTrace.Kind.Commit, item, count, slotWasMoved ? 1 : 0);

        // The atomic _executingItemActivationPending claim already happened in CommitTailWaiter /
        // ClearExecutingItem. Just clear for hygiene and inline-activate if we're the head.
        if (!activated)
        {
            _executingItemActivationPending = false;
            SetExecutingItem(default!);

            if (wasEmpty && !wasCompletedAtCommit)
            {
                // Live-activation gate, serialized with the C-path under _activationLock: only self-activate
                // the sole committed waiter if no reader is live. If one is (the executor's in-flight item
                // was granted the turn via the C-path), leave this item committed and the advancer activates
                // it in FIFO order (count>0 branch) when the live reader retires.
                // NOTE: this gate looks redundant and is not. Deleting it reproduces the commit-vs-C-path
                // double activation under soak. Do not remove it without a model-level witness of why it
                // would be safe.
                lock (_activationLock!)
                {
                    if (!_liveActivation)
                    {
                        // The captured verdict can be stale by now: the task may have completed and a
                        // drainer already serving the store may have claimed and retired the item.
                        // Activating a retiree grants a turn nothing will ever release, and the next
                        // committed item strands on the closed gate. The spurious activation is
                        // accepted (same cost as the D-path guards); the verify below detects the
                        // retirement purely from store words and releases the grant. Task state is
                        // deliberately NOT re-read - the token may already be consumed and recycled.
                        DrainTrace.RecordItem(DrainTrace.Kind.CommitSelfAct, item);
                        ActivateHeadItem(item, preferAsync: true);

                        // Post-grant verify, store words only, in this order: count re-read zero (the
                        // acquire that orders the taken probe behind the drain's fenced decrement),
                        // then head taken. Under complete-before-decrement the conjunction proves the
                        // item fully retired. Taken-but-counted is NOT a trip: that grant is transient
                        // and the item's own drain releases it, and clearing the slot there would
                        // stomp a live tenure. On a trip, release our own grant and clear the slot;
                        // the drain's own clears are idempotent against ours, and no re-grant can
                        // interleave because every granter serializes on this lock.
                        if (_waiters.Count is 0 && _waiters.CommitHeadTaken())
                        {
                            DrainTrace.RecordItem(DrainTrace.Kind.CommitVerifyClear, item);
                            SetActivatedItem(default!);
                            Volatile.Write(ref _liveActivation, false);
                        }
                    }
                    else
                    {
                        DrainTrace.RecordItem(DrainTrace.Kind.CommitGateSkip, item);
                    }
                }
            }
        }

        // Callback disposition, post-publish. A task completed at capture was never registered -
        // run its pass now that the item is visible. Otherwise drain the deferral gate: a fire
        // that landed in the registration-to-publish window was recorded instead of run, and the
        // replay owns it here. Idle means no early fire - the callback runs on its own when the
        // task completes, finding the item already visible.
        if (wasCompletedAtCommit)
            _onWaiterTaskCompletedAction();
        else if (Interlocked.Exchange(ref _commitFireGate, CommitFireIdle) == CommitFireDeferred)
            _onWaiterTaskCompletedAction();

        // Compensation check for the slot drainer's chain arm: if our escalation's move raced
        // the drainer's head peek, the drainer published its activation obligation instead of
        // waiting out our two enqueue writes. We claim it exactly once via the flag Exchange
        // against the drainer's one-shot re-peek, and decide on the completion verdict CAPTURED
        // at the escalation claim - the moved pair is published at the queue head and a drain may
        // consume its task at any moment, so no fresh read of it is legal here. A verdict gone
        // stale (completed after the capture) costs one spurious activation whose turn the item's
        // own drain releases; a completed moved head is drained, not activated - the nudge below
        // picks it up.
        if (slotWasMoved)
        {
            _waiters.TakeMovedSlotPair(out var movedItem, out var movedWasCompleted);
            var claimed = Interlocked.Exchange(ref _pendingHeadActivation, false);
            DrainTrace.RecordItem(DrainTrace.Kind.CommitMoved, movedItem, claimed ? 1 : 0, movedWasCompleted ? 1 : 0);
            if (claimed && !movedWasCompleted)
            {
                ActivateHeadItem(movedItem, preferAsync: true);
            }
        }

        // During first escalation a slot callback can fire after the queue is published but
        // before the slot contents are moved into it, bumping completedCount without finding
        // anything to drain. Without this nudge the slot item would wait for the next callback
        // fire, unbounded when the next item is a long-lived, long-running exclusive operation.
        // Losing the acquire deposits the obligation on the holder (including a stale-verdict
        // reclaim hold).
        // Every commit checks, not just the escalation move: a set signal at commit time means
        // a completed entry may be resident with its callback already spent, and committing
        // threads are the one actor guaranteed to keep arriving while the pipeline is fed.
        // Cost when the signal is clear: one volatile read.
        if (Volatile.Read(ref _drainSignal))
        {
            var acquired = _advancing.TryAcquireOrFlagPending();
            DrainTrace.RecordItem(DrainTrace.Kind.CommitNudge, item, acquired ? 1 : 0);
            if (acquired)
            {
                if (_waiters.IsEscalated)
                    DrainReadyWaiters();
                else
                    DrainSlotInline();
            }
        }
    }

    /// Called when a committed waiter's task completes. Dispatches to the drain matching the
    /// store's tier at fire time: slot occupant (pre-escalation) -> DrainSlotInline; queued
    /// entry, or a slot occupant whose contents were moved to queue head by a later
    /// escalation, -> DrainReadyWaiters.
    void OnWaiterTaskCompleted()
    {
        // Deferral gate: a fire landing while the owning commit is between its pre-publication
        // registration and its publish is recorded and replayed by the commit once the item is
        // visible (see CommitWaiter). Running the pass early would consume the drain signal
        // against a store that does not yet contain the item, and the count-gated conservation
        // cannot restore at count zero - the item would strand with its callback spent. The
        // callback is shared across items, so this can also defer a PRIOR item's fire that lands
        // in the window; the replayed pass drains every ready item, so the obligation is
        // preserved, just coalesced.
        if (Interlocked.CompareExchange(ref _commitFireGate, CommitFireDeferred, CommitFireArmed) == CommitFireArmed)
        {
            DrainTrace.Record(DrainTrace.Kind.CbDeferred);
            return;
        }

        // Plain store: the acquire RMW below is the full fence that publishes it. The flag
        // keeps the materializing-token role (gates the nudge and the reclaim); the lost-wake
        // hole the flag alone could not close now lives in the latch word.
        _drainSignal = true;
        DrainTrace.Record(DrainTrace.Kind.CbFire, _waiters.IsEscalated ? 1 : 0);

        // Try to become the advancer, only one thread processes completions at a time.
        // Process even during shutdown: drain's wait-for-advancer-idle is what coordinates with
        // us, and bailing out here would let drain "complete" the item via DrainOnCompletionAsync's
        // queue sweep while the body's pipeline task is still running, stranding the item in a
        // half-completed state (activated slot cleared but its body still reading).
        if (!_advancing.TryAcquireOrFlagPending())
        {
            // Obligation deposited in the latch word; the holder's release serves it.
            DrainTrace.Record(DrainTrace.Kind.CbBail);
            return;
        }
        DrainTrace.Record(DrainTrace.Kind.CbAcq, _waiters.IsEscalated ? 1 : 0);

        if (_waiters.IsEscalated)
            DrainReadyWaiters();
        else
            DrainSlotInline();
    }

    /// Slot-mode drain. Advancer latch is held by the caller. CAS-claims the slot and processes
    /// the (item, task). A concurrent escalation that won the CAS leaves nothing here to drain
    /// and we re-route to the queue drain.
    void DrainSlotInline()
    {
        while (true)
        {
        if (!_waiters.TryClaimSlotForDrain(out var item, out var task))
        {
            // Escalation got there first, and head of queue is our item now.
            if (_waiters.IsEscalated)
            {
                DrainReadyWaiters();
                return;
            }
            // Empty, a still-running occupant (tri-state peek bail), or claimed elsewhere.
            // (No signal bookkeeping: a stale dirty flag costs one spurious reclaim check.)
            var failServeDeposit = _advancing.ReleaseAndCheckPending();
            SignalDrainWakeupIfWaiting();
            // A deposit during our hold transfers the obligation here; re-enter the claim
            // loop to serve it. Losing the re-acquire re-deposits on the winner.
            var failReacquired = failServeDeposit && _advancing.TryAcquireOrFlagPending();
            DrainTrace.Record(DrainTrace.Kind.SlotFailRel, failServeDeposit ? 1 : 0, failReacquired ? 1 : 0);
            if (failReacquired)
            {
                continue;
            }
            return;
        }
        DrainTrace.RecordItem(DrainTrace.Kind.SlotClaim, item);

        // Pass start: consume the signal (full fence orders the clear before this pass's
        // reads). Completions that fire during the pass re-set it and the post-release
        // recheck below catches them - the clear/fence/recheck gate.
        Interlocked.Exchange(ref _drainSignal, false);

        // Consume the waiter task BEFORE DecrementCount publishes the freed position to the
        // executor's Count==0 inline-activation gate. The consume is the release point for
        // per-item resources tied to the task (pooled/shared IValueTaskSource implementations
        // reset in GetResult), and a successor dispatched the instant the count drops must
        // find them released. The queue drain already orders this way (consume at the head,
        // DecrementCount after processing): without it a successor could reuse a per-item shared
        // resource before its release, surfacing an already-started fault.
        Exception? taskException = null;
        try
        {
            task.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            taskException = ex;
        }

        // Fault decision BEFORE the decrement, mirroring the queue drain. Decrementing first would
        // publish the freed position mid-recovery: the executor's Count==0 inline-activation gate
        // could dispatch a successor against the recovery's own activation (a second active reader),
        // and the recovery rejoin's AdvanceAndDrain would decrement the SAME position again (a double
        // decrement, which the -1 skew arm then misattributes to a counted successor).
        bool emptyReached = false;
        if (taskException is not null && !RecoverWaiter(item, taskException, out emptyReached))
        {
            // Recovery item taking over; the count still carries this position. The advancer
            // flag stays held, and the recovery continuation decrements exactly once and
            // re-runs the activation partition via AdvanceAndDrainRecovery's slot-mode rejoin.
            return;
        }

        // Complete BEFORE Decrement. CompleteWaiterDeferred's _activatedItem clear is depth-gated
        // (it only fires when this completion drains the pipeline to empty), so an intermediate
        // completion cannot stomp a live successor in the first place. The depth-0 boundary case
        // still needs this ordering: were the store count decremented first, the inline-activation
        // gate (Count==0) could fire on another path - executor's deferred-publish branch, a
        // successor commit's wasEmpty inline-activate - admitting a brand-new item and writing
        // _activatedItem = NEW between our DecrementDepth-to-0 and the gated clear, which would then
        // null NEW. NEW's body's first decoder read would see _activatedItem=null and NRE in
        // CurrentExecutionControl. Running the complete (hence the clear) while the store count still
        // carries this position keeps that gate shut until after the clear. Mirrors the recovery-fault
        // path's Complete-before-Decrement ordering.
        if (taskException is null)
            emptyReached = CompleteWaiterDeferred(item, null);

        // The skew floor and the at-or-below-zero partition rationale live in the store (see
        // DecrementCount). No count==0 assertion: TryClaimSlotForDrain already emptied the
        // slot, so a successor's commit can CAS into it (or first-escalate) before our decrement
        // lands - the store's _hasSlot CAS is the ownership contract, not the count.
        var ownsCPath = _waiters.DecrementCount();

        ActivateNextAfterSlotAdvance(ownsCPath);

        // Capture the deposit consumed by the release: a contended acquirer (a callback, the nudge)
        // that bailed against this hold flagged the latch word. The obligation must be SERVED, not
        // delegated to the flag - the slot drain's own claim cleared _drainSignal at consume time, so
        // a deposit whose companion flag the claim ate would vanish if the reclaim below read only the
        // flag (stranding the queue head). OR-ing the deposit into the reclaim gate carries the
        // obligation past the flag's self-consumption.
        var serveDeposit = _advancing.ReleaseAndCheckPending();
        DrainTrace.Record(DrainTrace.Kind.SlotRel, serveDeposit ? 1 : 0, emptyReached ? 1 : 0);
        // Signal AFTER advancer release: prevents a WaitForEmptyAsync awaiter from resuming and
        // committing a new slot waiter whose callback would then race the still-held advancer
        // (the callback's failed acquire deposits, costing the holder a serve pass).
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalDrainWakeupIfWaiting();

        // Serve a consumed deposit unconditionally. ReleaseAndCheckPending cleared the pending bit,
        // so the obligation is ours and must not be gated on cancellation. During shutdown the drain
        // never dequeues, so a dropped wake strands the waiter and hangs completion on Count > 0.
        if (serveDeposit)
        {
            var serveReacquired = _advancing.TryAcquireOrFlagPending();
            DrainTrace.Record(DrainTrace.Kind.SlotServeReacq, serveReacquired ? 1 : 0);
            if (serveReacquired)
            {
                if (_waiters.IsEscalated)
                {
                    DrainReadyWaiters();
                    return;
                }
                continue;
            }
            return;
        }

        // Dirty-flag reclaim: a successor's waiter that completed while we held the advancer set the
        // signal from its callback, and without reclaim its activation is lost. A set signal in
        // non-escalated slot mode means that waiter is the current slot occupant with a completed
        // task, so looping back to the claim is safe. Gated off during shutdown, where the drain
        // sweep owns completions and the deposit-serve above already covers any real bail.
        var reclaimed = !_enumerator.CompletionToken.IsCancellationRequested
            && Volatile.Read(ref _drainSignal)
            && _advancing.TryAcquireOrFlagPending();
        DrainTrace.Record(DrainTrace.Kind.SlotReclaim, reclaimed ? 1 : 0);
        if (reclaimed)
        {
            if (_waiters.IsEscalated)
            {
                DrainReadyWaiters();
                return;
            }
            continue;
        }
        return;
        }
    }

    /// The slot-mode activation partition on a DecrementCount result, shared by
    /// DrainSlotInline's advance path and the recovery rejoin (AdvanceAndDrain's slot branch).
    /// Caller holds the advancer latch.
    void ActivateNextAfterSlotAdvance(bool ownsCPath)
    {
        if (!ownsCPath)
        {
            // Slot-mode D-path, mirroring DrainReadyWaiters' D-path arm: the successor's
            // commit-increment preceded our decrement, so that commit observed the claimed-but-
            // uncounted position (wasEmpty false) and skipped self-activation - the partition
            // designates us its activator. Without this arm both sides skip and the activation
            // decision is lost (a hang with a null pending-activation control).
            if (_waiters.TryPeekSlotForActivation(out var next, out var nextTask))
            {
                // Mirror CommitWaiter's guard: a completed-at-commit waiter is never
                // activated - its callback already fired (and bailed against our held
                // latch), so the post-release reclaim below drains it.
                if (!nextTask.IsCompleted)
                {
                    DrainTrace.RecordItem(DrainTrace.Kind.SlotDPathAct, next);
                    ActivateHeadItem(next, preferAsync: true);
                }
                else
                {
                    DrainTrace.RecordItem(DrainTrace.Kind.SlotDPathDone, next);
                }
            }
            else
            {
                // A first escalation is (or was) relocating the head to the queue; hand the
                // obligation to the escalating commit instead of waiting out its move.
                // Publish the obligation, then re-peek ONCE: the flag ops are full fences,
                // so an invisible head means the escalator's enqueue is not yet ordered
                // before our peek - its compensation check (CommitWaiter, slotWasMoved) is
                // still ahead and will claim the flag. A visible head means we claim our
                // own flag back; whichever Exchange wins owns the activation, exactly once.
                // No thread ever waits on another thread's progress.
                Interlocked.Exchange(ref _pendingHeadActivation, true);
                var headVisible = _waiters.TryPeek(out var head);
                var claimedBack = headVisible && Interlocked.Exchange(ref _pendingHeadActivation, false);
                DrainTrace.RecordItem(DrainTrace.Kind.SlotDPathPend, head.Waiter,
                    claimedBack ? 1 : 0, headVisible && head.WaiterTask.IsCompleted ? 1 : 0);
                if (claimedBack && !head.WaiterTask.IsCompleted)
                {
                    ActivateHeadItem(head.Waiter, preferAsync: true);
                }
            }
        }
        else
        {
            // Same lock-guarded claim+activate as DrainReadyWaiters' count==0 branch: the executor may
            // have deferred-published a next item against our Count==1 read. If so, claim and activate
            // under the lock so its ClearExecutingItem fence-acquire sees us done. Consume the publish
            // ONLY at Count == 0: a successor that raised the count since our decrement observed count 1
            // (wasEmpty) and self-activated, and its own drain continues the chain. Consuming here
            // without activating would orphan the published item (slot mode has no recurring D-path to
            // recover it). The Count-then-Exchange TOCTOU is benign: a commit's Exchange precedes its
            // count increment, so whichever Exchange wins owns that item's activation exactly once.
            // Pre-check outside the lock, an entry filter only: no publish pending means nothing to
            // claim, so skip the lock acquire on the common no-publish drain. The read itself is an
            // acquire load, fresh per drain. It writes nothing and skips nothing else; a stale-false
            // read is rescued by the committer's own locked arm (the wasEmpty self-activate or the
            // reclaim), which re-reads under its lock.
            if (Volatile.Read(ref _executingItemActivationPending))
            {
                lock (_activationLock!)
                {
                    // Count is clamped, so == 0 also covers the -1 face (see DecrementCount): the
                    // committer's own late increment returns wasEmpty false and its IsCompleted guard
                    // skips, so without this arm both sides skip and the publish strands.
                    if (_waiters.Count is 0 && Volatile.Read(ref _executingItemActivationPending))
                    {
                        // Claim is lock-serialized; the acquire load above orders the item read (see the
                        // queue C-path). Plain clear.
                        _executingItemActivationPending = false;
                        var executing = _executingItem;
                        DrainTrace.RecordItem(DrainTrace.Kind.SlotCPathAct, executing);
                        // _executingItem stays populated: the main loop is concurrently about to call
                        // _policy.ExecuteItemAsync(executing), which needs ExecutingItem for write-phase
                        // gating. The next iteration overwrites or clears it naturally.
                        ActivateHeadItem(executing, preferAsync: true);
                    }
                    else
                    {
                        DrainTrace.Record(DrainTrace.Kind.SlotCPathSkip, _waiters.Count,
                            Volatile.Read(ref _executingItemActivationPending) ? 1 : 0);
                    }
                }
            }
        }
    }

    /// The advancer loop: processes completed waiters from the head of the queue in order.
    /// Only one thread runs this at a time, ensuring head-first processing and ordered completion.
    /// Reachable only after a waiter was committed and escalated to the queue (slot-mode drains
    /// route through DrainSlotInline), so _activationLock is non-null and _waiters.Queue
    /// is non-null on every code path here.
    void DrainReadyWaiters()
    {
        var emptyReached = false;
        while (true)
        {
            // Sub-pass start: consume the signal (full fence orders the clear before the pass's queue
            // reads). Completions firing mid-pass re-set it and the do-while's post-release recheck
            // catches them - the clear/fence/recheck gate.
            Interlocked.Exchange(ref _drainSignal, false);
            DrainTrace.Record(DrainTrace.Kind.QPass, _waiters.Count);
            var drainedAny = false;

            while (_waiters.TryPeek(out var item) && item.WaiterTask.IsCompleted)
            {
                _waiters.TryDequeue(out _);
                drainedAny = true;
                DrainTrace.RecordItem(DrainTrace.Kind.QDrain, item.Waiter);

                // Process the completed waiter. The consume here already precedes this
                // path's DecrementCount below - the queue drain has always had the
                // consume-before-republish ordering (see DrainSlotInline).
                var waiter = item.Waiter;
                Exception? taskException = null;
                try
                {
                    item.WaiterTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    taskException = ex;
                }

                bool advance;
                if (taskException is null)
                {
                    // Accumulate the idle signal across the do-while: depth can reach 0 mid-loop
                    // (last drained item), but the OR captures it for after the final release.
                    emptyReached |= CompleteWaiterDeferred(waiter, null);
                    advance = true;
                }
                else
                {
                    advance = RecoverWaiter(waiter, taskException, out var recoveryEmpty);
                    emptyReached |= recoveryEmpty;
                }

                if (!advance)
                {
                    // Recovery item is occupying this pipeline position. The advancer flag stays
                    // held. The recovery continuation will complete the item, advance via
                    // AdvanceAndDrainRecovery, and release the flag (which signals any waiting drain).
                    // The recovery continuation also fires the idle signal if applicable, so it is
                    // safe to forget our local emptyReached here (cannot be true: depth hasn't yet
                    // hit zero or we wouldn't be doing recovery).
                    return;
                }

                // Advance: decrement queue count and take the activation-responsibility partition.
                // The skew floor and the C-path-owns-the--1 rationale live in the store (see
                // DecrementCount).
                if (_waiters.DecrementCount())
                {
                    // Pre-check outside the lock, an entry filter only: no publish pending means
                    // nothing to claim, so skip the lock acquire on the common no-publish drain and
                    // let the pass continue (further head drains, then the conservation/release
                    // tail). The read itself is an acquire load, fresh per pass (the decrement above
                    // is a full fence). It writes nothing and skips nothing else; a stale-false read
                    // is rescued by the committer's own locked arm (the wasEmpty self-activate or
                    // the reclaim), which re-reads under its lock.
                    if (Volatile.Read(ref _executingItemActivationPending))
                    {
                        // Last waiter drained. Hold the lock around claim+activate so the executor's
                        // ClearExecutingItem fence-acquire blocks until ActivateHeadItem finishes.
                        // Wrapping just the ActivateHeadItem call would leave a TOCTOU where the
                        // executor observes the Exchange and acquires its fence-lock uncontested.
                        lock (_activationLock!)
                        {
                            // The claim gates on the store being drained (Count is clamped, so `is 0` also
                            // covers the -1 face - see DecrementCount). A successor committing between our
                            // decrement and here reads non-zero, and the publish is the item we would have
                            // claimed, whose own commit sees alreadyActivated and does not self-activate -
                            // LEAVE it for the executor's CommitTailWaiter reclaim to activate;
                            // consuming-then-clearing here would strand the activation (neither side
                            // activates). The claim stays inside the lock so the executor's reclaim (which
                            // enters the same lock) cannot race ActivateHeadItem.
                            // Live-activation gate: only grant the turn if no activation is live (checked under
                            // the same _activationLock CommitWaiter's wasEmpty self-activate takes, so the two
                            // paths serialize). If one is live, leave the pending flag - its retirement clears
                            // _liveActivation and this C-path re-fires in the same advancer pass.
                            // Volatile.Read on the flag: claims are lock-serialized, but the executor's
                            // publish is a lock-free release store (STLR) - the acquire load orders the
                            // _executingItem read below after the flag observation (plain load-load can
                            // speculate on ARM; control dependency does not order loads). The clear is
                            // plain: claimants are lock-serialized and the executor-strand publishes
                            // never race a cross-strand clear (clears here are observation-gated).
                            if (_waiters.Count is 0 && !_liveActivation && Volatile.Read(ref _executingItemActivationPending))
                            {
                                // Peek-gated license: the count under-promises the store (a commit's enqueue
                                // precedes its increment), so a completed-unactivated head can be resident at
                                // count 0. Firing past it activates the publish out of FIFO order and the
                                // resident head's retirement clear then releases a turn its retiree never
                                // held. The peek is the residency truth, read fresh at the fire point under
                                // the advancer latch (the licensed consumer). A decline leaves publish and
                                // pending intact like the gate skip. This pass drains the resident head and
                                // the C-path re-fires.
                                if (!_waiters.TryPeek(out var resident))
                                {
                                    _executingItemActivationPending = false;
                                    var executing = _executingItem;
                                    DrainTrace.RecordItem(DrainTrace.Kind.QCPathAct, executing);
                                    ActivateHeadItem(executing, preferAsync: true);
                                }
                                else
                                {
                                    DrainTrace.RecordItem(DrainTrace.Kind.QCPathPeekDecline, resident.Waiter);
                                }
                            }
                            else
                            {
                                DrainTrace.Record(DrainTrace.Kind.QCPathSkip,
                                    (_waiters.Count is 0 ? 0 : 2) + (_liveActivation ? 1 : 0),
                                    Volatile.Read(ref _executingItemActivationPending) ? 1 : 0);
                            }
                        }
                    }
                }
                else
                {
                    // More waiters remain, activate the next one (different item from _executingItem
                    // so no executor-completion race, no activation lock needed). count > 0 implies a
                    // peekable head (the count under-promises the queue, never over-promises), so the
                    // failed peek is a canary, not a case.
                    if (_waiters.TryPeek(out var nextItem))
                    {
                        // Completed-head guard, mirroring the slot D-path: a completed-at-commit head
                        // is drain-only, and this pass's own next peek dequeues it. Best-effort
                        // dispatch saver, not load-bearing. A stale not-completed read costs one
                        // spurious activation whose turn the item's own drain promptly releases.
                        if (!nextItem.WaiterTask.IsCompleted)
                        {
                            DrainTrace.RecordItem(DrainTrace.Kind.QDPathAct, nextItem.Waiter);
                            ActivateHeadItem(nextItem.Waiter, preferAsync: true);
                        }
                        else
                        {
                            DrainTrace.RecordItem(DrainTrace.Kind.QDPathDone, nextItem.Waiter);
                        }
                    }
                    else
                        Debug.Assert(false, "count > 0 with no peekable queue head.");
                }
            }

            // SIGNAL CONSERVATION, count-gated: a pass that consumed the signal and dequeued nothing
            // did not satisfy it - the signaled work is still materializing (the escalation's slot-to-
            // queue move, or a commit between its enqueue and its increment) and was truthfully
            // invisible to our peeks. Destroying the token here would let every later rescue (the
            // one-shot nudge, the reclaim) read FALSE and decline, stranding completion. Restore before
            // the release: the release's full fence publishes it, and the materializer's own tail check
            // finds the token. The count gate kills the dangling restore - with no entry resident there
            // is no materializer left to owe a token to, and an unconditional restore fed a
            // reclaim-retry livelock.
            if (!drainedAny)
            {
                var conserveCount = _waiters.Count;
                DrainTrace.Record(DrainTrace.Kind.QConserve, conserveCount > 0 ? 1 : 0, conserveCount);
                if (conserveCount > 0)
                {
                    _drainSignal = true;
                }
            }

            // The release Exchange consumes a deposited obligation atomically with releasing: a
            // contended acquirer (callback, nudge, a sibling reclaim) that lost against our hold
            // flagged the latch word instead of spending a one-shot wake against a recheck that
            // already ran - the two-location strand the plain latch + flag protocol could not close.
            // Deposit set means we are obligated: re-acquire and run another pass. Losing the
            // re-acquire re-deposits on the winner, whose own release then serves - obligation
            // transfer, no kernel wait.
            if (_advancing.ReleaseAndCheckPending())
            {
                var serveReacquired = _advancing.TryAcquireOrFlagPending();
                DrainTrace.Record(DrainTrace.Kind.QRelServe, serveReacquired ? 1 : 0);
                if (serveReacquired)
                    continue;
                break;
            }
            DrainTrace.Record(DrainTrace.Kind.QRel);

            // No deposit consumed: re-check for late signals. The sandwich argument (release
            // fence before, TryReclaim's acquire fence after) licenses a plain read here;
            // Volatile.Read defeats JIT hoisting.
            var recheck = !_enumerator.CompletionToken.IsCancellationRequested
                && Volatile.Read(ref _drainSignal);
            DrainTrace.Record(DrainTrace.Kind.QRecheck, recheck ? 1 : 0);
            if (!recheck || !TryReclaimAdvancerForWork())
                break;
        }

        // Signal AFTER advancer release for the same reason as DrainSlotInline (external awaiter
        // must not resume while internal sync is still held).
        if (emptyReached)
            _depthState.OnDepthReachedZero();
        SignalDrainWakeupIfWaiting();

        // Re-acquire the advancer latch and check for a ready waiter. Returns true with the
        // latch held if the pass should continue, false otherwise. TryPeek MUST run inside the
        // latch's protection so the SPSC single-consumer contract on _waiters holds against a
        // racing OnWaiterTaskCompleted caller.
        bool TryReclaimAdvancerForWork()
        {
            // The transient hold had a strand: a callback's one-shot wake bailed against the hold
            // while the miss path below released with no post-release rendezvous - signal set, latch
            // free, completed entry resident, nobody left. The pending word closes it: a bail against
            // this hold deposits in the latch word, and the miss release reads the deposit atomically
            // with releasing and serves.
            if (!_advancing.TryAcquireOrFlagPending())
            {
                // Obligation deposited on the winner; its release serves.
                DrainTrace.Record(DrainTrace.Kind.QReclaimBail);
                return false;
            }

            if (_waiters.TryPeek(out var pending) && pending.WaiterTask.IsCompleted)
            {
                DrainTrace.RecordItem(DrainTrace.Kind.QReclaimHit, pending.Waiter);
                return true;
            }

            // Bounded stale-view retry: this reclaim is the last reader of the signal that
            // brought it here, and a completed head invisible to this exact peek would strand
            // forever (its callback is spent). A genuine miss re-misses instantly, a stale view
            // has a beat to converge. The commit-side nudge covers the fed pipeline, this covers
            // a quiescent tail.
            if (_waiters.Count > 0)
            {
                var spinner = new SpinWait();
                for (var attempt = 0; attempt < 4; attempt++)
                {
                    spinner.SpinOnce();
                    if (_waiters.TryPeek(out pending) && pending.WaiterTask.IsCompleted)
                    {
                        DrainTrace.RecordItem(DrainTrace.Kind.QReclaimHit, pending.Waiter, 1);
                        return true;
                    }
                }
            }

            if (_advancing.ReleaseAndCheckPending())
            {
                // A bail landed against our transient hold after the peek's verdict went
                // stale. Re-acquire and continue the pass; losing re-deposits on the winner.
                var reacquired = _advancing.TryAcquireOrFlagPending();
                DrainTrace.Record(DrainTrace.Kind.QReclaimMiss, 1, reacquired ? 1 : 0);
                return reacquired;
            }
            DrainTrace.Record(DrainTrace.Kind.QReclaimMiss);
            return false;
        }
    }

    /// Returns true if the advancer should continue (recovery completed or no recovery),
    /// false if recovery is occupying this pipeline position (advancer must stop, recovery will resume knowing it held the advancer flag).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal up to the drain caller
    /// on the sync return-true paths so it fires after the advancer release. The async return-false
    /// path now fires the depth-0 signal on EVERY branch: deferred completions thread emptyReached to
    /// AdvanceAndDrainRecovery(emptyReached) (fires after the advancer release); the inline
    /// CompleteRecoveryWaiter branches fire OnDepthReachedZero inline. (A queue-count reclaim window
    /// remains benign - DrainReadyWaiters' do-while reclaim catches stranded counts downstream - but
    /// the depth-0 idle signal is no longer among the things that can strand.)
    [MethodImpl(MethodImplOptions.NoInlining)]
    // The caller consumed the waiter task (the consume-before-republish ordering, see
    // DrainSlotInline) and hands over the extracted exception.
    bool RecoverWaiter(T failedItem, Exception ex, out bool emptyReached)
    {
        emptyReached = false;

        // A committed executor-side recovery faulting late, identified by the guard wrapper's
        // marker exception (see RecoverCommittedTailWaiterAsync): completed directly with the
        // real fault - recovery is the cleanup of last resort, its own failure means the wire
        // is gone and there is nothing a policy could substitute. This is what entitles
        // policies to assert they are never consulted about their own recovery items.
        if (ex is Pipeline.RecoveryItemFaultException recoveryFault)
        {
            emptyReached = CompleteWaiterDeferred(failedItem, recoveryFault.InnerException);
            return true;
        }

        var context = new PipelineItemFailureContext(PipelineItemFailureKind.PipelineTaskWaiter, ex);
        if (!_policy.TryRecoverItemFailure(context, failedItem, _enumerator.CompletionToken, out var recoveryItem))
        {
            emptyReached = CompleteWaiterDeferred(failedItem, ex);
            return true;
        }

        // Recovery item takes over, activate it at the current pipeline position. Ungated and
        // in place, deliberately: the substitution transfers the drained waiter's turn, and this
        // position still holds its count credit, so no commit can read wasEmpty and self-activate
        // concurrently. NOTE: do not add the activation gate here - the in-place activation is
        // load-bearing for liveness. A gate-skip would park this position's turn on the dead
        // predecessor while the substitute enqueues behind a head that never advances, wedging
        // the gate, the substitute, and the drain in a cycle none of them can break.
        ActivateHeadItem(recoveryItem);

        ValueTask<PipelineItemResult> executeTask;
        try
        {
            // waiterExecution: this is the waiter-drain recovery (RecoverWaiter off the advancer chain
            // via OnWaiterTaskCompleted). It can run on a non-pump thread, concurrently with the
            // executor's own next dispatch, so a policy must not share single-pump per-dispatch state.
            executeTask = _policy.ExecuteItemAsync(recoveryItem, waiterExecution: true, _enumerator.CompletionToken);
        }
        catch (Exception recoveryEx)
        {
            emptyReached = CompleteWaiterDeferred(recoveryItem, recoveryEx);
            return true;
        }

        // Publish the recovery item so the bailout path can complete it on shutdown.
        // The recovery continuation always completes this item itself (via CompleteRecoveryWaiter
        // on the normal path or BailoutRecoveryOnShutdown on shutdown), so no atomic claim race
        // with drain is needed, drain just waits for the advancer chain to quiesce.
        _waiterRecoveryItem = recoveryItem;

        if (executeTask.IsCompletedSuccessfully)
        {
            return RecoverWaiterResult(recoveryItem, executeTask.Result, out emptyReached);
        }

        // Execute is async, hook continuation. Advancer stops, continuation owns the flag.
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
                AdvanceAndDrainRecovery();
                return;
            }

            if (!RecoverWaiterResult(recoveryItem, result, out var emptyReached))
                return; // pipeline task pending, continuation will resume

            AdvanceAndDrainRecovery(emptyReached);
        });
        return false;
    }

    /// Handles a completed recovery execution result. Returns true if the advancer should continue,
    /// false if the recovery's pipeline task is pending (occupying this position).
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal on the sync return-true
    /// paths. The async continuation paths (RecoverWaiter / the trailing continuation) now capture this
    /// out-param and pass it to AdvanceAndDrainRecovery(emptyReached), which fires OnDepthReachedZero
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
                        AdvanceAndDrainRecovery();
                        return;
                    }

                    if (!RecoverWaiterPipelineTask(recoveryItem, pipelineTask, out var emptyReached))
                        return; // pipeline task pending, lock still held

                    AdvanceAndDrainRecovery(emptyReached);
                });
                return false;
            }
        }

        return RecoverWaiterPipelineTask(recoveryItem, result.PipelineTask, out emptyReached);
    }

    /// Handles the recovery item's pipeline task. Returns true if done, false if pending.
    /// <paramref name="emptyReached"/> propagates the deferred-empty signal to the caller on the sync
    /// return-true path; callers capture it and pass it to AdvanceAndDrainRecovery (fires
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

        // Pipeline task pending, hook continuation. Advancer stays held.
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
            AdvanceAndDrainRecovery();
        });
        return false;
    }

    /// Shared shutdown bailout for recovery continuations. Completes the recovery item with the
    /// shutdown exception, releases _advancing, and signals any drain waiting for the advancer
    /// chain to quiesce. The continuation always owns completion (no Exchange race with drain)
    /// because drain only waits for advancer-idle and does not compete for the recovery item.
    void BailoutRecoveryOnShutdown()
    {
        var recoveryItem = _waiterRecoveryItem;
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        CompleteWaiter(recoveryItem, _completionException);
        // The recovery park holds its position's count credit (the fault decision precedes the
        // decrement in both drains). Every park resolution settles it exactly once: the sync advance
        // paths decrement in their drain loop, the normal continuation decrements via AdvanceAndDrain,
        // and this bailout settles it here - without it the credit strands and DrainOnCompletionAsync's
        // count wait never falls to zero.
        // Partition result deliberately unused: this bailout only settles the count credit; no
        // activation follows (completion ownership transfers to the shutdown sweep below).
        _ = _waiters.DecrementCount();
        // Shutdown bailout: a deposit consumed here is deliberately dropped - completion
        // ownership has transferred to DrainOnCompletionAsync's queue sweep, which waits on
        // advancer-idle (signaled below) and sweeps without consulting the signal flag.
        _advancing.ReleaseAndCheckPending();
        SignalDrainWakeupIfWaiting();
    }

    /// Called from recovery continuations to continue advancer activity after the recovery item
    /// completion. Delegates to AdvanceAndDrain whose loop exit signals the advancer-idle TCS.
    ///
    /// <paramref name="emptyReached"/> carries the depth-0 crossing that happened INSIDE this
    /// continuation's CompleteRecoveryWaiterDeferred (the deferred variant decremented depth to 0 but
    /// did not fire OnDepthReachedZero - the continuation must). AdvanceAndDrain's own drain loops only
    /// fire off a freshly-accumulated local emptyReached (from draining ANOTHER waiter); when this
    /// recovery item was the LAST in-flight item there is nothing more to drain, so that local stays
    /// false and the zero-crossing would be lost - hanging a parked WaitForEmptyAsync. Fire it here,
    /// AFTER AdvanceAndDrain returns (the advancer is released by then, so the ordering matches the
    /// deferred contract: never fire OnDepthReachedZero while the advancer is held).
    void AdvanceAndDrainRecovery(bool emptyReached = false)
    {
        AdvanceAndDrain();
        if (emptyReached)
            _depthState.OnDepthReachedZero();
    }

    /// Signals any drain that's waiting for the advancer chain to quiesce. Null check is the
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
    /// chain to quiesce via AdvanceAndDrainRecovery's loop-exit signal.
    void CompleteRecoveryWaiter(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        CompleteWaiter(recoveryItem, exception);
    }

    /// <see cref="CompleteRecoveryWaiter"/> variant that returns the deferred-empty signal so the
    /// caller can fire <see cref="DepthState.OnDepthReachedZero"/> after releasing the advancer.
    /// Used by the sync recovery chain (<see cref="RecoverWaiter"/>, <see cref="RecoverWaiterResult"/>,
    /// <see cref="RecoverWaiterPipelineTask"/>) to avoid the same stranding window the slot drain fix
    /// closed: signalling OnDepthReachedZero while the drain caller still holds the advancer would
    /// let a WaitForEmptyAsync awaiter resume and commit a follow-up slot waiter whose callback
    /// would bail on TryAcquire.
    bool CompleteRecoveryWaiterDeferred(T recoveryItem, Exception? exception)
    {
        if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
            _waiterRecoveryItem = default!;
        return CompleteWaiterDeferred(recoveryItem, exception);
    }

    /// Decrements waiter count, activates the next item, and resumes draining.
    /// Must only be called when the advancer flag is held.
    void AdvanceAndDrain()
    {
        var ownsCPath = _waiters.DecrementCount();

        // Slot-mode rejoin (a slot drain parked for recovery; the count still carried the recovered
        // position - see DrainSlotInline's recovery return): the successor, if any, lives in the SLOT,
        // not the queue. The queue partition below would peek nothing and lose its activation (its
        // D-path arm asserts on the empty queue). Run the slot partition instead and re-enter the
        // slot claim loop, which serves deposits and releases the advancer.
        if (!_waiters.IsEscalated)
        {
            ActivateNextAfterSlotAdvance(ownsCPath);
            DrainSlotInline();
            return;
        }

        // Partition + checked peek, mirroring DrainReadyWaiters (this advance is the recovery
        // path's drain step and shares the count-skew exposure - see DecrementCount).
        if (ownsCPath)
        {
            // Pre-check outside the lock, an entry filter only, same as the queue C-path: skip the
            // lock acquire when no publish is pending (the read itself is an acquire load; the
            // decrement above is a full fence, so it is fresh). Writes nothing, skips nothing else;
            // a stale-false read is rescued by the committer's own locked arm.
            if (Volatile.Read(ref _executingItemActivationPending))
            {
                // Same lock-guarded claim+activate as DrainReadyWaiters: lock wraps the Exchange so
                // the executor's fence-acquire blocks until ActivateHeadItem finishes.
                // Advancer chain reachability implies _activationLock is non-null.
                lock (_activationLock!)
                {
                    // Count gates the Exchange - LEAVE the publish at Count > 0 for the executor's commit
                    // to reclaim and activate, never consume-and-clear it (see DrainReadyWaiters' C-path).
                    // Peek-gated like that C-path: a resident completed-unactivated head at count 0
                    // declines the license. Publish and pending stay put, and the DrainReadyWaiters
                    // pass below drains the head and re-fires.
                    if (_waiters.Count is 0 && !_waiters.TryPeek(out _) && Volatile.Read(ref _executingItemActivationPending))
                    {
                        // Lock-serialized claim; acquire load orders the item read (see the queue C-path).
                        _executingItemActivationPending = false;
                        var executing = _executingItem;
                        ActivateHeadItem(executing);
                    }
                }
            }
        }
        else if (_waiters.TryPeek(out var nextItem))
        {
            // Completed-head guard, mirroring DrainReadyWaiters' D-arm: a drain-only head is
            // picked up by the DrainReadyWaiters call below.
            if (!nextItem.WaiterTask.IsCompleted)
            {
                ActivateHeadItem(nextItem.Waiter);
            }
        }
        else
        {
            Debug.Assert(false, "count > 0 with no peekable queue head.");
        }

        // Continue draining ready items, we already hold the advancer flag.
        DrainReadyWaiters();
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

    // Live-activation occupancy bit (not a lock - nothing waits on it): 1 while an item has been
    // granted the activation turn and has not yet retired. Role-neutral by design: a client policy
    // reads on the activation side (Slon's shared read promise), a server policy writes there - either
    // way the policy owns ONE shared per-connection resource that exactly one live activation may
    // hold. Read as a decision input under _activationLock by the two independent activation paths
    // (the C-path activating _executingItem, and CommitWaiter's wasEmpty self-activate of a committed
    // head), which could otherwise both grant the turn to consecutive items. Set on every activation;
    // cleared by the retiring side (plain write ordered by the retirement path's own sequencing).
    bool _liveActivation;

    // Deferral gate for a completion callback firing between the commit's pre-publication
    // registration and its publish (see CommitWaiter / OnWaiterTaskCompleted). Armed by the
    // committing thread, CAS-claimed by an early fire, drained by the commit's post-publish
    // replay. Commits are serialized on the executor's logical thread, so one word suffices.
    const int CommitFireIdle = 0;
    const int CommitFireArmed = 1;
    const int CommitFireDeferred = 2;
    int _commitFireGate;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ActivateHeadItem(T item, bool preferAsync = true)
    {
        DrainTrace.RecordItem(DrainTrace.Kind.Act, item);
        // Update the publicly-observed ActivatedItem slot before dispatching to the policy:
        // a same-thread inline activation sees the new value via the policy call's own
        // sequencing, and a TP-dispatched activation publishes via the dispatch fence.
        SetActivatedItem(item);
        Volatile.Write(ref _liveActivation, true); // grant the activation turn (cleared at retirement)
        _policy.ActivateHeadItem(item, preferAsync);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void ClearExecutingItem(bool wasActivated)
    {
        // Single writer (the executor), so no RMW needed; the Volatile.Write carries the clear to
        // the enumerator's cross-thread read. Cleared in both paths so the enumerator stops yielding
        // the item once it's done (or transitioning to another channel like a tail-waiter).
        Volatile.Write(ref _hasInFlightItem, false);

        if (!wasActivated)
        {
            // Claim under the lock: replaces the lock-free Exchange plus the lost-race empty
            // fence-acquire - entering the lock means any advancer C-path ActivateHeadItem
            // finished, closing the activation-after-completion race before the caller proceeds
            // to CompleteWaiter. Executor strand, so a plain in-lock read is exact (the only
            // lock-free publisher is this strand itself). wasActivated=false means the
            // deferred-publish branch ran, which only triggers when a waiter was committed, so
            // _activationLock is non-null here.
            bool won;
            lock (_activationLock!)
            {
                won = _executingItemActivationPending;
                _executingItemActivationPending = false;
            }
            if (won)
            {
                // Won the race-back: advancer won't activate. Item is done (callers invoke us after
                // pipelineTask.IsCompleted), so activation is optional, skip it.
                SetExecutingItem(default!);
            }
            // Lost: the advancer activated it (finished before our lock entry); _executingItem
            // stays populated until the next dispatch overwrites or the commit reclaim clears it.
        }
        else
        {
            // Inline-activated path: no advancer is tracking, so we can drop the item reference
            // along with the visibility flag.
            SetExecutingItem(default!);
        }
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    static void ThrowCompleted() => throw new InvalidOperationException("The pipeline has been completed.");


    public struct Enumerator
    {
        readonly Pipeline<T, TPolicy, TSource, TEnumerator> _pipeline;
        SingleProducerSingleConsumerQueue<(T Waiter, ValueTask WaiterTask)>.Enumerator _waitersEnumerator;
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
                    // Null queue means the pipeline never escalated. Skip to the slot phase.
                    var queue = _pipeline._waiters.Queue;
                    if (queue is null)
                    {
                        _phase = 3;
                        goto case 3;
                    }
                    _waitersEnumerator = new(queue);
                    _phase = 2;
                    goto case 2;
                case 2:
                    while (_waitersEnumerator.MoveNext())
                    {
                        if (_waitersEnumerator.Current.Waiter is { } waiter)
                        {
                            Current = waiter;
                            return true;
                        }
                    }
                    _phase = 3;
                    goto case 3;
                case 3:
                    _phase = 4;
                    if (_pipeline._waiters.TrySnapshotSlot(out var slotItem) && slotItem is { } slot)
                    {
                        Current = slot;
                        return true;
                    }
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
    /// The shell allocation (waiter queue, delegates, depth/latch machinery) gets reused while
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
