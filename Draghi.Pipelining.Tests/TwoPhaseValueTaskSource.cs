using System.Threading.Tasks.Sources;

namespace Draghi.Pipelining.Tests;

/// A waiter-task source whose completion is deterministically TWO-PHASE, mirroring the production
/// MRVTSC-family cores: SetResult publishes the completed flag (phase 1 - from this instant
/// GetStatus reports Succeeded and the advancer's done-arm will claim the head), then reads the
/// registered (continuation, state) pair and invokes it (phase 2 - unlicensed, on the completer's
/// thread). The window hooks park the completer between phase 1 and phase 2's state read, making
/// the retire-vs-dispatch race deterministic with no pipeline-side hooks: the pipeline's own
/// RegisterAdvanceFire performs the real BCL ValueTaskAwaiter registration on this source, and the
/// real Advance performs the racing GetResult.
///
/// GetResult consumes AND resets the tenure (nulls the pair), as the production promise does on
/// consume - a GetResult landing inside the window is the tear: the completer's phase-2 state read
/// then yields null, and the BCL known callback (s_invokeActionDelegate) throws the production
/// "unexpected state object" ArgumentOutOfRangeException.
sealed class TwoPhaseValueTaskSource : IValueTaskSource
{
    Action<object?>? _continuation;
    object? _state;
    bool _completed;
    readonly ManualResetEventSlim _registered = new();

    /// Set by the completer once phase 1 is published and its continuation read has happened -
    /// phase 2's state read has NOT. The head is claimable while this is set.
    public ManualResetEventSlim WindowReached { get; } = new();

    /// Released by the test to let the completer proceed to its state read + dispatch.
    public ManualResetEventSlim WindowRelease { get; } = new();

    public bool WaitForRegistration(TimeSpan timeout) => _registered.Wait(timeout);

    public ValueTaskSourceStatus GetStatus(short token)
        => Volatile.Read(ref _completed) ? ValueTaskSourceStatus.Succeeded : ValueTaskSourceStatus.Pending;

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        // State before the continuation CAS, the production publication order.
        _state = state;
        if (Interlocked.CompareExchange(ref _continuation, continuation, null) is not null)
            throw new InvalidOperationException("Single registration only - this source models one tenure.");
        _registered.Set();
    }

    /// Phase 1: publish completed. Phase 2: read the pair and dispatch - parked at the window in
    /// between. Blocks until WindowRelease; call from a thread the test owns.
    public void SetResult()
    {
        Volatile.Write(ref _completed, true);
        var continuation = Volatile.Read(ref _continuation);
        if (continuation is null)
            return;
        WindowReached.Set();
        WindowRelease.Wait();
        // Phase 2's state read. If the done-arm retire consumed the tenure inside the window,
        // this is null and the BCL callback throws the production AOORE - propagated to the
        // SetResult caller, exactly like the completer's stack in the live capture.
        var state = _state;
        continuation(state);
    }

    public void GetResult(short token)
    {
        if (!Volatile.Read(ref _completed))
            throw new InvalidOperationException("GetResult before completion.");
        // Consume-and-reset, as the production promise does: the tenure's pair is gone the
        // instant a consumer claims the result.
        _continuation = null;
        _state = null;
    }
}
