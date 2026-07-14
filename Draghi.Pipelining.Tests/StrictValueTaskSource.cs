using System.Threading.Tasks.Sources;

namespace Draghi.Pipelining.Tests;

/// Thrown by <see cref="StrictValueTaskSource{T}"/> when the consumer violates one of the two
/// production waiter-task contracts. Carrying a dedicated type keeps the fault a conviction: the
/// message names the contract and the offending member, so a red run reads as "the protocol broke
/// contract X here" rather than a generic MRVTSC token-mismatch strand.
sealed class StrictValueTaskSourceViolationException(string message) : InvalidOperationException(message);

/// An <see cref="IValueTaskSource"/>/<see cref="IValueTaskSource{T}"/> backed by
/// <see cref="ManualResetValueTaskSourceCore{T}"/> that enforces, with loud named diagnostics, the
/// two task-layer contracts the production waiter tasks hold but that a TCS-backed test task hides:
///   1. SINGLE REGISTRATION - at most one OnCompleted per token (a ManualResetValueTaskSourceCore
///      throws on a second continuation; a Task silently allows N).
///   2. NO CALL AFTER GetResult - once GetResult runs for a token the source is considered consumed
///      for that token (production resets it for the next tenure); any later member call on the old
///      ValueTask (GetStatus/OnCompleted/GetResult) is token-mismatch UB. A Task tolerates all of it.
/// The raw core already throws on both, but with generic messages that read as strands; this wrapper
/// converts each into a named, test-failure-readable conviction. All tracking is Interlocked so the
/// diagnostics hold when the completion fires on a different thread than the registration.
sealed class StrictValueTaskSource<T> : IValueTaskSource, IValueTaskSource<T>
{
    ManualResetValueTaskSourceCore<T> _core;

    // Token that has already been handed an OnCompleted continuation. A second OnCompleted for the
    // same token is the single-registration violation.
    int _registeredForToken = -1;
    // Token whose GetResult has run. Any subsequent member call for that token is a post-consume
    // violation.
    int _consumedToken = -1;
    // First-writer-wins settle guard: the fixture's CompletePipelineTask and the shutdown-escalation
    // TryCancelPipelineTask race to settle, exactly as the two TCS TrySet callers do today. The raw
    // core throws on a double SetResult/SetException, so gate it here.
    int _settled;

    public short Version => _core.Version;

    // Settled state, for test-side observation only (PipelineTaskCompleted). Reads the
    // TrySet-guard field directly, never GetStatus/OnCompleted/GetResult, so it carries none of
    // the token-tenure risk those members have - safe to call at any time, including after
    // GetResult has consumed the token.
    public bool IsSettled => Volatile.Read(ref _settled) != 0;

    public bool TrySetResult(T result)
    {
        if (Interlocked.CompareExchange(ref _settled, 1, 0) != 0)
            return false;
        _core.SetResult(result);
        return true;
    }

    public bool TrySetException(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _settled, 1, 0) != 0)
            return false;
        _core.SetException(exception);
        return true;
    }

    void ThrowIfConsumed(short token, string member)
    {
        if (Volatile.Read(ref _consumedToken) == token)
            throw new StrictValueTaskSourceViolationException(
                $"NO CALL AFTER GetResult: '{member}' was invoked on a StrictValueTaskSource for token " +
                $"{token} after GetResult already consumed it. GetResult resets the source for the next " +
                $"tenure, so any further call on the old ValueTask is token-mismatch UB (a Task hides this).");
    }

    public ValueTaskSourceStatus GetStatus(short token)
    {
        ThrowIfConsumed(token, nameof(GetStatus));
        return _core.GetStatus(token);
    }

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        ThrowIfConsumed(token, nameof(OnCompleted));
        // Interlocked.Exchange returning the same token means a continuation was already registered
        // for this tenure - the single-registration violation. The raw core would also throw on the
        // OnCompleted below, but with a generic message; this fires first with the named contract.
        if (Interlocked.Exchange(ref _registeredForToken, token) == token)
            throw new StrictValueTaskSourceViolationException(
                $"SINGLE REGISTRATION: a second OnCompleted was registered for token {token} on a " +
                $"StrictValueTaskSource. A ManualResetValueTaskSourceCore permits at most one continuation " +
                $"per tenure (a second registration throws); a Task silently accepts N and hides the bug.");
        _core.OnCompleted(continuation, state, token, flags);
    }

    void IValueTaskSource.GetResult(short token)
    {
        ThrowIfConsumed(token, nameof(IValueTaskSource.GetResult));
        try { _core.GetResult(token); }
        finally { Volatile.Write(ref _consumedToken, token); }
    }

    T IValueTaskSource<T>.GetResult(short token)
    {
        ThrowIfConsumed(token, nameof(IValueTaskSource<T>.GetResult));
        try { return _core.GetResult(token); }
        finally { Volatile.Write(ref _consumedToken, token); }
    }
}
