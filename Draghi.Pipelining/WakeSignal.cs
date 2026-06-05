using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining.Internal;

/// <summary>
/// Single-consumer single-producer wake signal for suspending and waking the source's enumerator.
/// Doubles as its own awaitable and awaiter (GetAwaiter returns this).
/// </summary>
/// <remarks>
/// The wake lock is held from WaitUnsynchronized through OnCompleted, which stores the continuation
/// and releases the lock. Signal re-acquires the lock to claim the continuation.
/// A spinlock is used instead of Lock because the critical section is a few field reads/writes
/// with at most two threads contending (enumerator and Signal caller).
/// No code under the lock re-enters or is user code that may, so reentrancy support is not needed.
/// <para>
/// Exposed under <see cref="Draghi.Pipelining.Internal"/> as a building block for callers
/// composing their own <see cref="IPipelineSource{T,TEnumerator}"/> implementations.
/// </para>
/// </remarks>
[Experimental("DRAGHI001")]
public sealed class WakeSignal(bool runContinuationsAsynchronously, PipelineScheduler executionScheduler)
    : IThreadPoolWorkItem
{
    readonly CancellationTokenSource _cts = new();
    Action? _continuation;
    bool _pending;
    int _wakeLock;

    public PipelineScheduler Scheduler { get; } = executionScheduler;
    public CancellationToken CompletionToken => _cts.Token;
    public bool IsCompleted => _cts.IsCancellationRequested;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AcquireWakeLock()
    {
        if (Interlocked.Exchange(ref _wakeLock, 1) != 0)
            AcquireWakeLockSlow();

        [MethodImpl(MethodImplOptions.NoInlining)]
        void AcquireWakeLockSlow()
        {
            var spinner = new SpinWait();
            while (Interlocked.Exchange(ref _wakeLock, 1) != 0)
                spinner.SpinOnce();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseWakeLock() => Volatile.Write(ref _wakeLock, 0);

    /// <summary>
    /// Prepares the signal for a new wait. Must be called under the wake lock.
    /// Returns an awaiter that holds the lock through OnCompleted.
    /// </summary>
    public Awaiter WaitUnsynchronized()
    {
        Debug.Assert(!_pending, "Concurrent wait calls.");
        _pending = true;
        return new(this);
    }

    public bool Signal() => SignalCore(runContinuationsAsynchronously);

    /// <summary>
    /// Signal with an explicit dispatch mode, overriding the construction-time default. When
    /// <paramref name="runContinuationsAsynchronously"/> is false the consumer's continuation runs
    /// inline on the calling thread. The call site effectively takes over the executor for one
    /// turn. Use when a single producer needs to choose dispatch per-item (e.g. routing sync items
    /// inline while async items dispatch via the scheduler).
    /// </summary>
    /// <returns><c>true</c> if a parked continuation was claimed and dispatched, <c>false</c> if no
    /// one was waiting (the signal was a no-op).</returns>
    public bool Signal(bool runContinuationsAsynchronously) => SignalCore(runContinuationsAsynchronously);

    bool SignalCore(bool runContinuationsAsynchronously)
    {
        AcquireWakeLock();
        try
        {
            if (!_pending)
                return false;
            _pending = false;
        }
        finally
        {
            ReleaseWakeLock();
        }

        if (runContinuationsAsynchronously)
            Scheduler.SubmitDetached(this, preferLocal: true);
        else
            _continuation!();
        return true;
    }

    void IThreadPoolWorkItem.Execute() => _continuation!();

    /// <summary>Marks the source as completed, wakes any pending wait.</summary>
    public void Complete()
    {
        _cts.Cancel();
        SignalCore(runContinuationsAsynchronously: true);
    }

    public readonly struct Awaiter(WakeSignal signal) : ICriticalNotifyCompletion
    {
        public Awaiter GetAwaiter() => this;

        public bool IsCompleted => false;

        public bool GetResult() => !signal.IsCompleted;

        public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

        public void UnsafeOnCompleted(Action continuation)
        {
            if (!ReferenceEquals(signal._continuation, continuation))
                signal._continuation = continuation;
            signal.ReleaseWakeLock();

            if (signal.IsCompleted)
                signal.SignalCore(runContinuationsAsynchronously: true);
        }
    }
}
