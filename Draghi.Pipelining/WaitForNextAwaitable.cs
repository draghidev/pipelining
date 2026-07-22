using System.Diagnostics;
using System.Runtime.CompilerServices;
using Draghi.Pipelining.Internal;

namespace Draghi.Pipelining;

/// <summary>
/// Awaitable returned by <see cref="IPipelineEnumerator{T}.WaitForNextAsync"/>. Awaiting it yields
/// <c>true</c> when the executor should retry <see cref="IPipelineEnumerator{T}.TryGetNext"/>
/// (an item arrived or may have arrived) and <c>false</c> when the enumeration completed.
/// </summary>
/// <remarks>
/// A concrete union keeps the source contract allocation-free while supporting three wait forms:
/// <list type="bullet">
/// <item><b>Immediate:</b> retry or completion was observed without suspension.</item>
/// <item><b>Signal:</b> a <see cref="SourceWakeEvent"/> owns the wait. Its atomic empty check
/// remains active through continuation registration, closing the miss-and-arm race.</item>
/// <item><b>Task:</b> a <see cref="ValueTask{TResult}"/> backs sources with their own asynchronous
/// wait work.</item>
/// </list>
/// Each instance has one consumer and may be awaited once.
/// </remarks>
public readonly struct WaitForNextAwaitable
{
    enum Kind : byte
    {
        Immediate,
        Signal,
        Task,
    }

    readonly SourceWakeEvent? _signal;
    readonly ValueTask<bool> _wait;
    readonly bool _retry;
    readonly Kind _kind;

    internal WaitForNextAwaitable(SourceWakeEvent signal)
    {
        _signal = signal;
        _kind = Kind.Signal;
    }

    WaitForNextAwaitable(bool retry)
    {
        _retry = retry;
        _kind = Kind.Immediate;
    }

    WaitForNextAwaitable(ValueTask<bool> task)
    {
        _wait = task;
        _kind = Kind.Task;
    }

    /// <summary>Synchronously-resolved wait: retry the pull now.</summary>
    public static WaitForNextAwaitable Retry => new(retry: true);

    /// <summary>Synchronously-resolved wait: the enumeration completed.</summary>
    public static WaitForNextAwaitable Completed => new(retry: false);

    /// <summary>Wraps a wait whose result is true to retry or false when completed.</summary>
    public static WaitForNextAwaitable FromTask(ValueTask<bool> task) => new(task);

    public Awaiter GetAwaiter() => new(this);

    public struct Awaiter : ICriticalNotifyCompletion
    {
        readonly WaitForNextAwaitable _wait;
        ConfiguredValueTaskAwaitable<bool>.ConfiguredValueTaskAwaiter _taskAwaiter;

        internal Awaiter(WaitForNextAwaitable wait)
        {
            _wait = wait;
            if (wait._kind == Kind.Task)
                _taskAwaiter = wait._wait.ConfigureAwait(false).GetAwaiter();
        }

        public bool IsCompleted => _wait._kind switch
        {
            Kind.Immediate => true,
            Kind.Signal => false,
            _ => _taskAwaiter.IsCompleted,
        };

        public bool GetResult() => _wait._kind switch
        {
            Kind.Immediate => _wait._retry,
            // A signal always retries; source completion is observed by the next pull or wait.
            Kind.Signal => true,
            _ => _taskAwaiter.GetResult(),
        };

        public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

        public void UnsafeOnCompleted(Action continuation)
        {
            switch (_wait._kind)
            {
                case Kind.Signal:
                    _wait._signal!.WaitOnCompleted(continuation);
                    break;
                case Kind.Task:
                    _taskAwaiter.UnsafeOnCompleted(continuation);
                    break;
                default:
                    Debug.Fail("Immediate waits complete synchronously.");
                    break;
            }
        }
    }
}
