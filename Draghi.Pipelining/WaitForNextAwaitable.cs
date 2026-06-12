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
/// Three shapes behind one type, so the pull seam stays a single concrete awaitable (no
/// boxing, no interface dispatch on the wait path):
/// <list type="bullet">
/// <item><b>Immediate</b> - the wait raced an arrival or completion and resolved synchronously;
/// no suspension, no signal machinery.</item>
/// <item><b>Signal</b> - the thin path: armed against a <see cref="WakeSignal"/> with the wake
/// lock HELD from the source's miss-check through the awaiter's continuation registration
/// (lock-through-OnCompleted), which is what makes the miss-then-arm race-free against a
/// producer's Signal. The continuation is stored as a bare delegate and invoked directly by
/// the wake - no value-task source, no status round-trips.</item>
/// <item><b>Task</b> - fallback for sources whose wait is intrinsically async (observation
/// hooks, flush-before-wait): wraps a <see cref="ValueTask{TResult}"/>.</item>
/// </list>
/// Single consumer, single await per instance, like any ValueTask-shaped awaitable.
/// </remarks>
public readonly struct WaitForNextAwaitable
{
    enum Kind : byte
    {
        Immediate,
        Signal,
        Task,
    }

    readonly WakeSignal? _signal;
    readonly ValueTask<bool> _task;
    readonly bool _immediate;
    readonly Kind _kind;

#pragma warning disable DRAGHI001
    internal WaitForNextAwaitable(WakeSignal signal)
    {
        _signal = signal;
        _kind = Kind.Signal;
    }
#pragma warning restore DRAGHI001

    WaitForNextAwaitable(bool immediate)
    {
        _immediate = immediate;
        _kind = Kind.Immediate;
    }

    WaitForNextAwaitable(ValueTask<bool> task)
    {
        _task = task;
        _kind = Kind.Task;
    }

    /// <summary>Synchronously-resolved wait: retry the pull now.</summary>
    public static WaitForNextAwaitable Retry() => new(immediate: true);

    /// <summary>Synchronously-resolved wait: the enumeration completed.</summary>
    public static WaitForNextAwaitable Completed() => new(immediate: false);

    /// <summary>Async-fallback wait around a task that resolves to retry (true) / completed (false).</summary>
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
                _taskAwaiter = wait._task.ConfigureAwait(false).GetAwaiter();
        }

        public bool IsCompleted => _wait._kind switch
        {
            Kind.Immediate => true,
            Kind.Signal => false,
            _ => _taskAwaiter.IsCompleted,
        };

        public bool GetResult() => _wait._kind switch
        {
            Kind.Immediate => _wait._immediate,
            // Woken by Signal (retry) or Complete (drain the source's remaining items, then its
            // next wait resolves Completed). Either way: not done until the source says so.
            Kind.Signal => true,
            _ => _taskAwaiter.GetResult(),
        };

        public void OnCompleted(Action continuation) => UnsafeOnCompleted(continuation);

        public void UnsafeOnCompleted(Action continuation)
        {
            switch (_wait._kind)
            {
                case Kind.Signal:
#pragma warning disable DRAGHI001
                    _wait._signal!.WaitOnCompleted(continuation);
#pragma warning restore DRAGHI001
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
