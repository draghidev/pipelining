using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Draghi.Pipelining;

public abstract class PipelineScheduler
{
    static readonly Action<object?> ThreadPoolWorkItemAction = static item => ((IThreadPoolWorkItem)item!).Execute();

    /// <summary>Default scheduler that dispatches work via <see cref="System.Threading.ThreadPool"/>.</summary>
    public static PipelineScheduler ThreadPool { get; } = new ThreadPoolScheduler();

    /// <summary>
    /// Submits work, capturing the caller's ExecutionContext and restoring it before invoking the callback.
    /// </summary>
    /// <remarks>
    /// Default implementation captures EC and routes through <see cref="SubmitDetached(Action{object?}, object?, bool)"/>.
    /// Subclasses may override to use a scheduler-native EC-capture path (e.g. ThreadPool.QueueUserWorkItem).
    /// </remarks>
    public virtual void Submit(Action<object?> action, object? state, bool preferLocal = true)
    {
        var ec = ExecutionContext.Capture();
        if (ec is null)
        {
            SubmitDetached(action, state, preferLocal);
            return;
        }
        SubmitDetached(WorkState<object?>.InvokeAction, new WorkState<object?>(action, state, ec), preferLocal);
    }

    public void Submit<TState>(Action<TState> action, TState state, bool preferLocal = true)
    {
        ArgumentNullException.ThrowIfNull(action);
        SubmitDetached(WorkState<TState>.InvokeAction, new WorkState<TState>(action, state, ExecutionContext.Capture()), preferLocal);
    }

    /// <summary>
    /// Submits work without capturing ExecutionContext.
    /// The callback runs detached from the caller's logical execution stream, AsyncLocal values,
    /// security context, and culture from the caller will not flow to the callback.
    /// </summary>
    /// <remarks>
    /// Must not throw, like System.IO.Pipelines' PipeScheduler.Schedule: it is a thin fire-and-forget
    /// dispatch primitive called from detached dispatch/retirement work-item contexts that have no
    /// exception channel, so a throw crashes that thread. (Unlike <see cref="TaskScheduler"/>'s QueueTask,
    /// which the runtime hands a Task to fault, there is nowhere to surface a submit failure.)
    /// Implementations must contain any submit-time failure internally; a throwing scheduler is a caller
    /// contract violation, and the resulting connection breakage is the caller's responsibility.
    /// </remarks>
    public abstract void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true);

    public void SubmitDetached<TState>(Action<TState> action, TState state, bool preferLocal = true)
    {
        ArgumentNullException.ThrowIfNull(action);
        SubmitDetached(WorkState<TState>.InvokeAction, new WorkState<TState>(action, state, capturedContext: null), preferLocal);
    }

    public virtual void SubmitDetached(IThreadPoolWorkItem callBack, bool preferLocal = true)
        => SubmitDetached(ThreadPoolWorkItemAction, callBack, preferLocal);

    // Yield is actually a strange name for async programming, as the yielding part is immediate.
    // Doing `await Task.Yield()` you're not awaiting the yield but the resume/continue.
    // Continue also avoids the naming issues that arise for Yield being called on arbitrary (non-current) schedulers.
    // Related discussion https://github.com/dotnet/runtime/issues/20025 (TaskScheduler.SwitchTo)
    // Method has the Async suffix as this is not defined on a type limited to Task functionality.
    // Finally we return an awaitable like Task.Yield to avoid ConfigureAwait being available (or flagged by analyzers).
    internal ContinueOnAwaitable ContinueOnAsync(bool forceYielding = false, bool preferLocal = true)
        => new(this, forceYielding, preferLocal);

    sealed class WorkState<TState>(Action<TState> action, TState state, ExecutionContext? capturedContext)
    {
        readonly Action<TState> _action = action;
        readonly TState _state = state;
        readonly ExecutionContext? _capturedContext = capturedContext;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Execute()
        {
            if (_capturedContext is null)
            {
                _action(_state);
                return;
            }

            var outerContext = ExecutionContext.Capture();
            try
            {
                ExecutionContext.Restore(_capturedContext);
                _action(_state);
            }
            finally
            {
                if (outerContext is not null)
                    ExecutionContext.Restore(outerContext);
            }
        }

        /// <summary>
        /// Trampoline for routing through <see cref="PipelineScheduler.SubmitDetached(Action{object?}, object?, bool)"/>.
        /// Uniform cost across all schedulers (1 indirect + 1 cast + Execute as non-virtual call on sealed type).
        /// Direct callers of <see cref="PipelineScheduler.SubmitDetached(IThreadPoolWorkItem, bool)"/> use the IThreadPoolWorkItem path instead.
        /// </summary>
        public static Action<object?> InvokeAction { get; } =
            [StackTraceHidden] static (state) => ((WorkState<TState>)state!).Execute();
    }

    sealed class ThreadPoolScheduler : PipelineScheduler
    {
        public override void Submit(Action<object?> action, object? state, bool preferLocal = true)
            => System.Threading.ThreadPool.QueueUserWorkItem(action, state, preferLocal);

        public override void SubmitDetached(Action<object?> action, object? state, bool preferLocal = true)
            => System.Threading.ThreadPool.UnsafeQueueUserWorkItem(action, state, preferLocal);

        public override void SubmitDetached(IThreadPoolWorkItem callBack, bool preferLocal = true)
            => System.Threading.ThreadPool.UnsafeQueueUserWorkItem(callBack, preferLocal);
    }
}

readonly struct ContinueOnAwaitable
{
    readonly PipelineScheduler _scheduler;
    readonly bool _forceYielding;
    readonly bool _preferLocal;

    internal ContinueOnAwaitable(PipelineScheduler scheduler, bool forceYielding, bool preferLocal)
    {
        ArgumentNullException.ThrowIfNull(scheduler);
        _scheduler = scheduler;
        _forceYielding = forceYielding;
        _preferLocal = preferLocal;
    }

    public ContinueOnAwaiter GetAwaiter() => new(this);

    public readonly struct ContinueOnAwaiter : ICriticalNotifyCompletion
    {
        readonly ContinueOnAwaitable _awaitable;
        internal ContinueOnAwaiter(ContinueOnAwaitable awaitable) => _awaitable = awaitable;

        public bool IsCompleted => false;
        public void GetResult() { }
        public void OnCompleted(Action continuation)
            => _awaitable._scheduler.Submit(static state => ((Action)state!)(), (object)continuation, _awaitable._preferLocal);

        public void UnsafeOnCompleted(Action continuation)
            => _awaitable._scheduler.SubmitDetached(static state => ((Action)state!)(), (object)continuation, _awaitable._preferLocal);
    }
}
