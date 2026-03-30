using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Draghi.Pipelining.Benchmarks;

/// <summary>
/// Simulates the pipeline pattern using two channels:
/// - Enqueue channel: producer → executor
/// - Activation channel: executor → item (head notification)
/// Measures the baseline cost of the "double channel" approach.
/// </summary>
[IterationCount(15)]
[MemoryDiagnoser]
public class NaivePipelineBenchmarks
{
    Channel<ChannelItem> _enqueueChannel = null!;
    Channel<ChannelItem> _activationChannel = null!;
    Task _executorTask = null!;
    Task _activationTask = null!;
    ChannelItem _item = null!;

    [GlobalSetup]
    public void Setup()
    {
        _enqueueChannel = Channel.CreateUnbounded<ChannelItem>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true });
        _activationChannel = Channel.CreateUnbounded<ChannelItem>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = true });
        _item = new ChannelItem();

        // Executor loop: reads from enqueue channel, executes the item,
        // enqueues for activation, then awaits trailing execution before processing the next item.
        _executorTask = Task.Run(async () =>
        {
            await foreach (var item in _enqueueChannel.Reader.ReadAllAsync())
            {
                // Activate: signal the item it's at the head before execution.
                _activationChannel.Writer.TryWrite(item);

                // Execute: produces trailing execution work and a pipeline task.
                var trailingTask = item.Execute();

                // Trailing execution must complete before the next item can execute.
                await trailingTask;
            }
        });

        // Activation loop: reads from activation channel, waits for the item's
        // pipeline phase to complete, then completes the item.
        _activationTask = Task.Run(async () =>
        {
            await foreach (var item in _activationChannel.Reader.ReadAllAsync())
            {
                // Activate: tell the item it's at the head.
                item.Activate();
                // Wait for the item's pipeline phase (e.g. response reading).
                await item.WaitForPipelineTaskAsync();
                item.SignalComplete();
            }
        });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _enqueueChannel.Writer.Complete();
        _executorTask.GetAwaiter().GetResult();
        _activationChannel.Writer.Complete();
        _activationTask.GetAwaiter().GetResult();
    }

    /// <summary>
    /// Simple: no trailing task, pipeline phase completes immediately.
    /// Enqueue → execute → activate → complete.
    /// </summary>
    [Benchmark]
    public void EnqueueExecuteComplete()
    {
        var item = _item;
        item.Reset();
        _enqueueChannel.Writer.TryWrite(item);
        item.Wait();
    }

    /// <summary>
    /// With trailing execution: execute produces a trailing task that completes synchronously.
    /// Same as the pipeline's PipelineItemResult with a completed TrailingExecutionTask.
    /// </summary>
    [Benchmark]
    public void EnqueueExecuteTrailingComplete()
    {
        var item = _item;
        item.Reset(hasTrailing: true);
        _enqueueChannel.Writer.TryWrite(item);
        item.Wait();
    }
}

sealed class ChannelItem
{
    readonly ManualResetEventSlim _completed = new(false);
    bool _hasTrailing;

    public void Activate() { /* Item is now at the head. Start pipelined phase. */ }
    public void SignalComplete() => _completed.Set();
    public void Wait() => _completed.Wait();

    public void Reset(bool hasTrailing = false)
    {
        _completed.Reset();
        _hasTrailing = hasTrailing;
    }

    /// <summary>
    /// Simulates ExecuteItemAsync. Returns a trailing execution task.
    /// For the benchmark, trailing completes synchronously (like a buffered write).
    /// </summary>
    public ValueTask Execute()
    {
        return _hasTrailing ? new(Task.CompletedTask) : default;
    }

    /// <summary>
    /// Simulates waiting for the pipeline phase (e.g. response reading).
    /// For the benchmark, completes synchronously.
    /// </summary>
    public ValueTask WaitForPipelineTaskAsync() => default;
}
