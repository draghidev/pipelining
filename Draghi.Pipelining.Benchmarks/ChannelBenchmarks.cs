using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Draghi.Pipelining.Benchmarks;

/// <summary>
/// Measures the raw cost of a single Channel write+read cycle.
/// </summary>
[IterationCount(15)]
[MemoryDiagnoser]
public class ChannelBenchmarks
{
    Channel<bool> _channel = null!;
    Task _consumerTask = null!;
    readonly ManualResetEventSlim _consumed = new(false);

    [Params(true, false)]
    public bool AllowSynchronousContinuations { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _channel = Channel.CreateUnbounded<bool>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = AllowSynchronousContinuations
        });

        _consumerTask = Task.Run(async () =>
        {
            var reader = _channel.Reader;
            while (await reader.WaitToReadAsync())
            {
                while (reader.TryRead(out _))
                {
                    _consumed.Set();
                }
            }
        });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _channel.Writer.Complete();
        _consumerTask.GetAwaiter().GetResult();
    }

    [Benchmark]
    public void WriteRead()
    {
        _consumed.Reset();
        _channel.Writer.TryWrite(true);
        _consumed.Wait();
    }
}
