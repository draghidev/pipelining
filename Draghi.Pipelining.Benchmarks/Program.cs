using System.Runtime.CompilerServices;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

[module: SkipLocalsInit]

namespace Draghi.Pipelining.Benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
#if DEBUG
var config = new DebugInProcessConfig();
#else
        var config = DefaultConfig.Instance;
#endif
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
