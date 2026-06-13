using Microsoft.VisualStudio.TestTools.UnitTesting;

// Method-level parallelism by default. Classes that can't tolerate it (TP-pressure-sensitive
// concurrency stress, latch/store contention harnesses that saturate cores) opt out with
// [DoNotParallelize].
[assembly: Parallelize(Workers = 0, Scope = ExecutionScope.MethodLevel)]
