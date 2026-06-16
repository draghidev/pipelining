using System.Runtime.InteropServices;

namespace Draghi.Pipelining.Tests;

// TODO remove after https://github.com/microsoft/testfx/issues/9183
readonly struct MstestWhenAllWorkaround : IDisposable
{
    readonly GCHandle _handle;

    MstestWhenAllWorkaround(GCHandle handle) => _handle = handle;

    public static MstestWhenAllWorkaround Pin(object value) => new(GCHandle.Alloc(value));

    public void Dispose() => _handle.Free();
}
