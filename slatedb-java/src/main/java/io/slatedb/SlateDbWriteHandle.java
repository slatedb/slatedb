package io.slatedb;

import java.util.Objects;

/// Handle returned by SlateDB write operations.
public class SlateDbWriteHandle {
    private final NativeInterop.WriteHandleHandle handle;

    SlateDbWriteHandle(NativeInterop.WriteHandleHandle handle) {
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    /// @return The sequence number of the write operation.
    public long seq() {
        return handle.seq();
    }

    /// @return The creation timestamp of the write operation.
    public long createTs() {
        return handle.createTs();
    }
}
