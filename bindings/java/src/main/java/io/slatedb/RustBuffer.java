package io.slatedb;


import com.sun.jna.Structure;
import com.sun.jna.Pointer;

/**
 * This is a helper for safely working with byte buffers returned from the Rust code.
 * A rust-owned buffer is represented by its capacity, its current length, and a
 * pointer to the underlying data.
 */
@Structure.FieldOrder({ "capacity", "len", "data" })
public class RustBuffer extends Structure {
    public long capacity;
    public long len;
    public Pointer data;

    public static class ByValue extends RustBuffer implements Structure.ByValue {}
    public static class ByReference extends RustBuffer implements Structure.ByReference {}

    void setValue(RustBuffer other) {
        this.capacity = other.capacity;
        this.len = other.len;
        this.data = other.data;
    }

    public static RustBuffer.ByValue alloc(long size) {
        RustBuffer.ByValue buffer = UniffiHelpers.uniffiRustCall((UniffiRustCallStatus status) -> {
            return (RustBuffer.ByValue) UniffiLib.INSTANCE.ffi_slatedb_ffi_rustbuffer_alloc(size, status);
        });
        if (buffer.data == null) {
            throw new RuntimeException("RustBuffer.alloc() returned null data pointer (size=" + size + ")");
        }
        return buffer;
    }

    public static void free(RustBuffer.ByValue buffer) {
        UniffiHelpers.uniffiRustCall((status) -> {
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rustbuffer_free(buffer, status);
            return null;
        });
    }

    public java.nio.ByteBuffer asByteBuffer() {
        if (this.data != null) {
            java.nio.ByteBuffer byteBuffer = this.data.getByteBuffer(0, this.len);
            byteBuffer.order(java.nio.ByteOrder.BIG_ENDIAN);
            return byteBuffer;
        }
        return null;
    }
}

