package io.slatedb;

import io.slatedb.SlateDbConfig.PutOptions;

import java.lang.foreign.MemorySegment;

/// A batch of write operations that can be executed atomically.
///
/// Batches are consumed by [SlateDb#write(SlateDbWriteBatch)] and cannot be reused.
public final class SlateDbWriteBatch implements AutoCloseable {
    private MemorySegment batchPtr;
    private boolean closed;
    private boolean consumed;

    SlateDbWriteBatch(MemorySegment batchPtr) {
        this.batchPtr = batchPtr;
    }

    /// Adds a key/value pair to the batch using default put options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    public void put(byte[] key, byte[] value) {
        Native.writeBatchPut(batchPtr, key, value);
    }

    /// Adds a key/value pair to the batch using custom put options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    /// @param options put options or `null` for defaults.
    public void put(byte[] key, byte[] value, PutOptions options) {
        Native.writeBatchPutWithOptions(batchPtr, key, value, options);
    }

    /// Adds a delete operation to the batch.
    ///
    /// @param key key to delete.
    public void delete(byte[] key) {
        Native.writeBatchDelete(batchPtr, key);
    }

    /// Closes the batch and releases native resources.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        Native.writeBatchClose(batchPtr);
        closed = true;
        batchPtr = MemorySegment.NULL;
    }

    MemorySegment handle() {
        return batchPtr;
    }

    void markConsumed() {
        consumed = true;
    }
}
