package io.slatedb;

import io.slatedb.SlateDb.PutOptions;

import java.lang.foreign.MemorySegment;

/// A batch of write operations that can be executed atomically.
///
/// Batches are consumed by [SlateDb#write(WriteBatch)] and cannot be reused.
public final class WriteBatch implements AutoCloseable {
    private MemorySegment batchPtr;
    private boolean closed;
    private boolean consumed;

    WriteBatch(MemorySegment batchPtr) {
        this.batchPtr = batchPtr;
    }

    /// Adds a key/value pair to the batch using default put options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    public void put(byte[] key, byte[] value) {
        ensureOpen();
        Native.writeBatchPut(batchPtr, key, value);
    }

    /// Adds a key/value pair to the batch using custom put options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    /// @param options put options or `null` for defaults.
    public void put(byte[] key, byte[] value, PutOptions options) {
        ensureOpen();
        Native.writeBatchPutWithOptions(batchPtr, key, value, options);
    }

    /// Adds a delete operation to the batch.
    ///
    /// @param key key to delete.
    public void delete(byte[] key) {
        ensureOpen();
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

    boolean isClosed() {
        return closed;
    }

    boolean isConsumed() {
        return consumed;
    }

    void markConsumed() {
        consumed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("WriteBatch is closed");
        }
        if (consumed) {
            throw new IllegalStateException("WriteBatch already consumed");
        }
    }
}
