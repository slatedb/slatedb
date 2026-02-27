package io.slatedb;

import io.slatedb.SlateDbConfig.MergeOptions;
import io.slatedb.SlateDbConfig.PutOptions;
import org.jspecify.annotations.Nullable;

/// A batch of write operations that can be executed atomically.
///
/// Batches are consumed by [SlateDb#write(SlateDbWriteBatch)] and cannot be reused.
public final class SlateDbWriteBatch implements AutoCloseable {
    private NativeInterop.WriteBatchHandle batchPtr;
    private boolean closed;
    private boolean consumed;

    SlateDbWriteBatch(NativeInterop.WriteBatchHandle batchPtr) {
        this.batchPtr = batchPtr;
    }

    /// Adds a key/value pair to the batch using default put options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    public void put(byte[] key, byte[] value) {
        NativeInterop.slatedb_write_batch_put(batchPtr, key, value);
    }

    /// Adds a key/value pair to the batch using custom put options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    /// @param options put options or `null` for defaults.
    public void put(byte[] key, byte[] value, @Nullable PutOptions options) {
        NativeInterop.slatedb_write_batch_put_with_options(batchPtr, key, value, options);
    }

    /// Adds a delete operation to the batch.
    ///
    /// @param key key to delete.
    public void delete(byte[] key) {
        NativeInterop.slatedb_write_batch_delete(batchPtr, key);
    }

    /// Adds a merge operation to the batch using default merge options.
    ///
    /// @param key key to merge.
    /// @param value merge operand value.
    public void merge(byte[] key, byte[] value) {
        NativeInterop.slatedb_write_batch_merge(batchPtr, key, value);
    }

    /// Adds a merge operation to the batch using custom merge options.
    ///
    /// @param key key to merge.
    /// @param value merge operand value.
    /// @param options merge options or `null` for defaults.
    public void merge(byte[] key, byte[] value, @Nullable MergeOptions options) {
        if (options == null) {
            NativeInterop.slatedb_write_batch_merge(batchPtr, key, value);
            return;
        }
        NativeInterop.slatedb_write_batch_merge_with_options(batchPtr, key, value, options);
    }

    /// Closes the batch and releases native resources.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        NativeInterop.slatedb_write_batch_close(batchPtr);
        closed = true;
        batchPtr = null;
    }

    NativeInterop.WriteBatchHandle handle() {
        return batchPtr;
    }

    void markConsumed() {
        consumed = true;
    }
}
