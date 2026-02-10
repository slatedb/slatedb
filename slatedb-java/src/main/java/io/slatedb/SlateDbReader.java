package io.slatedb;

import io.slatedb.SlateDbConfig.ReadOptions;
import io.slatedb.SlateDbConfig.ScanOptions;

import java.lang.foreign.MemorySegment;

/// Read-only SlateDB handle.
///
/// Readers provide a stable, read-only view of the database and should be closed when done.
public final class SlateDbReader implements SlateDbReadable {
    private MemorySegment handle;
    private boolean closed;

    SlateDbReader(MemorySegment handle) {
        this.handle = handle;
    }

    /// Reads a value using default read options.
    ///
    /// @param key key to read.
    /// @return The value for the key, or `null` if the key does not exist.
    public byte[] get(byte[] key) {
        return get(key, null);
    }

    /// Reads a value with custom read options.
    ///
    /// @param key key to read.
    /// @param options read options or `null` for defaults.
    /// @return The value for the key, or `null` if the key does not exist.
    public byte[] get(byte[] key, ReadOptions options) {
        return Native.readerGet(handle, key, options);
    }

    /// Creates a scan iterator over the range `[startKey, endKey)` using default scan options.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @return A [SlateDbScanIterator]. Always close it.
    public SlateDbScanIterator scan(byte[] startKey, byte[] endKey) {
        return scan(startKey, endKey, null);
    }

    /// Creates a scan iterator over the range `[startKey, endKey)` using custom scan options.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator]. Always close it.
    public SlateDbScanIterator scan(byte[] startKey, byte[] endKey, ScanOptions options) {
        return new SlateDbScanIterator(Native.readerScan(handle, startKey, endKey, options));
    }

    /// Creates a scan iterator for the provided key prefix using default scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @return A [SlateDbScanIterator]. Always close it.
    public SlateDbScanIterator scanPrefix(byte[] prefix) {
        return scanPrefix(prefix, null);
    }

    /// Creates a scan iterator for the provided key prefix using custom scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator]. Always close it.
    public SlateDbScanIterator scanPrefix(byte[] prefix, ScanOptions options) {
        return new SlateDbScanIterator(Native.readerScanPrefix(handle, prefix, options));
    }

    /// Closes the reader handle.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        Native.readerClose(handle);
        handle = MemorySegment.NULL;
        closed = true;
    }

}
