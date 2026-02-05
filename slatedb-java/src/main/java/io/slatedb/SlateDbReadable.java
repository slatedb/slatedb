package io.slatedb;

import io.slatedb.SlateDbConfig.ReadOptions;
import io.slatedb.SlateDbConfig.ScanOptions;

/// Shared read-only interface for SlateDb and SlateDbReader.
///
/// Enables passing either type to read paths without additional wrappers.
public interface SlateDbReadable extends AutoCloseable {
    /// Reads a value using default read options.
    ///
    /// @param key key to read.
    /// @return The value for the key, or `null` if the key does not exist.
    byte[] get(byte[] key);

    /// Reads a value with custom read options.
    ///
    /// @param key key to read.
    /// @param options read options or `null` for defaults.
    /// @return The value for the key, or `null` if the key does not exist.
    byte[] get(byte[] key, ReadOptions options);

    /// Creates a scan iterator over the range `[startKey, endKey)` using default scan options.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @return A [SlateDbScanIterator]. Always close it.
    SlateDbScanIterator scan(byte[] startKey, byte[] endKey);

    /// Creates a scan iterator over the range `[startKey, endKey)` using custom scan options.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator]. Always close it.
    SlateDbScanIterator scan(byte[] startKey, byte[] endKey, ScanOptions options);

    /// Creates a scan iterator for the provided key prefix using default scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @return A [SlateDbScanIterator]. Always close it.
    SlateDbScanIterator scanPrefix(byte[] prefix);

    /// Creates a scan iterator for the provided key prefix using custom scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator]. Always close it.
    SlateDbScanIterator scanPrefix(byte[] prefix, ScanOptions options);

    /// Closes the underlying handle.
    ///
    /// This method is idempotent.
    @Override
    void close();
}
