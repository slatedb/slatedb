package io.slatedb;

/// A single WAL log entry.
public record SlateDbRowEntry(
    SlateDbConfig.RowEntryKind kind,
    byte[] key,
    byte[] value,
    long seq,
    Long createTs,
    Long expireTs
) {}
