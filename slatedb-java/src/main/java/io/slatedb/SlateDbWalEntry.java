package io.slatedb;

/// A single WAL log entry.
public record SlateDbWalEntry(
    SlateDbConfig.WalEntryKind kind,
    byte[] key,
    byte[] value,
    long seq,
    Long createTs,
    Long expireTs
) {}
