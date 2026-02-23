package io.slatedb;

import java.util.Optional;

/// A single WAL log entry.
public record SlateDbRowEntry(
    SlateDbConfig.RowEntryKind kind,
    byte[] key,
    byte[] value,
    long seq,
    Optional<Long> createTs,
    Optional<Long> expireTs
) {}
