package io.slatedb;

import java.time.Instant;

/// Metadata for a WAL file.
public record SlateDbWalFileMetadata(
    Instant lastModified,
    long sizeBytes,
    String location
) {}
