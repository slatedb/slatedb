package io.slatedb;

import java.time.Duration;
import java.util.Objects;

/// Configuration types for SlateDB operations.
public final class SlateDbConfig {
    private SlateDbConfig() {
    }

    /// Durability level used for reads and scans.
    public enum Durability {
        MEMORY((byte) 0),
        REMOTE((byte) 1);

        private final byte code;

        Durability(byte code) {
            this.code = code;
        }

        byte code() {
            return code;
        }
    }

    /// TTL behavior for put operations.
    public enum TtlType {
        DEFAULT((byte) 0),
        NO_EXPIRY((byte) 1),
        EXPIRE_AFTER((byte) 2);

        private final byte code;

        TtlType(byte code) {
            this.code = code;
        }

        byte code() {
            return code;
        }
    }

    /// Flush targets supported by explicit flush operations.
    public enum FlushType {
        MEMTABLE((byte) 0),
        WAL((byte) 1);

        private final byte code;

        FlushType(byte code) {
            this.code = code;
        }

        byte code() {
            return code;
        }
    }

    /// Supported SST block sizes for the builder.
    public enum SstBlockSize {
        KIB_1((byte) 1, 1024),
        KIB_2((byte) 2, 2048),
        KIB_4((byte) 3, 4096),
        KIB_8((byte) 4, 8192),
        KIB_16((byte) 5, 16384),
        KIB_32((byte) 6, 32768),
        KIB_64((byte) 7, 65536);

        private final byte code;
        private final int bytes;

        SstBlockSize(byte code, int bytes) {
            this.code = code;
            this.bytes = bytes;
        }

        byte code() {
            return code;
        }

        /// Returns the block size in bytes.
        public int bytes() {
            return bytes;
        }
    }

    /// Log levels supported by SlateDB logging.
    public enum LogLevel {
        OFF((byte) 0, "off"),
        ERROR((byte) 1, "error"),
        WARN((byte) 2, "warn"),
        INFO((byte) 3, "info"),
        DEBUG((byte) 4, "debug"),
        TRACE((byte) 5, "trace");

        private final byte code;
        private final String value;

        LogLevel(byte code, String value) {
            this.code = code;
            this.value = value;
        }

        byte code() {
            return code;
        }

        public String value() {
            return value;
        }
    }

    /// Kind of a row entry.
    public enum RowEntryKind {
        VALUE((byte) 0),
        TOMBSTONE((byte) 1),
        MERGE((byte) 2);

        private final byte code;

        RowEntryKind(byte code) {
            this.code = code;
        }

        byte code() {
            return code;
        }

        static RowEntryKind fromCode(byte code) {
            for (RowEntryKind value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Unknown row entry kind code: " + code);
        }
    }

    /// Options for put operations.
    public static final class PutOptions {
        private final TtlType ttlType;
        private final long ttlValueMs;

        private PutOptions(TtlType ttlType, long ttlValueMs) {
            this.ttlType = Objects.requireNonNull(ttlType, "ttlType");
            if (ttlValueMs < 0) {
                throw new IllegalArgumentException("ttlValueMs must be >= 0");
            }
            this.ttlValueMs = ttlValueMs;
        }

        /// Uses SlateDB default TTL behavior.
        public static PutOptions defaultTtl() {
            return new PutOptions(TtlType.DEFAULT, 0);
        }

        /// Disables TTL expiry for the entry.
        public static PutOptions noExpiry() {
            return new PutOptions(TtlType.NO_EXPIRY, 0);
        }

        /// Expires the entry after the provided duration.
        public static PutOptions expireAfter(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            return expireAfterMillis(ttl.toMillis());
        }

        /// Expires the entry after the provided number of milliseconds.
        public static PutOptions expireAfterMillis(long ttlMillis) {
            return new PutOptions(TtlType.EXPIRE_AFTER, ttlMillis);
        }

        public TtlType ttlType() {
            return ttlType;
        }

        public long ttlValueMs() {
            return ttlValueMs;
        }
    }

    /// Options for merge operations.
    public static final class MergeOptions {
        private final TtlType ttlType;
        private final long ttlValueMs;

        private MergeOptions(TtlType ttlType, long ttlValueMs) {
            this.ttlType = Objects.requireNonNull(ttlType, "ttlType");
            if (ttlValueMs < 0) {
                throw new IllegalArgumentException("ttlValueMs must be >= 0");
            }
            this.ttlValueMs = ttlValueMs;
        }

        /// Uses SlateDB default TTL behavior.
        public static MergeOptions defaultTtl() {
            return new MergeOptions(TtlType.DEFAULT, 0);
        }

        /// Disables TTL expiry for the entry.
        public static MergeOptions noExpiry() {
            return new MergeOptions(TtlType.NO_EXPIRY, 0);
        }

        /// Expires the entry after the provided duration.
        public static MergeOptions expireAfter(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            return expireAfterMillis(ttl.toMillis());
        }

        /// Expires the entry after the provided number of milliseconds.
        public static MergeOptions expireAfterMillis(long ttlMillis) {
            return new MergeOptions(TtlType.EXPIRE_AFTER, ttlMillis);
        }

        public TtlType ttlType() {
            return ttlType;
        }

        public long ttlValueMs() {
            return ttlValueMs;
        }
    }

    /// Options for write operations.
    public static final class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions(true);

        private final boolean awaitDurable;

        /// Creates write options.
        ///
        /// @param awaitDurable whether the call should await durability in object storage
        public WriteOptions(boolean awaitDurable) {
            this.awaitDurable = awaitDurable;
        }

        public boolean awaitDurable() {
            return awaitDurable;
        }
    }

    /// Options for read operations.
    public static final class ReadOptions {
        public static final ReadOptions DEFAULT = builder().build();

        private final Durability durabilityFilter;
        private final boolean dirty;
        private final boolean cacheBlocks;

        private ReadOptions(Builder builder) {
            this.durabilityFilter = builder.durabilityFilter;
            this.dirty = builder.dirty;
            this.cacheBlocks = builder.cacheBlocks;
        }

        public Durability durabilityFilter() {
            return durabilityFilter;
        }

        public boolean dirty() {
            return dirty;
        }

        public boolean cacheBlocks() {
            return cacheBlocks;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Durability durabilityFilter = Durability.MEMORY;
            private boolean dirty;
            private boolean cacheBlocks = true;

            public Builder durabilityFilter(Durability durabilityFilter) {
                this.durabilityFilter = Objects.requireNonNull(durabilityFilter, "durabilityFilter");
                return this;
            }

            public Builder dirty(boolean dirty) {
                this.dirty = dirty;
                return this;
            }

            public Builder cacheBlocks(boolean cacheBlocks) {
                this.cacheBlocks = cacheBlocks;
                return this;
            }

            public ReadOptions build() {
                return new ReadOptions(this);
            }
        }
    }

    /// Options for scan operations.
    public static final class ScanOptions {
        public static final ScanOptions DEFAULT = builder().build();

        private final Durability durabilityFilter;
        private final boolean dirty;
        private final long readAheadBytes;
        private final boolean cacheBlocks;
        private final long maxFetchTasks;

        private ScanOptions(Builder builder) {
            this.durabilityFilter = builder.durabilityFilter;
            this.dirty = builder.dirty;
            this.readAheadBytes = builder.readAheadBytes;
            this.cacheBlocks = builder.cacheBlocks;
            this.maxFetchTasks = builder.maxFetchTasks;
        }

        public Durability durabilityFilter() {
            return durabilityFilter;
        }

        public boolean dirty() {
            return dirty;
        }

        public long readAheadBytes() {
            return readAheadBytes;
        }

        public boolean cacheBlocks() {
            return cacheBlocks;
        }

        public long maxFetchTasks() {
            return maxFetchTasks;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Durability durabilityFilter = Durability.MEMORY;
            private boolean dirty;
            private long readAheadBytes = 1;
            private boolean cacheBlocks;
            private long maxFetchTasks = 1;

            public Builder durabilityFilter(Durability durabilityFilter) {
                this.durabilityFilter = Objects.requireNonNull(durabilityFilter, "durabilityFilter");
                return this;
            }

            public Builder dirty(boolean dirty) {
                this.dirty = dirty;
                return this;
            }

            public Builder readAheadBytes(long readAheadBytes) {
                if (readAheadBytes < 0) {
                    throw new IllegalArgumentException("readAheadBytes must be >= 0");
                }
                this.readAheadBytes = readAheadBytes;
                return this;
            }

            public Builder cacheBlocks(boolean cacheBlocks) {
                this.cacheBlocks = cacheBlocks;
                return this;
            }

            public Builder maxFetchTasks(long maxFetchTasks) {
                if (maxFetchTasks < 0) {
                    throw new IllegalArgumentException("maxFetchTasks must be >= 0");
                }
                this.maxFetchTasks = maxFetchTasks;
                return this;
            }

            public ScanOptions build() {
                return new ScanOptions(this);
            }
        }
    }

    /// Options for opening a read-only [SlateDbReader].
    public static final class ReaderOptions {
        private final long manifestPollIntervalMs;
        private final long checkpointLifetimeMs;
        private final long maxMemtableBytes;
        private final boolean skipWalReplay;

        private ReaderOptions(Builder builder) {
            this.manifestPollIntervalMs = builder.manifestPollIntervalMs;
            this.checkpointLifetimeMs = builder.checkpointLifetimeMs;
            this.maxMemtableBytes = builder.maxMemtableBytes;
            this.skipWalReplay = builder.skipWalReplay;
        }

        public long manifestPollIntervalMs() {
            return manifestPollIntervalMs;
        }

        public long checkpointLifetimeMs() {
            return checkpointLifetimeMs;
        }

        public long maxMemtableBytes() {
            return maxMemtableBytes;
        }

        public boolean skipWalReplay() {
            return skipWalReplay;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private long manifestPollIntervalMs;
            private long checkpointLifetimeMs;
            private long maxMemtableBytes;
            private boolean skipWalReplay;

            public Builder manifestPollInterval(Duration interval) {
                Objects.requireNonNull(interval, "interval");
                this.manifestPollIntervalMs = interval.toMillis();
                return this;
            }

            public Builder checkpointLifetime(Duration lifetime) {
                Objects.requireNonNull(lifetime, "lifetime");
                this.checkpointLifetimeMs = lifetime.toMillis();
                return this;
            }

            public Builder maxMemtableBytes(long maxMemtableBytes) {
                if (maxMemtableBytes < 0) {
                    throw new IllegalArgumentException("maxMemtableBytes must be >= 0");
                }
                this.maxMemtableBytes = maxMemtableBytes;
                return this;
            }

            public Builder skipWalReplay(boolean skipWalReplay) {
                this.skipWalReplay = skipWalReplay;
                return this;
            }

            public ReaderOptions build() {
                return new ReaderOptions(this);
            }
        }
    }
}
