package com.slatedb.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration options for SlateDB database instances.
 * 
 * This class contains tuning parameters that affect database behavior,
 * performance characteristics, and resource usage.
 */
public final class SlateDBOptions {
    private final Long l0SstSizeBytes;
    private final Duration flushInterval;
    private final String cacheFolder;
    private final SstBlockSize sstBlockSize;
    private final CompactorOptions compactorOptions;
    
    private SlateDBOptions(Builder builder) {
        this.l0SstSizeBytes = builder.l0SstSizeBytes;
        this.flushInterval = builder.flushInterval;
        this.cacheFolder = builder.cacheFolder;
        this.sstBlockSize = builder.sstBlockSize;
        this.compactorOptions = builder.compactorOptions;
    }
    
    /**
     * Gets the L0 SSTable size threshold in bytes.
     * 
     * @return the L0 SST size, or null if using default
     */
    public Long getL0SstSizeBytes() {
        return l0SstSizeBytes;
    }
    
    /**
     * Gets the WAL flush interval.
     * 
     * @return the flush interval, or null if using default
     */
    public Duration getFlushInterval() {
        return flushInterval;
    }
    
    /**
     * Gets the local cache folder path.
     * 
     * @return the cache folder path, or null if no caching
     */
    public String getCacheFolder() {
        return cacheFolder;
    }
    
    /**
     * Gets the SSTable block size.
     * 
     * @return the block size, or null if using default
     */
    public SstBlockSize getSstBlockSize() {
        return sstBlockSize;
    }
    
    /**
     * Gets the compactor options.
     * 
     * @return the compactor options, or null if using defaults
     */
    public CompactorOptions getCompactorOptions() {
        return compactorOptions;
    }
    
    /**
     * Creates a new builder for SlateDBOptions.
     * 
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Creates a new builder initialized with values from this configuration.
     * 
     * @return a new builder instance
     */
    public Builder toBuilder() {
        return new Builder()
                .l0SstSizeBytes(l0SstSizeBytes)
                .flushInterval(flushInterval)
                .cacheFolder(cacheFolder)
                .sstBlockSize(sstBlockSize)
                .compactorOptions(compactorOptions);
    }
    
    /**
     * Creates SlateDBOptions with default settings.
     * 
     * @return SlateDBOptions with all defaults
     */
    public static SlateDBOptions defaultOptions() {
        return builder().build();
    }
    
    /**
     * Creates SlateDBOptions optimized for high throughput.
     * 
     * @return SlateDBOptions for high throughput
     */
    public static SlateDBOptions highThroughput() {
        return builder()
                .l0SstSizeBytes(128L * 1024 * 1024) // 128MB
                .flushInterval(Duration.ofMillis(50)) // 50ms
                .sstBlockSize(SstBlockSize.SIZE_64_KIB)
                .build();
    }
    
    /**
     * Creates SlateDBOptions optimized for low latency.
     * 
     * @return SlateDBOptions for low latency
     */
    public static SlateDBOptions lowLatency() {
        return builder()
                .l0SstSizeBytes(32L * 1024 * 1024) // 32MB
                .flushInterval(Duration.ofMillis(10)) // 10ms
                .sstBlockSize(SstBlockSize.SIZE_4_KIB)
                .build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        SlateDBOptions that = (SlateDBOptions) obj;
        return Objects.equals(l0SstSizeBytes, that.l0SstSizeBytes) &&
               Objects.equals(flushInterval, that.flushInterval) &&
               Objects.equals(cacheFolder, that.cacheFolder) &&
               Objects.equals(sstBlockSize, that.sstBlockSize) &&
               Objects.equals(compactorOptions, that.compactorOptions);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(l0SstSizeBytes, flushInterval, cacheFolder, sstBlockSize, compactorOptions);
    }
    
    @Override
    public String toString() {
        return "SlateDBOptions{" +
                "l0SstSizeBytes=" + l0SstSizeBytes +
                ", flushInterval=" + flushInterval +
                ", cacheFolder='" + cacheFolder + '\'' +
                ", sstBlockSize=" + sstBlockSize +
                ", compactorOptions=" + compactorOptions +
                '}';
    }
    
    /**
     * Builder for creating SlateDBOptions instances.
     */
    public static final class Builder {
        private Long l0SstSizeBytes;
        private Duration flushInterval;
        private String cacheFolder;
        private SstBlockSize sstBlockSize;
        private CompactorOptions compactorOptions;
        
        private Builder() {}
        
        /**
         * Sets the L0 SSTable size threshold in bytes.
         * 
         * When the in-memory table reaches this size, it will be flushed
         * to an SSTable. Larger values reduce the number of API calls
         * but increase recovery time.
         * 
         * @param l0SstSizeBytes the L0 SST size threshold
         * @return this builder
         * @throws IllegalArgumentException if size is negative
         */
        public Builder l0SstSizeBytes(Long l0SstSizeBytes) {
            if (l0SstSizeBytes != null && l0SstSizeBytes < 0) {
                throw new IllegalArgumentException("L0 SST size cannot be negative");
            }
            this.l0SstSizeBytes = l0SstSizeBytes;
            return this;
        }
        
        /**
         * Sets the WAL flush interval.
         * 
         * This controls how often the write-ahead log is flushed to
         * object storage. Lower values reduce latency but increase
         * API costs.
         * 
         * @param flushInterval the flush interval
         * @return this builder
         * @throws IllegalArgumentException if interval is negative
         */
        public Builder flushInterval(Duration flushInterval) {
            if (flushInterval != null && flushInterval.isNegative()) {
                throw new IllegalArgumentException("Flush interval cannot be negative");
            }
            this.flushInterval = flushInterval;
            return this;
        }
        
        /**
         * Sets the local folder for caching SSTables.
         * 
         * When set, SSTables will be cached locally to improve
         * read performance. Set to null to disable caching.
         * 
         * @param cacheFolder the cache folder path
         * @return this builder
         */
        public Builder cacheFolder(String cacheFolder) {
            this.cacheFolder = cacheFolder;
            return this;
        }
        
        /**
         * Sets the block size for SSTable blocks.
         * 
         * Blocks are the unit of reading and caching. Larger blocks
         * can improve sequential read performance but may waste
         * space for random access patterns.
         * 
         * @param sstBlockSize the SST block size
         * @return this builder
         */
        public Builder sstBlockSize(SstBlockSize sstBlockSize) {
            this.sstBlockSize = sstBlockSize;
            return this;
        }
        
        /**
         * Sets the compactor options.
         * 
         * @param compactorOptions the compactor configuration
         * @return this builder
         */
        public Builder compactorOptions(CompactorOptions compactorOptions) {
            this.compactorOptions = compactorOptions;
            return this;
        }
        
        /**
         * Builds the SlateDBOptions instance.
         * 
         * @return a new SlateDBOptions instance
         */
        public SlateDBOptions build() {
            return new SlateDBOptions(this);
        }
    }
}