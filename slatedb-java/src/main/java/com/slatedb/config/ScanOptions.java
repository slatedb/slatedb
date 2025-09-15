package com.slatedb.config;

import java.util.Objects;

/**
 * Options for scan operations in SlateDB.
 * 
 * This class controls the behavior of scan operations, including
 * durability requirements, read-ahead behavior, and caching settings.
 */
public final class ScanOptions {
    private final DurabilityLevel durabilityFilter;
    private final boolean dirty;
    private final long readAheadBytes;
    private final boolean cacheBlocks;
    private final long maxFetchTasks;
    
    private ScanOptions(Builder builder) {
        this.durabilityFilter = Objects.requireNonNull(builder.durabilityFilter, "Durability filter cannot be null");
        this.dirty = builder.dirty;
        this.readAheadBytes = builder.readAheadBytes;
        this.cacheBlocks = builder.cacheBlocks;
        this.maxFetchTasks = builder.maxFetchTasks;
    }
    
    /**
     * Gets the minimum durability level for returned data.
     * 
     * @return the durability filter level
     */
    public DurabilityLevel getDurabilityFilter() {
        return durabilityFilter;
    }
    
    /**
     * Gets whether to include uncommitted data in scans.
     * 
     * @return true if uncommitted data should be included, false otherwise
     */
    public boolean isDirty() {
        return dirty;
    }
    
    /**
     * Gets the read-ahead buffer size in bytes.
     * 
     * @return the read-ahead buffer size
     */
    public long getReadAheadBytes() {
        return readAheadBytes;
    }
    
    /**
     * Gets whether to cache fetched blocks.
     * 
     * @return true if blocks should be cached, false otherwise
     */
    public boolean isCacheBlocks() {
        return cacheBlocks;
    }
    
    /**
     * Gets the maximum number of concurrent fetch tasks.
     * 
     * @return the maximum number of concurrent fetch tasks
     */
    public long getMaxFetchTasks() {
        return maxFetchTasks;
    }
    
    /**
     * Creates a new builder for ScanOptions.
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
                .durabilityFilter(durabilityFilter)
                .dirty(dirty)
                .readAheadBytes(readAheadBytes)
                .cacheBlocks(cacheBlocks)
                .maxFetchTasks(maxFetchTasks);
    }
    
    /**
     * Creates ScanOptions with default settings.
     * 
     * @return ScanOptions with default settings
     */
    public static ScanOptions defaultOptions() {
        return builder().build();
    }
    
    /**
     * Creates ScanOptions for high-performance scans with caching.
     * 
     * @return ScanOptions optimized for performance
     */
    public static ScanOptions highPerformance() {
        return builder()
                .cacheBlocks(true)
                .readAheadBytes(65536) // 64KB
                .maxFetchTasks(4)
                .build();
    }
    
    /**
     * Creates ScanOptions for consistent scans (only persisted data).
     * 
     * @return ScanOptions for consistent scans
     */
    public static ScanOptions consistentScan() {
        return builder()
                .durabilityFilter(DurabilityLevel.REMOTE)
                .dirty(false)
                .build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ScanOptions that = (ScanOptions) obj;
        return dirty == that.dirty &&
               readAheadBytes == that.readAheadBytes &&
               cacheBlocks == that.cacheBlocks &&
               maxFetchTasks == that.maxFetchTasks &&
               durabilityFilter == that.durabilityFilter;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(durabilityFilter, dirty, readAheadBytes, cacheBlocks, maxFetchTasks);
    }
    
    @Override
    public String toString() {
        return "ScanOptions{" +
                "durabilityFilter=" + durabilityFilter +
                ", dirty=" + dirty +
                ", readAheadBytes=" + readAheadBytes +
                ", cacheBlocks=" + cacheBlocks +
                ", maxFetchTasks=" + maxFetchTasks +
                '}';
    }
    
    /**
     * Builder for creating ScanOptions instances.
     */
    public static final class Builder {
        private DurabilityLevel durabilityFilter = DurabilityLevel.MEMORY; // Default
        private boolean dirty = false; // Default to clean reads
        private long readAheadBytes = 1; // Default minimal read-ahead
        private boolean cacheBlocks = false; // Default no caching
        private long maxFetchTasks = 1; // Default single-threaded
        
        private Builder() {}
        
        /**
         * Sets the minimum durability level for returned data.
         * 
         * @param durabilityFilter the durability level
         * @return this builder
         * @throws IllegalArgumentException if durabilityFilter is null
         */
        public Builder durabilityFilter(DurabilityLevel durabilityFilter) {
            this.durabilityFilter = Objects.requireNonNull(durabilityFilter, "Durability filter cannot be null");
            return this;
        }
        
        /**
         * Sets whether to include uncommitted data in scans.
         * 
         * When true, scans may return uncommitted data that hasn't been
         * written to the WAL yet. This provides the lowest latency but
         * may return data that could be lost on system failure.
         * 
         * When false (default), only committed data is returned.
         * 
         * @param dirty true to include uncommitted data, false otherwise
         * @return this builder
         */
        public Builder dirty(boolean dirty) {
            this.dirty = dirty;
            return this;
        }
        
        /**
         * Sets the read-ahead buffer size in bytes.
         * 
         * Higher values can improve sequential scan performance by
         * reducing the number of I/O operations, but use more memory.
         * 
         * @param readAheadBytes the read-ahead buffer size
         * @return this builder
         * @throws IllegalArgumentException if readAheadBytes is negative
         */
        public Builder readAheadBytes(long readAheadBytes) {
            if (readAheadBytes < 0) {
                throw new IllegalArgumentException("Read-ahead bytes cannot be negative");
            }
            this.readAheadBytes = readAheadBytes;
            return this;
        }
        
        /**
         * Sets whether to cache fetched blocks.
         * 
         * When true, fetched blocks are cached to improve performance
         * of subsequent reads. This uses more memory but can significantly
         * improve performance for repeated access patterns.
         * 
         * @param cacheBlocks true to cache blocks, false otherwise
         * @return this builder
         */
        public Builder cacheBlocks(boolean cacheBlocks) {
            this.cacheBlocks = cacheBlocks;
            return this;
        }
        
        /**
         * Sets the maximum number of concurrent fetch tasks.
         * 
         * Higher values can improve scan performance by parallelizing
         * I/O operations, but may increase resource usage and contention.
         * 
         * @param maxFetchTasks the maximum number of concurrent fetch tasks
         * @return this builder
         * @throws IllegalArgumentException if maxFetchTasks is less than 1
         */
        public Builder maxFetchTasks(long maxFetchTasks) {
            if (maxFetchTasks < 1) {
                throw new IllegalArgumentException("Max fetch tasks must be at least 1");
            }
            this.maxFetchTasks = maxFetchTasks;
            return this;
        }
        
        /**
         * Builds the ScanOptions instance.
         * 
         * @return a new ScanOptions instance
         */
        public ScanOptions build() {
            return new ScanOptions(this);
        }
    }
}