package com.slatedb.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration options for the SlateDB background compactor.
 * 
 * The compactor is responsible for merging and organizing SSTables
 * to maintain optimal read performance and storage efficiency.
 */
public final class CompactorOptions {
    private final Duration pollInterval;
    private final Duration manifestUpdateTimeout;
    private final Long maxSstSizeBytes;
    private final Integer maxConcurrentCompactions;
    
    private CompactorOptions(Builder builder) {
        this.pollInterval = builder.pollInterval;
        this.manifestUpdateTimeout = builder.manifestUpdateTimeout;
        this.maxSstSizeBytes = builder.maxSstSizeBytes;
        this.maxConcurrentCompactions = builder.maxConcurrentCompactions;
    }
    
    /**
     * Gets how often to check for compaction opportunities.
     * 
     * @return the poll interval, or null if using default
     */
    public Duration getPollInterval() {
        return pollInterval;
    }
    
    /**
     * Gets the timeout for manifest update operations.
     * 
     * @return the manifest update timeout, or null if using default
     */
    public Duration getManifestUpdateTimeout() {
        return manifestUpdateTimeout;
    }
    
    /**
     * Gets the target size for output SSTables during compaction.
     * 
     * @return the max SST size in bytes, or null if using default
     */
    public Long getMaxSstSizeBytes() {
        return maxSstSizeBytes;
    }
    
    /**
     * Gets the maximum number of concurrent compaction jobs.
     * 
     * @return the max concurrent compactions, or null if using default
     */
    public Integer getMaxConcurrentCompactions() {
        return maxConcurrentCompactions;
    }
    
    /**
     * Creates a new builder for CompactorOptions.
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
                .pollInterval(pollInterval)
                .manifestUpdateTimeout(manifestUpdateTimeout)
                .maxSstSizeBytes(maxSstSizeBytes)
                .maxConcurrentCompactions(maxConcurrentCompactions);
    }
    
    /**
     * Creates CompactorOptions with default settings.
     * 
     * @return CompactorOptions with all defaults
     */
    public static CompactorOptions defaultOptions() {
        return builder().build();
    }
    
    /**
     * Creates CompactorOptions optimized for high throughput.
     * 
     * @return CompactorOptions for high throughput
     */
    public static CompactorOptions highThroughput() {
        return builder()
                .pollInterval(Duration.ofSeconds(1)) // More frequent compaction
                .maxSstSizeBytes(512L * 1024 * 1024) // 512MB SSTs
                .maxConcurrentCompactions(8) // More parallel compactions
                .build();
    }
    
    /**
     * Creates CompactorOptions optimized for low resource usage.
     * 
     * @return CompactorOptions for minimal resource usage
     */
    public static CompactorOptions lowResource() {
        return builder()
                .pollInterval(Duration.ofSeconds(30)) // Less frequent compaction
                .maxSstSizeBytes(64L * 1024 * 1024) // 64MB SSTs
                .maxConcurrentCompactions(1) // Single-threaded compaction
                .build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        CompactorOptions that = (CompactorOptions) obj;
        return Objects.equals(pollInterval, that.pollInterval) &&
               Objects.equals(manifestUpdateTimeout, that.manifestUpdateTimeout) &&
               Objects.equals(maxSstSizeBytes, that.maxSstSizeBytes) &&
               Objects.equals(maxConcurrentCompactions, that.maxConcurrentCompactions);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(pollInterval, manifestUpdateTimeout, maxSstSizeBytes, maxConcurrentCompactions);
    }
    
    @Override
    public String toString() {
        return "CompactorOptions{" +
                "pollInterval=" + pollInterval +
                ", manifestUpdateTimeout=" + manifestUpdateTimeout +
                ", maxSstSizeBytes=" + maxSstSizeBytes +
                ", maxConcurrentCompactions=" + maxConcurrentCompactions +
                '}';
    }
    
    /**
     * Builder for creating CompactorOptions instances.
     */
    public static final class Builder {
        private Duration pollInterval;
        private Duration manifestUpdateTimeout;
        private Long maxSstSizeBytes;
        private Integer maxConcurrentCompactions;
        
        private Builder() {}
        
        /**
         * Sets how often to check for compaction opportunities.
         * 
         * More frequent polling can reduce space amplification but
         * uses more CPU resources.
         * 
         * @param pollInterval the poll interval
         * @return this builder
         * @throws IllegalArgumentException if interval is negative
         */
        public Builder pollInterval(Duration pollInterval) {
            if (pollInterval != null && pollInterval.isNegative()) {
                throw new IllegalArgumentException("Poll interval cannot be negative");
            }
            this.pollInterval = pollInterval;
            return this;
        }
        
        /**
         * Sets the timeout for manifest update operations.
         * 
         * This controls how long to wait for manifest updates during
         * compaction operations.
         * 
         * @param manifestUpdateTimeout the timeout duration
         * @return this builder
         * @throws IllegalArgumentException if timeout is negative
         */
        public Builder manifestUpdateTimeout(Duration manifestUpdateTimeout) {
            if (manifestUpdateTimeout != null && manifestUpdateTimeout.isNegative()) {
                throw new IllegalArgumentException("Manifest update timeout cannot be negative");
            }
            this.manifestUpdateTimeout = manifestUpdateTimeout;
            return this;
        }
        
        /**
         * Sets the target size for output SSTables during compaction.
         * 
         * Larger SSTables reduce metadata overhead but can increase
         * write amplification for updates.
         * 
         * @param maxSstSizeBytes the max SST size in bytes
         * @return this builder
         * @throws IllegalArgumentException if size is negative
         */
        public Builder maxSstSizeBytes(Long maxSstSizeBytes) {
            if (maxSstSizeBytes != null && maxSstSizeBytes <= 0) {
                throw new IllegalArgumentException("Max SST size must be positive");
            }
            this.maxSstSizeBytes = maxSstSizeBytes;
            return this;
        }
        
        /**
         * Sets the maximum number of concurrent compaction jobs.
         * 
         * Higher values can improve compaction throughput but use
         * more system resources.
         * 
         * @param maxConcurrentCompactions the max concurrent compactions
         * @return this builder
         * @throws IllegalArgumentException if count is negative
         */
        public Builder maxConcurrentCompactions(Integer maxConcurrentCompactions) {
            if (maxConcurrentCompactions != null && maxConcurrentCompactions <= 0) {
                throw new IllegalArgumentException("Max concurrent compactions must be positive");
            }
            this.maxConcurrentCompactions = maxConcurrentCompactions;
            return this;
        }
        
        /**
         * Builds the CompactorOptions instance.
         * 
         * @return a new CompactorOptions instance
         */
        public CompactorOptions build() {
            return new CompactorOptions(this);
        }
    }
}