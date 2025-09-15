package com.slatedb.config;

import java.util.Objects;

/**
 * Options for read operations in SlateDB.
 * 
 * This class controls the behavior of read operations, including
 * durability requirements and whether to include uncommitted data.
 */
public final class ReadOptions {
    private final DurabilityLevel durabilityFilter;
    private final boolean dirty;
    
    private ReadOptions(Builder builder) {
        this.durabilityFilter = Objects.requireNonNull(builder.durabilityFilter, "Durability filter cannot be null");
        this.dirty = builder.dirty;
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
     * Gets whether to include uncommitted data in reads.
     * 
     * @return true if uncommitted data should be included, false otherwise
     */
    public boolean isDirty() {
        return dirty;
    }
    
    /**
     * Creates a new builder for ReadOptions.
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
                .dirty(dirty);
    }
    
    /**
     * Creates ReadOptions with default settings.
     * 
     * @return ReadOptions with default settings
     */
    public static ReadOptions defaultOptions() {
        return builder().build();
    }
    
    /**
     * Creates ReadOptions for consistent reads (only persisted data).
     * 
     * @return ReadOptions for consistent reads
     */
    public static ReadOptions consistentRead() {
        return builder()
                .durabilityFilter(DurabilityLevel.REMOTE)
                .dirty(false)
                .build();
    }
    
    /**
     * Creates ReadOptions for eventual consistency (includes in-memory data).
     * 
     * @return ReadOptions for eventual consistency
     */
    public static ReadOptions eventualRead() {
        return builder()
                .durabilityFilter(DurabilityLevel.MEMORY)
                .dirty(false)
                .build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ReadOptions that = (ReadOptions) obj;
        return dirty == that.dirty &&
               durabilityFilter == that.durabilityFilter;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(durabilityFilter, dirty);
    }
    
    @Override
    public String toString() {
        return "ReadOptions{" +
                "durabilityFilter=" + durabilityFilter +
                ", dirty=" + dirty +
                '}';
    }
    
    /**
     * Builder for creating ReadOptions instances.
     */
    public static final class Builder {
        private DurabilityLevel durabilityFilter = DurabilityLevel.MEMORY; // Default
        private boolean dirty = false; // Default to clean reads
        
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
         * Sets whether to include uncommitted data in reads.
         * 
         * When true, reads may return uncommitted data that hasn't been
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
         * Builds the ReadOptions instance.
         * 
         * @return a new ReadOptions instance
         */
        public ReadOptions build() {
            return new ReadOptions(this);
        }
    }
}