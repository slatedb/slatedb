package com.slatedb.config;

import java.util.Objects;

/**
 * Options for write operations in SlateDB.
 * 
 * This class controls the behavior of write operations, particularly
 * whether to await durability (completion of write to object storage).
 */
public final class WriteOptions {
    private final boolean awaitDurable;
    
    private WriteOptions(Builder builder) {
        this.awaitDurable = builder.awaitDurable;
    }
    
    /**
     * Gets whether to await durability for write operations.
     * 
     * @return true if writes should wait for durability, false otherwise
     */
    public boolean isAwaitDurable() {
        return awaitDurable;
    }
    
    /**
     * Creates a new builder for WriteOptions.
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
        return new Builder().awaitDurable(awaitDurable);
    }
    
    /**
     * Creates WriteOptions with default settings (await durable = true).
     * 
     * @return WriteOptions with default settings
     */
    public static WriteOptions defaultOptions() {
        return builder().build();
    }
    
    /**
     * Creates WriteOptions for fast writes (await durable = false).
     * 
     * @return WriteOptions for fast writes
     */
    public static WriteOptions fastWrite() {
        return builder().awaitDurable(false).build();
    }
    
    /**
     * Creates WriteOptions for durable writes (await durable = true).
     * 
     * @return WriteOptions for durable writes
     */
    public static WriteOptions durableWrite() {
        return builder().awaitDurable(true).build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        WriteOptions that = (WriteOptions) obj;
        return awaitDurable == that.awaitDurable;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(awaitDurable);
    }
    
    @Override
    public String toString() {
        return "WriteOptions{awaitDurable=" + awaitDurable + '}';
    }
    
    /**
     * Builder for creating WriteOptions instances.
     */
    public static final class Builder {
        private boolean awaitDurable = true; // Default to durable writes
        
        private Builder() {}
        
        /**
         * Sets whether to await durability for write operations.
         * 
         * When true (default), write operations will wait until the data is
         * durably persisted to object storage before returning. This ensures
         * data safety but may have higher latency.
         * 
         * When false, write operations return immediately after writing to
         * the WAL, providing lower latency but less durability guarantee.
         * 
         * @param awaitDurable true to wait for durability, false otherwise
         * @return this builder
         */
        public Builder awaitDurable(boolean awaitDurable) {
            this.awaitDurable = awaitDurable;
            return this;
        }
        
        /**
         * Builds the WriteOptions instance.
         * 
         * @return a new WriteOptions instance
         */
        public WriteOptions build() {
            return new WriteOptions(this);
        }
    }
}