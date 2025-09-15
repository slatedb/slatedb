package com.slatedb.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Options for put operations in SlateDB.
 * 
 * This class controls the behavior of put operations, particularly
 * Time-To-Live (TTL) settings for automatic data expiration.
 */
public final class PutOptions {
    private final TTLType ttlType;
    private final long ttlValue;
    
    private PutOptions(Builder builder) {
        this.ttlType = Objects.requireNonNull(builder.ttlType, "TTL type cannot be null");
        this.ttlValue = builder.ttlValue;
    }
    
    /**
     * Gets the TTL type for this put operation.
     * 
     * @return the TTL type
     */
    public TTLType getTtlType() {
        return ttlType;
    }
    
    /**
     * Gets the TTL value in milliseconds.
     * 
     * @return the TTL value (only meaningful for TTLExpireAfter type)
     */
    public long getTtlValue() {
        return ttlValue;
    }
    
    /**
     * Creates a new builder for PutOptions.
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
                .ttlType(ttlType)
                .ttlValue(ttlValue);
    }
    
    /**
     * Creates PutOptions with default settings (use database default TTL).
     * 
     * @return PutOptions with default settings
     */
    public static PutOptions defaultOptions() {
        return builder().build();
    }
    
    /**
     * Creates PutOptions for data that never expires.
     * 
     * @return PutOptions for non-expiring data
     */
    public static PutOptions noExpiry() {
        return builder().ttlType(TTLType.NO_EXPIRY).build();
    }
    
    /**
     * Creates PutOptions for data that expires after the specified duration.
     * 
     * @param duration the expiration duration
     * @return PutOptions for expiring data
     */
    public static PutOptions expireAfter(Duration duration) {
        return builder()
                .ttlType(TTLType.EXPIRE_AFTER)
                .ttlValue(duration.toMillis())
                .build();
    }
    
    /**
     * Creates PutOptions for data that expires after the specified milliseconds.
     * 
     * @param milliseconds the expiration time in milliseconds
     * @return PutOptions for expiring data
     */
    public static PutOptions expireAfter(long milliseconds) {
        return builder()
                .ttlType(TTLType.EXPIRE_AFTER)
                .ttlValue(milliseconds)
                .build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        PutOptions that = (PutOptions) obj;
        return ttlValue == that.ttlValue &&
               ttlType == that.ttlType;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(ttlType, ttlValue);
    }
    
    @Override
    public String toString() {
        return "PutOptions{" +
                "ttlType=" + ttlType +
                ", ttlValue=" + ttlValue +
                '}';
    }
    
    /**
     * Builder for creating PutOptions instances.
     */
    public static final class Builder {
        private TTLType ttlType = TTLType.DEFAULT; // Default to database default
        private long ttlValue = 0;
        
        private Builder() {}
        
        /**
         * Sets the TTL type.
         * 
         * @param ttlType the TTL type
         * @return this builder
         * @throws IllegalArgumentException if ttlType is null
         */
        public Builder ttlType(TTLType ttlType) {
            this.ttlType = Objects.requireNonNull(ttlType, "TTL type cannot be null");
            return this;
        }
        
        /**
         * Sets the TTL value in milliseconds.
         * 
         * This value is only used when ttlType is TTLType.EXPIRE_AFTER.
         * 
         * @param ttlValue the TTL value in milliseconds
         * @return this builder
         * @throws IllegalArgumentException if ttlValue is negative
         */
        public Builder ttlValue(long ttlValue) {
            if (ttlValue < 0) {
                throw new IllegalArgumentException("TTL value cannot be negative");
            }
            this.ttlValue = ttlValue;
            return this;
        }
        
        /**
         * Sets the TTL value as a Duration.
         * 
         * This value is only used when ttlType is TTLType.EXPIRE_AFTER.
         * 
         * @param duration the TTL duration
         * @return this builder
         * @throws IllegalArgumentException if duration is null or negative
         */
        public Builder ttlValue(Duration duration) {
            Objects.requireNonNull(duration, "Duration cannot be null");
            return ttlValue(duration.toMillis());
        }
        
        /**
         * Builds the PutOptions instance.
         * 
         * @return a new PutOptions instance
         */
        public PutOptions build() {
            return new PutOptions(this);
        }
    }
}