package com.slatedb.config;

/**
 * Enumeration of durability levels for SlateDB operations.
 * 
 * This enum defines the minimum durability level required for data
 * to be included in read and scan operations.
 */
public enum DurabilityLevel {
    /**
     * Include data that is in memory (including both persisted and in-memory data).
     * This is the default level and provides the most recent view of data.
     */
    MEMORY(0),
    
    /**
     * Include only data that has been persisted to object storage.
     * This provides strong consistency guarantees but may not include
     * the most recent writes that are still in the write-ahead log.
     */
    REMOTE(1);
    
    private final int value;
    
    DurabilityLevel(int value) {
        this.value = value;
    }
    
    /**
     * Gets the integer representation of this durability level.
     * 
     * @return the durability level value
     */
    public int getValue() {
        return value;
    }
    
    /**
     * Creates a DurabilityLevel from its integer representation.
     * 
     * @param value the durability level value
     * @return the corresponding DurabilityLevel enum
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static DurabilityLevel fromValue(int value) {
        for (DurabilityLevel level : values()) {
            if (level.value == value) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown durability level: " + value);
    }
    
    @Override
    public String toString() {
        return name().toLowerCase();
    }
}