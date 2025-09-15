package com.slatedb.config;

/**
 * Enumeration of Time-To-Live (TTL) types for SlateDB put operations.
 * 
 * This enum defines how TTL should be handled for individual put operations.
 */
public enum TTLType {
    /**
     * Use the database's default TTL configuration.
     * The actual TTL behavior depends on the database's global TTL settings.
     */
    DEFAULT(0),
    
    /**
     * The data should never expire.
     * This overrides any database-level TTL configuration for this specific key.
     */
    NO_EXPIRY(1),
    
    /**
     * The data should expire after the specified duration.
     * The TTL value must be provided in milliseconds.
     */
    EXPIRE_AFTER(2);
    
    private final int value;
    
    TTLType(int value) {
        this.value = value;
    }
    
    /**
     * Gets the integer representation of this TTL type.
     * 
     * @return the TTL type value
     */
    public int getValue() {
        return value;
    }
    
    /**
     * Creates a TTLType from its integer representation.
     * 
     * @param value the TTL type value
     * @return the corresponding TTLType enum
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static TTLType fromValue(int value) {
        for (TTLType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown TTL type: " + value);
    }
    
    @Override
    public String toString() {
        return name().toLowerCase().replace('_', '-');
    }
}