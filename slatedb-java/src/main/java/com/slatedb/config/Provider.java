package com.slatedb.config;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enumeration of supported object storage providers for SlateDB.
 */
public enum Provider {
    /**
     * Local filesystem storage provider.
     * Used for development, testing, and single-node deployments.
     */
    LOCAL("local"),
    
    /**
     * Amazon Web Services S3 storage provider.
     * Used for production deployments requiring durability and scalability.
     */
    AWS("aws");
    
    private final String value;
    
    Provider(String value) {
        this.value = value;
    }
    
    /**
     * Gets the string representation of this provider.
     * 
     * @return the provider string value
     */
    @JsonValue
    public String getValue() {
        return value;
    }
    
    /**
     * Creates a Provider from its string representation.
     * 
     * @param value the provider string value
     * @return the corresponding Provider enum
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static Provider fromValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Provider value cannot be null");
        }
        
        for (Provider provider : values()) {
            if (provider.value.equalsIgnoreCase(value)) {
                return provider;
            }
        }
        
        throw new IllegalArgumentException("Unknown provider: " + value);
    }
    
    @Override
    public String toString() {
        return value;
    }
}