package com.slatedb;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a key-value pair from SlateDB operations.
 * 
 * This immutable class holds a key and its corresponding value as byte arrays,
 * typically returned from scan operations or iterator traversals.
 */
public final class KeyValue {
    private final byte[] key;
    private final byte[] value;
    
    /**
     * Creates a new KeyValue pair.
     * 
     * @param key the key as a byte array
     * @param value the value as a byte array
     * @throws IllegalArgumentException if key is null
     */
    public KeyValue(byte[] key, byte[] value) {
        this.key = Objects.requireNonNull(key, "Key cannot be null").clone();
        this.value = value != null ? value.clone() : new byte[0];
    }
    
    /**
     * Gets the key.
     * 
     * @return a copy of the key byte array
     */
    public byte[] getKey() {
        return key.clone();
    }
    
    /**
     * Gets the value.
     * 
     * @return a copy of the value byte array
     */
    public byte[] getValue() {
        return value.clone();
    }
    
    /**
     * Gets the key as a UTF-8 string.
     * 
     * @return the key as a string
     * @throws IllegalArgumentException if the key is not valid UTF-8
     */
    public String getKeyAsString() {
        return new String(key, java.nio.charset.StandardCharsets.UTF_8);
    }
    
    /**
     * Gets the value as a UTF-8 string.
     * 
     * @return the value as a string
     * @throws IllegalArgumentException if the value is not valid UTF-8
     */
    public String getValueAsString() {
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }
    
    /**
     * Gets the size of the key in bytes.
     * 
     * @return the key size
     */
    public int getKeySize() {
        return key.length;
    }
    
    /**
     * Gets the size of the value in bytes.
     * 
     * @return the value size
     */
    public int getValueSize() {
        return value.length;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        KeyValue keyValue = (KeyValue) obj;
        return Arrays.equals(key, keyValue.key) &&
               Arrays.equals(value, keyValue.value);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
    }
    
    @Override
    public String toString() {
        return String.format("KeyValue{key=%s, value=%s}", 
                Arrays.toString(key), Arrays.toString(value));
    }
}