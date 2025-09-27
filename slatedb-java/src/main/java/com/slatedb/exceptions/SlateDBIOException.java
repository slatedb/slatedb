package com.slatedb.exceptions;

/**
 * Exception thrown for I/O and network related errors in SlateDB operations.
 * 
 * This exception is typically thrown when there are issues with:
 * - Object storage operations (S3, local filesystem)
 * - Network connectivity problems
 * - File system access issues
 * - Serialization/deserialization errors
 */
public class SlateDBIOException extends SlateDBException {
    
    /**
     * Constructs a new SlateDBIOException with the specified detail message.
     * 
     * @param message the detail message
     */
    public SlateDBIOException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new SlateDBIOException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public SlateDBIOException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new SlateDBIOException with the specified detail message and error code.
     * 
     * @param message the detail message
     * @param errorCode the native error code
     */
    public SlateDBIOException(String message, int errorCode) {
        super(message, errorCode);
    }
    
    /**
     * Constructs a new SlateDBIOException with the specified detail message, cause, and error code.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param errorCode the native error code
     */
    public SlateDBIOException(String message, Throwable cause, int errorCode) {
        super(message, cause, errorCode);
    }
}