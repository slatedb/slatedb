package com.slatedb.exceptions;

/**
 * Exception thrown for internal SlateDB errors.
 * 
 * This exception is typically thrown when:
 * - Internal database consistency errors occur
 * - Unexpected native library errors
 * - Memory allocation failures
 * - Corruption or data integrity issues
 */
public class SlateDBInternalException extends SlateDBException {
    
    /**
     * Constructs a new SlateDBInternalException with the specified detail message.
     * 
     * @param message the detail message
     */
    public SlateDBInternalException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new SlateDBInternalException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public SlateDBInternalException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new SlateDBInternalException with the specified detail message and error code.
     * 
     * @param message the detail message
     * @param errorCode the native error code
     */
    public SlateDBInternalException(String message, int errorCode) {
        super(message, errorCode);
    }
    
    /**
     * Constructs a new SlateDBInternalException with the specified detail message, cause, and error code.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param errorCode the native error code
     */
    public SlateDBInternalException(String message, Throwable cause, int errorCode) {
        super(message, cause, errorCode);
    }
}