package com.slatedb.exceptions;

/**
 * Exception thrown when invalid arguments are provided to SlateDB operations.
 * 
 * This exception is typically thrown when:
 * - Empty or null keys are provided
 * - Invalid configuration parameters are specified
 * - Malformed data is passed to operations
 * - Out of range values are provided
 */
public class SlateDBInvalidArgumentException extends SlateDBException {
    
    /**
     * Constructs a new SlateDBInvalidArgumentException with the specified detail message.
     * 
     * @param message the detail message
     */
    public SlateDBInvalidArgumentException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new SlateDBInvalidArgumentException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public SlateDBInvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new SlateDBInvalidArgumentException with the specified detail message and error code.
     * 
     * @param message the detail message
     * @param errorCode the native error code
     */
    public SlateDBInvalidArgumentException(String message, int errorCode) {
        super(message, errorCode);
    }
    
    /**
     * Constructs a new SlateDBInvalidArgumentException with the specified detail message, cause, and error code.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param errorCode the native error code
     */
    public SlateDBInvalidArgumentException(String message, Throwable cause, int errorCode) {
        super(message, cause, errorCode);
    }
}