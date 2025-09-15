package com.slatedb.exceptions;

/**
 * Exception thrown when a requested key is not found in the database.
 * 
 * This exception is typically thrown during get operations when the
 * specified key does not exist in the database.
 */
public class SlateDBNotFoundException extends SlateDBException {
    
    /**
     * Constructs a new SlateDBNotFoundException with the specified detail message.
     * 
     * @param message the detail message
     */
    public SlateDBNotFoundException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new SlateDBNotFoundException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public SlateDBNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new SlateDBNotFoundException with the specified detail message and error code.
     * 
     * @param message the detail message
     * @param errorCode the native error code
     */
    public SlateDBNotFoundException(String message, int errorCode) {
        super(message, errorCode);
    }
    
    /**
     * Constructs a new SlateDBNotFoundException with the specified detail message, cause, and error code.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param errorCode the native error code
     */
    public SlateDBNotFoundException(String message, Throwable cause, int errorCode) {
        super(message, cause, errorCode);
    }
}