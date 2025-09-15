package com.slatedb.exceptions;

/**
 * Base exception for all SlateDB operations.
 * 
 * This is the root exception class for all SlateDB-related errors.
 * Specific error types are represented by subclasses.
 */
public class SlateDBException extends Exception {
    
    private final int errorCode;
    
    /**
     * Constructs a new SlateDBException with the specified detail message.
     * 
     * @param message the detail message
     */
    public SlateDBException(String message) {
        super(message);
        this.errorCode = 0;
    }
    
    /**
     * Constructs a new SlateDBException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public SlateDBException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = 0;
    }
    
    /**
     * Constructs a new SlateDBException with the specified detail message and error code.
     * 
     * @param message the detail message
     * @param errorCode the native error code
     */
    public SlateDBException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
    
    /**
     * Constructs a new SlateDBException with the specified detail message, cause, and error code.
     * 
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @param errorCode the native error code
     */
    public SlateDBException(String message, Throwable cause, int errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    /**
     * Gets the native error code associated with this exception.
     * 
     * @return the error code, or 0 if no specific code is available
     */
    public int getErrorCode() {
        return errorCode;
    }
}