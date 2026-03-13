package io.slatedb;



/**
 * Error returned by the SlateDB FFI layer.
 *
 * The FFI wrapper groups core SlateDB errors into a smaller set of stable
 * categories while preserving the original message text.
 */
public class SlatedbException extends Exception {
    private SlatedbException(String message) {
      super(message); 
    }

    
    /**
     * A transaction failed to commit or otherwise encountered a conflict.
     */
    public static class Transaction extends SlatedbException {
      
        /**
         * The original error message.
         */
      String message;
      public Transaction(String message) {
        super(new StringBuilder()
        .append("message=")
        .append(message)
        
        
        .toString());
        this.message = message;
        }

      public String message() {
        return this.message;
      }
      
      
      
    }
    
    /**
     * The database or transaction handle has already been closed.
     */
    public static class Closed extends SlatedbException {
      
        /**
         * The original error message.
         */
      String message;
      public Closed(String message) {
        super(new StringBuilder()
        .append("message=")
        .append(message)
        
        
        .toString());
        this.message = message;
        }

      public String message() {
        return this.message;
      }
      
      
      
    }
    
    /**
     * A required dependency or remote service is temporarily unavailable.
     */
    public static class Unavailable extends SlatedbException {
      
        /**
         * The original error message.
         */
      String message;
      public Unavailable(String message) {
        super(new StringBuilder()
        .append("message=")
        .append(message)
        
        
        .toString());
        this.message = message;
        }

      public String message() {
        return this.message;
      }
      
      
      
    }
    
    /**
     * The caller supplied invalid input.
     */
    public static class Invalid extends SlatedbException {
      
        /**
         * The original error message.
         */
      String message;
      public Invalid(String message) {
        super(new StringBuilder()
        .append("message=")
        .append(message)
        
        
        .toString());
        this.message = message;
        }

      public String message() {
        return this.message;
      }
      
      
      
    }
    
    /**
     * Stored data was invalid or could not be decoded.
     */
    public static class Data extends SlatedbException {
      
        /**
         * The original error message.
         */
      String message;
      public Data(String message) {
        super(new StringBuilder()
        .append("message=")
        .append(message)
        
        
        .toString());
        this.message = message;
        }

      public String message() {
        return this.message;
      }
      
      
      
    }
    
    /**
     * An unexpected internal failure occurred.
     */
    public static class Internal extends SlatedbException {
      
        /**
         * The original error message.
         */
      String message;
      public Internal(String message) {
        super(new StringBuilder()
        .append("message=")
        .append(message)
        
        
        .toString());
        this.message = message;
        }

      public String message() {
        return this.message;
      }
      
      
      
    }
     
}

