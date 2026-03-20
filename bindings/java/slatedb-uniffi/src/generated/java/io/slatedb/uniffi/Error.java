package io.slatedb.uniffi;



/**
 * Error type returned by the UniFFI bindings.
 */
public class Error extends Exception {
    private Error(String message) {
      super(message); 
    }

    
    /**
     * Transaction-specific failure.
     */
    public static class Transaction extends Error {
      
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
     * Operation attempted on a closed handle.
     */
    public static class Closed extends Error {
      
        /**
         * Reported reason the handle is closed.
         */
      CloseReason reason;
      
        /**
         * Human-readable error message.
         */
      String message;
      public Closed(CloseReason reason, String message) {
        super(new StringBuilder()
        .append("reason=")
        .append(reason)
        
        .append(", ")
        
        
        .append("message=")
        .append(message)
        
        
        .toString());
        this.reason = reason;
        this.message = message;
        }

      public CloseReason reason() {
        return this.reason;
      }
      public String message() {
        return this.message;
      }
      
      
      
    }
    
    /**
     * Temporary unavailability, such as an unavailable dependency.
     */
    public static class Unavailable extends Error {
      
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
     * Invalid input or invalid API usage.
     */
    public static class Invalid extends Error {
      
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
     * Corrupt, missing, or otherwise invalid data was encountered.
     */
    public static class Data extends Error {
      
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
     * Internal failure inside SlateDB or the binding layer.
     */
    public static class Internal extends Error {
      
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

