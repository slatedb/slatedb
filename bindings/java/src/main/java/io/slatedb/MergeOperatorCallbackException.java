package io.slatedb;



/**
 * Error returned by foreign merge operator callbacks.
 */
public class MergeOperatorCallbackException extends Exception {
    private MergeOperatorCallbackException(String message) {
      super(message); 
    }

    
    /**
     * The merge operator rejected the input or could not produce a merged value.
     */
    public static class Failed extends MergeOperatorCallbackException {
      
        /**
         * The original error message.
         */
      String message;
      public Failed(String message) {
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

