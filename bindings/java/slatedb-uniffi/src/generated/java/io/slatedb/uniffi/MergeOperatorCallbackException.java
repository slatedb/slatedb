package io.slatedb.uniffi;



/**
 * Error returned by a foreign [`crate::MergeOperator`] implementation.
 */
public class MergeOperatorCallbackException extends Exception {
    private MergeOperatorCallbackException(String message) {
      super(message); 
    }

    
    /**
     * The merge callback failed with an application-defined message.
     */
    public static class Failed extends MergeOperatorCallbackException {
      
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

