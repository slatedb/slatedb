package io.slatedb;


import java.util.List;
import java.util.Map;
/**
 * A single operation in a batch write.
 */
public sealed interface DbWriteOperation {
  
    /**
     * Put a value for a key.
     */
  record Put(
        /**
         * The key to write.
         */
    byte[] key, 
        /**
         * The value to write.
         */
    byte[] valueBytes, 
        /**
         * Per-operation put options.
         */
    DbPutOptions options) implements DbWriteOperation {
    
  }
  
    /**
     * Merge an operand into a key.
     */
  record Merge(
        /**
         * The key to merge into.
         */
    byte[] key, 
        /**
         * The merge operand.
         */
    byte[] operand, 
        /**
         * Per-operation merge options.
         */
    DbMergeOptions options) implements DbWriteOperation {
    
  }
  
    /**
     * Delete a key.
     */
  record Delete(
        /**
         * The key to delete.
         */
    byte[] key) implements DbWriteOperation {
    
  }
  
}

