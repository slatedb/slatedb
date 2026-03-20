package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
/**
 * Time-to-live policy applied to an inserted value or merge operand.
 */
public sealed interface Ttl {
  
    /**
     * Use the database default TTL.
     */
  record Default() implements Ttl {
    
  }
  
  
    /**
     * Store the value without expiration.
     */
  record NoExpiry() implements Ttl {
    
  }
  
  
    /**
     * Expire the value after the given number of clock ticks.
     */
  record ExpireAfterTicks(
    Long v1) implements Ttl {
    
  }
  
}

