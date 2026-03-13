package io.slatedb;


import java.util.List;
import java.util.Map;
/**
 * Time-to-live configuration for put and merge operations.
 */
public sealed interface Ttl {
  
    /**
     * Use the database default TTL behavior.
     */
  record Default() implements Ttl {
    
  }
  
  
    /**
     * Store the value without an expiry.
     */
  record NoExpiry() implements Ttl {
    
  }
  
  
    /**
     * Expire the value after the provided number of clock ticks.
     */
  record ExpireAfterTicks(
    Long v1) implements Ttl {
    
  }
  
}

