package io.slatedb;


import java.util.List;
import java.util.Map;

/**
 * Controls which durability level reads are allowed to observe.
 */

public enum DurabilityLevel {
    /**
     * Return only data durable in remote object storage.
     */
  REMOTE,
    /**
     * Return the latest visible data, including in-memory state.
     */
  MEMORY;
}


