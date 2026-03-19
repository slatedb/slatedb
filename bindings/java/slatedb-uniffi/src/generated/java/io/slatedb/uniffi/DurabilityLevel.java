package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Minimum durability level required for data returned by reads and scans.
 */

public enum DurabilityLevel {
    /**
     * Return only data that has been flushed to remote object storage.
     */
  REMOTE,
    /**
     * Return both remote data and newer in-memory data.
     */
  MEMORY;
}


