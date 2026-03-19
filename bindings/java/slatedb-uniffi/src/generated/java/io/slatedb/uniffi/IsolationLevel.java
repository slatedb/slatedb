package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Isolation level used when starting a transaction.
 */

public enum IsolationLevel {
    /**
     * Reads see a stable snapshot without full serializable conflict checking.
     */
  SNAPSHOT,
    /**
     * Reads see a stable snapshot with serializable conflict detection.
     */
  SERIALIZABLE_SNAPSHOT;
}


