package io.slatedb;


import java.util.List;
import java.util.Map;

/**
 * Isolation level used when starting a transaction.
 */

public enum IsolationLevel {
    /**
     * Snapshot isolation.
     */
  SNAPSHOT,
    /**
     * Serializable snapshot isolation.
     */
  SERIALIZABLE_SNAPSHOT;
}


