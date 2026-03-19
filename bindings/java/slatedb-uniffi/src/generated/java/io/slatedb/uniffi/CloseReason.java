package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Reason a database or reader reports itself as closed.
 */

public enum CloseReason {
    /**
     * Closed cleanly by the caller.
     */
  CLEAN,
    /**
     * Closed because another writer fenced this instance.
     */
  FENCED,
    /**
     * Closed because of a panic in a background task.
     */
  PANIC,
    /**
     * Closed for a reason not modeled explicitly by this binding.
     */
  UNKNOWN;
}


