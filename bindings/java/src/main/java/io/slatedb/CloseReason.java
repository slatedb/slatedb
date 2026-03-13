package io.slatedb;


import java.util.List;
import java.util.Map;

/**
 * The reason a database handle was closed.
 */

public enum CloseReason {
    /**
     * No close reason is available.
     */
  NONE,
    /**
     * The database was closed cleanly.
     */
  CLEAN,
    /**
     * The database instance was fenced by another writer.
     */
  FENCED,
    /**
     * A background task panicked.
     */
  PANIC,
    /**
     * The close reason was not recognized by this binding version.
     */
  UNKNOWN;
}


