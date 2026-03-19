package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Kind of row entry stored in WAL iteration results.
 */

public enum RowEntryKind {
    /**
     * A regular value row.
     */
  VALUE,
    /**
     * A delete tombstone.
     */
  TOMBSTONE,
    /**
     * A merge operand row.
     */
  MERGE;
}


