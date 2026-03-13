package io.slatedb;


import java.util.List;
import java.util.Map;

/**
 * The kind of entry stored in a WAL row.
 */

public enum RowEntryKind {
  VALUE,
  TOMBSTONE,
  MERGE;
}


