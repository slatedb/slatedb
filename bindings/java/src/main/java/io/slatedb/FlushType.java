package io.slatedb;


import java.util.List;
import java.util.Map;

/**
 * Selects which in-memory structures should be flushed.
 */

public enum FlushType {
    /**
     * Flush the memtable contents.
     */
  MEM_TABLE,
    /**
     * Flush the WAL contents.
     */
  WAL;
}


