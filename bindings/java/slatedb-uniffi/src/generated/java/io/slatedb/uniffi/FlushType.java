package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;

/**
 * Storage layer targeted by an explicit flush.
 */

public enum FlushType {
    /**
     * Flush the active memtable and any immutable memtables to object storage.
     */
  MEM_TABLE,
    /**
     * Flush the active WAL and any immutable WAL segments to object storage.
     */
  WAL;
}


