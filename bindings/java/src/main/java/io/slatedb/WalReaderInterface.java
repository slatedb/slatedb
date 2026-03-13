package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A WAL reader scoped to a single database path and object store.
 */
public interface WalReaderInterface {
    
    /**
     * Close the WAL reader.
     */
    public void close() throws SlatedbException;
    
    /**
     * Return a handle for a specific WAL file ID.
     */
    public WalFile get(Long id);
    
    /**
     * List WAL files in ascending ID order.
     */
    public CompletableFuture<List<WalFile>> list(Long startId, Long endId) ;
    
}

