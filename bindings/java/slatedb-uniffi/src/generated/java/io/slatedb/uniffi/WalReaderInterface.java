package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Reader for WAL files stored under a database path.
 */
public interface WalReaderInterface {
    
    /**
     * Returns a handle for the WAL file with the given ID.
     */
    public WalFile get(Long id);
    
    /**
     * Lists WAL files in ascending ID order.
     *
     * `start_id` is inclusive and `end_id` is exclusive when provided.
     */
    public CompletableFuture<List<WalFile>> list(Long startId, Long endId) ;
    
}

