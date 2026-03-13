package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Builder used to configure and open a [`DbReader`].
 */
public interface DbReaderBuilderInterface {
    
    /**
     * Build the configured database reader.
     */
    public CompletableFuture<DbReader> build() ;
    
    /**
     * Set the checkpoint UUID for the reader and validate it immediately.
     */
    public void withCheckpointId(String checkpointId) throws SlatedbException;
    
    /**
     * Set reader options.
     */
    public void withOptions(DbReaderOptions options) throws SlatedbException;
    
}

