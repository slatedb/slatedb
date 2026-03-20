package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Builder for opening a read-only [`crate::DbReader`].
 *
 * Builders are single-use: calling [`DbReaderBuilder::build`] consumes the builder.
 */
public interface DbReaderBuilderInterface {
    
    /**
     * Opens the reader and consumes this builder.
     */
    public CompletableFuture<DbReader> build() ;
    
    /**
     * Pins the reader to an existing checkpoint UUID string.
     */
    public void withCheckpointId(String checkpointId) throws Error;
    
    /**
     * Installs an application-defined merge operator used while reading merge rows.
     */
    public void withMergeOperator(MergeOperator mergeOperator) throws Error;
    
    /**
     * Applies custom reader options.
     */
    public void withOptions(ReaderOptions options) throws Error;
    
    /**
     * Uses a separate object store for WAL files.
     */
    public void withWalObjectStore(ObjectStore walObjectStore) throws Error;
    
}

