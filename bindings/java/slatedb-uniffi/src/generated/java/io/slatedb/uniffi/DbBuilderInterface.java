package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Builder for opening a writable [`crate::Db`].
 *
 * Builders are single-use: calling [`DbBuilder::build`] consumes the builder.
 */
public interface DbBuilderInterface {
    
    /**
     * Opens the database and consumes this builder.
     */
    public CompletableFuture<Db> build() ;
    
    /**
     * Disables the SST block and metadata cache.
     */
    public void withDbCacheDisabled() throws Error;
    
    /**
     * Installs an application-defined merge operator.
     */
    public void withMergeOperator(MergeOperator mergeOperator) throws Error;
    
    /**
     * Sets the seed used for SlateDB's internal random number generation.
     */
    public void withSeed(Long seed) throws Error;
    
    /**
     * Applies a [`crate::Settings`] object to the builder.
     */
    public void withSettings(Settings settings) throws Error;
    
    /**
     * Sets the SSTable block size used for newly written tables.
     */
    public void withSstBlockSize(SstBlockSize sstBlockSize) throws Error;
    
    /**
     * Uses a separate object store for WAL files.
     */
    public void withWalObjectStore(ObjectStore walObjectStore) throws Error;
    
}

