package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Builder used to configure and open a [`Db`].
 */
public interface DbBuilderInterface {
    
    /**
     * Open the database using the builder's current configuration.
     *
     * This consumes the builder state. Reusing the same builder after a
     * successful or failed call to `build()` returns an error.
     *
     * ## Returns
     * - `Result<Arc<Db>, SlatedbError>`: the opened database handle.
     *
     * ## Errors
     * - `SlatedbError`: if the builder was already consumed or the database cannot be opened.
     */
    public CompletableFuture<Db> build() ;
    
    /**
     * Disable the database-level cache created by the builder.
     */
    public void withDbCacheDisabled() throws SlatedbException;
    
    /**
     * Configure the merge operator used for merge reads and writes.
     *
     * ## Arguments
     * - `merge_operator`: the callback implementation to use.
     */
    public void withMergeOperator(MergeOperator mergeOperator) throws SlatedbException;
    
    /**
     * Set the random seed used by the database.
     *
     * ## Arguments
     * - `seed`: the seed to use when constructing the database.
     */
    public void withSeed(Long seed) throws SlatedbException;
    
    /**
     * Replace the default database settings with a JSON-encoded [`slatedb::Settings`] document.
     *
     * ## Arguments
     * - `settings_json`: the full settings document encoded as JSON.
     *
     * ## Errors
     * - `SlatedbError::Invalid`: if the JSON cannot be parsed.
     */
    public void withSettingsJson(String settingsJson) throws SlatedbException;
    
    /**
     * Override the SST block size used for new SSTs.
     *
     * ## Arguments
     * - `sst_block_size`: the block size to use.
     */
    public void withSstBlockSize(SstBlockSize sstBlockSize) throws SlatedbException;
    
    /**
     * Configure a separate object store for WAL data.
     *
     * ## Arguments
     * - `wal_object_store`: the object store to use for WAL files.
     */
    public void withWalObjectStore(ObjectStore walObjectStore) throws SlatedbException;
    
}

