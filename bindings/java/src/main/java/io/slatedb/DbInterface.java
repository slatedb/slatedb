package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Handle to an open SlateDB database.
 *
 * Instances of this type are created by [`crate::DbBuilder::build`].
 */
public interface DbInterface {
    
    /**
     * Begin a new transaction at the requested isolation level.
     */
    public CompletableFuture<DbTransaction> begin(IsolationLevel isolationLevel) ;
    
    /**
     * Delete a key using default write options.
     */
    public CompletableFuture<WriteHandle> delete(byte[] key) ;
    
    /**
     * Delete a key using custom write options.
     */
    public CompletableFuture<WriteHandle> deleteWithOptions(byte[] key, DbWriteOptions options) ;
    
    /**
     * Flush in-memory state using the database defaults.
     */
    public CompletableFuture<Void> flush() ;
    
    /**
     * Flush in-memory state using explicit flush options.
     */
    public CompletableFuture<Void> flushWithOptions(DbFlushOptions options) ;
    
    /**
     * Get the value for a key using default read options.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Get the full row metadata for a key using default read options.
     */
    public CompletableFuture<KeyValue> getKeyValue(byte[] key) ;
    
    /**
     * Get the full row metadata for a key using custom read options.
     */
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, DbReadOptions options) ;
    
    /**
     * Get the value for a key using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, DbReadOptions options) ;
    
    /**
     * Merge an operand into a key using default options.
     */
    public CompletableFuture<WriteHandle> merge(byte[] key, byte[] operand) ;
    
    /**
     * Merge an operand into a key using custom merge and write options.
     */
    public CompletableFuture<WriteHandle> mergeWithOptions(byte[] key, byte[] operand, DbMergeOptions mergeOptions, DbWriteOptions writeOptions) ;
    
    /**
     * Put a value for a key using default options.
     *
     * ## Errors
     * - `SlatedbError::Invalid`: if the key is empty or exceeds SlateDB limits.
     */
    public CompletableFuture<WriteHandle> put(byte[] key, byte[] value) ;
    
    /**
     * Put a value for a key using custom put and write options.
     */
    public CompletableFuture<WriteHandle> putWithOptions(byte[] key, byte[] value, DbPutOptions putOptions, DbWriteOptions writeOptions) ;
    
    /**
     * Scan a key range using default scan options.
     */
    public CompletableFuture<DbIterator> scan(DbKeyRange range) ;
    
    /**
     * Scan all keys that share the provided prefix.
     */
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix) ;
    
    /**
     * Scan all keys that share the provided prefix using custom scan options.
     */
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, DbScanOptions options) ;
    
    /**
     * Scan a key range using custom scan options.
     */
    public CompletableFuture<DbIterator> scanWithOptions(DbKeyRange range, DbScanOptions options) ;
    
    /**
     * Close the database.
     */
    public CompletableFuture<Void> shutdown() ;
    
    /**
     * Create a point-in-time snapshot of the database.
     */
    public CompletableFuture<DbSnapshot> snapshot() ;
    
    /**
     * Check whether the database is still open.
     *
     * ## Returns
     * - `Result<(), SlatedbError>`: `Ok(())` if the database is open.
     */
    public void status() throws SlatedbException;
    
    /**
     * Apply a batch of operations atomically using default write options.
     */
    public CompletableFuture<WriteHandle> write(List<DbWriteOperation> operations) ;
    
    /**
     * Apply a batch of operations atomically using custom write options.
     */
    public CompletableFuture<WriteHandle> writeWithOptions(List<DbWriteOperation> operations, DbWriteOptions options) ;
    
}

