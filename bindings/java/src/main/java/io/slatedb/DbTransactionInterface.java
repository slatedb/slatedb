package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A read-write transaction over a [`crate::Db`].
 *
 * Transactions can be read from and written to until they are committed or
 * rolled back. After completion, all further method calls return an error.
 */
public interface DbTransactionInterface {
    
    /**
     * Commit the transaction using default write options.
     *
     * ## Returns
     * - `Result<Option<WriteHandle>, SlatedbError>`: metadata for the committed
     * write, or `None` if the transaction had no writes.
     */
    public CompletableFuture<WriteHandle> commit() ;
    
    /**
     * Commit the transaction using custom write options.
     */
    public CompletableFuture<WriteHandle> commitWithOptions(DbWriteOptions options) ;
    
    /**
     * Buffer a delete inside the transaction.
     */
    public CompletableFuture<Void> delete(byte[] key) ;
    
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
     * Return the unique identifier assigned to this transaction.
     */
    public String id();
    
    /**
     * Explicitly mark keys as read for conflict detection.
     */
    public CompletableFuture<Void> markRead(List<byte[]> keys) ;
    
    /**
     * Buffer a merge inside the transaction using default options.
     */
    public CompletableFuture<Void> merge(byte[] key, byte[] operand) ;
    
    /**
     * Buffer a merge inside the transaction using custom merge options.
     */
    public CompletableFuture<Void> mergeWithOptions(byte[] key, byte[] operand, DbMergeOptions options) ;
    
    /**
     * Buffer a put inside the transaction using default options.
     */
    public CompletableFuture<Void> put(byte[] key, byte[] value) ;
    
    /**
     * Buffer a put inside the transaction using custom put options.
     */
    public CompletableFuture<Void> putWithOptions(byte[] key, byte[] value, DbPutOptions options) ;
    
    /**
     * Roll back the transaction.
     */
    public CompletableFuture<Void> rollback() ;
    
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
     * Return the sequence number visible to this transaction.
     */
    public Long seqnum();
    
    /**
     * Exclude written keys from conflict tracking.
     */
    public CompletableFuture<Void> unmarkWrite(List<byte[]> keys) ;
    
}

