package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Transaction handle returned by [`crate::Db::begin`].
 *
 * A transaction becomes unusable after `commit`, `commit_with_options`, or
 * `rollback`.
 */
public interface DbTransactionInterface {
    
    /**
     * Commits the transaction.
     *
     * Returns `None` when the transaction performed no writes.
     */
    public CompletableFuture<WriteHandle> commit() ;
    
    /**
     * Commits the transaction using custom write options.
     *
     * Returns `None` when the transaction performed no writes.
     */
    public CompletableFuture<WriteHandle> commitWithOptions(WriteOptions options) ;
    
    /**
     * Buffers a delete inside the transaction.
     */
    public CompletableFuture<Void> delete(byte[] key) ;
    
    /**
     * Reads the value visible to this transaction for `key`.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Reads the row version visible to this transaction for `key`.
     */
    public CompletableFuture<KeyValue> getKeyValue(byte[] key) ;
    
    /**
     * Reads the row version visible to this transaction for `key` using custom options.
     */
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options) ;
    
    /**
     * Reads the value visible to this transaction for `key` using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options) ;
    
    /**
     * Returns the transaction identifier as a UUID string.
     */
    public String id();
    
    /**
     * Marks keys as read for conflict detection.
     */
    public CompletableFuture<Void> markRead(List<byte[]> keys) ;
    
    /**
     * Buffers a merge operand inside the transaction.
     */
    public CompletableFuture<Void> merge(byte[] key, byte[] operand) ;
    
    /**
     * Buffers a merge operand inside the transaction using custom merge options.
     */
    public CompletableFuture<Void> mergeWithOptions(byte[] key, byte[] operand, MergeOptions options) ;
    
    /**
     * Buffers a put inside the transaction.
     */
    public CompletableFuture<Void> put(byte[] key, byte[] value) ;
    
    /**
     * Buffers a put inside the transaction using custom put options.
     */
    public CompletableFuture<Void> putWithOptions(byte[] key, byte[] value, PutOptions options) ;
    
    /**
     * Rolls back the transaction and marks it completed.
     */
    public CompletableFuture<Void> rollback() ;
    
    /**
     * Scans rows inside `range` as visible to this transaction.
     */
    public CompletableFuture<DbIterator> scan(KeyRange range) ;
    
    /**
     * Scans rows whose keys start with `prefix` as visible to this transaction.
     */
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix) ;
    
    /**
     * Scans rows whose keys start with `prefix` as visible to this transaction using custom options.
     */
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options) ;
    
    /**
     * Scans rows inside `range` as visible to this transaction using custom options.
     */
    public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options) ;
    
    /**
     * Returns the sequence number assigned when the transaction started.
     */
    public Long seqnum();
    
    /**
     * Excludes written keys from transaction conflict detection.
     */
    public CompletableFuture<Void> unmarkWrite(List<byte[]> keys) ;
    
}

