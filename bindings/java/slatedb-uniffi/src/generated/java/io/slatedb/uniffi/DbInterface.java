package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A writable SlateDB handle.
 */
public interface DbInterface {
    
    /**
     * Starts a transaction at the requested isolation level.
     */
    public CompletableFuture<DbTransaction> begin(IsolationLevel isolationLevel) ;
    
    /**
     * Deletes `key` and returns metadata for the write.
     */
    public CompletableFuture<WriteHandle> delete(byte[] key) ;
    
    /**
     * Deletes `key` using custom write options.
     */
    public CompletableFuture<WriteHandle> deleteWithOptions(byte[] key, WriteOptions options) ;
    
    /**
     * Flushes the default storage layer.
     */
    public CompletableFuture<Void> flush() ;
    
    /**
     * Flushes according to the provided flush options.
     */
    public CompletableFuture<Void> flushWithOptions(FlushOptions options) ;
    
    /**
     * Reads the current value for `key`.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Reads the current row version for `key`, including metadata.
     */
    public CompletableFuture<KeyValue> getKeyValue(byte[] key) ;
    
    /**
     * Reads the current row version for `key` using custom read options.
     */
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options) ;
    
    /**
     * Reads the current value for `key` using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options) ;
    
    /**
     * Appends a merge operand for `key` and returns metadata for the write.
     */
    public CompletableFuture<WriteHandle> merge(byte[] key, byte[] operand) ;
    
    /**
     * Appends a merge operand using custom merge and write options.
     */
    public CompletableFuture<WriteHandle> mergeWithOptions(byte[] key, byte[] operand, MergeOptions mergeOptions, WriteOptions writeOptions) ;
    
    /**
     * Returns a snapshot of the current integer metrics registry.
     */
    public Map<String, Long> metrics() throws Error;
    
    /**
     * Inserts or overwrites a value and returns metadata for the write.
     *
     * Keys must be non-empty and at most `u16::MAX` bytes. Values must be at
     * most `u32::MAX` bytes.
     */
    public CompletableFuture<WriteHandle> put(byte[] key, byte[] value) ;
    
    /**
     * Inserts or overwrites a value using custom put and write options.
     */
    public CompletableFuture<WriteHandle> putWithOptions(byte[] key, byte[] value, PutOptions putOptions, WriteOptions writeOptions) ;
    
    /**
     * Scans rows inside `range`.
     */
    public CompletableFuture<DbIterator> scan(KeyRange range) ;
    
    /**
     * Scans rows whose keys start with `prefix`.
     */
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix) ;
    
    /**
     * Scans rows whose keys start with `prefix` using custom scan options.
     */
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options) ;
    
    /**
     * Scans rows inside `range` using custom scan options.
     */
    public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options) ;
    
    /**
     * Flushes outstanding work and closes the database.
     */
    public CompletableFuture<Void> shutdown() ;
    
    /**
     * Creates a read-only snapshot representing a consistent point in time.
     */
    public CompletableFuture<DbSnapshot> snapshot() ;
    
    /**
     * Returns an error if the database is not currently healthy and open.
     */
    public void status() throws Error;
    
    /**
     * Applies all operations in `batch` atomically.
     *
     * The provided batch is consumed and cannot be reused afterwards.
     */
    public CompletableFuture<WriteHandle> write(WriteBatch batch) ;
    
    /**
     * Applies all operations in `batch` atomically using custom write options.
     *
     * The provided batch is consumed and cannot be reused afterwards.
     */
    public CompletableFuture<WriteHandle> writeWithOptions(WriteBatch batch, WriteOptions options) ;
    
}

