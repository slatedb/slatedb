package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Read-only snapshot representing a consistent view of the database.
 */
public interface DbSnapshotInterface {
    
    /**
     * Reads the value visible in this snapshot for `key`.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Reads the row version visible in this snapshot for `key`.
     */
    public CompletableFuture<KeyValue> getKeyValue(byte[] key) ;
    
    /**
     * Reads the row version visible in this snapshot for `key` using custom read options.
     */
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options) ;
    
    /**
     * Reads the value visible in this snapshot for `key` using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options) ;
    
    /**
     * Scans rows inside `range` as of this snapshot.
     */
    public CompletableFuture<DbIterator> scan(KeyRange range) ;
    
    /**
     * Scans rows whose keys start with `prefix` as of this snapshot.
     */
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix) ;
    
    /**
     * Scans rows whose keys start with `prefix` as of this snapshot using custom options.
     */
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options) ;
    
    /**
     * Scans rows inside `range` as of this snapshot using custom scan options.
     */
    public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options) ;
    
}

