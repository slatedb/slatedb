package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A stable point-in-time view of a database.
 */
public interface DbSnapshotInterface {
    
    /**
     * Get the value for a key from the snapshot using default read options.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Get the full row metadata for a key from the snapshot using default read options.
     */
    public CompletableFuture<KeyValue> getKeyValue(byte[] key) ;
    
    /**
     * Get the full row metadata for a key from the snapshot using custom read options.
     */
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, DbReadOptions options) ;
    
    /**
     * Get the value for a key from the snapshot using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, DbReadOptions options) ;
    
    /**
     * Scan a key range from the snapshot using default scan options.
     */
    public CompletableFuture<DbIterator> scan(DbKeyRange range) ;
    
    /**
     * Scan all keys with the provided prefix from the snapshot.
     */
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix) ;
    
    /**
     * Scan all keys with the provided prefix from the snapshot using custom scan options.
     */
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, DbScanOptions options) ;
    
    /**
     * Scan a key range from the snapshot using custom scan options.
     */
    public CompletableFuture<DbIterator> scanWithOptions(DbKeyRange range, DbScanOptions options) ;
    
}

