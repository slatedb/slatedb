package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A read-only database reader.
 */
public interface DbReaderInterface {
    
    /**
     * Close the reader.
     */
    public CompletableFuture<Void> close() ;
    
    /**
     * Get the value for a key using default read options.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Get the value for a key using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, DbReadOptions options) ;
    
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
    
}

