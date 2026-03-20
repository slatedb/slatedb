package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Read-only database handle opened by [`crate::DbReaderBuilder`].
 */
public interface DbReaderInterface {
    
    /**
     * Reads the current value for `key`.
     */
    public CompletableFuture<byte[]> get(byte[] key) ;
    
    /**
     * Reads the current value for `key` using custom read options.
     */
    public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options) ;
    
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
     * Closes the reader.
     */
    public CompletableFuture<Void> shutdown() ;
    
}

