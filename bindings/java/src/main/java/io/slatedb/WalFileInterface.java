package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A WAL file handle.
 */
public interface WalFileInterface {
    
    /**
     * Close the WAL file handle.
     */
    public void close() throws SlatedbException;
    
    /**
     * Return this file's WAL ID.
     */
    public Long id();
    
    /**
     * Create an iterator over rows in this WAL file.
     */
    public CompletableFuture<WalFileIterator> iterator() ;
    
    /**
     * Fetch metadata for this WAL file.
     */
    public CompletableFuture<WalFileMetadata> metadata() ;
    
    /**
     * Return a handle for the next WAL file after this one.
     */
    public WalFile nextFile();
    
    /**
     * Return the next WAL ID after this file.
     */
    public Long nextId();
    
}

