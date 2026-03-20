package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Handle for a single WAL file.
 */
public interface WalFileInterface {
    
    /**
     * Returns the WAL file ID.
     */
    public Long id();
    
    /**
     * Opens an iterator over raw row entries in this WAL file.
     */
    public CompletableFuture<WalFileIterator> iterator() ;
    
    /**
     * Reads object-store metadata for this WAL file.
     */
    public CompletableFuture<WalFileMetadata> metadata() ;
    
    /**
     * Returns a handle for the next WAL file ID without checking existence.
     */
    public WalFile nextFile();
    
    /**
     * Returns the WAL ID immediately after this file.
     */
    public Long nextId();
    
}

