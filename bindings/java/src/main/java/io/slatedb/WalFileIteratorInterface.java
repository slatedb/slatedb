package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * An iterator over rows in a WAL file.
 */
public interface WalFileIteratorInterface {
    
    /**
     * Close the WAL iterator.
     */
    public void close() throws SlatedbException;
    
    /**
     * Return the next WAL entry, or `None` when the iterator is exhausted.
     */
    public CompletableFuture<RowEntry> next() ;
    
}

