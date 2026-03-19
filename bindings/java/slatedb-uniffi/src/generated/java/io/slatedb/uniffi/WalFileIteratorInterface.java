package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Iterator over raw row entries stored in a WAL file.
 */
public interface WalFileIteratorInterface {
    
    /**
     * Returns the next raw row entry from the WAL file.
     */
    public CompletableFuture<RowEntry> next() ;
    
}

