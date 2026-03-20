package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Async iterator returned by scan APIs.
 */
public interface DbIteratorInterface {
    
    /**
     * Returns the next key/value pair from the iterator.
     */
    public CompletableFuture<KeyValue> next() ;
    
    /**
     * Seeks the iterator to the first entry at or after `key`.
     */
    public CompletableFuture<Void> seek(byte[] key) ;
    
}

