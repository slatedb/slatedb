package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * An asynchronous iterator over key-value pairs.
 *
 * Instances of this type are returned by scan operations on [`crate::Db`],
 * [`crate::DbSnapshot`], and [`crate::DbTransaction`].
 */
public interface DbIteratorInterface {
    
    /**
     * Return the next item from the iterator.
     *
     * ## Returns
     * - `Result<Option<KeyValue>, SlatedbError>`: the next key-value pair, or
     * `None` when the iterator is exhausted.
     */
    public CompletableFuture<KeyValue> next() ;
    
    /**
     * Reposition the iterator to the first key greater than or equal to `key`.
     *
     * ## Arguments
     * - `key`: the key to seek to within the iterator's range.
     *
     * ## Errors
     * - `SlatedbError::Invalid`: if `key` is empty.
     * - `SlatedbError`: if the key falls outside the iterator's valid range.
     */
    public CompletableFuture<Void> seek(byte[] key) ;
    
}

