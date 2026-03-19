package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Mutable batch of write operations applied atomically by [`crate::Db::write`].
 *
 * A batch is single-use once submitted to the database.
 */
public interface WriteBatchInterface {
    
    /**
     * Appends a delete operation to the batch.
     */
    public void delete(byte[] key) throws Error;
    
    /**
     * Appends a merge operation to the batch.
     */
    public void merge(byte[] key, byte[] operand) throws Error;
    
    /**
     * Appends a merge operation with custom merge options.
     */
    public void mergeWithOptions(byte[] key, byte[] operand, MergeOptions options) throws Error;
    
    /**
     * Appends a put operation to the batch.
     */
    public void put(byte[] key, byte[] value) throws Error;
    
    /**
     * Appends a put operation with custom put options.
     */
    public void putWithOptions(byte[] key, byte[] value, PutOptions options) throws Error;
    
}

