package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * A mutable batch of write operations that can be written atomically.
 */
public interface WriteBatchInterface {
    
    /**
     * Explicitly close the batch handle.
     */
    public void close() throws SlatedbException;
    
    /**
     * Append a delete operation.
     */
    public void delete(byte[] key) throws SlatedbException;
    
    /**
     * Append a merge operation using default merge options.
     */
    public void merge(byte[] key, byte[] operand) throws SlatedbException;
    
    /**
     * Append a merge operation using explicit merge options.
     */
    public void mergeWithOptions(byte[] key, byte[] operand, DbMergeOptions options) throws SlatedbException;
    
    /**
     * Append a put operation using default put options.
     */
    public void put(byte[] key, byte[] value) throws SlatedbException;
    
    /**
     * Append a put operation using explicit put options.
     */
    public void putWithOptions(byte[] key, byte[] value, DbPutOptions options) throws SlatedbException;
    
}

