package com.slatedb;

import com.slatedb.config.PutOptions;
import com.slatedb.exceptions.SlateDBException;
import com.slatedb.exceptions.SlateDBInvalidArgumentException;
import com.slatedb.internal.Native;

import java.lang.foreign.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A batch of write operations that can be executed atomically.
 * 
 * WriteBatch allows you to group multiple put and delete operations
 * and execute them atomically. This provides better performance for
 * bulk operations and ensures consistency.
 * 
 * <p>Example usage:</p>
 * <pre>{@code
 * try (WriteBatch batch = new WriteBatch()) {
 *     batch.put("key1".getBytes(), "value1".getBytes());
 *     batch.put("key2".getBytes(), "value2".getBytes());
 *     batch.delete("key3".getBytes());
 *     
 *     db.write(batch);
 * }
 * }</pre>
 * 
 * <p><strong>Important:</strong> WriteBatch instances must be closed after use
 * to free native resources. Use try-with-resources for automatic cleanup.</p>
 */
public final class WriteBatch implements AutoCloseable {
    private final MemorySegment handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean consumed = new AtomicBoolean(false);
    
    /**
     * Creates a new WriteBatch.
     * 
     * @throws SlateDBException if the batch cannot be created
     */
    public WriteBatch() throws SlateDBException {
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment batchPtr = opArena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) Native.slatedb_write_batch_new.invoke(opArena, batchPtr);
            Native.checkResult(result);
            this.handle = batchPtr.get(ValueLayout.ADDRESS, 0);
            
            if (this.handle.equals(MemorySegment.NULL)) {
                throw new SlateDBException("Failed to create write batch");
            }
        } catch (Throwable e) {
            throw new SlateDBException("Failed to create write batch", e);
        }
    }
    
    /**
     * Adds a put operation to the batch with default options.
     * 
     * @param key the key (cannot be null or empty)
     * @param value the value (can be null, treated as empty)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void put(byte[] key, byte[] value) throws SlateDBException {
        put(key, value, null);
    }
    
    /**
     * Adds a put operation to the batch with custom put options.
     * 
     * @param key the key (cannot be null or empty)
     * @param value the value (can be null, treated as empty)
     * @param putOptions options for the put operation (can be null for defaults)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void put(byte[] key, byte[] value, PutOptions putOptions) throws SlateDBException {
        checkNotClosed();
        checkNotConsumed();
        validateKey(key);
        
        if (value == null) {
            value = new byte[0];
        }
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment keySegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            MemorySegment valueSegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, value);
            
            MemorySegment result;
            if (putOptions == null) {
                result = (MemorySegment) Native.slatedb_write_batch_put.invoke(
                        opArena, handle, keySegment, (long) key.length, 
                        valueSegment, (long) value.length);
            } else {
                MemorySegment putOptsSegment = createPutOptions(opArena, putOptions);
                result = (MemorySegment) Native.slatedb_write_batch_put_with_options.invoke(
                        opArena, handle, keySegment, (long) key.length, 
                        valueSegment, (long) value.length, putOptsSegment);
            }
            
            Native.checkResult(result);
        } catch (Throwable e) {
            throw new SlateDBException("Batch put operation failed", e);
        }
    }
    
    /**
     * Adds a delete operation to the batch.
     * 
     * @param key the key to delete (cannot be null or empty)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void delete(byte[] key) throws SlateDBException {
        checkNotClosed();
        checkNotConsumed();
        validateKey(key);
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment keySegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            
            MemorySegment result = (MemorySegment) Native.slatedb_write_batch_delete.invoke(
                    opArena, handle, keySegment, (long) key.length);
            
            Native.checkResult(result);
        } catch (Throwable e) {
            throw new SlateDBException("Batch delete operation failed", e);
        }
    }
    
    /**
     * Closes the WriteBatch and frees native resources.
     * 
     * This method is idempotent and can be safely called multiple times.
     * After calling this method, the WriteBatch should not be used.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try (Arena closeArena = Arena.ofConfined()) {
                MemorySegment result = (MemorySegment) Native.slatedb_write_batch_close.invoke(closeArena, handle);
                Native.checkResult(result);
            } catch (Throwable e) {
                // Log but don't throw on close
                System.err.println("Warning: Error closing write batch: " + e.getMessage());
            }
        }
    }
    
    /**
     * Checks if the WriteBatch is closed.
     * 
     * @return true if the batch is closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }
    
    /**
     * Checks if the WriteBatch has been consumed by a write operation.
     * 
     * @return true if the batch has been consumed, false otherwise
     */
    public boolean isConsumed() {
        return consumed.get();
    }
    
    /**
     * Gets the native handle for this WriteBatch.
     * 
     * <p><strong>Internal use only.</strong> This method is used by the
     * SlateDB class to access the native handle for write operations.</p>
     * 
     * @return the native handle
     */
    MemorySegment getHandle() throws SlateDBException {
        checkNotClosed();
        return handle;
    }
    
    /**
     * Marks this batch as consumed.
     * 
     * <p><strong>Internal use only.</strong> This method is called by the
     * SlateDB class after a write operation to prevent reuse of the batch.</p>
     */
    void markConsumed() {
        consumed.set(true);
    }
    
    // Helper methods
    
    private void checkNotClosed() throws SlateDBException {
        if (closed.get()) {
            throw new SlateDBException("WriteBatch is closed");
        }
    }
    
    private void checkNotConsumed() throws SlateDBException {
        if (consumed.get()) {
            throw new SlateDBException("WriteBatch has already been consumed");
        }
    }
    
    private void validateKey(byte[] key) throws SlateDBInvalidArgumentException {
        if (key == null || key.length == 0) {
            throw new SlateDBInvalidArgumentException("Key cannot be null or empty");
        }
    }
    
    private MemorySegment createPutOptions(Arena arena, PutOptions options) {
        MemorySegment segment = arena.allocate(Native.CSdbPutOptions_LAYOUT);
        
        if (options == null) {
            segment.set(ValueLayout.JAVA_INT, 0, 0); // TTLDefault
            segment.set(ValueLayout.JAVA_LONG, 8, 0L);
        } else {
            segment.set(ValueLayout.JAVA_INT, 0, options.getTtlType().getValue());
            segment.set(ValueLayout.JAVA_LONG, 8, options.getTtlValue());
        }
        
        return segment;
    }
}