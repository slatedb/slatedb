package com.slatedb;

import com.slatedb.exceptions.SlateDBException;
import com.slatedb.exceptions.SlateDBNotFoundException;
import com.slatedb.internal.Native;

import java.lang.foreign.*;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Iterator for traversing key-value pairs in a SlateDB scan operation.
 * 
 * Iterator provides a streaming interface for efficiently traversing
 * large ranges of key-value pairs without loading all data into memory
 * at once. It supports seeking to specific keys and provides standard
 * Java iteration patterns.
 * 
 * <p>Example usage:</p>
 * <pre>{@code
 * try (Iterator iter = db.scan("prefix".getBytes(), null)) {
 *     while (iter.hasNext()) {
 *         KeyValue kv = iter.next();
 *         System.out.println("Key: " + new String(kv.getKey()));
 *         System.out.println("Value: " + new String(kv.getValue()));
 *     }
 * }
 * }</pre>
 * 
 * <p><strong>Important:</strong> Iterator instances must be closed after use
 * to free native resources. Use try-with-resources for automatic cleanup.</p>
 */
public final class Iterator implements AutoCloseable {
    private final MemorySegment handle;
    private final Arena arena;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KeyValue nextItem = null;
    private boolean hasNextCached = false;
    
    /**
     * Creates a new Iterator with the given native handle.
     * 
     * <p><strong>Internal use only.</strong> Iterators are created by
     * scan operations on SlateDB and DbReader instances.</p>
     * 
     * @param handle the native iterator handle
     * @param arena the arena that owns the iterator memory
     */
    Iterator(MemorySegment handle, Arena arena) {
        this.handle = handle;
        this.arena = arena;
    }
    
    /**
     * Creates a new Iterator with the given native handle and no arena management.
     * Used for backward compatibility.
     * 
     * @param handle the native iterator handle
     */
    Iterator(MemorySegment handle) {
        this.handle = handle;
        this.arena = null;
    }
    
    /**
     * Returns true if the iteration has more elements.
     * 
     * @return true if there are more elements, false otherwise
     * @throws SlateDBException if an error occurs during iteration
     */
    public boolean hasNext() throws SlateDBException {
        checkNotClosed();
        
        if (hasNextCached) {
            return nextItem != null;
        }
        
        try {
            nextItem = fetchNext();
            hasNextCached = true;
            return nextItem != null;
        } catch (SlateDBNotFoundException e) {
            // End of iteration
            nextItem = null;
            hasNextCached = true;
            return false;
        }
    }
    
    /**
     * Returns the next element in the iteration.
     * 
     * @return the next KeyValue pair
     * @throws NoSuchElementException if the iteration has no more elements
     * @throws SlateDBException if an error occurs during iteration
     */
    public KeyValue next() throws SlateDBException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        
        KeyValue result = nextItem;
        nextItem = null;
        hasNextCached = false;
        return result;
    }
    
    /**
     * Seeks the iterator to a specific key.
     * 
     * After seeking, the iterator will be positioned at the first key
     * greater than or equal to the specified key within the scan range.
     * 
     * @param key the key to seek to (cannot be null or empty)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void seek(byte[] key) throws SlateDBException {
        checkNotClosed();
        
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment keySegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            
            MemorySegment result = (MemorySegment) Native.slatedb_iterator_seek.invoke(
                    opArena, handle, keySegment, (long) key.length);
            
            Native.checkResult(result);
            
            // Reset cached state after seek
            nextItem = null;
            hasNextCached = false;
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Iterator seek operation failed", e);
        }
    }
    
    /**
     * Closes the iterator and frees native resources.
     * 
     * This method is idempotent and can be safely called multiple times.
     * After calling this method, the iterator should not be used.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try (Arena closeArena = Arena.ofConfined()) {
                MemorySegment result = (MemorySegment) Native.slatedb_iterator_close.invoke(closeArena, handle);
                Native.checkResult(result);
            } catch (Throwable e) {
                // Log but don't throw on close
                System.err.println("Warning: Error closing iterator: " + e.getMessage());
            } finally {
                if (arena != null && arena.scope().isAlive()) {
                    arena.close();
                }
            }
        }
    }
    
    /**
     * Checks if the iterator is closed.
     * 
     * @return true if the iterator is closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }
    
    // Helper methods
    
    private void checkNotClosed() throws SlateDBException {
        if (closed.get()) {
            throw new SlateDBException("Iterator is closed");
        }
    }
    
    private KeyValue fetchNext() throws SlateDBException {
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment kvSegment = opArena.allocate(Native.CSdbKeyValue_LAYOUT);
            
            MemorySegment result = (MemorySegment) Native.slatedb_iterator_next.invoke(
                    opArena, handle, kvSegment);
            
            try {
                Native.checkResult(result);
                
                // Extract key-value from C structure
                MemorySegment keyPtr = kvSegment.get(ValueLayout.ADDRESS, 0);
                long keyLen = kvSegment.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
                MemorySegment valuePtr = kvSegment.get(ValueLayout.ADDRESS, ValueLayout.ADDRESS.byteSize() + 8);
                long valueLen = kvSegment.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize() + 8 + ValueLayout.ADDRESS.byteSize());
                
                if (keyPtr.equals(MemorySegment.NULL) || keyLen == 0) {
                    return null; // End of iteration
                }
                
                byte[] keyBytes = new byte[(int) keyLen];
                if (keyPtr.byteSize() != keyLen && keyPtr.byteSize() == 0 && keyLen > 0) {
                    keyPtr = keyPtr.reinterpret(keyLen);
                }
                MemorySegment.copy(keyPtr, ValueLayout.JAVA_BYTE, 0, keyBytes, 0, keyBytes.length);
                
                byte[] valueBytes = new byte[(int) valueLen];
                if (!valuePtr.equals(MemorySegment.NULL) && valueLen > 0) {
                    if (valuePtr.byteSize() != valueLen && valuePtr.byteSize() == 0 && valueLen > 0) {
                        valuePtr = valuePtr.reinterpret(valueLen);
                    }
                    MemorySegment.copy(valuePtr, ValueLayout.JAVA_BYTE, 0, valueBytes, 0, valueBytes.length);
                }
                
                // Free native memory
                // Note: Go bindings manage KeyValue memory automatically
                
                return new KeyValue(keyBytes, valueBytes);
                
            } catch (SlateDBNotFoundException e) {
                // End of iteration
                throw e;
            }
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Iterator next operation failed", e);
        }
    }
}