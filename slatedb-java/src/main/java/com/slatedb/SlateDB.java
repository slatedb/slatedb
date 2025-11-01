package com.slatedb;

import com.slatedb.config.*;
import com.slatedb.config.AWSConfig;
import com.slatedb.exceptions.SlateDBException;
import com.slatedb.exceptions.SlateDBInvalidArgumentException;
import com.slatedb.internal.Native;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slatedb.internal.SlateDBJacksonModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.*;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main SlateDB database interface.
 * 
 * SlateDB is a high-performance key-value database that provides ACID transactions
 * and is built with Rust for safety and performance. This Java client uses
 * Foreign Function Interface (FFI) for efficient native integration.
 * 
 * <p>Example usage:</p>
 * <pre>{@code
 * // Local storage
 * try (SlateDB db = SlateDB.open("/tmp/mydb", StoreConfig.local(), null)) {
 *     db.put("key".getBytes(), "value".getBytes());
 *     byte[] value = db.get("key".getBytes());
 * }
 * 
 * // AWS S3 storage
 * AWSConfig awsConfig = AWSConfig.builder()
 *     .bucket("my-bucket")
 *     .region("us-east-1")
 *     .build();
 * 
 * try (SlateDB db = SlateDB.open("/tmp/mydb", StoreConfig.aws(awsConfig), null)) {
 *     db.put("key".getBytes(), "value".getBytes());
 *     byte[] value = db.get("key".getBytes());
 * }
 * }</pre>
 */
public final class SlateDB implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SlateDB.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new SlateDBJacksonModule())
            .setPropertyNamingStrategy(com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE);
    
    private final MemorySegment handle;
    private final Arena arena;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    static {
        // Initialize native logging
        try {
            Native.initLogging("info");
        } catch (Throwable e) {
            logger.warn("Failed to initialize native logging", e);
        }
    }
    
    private SlateDB(MemorySegment handle, Arena arena) {
        this.handle = Objects.requireNonNull(handle, "Handle cannot be null");
        this.arena = Objects.requireNonNull(arena, "Arena cannot be null");
    }
    
    /**
     * Opens a SlateDB database with the specified configuration.
     * 
     * @param path the local path for database metadata and WAL files
     * @param storeConfig the object storage provider configuration
     * @param options optional database configuration (can be null for defaults)
     * @return a new SlateDB instance
     * @throws SlateDBException if the database cannot be opened
     * @throws IllegalArgumentException if path or storeConfig is null
     */
    public static SlateDB open(String path, StoreConfig storeConfig, SlateDBOptions options) throws SlateDBException {
        Objects.requireNonNull(path, "Path cannot be null");
        Objects.requireNonNull(storeConfig, "Store config cannot be null");
        
        Arena arena = Arena.ofShared();
        try {
            // Convert configurations to JSON
            MemorySegment pathSegment = arena.allocateFrom(path);
            MemorySegment storeConfigJson = arena.allocateFrom(serializeStoreConfig(storeConfig));
            MemorySegment optionsJson = options != null ? 
                    arena.allocateFrom(serializeOptions(options)) : 
                    MemorySegment.NULL;
            
            // Open database - function returns CSdbHandle struct directly
            MemorySegment handleStruct = (MemorySegment) Native.slatedb_open.invoke(
                    arena, pathSegment, storeConfigJson, optionsJson);
            
            // Extract the actual pointer from the CSdbHandle struct
            MemorySegment handlePtr = handleStruct.get(ValueLayout.ADDRESS, 0);
            
            if (handlePtr.equals(MemorySegment.NULL)) {
                try {
                    if (arena.scope().isAlive()) {
                        arena.close();
                    }
                } catch (Exception closeException) {
                    // Ignore close exceptions
                }
                throw new SlateDBException("Failed to open database - handle pointer is NULL");
            }
            
            return new SlateDB(handleStruct, arena);
            
        } catch (Throwable e) {
            try {
                if (!arena.scope().isAlive()) {
                    // Arena is already closed, don't try to close it again
                } else {
                    arena.close();
                }
            } catch (Exception closeException) {
                // Ignore close exceptions in error handling
            }
            throw new SlateDBException("Failed to open database", e);
        }
    }
    
    /**
     * Stores a key-value pair in the database with default options.
     * 
     * @param key the key (cannot be null or empty)
     * @param value the value (can be null, treated as empty)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void put(byte[] key, byte[] value) throws SlateDBException {
        put(key, value, null, null);
    }
    
    /**
     * Stores a key-value pair in the database with custom options.
     * 
     * @param key the key (cannot be null or empty)
     * @param value the value (can be null, treated as empty)
     * @param putOptions options for the put operation (can be null for defaults)
     * @param writeOptions options for write behavior (can be null for defaults)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void put(byte[] key, byte[] value, PutOptions putOptions, WriteOptions writeOptions) throws SlateDBException {
        checkNotClosed();
        validateKey(key);
        
        if (value == null) {
            value = new byte[0];
        }
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment keySegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            MemorySegment valueSegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, value);
            
            MemorySegment putOptsSegment = createPutOptions(opArena, putOptions);
            MemorySegment writeOptsSegment = createWriteOptions(opArena, writeOptions);
            
            MemorySegment result = (MemorySegment) Native.slatedb_put_with_options.invoke(
                    opArena, handle, keySegment, (long) key.length, 
                    valueSegment, (long) value.length,
                    putOptsSegment, writeOptsSegment);
            
            Native.checkResult(result);
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Put operation failed", e);
        }
    }
    
    /**
     * Retrieves a value by key with default options.
     * 
     * @param key the key to lookup (cannot be null or empty)
     * @return the value as a byte array, or null if not found
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public byte[] get(byte[] key) throws SlateDBException {
        return get(key, null);
    }
    
    /**
     * Retrieves a value by key with custom read options.
     * 
     * @param key the key to lookup (cannot be null or empty)
     * @param readOptions options for read behavior (can be null for defaults)
     * @return the value as a byte array, or null if not found
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public byte[] get(byte[] key, ReadOptions readOptions) throws SlateDBException {
        checkNotClosed();
        validateKey(key);
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment keySegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            MemorySegment readOptsSegment = createReadOptions(opArena, readOptions);
            MemorySegment valueSegment = opArena.allocate(Native.CSdbValue_LAYOUT);
            
            MemorySegment result = (MemorySegment) Native.slatedb_get_with_options.invoke(
                    opArena, handle, keySegment, (long) key.length, readOptsSegment, valueSegment);
            
            try {
                Native.checkResult(result);
                
                // Extract value from C structure
                MemorySegment dataPtr = valueSegment.get(ValueLayout.ADDRESS, 0);
                long dataLen = valueSegment.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
                
                if (dataPtr.equals(MemorySegment.NULL) || dataLen == 0) {
                    return new byte[0];
                }
                
                // Check if the data segment has the expected size
                if (dataPtr.byteSize() != dataLen) {
                    logger.warn("Data segment size mismatch: expected {}, actual {}", dataLen, dataPtr.byteSize());
                    if (dataPtr.byteSize() == 0) {
                        // The native library returned a valid pointer but zero-sized segment
                        // This likely means we need to create a properly sized segment from the pointer
                        if (dataLen > 0) {
                            dataPtr = dataPtr.reinterpret(dataLen);
                        } else {
                            return new byte[0];
                        }
                    }
                }
                
                byte[] resultBytes = new byte[(int) dataLen];
                MemorySegment.copy(dataPtr, ValueLayout.JAVA_BYTE, 0, resultBytes, 0, resultBytes.length);
                
                // Free native memory
                Native.slatedb_free_value.invoke(valueSegment);
                
                return resultBytes;
                
            } catch (SlateDBException e) {
                if (e instanceof com.slatedb.exceptions.SlateDBNotFoundException) {
                    return null;
                }
                throw e;
            }
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Get operation failed", e);
        }
    }
    
    /**
     * Deletes a key from the database with default options.
     * 
     * @param key the key to delete (cannot be null or empty)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void delete(byte[] key) throws SlateDBException {
        delete(key, null);
    }
    
    /**
     * Deletes a key from the database with custom write options.
     * 
     * @param key the key to delete (cannot be null or empty)
     * @param writeOptions options for write behavior (can be null for defaults)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if key is null or empty
     */
    public void delete(byte[] key, WriteOptions writeOptions) throws SlateDBException {
        checkNotClosed();
        validateKey(key);
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment keySegment = opArena.allocateFrom(ValueLayout.JAVA_BYTE, key);
            MemorySegment writeOptsSegment = createWriteOptions(opArena, writeOptions);
            
            MemorySegment result = (MemorySegment) Native.slatedb_delete_with_options.invoke(
                    opArena, handle, keySegment, (long) key.length, writeOptsSegment);
            
            Native.checkResult(result);
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Delete operation failed", e);
        }
    }
    
    /**
     * Writes a batch of operations atomically with default write options.
     * 
     * @param batch the write batch to execute
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if batch is null
     */
    public void write(WriteBatch batch) throws SlateDBException {
        write(batch, null);
    }
    
    /**
     * Writes a batch of operations atomically with custom write options.
     * 
     * @param batch the write batch to execute
     * @param writeOptions options for write behavior (can be null for defaults)
     * @throws SlateDBException if the operation fails
     * @throws IllegalArgumentException if batch is null
     */
    public void write(WriteBatch batch, WriteOptions writeOptions) throws SlateDBException {
        checkNotClosed();
        Objects.requireNonNull(batch, "Batch cannot be null");
        
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment writeOptsSegment = createWriteOptions(opArena, writeOptions);
            
            MemorySegment result = (MemorySegment) Native.slatedb_write_batch_write.invoke(
                    opArena, handle, batch.getHandle(), writeOptsSegment);
            
            Native.checkResult(result);
            
            // Mark batch as consumed to prevent reuse
            batch.markConsumed();
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Write batch operation failed", e);
        }
    }
    
    /**
     * Creates an iterator for scanning a key range with default options.
     * 
     * @param start the start key (inclusive, can be null for beginning)
     * @param end the end key (exclusive, can be null for end)
     * @return a new iterator instance
     * @throws SlateDBException if the operation fails
     */
    public Iterator scan(byte[] start, byte[] end) throws SlateDBException {
        return scan(start, end, null);
    }
    
    /**
     * Creates an iterator for scanning a key range with custom options.
     * 
     * @param start the start key (inclusive, can be null for beginning)
     * @param end the end key (exclusive, can be null for end)
     * @param scanOptions options for scan behavior (can be null for defaults)
     * @return a new iterator instance
     * @throws SlateDBException if the operation fails
     */
    public Iterator scan(byte[] start, byte[] end, ScanOptions scanOptions) throws SlateDBException {
        checkNotClosed();
        
        // Create a shared Arena that the iterator will own and close
        Arena iterArena = Arena.ofShared();
        try (Arena opArena = Arena.ofConfined()) {
            MemorySegment startSegment = start != null ? 
                    opArena.allocateFrom(ValueLayout.JAVA_BYTE, start) : 
                    MemorySegment.NULL;
            long startLen = start != null ? start.length : 0;
            
            MemorySegment endSegment = end != null ? 
                    opArena.allocateFrom(ValueLayout.JAVA_BYTE, end) : 
                    MemorySegment.NULL;
            long endLen = end != null ? end.length : 0;
            
            MemorySegment scanOptsSegment = createScanOptions(opArena, scanOptions);
            MemorySegment iteratorSegment = iterArena.allocate(ValueLayout.ADDRESS);
            
            MemorySegment result = (MemorySegment) Native.slatedb_scan_with_options.invoke(
                    opArena, handle, startSegment, startLen, endSegment, endLen, 
                    scanOptsSegment, iteratorSegment);
            
            Native.checkResult(result);
            
            // Get the actual iterator handle
            MemorySegment iterHandle = iteratorSegment.get(ValueLayout.ADDRESS, 0);
            
            return new Iterator(iterHandle, iterArena);
        } catch (SlateDBException e) {
            iterArena.close();
            throw e;
        } catch (Throwable e) {
            iterArena.close();
            throw new SlateDBException("Scan operation failed", e);
        }
    }
    
    /**
     * Flushes in-memory writes to persistent storage.
     * 
     * This ensures all pending data is durably written to object storage.
     * Call this before opening a DbReader if you need to read recently written data.
     * 
     * @throws SlateDBException if the operation fails
     */
    public void flush() throws SlateDBException {
        checkNotClosed();
        
        try (Arena flushArena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) Native.slatedb_flush.invoke(flushArena, handle);
            Native.checkResult(result);
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Flush operation failed", e);
        }
    }
    
    /**
     * Closes the database and releases all resources.
     * 
     * After calling this method, the database instance should not be used.
     * This method is idempotent and can be safely called multiple times.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                try (Arena closeArena = Arena.ofConfined()) {
                    MemorySegment result = (MemorySegment) Native.slatedb_close.invoke(closeArena, handle);
                    Native.checkResult(result);
                }
            } catch (Throwable e) {
                logger.warn("Error closing database", e);
            } finally {
                arena.close();
            }
        }
    }
    
    /**
     * Checks if the database is closed.
     * 
     * @return true if the database is closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }
    
    // Helper methods
    
    private void checkNotClosed() throws SlateDBException {
        if (closed.get()) {
            throw new SlateDBException("Database is closed");
        }
    }
    
    private void validateKey(byte[] key) throws SlateDBInvalidArgumentException {
        if (key == null || key.length == 0) {
            throw new SlateDBInvalidArgumentException("Key cannot be null or empty");
        }
    }
    
    private static String serializeStoreConfig(StoreConfig config) throws SlateDBException {
        try {
            // Create the JSON manually since Jackson mixins aren't working with @JsonIgnore
            if (config.getProvider() == com.slatedb.config.Provider.LOCAL) {
                return "{\"provider\":\"local\"}";
            } else if (config.getProvider() == com.slatedb.config.Provider.AWS) {
                AWSConfig awsConfig = config.getAws();
                StringBuilder jsonBuilder = new StringBuilder();
                jsonBuilder.append("{\"provider\":\"aws\",\"aws\":{");
                
                // Add bucket
                jsonBuilder.append("\"bucket\":").append(awsConfig.getBucket() != null ? "\"" + awsConfig.getBucket() + "\"" : "null");
                
                // Add region
                jsonBuilder.append(",\"region\":").append(awsConfig.getRegion() != null ? "\"" + awsConfig.getRegion() + "\"" : "null");
                
                // Add endpoint
                jsonBuilder.append(",\"endpoint\":").append(awsConfig.getEndpoint() != null ? "\"" + awsConfig.getEndpoint() + "\"" : "null");
                
                // Add request_timeout 
                jsonBuilder.append(",\"request_timeout\":").append(awsConfig.getRequestTimeout() != null ? "\"" + awsConfig.getRequestTimeout().toString() + "\"" : "null");
                
                jsonBuilder.append("}}");
                
                String json = jsonBuilder.toString();
                logger.debug("StoreConfig JSON (for Go bindings): {}", json);
                return json;
            }
            
            throw new SlateDBException("Unsupported store provider: " + config.getProvider());
        } catch (SlateDBException e) {
            throw e;
        } catch (Throwable e) {
            throw new SlateDBException("Failed to serialize store configuration", e);
        }
    }
    
    private static String serializeOptions(SlateDBOptions options) throws SlateDBException {
        try {
            return objectMapper.writeValueAsString(options);
        } catch (Throwable e) {
            throw new SlateDBException("Failed to serialize database options", e);
        }
    }
    
    private MemorySegment createWriteOptions(Arena arena, WriteOptions options) {
        if (options == null) {
            options = WriteOptions.defaultOptions();
        }
        
        MemorySegment segment = arena.allocate(Native.CSdbWriteOptions_LAYOUT);
        segment.set(ValueLayout.JAVA_BOOLEAN, 0, options.isAwaitDurable());
        return segment;
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
    
    private MemorySegment createReadOptions(Arena arena, ReadOptions options) {
        if (options == null) {
            options = ReadOptions.defaultOptions();
        }
        
        MemorySegment segment = arena.allocate(Native.CSdbReadOptions_LAYOUT);
        segment.set(ValueLayout.JAVA_INT, 0, options.getDurabilityFilter().getValue());
        segment.set(ValueLayout.JAVA_BOOLEAN, 4, options.isDirty());
        return segment;
    }
    
    private MemorySegment createScanOptions(Arena arena, ScanOptions options) {
        if (options == null) {
            options = ScanOptions.defaultOptions();
        }
        
        MemorySegment segment = arena.allocate(Native.CSdbScanOptions_LAYOUT);
        segment.set(ValueLayout.JAVA_INT, 0, options.getDurabilityFilter().getValue());
        segment.set(ValueLayout.JAVA_BOOLEAN, 4, options.isDirty());
        segment.set(ValueLayout.JAVA_LONG, 8, options.getReadAheadBytes());
        segment.set(ValueLayout.JAVA_BOOLEAN, 16, options.isCacheBlocks());
        segment.set(ValueLayout.JAVA_LONG, 24, options.getMaxFetchTasks()); // Changed from 20 to 24 for proper alignment
        return segment;
    }
}