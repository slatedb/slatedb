package com.slatedb.internal;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

/**
 * Native FFI integration layer for SlateDB using Go bindings C API
 * 
 * This class provides Java FFI method handles for all SlateDB operations
 * using the proven Go bindings native library.
 */
public final class Native {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBRARY_LOOKUP;
    
    // Memory layouts for C structures
    public static final MemoryLayout CSdbResult_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("error"),
        MemoryLayout.paddingLayout(4), // Padding for 8-byte alignment on 64-bit systems
        ValueLayout.ADDRESS.withName("message")
    ).withName("CSdbResult");
    
    public static final MemoryLayout CSdbHandle_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("_0")
    ).withName("CSdbHandle");
    
    public static final MemoryLayout CSdbValue_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("data"),
        ValueLayout.JAVA_LONG.withName("len")
    ).withName("CSdbValue");
    
    public static final MemoryLayout CSdbKeyValue_LAYOUT = MemoryLayout.structLayout(
        CSdbValue_LAYOUT.withName("key"),
        CSdbValue_LAYOUT.withName("value")
    ).withName("CSdbKeyValue");
    
    public static final MemoryLayout CSdbPutOptions_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("ttl_type"),
        MemoryLayout.paddingLayout(4), // Padding for 8-byte alignment
        ValueLayout.JAVA_LONG.withName("ttl_value")
    ).withName("CSdbPutOptions");
    
    public static final MemoryLayout CSdbWriteOptions_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_BOOLEAN.withName("await_durable")
    ).withName("CSdbWriteOptions");
    
    public static final MemoryLayout CSdbReadOptions_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("durability_filter"),
        ValueLayout.JAVA_BOOLEAN.withName("dirty")
    ).withName("CSdbReadOptions");
    
    public static final MemoryLayout CSdbScanOptions_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("durability_filter"),
        ValueLayout.JAVA_BOOLEAN.withName("dirty"),
        MemoryLayout.paddingLayout(3), // Padding after boolean to align long to 8 bytes
        ValueLayout.JAVA_LONG.withName("read_ahead_bytes"),
        ValueLayout.JAVA_BOOLEAN.withName("cache_blocks"),
        MemoryLayout.paddingLayout(7), // Padding after boolean to align long to 8 bytes  
        ValueLayout.JAVA_LONG.withName("max_fetch_tasks")
    ).withName("CSdbScanOptions");
    
    // VarHandles for accessing struct fields
    public static final VarHandle CSdbResult_error = CSdbResult_LAYOUT.varHandle(
        MemoryLayout.PathElement.groupElement("error"));
    public static final VarHandle CSdbResult_message = CSdbResult_LAYOUT.varHandle(
        MemoryLayout.PathElement.groupElement("message"));
        
    public static final VarHandle CSdbHandle_ptr = CSdbHandle_LAYOUT.varHandle(
        MemoryLayout.PathElement.groupElement("_0"));
        
    public static final VarHandle CSdbValue_data = CSdbValue_LAYOUT.varHandle(
        MemoryLayout.PathElement.groupElement("data"));
    public static final VarHandle CSdbValue_len = CSdbValue_LAYOUT.varHandle(
        MemoryLayout.PathElement.groupElement("len"));
    
    // Method handles for Go bindings C functions
    public static final MethodHandle slatedb_init_logging;
    public static final MethodHandle slatedb_open;
    public static final MethodHandle slatedb_close;
    public static final MethodHandle slatedb_put_with_options;
    public static final MethodHandle slatedb_get_with_options;
    public static final MethodHandle slatedb_delete_with_options;
    public static final MethodHandle slatedb_flush;
    public static final MethodHandle slatedb_scan_with_options;
    
    // WriteBatch operations
    public static final MethodHandle slatedb_write_batch_new;
    public static final MethodHandle slatedb_write_batch_put;
    public static final MethodHandle slatedb_write_batch_put_with_options;
    public static final MethodHandle slatedb_write_batch_delete;
    public static final MethodHandle slatedb_write_batch_write;
    public static final MethodHandle slatedb_write_batch_close;
    
    // Iterator operations
    public static final MethodHandle slatedb_iterator_next;
    public static final MethodHandle slatedb_iterator_seek;
    public static final MethodHandle slatedb_iterator_close;
    
    // Memory management
    public static final MethodHandle slatedb_free_result;
    public static final MethodHandle slatedb_free_value;
    
    static {
        try {
            // Load native library
            String libraryPath = findNativeLibrary();
            System.load(libraryPath);
            
            // Get symbol lookup for the library
            LIBRARY_LOOKUP = SymbolLookup.libraryLookup(libraryPath, Arena.global());
            
            // Initialize method handles
            slatedb_init_logging = findFunction("slatedb_init_logging",
                FunctionDescriptor.of(CSdbResult_LAYOUT, ValueLayout.ADDRESS),
                true);
                
            slatedb_open = findFunction("slatedb_open", 
                FunctionDescriptor.of(CSdbHandle_LAYOUT,
                    ValueLayout.ADDRESS,  // path
                    ValueLayout.ADDRESS,  // store_config_json  
                    ValueLayout.ADDRESS   // options_json
                ),
                true);
                
            slatedb_close = findFunction("slatedb_close",
                FunctionDescriptor.of(CSdbResult_LAYOUT, CSdbHandle_LAYOUT),
                true);
                
            slatedb_put_with_options = findFunction("slatedb_put_with_options",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    CSdbHandle_LAYOUT,      // handle
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG,  // key_len
                    ValueLayout.ADDRESS,    // value
                    ValueLayout.JAVA_LONG,  // value_len
                    ValueLayout.ADDRESS,    // put_options
                    ValueLayout.ADDRESS     // write_options
                ),
                true);
                
            slatedb_get_with_options = findFunction("slatedb_get_with_options",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    CSdbHandle_LAYOUT,      // handle
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG,  // key_len
                    ValueLayout.ADDRESS,    // read_options
                    ValueLayout.ADDRESS     // value_out
                ),
                true);
                
            slatedb_delete_with_options = findFunction("slatedb_delete_with_options",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    CSdbHandle_LAYOUT,      // handle
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG,  // key_len
                    ValueLayout.ADDRESS     // write_options
                ),
                true);
                
            slatedb_flush = findFunction("slatedb_flush",
                FunctionDescriptor.of(CSdbResult_LAYOUT, CSdbHandle_LAYOUT),
                true);
                
            slatedb_scan_with_options = findFunction("slatedb_scan_with_options",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    CSdbHandle_LAYOUT,      // handle
                    ValueLayout.ADDRESS,    // start
                    ValueLayout.JAVA_LONG,  // start_len
                    ValueLayout.ADDRESS,    // end
                    ValueLayout.JAVA_LONG,  // end_len
                    ValueLayout.ADDRESS,    // options
                    ValueLayout.ADDRESS     // iter_out
                ),
                true);
                
            // WriteBatch functions
            slatedb_write_batch_new = findFunction("slatedb_write_batch_new",
                FunctionDescriptor.of(CSdbResult_LAYOUT, ValueLayout.ADDRESS),
                true);
                
            slatedb_write_batch_put = findFunction("slatedb_write_batch_put",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    ValueLayout.ADDRESS,    // batch
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG,  // key_len
                    ValueLayout.ADDRESS,    // value
                    ValueLayout.JAVA_LONG   // value_len
                ),
                true);
                
            slatedb_write_batch_put_with_options = findFunction("slatedb_write_batch_put_with_options",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    ValueLayout.ADDRESS,    // batch
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG,  // key_len
                    ValueLayout.ADDRESS,    // value
                    ValueLayout.JAVA_LONG,  // value_len
                    ValueLayout.ADDRESS     // options
                ),
                true);
                
            slatedb_write_batch_delete = findFunction("slatedb_write_batch_delete",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    ValueLayout.ADDRESS,    // batch
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG   // key_len
                ),
                true);
                
            slatedb_write_batch_write = findFunction("slatedb_write_batch_write",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    CSdbHandle_LAYOUT,      // handle
                    ValueLayout.ADDRESS,    // batch
                    ValueLayout.ADDRESS     // options
                ),
                true);
                
            slatedb_write_batch_close = findFunction("slatedb_write_batch_close",
                FunctionDescriptor.of(CSdbResult_LAYOUT, ValueLayout.ADDRESS),
                true);
                
            // Iterator functions
            slatedb_iterator_next = findFunction("slatedb_iterator_next",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    ValueLayout.ADDRESS,    // iter
                    ValueLayout.ADDRESS     // kv_out
                ),
                true);
                
            slatedb_iterator_seek = findFunction("slatedb_iterator_seek",
                FunctionDescriptor.of(CSdbResult_LAYOUT,
                    ValueLayout.ADDRESS,    // iter
                    ValueLayout.ADDRESS,    // key
                    ValueLayout.JAVA_LONG   // key_len
                ),
                true);
                
            slatedb_iterator_close = findFunction("slatedb_iterator_close",
                FunctionDescriptor.of(CSdbResult_LAYOUT, ValueLayout.ADDRESS),
                true);
                
            // Memory management functions
            slatedb_free_result = findFunction("slatedb_free_result",
                FunctionDescriptor.ofVoid(CSdbResult_LAYOUT));
                
            slatedb_free_value = findFunction("slatedb_free_value",
                FunctionDescriptor.ofVoid(CSdbValue_LAYOUT));
                
        } catch (Exception e) {
            throw new ExceptionInInitializerError("Failed to initialize SlateDB native library: " + e.getMessage());
        }
    }
    
    private static MethodHandle findFunction(String name, FunctionDescriptor descriptor) {
        return findFunction(name, descriptor, false);
    }
    
    private static MethodHandle findFunction(String name, FunctionDescriptor descriptor, boolean needsAllocator) {
        var handle = LIBRARY_LOOKUP.find(name)
            .map(addr -> LINKER.downcallHandle(addr, descriptor))
            .orElseThrow(() -> new UnsatisfiedLinkError("Cannot find native function: " + name));
        
        if (needsAllocator) {
            // Bind the allocator for functions that need it
            return handle;
        }
        return handle;
    }
    
    private static String findNativeLibrary() {
        String osName = System.getProperty("os.name").toLowerCase();
        String libName;
        
        if (osName.contains("win")) {
            libName = "slatedb_go.dll";
        } else if (osName.contains("mac")) {
            libName = "libslatedb_go.dylib";
        } else {
            libName = "libslatedb_go.so";
        }
        
        // Try to find library in resources
        String resourcePath = "/native/" + libName;
        var resource = Native.class.getResourceAsStream(resourcePath);
        if (resource != null) {
            try {
                // Extract to temp file
                var tempFile = java.nio.file.Files.createTempFile("slatedb_go", 
                    osName.contains("win") ? ".dll" : osName.contains("mac") ? ".dylib" : ".so");
                java.nio.file.Files.copy(resource, tempFile, 
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                tempFile.toFile().deleteOnExit();
                return tempFile.toString();
            } catch (Exception e) {
                throw new RuntimeException("Failed to extract native library: " + e.getMessage(), e);
            }
        }
        
        throw new UnsatisfiedLinkError("Native library not found: " + libName);
    }
    
    /**
     * Initialize logging for SlateDB
     * @param level Log level (trace, debug, info, warn, error) or null for default
     */
    public static void initLogging(String level) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment levelPtr = MemorySegment.NULL;
            if (level != null) {
                levelPtr = arena.allocateFrom(level);
            }
            
            MemorySegment result = (MemorySegment) slatedb_init_logging.invoke(arena, levelPtr);
            int errorCode = (int) CSdbResult_error.get(result, 0);
            
            if (errorCode != 0) {
                MemorySegment messagePtr = (MemorySegment) CSdbResult_message.get(result, 0);
                String message = messagePtr.equals(MemorySegment.NULL) ? "Unknown error" : 
                    messagePtr.getString(0);
                throw new RuntimeException("Failed to initialize logging: " + message);
            }
        } catch (Throwable e) {
            throw new RuntimeException("Failed to call slatedb_init_logging", e);
        }
    }
    
    /**
     * Check a CSdbResult and throw exception if there's an error
     */
    public static void checkResult(MemorySegment result) throws com.slatedb.exceptions.SlateDBException {
        int errorCode = (int) CSdbResult_error.get(result, 0);
        
        if (errorCode != 0) {
            MemorySegment messagePtr = (MemorySegment) CSdbResult_message.get(result, 0);
            String message = messagePtr.equals(MemorySegment.NULL) || messagePtr.byteSize() == 0 ? "Unknown error" : 
                messagePtr.getString(0);
            
            try {
                // Free the result message
                slatedb_free_result.invoke(result);
            } catch (Throwable ignore) {
                // Best effort cleanup
            }
            
            // Map error codes to appropriate exceptions
            switch (errorCode) {
                case 1: // InvalidArgument
                    throw new com.slatedb.exceptions.SlateDBInvalidArgumentException(message);
                case 2: // NotFound  
                    throw new com.slatedb.exceptions.SlateDBNotFoundException(message);
                case 4: // IOError
                    throw new com.slatedb.exceptions.SlateDBIOException(message);
                case 5: // InternalError
                    throw new com.slatedb.exceptions.SlateDBInternalException(message);
                default:
                    throw new com.slatedb.exceptions.SlateDBException(message);
            }
        }
    }
    
    // Private constructor - utility class
    private Native() {}
}