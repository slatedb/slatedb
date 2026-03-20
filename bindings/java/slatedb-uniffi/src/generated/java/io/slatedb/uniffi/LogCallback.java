package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Callback invoked for each emitted log record.
 */
public interface LogCallback {
    
    /**
     * Handles one log record.
     */
    public void log(LogRecord record);
    
}

