package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Application-provided merge operator used by merge-enabled databases.
 */
public interface MergeOperator {
    
    /**
     * Combines an existing value and a new merge operand into the next value.
     *
     * `existing_value` is `None` when the key has no visible base value.
     */
    public byte[] merge(byte[] key, byte[] existingValue, byte[] operand) throws MergeOperatorCallbackException;
    
}

