package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Callback interface for SlateDB merge operators.
 *
 * Merge operators are configured on [`DbBuilder`] and are used by merge reads
 * and writes to combine an existing value with a new operand.
 */
public interface MergeOperator {
    
    /**
     * Merge a new operand into the existing value for a key.
     *
     * ## Arguments
     * - `key`: the key being merged.
     * - `existing_value`: the current value, if one exists.
     * - `operand`: the new merge operand.
     *
     * ## Returns
     * - `Result<Vec<u8>, MergeOperatorCallbackError>`: the merged value that
     * should become visible for the key.
     */
    public byte[] merge(byte[] key, byte[] existingValue, byte[] operand) throws MergeOperatorCallbackException;
    
}
