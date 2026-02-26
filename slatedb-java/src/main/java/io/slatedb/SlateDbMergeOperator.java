package io.slatedb;

import org.jspecify.annotations.Nullable;

/// Java merge operator callback used to resolve merge operands at read/compaction time.
///
/// Implementations must be thread-safe because callbacks may be invoked concurrently from
/// native SlateDB runtime threads.
///
/// The callback must return a non-null byte array. Throwing an exception or returning `null`
/// causes the native merge operation to fail.
@FunctionalInterface
public interface SlateDbMergeOperator {
    /// Merges an operand into an optional existing value.
    ///
    /// @param key key being merged.
    /// @param existingValue current value for the key, or `null` when absent.
    /// @param operand new merge operand.
    /// @return merged value bytes (non-null).
    byte[] merge(byte[] key, @Nullable byte[] existingValue, byte[] operand);
}
