package io.slatedb;

import org.jspecify.annotations.Nullable;

/// Java merge operator callback used to resolve merge operands at read/compaction time.
///
/// Implementations must be thread-safe because callbacks may be invoked concurrently from
/// native SlateDB runtime threads.
///
/// The callback must return a non-null byte array. Throwing an exception or returning `null`
/// causes the native merge operation to fail.
///
/// ### Example
///
/// ```java
/// import io.slatedb.SlateDb;
/// import io.slatedb.SlateDbConfig;
/// import io.slatedb.SlateDbMergeOperator;
///
/// import java.nio.charset.StandardCharsets;
/// import java.nio.file.Files;
/// import java.nio.file.Path;
/// import java.util.Arrays;
///
/// public final class MergeOperatorExample {
///     private static byte[] concat(byte[] existingValue, byte[] operand) {
///         byte[] base = existingValue == null ? new byte[0] : existingValue;
///         byte[] merged = Arrays.copyOf(base, base.length + operand.length);
///         System.arraycopy(operand, 0, merged, base.length, operand.length);
///         return merged;
///     }
///
///     public static void main(String[] args) throws Exception {
///         SlateDb.initLogging(SlateDbConfig.LogLevel.INFO);
///
///         Path dbPath = Files.createTempDirectory("slatedb-java-db");
///         Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
///         String objectStoreUrl = "file://" + objectStoreRoot.toAbsolutePath();
///
///         byte[] key = "k".getBytes(StandardCharsets.UTF_8);
///
///         try (SlateDb.Builder builder = SlateDb.builder(dbPath.toString(), objectStoreUrl, null)) {
///             builder.withMergeOperator((k, existingValue, operand) -> concat(existingValue, operand));
///
///             try (SlateDb db = builder.build()) {
///                 db.merge(key, "a".getBytes(StandardCharsets.UTF_8));
///                 db.merge(key, "b".getBytes(StandardCharsets.UTF_8));
///
///                 byte[] merged = db.get(key);
///                 System.out.println(new String(merged, StandardCharsets.UTF_8)); // "ab"
///             }
///         }
///     }
/// }
/// ```
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
