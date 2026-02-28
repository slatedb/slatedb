package io.slatedb;

import org.junit.jupiter.api.Test;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlateDbMergeOperatorTest {
    private static byte[] concat(@Nullable byte[] existingValue, byte[] operand) {
        byte[] base = existingValue == null ? new byte[0] : existingValue;
        byte[] merged = Arrays.copyOf(base, base.length + operand.length);
        System.arraycopy(operand, 0, merged, base.length, operand.length);
        return merged;
    }

    @Test
    void builderMergeOperatorConcatWorks() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();
        byte[] key = "merge-key".getBytes(StandardCharsets.UTF_8);

        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            builder.withMergeOperator((k, existing, operand) -> concat(existing, operand));

            try (SlateDb db = builder.build()) {
                db.merge(key, "a".getBytes(StandardCharsets.UTF_8));
                assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), db.get(key));

                db.merge(key, "b".getBytes(StandardCharsets.UTF_8));
                db.merge(key, "c".getBytes(StandardCharsets.UTF_8));
                assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), db.get(key));
            }
        }
    }

    @Test
    void mergeWithoutOperatorReadFails() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();
        byte[] key = "merge-no-op-key".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            db.merge(key, "v".getBytes(StandardCharsets.UTF_8));

            SlateDbException ex = assertThrows(SlateDbException.class, () -> db.get(key));
            assertInstanceOf(SlateDbException.InvalidException.class, ex);
            assertNotNull(ex.getMessage());
            assertTrue(ex.getMessage().toLowerCase().contains("merge operator"));
        }
    }

    @Test
    void mergeCallbackExceptionFailsOperation() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();
        byte[] key = "merge-callback-fail-key".getBytes(StandardCharsets.UTF_8);

        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            builder.withMergeOperator((k, existing, operand) -> {
                throw new IllegalStateException("boom");
            });

            try (SlateDb db = builder.build()) {
                SlateDbException ex = assertThrows(
                    SlateDbException.class,
                    () -> db.merge(key, "a".getBytes(StandardCharsets.UTF_8))
                );
                assertInstanceOf(SlateDbException.InvalidException.class, ex);
                assertNotNull(ex.getMessage());
                assertTrue(ex.getMessage().toLowerCase().contains("merge"));
            }
        }
    }

    @Test
    void withMergeOperatorReplacesPreviousOperator() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();
        byte[] key = "merge-replace-key".getBytes(StandardCharsets.UTF_8);

        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            builder.withMergeOperator((k, existing, operand) -> "wrong".getBytes(StandardCharsets.UTF_8));
            builder.withMergeOperator((k, existing, operand) -> concat(existing, operand));

            try (SlateDb db = builder.build()) {
                db.merge(key, "a".getBytes(StandardCharsets.UTF_8));
                db.merge(key, "b".getBytes(StandardCharsets.UTF_8));
                assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), db.get(key));
            }
        }
    }
}
