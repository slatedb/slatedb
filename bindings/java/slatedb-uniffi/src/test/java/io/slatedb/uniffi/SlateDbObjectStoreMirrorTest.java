package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SlateDbObjectStoreMirrorTest {
    private static ObjectStore buildMirror(Path localDir, ObjectStore store) throws Exception {
        try (ObjectStoreMirrorBuilder builder =
                new ObjectStoreMirrorBuilder(localDir.toString(), store)) {
            return TestSupport.await(builder.build());
        }
    }

    @Test
    void mirrorKeepsFlushedSstsLocal(@TempDir Path localDir) throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                ObjectStore mirror = buildMirror(localDir, store)) {
            try (TestSupport.ManagedDb handle = TestSupport.openDb(mirror)) {
                Db db = handle.db();
                TestSupport.await(db.put(TestSupport.bytes("key"), TestSupport.bytes("value")));
                TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            }
            try (Stream<Path> files = Files.list(localDir)) {
                assertTrue(files.anyMatch(file -> file.getFileName().toString().endsWith(".obj")));
            }

            // The same mirror serves each open of its database.
            try (TestSupport.ManagedDb handle = TestSupport.openDb(mirror)) {
                assertArrayEquals(
                        TestSupport.bytes("value"),
                        TestSupport.await(handle.db().get(TestSupport.bytes("key"))));
            }
        }
    }

    @Test
    void mirrorRejectsLockedDirectory(@TempDir Path localDir) throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                ObjectStore mirror = buildMirror(localDir, store)) {
            Error.Unavailable error =
                    TestSupport.expectFailure(
                            Error.Unavailable.class, () -> buildMirror(localDir, store).close());
            assertTrue(error.getMessage().contains("locked"));
        }
    }

    private static final class RejectingFilter implements SegmentFilter {
        private final Set<String> asked = ConcurrentHashMap.newKeySet();

        @Override
        public boolean shouldMirror(byte[] segment) {
            asked.add(new String(segment, StandardCharsets.UTF_8));
            return false;
        }
    }

    private static final class FailingFilter implements SegmentFilter {
        @Override
        public boolean shouldMirror(byte[] segment) {
            throw new IllegalStateException("boom");
        }
    }

    private static void assertNothingMirrored(Path localDir, SegmentFilter filter)
            throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                ObjectStoreMirrorBuilder builder =
                        new ObjectStoreMirrorBuilder(localDir.toString(), store)) {
            builder.withSegmentFilter(filter);
            try (ObjectStore mirror = TestSupport.await(builder.build());
                    TestSupport.ManagedDb handle = TestSupport.openDb(mirror)) {
                Db db = handle.db();
                TestSupport.await(db.put(TestSupport.bytes("key"), TestSupport.bytes("value")));
                TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
                assertArrayEquals(
                        TestSupport.bytes("value"),
                        TestSupport.await(db.get(TestSupport.bytes("key"))));
            }
        }
        try (Stream<Path> files = Files.list(localDir)) {
            assertTrue(files.noneMatch(file -> file.getFileName().toString().endsWith(".obj")));
        }
    }

    @Test
    void mirrorSkipsSegmentsThatFilterRejects(@TempDir Path localDir) throws Exception {
        RejectingFilter filter = new RejectingFilter();
        assertNothingMirrored(localDir, filter);
        // A database without a segment extractor has only the root segment.
        assertEquals(Set.of(""), filter.asked);
    }

    @Test
    void mirrorSkipsSegmentsWhenFilterThrows(@TempDir Path localDir) throws Exception {
        assertNothingMirrored(localDir, new FailingFilter());
    }
}
