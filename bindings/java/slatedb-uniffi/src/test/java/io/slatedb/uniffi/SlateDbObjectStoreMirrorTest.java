package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
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
}
