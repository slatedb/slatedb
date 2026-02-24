package io.slatedb;

import org.junit.jupiter.api.Assertions;

import java.nio.file.Files;
import java.nio.file.Path;

/// Test helpers for SlateDB JUnit tests.
///
/// Provides native library initialization and temporary database/object store paths.
final class TestSupport {
    private static final Object INIT_LOCK = new Object();
    private static volatile boolean initialized;

    private TestSupport() {
    }

    /// Loads the native SlateDB library once for the test JVM.
    static void ensureNativeReady() throws Exception {
        synchronized (INIT_LOCK) {
            if (initialized) {
                return;
            }
            Assertions.assertDoesNotThrow(
                () -> SlateDb.initLogging(SlateDbConfig.LogLevel.INFO),
                "Unable to initialize SlateDB native library from classpath resources"
            );
            initialized = true;
        }
    }

    /// Creates a temporary database directory and an in-memory object store URL.
    static DbContext createDbContext() throws Exception {
        Path dbPath = Files.createTempDirectory("slatedb-java-db");
        Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
        String objectStoreUrl = "memory://";
        return new DbContext(dbPath, objectStoreRoot, objectStoreUrl);
    }

    /// Paths and URL used by tests for a file-backed object store.
    record DbContext(Path dbPath, Path objectStoreRoot, String objectStoreUrl) {
    }
}
