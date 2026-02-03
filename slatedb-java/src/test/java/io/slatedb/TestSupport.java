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
    ///
    /// Tests are skipped if the library path is not provided via
    /// `SLATEDB_C_LIB` or `-Dslatedb.c.lib`.
    static void ensureNativeReady() throws Exception {
        Path nativeLib = findNativeLibrary();
        Assertions.assertTrue(
            nativeLib != null && Files.exists(nativeLib),
            "Set SLATEDB_C_LIB or -Dslatedb.c.lib to the slatedb_c native library"
        );
        synchronized (INIT_LOCK) {
            if (initialized) {
                return;
            }
            SlateDb.loadLibrary(nativeLib.toAbsolutePath().toString());
            SlateDb.initLogging("info");
            initialized = true;
        }
    }

    /// Creates a temporary database directory and a local file-based object store URL.
    static DbContext createDbContext() throws Exception {
        Path dbPath = Files.createTempDirectory("slatedb-java-db");
        Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
        String objectStoreUrl = "file://" + objectStoreRoot.toAbsolutePath();
        return new DbContext(dbPath, objectStoreRoot, objectStoreUrl);
    }

    /// Resolves the native library path from `SLATEDB_C_LIB` or `-Dslatedb.c.lib`.
    private static Path findNativeLibrary() {
        String env = System.getenv("SLATEDB_C_LIB");
        if (env != null && !env.isBlank()) {
            return Path.of(env);
        }
        String prop = System.getProperty("slatedb.c.lib");
        if (prop != null && !prop.isBlank()) {
            return Path.of(prop);
        }
        return null;
    }

    /// Paths and URL used by tests for a file-backed object store.
    record DbContext(Path dbPath, Path objectStoreRoot, String objectStoreUrl) {
    }
}
