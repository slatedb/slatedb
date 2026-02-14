package io.slatedb;

import org.junit.jupiter.api.Assertions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
    /// Tests fail if the library path is not provided via `SLATEDB_C_LIB` or
    /// `-Dslatedb.c.lib` and a default build artifact cannot be found.
    static void ensureNativeReady() throws Exception {
        Path nativeLib = findNativeLibrary();
        Assertions.assertTrue(
            nativeLib != null && Files.exists(nativeLib),
            "Set SLATEDB_C_LIB or -Dslatedb.c.lib to the slatedb_c native library " +
                "(expected filename: " + expectedLibraryFileName() + ")"
        );
        synchronized (INIT_LOCK) {
            if (initialized) {
                return;
            }
            SlateDb.loadLibrary(nativeLib.toAbsolutePath().toString());
            SlateDb.initLogging(SlateDbConfig.LogLevel.INFO);
            initialized = true;
        }
    }

    static Path nativeLibraryPath() {
        return findNativeLibrary();
    }

    /// Creates a temporary database directory and an in-memory object store URL.
    static DbContext createDbContext() throws Exception {
        Path dbPath = Files.createTempDirectory("slatedb-java-db");
        Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
        String objectStoreUrl = "memory://";
        return new DbContext(dbPath, objectStoreRoot, objectStoreUrl);
    }

    /// Resolves the native library path from `SLATEDB_C_LIB`, `-Dslatedb.c.lib`,
    /// or the default build output paths under `target/`.
    private static Path findNativeLibrary() {
        String env = System.getenv("SLATEDB_C_LIB");
        if (env != null && !env.isBlank()) {
            return Path.of(env);
        }
        String prop = System.getProperty("slatedb.c.lib");
        if (prop != null && !prop.isBlank()) {
            return Path.of(prop);
        }
        String libName = expectedLibraryFileName();
        Path cwd = Path.of("").toAbsolutePath();
        Path repoRoot = cwd.getParent();

        List<Path> candidates = new ArrayList<>();
        candidates.add(cwd.resolve("target").resolve("debug").resolve(libName));
        candidates.add(cwd.resolve("target").resolve("release").resolve(libName));
        if (repoRoot != null && !repoRoot.equals(cwd)) {
            candidates.add(repoRoot.resolve("target").resolve("debug").resolve(libName));
            candidates.add(repoRoot.resolve("target").resolve("release").resolve(libName));
        }

        for (Path candidate : candidates) {
            if (Files.exists(candidate)) {
                return candidate;
            }
        }

        return null;
    }

    /// Determines the expected native library filename for the current OS.
    /// @return The expected filename (e.g., `libslatedb_c.so`, `slatedb_c.dll`, `libslatedb_c.dylib`).
    private static String expectedLibraryFileName() {
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (osName.contains("win")) {
            return "slatedb_c.dll";
        }
        if (osName.contains("mac")) {
            return "libslatedb_c.dylib";
        }
        return "libslatedb_c.so";
    }

    /// Paths and URL used by tests for a file-backed object store.
    record DbContext(Path dbPath, Path objectStoreRoot, String objectStoreUrl) {
    }
}
