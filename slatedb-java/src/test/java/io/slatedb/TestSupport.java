package io.slatedb;

import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;

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
    /// Tests fail if `java.library.path` does not contain the library.
    static void ensureNativeReady() throws Exception {
        Path nativeLib = findNativeLibrary();
        Assertions.assertTrue(
            nativeLib != null && Files.exists(nativeLib),
            "Unable to locate SlateDB native library (expected filename: " + expectedLibraryFileName() +
                "). Verify java.library.path includes the native library directory. " +
                "Current java.library.path: " + System.getProperty("java.library.path", "")
        );
        synchronized (INIT_LOCK) {
            if (initialized) {
                return;
            }
            SlateDb.initLogging(SlateDbConfig.LogLevel.INFO);
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

    /// Resolves the native library path from `java.library.path`.
    private static Path findNativeLibrary() {
        return findInJavaLibraryPath(expectedLibraryFileName());
    }

    private static Path findInJavaLibraryPath(String libName) {
        String javaLibraryPath = System.getProperty("java.library.path");
        if (javaLibraryPath == null || javaLibraryPath.isBlank()) {
            return null;
        }

        String[] entries = javaLibraryPath.split(Pattern.quote(File.pathSeparator));
        for (String entry : entries) {
            Path directory;
            try {
                directory = (entry == null || entry.isBlank())
                    ? Path.of("").toAbsolutePath()
                    : Path.of(entry).toAbsolutePath();
            } catch (InvalidPathException ignored) {
                continue;
            }

            Path candidate = directory.resolve(libName);
            if (Files.isRegularFile(candidate)) {
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
