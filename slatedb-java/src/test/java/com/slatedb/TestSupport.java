package com.slatedb;

import org.junit.jupiter.api.Assumptions;

import java.nio.file.Files;
import java.nio.file.Path;

final class TestSupport {
    private static final Object INIT_LOCK = new Object();
    private static volatile boolean initialized;

    private TestSupport() {
    }

    static void ensureNativeReady() throws Exception {
        Path nativeLib = findNativeLibrary();
        Assumptions.assumeTrue(nativeLib != null && Files.exists(nativeLib),
            "Set SLATEDB_C_LIB or -Dslatedb.c.lib to the slatedb_c native library");
        synchronized (INIT_LOCK) {
            if (initialized) {
                return;
            }
            SlateDb.loadLibrary(nativeLib.toAbsolutePath().toString());
            SlateDb.initLogging("info");
            initialized = true;
        }
    }

    static DbContext createDbContext() throws Exception {
        Path dbPath = Files.createTempDirectory("slatedb-java-db");
        Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
        String objectStoreUrl = "file://" + objectStoreRoot.toAbsolutePath();
        return new DbContext(dbPath, objectStoreRoot, objectStoreUrl);
    }

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

    record DbContext(Path dbPath, Path objectStoreRoot, String objectStoreUrl) {
    }
}
