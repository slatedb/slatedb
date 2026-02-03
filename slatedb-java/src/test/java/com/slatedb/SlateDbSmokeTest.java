package com.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

class SlateDbSmokeTest {
    @Test
    void openPutGetClose() throws Exception {
        Path nativeLib = findNativeLibrary();
        Assumptions.assumeTrue(nativeLib != null && Files.exists(nativeLib),
            "Set SLATEDB_C_LIB or -Dslatedb.c.lib to the slatedb_c native library");

        SlateDb.loadLibrary(nativeLib.toAbsolutePath().toString());
        SlateDb.initLogging("info");

        Path dbPath = Files.createTempDirectory("slatedb-java-db");
        Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
        String objectStoreUrl = "file://" + objectStoreRoot.toAbsolutePath();

        byte[] key = "smoke-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "smoke-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(dbPath.toAbsolutePath().toString(), objectStoreUrl, null)) {
            db.put(key, value);
            byte[] loaded = db.get(key);
            Assertions.assertArrayEquals(value, loaded);
        }
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
}
