package io.slatedb;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class SlateDbWalReaderTest {
    private static void createDb(TestSupport.DbContext context, String url) {
        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), url, null)) {
            db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
            db.put("key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
            db.delete("key1".getBytes(UTF_8));
            db.flush();
        }
    }

    @Test
    void walReaderListAndIterate() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var url = "file://" + context.objectStoreRoot().toAbsolutePath();

        createDb(context, url);

        try (SlateDbWalReader reader = SlateDbWalReader.open(
            context.dbPath().toAbsolutePath().toString(), url, null
        )) {
            List<SlateDbWalFile> files = reader.list(null, null);
            assertFalse(files.isEmpty(), "Expected at least one WAL file");

            int entryCount = 0;
            for (SlateDbWalFile file : files) {
                try (file; SlateDbWalFileIterator iter = file.iterator()) {
                    SlateDbRowEntry entry;
                    while ((entry = iter.next()) != null) {
                        assertNotNull(entry.kind());
                        assertNotNull(entry.key());
                        assertTrue(entry.seq() >= 0);
                        entryCount++;
                    }
                }
            }
            assertTrue(entryCount > 0, "Expected at least one row entry");
        }
    }

    @Test
    void walReaderGetById() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var url = "file://" + context.objectStoreRoot().toAbsolutePath();

        createDb(context, url);

        try (SlateDbWalReader reader = SlateDbWalReader.open(
            context.dbPath().toAbsolutePath().toString(), url, null
        )) {
            List<SlateDbWalFile> files = reader.list(null, null);
            assertFalse(files.isEmpty());

            long firstId = files.get(0).id();
            for (SlateDbWalFile file : files) {
                file.close();
            }

            try (SlateDbWalFile fetched = reader.get(firstId)) {
                assertEquals(firstId, fetched.id());
            }
        }
    }

    @Test
    void walFileMetadata() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var url = "file://" + context.objectStoreRoot().toAbsolutePath();

        createDb(context, url);

        try (SlateDbWalReader reader = SlateDbWalReader.open(
            context.dbPath().toAbsolutePath().toString(), url, null
        )) {
            List<SlateDbWalFile> files = reader.list(null, null);
            assertFalse(files.isEmpty());

            try (SlateDbWalFile file = files.get(0)) {
                for (int i = 1; i < files.size(); i++) {
                    files.get(i).close();
                }

                SlateDbWalFileMetadata meta = file.metadata();
                assertNotNull(meta.lastModified());
                assertTrue(meta.sizeBytes() > 0, "Expected non-zero WAL file size");
                assertNotNull(meta.location());
                assertFalse(meta.location().isEmpty());
            }
        }
    }

    @Test
    void walEntryKinds() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var url = "file://" + context.objectStoreRoot().toAbsolutePath();

        createDb(context, url);

        try (SlateDbWalReader reader = SlateDbWalReader.open(
            context.dbPath().toAbsolutePath().toString(), url, null
        )) {
            List<SlateDbWalFile> files = reader.list(null, null);
            assertFalse(files.isEmpty());

            boolean sawValue = false;
            boolean sawTombstone = false;

            for (SlateDbWalFile file : files) {
                try (file; SlateDbWalFileIterator iter = file.iterator()) {
                    SlateDbRowEntry entry;
                    while ((entry = iter.next()) != null) {
                        if (entry.kind() == SlateDbConfig.RowEntryKind.VALUE) {
                            sawValue = true;
                            assertNotNull(entry.value());
                        } else if (entry.kind() == SlateDbConfig.RowEntryKind.TOMBSTONE) {
                            sawTombstone = true;
                        }
                    }
                }
            }

            assertTrue(sawValue, "Expected at least one VALUE entry");
            assertTrue(sawTombstone, "Expected at least one TOMBSTONE entry");
        }
    }

    @Test
    void walReaderCloseIsIdempotent() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var url = "file://" + context.objectStoreRoot().toAbsolutePath();

        createDb(context, url);

        SlateDbWalReader reader = SlateDbWalReader.open(
            context.dbPath().toAbsolutePath().toString(), url, null
        );
        reader.close();
        assertDoesNotThrow(reader::close);
    }

    @Test
    void walReaderListWithIdRange() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var url = "file://" + context.objectStoreRoot().toAbsolutePath();

        createDb(context, url);

        try (SlateDbWalReader reader = SlateDbWalReader.open(
            context.dbPath().toAbsolutePath().toString(), url, null
        )) {
            List<SlateDbWalFile> allFiles = reader.list(null, null);
            assertFalse(allFiles.isEmpty());

            long firstId = allFiles.get(0).id();
            long lastId = allFiles.get(allFiles.size() - 1).id();
            for (SlateDbWalFile file : allFiles) {
                file.close();
            }

            // List with a range that excludes the last file
            List<SlateDbWalFile> rangedFiles = reader.list(firstId, lastId);
            for (SlateDbWalFile file : rangedFiles) {
                assertTrue(file.id() >= firstId && file.id() < lastId);
                file.close();
            }
        }
    }
}
