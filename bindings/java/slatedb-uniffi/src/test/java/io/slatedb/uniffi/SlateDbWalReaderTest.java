package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SlateDbWalReaderTest {
    @Test
    void walReaderEmptyStore() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                WalReader reader = TestSupport.openWalReader(store)) {
            List<WalFile> files = TestSupport.await(reader.list(null, null));
            try {
                assertEquals(0, files.size());
            } finally {
                TestSupport.closeAll(files);
            }
        }
    }

    @Test
    void walReaderListingAndNavigation() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);

            try (WalReader reader = TestSupport.openWalReader(store)) {
                List<WalFile> files = TestSupport.await(reader.list(null, null));
                try {
                    assertTrue(files.size() >= 3);

                    List<Long> ids = new ArrayList<>();
                    for (int i = 0; i < files.size(); i++) {
                        long id = files.get(i).id();
                        ids.add(id);
                        if (i > 0) {
                            assertTrue(id > ids.get(i - 1));
                        }
                    }

                    List<WalFile> bounded = TestSupport.await(reader.list(ids.get(1), ids.get(2)));
                    try {
                        assertEquals(1, bounded.size());
                        assertEquals(ids.get(1), bounded.get(0).id());
                    } finally {
                        TestSupport.closeAll(bounded);
                    }

                    List<WalFile> empty = TestSupport.await(reader.list(ids.get(ids.size() - 1) + 1000L, null));
                    try {
                        assertEquals(0, empty.size());
                    } finally {
                        TestSupport.closeAll(empty);
                    }

                    try (WalFile first = reader.get(ids.get(0))) {
                        assertEquals(ids.get(0), first.id());
                        assertEquals(ids.get(1), first.nextId());

                        try (WalFile next = first.nextFile()) {
                            assertEquals(ids.get(1), next.id());
                        }
                    }
                } finally {
                    TestSupport.closeAll(files);
                }
            }
        }
    }

    @Test
    void walReaderMetadataAndRows() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);

            try (WalReader reader = TestSupport.openWalReader(store)) {
                List<WalFile> files = TestSupport.await(reader.list(null, null));
                try {
                    assertTrue(files.size() >= 3);

                    List<RowEntry> allRows = new ArrayList<>();
                    for (WalFile file : files) {
                        WalFileMetadata metadata = TestSupport.await(file.metadata());
                        assertNotNull(metadata);
                        assertTrue(metadata.sizeBytes() > 0);
                        assertFalse(metadata.location().isEmpty());

                        try (WalFileIterator iterator = TestSupport.await(file.iterator())) {
                            List<RowEntry> rows = TestSupport.drainWalIterator(iterator);
                            for (RowEntry row : rows) {
                                assertTrue(row.seq() > 0);
                            }
                            allRows.addAll(rows);
                        }
                    }

                    assertEquals(4, allRows.size());
                    TestSupport.assertWalRow(allRows.get(0), RowEntryKind.VALUE, "a", "1");
                    TestSupport.assertWalRow(allRows.get(1), RowEntryKind.VALUE, "b", "2");
                    TestSupport.assertWalRow(allRows.get(2), RowEntryKind.TOMBSTONE, "a", null);
                    TestSupport.assertWalRow(allRows.get(3), RowEntryKind.MERGE, "m", "x");
                } finally {
                    TestSupport.closeAll(files);
                }
            }
        }
    }

    @Test
    void walReaderMissingFile() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);

            try (WalReader reader = TestSupport.openWalReader(store)) {
                List<WalFile> files = TestSupport.await(reader.list(null, null));
                try {
                    assertFalse(files.isEmpty());
                    long missingId = files.get(files.size() - 1).id() + 1000L;

                    try (WalFile missing = reader.get(missingId)) {
                        assertEquals(missingId, missing.id());
                        TestSupport.awaitFailure(Error.class, missing.metadata());
                    }
                } finally {
                    TestSupport.closeAll(files);
                }
            }
        }
    }

    private static void assertFalse(boolean condition) {
        org.junit.jupiter.api.Assertions.assertFalse(condition);
    }
}
