package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SlateDbWalReaderTest {
    private static List<RowEntry> rowsOf(List<WalRows> batches) {
        List<RowEntry> rows = new ArrayList<>();
        for (WalRows batch : batches) {
            rows.addAll(batch.rows());
        }
        return rows;
    }

    @Test
    void walReaderReportsNoNewFilesAfterCursor() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);
            try (SlateDbWalReader reader = TestSupport.openSlateDbWalReader(store)) {
                long cursor = TestSupport.await(reader.lastWalFileId(0L));
                assertEquals(cursor, TestSupport.await(reader.lastWalFileId(cursor)));
            }
        }
    }

    @Test
    void walReaderStreamsNewWalsThroughOneIterator() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);
            try (SlateDbWalReader reader = TestSupport.openSlateDbWalReader(store)) {
                long firstTail = TestSupport.await(reader.lastWalFileId(0L));
                try (SlateDbWalIterator iterator = TestSupport.await(reader.iterator(1L))) {
                    List<WalRows> firstBatches =
                            TestSupport.readWalBatchesThrough(iterator, firstTail);
                    assertFalse(firstBatches.isEmpty());
                    assertTrue(firstBatches.stream().anyMatch(batch -> batch.rows().isEmpty()));
                    long previous = 0L;
                    for (WalRows batch : firstBatches) {
                        assertTrue(batch.lastConsumedWalFileId() > previous);
                        previous = batch.lastConsumedWalFileId();
                    }
                    assertEquals(firstTail, previous);
                    assertEquals(firstTail, TestSupport.await(reader.lastWalFileId(firstTail)));

                    TestSupport.appendWalValue(store, "next", "3");
                    long secondTail = TestSupport.await(reader.lastWalFileId(firstTail));
                    assertTrue(secondTail > firstTail);
                    List<WalRows> secondBatches =
                            TestSupport.readWalBatchesThrough(iterator, secondTail);
                    assertFalse(secondBatches.isEmpty());
                    assertEquals(
                            secondTail,
                            secondBatches.get(secondBatches.size() - 1).lastConsumedWalFileId());
                    List<RowEntry> secondRows = rowsOf(secondBatches);
                    assertEquals(1, secondRows.size());
                    TestSupport.assertWalRow(secondRows.get(0), RowEntryKind.VALUE, "next", "3");
                    assertEquals(secondTail, TestSupport.await(reader.lastWalFileId(secondTail)));
                }
            }
        }
    }

    @Test
    void walReaderDecodesValueTombstoneAndMergeRows() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);
            try (SlateDbWalReader reader = TestSupport.openSlateDbWalReader(store)) {
                long tail = TestSupport.await(reader.lastWalFileId(0L));
                List<WalRows> batches;
                try (SlateDbWalIterator iterator = TestSupport.await(reader.iterator(1L))) {
                    batches = TestSupport.readWalBatchesThrough(iterator, tail);
                    assertEquals(tail, batches.get(batches.size() - 1).lastConsumedWalFileId());
                }

                List<RowEntry> rows = rowsOf(batches);
                assertEquals(4, rows.size());
                assertTrue(rows.stream().allMatch(row -> row.seq() > 0L));
                TestSupport.assertWalRow(rows.get(0), RowEntryKind.VALUE, "a", "1");
                TestSupport.assertWalRow(rows.get(1), RowEntryKind.VALUE, "b", "2");
                TestSupport.assertWalRow(rows.get(2), RowEntryKind.TOMBSTONE, "a", null);
                TestSupport.assertWalRow(rows.get(3), RowEntryKind.MERGE, "m", "x");
            }
        }
    }

    @Test
    void walReaderCanStartAtTheNextWal() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            TestSupport.seedWalFiles(store);
            try (SlateDbWalReader reader = TestSupport.openSlateDbWalReader(store)) {
                long tail = TestSupport.await(reader.lastWalFileId(0L));
                try (SlateDbWalIterator iterator =
                        TestSupport.await(reader.iterator(Math.addExact(tail, 1L)))) {
                    TestSupport.appendWalValue(store, "resumed", "4");
                    long newTail = TestSupport.await(reader.lastWalFileId(tail));
                    List<RowEntry> rows = rowsOf(
                            TestSupport.readWalBatchesThrough(iterator, newTail));
                    assertEquals(1, rows.size());
                    TestSupport.assertWalRow(rows.get(0), RowEntryKind.VALUE, "resumed", "4");
                }
            }
        }
    }
}
