package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SlateDbReaderTest {
    @Test
    void readerLifecycle() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store)) {
            Db db = dbHandle.db();
            TestSupport.await(db.put(TestSupport.bytes("reader"), TestSupport.bytes("value")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle = TestSupport.openReader(store)) {
                DbReader reader = readerHandle.reader();
                assertArrayEquals(
                        TestSupport.bytes("value"),
                        TestSupport.await(reader.get(TestSupport.bytes("reader"))));

                TestSupport.await(reader.shutdown());
                readerHandle.markClosed();
                TestSupport.awaitFailure(Error.Closed.class, reader.get(TestSupport.bytes("reader")));
            }
        }
    }

    @Test
    void readerBuildFailsWhenDatabaseMissing() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                DbReaderBuilder builder = new DbReaderBuilder(TestSupport.TEST_DB_PATH, store)) {
            TestSupport.awaitFailure(Error.Data.class, builder.build());
        }
    }

    @Test
    void readerPointReads() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store)) {
            Db db = dbHandle.db();
            TestSupport.await(db.put(TestSupport.bytes("alpha"), TestSupport.bytes("one")));
            TestSupport.await(db.put(TestSupport.bytes("empty"), new byte[0]));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle = TestSupport.openReader(store)) {
                DbReader reader = readerHandle.reader();
                assertArrayEquals(
                        TestSupport.bytes("one"),
                        TestSupport.await(reader.get(TestSupport.bytes("alpha"))));
                assertArrayEquals(
                        TestSupport.bytes("one"),
                        TestSupport.await(
                                reader.getWithOptions(TestSupport.bytes("alpha"), TestSupport.readOptions())));
                assertArrayEquals(new byte[0], TestSupport.await(reader.get(TestSupport.bytes("empty"))));
                assertNull(TestSupport.await(reader.get(TestSupport.bytes("missing"))));
            }
        }
    }

    @Test
    void readerScanVariants() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store)) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("item:01"), TestSupport.bytes("first")));
            TestSupport.await(db.put(TestSupport.bytes("item:02"), TestSupport.bytes("second")));
            TestSupport.await(db.put(TestSupport.bytes("item:03"), TestSupport.bytes("third")));
            TestSupport.await(db.put(TestSupport.bytes("other:01"), TestSupport.bytes("other")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle = TestSupport.openReader(store)) {
                DbReader reader = readerHandle.reader();

                try (DbIterator iterator =
                        TestSupport.await(reader.scan(new KeyRange(null, false, null, false)))) {
                    TestSupport.assertRows(
                            TestSupport.drainIterator(iterator),
                            new String[] {"item:01", "item:02", "item:03", "other:01"},
                            new String[] {"first", "second", "third", "other"});
                }

                try (DbIterator iterator =
                        TestSupport.await(
                                reader.scan(
                                        new KeyRange(
                                                TestSupport.bytes("item:02"),
                                                true,
                                                TestSupport.bytes("item:03"),
                                                true)))) {
                    TestSupport.assertRows(
                            TestSupport.drainIterator(iterator),
                            new String[] {"item:02", "item:03"},
                            new String[] {"second", "third"});
                }

                try (DbIterator iterator =
                        TestSupport.await(
                                reader.scanWithOptions(
                                        new KeyRange(
                                                TestSupport.bytes("item:01"),
                                                true,
                                                TestSupport.bytes("item:99"),
                                                false),
                                        TestSupport.scanOptions(64L, true, 2L)))) {
                    TestSupport.assertRows(
                            TestSupport.drainIterator(iterator),
                            new String[] {"item:01", "item:02", "item:03"},
                            new String[] {"first", "second", "third"});
                }

                try (DbIterator iterator =
                        TestSupport.await(reader.scanPrefix(TestSupport.bytes("item:")))) {
                    TestSupport.assertRows(
                            TestSupport.drainIterator(iterator),
                            new String[] {"item:01", "item:02", "item:03"},
                            new String[] {"first", "second", "third"});
                }

                try (DbIterator iterator =
                        TestSupport.await(
                                reader.scanPrefixWithOptions(
                                        TestSupport.bytes("item:"),
                                        TestSupport.scanOptions(32L, false, 1L)))) {
                    TestSupport.assertRows(
                            TestSupport.drainIterator(iterator),
                            new String[] {"item:01", "item:02", "item:03"},
                            new String[] {"first", "second", "third"});
                }
            }
        }
    }

    @Test
    void readerRefreshBehavior() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store)) {
            Db db = dbHandle.db();
            TestSupport.await(db.put(TestSupport.bytes("seed"), TestSupport.bytes("value")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle =
                    TestSupport.openReader(store, builder -> builder.withOptions(TestSupport.readerOptions(false)))) {
                DbReader reader = readerHandle.reader();

                TestSupport.await(db.put(TestSupport.bytes("refresh"), TestSupport.bytes("updated")));
                TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

                TestSupport.waitUntil(
                        TestSupport.WAIT_TIMEOUT,
                        TestSupport.WAIT_STEP,
                        () -> {
                            byte[] value = TestSupport.await(reader.get(TestSupport.bytes("refresh")));
                            return value != null
                                    && java.util.Arrays.equals(value, TestSupport.bytes("updated"));
                        });
            }
        }
    }

    @Test
    void readerWalReplayDefault() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store);
                TestSupport.ManagedReader readerHandle =
                        TestSupport.openReader(store, builder -> builder.withOptions(TestSupport.readerOptions(false)))) {
            Db db = dbHandle.db();
            DbReader reader = readerHandle.reader();

            TestSupport.await(db.put(TestSupport.bytes("wal-key"), TestSupport.bytes("wal-value")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.WAL)));

            TestSupport.waitUntil(
                    TestSupport.WAIT_TIMEOUT,
                    TestSupport.WAIT_STEP,
                    () -> {
                        byte[] value = TestSupport.await(reader.get(TestSupport.bytes("wal-key")));
                        return value != null
                                && java.util.Arrays.equals(value, TestSupport.bytes("wal-value"));
                    });
        }
    }

    @Test
    void readerSkipWalReplayIgnoresWalOnlyData() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store)) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("flushed-key"), TestSupport.bytes("flushed-value")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            TestSupport.await(db.put(TestSupport.bytes("wal-only-key"), TestSupport.bytes("wal-only-value")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.WAL)));

            try (TestSupport.ManagedReader readerHandle =
                    TestSupport.openReader(store, builder -> builder.withOptions(TestSupport.readerOptions(true)))) {
                DbReader reader = readerHandle.reader();

                assertArrayEquals(
                        TestSupport.bytes("flushed-value"),
                        TestSupport.await(reader.get(TestSupport.bytes("flushed-key"))));
                assertNull(TestSupport.await(reader.get(TestSupport.bytes("wal-only-key"))));
            }

            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle =
                    TestSupport.openReader(store, builder -> builder.withOptions(TestSupport.readerOptions(true)))) {
                assertArrayEquals(
                        TestSupport.bytes("wal-only-value"),
                        TestSupport.await(readerHandle.reader().get(TestSupport.bytes("wal-only-key"))));
            }
        }
    }

    @Test
    void readerMergeOperator() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle =
                        TestSupport.openDb(store, builder -> builder.withMergeOperator(new TestSupport.ConcatMergeOperator()))) {
            Db db = dbHandle.db();
            TestSupport.await(db.put(TestSupport.bytes("merge"), TestSupport.bytes("base")));
            TestSupport.await(db.merge(TestSupport.bytes("merge"), TestSupport.bytes(":reader")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle =
                    TestSupport.openReader(store, builder -> builder.withMergeOperator(new TestSupport.ConcatMergeOperator()))) {
                assertArrayEquals(
                        TestSupport.bytes("base:reader"),
                        TestSupport.await(readerHandle.reader().get(TestSupport.bytes("merge"))));
            }
        }
    }

    @Test
    void readerRejectsInvalidCheckpointId() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store);
                DbReaderBuilder builder = new DbReaderBuilder(TestSupport.TEST_DB_PATH, store)) {
            TestSupport.await(dbHandle.db().put(TestSupport.bytes("seed"), TestSupport.bytes("value")));
            TestSupport.await(dbHandle.db().flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            TestSupport.expectFailure(Error.Invalid.class, () -> builder.withCheckpointId("not-a-uuid"));
        }
    }

    @Test
    void readerMissingCheckpointIdFailsBuild() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store);
                DbReaderBuilder builder = new DbReaderBuilder(TestSupport.TEST_DB_PATH, store)) {
            TestSupport.await(dbHandle.db().put(TestSupport.bytes("seed"), TestSupport.bytes("value")));
            TestSupport.await(dbHandle.db().flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            builder.withCheckpointId("ffffffff-ffff-ffff-ffff-ffffffffffff");
            TestSupport.awaitFailure(Error.Data.class, builder.build());
        }
    }

    @Test
    void readerBuilderIsConsumedAfterBuild() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store);
                DbReaderBuilder builder = new DbReaderBuilder(TestSupport.TEST_DB_PATH, store)) {
            TestSupport.await(dbHandle.db().put(TestSupport.bytes("seed"), TestSupport.bytes("value")));
            TestSupport.await(dbHandle.db().flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (DbReader reader = TestSupport.await(builder.build())) {
                TestSupport.await(reader.shutdown());
            }

            TestSupport.awaitFailure(Error.Invalid.class, builder.build());
        }
    }

    @Test
    void readerRejectsInvalidKeyRanges() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store)) {
            TestSupport.await(dbHandle.db().put(TestSupport.bytes("seed"), TestSupport.bytes("value")));
            TestSupport.await(dbHandle.db().flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            try (TestSupport.ManagedReader readerHandle = TestSupport.openReader(store)) {
                DbReader reader = readerHandle.reader();
                TestSupport.awaitFailure(
                        Error.Invalid.class,
                        reader.scan(
                                new KeyRange(
                                        TestSupport.bytes("z"),
                                        true,
                                        TestSupport.bytes("a"),
                                        true)));

                // Scan with empty start bound should succeed and be treated as unbounded start.
                try (DbIterator iterator =
                        TestSupport.await(reader.scan(new KeyRange(new byte[0], true, null, false)))) {
                    TestSupport.assertRows(
                            TestSupport.drainIterator(iterator),
                            new String[] {"seed"},
                            new String[] {"value"});
                }
            }
        }
    }
}
