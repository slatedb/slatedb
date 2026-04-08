package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SlateDbDbTest {
    @Test
    void dbLifecycleAndStatus() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            db.status();
            TestSupport.await(db.put(TestSupport.bytes("lifecycle"), TestSupport.bytes("value")));

            TestSupport.await(db.shutdown());
            handle.markClosed();

            Error.Closed statusError = TestSupport.expectFailure(Error.Closed.class, db::status);
            assertEquals(CloseReason.CLEAN, statusError.reason());

            TestSupport.awaitFailure(
                    Error.Closed.class,
                    db.put(TestSupport.bytes("after-shutdown"), TestSupport.bytes("value")));
        }
    }

    @Test
    void dbCrudAndMetadata() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            ReadOptions readOptions = TestSupport.readOptions();
            PutOptions putOptions = new PutOptions(new Ttl.Default());
            WriteOptions writeOptions = new WriteOptions(true);

            WriteHandle firstWrite = TestSupport.await(db.put(TestSupport.bytes("alpha"), TestSupport.bytes("one")));
            assertNotNull(firstWrite);
            assertTrue(firstWrite.seqnum() > 0);
            assertTrue(firstWrite.createTs() > 0);

            assertArrayEquals(TestSupport.bytes("one"), TestSupport.await(db.get(TestSupport.bytes("alpha"))));
            assertArrayEquals(
                    TestSupport.bytes("one"),
                    TestSupport.await(db.getWithOptions(TestSupport.bytes("alpha"), readOptions)));

            KeyValue metadata = TestSupport.await(db.getKeyValue(TestSupport.bytes("alpha")));
            assertNotNull(metadata);
            assertArrayEquals(TestSupport.bytes("alpha"), metadata.key());
            assertArrayEquals(TestSupport.bytes("one"), metadata.value());
            assertEquals(firstWrite.seqnum(), metadata.seq());
            assertEquals(firstWrite.createTs(), metadata.createTs());

            KeyValue metadataWithOptions =
                    TestSupport.await(db.getKeyValueWithOptions(TestSupport.bytes("alpha"), readOptions));
            assertNotNull(metadataWithOptions);
            assertArrayEquals(TestSupport.bytes("one"), metadataWithOptions.value());

            WriteHandle secondWrite =
                    TestSupport.await(
                            db.putWithOptions(
                                    TestSupport.bytes("beta"),
                                    TestSupport.bytes("two"),
                                    putOptions,
                                    writeOptions));
            assertNotNull(secondWrite);
            assertTrue(secondWrite.seqnum() > firstWrite.seqnum());
            assertTrue(secondWrite.createTs() > 0);

            assertArrayEquals(TestSupport.bytes("two"), TestSupport.await(db.get(TestSupport.bytes("beta"))));

            TestSupport.await(db.put(TestSupport.bytes("empty"), new byte[0]));
            assertArrayEquals(new byte[0], TestSupport.await(db.get(TestSupport.bytes("empty"))));

            assertNull(TestSupport.await(db.get(TestSupport.bytes("missing"))));

            WriteHandle deleteAlpha = TestSupport.await(db.delete(TestSupport.bytes("alpha")));
            assertNotNull(deleteAlpha);
            assertTrue(deleteAlpha.seqnum() > secondWrite.seqnum());
            assertNull(TestSupport.await(db.get(TestSupport.bytes("alpha"))));

            WriteHandle deleteBeta =
                    TestSupport.await(db.deleteWithOptions(TestSupport.bytes("beta"), writeOptions));
            assertNotNull(deleteBeta);
            assertTrue(deleteBeta.seqnum() > secondWrite.seqnum());
            assertNull(TestSupport.await(db.get(TestSupport.bytes("beta"))));
        }
    }

    @Test
    void dbScanVariants() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            TestSupport.await(db.put(TestSupport.bytes("item:01"), TestSupport.bytes("first")));
            TestSupport.await(db.put(TestSupport.bytes("item:02"), TestSupport.bytes("second")));
            TestSupport.await(db.put(TestSupport.bytes("item:03"), TestSupport.bytes("third")));
            TestSupport.await(db.put(TestSupport.bytes("other:01"), TestSupport.bytes("other")));

            try (DbIterator iterator = TestSupport.await(db.scan(new KeyRange(null, false, null, false)))) {
                TestSupport.assertRows(
                        TestSupport.drainIterator(iterator),
                        new String[] {"item:01", "item:02", "item:03", "other:01"},
                        new String[] {"first", "second", "third", "other"});
            }

            try (DbIterator iterator =
                    TestSupport.await(
                            db.scan(
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
                            db.scanWithOptions(
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

            try (DbIterator iterator = TestSupport.await(db.scanPrefix(TestSupport.bytes("item:")))) {
                TestSupport.assertRows(
                        TestSupport.drainIterator(iterator),
                        new String[] {"item:01", "item:02", "item:03"},
                        new String[] {"first", "second", "third"});
            }

            try (DbIterator iterator =
                    TestSupport.await(
                            db.scanPrefixWithOptions(
                                    TestSupport.bytes("item:"),
                                    TestSupport.scanOptions(32L, false, 1L)))) {
                TestSupport.assertRows(
                        TestSupport.drainIterator(iterator),
                        new String[] {"item:01", "item:02", "item:03"},
                        new String[] {"first", "second", "third"});
            }
        }
    }

    @Test
    void dbBatchWriteAndConsumption() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            TestSupport.await(db.put(TestSupport.bytes("remove-me"), TestSupport.bytes("old")));

            try (WriteBatch batch = new WriteBatch()) {
                batch.put(TestSupport.bytes("batch-put"), TestSupport.bytes("value"));
                batch.delete(TestSupport.bytes("remove-me"));

                WriteHandle batchWrite = TestSupport.await(db.write(batch));
                assertNotNull(batchWrite);
                assertTrue(batchWrite.seqnum() > 0);

                assertArrayEquals(
                        TestSupport.bytes("value"), TestSupport.await(db.get(TestSupport.bytes("batch-put"))));
                assertNull(TestSupport.await(db.get(TestSupport.bytes("remove-me"))));

                TestSupport.awaitFailure(Error.Invalid.class, db.write(batch));
            }

            try (WriteBatch secondBatch = new WriteBatch()) {
                secondBatch.putWithOptions(
                        TestSupport.bytes("batch-put-2"),
                        TestSupport.bytes("value-2"),
                        new PutOptions(new Ttl.Default()));

                TestSupport.await(db.writeWithOptions(secondBatch, new WriteOptions(true)));
            }

            assertArrayEquals(
                    TestSupport.bytes("value-2"),
                    TestSupport.await(db.get(TestSupport.bytes("batch-put-2"))));
        }
    }

    @Test
    void dbFlush() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            TestSupport.await(db.put(TestSupport.bytes("flush-key"), TestSupport.bytes("value")));
            TestSupport.await(db.flush());
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.WAL)));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
        }
    }

    @Test
    void dbMerge() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle =
                        TestSupport.openDb(store, builder -> builder.withMergeOperator(new TestSupport.ConcatMergeOperator()))) {
            Db db = handle.db();

            TestSupport.await(db.put(TestSupport.bytes("merge"), TestSupport.bytes("base")));
            TestSupport.await(db.merge(TestSupport.bytes("merge"), TestSupport.bytes(":one")));
            assertArrayEquals(TestSupport.bytes("base:one"), TestSupport.await(db.get(TestSupport.bytes("merge"))));

            TestSupport.await(
                    db.mergeWithOptions(
                            TestSupport.bytes("merge"),
                            TestSupport.bytes(":two"),
                            new MergeOptions(new Ttl.Default()),
                            new WriteOptions(true)));
            assertArrayEquals(
                    TestSupport.bytes("base:one:two"),
                    TestSupport.await(db.get(TestSupport.bytes("merge"))));
        }
    }

    @Test
    void dbSnapshot() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            TestSupport.await(db.put(TestSupport.bytes("snapshot"), TestSupport.bytes("old")));

            try (DbSnapshot snapshot = TestSupport.await(db.snapshot())) {
                TestSupport.await(db.put(TestSupport.bytes("snapshot"), TestSupport.bytes("new")));

                assertArrayEquals(
                        TestSupport.bytes("old"),
                        TestSupport.await(snapshot.get(TestSupport.bytes("snapshot"))));
                assertArrayEquals(
                        TestSupport.bytes("new"),
                        TestSupport.await(db.get(TestSupport.bytes("snapshot"))));
            }
        }
    }

    @Test
    void dbTransactions() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            try (DbTransaction transaction = TestSupport.await(db.begin(IsolationLevel.SNAPSHOT))) {
                assertFalse(transaction.id().isEmpty());

                TestSupport.await(transaction.put(TestSupport.bytes("txn-key"), TestSupport.bytes("pending")));
                assertArrayEquals(
                        TestSupport.bytes("pending"),
                        TestSupport.await(transaction.get(TestSupport.bytes("txn-key"))));
                assertNull(TestSupport.await(db.get(TestSupport.bytes("txn-key"))));

                WriteHandle commitHandle = TestSupport.await(transaction.commit());
                assertNotNull(commitHandle);
                assertTrue(commitHandle.seqnum() > 0);
            }

            assertArrayEquals(
                    TestSupport.bytes("pending"), TestSupport.await(db.get(TestSupport.bytes("txn-key"))));

            try (DbTransaction rollbackTx = TestSupport.await(db.begin(IsolationLevel.SNAPSHOT))) {
                TestSupport.await(rollbackTx.put(TestSupport.bytes("rolled-back"), TestSupport.bytes("value")));
                TestSupport.await(rollbackTx.rollback());
            }

            assertNull(TestSupport.await(db.get(TestSupport.bytes("rolled-back"))));
        }
    }

    @Test
    void dbInvalidInputs() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(store)) {
            Db db = handle.db();

            TestSupport.awaitFailure(Error.Invalid.class, db.put(new byte[0], TestSupport.bytes("value")));
            TestSupport.awaitFailure(Error.Invalid.class, db.delete(new byte[0]));
            TestSupport.awaitFailure(
                    Error.Invalid.class,
                    db.scan(
                            new KeyRange(
                                    TestSupport.bytes("z"),
                                    true,
                                    TestSupport.bytes("a"),
                                    true)));

            // Scan with empty start bound should succeed and be treated as unbounded start.
            TestSupport.await(db.put(TestSupport.bytes("seed"), TestSupport.bytes("value")));
            try (DbIterator iterator =
                    TestSupport.await(db.scan(new KeyRange(new byte[0], true, null, false)))) {
                TestSupport.assertRows(
                        TestSupport.drainIterator(iterator),
                        new String[] {"seed"},
                        new String[] {"value"});
            }

            try (WriteBatch batch = new WriteBatch()) {
                batch.put(TestSupport.bytes("batch"), TestSupport.bytes("value"));
                TestSupport.await(db.write(batch));
                TestSupport.awaitFailure(Error.Invalid.class, db.write(batch));
            }
        }
    }

    @Test
    void dbWriterFencing() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb primary = TestSupport.openDb(store)) {
            Db primaryDb = primary.db();
            TestSupport.await(primaryDb.put(TestSupport.bytes("primary"), TestSupport.bytes("value")));

            try (TestSupport.ManagedDb secondary = TestSupport.openDb(store)) {
                Db secondaryDb = secondary.db();
                TestSupport.await(secondaryDb.put(TestSupport.bytes("secondary"), TestSupport.bytes("value")));
            }

            Error.Closed error =
                    TestSupport.awaitFailure(
                            Error.Closed.class,
                            primaryDb.put(TestSupport.bytes("stale"), TestSupport.bytes("value")));
            primary.markClosed();

            assertEquals(CloseReason.FENCED, error.reason());
        }
    }
}
