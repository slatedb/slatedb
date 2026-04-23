package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

class SlateDbAdminTest {
    private static final String VALID_ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    @Test
    void adminBuilderAcceptsConfigurationAndIsSingleUse() throws Exception {
        String path = TestSupport.uniquePath("admin-builder");

        try (ObjectStore mainStore = TestSupport.newMemoryStore();
                ObjectStore walStore = TestSupport.newMemoryStore();
                DbBuilder dbBuilder = new DbBuilder(path, mainStore);
                AdminBuilder adminBuilder = new AdminBuilder(path, mainStore)) {
            dbBuilder.withWalObjectStore(walStore);
            try (Db db = TestSupport.await(dbBuilder.build())) {
                TestSupport.await(db.shutdown());
            }

            adminBuilder.withSeed(7L);
            adminBuilder.withWalObjectStore(walStore);

            try (Admin admin = adminBuilder.build()) {
                VersionedManifest manifest = TestSupport.await(admin.readManifest(null));
                assertNotNull(manifest);
                assertTrue(manifest.id() > 0L);
            }

            Error.Invalid configureAfterBuild =
                    TestSupport.expectFailure(Error.Invalid.class, () -> adminBuilder.withSeed(9L));
            assertTrue(configureAfterBuild.getMessage().contains("builder has already been consumed"));

            Error.Invalid secondBuild =
                    TestSupport.expectFailure(Error.Invalid.class, () -> adminBuilder.build());
            assertTrue(secondBuild.getMessage().contains("builder has already been consumed"));
        }
    }

    @Test
    void adminManifestReadAndList() throws Exception {
        String path = TestSupport.uniquePath("admin-manifest");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = handle.db();

            WriteHandle firstWrite = TestSupport.await(db.put(TestSupport.bytes("alpha"), TestSupport.bytes("one")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            WriteHandle secondWrite =
                    TestSupport.await(db.put(TestSupport.bytes("beta"), TestSupport.bytes("two")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            VersionedManifest latest = TestSupport.await(admin.readManifest(null));
            assertNotNull(latest);
            assertTrue(latest.id() >= 3L);
            assertTrue(latest.lastL0Seq() >= secondWrite.seqnum());

            VersionedManifest first = TestSupport.await(admin.readManifest(1L));
            assertNotNull(first);
            assertEquals(1L, first.id());
            assertEquals(0L, first.lastL0Seq());

            assertNull(TestSupport.await(admin.readManifest(99_999L)));

            List<VersionedManifest> manifests =
                    TestSupport.await(admin.listManifests(null, null));
            assertTrue(manifests.size() >= 3);
            assertEquals(1L, manifests.get(0).id());
            assertEquals(latest.id(), manifests.get(manifests.size() - 1).id());

            List<VersionedManifest> bounded = TestSupport.await(admin.listManifests(2L, 3L));
            assertEquals(1, bounded.size());
            assertEquals(2L, bounded.get(0).id());

            List<VersionedManifest> reversed = TestSupport.await(admin.listManifests(3L, 2L));
            assertEquals(0, reversed.size());

            CompactorStateView stateView = TestSupport.await(admin.readCompactorStateView());
            assertNotNull(stateView);
            assertEquals(latest.id(), stateView.manifest().id());
            assertTrue(firstWrite.seqnum() > 0L);
        }
    }

    @Test
    void adminCompactionQueriesHandleEmptyStoresAndInvalidIds() throws Exception {
        String path = TestSupport.uniquePath("admin-compactions");

        try (ObjectStore store = TestSupport.newMemoryStore();
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            assertNull(TestSupport.await(admin.readCompactions(null)));
            assertNull(TestSupport.await(admin.readCompactions(1L)));
            assertNull(TestSupport.await(admin.readCompaction(VALID_ULID, null)));

            List<VersionedCompactions> compactions =
                    TestSupport.await(admin.listCompactions(null, null));
            assertEquals(0, compactions.size());

            List<VersionedCompactions> empty = TestSupport.await(admin.listCompactions(2L, 2L));
            assertEquals(0, empty.size());

            Error.Invalid invalidCompactionId =
                    TestSupport.awaitFailure(Error.Invalid.class, admin.readCompaction("not-a-ulid", null));
            assertTrue(invalidCompactionId.getMessage().contains("invalid compaction_id ULID"));
        }
    }

    @Test
    void adminCheckpointListingTracksReaderLifecycle() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoints");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            TestSupport.await(dbHandle.db().shutdown());
            dbHandle.markClosed();

            try (TestSupport.ManagedReader readerHandle = TestSupport.openReader(path, store, null)) {
                List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
                assertEquals(1, checkpoints.size());

                Checkpoint checkpoint = checkpoints.get(0);
                assertFalse(checkpoint.id().isEmpty());
                assertTrue(checkpoint.manifestId() > 0L);
                assertTrue(checkpoint.createTimeSecs() > 0L);
                assertNull(checkpoint.name());

                List<Checkpoint> unnamed = TestSupport.await(admin.listCheckpoints(""));
                assertEquals(1, unnamed.size());
                assertEquals(checkpoint.id(), unnamed.get(0).id());

                List<Checkpoint> missing = TestSupport.await(admin.listCheckpoints("non-existent"));
                assertEquals(0, missing.size());

                assertNotNull(readerHandle.reader());
            }

            TestSupport.waitUntil(
                    TestSupport.WAIT_TIMEOUT,
                    TestSupport.WAIT_STEP,
                    () -> TestSupport.await(admin.listCheckpoints(null)).isEmpty());
        }
    }

    @Test
    void adminSequenceLookupsUsePersistedTracker() throws Exception {
        String path = TestSupport.uniquePath("admin-seq-tracker");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb handle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = handle.db();

            WriteHandle firstWrite = TestSupport.await(db.put(TestSupport.bytes("k1"), TestSupport.bytes("v1")));
            TestSupport.await(db.put(TestSupport.bytes("k2"), TestSupport.bytes("v2")));
            WriteHandle thirdWrite = TestSupport.await(db.put(TestSupport.bytes("k3"), TestSupport.bytes("v3")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            Long firstTimestamp = TestSupport.await(admin.getTimestampForSequence(firstWrite.seqnum(), true));
            assertNotNull(firstTimestamp);

            Long afterLastTimestamp = TestSupport.await(admin.getTimestampForSequence(Long.MAX_VALUE, true));
            assertNull(afterLastTimestamp);

            Long seqBeforeFirst = TestSupport.await(admin.getSequenceForTimestamp(1L, false));
            assertNull(seqBeforeFirst);

            Long seqAfterLast =
                    TestSupport.await(
                            admin.getSequenceForTimestamp(
                                    Instant.now().plusSeconds(86_400L).getEpochSecond(),
                                    false));
            assertNotNull(seqAfterLast);
            assertTrue(seqAfterLast > 0L);

            Error.Invalid invalidTimestamp =
                    TestSupport.awaitFailure(
                            Error.Invalid.class,
                            admin.getSequenceForTimestamp(Long.MAX_VALUE, false));
            assertTrue(invalidTimestamp.getMessage().contains("invalid timestamp seconds"));
        }
    }
}
