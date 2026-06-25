package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
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
    void adminCloneMergesSourcesIntoDestination() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            List<CloneSourceSpec> sources = new ArrayList<>();

            for (int i = 0; i < 3; i++) {
                String sourcePath = TestSupport.uniquePath("admin-clone-original-" + i);
                try (TestSupport.ManagedDb handle = TestSupport.openDb(sourcePath, store, null)) {
                    Db db = handle.db();
                    TestSupport.await(db.put(TestSupport.bytes("k" + i), TestSupport.bytes("v" + i)));
                    TestSupport.await(db.flush());
                }
                sources.add(new CloneSourceSpec(sourcePath, null, null));
            }

            String clonePath = TestSupport.uniquePath("admin-clone-clone");
            try (AdminBuilder adminBuilder = new AdminBuilder(clonePath, store);
                    Admin admin = adminBuilder.build();
                    CloneBuilder cloneBuilder = admin.createCloneBuilderFromSource(sources.get(0))) {
                for (CloneSourceSpec source : sources.subList(1, sources.size())) {
                    cloneBuilder.withSource(source);
                }
                TestSupport.await(cloneBuilder.build());
            }

            try (TestSupport.ManagedDb handle = TestSupport.openDb(clonePath, store, null)) {
                Db db = handle.db();
                for (int i = 0; i < 3; i++) {
                    assertArrayEquals(
                            TestSupport.bytes("v" + i),
                            TestSupport.await(db.get(TestSupport.bytes("k" + i))));
                }
            }
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

    @Test
    void adminCreateDetachedCheckpointWithoutOptions() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-create");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            CheckpointOptions options = new CheckpointOptions(null, null, null);
            CheckpointCreateResult result = TestSupport.await(admin.createDetachedCheckpoint(options));

            assertNotNull(result);
            assertFalse(result.id().isEmpty());
            assertTrue(result.manifestId() > 0L);

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(1, checkpoints.size());
            assertEquals(result.id(), checkpoints.get(0).id());
            assertEquals(result.manifestId(), checkpoints.get(0).manifestId());
            assertNull(checkpoints.get(0).name());
            assertNull(checkpoints.get(0).expireTimeSecs());
        }
    }

    @Test
    void adminCreateDetachedCheckpointWithLifetime() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-lifetime");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            long lifetimeMs = 60_000L;
            CheckpointOptions options = new CheckpointOptions(lifetimeMs, null, null);
            CheckpointCreateResult result = TestSupport.await(admin.createDetachedCheckpoint(options));

            assertNotNull(result);
            assertFalse(result.id().isEmpty());

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(1, checkpoints.size());
            Checkpoint checkpoint = checkpoints.get(0);
            assertEquals(result.id(), checkpoint.id());
            assertNotNull(checkpoint.expireTimeSecs());
            assertTrue(checkpoint.expireTimeSecs() > checkpoint.createTimeSecs());

            long expectedExpireSecs = checkpoint.createTimeSecs() + (lifetimeMs / 1000);
            long delta = Math.abs(checkpoint.expireTimeSecs() - expectedExpireSecs);
            assertTrue(delta <= 2, "Expire time should be approximately create time + lifetime");
        }
    }

    @Test
    void adminCreateDetachedCheckpointWithName() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-name");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            String checkpointName = "backup-2026-06-25";
            CheckpointOptions options = new CheckpointOptions(null, null, checkpointName);
            CheckpointCreateResult result = TestSupport.await(admin.createDetachedCheckpoint(options));

            assertNotNull(result);
            assertFalse(result.id().isEmpty());

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(1, checkpoints.size());
            assertEquals(result.id(), checkpoints.get(0).id());
            assertEquals(checkpointName, checkpoints.get(0).name());

            List<Checkpoint> filteredByName = TestSupport.await(admin.listCheckpoints(checkpointName));
            assertEquals(1, filteredByName.size());
            assertEquals(result.id(), filteredByName.get(0).id());

            List<Checkpoint> filteredOther = TestSupport.await(admin.listCheckpoints("other-name"));
            assertEquals(0, filteredOther.size());
        }
    }

    @Test
    void adminCreateDetachedCheckpointFromSource() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-source");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            CheckpointOptions firstOptions = new CheckpointOptions(null, null, "first-checkpoint");
            CheckpointCreateResult firstResult = TestSupport.await(admin.createDetachedCheckpoint(firstOptions));

            CheckpointOptions secondOptions = new CheckpointOptions(120_000L, firstResult.id(), "second-checkpoint");
            CheckpointCreateResult secondResult = TestSupport.await(admin.createDetachedCheckpoint(secondOptions));

            assertNotNull(secondResult);
            assertFalse(secondResult.id().isEmpty());
            assertEquals(firstResult.manifestId(), secondResult.manifestId());

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(2, checkpoints.size());
        }
    }

    @Test
    void adminRefreshCheckpointUpdatesLifetime() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-refresh");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            long initialLifetimeMs = 30_000L;
            CheckpointOptions options = new CheckpointOptions(initialLifetimeMs, null, "refresh-test");
            CheckpointCreateResult result = TestSupport.await(admin.createDetachedCheckpoint(options));

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            Checkpoint initial = checkpoints.get(0);
            Long initialExpireTime = initial.expireTimeSecs();
            assertNotNull(initialExpireTime);

            Thread.sleep(1000);

            long newLifetimeMs = 90_000L;
            TestSupport.await(admin.refreshCheckpoint(result.id(), newLifetimeMs));

            checkpoints = TestSupport.await(admin.listCheckpoints(null));
            Checkpoint refreshed = checkpoints.get(0);
            assertNotNull(refreshed.expireTimeSecs());
            assertTrue(refreshed.expireTimeSecs() > initialExpireTime,
                    "Refreshed expire time should be later than initial");
        }
    }

    @Test
    void adminRefreshCheckpointWithoutLifetime() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-refresh-no-lifetime");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            CheckpointOptions options = new CheckpointOptions(null, null, null);
            CheckpointCreateResult result = TestSupport.await(admin.createDetachedCheckpoint(options));

            TestSupport.await(admin.refreshCheckpoint(result.id(), null));

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(1, checkpoints.size());
            assertEquals(result.id(), checkpoints.get(0).id());
        }
    }

    @Test
    void adminRefreshCheckpointWithInvalidIdFails() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-refresh-invalid");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            Error.Invalid error = TestSupport.awaitFailure(
                    Error.Invalid.class,
                    admin.refreshCheckpoint("invalid-uuid", 60_000L));
            assertTrue(error.getMessage().contains("invalid checkpoint_id"));
        }
    }

    @Test
    void adminDeleteCheckpointRemovesCheckpoint() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-delete");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            CheckpointOptions options = new CheckpointOptions(null, null, "delete-test");
            CheckpointCreateResult result = TestSupport.await(admin.createDetachedCheckpoint(options));

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(1, checkpoints.size());
            assertEquals(result.id(), checkpoints.get(0).id());

            TestSupport.await(admin.deleteCheckpoint(result.id()));

            TestSupport.waitUntil(
                    TestSupport.WAIT_TIMEOUT,
                    TestSupport.WAIT_STEP,
                    () -> TestSupport.await(admin.listCheckpoints(null)).isEmpty());
        }
    }

    @Test
    void adminDeleteCheckpointWithInvalidIdFails() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-delete-invalid");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            Error.Invalid error = TestSupport.awaitFailure(
                    Error.Invalid.class,
                    admin.deleteCheckpoint("invalid-uuid"));
            assertTrue(error.getMessage().contains("invalid checkpoint_id"));
        }
    }

    @Test
    void adminDeleteMultipleCheckpoints() throws Exception {
        String path = TestSupport.uniquePath("admin-checkpoint-delete-multiple");

        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(path, store, null);
                AdminBuilder adminBuilder = new AdminBuilder(path, store);
                Admin admin = adminBuilder.build()) {
            Db db = dbHandle.db();

            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));

            CheckpointCreateResult first = TestSupport.await(admin.createDetachedCheckpoint(
                    new CheckpointOptions(null, null, "checkpoint-1")));
            CheckpointCreateResult second = TestSupport.await(admin.createDetachedCheckpoint(
                    new CheckpointOptions(null, null, "checkpoint-2")));
            CheckpointCreateResult third = TestSupport.await(admin.createDetachedCheckpoint(
                    new CheckpointOptions(null, null, "checkpoint-3")));

            List<Checkpoint> checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertEquals(3, checkpoints.size());

            TestSupport.await(admin.deleteCheckpoint(first.id()));
            checkpoints = TestSupport.await(admin.listCheckpoints(null));
            assertTrue(checkpoints.stream().noneMatch(c -> c.id().equals(first.id())));

            TestSupport.await(admin.deleteCheckpoint(second.id()));
            TestSupport.await(admin.deleteCheckpoint(third.id()));

            TestSupport.waitUntil(
                    TestSupport.WAIT_TIMEOUT,
                    TestSupport.WAIT_STEP,
                    () -> TestSupport.await(admin.listCheckpoints(null)).isEmpty());
        }
    }
}
