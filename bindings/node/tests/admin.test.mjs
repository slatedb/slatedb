import assert from "node:assert/strict";
import test from "node:test";

import { AdminBuilder, FlushType } from "../index.js";
import {
  bytes,
  createCleanup,
  expectInvalid,
  newMemoryStore,
  openDb,
  openReader,
  putOptions,
  uniquePath,
  waitUntil,
  writeOptions,
} from "./support.mjs";

const MAX_I64 = 9_223_372_036_854_775_807n;
const MAX_U64 = 18_446_744_073_709_551_615n;
const VALID_ULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

function openAdmin(store, { path, configure, cleanup } = {}) {
  const builder = cleanup.track(new AdminBuilder(path, store), { shutdown: false });
  if (configure != null) {
    configure(builder);
  }
  const admin = builder.build();
  return cleanup.track(admin, { shutdown: false });
}

test("admin builder accepts configuration and is single use", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-builder");
  const mainStore = cleanup.track(newMemoryStore());
  const walStore = cleanup.track(newMemoryStore());

  const db = await openDb(mainStore, {
    path,
    cleanup,
    configure(builder) {
      builder.with_wal_object_store(walStore);
    },
  });
  await db.shutdown();

  const adminBuilder = cleanup.track(new AdminBuilder(path, mainStore), { shutdown: false });
  adminBuilder.with_seed(7n);
  adminBuilder.with_wal_object_store(walStore);

  const admin = cleanup.track(adminBuilder.build(), { shutdown: false });
  const latest = await admin.read_manifest(undefined);
  assert.notEqual(latest, undefined);
  assert.ok(BigInt(latest.id) > 0n);

  const configureAfterBuild = await expectInvalid(
    () => adminBuilder.with_seed(9n),
    { message: "builder has already been consumed" },
  );
  assert.match(configureAfterBuild.message, /builder has already been consumed/);

  const secondBuild = await expectInvalid(
    () => adminBuilder.build(),
    { message: "builder has already been consumed" },
  );
  assert.match(secondBuild.message, /builder has already been consumed/);
});

test("admin manifest read list and state view", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-manifest");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  const firstWrite = await db.put_with_options(
    bytes("alpha"),
    bytes("one"),
    putOptions(),
    writeOptions(false),
  );
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const secondWrite = await db.put_with_options(
    bytes("beta"),
    bytes("two"),
    putOptions(),
    writeOptions(false),
  );
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const latest = await admin.read_manifest(undefined);
  assert.notEqual(latest, undefined);
  assert.ok(BigInt(latest.id) >= 3n);
  assert.ok(BigInt(latest.last_l0_seq) >= BigInt(secondWrite.seqnum));

  const first = await admin.read_manifest(1n);
  assert.notEqual(first, undefined);
  assert.equal(BigInt(first.id), 1n);
  assert.equal(BigInt(first.last_l0_seq), 0n);

  assert.equal(await admin.read_manifest(99_999n), undefined);

  const manifests = await admin.list_manifests(undefined, undefined);
  assert.ok(manifests.length >= 3);
  assert.equal(BigInt(manifests[0].id), 1n);
  assert.equal(BigInt(manifests.at(-1).id), BigInt(latest.id));

  const bounded = await admin.list_manifests(2n, 3n);
  assert.deepEqual(bounded.map((manifest) => BigInt(manifest.id)), [2n]);

  const reversed = await admin.list_manifests(3n, 2n);
  assert.deepEqual(reversed, []);

  const stateView = await admin.read_compactor_state_view();
  assert.equal(BigInt(stateView.manifest.id), BigInt(latest.id));
  assert.ok(BigInt(firstWrite.seqnum) > 0n);
});

test("admin compaction queries handle empty store and invalid ids", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-compactions");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });

  assert.equal(await admin.read_compactions(undefined), undefined);
  assert.equal(await admin.read_compactions(1n), undefined);
  assert.equal(await admin.read_compaction(VALID_ULID, undefined), undefined);

  const compactions = await admin.list_compactions(undefined, undefined);
  assert.deepEqual(compactions, []);

  const empty = await admin.list_compactions(2n, 2n);
  assert.deepEqual(empty, []);

  const invalidCompactionId = await expectInvalid(
    () => admin.read_compaction("not-a-ulid", undefined),
  );
  assert.match(invalidCompactionId.message, /invalid compaction_id ULID/);
});

test("admin run_gc_once accepts default and custom options", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-run-gc-once");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key"), bytes("value"));
  await db.flush();

  await admin.run_gc_once(undefined);

  const directoryOptions = {
    interval_ms: undefined,
    min_age_ms: 0n,
    dry_run: true,
  };
  await admin.run_gc_once({
    manifest_options: undefined,
    wal_options: directoryOptions,
    wal_fence_options: directoryOptions,
    compacted_options: undefined,
    compactions_options: undefined,
    detach_options: { interval_ms: undefined },
  });
});

test("admin checkpoint listing tracks reader lifecycle", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoints");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });
  await db.shutdown();

  const reader = await openReader(store, { path, cleanup });
  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 1);

  const checkpoint = checkpoints[0];
  assert.notEqual(checkpoint.id, "");
  assert.ok(BigInt(checkpoint.manifest_id) > 0n);
  assert.ok(BigInt(checkpoint.create_time_secs) > 0n);
  assert.equal(checkpoint.name, undefined);

  const unnamed = await admin.list_checkpoints("");
  assert.equal(unnamed.length, 1);
  assert.equal(unnamed[0].id, checkpoint.id);

  const missing = await admin.list_checkpoints("non-existent");
  assert.deepEqual(missing, []);

  assert.notEqual(reader, undefined);
  await reader.shutdown();

  await waitUntil(async () => {
    return (await admin.list_checkpoints(undefined)).length === 0;
  });
});

test("admin clone merges sources into destination", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());

  const sources = [];
  for (let i = 0; i < 3; i++) {
    const path = uniquePath(`admin-clone-original-${i}`);
    const db = await openDb(store, { path, cleanup });
    await db.put(bytes(`k${i}`), bytes(`v${i}`));
    await db.flush();
    await db.shutdown();
    sources.push({ path, checkpoint: undefined, projection_range: undefined });
  }

  const clonePath = uniquePath("admin-clone-clone");
  const admin = openAdmin(store, { path: clonePath, cleanup });
  const cloneBuilder = cleanup.track(admin.create_clone_builder_from_source(sources[0]), { shutdown: false });
  for (const source of sources.slice(1)) {
    cloneBuilder.with_source(source);
  }
  await cloneBuilder.build();

  const db = await openDb(store, { path: clonePath, cleanup });
  for (let i = 0; i < 3; i++) {
    assert.deepEqual(await db.get(bytes(`k${i}`)), bytes(`v${i}`));
  }
});

test("admin sequence lookups use persisted tracker", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-seq-tracker");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  const firstWrite = await db.put_with_options(
    bytes("k1"),
    bytes("v1"),
    putOptions(),
    writeOptions(false),
  );
  await db.put_with_options(
    bytes("k2"),
    bytes("v2"),
    putOptions(),
    writeOptions(false),
  );
  const thirdWrite = await db.put_with_options(
    bytes("k3"),
    bytes("v3"),
    putOptions(),
    writeOptions(false),
  );
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const firstTimestamp = await admin.get_timestamp_for_sequence(firstWrite.seqnum, true);
  assert.notEqual(firstTimestamp, undefined);

  const afterLastTimestamp = await admin.get_timestamp_for_sequence(MAX_U64, true);
  assert.equal(afterLastTimestamp, undefined);

  const seqBeforeFirst = await admin.get_sequence_for_timestamp(1n, false);
  assert.equal(seqBeforeFirst, undefined);

  const tomorrow = BigInt(Math.trunc(Date.now() / 1_000) + 86_400);
  const seqAfterLast = await admin.get_sequence_for_timestamp(tomorrow, false);
  assert.notEqual(seqAfterLast, undefined);
  assert.ok(BigInt(seqAfterLast) > 0n);
  assert.ok(BigInt(thirdWrite.seqnum) > BigInt(firstWrite.seqnum));

  const invalidTimestamp = await expectInvalid(
    () => admin.get_sequence_for_timestamp(MAX_I64, false),
  );
  assert.match(invalidTimestamp.message, /invalid timestamp seconds/);
});

test("admin create detached checkpoint without options", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-create");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const options = {
    lifetime_ms: undefined,
    source: undefined,
    name: undefined,
  };
  const result = await admin.create_detached_checkpoint(options);

  assert.notEqual(result, undefined);
  assert.notEqual(result.id, "");
  assert.ok(BigInt(result.manifest_id) > 0n);

  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 1);
  assert.equal(checkpoints[0].id, result.id);
  assert.equal(BigInt(checkpoints[0].manifest_id), BigInt(result.manifest_id));
  assert.equal(checkpoints[0].name, undefined);
  assert.equal(checkpoints[0].expire_time_secs, undefined);
});

test("admin create detached checkpoint with lifetime", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-lifetime");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const lifetimeMs = 60_000n;
  const options = {
    lifetime_ms: lifetimeMs,
    source: undefined,
    name: undefined,
  };
  const result = await admin.create_detached_checkpoint(options);

  assert.notEqual(result, undefined);
  assert.notEqual(result.id, "");

  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 1);
  const checkpoint = checkpoints[0];
  assert.equal(checkpoint.id, result.id);
  assert.notEqual(checkpoint.expire_time_secs, undefined);
  assert.ok(BigInt(checkpoint.expire_time_secs) > BigInt(checkpoint.create_time_secs));

  const expectedExpireSecs = BigInt(checkpoint.create_time_secs) + (lifetimeMs / 1000n);
  const delta = BigInt(checkpoint.expire_time_secs) - expectedExpireSecs;
  assert.ok(delta >= -2n && delta <= 2n, "Expire time should be approximately create time + lifetime");
});

test("admin create detached checkpoint with name", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-name");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const checkpointName = "backup-2026-06-25";
  const options = {
    lifetime_ms: undefined,
    source: undefined,
    name: checkpointName,
  };
  const result = await admin.create_detached_checkpoint(options);

  assert.notEqual(result, undefined);
  assert.notEqual(result.id, "");

  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 1);
  assert.equal(checkpoints[0].id, result.id);
  assert.equal(checkpoints[0].name, checkpointName);

  const filteredByName = await admin.list_checkpoints(checkpointName);
  assert.equal(filteredByName.length, 1);
  assert.equal(filteredByName[0].id, result.id);

  const filteredOther = await admin.list_checkpoints("other-name");
  assert.equal(filteredOther.length, 0);
});

test("admin create detached checkpoint from source", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-source");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const firstOptions = {
    lifetime_ms: undefined,
    source: undefined,
    name: "first-checkpoint",
  };
  const firstResult = await admin.create_detached_checkpoint(firstOptions);

  const secondOptions = {
    lifetime_ms: 120_000n,
    source: firstResult.id,
    name: "second-checkpoint",
  };
  const secondResult = await admin.create_detached_checkpoint(secondOptions);

  assert.notEqual(secondResult, undefined);
  assert.notEqual(secondResult.id, "");
  assert.equal(BigInt(secondResult.manifest_id), BigInt(firstResult.manifest_id));

  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 2);
});

test("admin refresh checkpoint updates lifetime", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-refresh");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const initialLifetimeMs = 30_000n;
  const options = {
    lifetime_ms: initialLifetimeMs,
    source: undefined,
    name: "refresh-test",
  };
  const result = await admin.create_detached_checkpoint(options);

  const checkpoints = await admin.list_checkpoints(undefined);
  const initial = checkpoints[0];
  const initialExpireTime = BigInt(initial.expire_time_secs);

  await new Promise((resolve) => setTimeout(resolve, 1000));

  const newLifetimeMs = 90_000n;
  await admin.refresh_checkpoint(result.id, newLifetimeMs);

  const updatedCheckpoints = await admin.list_checkpoints(undefined);
  const refreshed = updatedCheckpoints[0];
  assert.notEqual(refreshed.expire_time_secs, undefined);
  assert.ok(
    BigInt(refreshed.expire_time_secs) > initialExpireTime,
    "Refreshed expire time should be later than initial"
  );
});

test("admin refresh checkpoint without lifetime", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-refresh-no-lifetime");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const options = {
    lifetime_ms: undefined,
    source: undefined,
    name: undefined,
  };
  const result = await admin.create_detached_checkpoint(options);

  await admin.refresh_checkpoint(result.id, undefined);

  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 1);
  assert.equal(checkpoints[0].id, result.id);
});

test("admin refresh checkpoint with invalid id fails", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-refresh-invalid");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const error = await expectInvalid(() => admin.refresh_checkpoint("invalid-uuid", 60_000n));
  assert.match(error.message, /invalid checkpoint_id/);
});

test("admin delete checkpoint removes checkpoint", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-delete");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const options = {
    lifetime_ms: undefined,
    source: undefined,
    name: "delete-test",
  };
  const result = await admin.create_detached_checkpoint(options);

  const checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 1);
  assert.equal(checkpoints[0].id, result.id);

  await admin.delete_checkpoint(result.id);

  await waitUntil(async () => {
    return (await admin.list_checkpoints(undefined)).length === 0;
  });
});

test("admin delete checkpoint with invalid id fails", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-delete-invalid");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const error = await expectInvalid(() => admin.delete_checkpoint("invalid-uuid"));
  assert.match(error.message, /invalid checkpoint_id/);
});

test("admin delete multiple checkpoints", async (t) => {
  const cleanup = createCleanup(t);
  const path = uniquePath("admin-checkpoint-delete-multiple");
  const store = cleanup.track(newMemoryStore());
  const admin = openAdmin(store, { path, cleanup });
  const db = await openDb(store, { path, cleanup });

  await db.put(bytes("key1"), bytes("value1"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const first = await admin.create_detached_checkpoint({
    lifetime_ms: undefined,
    source: undefined,
    name: "checkpoint-1",
  });
  const second = await admin.create_detached_checkpoint({
    lifetime_ms: undefined,
    source: undefined,
    name: "checkpoint-2",
  });
  const third = await admin.create_detached_checkpoint({
    lifetime_ms: undefined,
    source: undefined,
    name: "checkpoint-3",
  });

  let checkpoints = await admin.list_checkpoints(undefined);
  assert.equal(checkpoints.length, 3);

  await admin.delete_checkpoint(first.id);
  checkpoints = await admin.list_checkpoints(undefined);
  assert.ok(checkpoints.every((c) => c.id !== first.id));

  await admin.delete_checkpoint(second.id);
  await admin.delete_checkpoint(third.id);

  await waitUntil(async () => {
    return (await admin.list_checkpoints(undefined)).length === 0;
  });
});
