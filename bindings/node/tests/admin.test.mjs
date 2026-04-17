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

  const invalidRange = await expectInvalid(
    () => admin.list_manifests(3n, 2n),
  );
  assert.match(invalidRange.message, /range start must not be greater than range end/);

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

  const invalidRange = await expectInvalid(
    () => admin.list_compactions(2n, 2n),
  );
  assert.match(invalidRange.message, /range must be non-empty/);

  const invalidCompactionId = await expectInvalid(
    () => admin.read_compaction("not-a-ulid", undefined),
  );
  assert.match(invalidCompactionId.message, /invalid compaction_id ULID/);
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
