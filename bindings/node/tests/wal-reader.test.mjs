import assert from "node:assert/strict";
import test from "node:test";

import { ErrorData } from "../index.js";
import {
  RowEntryKind,
  createCleanup,
  drainWalIterator,
  expectError,
  newMemoryStore,
  openWalReader,
  requireWalRow,
  seedWalFiles,
} from "./support.mjs";

test("wal reader empty store listing", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const reader = openWalReader(store, { cleanup });

  assert.deepEqual(await reader.list(undefined, undefined), []);
});

test("wal reader listing bounds and navigation", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);

  const reader = openWalReader(store, { cleanup });
  const files = (await reader.list(undefined, undefined)).map((file) => cleanup.track(file));
  assert.ok(files.length >= 3);

  const ids = files.map((file) => BigInt(file.id()));
  assert.deepEqual(
    ids,
    [...ids].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0)),
  );

  const bounded = (await reader.list(ids[1], ids[2])).map((file) => cleanup.track(file));
  assert.deepEqual(
    bounded.map((file) => BigInt(file.id())),
    [ids[1]],
  );

  assert.deepEqual(await reader.list(ids.at(-1) + 1_000n, undefined), []);

  const first = cleanup.track(reader.get(ids[0]));
  assert.equal(BigInt(first.id()), ids[0]);
  assert.equal(BigInt(first.next_id()), ids[1]);

  const nextFile = cleanup.track(first.next_file());
  assert.equal(BigInt(nextFile.id()), ids[1]);
});

test("wal reader metadata and row decoding", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);

  const reader = openWalReader(store, { cleanup });
  const files = (await reader.list(undefined, undefined)).map((file) => cleanup.track(file));
  assert.ok(files.length >= 3);

  const allRows = [];
  for (const walFile of files) {
    const metadata = await walFile.metadata();
    assert.ok(BigInt(metadata.size_bytes) > 0n);
    assert.notEqual(metadata.location, "");

    const iterator = cleanup.track(await walFile.iterator());
    const rows = await drainWalIterator(iterator);
    for (const row of rows) {
      assert.ok(BigInt(row.seq) > 0n);
    }
    allRows.push(...rows);
  }

  assert.equal(allRows.length, 4);
  requireWalRow(allRows[0], RowEntryKind.Value, "a", "1");
  requireWalRow(allRows[1], RowEntryKind.Value, "b", "2");
  requireWalRow(allRows[2], RowEntryKind.Tombstone, "a", undefined);
  requireWalRow(allRows[3], RowEntryKind.Merge, "m", "x");
});

test("wal reader missing file metadata failure", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);

  const reader = openWalReader(store, { cleanup });
  const files = (await reader.list(undefined, undefined)).map((file) => cleanup.track(file));
  assert.ok(files.length > 0);

  const missingId = BigInt(files.at(-1).id()) + 1_000n;
  const missing = cleanup.track(reader.get(missingId));
  assert.equal(BigInt(missing.id()), missingId);

  const error = await expectError(() => missing.metadata(), ErrorData);
  assert.match(error.message, /not found/);
});
