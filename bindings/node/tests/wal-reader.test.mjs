import assert from "node:assert/strict";
import test from "node:test";

import {
  RowEntryKind,
  appendWalValue,
  createCleanup,
  newMemoryStore,
  openSlateDbWalReader,
  readWalBatchesThrough,
  requireWalRow,
  seedWalFiles,
} from "./support.mjs";

function cursorOf(batch) {
  return BigInt(batch.last_consumed_wal_file_id);
}

function rowsOf(batches) {
  return batches.flatMap((batch) => batch.rows);
}

test("wal reader reports no new files after the cursor", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);
  const reader = openSlateDbWalReader(store, { cleanup });

  const cursor = BigInt(await reader.last_wal_file_id(0n));
  assert.equal(BigInt(await reader.last_wal_file_id(cursor)), cursor);
});

test("wal reader streams new WALs through one iterator", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);
  const reader = openSlateDbWalReader(store, { cleanup });

  const firstTail = BigInt(await reader.last_wal_file_id(0n));
  const iterator = cleanup.track(await reader.iterator(1n));
  const firstBatches = await readWalBatchesThrough(iterator, firstTail);
  assert.ok(firstBatches.length > 0);
  assert.ok(firstBatches.some((batch) => batch.rows.length === 0));
  const firstCursors = firstBatches.map(cursorOf);
  assert.equal(firstCursors.at(-1), firstTail);
  assert.ok(firstCursors.every((cursor, index) => index === 0 || cursor > firstCursors[index - 1]));
  assert.equal(BigInt(await reader.last_wal_file_id(firstTail)), firstTail);

  await appendWalValue(store, "next", "3");
  const secondTail = BigInt(await reader.last_wal_file_id(firstTail));
  assert.ok(secondTail > firstTail);
  const secondBatches = await readWalBatchesThrough(iterator, secondTail);
  assert.ok(secondBatches.length > 0);
  assert.equal(cursorOf(secondBatches.at(-1)), secondTail);
  assert.deepEqual(
    rowsOf(secondBatches).map((row) => Buffer.from(row.key).toString("utf8")),
    ["next"],
  );
  assert.equal(BigInt(await reader.last_wal_file_id(secondTail)), secondTail);
});

test("wal reader decodes value, tombstone, and merge rows", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);
  const reader = openSlateDbWalReader(store, { cleanup });

  const tail = BigInt(await reader.last_wal_file_id(0n));
  const batches = await readWalBatchesThrough(cleanup.track(await reader.iterator(1n)), tail);
  assert.equal(cursorOf(batches.at(-1)), tail);
  const rows = rowsOf(batches);

  assert.equal(rows.length, 4);
  assert.ok(rows.every((row) => BigInt(row.seq) > 0n));
  requireWalRow(rows[0], RowEntryKind.Value, "a", "1");
  requireWalRow(rows[1], RowEntryKind.Value, "b", "2");
  requireWalRow(rows[2], RowEntryKind.Tombstone, "a", undefined);
  requireWalRow(rows[3], RowEntryKind.Merge, "m", "x");
});

test("wal reader can start at the next WAL", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  await seedWalFiles(store);
  const reader = openSlateDbWalReader(store, { cleanup });
  const tail = BigInt(await reader.last_wal_file_id(0n));

  const iterator = cleanup.track(await reader.iterator(tail + 1n));
  await appendWalValue(store, "resumed", "4");
  const newTail = BigInt(await reader.last_wal_file_id(tail));
  const batches = await readWalBatchesThrough(iterator, newTail);
  assert.deepEqual(
    rowsOf(batches).map((row) => Buffer.from(row.key).toString("utf8")),
    ["resumed"],
  );
});
