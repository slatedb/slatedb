import assert from "node:assert/strict";
import test from "node:test";

import {
  CloseReason,
  FlushType,
  IsolationLevel,
  WriteBatch,
} from "../index.js";
import {
  ConcatMergeOperator,
  bytes,
  createCleanup,
  drainIterator,
  expectClosed,
  expectInvalid,
  fullRange,
  mergeOptions,
  newMemoryStore,
  openDb,
  putOptions,
  readOptions,
  requireRows,
  scanOptions,
  writeOptions,
} from "./support.mjs";

test("db lifecycle and status", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  db.status();
  await db.put(bytes("lifecycle"), bytes("value"));
  await db.shutdown();

  await expectClosed(
    () => db.status(),
    {
      reason: CloseReason.Clean,
      message: "Closed error: db is closed",
    },
  );

  await expectClosed(
    () => db.put(bytes("after-shutdown"), bytes("value")),
    {
      reason: CloseReason.Clean,
      message: "Closed error: db is closed",
    },
  );
});

test("db crud and metadata", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  const firstWrite = await db.put_with_options(
    bytes("alpha"),
    bytes("one"),
    putOptions(),
    writeOptions(false),
  );
  assert.ok(firstWrite.seqnum > 0);
  assert.ok(firstWrite.create_ts > 0);

  assert.deepEqual(await db.get(bytes("alpha")), bytes("one"));
  assert.deepEqual(
    await db.get_with_options(bytes("alpha"), readOptions()),
    bytes("one"),
  );

  const metadata = await db.get_key_value(bytes("alpha"));
  assert.notEqual(metadata, undefined);
  assert.deepEqual(metadata.key, bytes("alpha"));
  assert.deepEqual(metadata.value, bytes("one"));
  assert.deepEqual(metadata.seq, firstWrite.seqnum);
  assert.deepEqual(metadata.create_ts, firstWrite.create_ts);

  const metadataWithOptions = await db.get_key_value_with_options(bytes("alpha"), readOptions());
  assert.notEqual(metadataWithOptions, undefined);
  assert.deepEqual(metadataWithOptions.value, bytes("one"));

  const secondWrite = await db.put_with_options(
    bytes("beta"),
    bytes("two"),
    putOptions(),
    writeOptions(false),
  );
  assert.ok(secondWrite.seqnum > firstWrite.seqnum);
  assert.ok(secondWrite.create_ts > 0);
  assert.deepEqual(await db.get(bytes("beta")), bytes("two"));

  await db.put_with_options(
    bytes("empty"),
    bytes(""),
    putOptions(),
    writeOptions(false),
  );
  assert.deepEqual(await db.get(bytes("empty")), bytes(""));
  assert.equal(await db.get(bytes("missing")), undefined);

  const deleteAlpha = await db.delete_with_options(bytes("alpha"), writeOptions(false));
  assert.ok(deleteAlpha.seqnum > secondWrite.seqnum);
  assert.equal(await db.get(bytes("alpha")), undefined);

  const deleteBeta = await db.delete_with_options(bytes("beta"), writeOptions(false));
  assert.ok(deleteBeta.seqnum > secondWrite.seqnum);
  assert.equal(await db.get(bytes("beta")), undefined);
});

test("db scan variants", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  const batch = cleanup.track(new WriteBatch());
  batch.put(bytes("item:01"), bytes("first"));
  batch.put(bytes("item:02"), bytes("second"));
  batch.put(bytes("item:03"), bytes("third"));
  batch.put(bytes("other:01"), bytes("other"));
  await db.write_with_options(batch, writeOptions(false));

  const fullScan = cleanup.track(await db.scan(fullRange()));
  requireRows(
    await drainIterator(fullScan),
    ["item:01", "item:02", "item:03", "other:01"],
    ["first", "second", "third", "other"],
  );

  const boundedScan = cleanup.track(await db.scan({
    start: bytes("item:02"),
    start_inclusive: true,
    end: bytes("item:03"),
    end_inclusive: true,
  }));
  requireRows(
    await drainIterator(boundedScan),
    ["item:02", "item:03"],
    ["second", "third"],
  );

  const scanWithOptions = cleanup.track(await db.scan_with_options(
    {
      start: bytes("item:01"),
      start_inclusive: true,
      end: bytes("item:99"),
      end_inclusive: false,
    },
    scanOptions(64, true, 2),
  ));
  requireRows(
    await drainIterator(scanWithOptions),
    ["item:01", "item:02", "item:03"],
    ["first", "second", "third"],
  );

  const prefixScan = cleanup.track(await db.scan_prefix(bytes("item:")));
  requireRows(
    await drainIterator(prefixScan),
    ["item:01", "item:02", "item:03"],
    ["first", "second", "third"],
  );

  const prefixScanWithOptions = cleanup.track(await db.scan_prefix_with_options(
    bytes("item:"),
    scanOptions(32, false, 1),
  ));
  requireRows(
    await drainIterator(prefixScanWithOptions),
    ["item:01", "item:02", "item:03"],
    ["first", "second", "third"],
  );
});

test("db batch write and consumption", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put_with_options(
    bytes("remove-me"),
    bytes("old"),
    putOptions(),
    writeOptions(false),
  );

  const batch = cleanup.track(new WriteBatch());
  batch.put(bytes("batch-put"), bytes("value"));
  batch.delete(bytes("remove-me"));

  const batchWrite = await db.write(batch);
  assert.ok(batchWrite.seqnum > 0);
  assert.deepEqual(await db.get(bytes("batch-put")), bytes("value"));
  assert.equal(await db.get(bytes("remove-me")), undefined);

  await expectInvalid(
    () => db.write(batch),
    { message: "write batch has already been consumed" },
  );

  const secondBatch = cleanup.track(new WriteBatch());
  secondBatch.put_with_options(bytes("batch-put-2"), bytes("value-2"), putOptions());
  await db.write_with_options(secondBatch, writeOptions());
  assert.deepEqual(await db.get(bytes("batch-put-2")), bytes("value-2"));
});

test("db flush", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put_with_options(
    bytes("flush-key"),
    bytes("value"),
    putOptions(),
    writeOptions(false),
  );
  await db.flush();
  await db.flush_with_options({ flush_type: FlushType.Wal });
  await db.flush_with_options({ flush_type: FlushType.MemTable });
  assert.deepEqual(await db.get(bytes("flush-key")), bytes("value"));
});

test("db merge and merge_with_options", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, {
    cleanup,
    configure(builder) {
      builder.with_merge_operator(new ConcatMergeOperator());
    },
  });

  await db.put_with_options(
    bytes("merge"),
    bytes("base"),
    putOptions(),
    writeOptions(false),
  );
  await db.merge(bytes("merge"), bytes(":one"));
  assert.deepEqual(await db.get(bytes("merge")), bytes("base:one"));

  await db.merge_with_options(
    bytes("merge"),
    bytes(":two"),
    mergeOptions(),
    writeOptions(),
  );
  assert.deepEqual(await db.get(bytes("merge")), bytes("base:one:two"));
});

test("db snapshot isolation", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put_with_options(
    bytes("snapshot"),
    bytes("old"),
    putOptions(),
    writeOptions(false),
  );

  const snapshot = cleanup.track(await db.snapshot());
  await db.put_with_options(
    bytes("snapshot"),
    bytes("new"),
    putOptions(),
    writeOptions(false),
  );

  assert.deepEqual(await snapshot.get(bytes("snapshot")), bytes("old"));
  assert.deepEqual(await db.get(bytes("snapshot")), bytes("new"));
});

test("db transactions", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  const transaction = cleanup.track(await db.begin(IsolationLevel.Snapshot));
  assert.notEqual(transaction.id(), "");

  await transaction.put(bytes("txn-key"), bytes("pending"));
  assert.deepEqual(await transaction.get(bytes("txn-key")), bytes("pending"));
  assert.equal(await db.get(bytes("txn-key")), undefined);

  const commitHandle = await transaction.commit();
  assert.notEqual(commitHandle, undefined);
  assert.ok(commitHandle.seqnum > 0);
  assert.deepEqual(await db.get(bytes("txn-key")), bytes("pending"));

  const rollbackTx = cleanup.track(await db.begin(IsolationLevel.Snapshot));
  await rollbackTx.put(bytes("rolled-back"), bytes("value"));
  await rollbackTx.rollback();
  assert.equal(await db.get(bytes("rolled-back")), undefined);
});

test("db invalid inputs map to typed errors", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await expectInvalid(
    () => db.put(bytes(""), bytes("value")),
    { message: "key cannot be empty" },
  );

  await expectInvalid(
    () => db.delete(bytes("")),
    { message: "key cannot be empty" },
  );

  await expectInvalid(
    () => db.scan({
      start: bytes(""),
      start_inclusive: true,
      end: undefined,
      end_inclusive: false,
    }),
    { message: "range start cannot be empty" },
  );

  await expectInvalid(
    () => db.scan({
      start: bytes("z"),
      start_inclusive: true,
      end: bytes("a"),
      end_inclusive: true,
    }),
    { message: "range start must not be greater than range end" },
  );

  await expectInvalid(
    () => db.scan({
      start: bytes("a"),
      start_inclusive: true,
      end: bytes("a"),
      end_inclusive: false,
    }),
    { message: "range must be non-empty" },
  );
});

test("db writer fencing reports closed reason", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const primary = await openDb(store, { cleanup });
  await primary.put_with_options(
    bytes("primary"),
    bytes("value"),
    putOptions(),
    writeOptions(false),
  );

  const secondary = await openDb(store, { cleanup });
  await secondary.put_with_options(
    bytes("secondary"),
    bytes("value"),
    putOptions(),
    writeOptions(false),
  );

  const error = await expectClosed(
    () => primary.put(bytes("stale"), bytes("value")),
    { reason: CloseReason.Fenced },
  );
  assert.match(error.message, /detected newer DB client/);
});
