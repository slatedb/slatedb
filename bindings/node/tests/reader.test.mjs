import assert from "node:assert/strict";
import test from "node:test";

import {
  CloseReason,
  DbReaderBuilder,
  ErrorData,
  FlushType,
} from "../index.js";
import {
  ConcatMergeOperator,
  TEST_DB_PATH,
  bytes,
  createCleanup,
  drainIterator,
  expectClosed,
  expectError,
  expectInvalid,
  fullRange,
  newMemoryStore,
  openDb,
  openReader,
  readOptions,
  readerOptions,
  requireRows,
  scanOptions,
  waitUntil,
} from "./support.mjs";

test("reader lifecycle", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("reader"), bytes("value"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const reader = await openReader(store, { cleanup });
  assert.deepEqual(await reader.get(bytes("reader")), bytes("value"));

  await reader.shutdown();

  await expectClosed(
    () => reader.get(bytes("reader")),
    {
      reason: CloseReason.Clean,
      message: "Closed error: db is closed",
    },
  );
});

test("reader build fails when database is missing", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const builder = cleanup.track(new DbReaderBuilder(TEST_DB_PATH, store), { shutdown: false });

  const error = await expectError(() => builder.build(), ErrorData);
  assert.match(error.message, /failed to find latest transactional object/);
});

test("reader point reads", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("alpha"), bytes("one"));
  await db.put(bytes("empty"), bytes(""));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const reader = await openReader(store, { cleanup });
  assert.deepEqual(await reader.get(bytes("alpha")), bytes("one"));
  assert.deepEqual(
    await reader.get_with_options(bytes("alpha"), readOptions()),
    bytes("one"),
  );
  assert.deepEqual(await reader.get(bytes("empty")), bytes(""));
  assert.equal(await reader.get(bytes("missing")), undefined);
});

test("reader scan variants", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("item:01"), bytes("first"));
  await db.put(bytes("item:02"), bytes("second"));
  await db.put(bytes("item:03"), bytes("third"));
  await db.put(bytes("other:01"), bytes("other"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const reader = await openReader(store, { cleanup });

  const fullScan = cleanup.track(await reader.scan(fullRange()));
  requireRows(
    await drainIterator(fullScan),
    ["item:01", "item:02", "item:03", "other:01"],
    ["first", "second", "third", "other"],
  );

  const boundedScan = cleanup.track(await reader.scan({
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

  const scanWithOptions = cleanup.track(await reader.scan_with_options(
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

  const prefixScan = cleanup.track(await reader.scan_prefix(bytes("item:")));
  requireRows(
    await drainIterator(prefixScan),
    ["item:01", "item:02", "item:03"],
    ["first", "second", "third"],
  );

  const prefixScanWithOptions = cleanup.track(await reader.scan_prefix_with_options(
    bytes("item:"),
    scanOptions(32, false, 1),
  ));
  requireRows(
    await drainIterator(prefixScanWithOptions),
    ["item:01", "item:02", "item:03"],
    ["first", "second", "third"],
  );
});

test("reader refresh polling updates visible state", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("seed"), bytes("value"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const reader = await openReader(store, {
    cleanup,
    configure(builder) {
      builder.with_options(readerOptions(false));
    },
  });

  await db.put(bytes("refresh"), bytes("updated"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  await waitUntil(async () => {
    return Buffer.compare(
      Buffer.from(await reader.get(bytes("refresh")) ?? []),
      Buffer.from(bytes("updated")),
    ) === 0;
  });
});

test("reader default mode replays new wal data", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });
  const reader = await openReader(store, {
    cleanup,
    configure(builder) {
      builder.with_options(readerOptions(false));
    },
  });

  await db.put(bytes("wal-key"), bytes("wal-value"));
  await db.flush_with_options({ flush_type: FlushType.Wal });

  await waitUntil(async () => {
    return Buffer.compare(
      Buffer.from(await reader.get(bytes("wal-key")) ?? []),
      Buffer.from(bytes("wal-value")),
    ) === 0;
  });
});

test("reader skip wal replay ignores wal-only data", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("flushed-key"), bytes("flushed-value"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  await db.put(bytes("wal-only-key"), bytes("wal-only-value"));
  await db.flush_with_options({ flush_type: FlushType.Wal });

  const initialReader = await openReader(store, {
    cleanup,
    configure(builder) {
      builder.with_options(readerOptions(true));
    },
  });
  assert.deepEqual(await initialReader.get(bytes("flushed-key")), bytes("flushed-value"));
  assert.equal(await initialReader.get(bytes("wal-only-key")), undefined);

  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const refreshedReader = await openReader(store, {
    cleanup,
    configure(builder) {
      builder.with_options(readerOptions(true));
    },
  });
  assert.deepEqual(
    await refreshedReader.get(bytes("wal-only-key")),
    bytes("wal-only-value"),
  );
});

test("reader merge operator", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, {
    cleanup,
    configure(builder) {
      builder.with_merge_operator(new ConcatMergeOperator());
    },
  });

  await db.put(bytes("merge"), bytes("base"));
  await db.merge(bytes("merge"), bytes(":reader"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const reader = await openReader(store, {
    cleanup,
    configure(builder) {
      builder.with_merge_operator(new ConcatMergeOperator());
    },
  });
  assert.deepEqual(await reader.get(bytes("merge")), bytes("base:reader"));
});

test("reader builder validation and errors", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("seed"), bytes("value"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const invalidBuilder = cleanup.track(new DbReaderBuilder(TEST_DB_PATH, store), { shutdown: false });
  const invalidCheckpointError = await expectInvalid(
    () => invalidBuilder.with_checkpoint_id("not-a-uuid"),
  );
  assert.match(invalidCheckpointError.message, /^invalid checkpoint_id UUID:/);

  const missingCheckpointId = "ffffffff-ffff-ffff-ffff-ffffffffffff";
  const missingBuilder = cleanup.track(new DbReaderBuilder(TEST_DB_PATH, store), { shutdown: false });
  missingBuilder.with_checkpoint_id(missingCheckpointId);
  const missingCheckpointError = await expectError(
    () => missingBuilder.build(),
    ErrorData,
  );
  assert.match(missingCheckpointError.message, /checkpoint missing/);
  assert.match(missingCheckpointError.message, new RegExp(missingCheckpointId));

  const consumedBuilder = cleanup.track(new DbReaderBuilder(TEST_DB_PATH, store), { shutdown: false });
  const reader = cleanup.track(await consumedBuilder.build());
  await reader.shutdown();

  await expectInvalid(
    () => consumedBuilder.build(),
    { message: "builder has already been consumed" },
  );
});

test("reader rejects invalid key ranges", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put(bytes("seed"), bytes("value"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });

  const reader = await openReader(store, { cleanup });

  await expectInvalid(
    () => reader.scan({
      start: bytes(""),
      start_inclusive: true,
      end: undefined,
      end_inclusive: false,
    }),
    { message: "range start cannot be empty" },
  );

  await expectInvalid(
    () => reader.scan({
      start: bytes("z"),
      start_inclusive: true,
      end: bytes("a"),
      end_inclusive: true,
    }),
    { message: "range start must not be greater than range end" },
  );

  await expectInvalid(
    () => reader.scan({
      start: bytes("a"),
      start_inclusive: true,
      end: bytes("a"),
      end_inclusive: false,
    }),
    { message: "range must be non-empty" },
  );
});
