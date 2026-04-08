import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import {
  DbBuilder,
  DbReaderBuilder,
  ErrorClosed,
  ErrorInvalid,
  FlushType,
  LogLevel,
  ObjectStore,
  RowEntryKind,
  Ttl,
  WalReader,
} from "../index.js";

export const TEST_DB_PATH = "test-db";
export const WAIT_TIMEOUT_MS = 60_000;
export const WAIT_STEP_MS = 25;

export function bytes(value) {
  if (typeof value === "string") {
    return Uint8Array.from(Buffer.from(value, "utf8"));
  }
  if (value instanceof Uint8Array) {
    return Uint8Array.from(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice();
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value.slice(0));
  }
  return Uint8Array.from(value);
}

export function utf8(value) {
  return Buffer.from(value).toString("utf8");
}

export function newMemoryStore() {
  return ObjectStore.resolve("memory:///");
}

export function readOptions() {
  return {
    durability_filter: "Memory",
    dirty: false,
    cache_blocks: true,
    read_sources: { sources: [] },
  };
}

export function scanOptions(readAheadBytes, cacheBlocks, maxFetchTasks) {
  return {
    durability_filter: "Memory",
    dirty: false,
    read_ahead_bytes: readAheadBytes,
    cache_blocks: cacheBlocks,
    max_fetch_tasks: maxFetchTasks,
    read_sources: { sources: [] },
  };
}

export function readerOptions(skipWalReplay) {
  return {
    manifest_poll_interval_ms: 100,
    checkpoint_lifetime_ms: 1_000,
    max_memtable_bytes: 64 * 1024 * 1024,
    skip_wal_replay: skipWalReplay,
  };
}

export function writeOptions(awaitDurable = true) {
  return {
    await_durable: awaitDurable,
  };
}

export function putOptions() {
  return {
    ttl: Ttl.Default(),
  };
}

export function mergeOptions() {
  return {
    ttl: Ttl.Default(),
  };
}

export function fullRange() {
  return {
    start: undefined,
    start_inclusive: false,
    end: undefined,
    end_inclusive: false,
  };
}

export class CleanupScope {
  #resources = [];

  constructor(t) {
    t.after(async () => {
      await this.disposeAll();
    });
  }

  track(resource, options = {}) {
    if (resource != null) {
      this.#resources.push({
        resource,
        shutdown: options.shutdown ?? typeof resource.shutdown === "function",
      });
    }
    return resource;
  }

  async disposeAll() {
    while (this.#resources.length > 0) {
      const entry = this.#resources.pop();
      await shutdownAndDispose(entry.resource, { shutdown: entry.shutdown });
    }
  }
}

export function createCleanup(t) {
  return new CleanupScope(t);
}

export async function shutdownAndDispose(resource, { shutdown = typeof resource?.shutdown === "function" } = {}) {
  if (resource == null) {
    return;
  }

  if (shutdown && typeof resource.shutdown === "function") {
    try {
      await resource.shutdown();
    } catch {
      // Cleanup should not obscure the test failure that prompted teardown.
    }
  }

  if (typeof resource.dispose === "function") {
    try {
      resource.dispose();
    } catch {
      // Ignore cleanup-time disposal errors for already-closed handles.
    }
  }
}

export async function openDb(store, { path = TEST_DB_PATH, configure, cleanup } = {}) {
  const builder = new DbBuilder(path, store);
  try {
    if (configure != null) {
      await configure(builder);
    }
    const db = await builder.build();
    return cleanup?.track(db) ?? db;
  } finally {
    builder.dispose();
  }
}

export async function openReader(store, { path = TEST_DB_PATH, configure, cleanup } = {}) {
  const builder = new DbReaderBuilder(path, store);
  try {
    if (configure != null) {
      await configure(builder);
    }
    const reader = await builder.build();
    return cleanup?.track(reader) ?? reader;
  } finally {
    builder.dispose();
  }
}

export function openWalReader(store, { path = TEST_DB_PATH, cleanup } = {}) {
  const reader = new WalReader(path, store);
  return cleanup?.track(reader, { shutdown: false }) ?? reader;
}

export async function drainIterator(iterator) {
  const rows = [];
  for (;;) {
    const row = await iterator.next();
    if (row == null) {
      return rows;
    }
    rows.push(row);
  }
}

export async function drainWalIterator(iterator) {
  const rows = [];
  for (;;) {
    const row = await iterator.next();
    if (row == null) {
      return rows;
    }
    rows.push(row);
  }
}

export function requireRows(rows, wantKeys, wantValues) {
  assert.equal(rows.length, wantKeys.length);
  assert.equal(rows.length, wantValues.length);

  for (const [index, row] of rows.entries()) {
    assert.deepEqual(row.key, bytes(wantKeys[index]));
    assert.deepEqual(row.value, bytes(wantValues[index]));
  }
}

export function requireWalRow(row, kind, key, value) {
  assert.equal(row.kind, kind);
  assert.deepEqual(row.key, bytes(key));
  if (value == null) {
    assert.equal(row.value, undefined);
    return;
  }
  assert.deepEqual(row.value, bytes(value));
}

export async function expectError(action, ErrorClass, expected = {}) {
  try {
    if (typeof action === "function") {
      await action();
    } else {
      await action;
    }
  } catch (error) {
    assert.ok(
      error instanceof ErrorClass,
      `expected ${ErrorClass.name}, got ${error?.constructor?.name ?? typeof error}`,
    );
    for (const [key, value] of Object.entries(expected)) {
      assert.deepEqual(error[key], value, `expected error.${key}`);
    }
    return error;
  }

  assert.fail(`expected ${ErrorClass.name}`);
}

export async function expectClosed(action, expected) {
  return expectError(action, ErrorClosed, expected);
}

export async function expectInvalid(action, expected) {
  return expectError(action, ErrorInvalid, expected);
}

export async function waitUntil(check, { timeoutMs = WAIT_TIMEOUT_MS, stepMs = WAIT_STEP_MS } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastError = undefined;

  for (;;) {
    try {
      if (await check()) {
        return;
      }
      lastError = undefined;
    } catch (error) {
      lastError = error;
    }

    if (Date.now() >= deadline) {
      break;
    }

    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }

  if (lastError != null) {
    throw new assert.AssertionError({
      message: `timed out after ${timeoutMs}ms`,
      cause: lastError,
    });
  }
  throw new assert.AssertionError({
    message: `timed out after ${timeoutMs}ms`,
  });
}

export class ConcatMergeOperator {
  merge(_key, existing_value, operand) {
    const parts = [];
    if (existing_value != null) {
      parts.push(Buffer.from(existing_value));
    }
    parts.push(Buffer.from(operand));
    return Buffer.concat(parts);
  }
}

export function cloneLogRecord(record) {
  return {
    level: record.level,
    target: record.target,
    message: record.message,
    module_path: record.module_path,
    file: record.file,
    line: record.line,
  };
}

export class LogCollector {
  #records = [];

  log(record) {
    this.#records.push(cloneLogRecord(record));
  }

  matchingRecord(predicate) {
    for (const record of this.#records) {
      if (predicate(record)) {
        return cloneLogRecord(record);
      }
    }
    return undefined;
  }

  records() {
    return this.#records.map((record) => cloneLogRecord(record));
  }
}

export async function seedWalFiles(store) {
  const db = await openDb(store, {
    configure(builder) {
      builder.with_merge_operator(new ConcatMergeOperator());
    },
  });

  try {
    await db.put_with_options(bytes("a"), bytes("1"), putOptions(), writeOptions(false));
    await db.put_with_options(bytes("b"), bytes("2"), putOptions(), writeOptions(false));
    await db.flush_with_options({ flush_type: FlushType.Wal });

    await db.delete_with_options(bytes("a"), writeOptions(false));
    await db.flush_with_options({ flush_type: FlushType.Wal });

    await db.merge_with_options(
      bytes("m"),
      bytes("x"),
      mergeOptions(),
      writeOptions(false),
    );
    await db.flush_with_options({ flush_type: FlushType.Wal });
  } finally {
    await shutdownAndDispose(db);
  }
}

export function uniquePath(prefix) {
  return `${prefix}-${randomUUID()}`;
}

export {
  ErrorClosed,
  ErrorInvalid,
  LogLevel,
  RowEntryKind,
};
