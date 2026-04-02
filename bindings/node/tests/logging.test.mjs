import assert from "node:assert/strict";
import test from "node:test";

import { init_logging } from "../index.js";
import {
  LogCollector,
  LogLevel,
  createCleanup,
  expectInvalid,
  newMemoryStore,
  openDb,
  uniquePath,
  waitUntil,
} from "./support.mjs";

test("logging callback delivery and duplicate-init rejection", async (t) => {
  const cleanup = createCleanup(t);
  const collector = new LogCollector();

  init_logging(LogLevel.Info, collector);
  await expectInvalid(
    () => init_logging(LogLevel.Info, collector),
    { message: "logging already initialized" },
  );

  let matched;
  const path = uniquePath("test-db-logging");
  const store = cleanup.track(newMemoryStore());
  await openDb(store, { path, cleanup });

  await waitUntil(() => {
    matched = collector.matchingRecord((record) => {
      return record.level === LogLevel.Info
        && record.message.includes("opening SlateDB database")
        && record.message.includes(path);
    });
    return matched != null;
  });

  assert.notEqual(matched, undefined);
  assert.notEqual(matched.target, "");
  assert.notEqual(matched.module_path, undefined);
  assert.notEqual(matched.module_path, "");
  assert.notEqual(matched.file, undefined);
  assert.notEqual(matched.file, "");
  assert.notEqual(matched.line, undefined);
  assert.ok(matched.line > 0);
});
