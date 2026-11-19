import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { ErrorUnavailable, FlushType, ObjectStore, ObjectStoreMirrorBuilder } from "../index.js";
import {
  bytes,
  createCleanup,
  expectError,
  expectInvalid,
  newMemoryStore,
  openDb,
  shutdownAndDispose,
  utf8,
} from "./support.mjs";

test("resolve_with_options opens a usable store", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(ObjectStore.resolve_with_options("memory:///", new Map()));
  const db = await openDb(store, { cleanup });

  await db.put(bytes("key"), bytes("value"));
  assert.equal(utf8(await db.get(bytes("key"))), "value");
});

test("resolve_with_options accepts S3 credentials", (t) => {
  const cleanup = createCleanup(t);
  const options = new Map(
    Object.entries({
      aws_access_key_id: "AKIDEXAMPLE",
      aws_secret_access_key: "secret",
      aws_session_token: "token",
      aws_region: "us-east-1",
    }),
  );
  cleanup.track(ObjectStore.resolve_with_options("s3://my-bucket", options));
});

test("resolve_with_options rejects an unknown S3 key", async () => {
  const options = new Map([["aws_acess_key_id", "AKIDEXAMPLE"]]);
  const error = await expectInvalid(() =>
    ObjectStore.resolve_with_options("s3://my-bucket", options),
  );
  assert.match(error.message, /aws_acess_key_id/);
});

function mirrorDir(t) {
  const dir = mkdtempSync(join(tmpdir(), "slatedb-mirror-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

async function buildMirror(cleanup, localDir, store) {
  const builder = cleanup.track(new ObjectStoreMirrorBuilder(localDir, store));
  return cleanup.track(await builder.build());
}

test("a mirror keeps flushed SSTs on local disk", async (t) => {
  const cleanup = createCleanup(t);
  const localDir = mirrorDir(t);
  const mirror = await buildMirror(cleanup, localDir, cleanup.track(newMemoryStore()));

  const db = await openDb(mirror);
  await db.put(bytes("key"), bytes("value"));
  await db.flush_with_options({ flush_type: FlushType.MemTable });
  await shutdownAndDispose(db);
  assert.ok(readdirSync(localDir).some((name) => name.endsWith(".obj")));

  // The same mirror serves each open of its database.
  const reopened = await openDb(mirror, { cleanup });
  assert.equal(utf8(await reopened.get(bytes("key"))), "value");
});

test("a mirror rejects a locked directory", async (t) => {
  const cleanup = createCleanup(t);
  const localDir = mirrorDir(t);
  const store = cleanup.track(newMemoryStore());
  await buildMirror(cleanup, localDir, store);

  const error = await expectError(() => buildMirror(cleanup, localDir, store), ErrorUnavailable);
  assert.match(error.message, /locked/);
});
