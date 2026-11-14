import assert from "node:assert/strict";
import test from "node:test";

import { ObjectStore } from "../index.js";
import { bytes, createCleanup, expectInvalid, openDb, utf8 } from "./support.mjs";

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
