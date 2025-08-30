---
title: Consistency
description: Learn about SlateDB's consistency model
---

Snapshots provide consistent read-only views over the entire state of the key-value store.

Once created, it supports all read operations—`get` or `scan`—giving you a stable, consistent view of the data as it existed at the instant the snapshot was taken.

## Creating a snapshot

Call `snapshot()` on any SlateDB handle.  

The call is cheap and non-blocking; it merely pins the current state so it can’t be garbage-collected while the snapshot is alive.

<Code code={createSnapshot} lang="rust" title="main.rs" />

## Reading from a snapshot

Use the returned snapshot handle exactly like a regular `Db`, but every operation sees the same, immutable state.  

<Code code={scanSnapshot} lang="rust" title="main.rs" />