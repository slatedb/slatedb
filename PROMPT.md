# Add Node Binding Integration Tests, With Generator Compatibility Fixes

## Summary

- Add a committed Node test suite under bindings/node/tests that mirrors the current Python, Java, and Go binding coverage: DB lifecycle/CRUD/scans/batches/flush/merge/
  snapshot/transactions/errors/fencing, reader behavior, WAL reader behavior, and logging callback behavior.
- Make the Node test entrypoint run after the package build step, because the generated Node package is not committed and import-time loading requires the staged prebuilds/
  <target>/... layout.
- Include a prerequisite fix in uniffi-bindgen-node-js: as observed locally on March 27, 2026 with Node v24.3.0 and koffi 2.15.2, a freshly generated SlateDB package fails on
  the first db.put() after DbBuilder.build() with TypeError: Unexpected <anonymous_49> * value, expected RustArcPtrDb. That generator/runtime incompatibility must be fixed
  before the Node suite can pass.

## Key Changes

- In bindings/node:
    - Add a test script that runs npm run build first, then executes Node’s built-in test runner.
    - Add a shared test support module with newMemoryStore(), option builders, openDb, openReader, openWalReader, iterator drain helpers, assertion helpers, waitUntil,
      seedWalFiles, uniquePath, a ConcatMergeOperator, and cleanup helpers that always shutdown() when applicable and then dispose() every UniFFI object.
    - Add db.test.mjs covering lifecycle/status, CRUD/metadata, scan variants, batch write consumption, flush/metrics, merge, snapshot isolation, transactions, invalid input
      mapping, and writer fencing.
    - Add reader.test.mjs covering lifecycle, missing DB build failure, point reads, scan variants, refresh polling, default WAL replay, skip-WAL-replay behavior, merge
      operator, builder validation, and invalid ranges.
    - Add wal-reader.test.mjs covering empty listing, bounded listing/navigation, metadata/row decoding, and missing file metadata failure.
    - Add logging.test.mjs covering callback delivery plus duplicate-init rejection.
- In uniffi-bindgen-node-js:
    - Fix generated opaque-object handle handling so objects returned from async pointer completions can immediately be used in subsequent clone/free/method calls with real
      koffi.
    - Treat this as a real-runtime compatibility fix, not a type-only fix: the generated runtime must stop assuming the local fixture’s looser pointer semantics match real
      koffi.
    - Add a generator regression that exercises “async returns object, then immediately call a method on it” with named opaque pointers, so the current failure is reproducible
      offline.
    - Update the local Koffi fixture used by generator tests to model real named opaque-pointer strictness closely enough to catch this class of bug.

## Public Interfaces

- No intended changes to the exported SlateDB Node API surface.
- Add a package-level npm test entrypoint for bindings/node.
- Internal generated-runtime handle behavior may change to become compatible with real koffi, but the exported SlateDB classes/functions/types should remain the same.

## Test Plan

- npm --prefix bindings/node run test
    - Must build the package, stage the host prebuild, import the real generated package, and pass the full runtime suite.
- Validate the same behavior already covered elsewhere:
    - closed errors after shutdown
    - typed invalid/data/closed errors
    - merge-operator callbacks
    - iterator draining and scan boundaries
    - reader polling and WAL replay rules
    - WAL file navigation and row decoding
    - logging duplicate-init rejection and callback record fields
- In uniffi-bindgen-node-js, add or update regression coverage so a generated fixture with an async-returned object fails before the fix and passes after it.

## Assumptions

- Use ESM JavaScript tests with node:test and node:assert/strict; do not add Jest, Vitest, or Mocha.
- Keep the test layout parallel to the other bindings: one shared helper module plus separate files by behavior area.
- Rely on an installed koffi dependency for the SlateDB Node package tests; the tests themselves add no extra Node dependencies.
- Include the generator fix in the same workstream because the current generated package is not usable enough for the planned Node suite to pass end-to-end.

## Instructions

You are in a Ralph Wiggum loop. Work through the first few TODOs in the `## TODO` section below.

- update PROMPT.md with an updated TODO list after each change
- never ever change any PROMPT.md text _except_ the items in the `## TODO` section
- you may update the TODO items as you see fit--remove outdated items, add new items, mark items as completed
- commit after each completed TODO
- use conventional commit syntax for commit messages
- if there are no items left in the TODO, append a new line to the end of the file that simply contains ✅

## uniffi-bindgen-node-js changes

You are allowed to edit uniffi-bindgen-node-js if you find issues with it. The code is in:

- /Users/chrisriccomini/Code/uniffi-bindgen-node-js

You are allowed to commit changes in that repository. Use conventional commit syntax.

Make sure to re-install and rebuild after changing that repo:

```
cargo install --path ~/Code/uniffi-bindgen-node-js --force
npm --prefix bindings/node run build
```

## TODO

- [x] Add a test script in bindings/node/package.json that builds the package and runs the Node test runner.
- [x] Add a shared bindings/node/tests support module with store/build/open/drain/assert/wait/cleanup helpers and ConcatMergeOperator.
- [x] Port the DB lifecycle, CRUD, scan, batch, flush, merge, snapshot, transaction, invalid-input, and fencing coverage into bindings/node/tests/db.test.mjs.
- [x] Port the reader lifecycle, build-failure, point-read, scan, refresh, WAL replay, merge-operator, builder-validation, and invalid-range coverage into bindings/node/tests/
  reader.test.mjs.
- [x] Port the WAL reader empty-listing, navigation, metadata/row decoding, and missing-file coverage into bindings/node/tests/wal-reader.test.mjs.
- [x] Port the logging callback and duplicate-init coverage into bindings/node/tests/logging.test.mjs.
- [ ] Ensure all Node tests explicitly shutdown() and dispose() UniFFI objects to avoid leaked handles between tests.
- [ ] Fix uniffi-bindgen-node-js so async-returned opaque objects are usable with real koffi in subsequent clone/free/method calls.
- [ ] Add a generator regression in uniffi-bindgen-node-js for “async returns object, then immediately call a method on it”.
- [ ] Tighten the local Koffi fixture in uniffi-bindgen-node-js so named opaque-pointer behavior matches real koffi closely enough to catch this bug.
- [ ] Run the generator regressions in uniffi-bindgen-node-js after the runtime fix.
- [ ] Run npm --prefix bindings/node run test as the final verification step.
