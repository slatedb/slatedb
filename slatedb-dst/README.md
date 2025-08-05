# Deterministic Simulation Testing (DST)

This crate provides deterministic simulation tools and tests for SlateDB.

## Design

SlateDB's `Dst` struct is designed to apply a sequence of operations on a
SlateDB database and verify the results against an in-memory copy of the
database. The operations are generated using a `DstDistribution` trait that
provides the probability distribution of operations.

Everything in DST is deterministic, including the random number generator,
the system clock, and the runtime. This means a failed test can be re-run
with the same seed to reproduce the failure.

See the [slatedb::dst] module for design details.

## Usage

All of SlateDB's deterministic simulation tests are gated behind a `cfg(dst)`
attribute. The tests require a deterministic Tokio runtime, which must also
be enabled with `cfg(tokio_unstable)` and Tokio's `rt` feature.

To run DST tests, you must set the `RUSTFLAGS` environment variable to include
`--cfg dst` and `--cfg tokio_unstable`.

```bash
$ RUSTFLAGS="--cfg dst --cfg tokio_unstable" \
  RUSTDOCFLAGS="--cfg tokio_unstable" \
  cargo test -p slatedb-dst --all-features
```

Or if you prefer nextest:

```bash
$ RUSTFLAGS="--cfg dst --cfg tokio_unstable" \
  RUSTDOCFLAGS="--cfg tokio_unstable" \
  cargo nextest run -p slatedb-dst --profile dst
```

## Nightly

This crate also contains a longer-running test that's meant to be run every
night. It is only run when `slow`, `dst`, and `tokio_unstable` cfgs are all set.

The `.github/workflows/nightly.yaml` is configured to run this test.

To run it locally, you must set the `RUSTFLAGS` environment variable to include
`--cfg dst --cfg tokio_unstable --cfg slow`.

```bash
$ RUSTFLAGS="--cfg dst --cfg tokio_unstable --cfg slow" \
  RUSTDOCFLAGS="--cfg tokio_unstable" \
  cargo test test_dst_nightly -p slatedb-dst --all-features
```
