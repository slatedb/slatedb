#!/usr/bin/env bash

set -eu # stop on errors and undefined variables

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="target/bencher/transaction-results"

mkdir -p "$OUT/logs"

run_txn_bench() {
  local isolation_level="$1"
  local concurrency="$2"
  local transaction_size="$3"
  local use_write_batch="$4"
  local log_file="$5"

  local clean_flag=""
  if [ -n "${SLATEDB_BENCH_CLEAN:-}" ]; then
    clean_flag="--clean"
  fi

  local batch_flag=""
  if [ "$use_write_batch" = "true" ]; then
    batch_flag="--use-write-batch"
  fi

  local bench_cmd="cargo run -r --package slatedb-bencher -- \
    --path /slatedb-txn-bencher_${isolation_level}_${concurrency}_${transaction_size}_${use_write_batch} $clean_flag transaction \
    --db-options-path $DIR/Slatedb.toml \
    --duration 60 \
    --val-len 1024 \
    --block-cache-size 100663296 \
    --meta-cache-size 33554432 \
    --concurrency $concurrency \
    --key-count 10000 \
    --transaction-size $transaction_size \
    --abort-percentage 5 \
    --isolation-level $isolation_level \
    $batch_flag \
  "

  echo "Running: $bench_cmd"
  $bench_cmd | tee "$log_file"
}

# Set CLOUD_PROVIDER to local if not already set
export CLOUD_PROVIDER=${CLOUD_PROVIDER:-local}
echo "Using cloud provider: $CLOUD_PROVIDER"

# Set LOCAL_PATH if CLOUD_PROVIDER is local and path not already set
if [ "$CLOUD_PROVIDER" = "local" ]; then
    export LOCAL_PATH=${LOCAL_PATH:-/tmp/slatedb-txn}
    mkdir -p $LOCAL_PATH
    echo "Using local path: $LOCAL_PATH"
fi

echo "=== Transaction Benchmarks ==="
echo ""

# Test 1: Low concurrency, Snapshot isolation, Transaction
echo "Test 1: Low concurrency with Transactions (Snapshot)"
run_txn_bench "snapshot" 4 10 false "$OUT/logs/snapshot_4_10_txn.log"

# Test 2: Low concurrency, Snapshot isolation, WriteBatch
echo "Test 2: Low concurrency with WriteBatch"
run_txn_bench "snapshot" 4 10 true "$OUT/logs/snapshot_4_10_batch.log"

# Test 3: High concurrency, Snapshot isolation, Transaction
echo "Test 3: High concurrency with Transactions (Snapshot)"
run_txn_bench "snapshot" 32 10 false "$OUT/logs/snapshot_32_10_txn.log"

# Test 4: High concurrency, Snapshot isolation, WriteBatch
echo "Test 4: High concurrency with WriteBatch"
run_txn_bench "snapshot" 32 10 true "$OUT/logs/snapshot_32_10_batch.log"

# Test 5: High concurrency, SerializableSnapshot isolation
echo "Test 5: High concurrency with SerializableSnapshot"
run_txn_bench "serializable" 32 10 false "$OUT/logs/serializable_32_10_txn.log"

# Test 6: Large transactions
echo "Test 6: Large transactions (50 ops)"
run_txn_bench "snapshot" 8 50 false "$OUT/logs/snapshot_8_50_txn.log"

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to: $OUT/logs/"
echo ""
echo "To view results:"
echo "  ls -lh $OUT/logs/"
