#!/usr/bin/env bash

set -euo pipefail # stop on errors, undefined variables, and pipe failures

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="target/bencher/transaction-results"

mkdir -p "$OUT/logs"
mkdir -p "$OUT/dats"

# Define DB path once for both bencher and cleanup
DB_PATH_NAME="slatedb-txn-bencher"

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
    --path /${DB_PATH_NAME}_${isolation_level}_${concurrency}_${transaction_size}_${use_write_batch} $clean_flag transaction \
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
  # Set RUST_LOG to INFO level to prevent gigabyte-sized log files
  # Override with RUST_LOG=debug to see all logs if needed
  RUST_LOG=${RUST_LOG:-info} $bench_cmd 2>&1 | tee "$log_file"
}

generate_dat() {
    local input_file="$1"
    local output_file="$2"

    echo "Parsing stats for $input_file -> $output_file"

    if ! grep -q "txn stats" "$input_file"; then
        echo "Warning: no txn stats lines in $input_file"
        return 1
    fi

    grep "txn stats" "$input_file" \
        | sed -nE 's/.*elapsed ([0-9.]+).*commit\/s: ([0-9.]+).*abort\/s: ([0-9.]+).*conflict\/s: ([0-9.]+).*ops\/s: ([0-9.]+).*/\1 \2 \3 \4 \5/p' \
        > "$output_file"

    # Guard: check if parsing produced valid output
    if [ ! -s "$output_file" ]; then
        echo "Warning: failed to parse txn stats from $input_file"
        return 1
    fi
}

# Set CLOUD_PROVIDER to local if not already set
export CLOUD_PROVIDER=${CLOUD_PROVIDER:-local}
echo "Using cloud provider: $CLOUD_PROVIDER"

# Set LOCAL_PATH if CLOUD_PROVIDER is local and path not already set
if [ "$CLOUD_PROVIDER" = "local" ]; then
    export LOCAL_PATH=${LOCAL_PATH:-/tmp/slatedb-txn}
    mkdir -p "$LOCAL_PATH"
    echo "Using local path: $LOCAL_PATH"
fi

echo "=== Transaction Benchmarks ==="
echo "Git: $(git rev-parse --short=7 HEAD 2>/dev/null || echo 'unknown')"
echo ""

# Define test configurations: isolation_level concurrency transaction_size use_write_batch
# Format creates filenames like: snapshot_4_10_txn or snapshot_4_10_batch

# Test 1: Low concurrency, Snapshot isolation, Transaction
echo "Test 1: Low concurrency with Transactions (Snapshot)"
run_txn_bench "snapshot" 4 10 false "$OUT/logs/snapshot_4_10_txn.log"
generate_dat "$OUT/logs/snapshot_4_10_txn.log" "$OUT/dats/snapshot_4_10_txn.dat"

# Test 2: Low concurrency, Snapshot isolation, WriteBatch
echo "Test 2: Low concurrency with WriteBatch"
run_txn_bench "snapshot" 4 10 true "$OUT/logs/snapshot_4_10_batch.log"
generate_dat "$OUT/logs/snapshot_4_10_batch.log" "$OUT/dats/snapshot_4_10_batch.dat"

# Test 3: High concurrency, Snapshot isolation, Transaction
echo "Test 3: High concurrency with Transactions (Snapshot)"
run_txn_bench "snapshot" 16 10 false "$OUT/logs/snapshot_16_10_txn.log"
generate_dat "$OUT/logs/snapshot_16_10_txn.log" "$OUT/dats/snapshot_16_10_txn.dat"

# Test 4: High concurrency, Snapshot isolation, WriteBatch
echo "Test 4: High concurrency with WriteBatch"
run_txn_bench "snapshot" 16 10 true "$OUT/logs/snapshot_16_10_batch.log"
generate_dat "$OUT/logs/snapshot_16_10_batch.log" "$OUT/dats/snapshot_16_10_batch.dat"

# Test 5: High concurrency, SerializableSnapshot isolation
echo "Test 5: High concurrency with SerializableSnapshot"
run_txn_bench "serializable" 16 10 false "$OUT/logs/serializable_16_10_txn.log"
generate_dat "$OUT/logs/serializable_16_10_txn.log" "$OUT/dats/serializable_16_10_txn.dat"

# Test 6: Large transactions
echo "Test 6: Large transactions (50 ops)"
run_txn_bench "snapshot" 8 50 false "$OUT/logs/snapshot_8_50_txn.log"
generate_dat "$OUT/logs/snapshot_8_50_txn.log" "$OUT/dats/snapshot_8_50_txn.dat"

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to:"
echo "  Logs:    $OUT/logs/"
echo "  Data:    $OUT/dats/"
