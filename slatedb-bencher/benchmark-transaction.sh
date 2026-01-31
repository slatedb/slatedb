#!/usr/bin/env bash

set -euo pipefail # stop on errors, undefined variables, and pipe failures

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="target/bencher/transaction-results"

mkdir -p "$OUT/logs"
mkdir -p "$OUT/dats"
mkdir -p "$OUT/mermaid"

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

generate_mermaid() {
    local dat_file="$1"
    local mermaid_file="$2"

    # Create mermaid directory if it doesn't exist
    mkdir -p "$(dirname "$mermaid_file")"

    # Get the last line from dat file (most recent benchmark result)
    if [ ! -f "$dat_file" ] || [ ! -s "$dat_file" ]; then
        echo "Warning: dat file $dat_file does not exist or is empty"
        return 1
    fi

    local last_line=$(tail -n 1 "$dat_file")
    local commit_value=$(echo "$last_line" | awk '{print $2}')
    local abort_value=$(echo "$last_line" | awk '{print $3}')
    local conflict_value=$(echo "$last_line" | awk '{print $4}')
    local ops_value=$(echo "$last_line" | awk '{print $5}')

    # Get git commit hash (first 7 characters)
    local git_hash=$(git rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")

    # Get current date in YYYY-MM-DD format
    local current_date=$(date +"%Y-%m-%d")

    # Create x-axis entry
    local x_entry="$current_date ($git_hash)"

    # Extract test parameters from mermaid filename
    # Format: isolation_concurrency_txnsize_mode.mermaid (e.g., snapshot_4_10_txn.mermaid)
    local filename=$(basename "$mermaid_file" .mermaid)
    local isolation=$(echo "$filename" | cut -d'_' -f1)
    local concurrency=$(echo "$filename" | cut -d'_' -f2)
    local txn_size=$(echo "$filename" | cut -d'_' -f3)
    local mode=$(echo "$filename" | cut -d'_' -f4)

    local title_mode="Transaction"
    if [ "$mode" = "batch" ]; then
        title_mode="WriteBatch"
    fi

    # Calculate max value for y-axis scaling (consider all 4 metrics)
    local max_value=$(printf "%s\n" "$commit_value" "$abort_value" "$conflict_value" "$ops_value" | sort -nr | head -n1)
    local y_max=$(awk -v m="$max_value" 'BEGIN{printf "%d", (m*1.2)}')

    if [ ! -f "$mermaid_file" ]; then
        # Create new mermaid file
        cat > "$mermaid_file" << EOF
---
config:
  xyChart:
    chartOrientation: horizontal
    height: 768
    width: 1024
  themeVariables:
    xyChart:
      plotColorPalette: '#2ecc71, #e74c3c, #f39c12, #3498db'
---
xychart-beta
    title "SlateDB Txn [${isolation}, threads=${concurrency}, txn_size=${txn_size}, ${title_mode}] 🟢=commit 🔴=abort 🟠=conflict 🔵=ops"
    x-axis ["$x_entry"]
    y-axis "requests/s" 0 --> $y_max
    line [$commit_value]
    line [$abort_value]
    line [$conflict_value]
    line [$ops_value]
EOF
    else
        # Update existing mermaid file
        # Read current content (match only Mermaid series lines, not YAML like plotColorPalette)
        local title_line=$(grep -E "^[[:space:]]*title[[:space:]]" "$mermaid_file" | sed 's/^[[:space:]]*//')
        local x_axis_line=$(grep -E "^[[:space:]]*x-axis[[:space:]]*\\[" "$mermaid_file")
        local commit_line=$(grep -E "^[[:space:]]*line[[:space:]]*\\[" "$mermaid_file" | sed -n '1p')
        local abort_line=$(grep -E "^[[:space:]]*line[[:space:]]*\\[" "$mermaid_file" | sed -n '2p')
        local conflict_line=$(grep -E "^[[:space:]]*line[[:space:]]*\\[" "$mermaid_file" | sed -n '3p')
        local ops_line=$(grep -E "^[[:space:]]*line[[:space:]]*\\[" "$mermaid_file" | sed -n '4p')

        # Extract current values
        local current_x_values=$(echo "$x_axis_line" | sed 's/.*\[//;s/\].*//' | tr ',' '\n' | sed 's/^[[:space:]]*"//;s/"[[:space:]]*$//')
        local current_commit_values=$(echo "$commit_line" | sed 's/.*\[//;s/\].*//')
        local current_abort_values=$(echo "$abort_line" | sed 's/.*\[//;s/\].*//')
        local current_conflict_values=$(echo "$conflict_line" | sed 's/.*\[//;s/\].*//')
        local current_ops_values=$(echo "$ops_line" | sed 's/.*\[//;s/\].*//')

        # Convert to arrays
        local x_array=()
        local commit_array=()
        local abort_array=()
        local conflict_array=()
        local ops_array=()

        # Parse existing x-axis values
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                x_array+=("$line")
            fi
        done <<< "$current_x_values"

        # Parse existing values
        IFS=',' read -ra commit_array <<< "$current_commit_values"
        IFS=',' read -ra abort_array <<< "$current_abort_values"
        IFS=',' read -ra conflict_array <<< "$current_conflict_values"
        IFS=',' read -ra ops_array <<< "$current_ops_values"

        # Trim whitespace from array elements (comma-split can leave leading spaces)
        # Use separate loops to handle potential length mismatches safely
        for i in "${!commit_array[@]}"; do commit_array[i]="${commit_array[i]//[[:space:]]/}"; done
        for i in "${!abort_array[@]}"; do abort_array[i]="${abort_array[i]//[[:space:]]/}"; done
        for i in "${!conflict_array[@]}"; do conflict_array[i]="${conflict_array[i]//[[:space:]]/}"; done
        for i in "${!ops_array[@]}"; do ops_array[i]="${ops_array[i]//[[:space:]]/}"; done

        # Prepend new values (newest first)
        x_array=("$x_entry" "${x_array[@]}")
        commit_array=("$commit_value" "${commit_array[@]}")
        abort_array=("$abort_value" "${abort_array[@]}")
        conflict_array=("$conflict_value" "${conflict_array[@]}")
        ops_array=("$ops_value" "${ops_array[@]}")

        # Keep only first 30 values (newest-first) if we have more
        if [ ${#x_array[@]} -gt 30 ]; then
            x_array=("${x_array[@]:0:30}")
            commit_array=("${commit_array[@]:0:30}")
            abort_array=("${abort_array[@]:0:30}")
            conflict_array=("${conflict_array[@]:0:30}")
            ops_array=("${ops_array[@]:0:30}")
        fi

        # Build new x-axis string
        local new_x_axis="x-axis ["
        for i in "${!x_array[@]}"; do
            if [ $i -gt 0 ]; then
                new_x_axis="$new_x_axis, "
            fi
            new_x_axis="$new_x_axis\"${x_array[i]}\""
        done
        new_x_axis="$new_x_axis]"

        # Build new line strings
        local new_commit_line="line ["
        local new_abort_line="line ["
        local new_conflict_line="line ["
        local new_ops_line="line ["
        for i in "${!commit_array[@]}"; do
            if [ $i -gt 0 ]; then
                new_commit_line="$new_commit_line, "
                new_abort_line="$new_abort_line, "
                new_conflict_line="$new_conflict_line, "
                new_ops_line="$new_ops_line, "
            fi
            new_commit_line="$new_commit_line${commit_array[i]}"
            new_abort_line="$new_abort_line${abort_array[i]}"
            new_conflict_line="$new_conflict_line${conflict_array[i]}"
            new_ops_line="$new_ops_line${ops_array[i]}"
        done
        new_commit_line="$new_commit_line]"
        new_abort_line="$new_abort_line]"
        new_conflict_line="$new_conflict_line]"
        new_ops_line="$new_ops_line]"

        # Calculate max value for y-axis scaling from all values (all 4 metrics)
        local max_value=$(printf "%s\n" "${commit_array[@]}" "${abort_array[@]}" "${conflict_array[@]}" "${ops_array[@]}" | sort -nr | head -n1)
        local y_max=$(awk -v m="$max_value" 'BEGIN{printf "%d", (m*1.2)}')

        # Write updated mermaid file
        cat > "$mermaid_file" << EOF
---
config:
  xyChart:
    chartOrientation: horizontal
    height: 768
    width: 1024
  themeVariables:
    xyChart:
      plotColorPalette: '#2ecc71, #e74c3c, #f39c12, #3498db'
---
xychart-beta
    $title_line
    $new_x_axis
    y-axis "requests/s" 0 --> $y_max
    $new_commit_line
    $new_abort_line
    $new_conflict_line
    $new_ops_line
EOF
    fi

    echo "Generated/updated mermaid chart: $mermaid_file"
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
generate_mermaid "$OUT/dats/snapshot_4_10_txn.dat" "$OUT/mermaid/snapshot_4_10_txn.mermaid"

# Test 2: Low concurrency, Snapshot isolation, WriteBatch
echo "Test 2: Low concurrency with WriteBatch"
run_txn_bench "snapshot" 4 10 true "$OUT/logs/snapshot_4_10_batch.log"
generate_dat "$OUT/logs/snapshot_4_10_batch.log" "$OUT/dats/snapshot_4_10_batch.dat"
generate_mermaid "$OUT/dats/snapshot_4_10_batch.dat" "$OUT/mermaid/snapshot_4_10_batch.mermaid"

# Test 3: High concurrency, Snapshot isolation, Transaction
echo "Test 3: High concurrency with Transactions (Snapshot)"
run_txn_bench "snapshot" 16 10 false "$OUT/logs/snapshot_16_10_txn.log"
generate_dat "$OUT/logs/snapshot_16_10_txn.log" "$OUT/dats/snapshot_16_10_txn.dat"
generate_mermaid "$OUT/dats/snapshot_16_10_txn.dat" "$OUT/mermaid/snapshot_16_10_txn.mermaid"

# Test 4: High concurrency, Snapshot isolation, WriteBatch
echo "Test 4: High concurrency with WriteBatch"
run_txn_bench "snapshot" 16 10 true "$OUT/logs/snapshot_16_10_batch.log"
generate_dat "$OUT/logs/snapshot_16_10_batch.log" "$OUT/dats/snapshot_16_10_batch.dat"
generate_mermaid "$OUT/dats/snapshot_16_10_batch.dat" "$OUT/mermaid/snapshot_16_10_batch.mermaid"

# Test 5: High concurrency, SerializableSnapshot isolation
echo "Test 5: High concurrency with SerializableSnapshot"
run_txn_bench "serializable" 16 10 false "$OUT/logs/serializable_16_10_txn.log"
generate_dat "$OUT/logs/serializable_16_10_txn.log" "$OUT/dats/serializable_16_10_txn.dat"
generate_mermaid "$OUT/dats/serializable_16_10_txn.dat" "$OUT/mermaid/serializable_16_10_txn.mermaid"

# Test 6: Large transactions
echo "Test 6: Large transactions (50 ops)"
run_txn_bench "snapshot" 8 50 false "$OUT/logs/snapshot_8_50_txn.log"
generate_dat "$OUT/logs/snapshot_8_50_txn.log" "$OUT/dats/snapshot_8_50_txn.dat"
generate_mermaid "$OUT/dats/snapshot_8_50_txn.dat" "$OUT/mermaid/snapshot_8_50_txn.mermaid"

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to:"
echo "  Logs:    $OUT/logs/"
echo "  Data:    $OUT/dats/"
echo "  Charts:  $OUT/mermaid/"
