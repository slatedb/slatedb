#!/usr/bin/env bash

set -eu # stop on errors and undefined variables

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WARMUP=0 # ignore the first N samples, equal to 30 seconds with default settings
OUT="target/bencher/results"

mkdir -p $OUT/dats
mkdir -p $OUT/logs
mkdir -p $OUT/mermaid

run_bench() {
  local put_percentage="$1"
  local concurrency="$2"
  local num_keys="$3"
  local log_file="$4"

  local clean_flag=""
  if [ -n "${SLATEDB_BENCH_CLEAN:-}" ]; then
    clean_flag="--clean"
  fi

  local bench_cmd="cargo run -r --package slatedb-bencher -- \
    --path /slatedb-bencher_${put_percentage}_${concurrency} $clean_flag db \
    --db-options-path $DIR/Slatedb.toml \
    --duration 60 \
    --val-len 8192 \
    --block-cache-size 100663296 \
    --meta-cache-size 33554432 \
    --put-percentage $put_percentage \
    --concurrency $concurrency \
    --key-count $num_keys \
  "

  $bench_cmd | tee "$log_file"
}

generate_dat() {
    local input_file="$1"
    local output_file="$2"

    echo "Parsing stats for $input_file -> $output_file"

    # Extract elapsed time, puts/s, and gets/s using sed and awk for cross-platform compatibility
    grep "stats dump" "$input_file" | sed -E 's/.*elapsed ([0-9.]+).*put\/s: ([0-9.]+).*get\/s: ([0-9.]+).*/\1 \2 \3/' > "$output_file"
}

generate_mermaid () {
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
    local put_value=$(echo "$last_line" | awk '{print $2}')
    local get_value=$(echo "$last_line" | awk '{print $3}')

    # Get git commit hash (first 7 characters)
    local git_hash=$(git rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")

    # Get current date in YYYY-MM-DD format
    local current_date=$(date +"%Y-%m-%d")

    # Create x-axis entry
    local x_entry="$current_date ($git_hash)"

    # Extract put_percentage and concurrency from mermaid filename
    local filename=$(basename "$mermaid_file" .mermaid)
    local put_percentage=$(echo "$filename" | cut -d'_' -f1)
    local concurrency=$(echo "$filename" | cut -d'_' -f2)

    # Calculate max value for y-axis scaling
    local max_value=$(echo "$put_value $get_value" | tr ' ' '\n' | sort -nr | head -n1)
    local y_max=$(echo "$max_value * 1.2" | bc -l | cut -d'.' -f1)

    if [ ! -f "$mermaid_file" ]; then
        # Create new mermaid file
        cat > "$mermaid_file" << EOF
---
config:
  themeVariables:
    xyChart:
      plotColorPalette: '#1e81b0, #e28743'
---
xychart-beta
    title "SlateDB [puts=${put_percentage}%, threads=${concurrency}, 🔵=puts, 🟠=get]"
    x-axis ["$x_entry"]
    y-axis "requests/s" 0 --> $y_max
    line [$put_value]
    line [$get_value]
EOF
    else
        # Update existing mermaid file
        local temp_file=$(mktemp)

        # Read current content
        local title_line=$(grep "title" "$mermaid_file" | sed 's/^[[:space:]]*//')
        local x_axis_line=$(grep "x-axis" "$mermaid_file")
        local put_line=$(grep -m1 "line" "$mermaid_file")
        local get_line=$(grep "line" "$mermaid_file" | tail -n1)

        # Extract current values
        local current_x_values=$(echo "$x_axis_line" | sed 's/.*\[//;s/\].*//' | tr ',' '\n' | sed 's/^[[:space:]]*"//;s/"[[:space:]]*$//')
        local current_put_values=$(echo "$put_line" | sed 's/.*\[//;s/\].*//')
        local current_get_values=$(echo "$get_line" | sed 's/.*\[//;s/\].*//')

        # Convert to arrays
        local x_array=()
        local put_array=()
        local get_array=()

        # Parse existing x-axis values
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                x_array+=("$line")
            fi
        done <<< "$current_x_values"

        # Parse existing put values
        IFS=',' read -ra put_array <<< "$current_put_values"

        # Parse existing get values
        IFS=',' read -ra get_array <<< "$current_get_values"

        # Add new values
        x_array+=("$x_entry")
        put_array+=("$put_value")
        get_array+=("$get_value")

        # Keep only last 30 values if we have more
        if [ ${#x_array[@]} -gt 30 ]; then
            x_array=("${x_array[@]: -30}")
            put_array=("${put_array[@]: -30}")
            get_array=("${get_array[@]: -30}")
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

        # Build new put line string
        local new_put_line="line ["
        for i in "${!put_array[@]}"; do
            if [ $i -gt 0 ]; then
                new_put_line="$new_put_line, "
            fi
            new_put_line="$new_put_line${put_array[i]}"
        done
        new_put_line="$new_put_line]"

        # Build new get line string
        local new_get_line="line ["
        for i in "${!get_array[@]}"; do
            if [ $i -gt 0 ]; then
                new_get_line="$new_get_line, "
            fi
            new_get_line="$new_get_line${get_array[i]}"
        done
        new_get_line="$new_get_line]"

        # Calculate max value for y-axis scaling from all values
        local all_values="${put_array[*]} ${get_array[*]}"
        local max_value=$(echo "$all_values" | tr ' ' '\n' | sort -nr | head -n1)
        local y_max=$(echo "$max_value * 1.2" | bc -l | cut -d'.' -f1)

        # Write updated mermaid file
        cat > "$mermaid_file" << EOF
---
config:
  themeVariables:
    xyChart:
      plotColorPalette: '#1e81b0, #e28743'
---
xychart-beta
    $title_line
    $new_x_axis
    y-axis "requests/s" 0 --> $y_max
    $new_put_line
    $new_get_line
EOF
    fi

    echo "Generated/updated mermaid chart: $mermaid_file"
}

# Set CLOUD_PROVIDER to local if not already set
export CLOUD_PROVIDER=${CLOUD_PROVIDER:-local}
echo "Using cloud provider: $CLOUD_PROVIDER"

# Set LOCAL_PATH if CLOUD_PROVIDER is local and path not already set
if [ "$CLOUD_PROVIDER" = "local" ]; then
    export LOCAL_PATH=${LOCAL_PATH:-/tmp/slatedb}
    mkdir -p $LOCAL_PATH
    echo "Using local path: $LOCAL_PATH"
fi

for put_percentage in 20 40 60 80 100; do
  for concurrency in 1 32; do
    log_file="$OUT/logs/${put_percentage}_${concurrency}.log"
    dat_file="$OUT/dats/${put_percentage}_${concurrency}.dat"
    mermaid_file="$OUT/mermaid/${put_percentage}_${concurrency}.mermaid"
    num_keys=$((put_percentage * 1000))

    run_bench "$put_percentage" "$concurrency" "$num_keys" "$log_file"
    generate_dat "$log_file" "$dat_file"
    generate_mermaid "$dat_file" "$mermaid_file"
  done
done
