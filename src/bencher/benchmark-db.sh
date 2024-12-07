#!/usr/bin/env bash

set -eu # stop on errors and undefined variables

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WARMUP=0 # ignore the first N samples, equal to 30 seconds with default settings
OUT="target/bencher/results"

mkdir -p $OUT/plots
mkdir -p $OUT/dats
mkdir -p $OUT/logs

# Check if gnuplot is available
has_gnuplot() {
    if command -v gnuplot >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

run_bench() {
  local put_percentage="$1"
  local concurrency="$2"
  local log_file="$3"

  local bench_cmd="cargo run -r --bin bencher --features=bencher -- \
    --path /slatedb-bencher_${put_percentage}_${concurrency} db \
    --db-options-path $DIR/Slatedb.toml \
    --duration 60 \
    --val-len 8192 \
    --block-cache-size 134217728 \
    --put-percentage $put_percentage \
    --concurrency $concurrency \
  "

  $bench_cmd | tee "$log_file"
}

generate_dat() {
    local input_file="$1"
    local output_file="$2"

    echo "Parsing stats for $input_file -> $output_file"

    cat "$input_file" | \
      grep 'stats dump' | \
      sed 's/.*elapsed \([^,]*\), put\/s: \([^,]*\), get\/s: \([^,]*\).*/\1 \2 \3/' > "$output_file"
}

generate_plot() {
  local input_file="$1"
  local output_file="$2"
  local warmup="${WARMUP:-0}" # Default to 0 if WARMUP is not set

  echo "Generating plot for $input_file -> $output_file"

  gnuplot -e "
    set terminal svg size 960,540 background rgb 'white';
    set output '$output_file';
    set title 'Puts/s and Gets/s Over Time';
    set xlabel 'Elapsed Time (s)';
    set ylabel 'Operations per Second';
    set grid;
    set key outside;
    plot \
    '$input_file' skip $warmup using 1:2 with linespoints linewidth 2 title 'Puts/s', \
    '$input_file' skip $warmup using 1:3 with linespoints linewidth 2 title 'Gets/s';"
}

generate_json() {
    local output_file="$OUT/benchmark-data.json"
    echo "[" > "$output_file"
    local first_entry=true

    # Use find to get a sorted list of dat files
    for dat_file in $(find "$OUT/dats" -name "*.dat" | sort -Vr); do
        # Extract put_percentage and concurrency from filename
        local filename=$(basename "$dat_file")
        local put_percentage=$(echo "$filename" | cut -d'_' -f1)
        local concurrency=$(echo "$filename" | cut -d'_' -f2 | cut -d'.' -f1)

        # Read the last line of the dat file for final stats
        local stats=$(tail -n 1 "$dat_file")
        local puts=$(echo "$stats" | awk '{print $2}')
        local gets=$(echo "$stats" | awk '{print $3}')

        # Assert that all required values are non-empty
        [ -n "$puts" ] || { echo "Error: puts/s is empty in $dat_file"; exit 1; }
        [ -n "$gets" ] || { echo "Error: gets/s is empty in $dat_file"; exit 1; }

        # Add comma for all but first entry
        if [ "$first_entry" = true ]; then
            first_entry=false
        else
            echo "," >> "$output_file"
        fi

        # Append benchmark results
        cat >> "$output_file" << EOF
    {
        "name": "SlateDB ${put_percentage}% Puts ${concurrency} Threads - Puts/s",
        "unit": "ops/sec",
        "value": $puts
    },
    {
        "name": "SlateDB ${put_percentage}% Puts ${concurrency} Threads - Gets/s",
        "unit": "ops/sec",
        "value": $gets
    }
EOF
    done

    # Remove the last comma and close the array
    echo "]" >> "$output_file"
    echo "Generated benchmark data in $output_file"
}

for put_percentage in 20 40 60 80 100; do
  for concurrency in 1 4; do
    log_file="$OUT/logs/${put_percentage}_${concurrency}.log"
    dat_file="$OUT/dats/${put_percentage}_${concurrency}.dat"
    svg_file="$OUT/plots/${put_percentage}_${concurrency}.svg"

    run_bench "$put_percentage" "$concurrency" "$log_file"
    generate_dat "$log_file" "$dat_file"
    if has_gnuplot; then
      generate_plot "$dat_file" "$svg_file"
    else
      echo "gnuplot is missing, so skipping plot generation"
    fi
  done
done

generate_json
