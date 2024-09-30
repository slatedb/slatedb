#!/usr/bin/env bash

set -eu # stop on errors and undefined variables

OUT="target/s3-bench/results"
mkdir -p $OUT/plots
echo "Results location: $(pwd)/$OUT"

gnuplot -V # just to make sure gnuplot is present

WARMUP=20 # samples, equal to seconds with default settings

CMD="cargo run -r --bin s3-bench --features=s3_bench -- \
  --duration 600 --key-count 100000 --value-size 8192 --plot \
  --no-wal --block-cache 134217728 1024 --object-cache 16777216"

# 0% writes
$CMD --put-percentage 0 --prepopulate 100 --concurrency 1 | tee $OUT/0-1.txt
$CMD --put-percentage 0 --prepopulate 100 --concurrency 4 | tee $OUT/0-4.txt

# 25% writes
$CMD --put-percentage 25 --concurrency 1 | tee $OUT/25-1.txt
$CMD --put-percentage 25 --concurrency 4 | tee $OUT/25-4.txt

# 50% writes
$CMD --put-percentage 50 --concurrency 1 | tee $OUT/50-1.txt
$CMD --put-percentage 50 --concurrency 4 | tee $OUT/50-4.txt

# 100% writes
$CMD --put-percentage 100 --concurrency 1 | tee $OUT/100-1.txt
$CMD --put-percentage 100 --concurrency 4 | tee $OUT/100-4.txt

PLOT="set terminal svg dynamic size 960,540 background rgb 'white';
  set xlabel 'time, s';
  set ylabel 'throughput, op/s';
  set yrange [0:];"

# 0% writes, 1 thread
gnuplot -e "$PLOT plot \
  '$OUT/0-1.txt' skip $WARMUP with lines ls 1 lw 0.5 title '0% writes, 1 thread, raw', \
  '$OUT/0-1.txt' skip $WARMUP with lines ls 1 lw 2 smooth bezier title '0% writes, 1 thread, smooth'" \
  > $OUT/plots/0-1.svg

# 0% writes, 4 threads
gnuplot -e "$PLOT plot \
  '$OUT/0-4.txt' skip $WARMUP with lines ls 1 lw 0.5 title '0% writes, 4 threads, raw', \
  '$OUT/0-4.txt' skip $WARMUP with lines ls 1 lw 2 smooth bezier title '0% writes, 4 threads, smooth'" \
  > $OUT/plots/0-4.svg

# 0% writes, 1 thread vs 4 threads
gnuplot -e "$PLOT plot \
  '$OUT/0-1.txt' skip $WARMUP with lines ls 1 dt 3 lw 2 smooth bezier title '0% writes, 1 thread, smooth', \
  '$OUT/0-4.txt' skip $WARMUP with lines ls 1 lw 2 smooth bezier title '0% writes, 4 threads, smooth'" \
  > $OUT/plots/0-1vs4.svg

# 25 and 50% writes, 1 thread
gnuplot -e "$PLOT plot \
  '$OUT/25-1.txt' skip $WARMUP with lines ls 2 lw 0.5 title '25% writes, 1 thread, raw', \
  '$OUT/50-1.txt' skip $WARMUP with lines ls 4 lw 0.5 title '50% writes, 1 thread, raw', \
  '$OUT/25-1.txt' skip $WARMUP with lines ls 2 lw 2 smooth bezier title '25% writes, 1 thread, smooth', \
  '$OUT/50-1.txt' skip $WARMUP with lines ls 4 lw 2 smooth bezier title '50% writes, 1 thread, smooth'" \
  > $OUT/plots/25-50-1.svg

# 25 and 50% writes, 4 threads
gnuplot -e "$PLOT plot \
  '$OUT/25-4.txt' skip $WARMUP with lines ls 2 lw 0.5 title '25% writes, 4 threads, raw', \
  '$OUT/50-4.txt' skip $WARMUP with lines ls 4 lw 0.5 title '50% writes, 4 threads, raw', \
  '$OUT/25-4.txt' skip $WARMUP with lines ls 2 lw 2 smooth bezier title '25% writes, 4 threads, smooth', \
  '$OUT/50-4.txt' skip $WARMUP with lines ls 4 lw 2 smooth bezier title '50% writes, 4 threads, smooth'" \
  > $OUT/plots/25-50-4.svg

# 25 and 50% writes, 1 thread vs 4 threads
gnuplot -e "$PLOT plot \
  '$OUT/25-1.txt' skip $WARMUP with lines ls 2 dt 3 lw 2 smooth bezier title '25% writes, 1 thread, smooth', \
  '$OUT/50-1.txt' skip $WARMUP with lines ls 4 dt 3 lw 2 smooth bezier title '50% writes, 1 thread, smooth', \
  '$OUT/25-4.txt' skip $WARMUP with lines ls 2 lw 2 smooth bezier title '25% writes, 4 threads, smooth', \
  '$OUT/50-4.txt' skip $WARMUP with lines ls 4 lw 2 smooth bezier title '50% writes, 4 threads, smooth'" \
  > $OUT/plots/25-50-1vs4.svg

# 100% writes, 1 thread
gnuplot -e "$PLOT plot \
  '$OUT/100-1.txt' skip $WARMUP with lines ls 6 lw 0.5 title '100% writes, 1 thread, raw', \
  '$OUT/100-1.txt' skip $WARMUP with lines ls 6 lw 2 smooth bezier title '100% writes, 1 thread, smooth'" \
  > $OUT/plots/100-1.svg

# 100% writes, 4 threads
gnuplot -e "$PLOT plot \
  '$OUT/100-4.txt' skip $WARMUP with lines ls 6 lw 0.5 title '100% writes, 4 threads, raw', \
  '$OUT/100-4.txt' skip $WARMUP with lines ls 6 lw 2 smooth bezier title '100% writes, 4 threads, smooth'" \
  > $OUT/plots/100-4.svg

# 100% writes, 1 thread vs 4 threads
gnuplot -e "$PLOT plot \
  '$OUT/100-1.txt' skip $WARMUP with lines ls 6 dt 3 lw 2 smooth bezier title '100% writes, 1 thread, smooth', \
  '$OUT/100-4.txt' skip $WARMUP with lines ls 6 lw 2 smooth bezier title '100% writes, 4 threads, smooth'" \
  > $OUT/plots/100-1vs4.svg
