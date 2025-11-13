#!/usr/bin/env bash
#
# SlateDB Chaos Scenarios Runner
#
# Orchestrates network- and HTTP-level chaos to validate SlateDB resilience against
# latency, bandwidth constraints, TCP failures, and transient HTTP error responses.
#
# Components (pre-started by the workflow):
# - MinIO (S3-compatible)
# - mikkmokk-proxy (HTTP faults): proxy on 8080, admin on 7070
# - Toxiproxy (TCP faults): API on 8474
#
# Local bindings used by this script:
# - mikkmokk-proxy HTTP: localhost:8080 (for Toxiproxy upstream)
# - mikkmokk-proxy admin: localhost:7070 (runtime fault config)
# - Toxiproxy API: localhost:8474
# - Toxiproxy S3 proxy: localhost:9001
#
# Data path:
#   SlateDB -> Toxiproxy (localhost:9001) -> mikkmokk-proxy:8080 -> MinIO:9000
#
# Scenarios executed by this script:
# - baseline: No HTTP or TCP faults (green path).
# - latency_jitter: Add latency with jitter both ways.
# - bandwidth_cap: Cap downstream bandwidth to ~200 kbps.
# - reset_peer: Intermittent downstream TCP resets.
# - slow_close: Delay downstream TCP close by ~2000ms.
# - timeoutish: Large downstream latency (~3000ms).
# - http_500s: ~10% fail-before responses with HTTP 500 (transient server errors).
# - http_404s: ~5% fail-before responses with HTTP 404 (transient missing paths/keys).
# - http_429s: ~5% fail-before responses with HTTP 429 (transient throttling).
#
# Environment variables respected:
# - SLATEDB_TEST_NUM_WRITERS: Number of writer tasks (default: 10)
# - SLATEDB_TEST_NUM_READERS: Number of concurrent reader tasks (default: 2)
# - SLATEDB_TEST_WRITES_PER_TASK: Writes per writer task (default: 100)
# - SLATEDB_TEST_KEY_LENGTH: Key length in bytes for padded keys (default: 256)
# - RUST_LOG: Optional logging level for the test (default: info)
#
# Exit behavior: prints a summary and returns non-zero if any scenario failed.

set -euo pipefail

TOXIPROXY_API=http://127.0.0.1:8474
MIKKMOKK_API=http://127.0.0.1:7070

# Print a prefixed message for easier scanning in CI logs.
log() { echo "[chaos] $*"; }

# Idempotently create a Toxiproxy proxy mapping listen -> upstream.
# Args:
#   $1 name     : proxy name (e.g., s3)
#   $2 listen   : listen port (e.g., 9001)
#   $3 upstream : upstream host:port (e.g., mikkmokk:8080)
init_toxiproxy() {
  local name=$1
  local listen=$2
  local upstream=$3
  if curl -fsS "$TOXIPROXY_API/proxies/$name" >/dev/null 2>&1; then
    log "proxy '$name' already exists"
  else
    log "creating proxy '$name' -> $upstream (:$listen)"
    curl -fsS -X POST "$TOXIPROXY_API/proxies" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"$name\",\"listen\":\"0.0.0.0:$listen\",\"upstream\":\"$upstream\"}"
  fi
}

# Globally reset all toxics/proxies via Toxiproxy. Safe here (single proxy).
clear_toxics() {
  log "toxiproxy: resetting all proxies/toxics"
  curl -fsS -X POST "$TOXIPROXY_API/reset" >/dev/null
}

# Attach a toxic to a proxy (optionally with partial toxicity).
# Args:
#   $1 name        : proxy name (e.g., s3)
#   $2 toxic_name  : label/id for the toxic
#   $3 type        : latency|bandwidth|reset_peer|slow_close|timeout|...
#   $4 stream      : downstream|upstream
#   $5 attrs_json  : JSON object of attributes (e.g., '{"latency":1000,"jitter":300}')
#   $6 toxicity    : optional 0.0..1.0, default 1.0
add_toxic() {
  local name=$1; shift
  local toxic_name=$1; shift
  local type=$1; shift
  local stream=$1; shift
  local attrs_json=$1; shift
  local toxicity=${1:-1.0}
  log "adding toxic $toxic_name ($type/$stream) toxicity=$toxicity attrs=$attrs_json"
  curl -fsS -X POST "$TOXIPROXY_API/proxies/$name/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$toxic_name\",\"type\":\"$type\",\"stream\":\"$stream\",\"toxicity\":$toxicity,\"attributes\":$attrs_json}"
}

# Set mikkmokk-proxy default fail-before percentage/code at runtime.
# Args:
#   $1 percent : 0..100 (chance to fail-before)
#   $2 code    : HTTP status code to return (e.g., 404|429|500)
add_http_failure() {
  local percent=${1:-0}
  local code=${2:-503}
  log "mikkmokk update: fail-before ${percent}% code=${code}"
  curl -fsS -X POST \
    -H "x-mikkmokk-fail-before-percentage: ${percent}" \
    -H "x-mikkmokk-fail-before-code: ${code}" \
    "$MIKKMOKK_API/api/v1/update" >/dev/null
}

# Restore mikkmokk-proxy runtime fault settings to startup defaults.
clear_http_failures() {
  log "mikkmokk reset defaults"
  curl -fsS -X POST "$MIKKMOKK_API/api/v1/reset" >/dev/null
}

# Execute the SlateDB integration test against the configured proxies.
# Args:
#   $1 name : scenario label for logging
run_smoke() {
  local name=$1
  log "running scenario: $name"
  # `AWS_S3_FORCE_PATH_STYLE` is set below to avoid virtual-hosted-style Host/SigV4
  # issues when routing through localhost ports and proxies (Toxiproxy + mikkmokk).
  CLOUD_PROVIDER=aws \
  AWS_ACCESS_KEY_ID=minioadmin \
  AWS_SECRET_ACCESS_KEY=minioadmin \
  AWS_BUCKET=slatedb-test \
  AWS_REGION=us-east-1 \
  AWS_S3_FORCE_PATH_STYLE=true \
  AWS_ENDPOINT="http://127.0.0.1:9001" \
  RUST_LOG=${RUST_LOG:-info} \
  cargo test --quiet -p slatedb --test db test_concurrent_writers_and_readers -- --nocapture
}

# Initialize proxies
init_toxiproxy s3 9001 mikkmokk:8080

# Scenarios
pass=0; fail=0
scenarios=()

# Run a scenario function and track pass/fail.
# Args:
#   $1 name : scenario name (for reporting)
#   $@      : command/function to execute
scenario() {
  local name=$1; shift
  scenarios+=("$name")
  if "$@"; then
    log "scenario '$name' OK"; pass=$((pass+1))
  else
    log "scenario '$name' FAILED"; fail=$((fail+1))
  fi
}

# No HTTP faults, no TCP faults (green path).
baseline() {
  clear_toxics s3; clear_http_failures; run_smoke baseline
}

# Add high latency + jitter both directions; HTTP faults disabled.
latency_jitter() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_latency latency downstream '{"latency":1000,"jitter":300}' 1.0
  add_toxic s3 t_latency_up latency upstream '{"latency":600,"jitter":200}' 1.0
  run_smoke latency_jitter
}

# Limit downstream bandwidth to 200 kbps.
bandwidth_cap() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_bw bandwidth downstream '{"rate":200}' 1.0
  run_smoke bandwidth_cap
}

# Inject intermittent TCP RST on downstream (15% of connections).
reset_peer() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_reset reset_peer downstream '{}' 0.15
  run_smoke reset_peer
}

# Delay TCP close on downstream (30% of connections).
slow_close() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_slow slow_close downstream '{"delay":2000}' 0.3
  run_smoke slow_close
}

# Large downstream latency (3s) affecting ~35% of connections.
timeoutish() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_timeout latency downstream '{"latency":3000}' 0.35
  run_smoke timeoutish
}

# 10% fail-before with HTTP 500 (transient server errors).
http_500s() {
  clear_toxics s3; add_http_failure 10 500
  run_smoke http_500s
}

# 5% fail-before with HTTP 404 (transient missing paths/keys).
http_404s() {
  clear_toxics s3; add_http_failure 5 404
  run_smoke http_404s
}

# 5% fail-before with HTTP 429 (transient throttling).
http_429s() {
  clear_toxics s3; add_http_failure 5 429
  run_smoke http_429s
}

# Execute
scenario baseline baseline
scenario latency_jitter latency_jitter
scenario bandwidth_cap bandwidth_cap
scenario reset_peer reset_peer
scenario slow_close slow_close
scenario timeoutish timeoutish
scenario http_500s http_500s
scenario http_404s http_404s
scenario http_429s http_429s

log "summary: pass=$pass fail=$fail"
[ "$fail" -eq 0 ]
