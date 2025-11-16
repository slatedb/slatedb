#!/usr/bin/env bash
#
# SlateDB Chaos Scenarios Runner
#
# Orchestrates network- and HTTP-level chaos to validate SlateDB resilience against
# latency, bandwidth constraints, TCP failures, and transient HTTP error responses.
#
# Components (pre-started by the workflow):
# - LocalStack (S3-compatible)
# - chaos-http-proxy (HTTP faults): proxy on 1080, admin on 1080/chaos/api
# - Toxiproxy (TCP faults): API on 8474
#
# Local bindings used by this script:
# - chaos-http-proxy: localhost:1080
# - Toxiproxy S3: localhost:9001 -> LocalStack:4566
# - Toxiproxy admin API: localhost:8474
#
# Data path:
#   TCP-level scenarios:
#     SlateDB -> Toxiproxy (localhost:9001) -> LocalStack:4566
#   HTTP-level scenarios (fail-before only):
#     SlateDB -> chaos-http-proxy (localhost:1080) -> LocalStack:4566
#
# Scenarios executed by this script:
# - baseline: No HTTP or TCP faults (green path).
# - latency_jitter: Add latency with jitter both ways.
# - bandwidth_cap: Cap downstream bandwidth to ~200 kbps.
# - reset_peer: ~15% intermittent downstream TCP resets.
# - slow_close: ~30% downstream TCP close delay by ~2000ms.
# - timeoutish: ~35% downstream latency (~3000ms).
# - http_503s: ~10% fail-before responses with HTTP 503 (transient server errors).
# - http_404s: ~5% fail-before responses with HTTP 404 (transient missing paths/keys).
# - http_429s: ~5% fail-before responses with HTTP 429 (transient throttling).
#
# Local usage (running the chaos services + script):
# 1. Prerequisites:
#    - Docker and docker compose.
#    - Rust toolchain + cargo.
#    - AWS CLI (`aws`) installed and on PATH.
#    - Java + Maven to build chaos-http-proxy.
# 2. From the SlateDB repo root, clone and build chaos-http-proxy:
#      git clone https://github.com/bouncestorage/chaos-http-proxy
#      cd chaos-http-proxy
#      mvn -q -DskipTests package
#      cd ..
# 3. Start the chaos services via docker compose from the SlateDB repo root:
#      docker compose -f scripts/run_chaos_scenarios.compose.yaml up -d
#    This starts:
#      - LocalStack S3 on localhost:4566
#      - chaos-http-proxy on localhost:1080 (admin at /chaos/api)
#      - Toxiproxy on localhost:8474 (API) and localhost:9001 (S3 proxy)
# 4. Optionally verify readiness:
#      curl -sf http://127.0.0.1:4566/health
#      curl -sf http://127.0.0.1:1080/chaos/api
#      curl -sf http://127.0.0.1:8474/proxies
# 5. Run the chaos scenarios from the SlateDB repo root:
#      chmod +x scripts/run_chaos_scenarios.sh
#      scripts/run_chaos_scenarios.sh
#    The script will create/reset the `slatedb-test` bucket in LocalStack and
#    run the `test_concurrent_writers_and_readers` integration test against the
#    configured proxies for each scenario.
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

TOXIPROXY_S3=http://127.0.0.1:9001
TOXIPROXY_API=http://127.0.0.1:8474
LOCALSTACK_S3=http://127.0.0.1:4566
CHAOS_HTTP_PROXY=http://127.0.0.1:1080
CHAOS_HTTP_API=http://127.0.0.1:1080/chaos/api

# Print a prefixed message for easier scanning in CI logs.
log() { echo "[chaos] $*"; }

# Require aws CLI for bucket reset and environment setup.
if ! command -v aws >/dev/null 2>&1; then
  log "aws CLI not found; please install aws-cli before running chaos scenarios"
  exit 1
fi

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
  curl -fsS -w '\n' -X POST "$TOXIPROXY_API/proxies/$name/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$toxic_name\",\"type\":\"$type\",\"stream\":\"$stream\",\"toxicity\":$toxicity,\"attributes\":$attrs_json}"
}

# Configure chaos-http-proxy failure rates at runtime.
# Args:
#   $1 percent : 0..100 (chance to fail-before)
#   $2 code    : HTTP status code to emulate (503|404|429)
add_http_failure() {
  local percent=${1:-0}
  local code=${2:-503}
  local fail_key

  case "$code" in
    503) fail_key="com.bouncestorage.chaoshttpproxy.http_503" ;;
    404) fail_key="com.bouncestorage.chaoshttpproxy.http_500" ;;
    429) fail_key="com.bouncestorage.chaoshttpproxy.http_504" ;;
    *)   fail_key="com.bouncestorage.chaoshttpproxy.http_503" ;;
  esac

  local success=$((100 - percent))
  log "chaos-http-proxy update: ${percent}% -> code=${code} (key=${fail_key})"

  cat <<EOF | curl -fsS --request POST --data-binary @- "$CHAOS_HTTP_API" >/dev/null
$fail_key=$percent
com.bouncestorage.chaoshttpproxy.success=$success
EOF
}

# Restore chaos-http-proxy runtime fault settings to all-success.
clear_http_failures() {
  log "chaos-http-proxy reset defaults"
  cat <<EOF | curl -fsS --request POST --data-binary @- "$CHAOS_HTTP_API" >/dev/null
com.bouncestorage.chaoshttpproxy.success=100
EOF
}

# Drop and recreate the LocalStack S3 bucket used by tests.
reset_bucket() {
  local bucket="slatedb-test"
  log "s3: resetting bucket s3://${bucket}"

  # Remove bucket and all its contents if it exists.
  AWS_ACCESS_KEY_ID=test \
  AWS_SECRET_ACCESS_KEY=test \
  AWS_REGION=us-east-1 \
  aws --endpoint-url "$LOCALSTACK_S3" s3 rb "s3://${bucket}" --force >/dev/null 2>&1 || true

  # Recreate the bucket.
  AWS_ACCESS_KEY_ID=test \
  AWS_SECRET_ACCESS_KEY=test \
  AWS_REGION=us-east-1 \
  aws --endpoint-url "$LOCALSTACK_S3" s3 mb "s3://${bucket}" >/dev/null
}

# List all objects currently stored in the LocalStack bucket.
list_bucket_contents() {
  local bucket="slatedb-test"
  log "s3: listing objects in s3://${bucket}"
  AWS_ACCESS_KEY_ID=test \
  AWS_SECRET_ACCESS_KEY=test \
  AWS_REGION=us-east-1 \
  aws --endpoint-url "$LOCALSTACK_S3" s3 ls "s3://${bucket}" --recursive || \
    log "s3: failed to list contents of s3://${bucket}"
}

# Execute the SlateDB integration test against the configured proxies.
# Args:
#   $1 name     : scenario label for logging
#   $2 endpoint : S3 endpoint URL
run_smoke() {
  local name=$1
  local endpoint=$2
  log "running scenario: $name (endpoint=$endpoint)"
  # `AWS_S3_FORCE_PATH_STYLE` is set below to avoid virtual-hosted-style Host/SigV4
  # issues when routing through localhost ports and proxies (Toxiproxy + chaos-http-proxy).
  CLOUD_PROVIDER=aws \
  AWS_ACCESS_KEY_ID=test \
  AWS_SECRET_ACCESS_KEY=test \
  AWS_BUCKET=slatedb-test \
  AWS_REGION=us-east-1 \
  AWS_ENDPOINT="$endpoint" \
  AWS_S3_FORCE_PATH_STYLE=true \
  RUST_LOG=${RUST_LOG:-info} \
  cargo test --quiet -p slatedb --test db test_concurrent_writers_and_readers -- --nocapture
}

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
  log "resetting S3 bucket before scenario '$name'"
  reset_bucket
  if "$@"; then
    log "scenario '$name' OK"; pass=$((pass+1))
  else
    log "scenario '$name' FAILED"; fail=$((fail+1))
  fi

  # After each scenario, print the final contents of the
  # backing object store so chaos runs can inspect state.
  list_bucket_contents
}

# No HTTP faults, no TCP faults (green path).
baseline() {
  clear_toxics s3; clear_http_failures; run_smoke baseline "$TOXIPROXY_S3"
}

# Add high latency + jitter both directions; HTTP faults disabled.
latency_jitter() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_latency latency downstream '{"latency":1000,"jitter":300}' 1.0
  add_toxic s3 t_latency_up latency upstream '{"latency":600,"jitter":200}' 1.0
  run_smoke latency_jitter "$TOXIPROXY_S3"
}

# Limit downstream bandwidth to 200 kbps.
bandwidth_cap() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_bw bandwidth downstream '{"rate":200}' 1.0
  run_smoke bandwidth_cap "$TOXIPROXY_S3"
}

# Inject intermittent TCP RST on downstream (15% of connections).
reset_peer() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_reset reset_peer downstream '{}' 0.15
  run_smoke reset_peer "$TOXIPROXY_S3"
}

# Delay TCP close on downstream (30% of connections).
slow_close() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_slow slow_close downstream '{"delay":2000}' 0.3
  run_smoke slow_close "$TOXIPROXY_S3"
}

# Large downstream latency (3s) affecting ~35% of connections.
timeoutish() {
  clear_toxics s3; clear_http_failures
  add_toxic s3 t_timeout latency downstream '{"latency":3000}' 0.35
  run_smoke timeoutish "$TOXIPROXY_S3"
}

# 10% fail-before with HTTP 503 (transient server errors).
http_503s() {
  clear_toxics s3; add_http_failure 10 503
  AWS_PROXY_URL="$CHAOS_HTTP_PROXY" run_smoke http_503s "$LOCALSTACK_S3"
}

# 5% fail-before with HTTP 404 (transient missing paths/keys).
http_404s() {
  clear_toxics s3; add_http_failure 5 404
  AWS_PROXY_URL="$CHAOS_HTTP_PROXY" run_smoke http_404s "$LOCALSTACK_S3"
}

# 5% fail-before with HTTP 429 (transient throttling).
http_429s() {
  clear_toxics s3; add_http_failure 5 429
  AWS_PROXY_URL="$CHAOS_HTTP_PROXY" run_smoke http_429s "$LOCALSTACK_S3"
}

# Execute
scenario baseline baseline
scenario latency_jitter latency_jitter
scenario bandwidth_cap bandwidth_cap
scenario reset_peer reset_peer
scenario slow_close slow_close
scenario timeoutish timeoutish
scenario http_503s http_503s
scenario http_404s http_404s
scenario http_429s http_429s

log "summary: pass=$pass fail=$fail"
[ "$fail" -eq 0 ]
