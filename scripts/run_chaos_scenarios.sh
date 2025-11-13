#!/usr/bin/env bash
set -euo pipefail

# Env: expects MinIO, Envoy and Toxiproxy containers running and mapped to localhost
# Traffic flow is: SlateDB -> mikkmokk-proxy -> Toxiproxy -> MinIO
# - MinIO S3:       localhost:9000
# - mikkmokk-proxy: localhost:8080 (proxies to minio:9000)
# - Toxiproxy API:  localhost:8474
# - Toxiproxy S3:   localhost:9001 (proxy to envoy:8080)

API="http://127.0.0.1:8474"
LISTEN_PORT=9001

log() { echo "[chaos] $*"; }

create_proxy() {
  local name=$1
  local listen=$2
  local upstream=$3
  if curl -fsS "$API/proxies/$name" >/dev/null 2>&1; then
    log "proxy '$name' already exists"
  else
    log "creating proxy '$name' -> $upstream (:$listen)"
    curl -fsS -X POST "$API/proxies" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"$name\",\"listen\":\"0.0.0.0:$listen\",\"upstream\":\"$upstream\"}"
  fi
}

clear_toxics() {
  log "toxiproxy: resetting all proxies/toxics"
  curl -fsS -X POST "$API/reset" >/dev/null
}

add_toxic() {
  local name=$1; shift
  local toxic_name=$1; shift
  local type=$1; shift
  local stream=$1; shift
  local attrs_json=$1; shift
  local toxicity=${1:-1.0}
  log "adding toxic $toxic_name ($type/$stream) toxicity=$toxicity attrs=$attrs_json"
  curl -fsS -X POST "$API/proxies/$name/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$toxic_name\",\"type\":\"$type\",\"stream\":\"$stream\",\"toxicity\":$toxicity,\"attributes\":$attrs_json}"
}

mikkmokk_update_fail_before() {
  local percent=${1:-0}
  local code=${2:-503}
  log "mikkmokk update: fail-before ${percent}% code=${code}"
  curl -fsS -X POST \
    -H "x-mikkmokk-fail-before-percentage: ${percent}" \
    -H "x-mikkmokk-fail-before-code: ${code}" \
    http://127.0.0.1:7070/api/v1/update >/dev/null
}

mikkmokk_reset() {
  log "mikkmokk reset defaults"
  curl -fsS -X POST http://127.0.0.1:7070/api/v1/reset >/dev/null
}

run_smoke() {
  local name=$1
  log "running scenario: $name"
  CLOUD_PROVIDER=aws \
  AWS_ACCESS_KEY_ID=minioadmin \
  AWS_SECRET_ACCESS_KEY=minioadmin \
  AWS_BUCKET=slatedb-test \
  AWS_REGION=us-east-1 \
  AWS_ENDPOINT="http://127.0.0.1:$LISTEN_PORT" \
  RUST_LOG=info \
  cargo nextest run test_concurrent_writers_and_readers -p slatedb --all-features --profile chaos-nightly
}

# Ensure proxy exists
create_proxy s3 $LISTEN_PORT mikkmokk:8080

# Scenarios
pass=0; fail=0
scenarios=()

scenario() {
  local name=$1; shift
  scenarios+=("$name")
  if "$@"; then
    log "scenario '$name' OK"; pass=$((pass+1))
  else
    log "scenario '$name' FAILED"; fail=$((fail+1))
  fi
}

baseline() {
  clear_toxics s3; mikkmokk_reset; run_smoke baseline
}

latency_jitter() {
  clear_toxics s3; mikkmokk_reset
  add_toxic s3 t_latency latency downstream '{"latency":1000,"jitter":300}' 1.0
  add_toxic s3 t_latency_up latency upstream '{"latency":600,"jitter":200}' 1.0
  run_smoke latency_jitter
}

bandwidth_cap() {
  clear_toxics s3; mikkmokk_reset
  add_toxic s3 t_bw bandwidth downstream '{"rate":200}' 1.0
  run_smoke bandwidth_cap
}

reset_peer() {
  clear_toxics s3; mikkmokk_reset
  add_toxic s3 t_reset reset_peer downstream '{}' 0.15
  run_smoke reset_peer
}

slow_close() {
  clear_toxics s3; mikkmokk_reset
  add_toxic s3 t_slow slow_close downstream '{"delay":2000}' 0.3
  run_smoke slow_close
}

timeoutish() {
  clear_toxics s3; mikkmokk_reset
  add_toxic s3 t_timeout latency downstream '{"latency":3000}' 0.35
  run_smoke timeoutish
}

http_500s() {
  clear_toxics s3; mikkmokk_update_fail_before 10 500
  run_smoke http_500s
}

http_404s() {
  clear_toxics s3; mikkmokk_update_fail_before 5 404
  run_smoke http_404s
}

http_429s() {
  clear_toxics s3; mikkmokk_update_fail_before 5 429
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
