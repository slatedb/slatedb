#!/usr/bin/env bash
#
# Run the S3-backed Go timeout tests against LocalStack + Toxiproxy.
#
# Prerequisites:
#   - Docker and docker compose
#   - AWS CLI (aws) on PATH
#   - Go toolchain
#   - Rust toolchain (to build slatedb-uniffi)
set -euo pipefail

REPO_ROOT="$(realpath $(dirname $0)/..)"
COMPOSE_FILE="$REPO_ROOT/scripts/go-timeout-tests.compose.yaml"
LOCALSTACK_ENDPOINT="http://127.0.0.1:14566"
TOXIPROXY_ADMIN="http://127.0.0.1:18474"
TOXIPROXY_PROXY="http://127.0.0.1:19001"
BUCKET="slatedb-go-test"
DB_PATH="timeout-test"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d --wait

cargo build -p slatedb-uniffi

AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url "$LOCALSTACK_ENDPOINT" s3 mb "s3://$BUCKET"

curl -sf -X POST "$TOXIPROXY_ADMIN/proxies" \
  -H 'Content-Type: application/json' \
  -d '{"name":"s3","listen":"0.0.0.0:9001","upstream":"go-timeout-localstack:4566"}'

curl -fsS -w '\n' -X POST "$TOXIPROXY_ADMIN/proxies/s3/toxics" \
    -H 'Content-Type: application/json' \
    -d '{"name":"s3_latency","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{"latency":100}}'

curl -fsS -w '\n' -X POST "$TOXIPROXY_ADMIN/proxies/s3/toxics" \
    -H 'Content-Type: application/json' \
    -d '{"name":"s3_latency_up","type":"latency","stream":"upstream","toxicity":1.0,"attributes":{"latency":100}}'


cd "$REPO_ROOT/bindings/go"

CGO_ENABLED=1 \
CGO_LDFLAGS=-L$REPO_ROOT/target/debug \
LD_LIBRARY_PATH=$REPO_ROOT/target/debug:${LD_LIBRARY_PATH:-} \
DYLD_LIBRARY_PATH=$REPO_ROOT/target/debug:${DYLD_LIBRARY_PATH:-} \
SLATEDB_S3_URL=s3://$BUCKET/$DB_PATH \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
AWS_REGION=us-east-1 \
AWS_ALLOW_HTTP=true \
AWS_ENDPOINT_URL="$TOXIPROXY_PROXY" \
TOXIPROXY_ADMIN_URL="$TOXIPROXY_ADMIN" \
go test -v -run 'WithTimeoutTripped$' -tags toxic -timeout 120s ./uniffi/
