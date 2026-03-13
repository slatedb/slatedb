#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROFILE="${PROFILE:-debug}"
UNIFFI_BINDGEN_GO="${UNIFFI_BINDGEN_GO:-uniffi-bindgen-go}"

case "$(uname -s)" in
  Darwin)
    LIB_EXT="dylib"
    ;;
  Linux)
    LIB_EXT="so"
    ;;
  *)
    echo "unsupported platform: $(uname -s)" >&2
    exit 1
    ;;
esac

if ! command -v "${UNIFFI_BINDGEN_GO}" >/dev/null 2>&1; then
  echo "missing ${UNIFFI_BINDGEN_GO} in PATH" >&2
  exit 1
fi

if [[ "${PROFILE}" == "release" ]]; then
  TARGET_DIR="release"
else
  TARGET_DIR="debug"
fi

LIB_FILE="${REPO_ROOT}/target/${TARGET_DIR}/libslatedb_ffi.${LIB_EXT}"
STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-go-gen.XXXXXX")"
trap 'rm -rf "${STAGING_DIR}"' EXIT

if [[ "${PROFILE}" == "release" ]]; then
  cargo build --release -p slatedb-ffi
else
  cargo build -p slatedb-ffi
fi

"${UNIFFI_BINDGEN_GO}" \
  "${LIB_FILE}" \
  --library \
  --crate slatedb_ffi \
  --config "${SCRIPT_DIR}/uniffi.toml" \
  --out-dir "${STAGING_DIR}"

cp "${STAGING_DIR}/slatedb/slatedb.go" "${SCRIPT_DIR}/slatedb.go"
cp "${STAGING_DIR}/slatedb/slatedb.h" "${SCRIPT_DIR}/slatedb.h"
