#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
GO_PKG_DIR="${ROOT_DIR}/bindings/go/uniffi"
UNIFFI_DIR="${ROOT_DIR}/bindings/uniffi"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-go-uniffi.XXXXXX")"

cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

if ! command -v uniffi-bindgen-go >/dev/null 2>&1; then
  echo "uniffi-bindgen-go is required on PATH" >&2
  echo "Install it with:" >&2
  echo "  cargo install uniffi-bindgen-go --git https://github.com/NordSecurity/uniffi-bindgen-go --tag v0.5.0+v0.29.5" >&2
  exit 1
fi

cargo build --manifest-path "${ROOT_DIR}/Cargo.toml" -p slatedb-uniffi

LIB_FILE=""
for candidate in \
  "${ROOT_DIR}/target/debug/libslatedb_uniffi.so" \
  "${ROOT_DIR}/target/debug/libslatedb_uniffi.dylib" \
  "${ROOT_DIR}/target/debug/slatedb_uniffi.dll"; do
  if [[ -f "${candidate}" ]]; then
    LIB_FILE="${candidate}"
    break
  fi
done

if [[ -z "${LIB_FILE}" ]]; then
  echo "failed to locate built slatedb-uniffi library under target/debug" >&2
  exit 1
fi

mkdir -p "${GO_PKG_DIR}"

uniffi-bindgen-go "${LIB_FILE}" \
  --library \
  --config "${UNIFFI_DIR}/uniffi.toml" \
  --out-dir "${TMP_DIR}/out"

GENERATED_DIR="${TMP_DIR}/out/slatedb"

GENERATED_GO_FILE="$(find "${GENERATED_DIR}" -maxdepth 1 -type f -name '*.go' | head -n 1)"
GENERATED_H_FILE="$(find "${GENERATED_DIR}" -maxdepth 1 -type f -name '*.h' | head -n 1)"

if [[ -z "${GENERATED_GO_FILE}" || -z "${GENERATED_H_FILE}" ]]; then
  echo "unexpected generator output in ${GENERATED_DIR}" >&2
  exit 1
fi

rm -f \
  "${GO_PKG_DIR}/slatedb.go" \
  "${GO_PKG_DIR}/slatedb.h"
cp "${GENERATED_GO_FILE}" "${GO_PKG_DIR}/$(basename "${GENERATED_GO_FILE}")"
cp "${GENERATED_H_FILE}" "${GO_PKG_DIR}/$(basename "${GENERATED_H_FILE}")"
go -C "${GO_PKG_DIR}" fmt ./...
