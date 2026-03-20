#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
UNIFFI_DIR="${ROOT_DIR}/bindings/uniffi"
JAVA_GEN_DIR="${ROOT_DIR}/bindings/java/slatedb-uniffi/src/generated/java"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-java-uniffi.XXXXXX")"

cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

if ! command -v uniffi-bindgen-java >/dev/null 2>&1; then
  echo "uniffi-bindgen-java is required on PATH" >&2
  echo "Install it with:" >&2
  echo "  cargo install uniffi-bindgen-java --git https://github.com/IronCoreLabs/uniffi-bindgen-java.git --tag 0.2.1" >&2
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
  echo "failed to locate built slatedb_uniffi library under target/debug" >&2
  exit 1
fi

uniffi-bindgen-java generate \
  --library \
  --config "${UNIFFI_DIR}/uniffi.toml" \
  --out-dir "${TMP_DIR}/out" \
  "${LIB_FILE}"

if [[ ! -d "${TMP_DIR}/out/io/slatedb/uniffi" ]]; then
  echo "unexpected generator output under ${TMP_DIR}/out" >&2
  exit 1
fi

rm -rf "${JAVA_GEN_DIR}"
mkdir -p "${JAVA_GEN_DIR}"
cp -R "${TMP_DIR}/out/." "${JAVA_GEN_DIR}/"
