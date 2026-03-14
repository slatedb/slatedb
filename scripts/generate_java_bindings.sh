#!/usr/bin/env bash

set -euo pipefail

EXPECTED_BINDGEN_VERSION="0.2.1"
DEFAULT_OUT_DIR="bindings/java/src/main/java"
CRATE_NAME="slatedb_ffi"
LIB_NAME="slatedb_ffi"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

UNIFFI_BINDGEN_JAVA_BIN="${UNIFFI_BINDGEN_JAVA_BIN:-uniffi-bindgen-java}"
CARGO_PROFILE="${CARGO_PROFILE:-debug}"
JAVA_BINDINGS_OUT_DIR="${JAVA_BINDINGS_OUT_DIR:-${DEFAULT_OUT_DIR}}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/target}"

if [[ "${JAVA_BINDINGS_OUT_DIR}" = /* ]]; then
    out_dir="${JAVA_BINDINGS_OUT_DIR}"
else
    out_dir="${repo_root}/${JAVA_BINDINGS_OUT_DIR}"
fi

fail() {
    echo "error: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

case "${CARGO_PROFILE}" in
debug|release)
    ;;
*)
    fail "CARGO_PROFILE must be 'debug' or 'release', got '${CARGO_PROFILE}'"
    ;;
esac

require_cmd cargo
require_cmd "${UNIFFI_BINDGEN_JAVA_BIN}"

bindgen_version="$("${UNIFFI_BINDGEN_JAVA_BIN}" --version 2>/dev/null || true)"
case "${bindgen_version}" in
    *"${EXPECTED_BINDGEN_VERSION}"*)
        ;;
    *)
        fail "expected ${UNIFFI_BINDGEN_JAVA_BIN} version ${EXPECTED_BINDGEN_VERSION}, got '${bindgen_version:-missing}'"
        ;;
esac

case "$(uname -s)" in
Darwin)
    lib_filename="lib${LIB_NAME}.dylib"
    ;;
Linux)
    lib_filename="lib${LIB_NAME}.so"
    ;;
MINGW*|MSYS*|CYGWIN*)
    lib_filename="${LIB_NAME}.dll"
    ;;
*)
    fail "unsupported platform: $(uname -s)"
    ;;
esac

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-java-bindings.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

raw_dir="${tmp_dir}/raw"
mkdir -p "${raw_dir}" "${out_dir}"

(
    cd "${repo_root}"
    if [[ "${CARGO_PROFILE}" = "release" ]]; then
        cargo build --release -p slatedb-ffi
    else
        cargo build -p slatedb-ffi
    fi
)

lib_dir="${CARGO_TARGET_DIR}/${CARGO_PROFILE}"
lib_path="${lib_dir}/${lib_filename}"
[[ -f "${lib_path}" ]] || fail "generated library not found at ${lib_path}"

(
    cd "${repo_root}"
    "${UNIFFI_BINDGEN_JAVA_BIN}" generate --library --crate "${CRATE_NAME}" -o "${raw_dir}" "${lib_path}"
)

generated_dir="${raw_dir}/io/slatedb/jna/ffi"
[[ -d "${generated_dir}" ]] || fail "generated package directory not found at ${generated_dir}"

rm -rf "${out_dir}/io/slatedb/jna/ffi"
mkdir -p "${out_dir}/io/slatedb/jna"
cp -R "${generated_dir}" "${out_dir}/io/slatedb/jna/"

generated_file_count="$(find "${out_dir}/io/slatedb/jna/ffi" -type f -name '*.java' | wc -l | tr -d '[:space:]')"
[[ "${generated_file_count}" -gt 0 ]] || fail "no generated Java files found in ${out_dir}/io/slatedb/jna/ffi"

(
    cd "${repo_root}"
    ./bindings/java/gradlew --quiet formatGeneratedJava
)
