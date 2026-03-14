#!/usr/bin/env bash

set -euo pipefail

DEFAULT_OUT_DIR="bindings/python/src/slatedb/ffi"
CRATE_NAME="slatedb_ffi"
LIB_NAME="slatedb_ffi"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

CARGO_PROFILE="${CARGO_PROFILE:-debug}"
PYTHON_BINDINGS_OUT_DIR="${PYTHON_BINDINGS_OUT_DIR:-${DEFAULT_OUT_DIR}}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/target}"
FORMAT_GENERATED_PYTHON="${FORMAT_GENERATED_PYTHON:-1}"
PYTHON_FORMATTER_BIN="${PYTHON_FORMATTER_BIN:-ruff}"

if [[ "${PYTHON_BINDINGS_OUT_DIR}" = /* ]]; then
    out_dir="${PYTHON_BINDINGS_OUT_DIR}"
else
    out_dir="${repo_root}/${PYTHON_BINDINGS_OUT_DIR}"
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

case "${FORMAT_GENERATED_PYTHON}" in
0|1)
    ;;
*)
    fail "FORMAT_GENERATED_PYTHON must be '0' or '1', got '${FORMAT_GENERATED_PYTHON}'"
    ;;
esac

if [[ "${FORMAT_GENERATED_PYTHON}" = "1" ]]; then
    require_cmd uv
fi

case "$(uname -s)" in
Darwin)
    lib_filename="lib${LIB_NAME}.dylib"
    stale_libraries=(
        "lib${LIB_NAME}.so"
        "${LIB_NAME}.dll"
    )
    ;;
Linux)
    lib_filename="lib${LIB_NAME}.so"
    stale_libraries=(
        "lib${LIB_NAME}.dylib"
        "${LIB_NAME}.dll"
    )
    ;;
MINGW*|MSYS*|CYGWIN*)
    lib_filename="${LIB_NAME}.dll"
    stale_libraries=(
        "lib${LIB_NAME}.dylib"
        "lib${LIB_NAME}.so"
    )
    ;;
*)
    fail "unsupported platform: $(uname -s)"
    ;;
esac

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-python-bindings.XXXXXX")"
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
    cargo run -p slatedb-ffi --bin slatedb-uniffi-bindgen --features bindgen-cli -- generate \
        --library \
        --crate "${CRATE_NAME}" \
        --language python \
        --out-dir "${raw_dir}" \
        --no-format \
        "${lib_path}"
)

generated_file="${raw_dir}/slatedb.py"
[[ -f "${generated_file}" ]] || fail "generated Python bindings not found at ${generated_file}"

cp "${generated_file}" "${out_dir}/slatedb.py"

for stale_library in "${stale_libraries[@]}"; do
    rm -f "${out_dir}/${stale_library}"
done

cp "${lib_path}" "${out_dir}/"

if [[ "${FORMAT_GENERATED_PYTHON}" = "1" ]]; then
    uv tool run "${PYTHON_FORMATTER_BIN}" format "${out_dir}/slatedb.py"
fi
