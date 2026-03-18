#!/usr/bin/env bash

set -euo pipefail

EXPECTED_BINDGEN_VERSION="0.5.0+v0.29.5"
DEFAULT_OUT_DIR="bindings/go/ffi"
CRATE_NAME="slatedb_uniffi"
LIB_NAME="slatedb_uniffi"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

UNIFFI_BINDGEN_GO_BIN="${UNIFFI_BINDGEN_GO_BIN:-uniffi-bindgen-go}"
CARGO_PROFILE="${CARGO_PROFILE:-debug}"
GO_BINDINGS_OUT_DIR="${GO_BINDINGS_OUT_DIR:-${DEFAULT_OUT_DIR}}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/target}"

if [[ "${GO_BINDINGS_OUT_DIR}" = /* ]]; then
	out_dir="${GO_BINDINGS_OUT_DIR}"
else
	out_dir="${repo_root}/${GO_BINDINGS_OUT_DIR}"
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
require_cmd gofmt
require_cmd "${UNIFFI_BINDGEN_GO_BIN}"

bindgen_version="$("${UNIFFI_BINDGEN_GO_BIN}" --version 2>/dev/null || true)"
case "${bindgen_version}" in
	*"${EXPECTED_BINDGEN_VERSION}"*)
		;;
	*)
		fail "expected ${UNIFFI_BINDGEN_GO_BIN} version ${EXPECTED_BINDGEN_VERSION}, got '${bindgen_version:-missing}'"
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

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-go-bindings.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

raw_dir="${tmp_dir}/raw"
stage_dir="${tmp_dir}/stage"
mkdir -p "${raw_dir}" "${stage_dir}" "${out_dir}"

(
	cd "${repo_root}"
	if [[ "${CARGO_PROFILE}" = "release" ]]; then
		cargo build --release -p slatedb-uniffi
	else
		cargo build -p slatedb-uniffi
	fi
)

lib_dir="${CARGO_TARGET_DIR}/${CARGO_PROFILE}"
lib_path="${lib_dir}/${lib_filename}"
[[ -f "${lib_path}" ]] || fail "generated library not found at ${lib_path}"

(
	cd "${repo_root}"
	"${UNIFFI_BINDGEN_GO_BIN}" --library --crate "${CRATE_NAME}" -o "${raw_dir}" "${lib_path}"
)

generated_dir=""
generated_dir_count=0
for candidate in "${raw_dir}"/*; do
	[[ -d "${candidate}" ]] || continue
	generated_dir="${candidate}"
	generated_dir_count=$((generated_dir_count + 1))
done

[[ "${generated_dir_count}" -eq 1 ]] || fail "expected exactly one generated package directory, found ${generated_dir_count}"

generated_file_count=0
for generated_file in "${generated_dir}"/*; do
	[[ -f "${generated_file}" ]] || continue
	cp "${generated_file}" "${stage_dir}/"
	generated_file_count=$((generated_file_count + 1))
done

[[ "${generated_file_count}" -gt 0 ]] || fail "no generated files found in ${generated_dir}"

for go_file in "${stage_dir}"/*.go; do
	[[ -f "${go_file}" ]] || continue
	first_line="$(head -n 1 "${go_file}")"
	case "${first_line}" in
	"package "*)
		{
			printf 'package ffi\n'
			tail -n +2 "${go_file}"
		} > "${go_file}.tmp"
		mv "${go_file}.tmp" "${go_file}"
		;;
	*)
		fail "unexpected first line in ${go_file}: ${first_line}"
		;;
	esac
done

preserve_files=(
	doc.go
	link.go
)

should_preserve() {
	local basename="$1"
	local preserved
	for preserved in "${preserve_files[@]}"; do
		if [[ "${basename}" = "${preserved}" ]]; then
			return 0
		fi
	done
	return 1
}

for existing in "${out_dir}"/*; do
	[[ -e "${existing}" ]] || continue
	basename="$(basename "${existing}")"
	if should_preserve "${basename}"; then
		continue
	fi
	rm -rf "${existing}"
done

for staged in "${stage_dir}"/*; do
	[[ -f "${staged}" ]] || continue
	cp "${staged}" "${out_dir}/"
done

gofmt -w "${out_dir}"/*.go
