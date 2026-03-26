#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Generate a publishable SlateDB Node package into a staging directory.

Usage:
  generate-node-package.sh --out-dir <dir> --version <semver> [--release] [--stage-host-prebuild]

Options:
  --out-dir <dir>           Package staging directory. It must not contain existing files.
  --version <semver>        Package version written into package.json.
  --release                 Build the host cdylib with cargo --release.
  --stage-host-prebuild     Copy the built host cdylib into prebuilds/<host-target>/.
  -h, --help                Show this help text.
EOF
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
root_dir="$(cd "${script_dir}/.." >/dev/null 2>&1 && pwd)"

out_dir=""
version=""
build_profile="debug"
stage_host_prebuild=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      out_dir="$2"
      shift 2
      ;;
    --version)
      version="$2"
      shift 2
      ;;
    --release)
      build_profile="release"
      shift
      ;;
    --stage-host-prebuild)
      stage_host_prebuild=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${out_dir}" || -z "${version}" ]]; then
  usage >&2
  exit 1
fi

if ! command -v uniffi-bindgen-node-js >/dev/null 2>&1; then
  echo "uniffi-bindgen-node-js is required on PATH" >&2
  echo "Install it with:" >&2
  echo "  cargo install uniffi-bindgen-node-js --git https://github.com/criccomini/uniffi-bindgen-node-js.git --branch main --locked" >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "node is required on PATH to finalize the generated package metadata" >&2
  exit 1
fi

if [[ -e "${out_dir}" ]] && find "${out_dir}" -mindepth 1 -print -quit | grep -q .; then
  echo "output directory must be empty: ${out_dir}" >&2
  exit 1
fi

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-node-package.XXXXXX")"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

build_args=(
  build
  --manifest-path "${root_dir}/Cargo.toml"
  -p slatedb-uniffi
)
if [[ "${build_profile}" == "release" ]]; then
  build_args+=(--release)
fi

cargo "${build_args[@]}"

lib_file=""
for candidate in \
  "${root_dir}/target/${build_profile}/libslatedb_uniffi.so" \
  "${root_dir}/target/${build_profile}/libslatedb_uniffi.dylib" \
  "${root_dir}/target/${build_profile}/slatedb_uniffi.dll"; do
  if [[ -f "${candidate}" ]]; then
    lib_file="${candidate}"
    break
  fi
done

if [[ -z "${lib_file}" ]]; then
  echo "failed to locate built slatedb-uniffi library under target/${build_profile}" >&2
  exit 1
fi

uniffi-bindgen-node-js generate \
  "${lib_file}" \
  --crate-name slatedb-uniffi \
  --out-dir "${tmp_dir}"

node "${root_dir}/bindings/node/scripts/prepare-package.mjs" "${tmp_dir}" "${version}"
cp "${root_dir}/bindings/node/README.md" "${tmp_dir}/README.md"
cp "${root_dir}/LICENSE" "${tmp_dir}/LICENSE"

if [[ "${stage_host_prebuild}" -eq 1 ]]; then
  host_target="$("${root_dir}/scripts/detect-node-prebuild-target.sh")"
  "${root_dir}/scripts/stage-node-prebuild.sh" "${tmp_dir}" "${host_target}" "${lib_file}"
fi

mkdir -p "${out_dir}"
cp -R "${tmp_dir}/." "${out_dir}/"
