#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $(basename "$0") <package-dir> <target-id> <library-path>" >&2
  exit 1
fi

package_dir="$1"
target_id="$2"
library_path="$3"

if [[ ! -d "${package_dir}" ]]; then
  echo "package directory does not exist: ${package_dir}" >&2
  exit 1
fi

if [[ ! -f "${library_path}" ]]; then
  echo "library file does not exist: ${library_path}" >&2
  exit 1
fi

destination_dir="${package_dir}/prebuilds/${target_id}"
mkdir -p "${destination_dir}"
cp "${library_path}" "${destination_dir}/$(basename "${library_path}")"
