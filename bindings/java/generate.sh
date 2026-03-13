#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PROFILE="${PROFILE:-debug}"
UNIFFI_BINDGEN_JAVA="${UNIFFI_BINDGEN_JAVA:-uniffi-bindgen-java}"

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

if ! command -v "${UNIFFI_BINDGEN_JAVA}" >/dev/null 2>&1; then
  echo "missing ${UNIFFI_BINDGEN_JAVA} in PATH" >&2
  exit 1
fi

if [[ "${PROFILE}" == "release" ]]; then
  TARGET_DIR="release"
  cargo build --release -p slatedb-ffi
else
  TARGET_DIR="debug"
  cargo build -p slatedb-ffi
fi

LIB_FILE="${REPO_ROOT}/target/${TARGET_DIR}/libslatedb_ffi.${LIB_EXT}"
STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-java-gen.XXXXXX")"
TARGET_DIR_JAVA="${SCRIPT_DIR}/src/main/java/io/slatedb"
trap 'rm -rf "${STAGING_DIR}"' EXIT

"${UNIFFI_BINDGEN_JAVA}" \
  generate "${LIB_FILE}" \
  --library \
  --crate slatedb_ffi \
  --config "${SCRIPT_DIR}/uniffi.toml" \
  --out-dir "${STAGING_DIR}"

mkdir -p "${TARGET_DIR_JAVA}"
find "${TARGET_DIR_JAVA}" -maxdepth 1 -type f -name '*.java' -delete 2>/dev/null || true
cp "${STAGING_DIR}/io/slatedb/"*.java "${TARGET_DIR_JAVA}/"

# UniFFI-generated Java objects already include an AutoCloseable `close()` used
# for handle cleanup. For the object types that also expose an explicit FFI
# `close()` method, that generated cleanup method collides with the API method.
# Rename the cleanup method to `destroy()` in those classes so the exported
# close surface remains available.
for file in \
  "${TARGET_DIR_JAVA}/DbReader.java" \
  "${TARGET_DIR_JAVA}/WalFile.java" \
  "${TARGET_DIR_JAVA}/WalFileIterator.java" \
  "${TARGET_DIR_JAVA}/WalReader.java" \
  "${TARGET_DIR_JAVA}/WriteBatch.java"; do
  perl -0pi -e 's/\n\s*\@Override\n\s*public synchronized void close\(\) \{\n/\n  public synchronized void destroy() {\n/s' "${file}"
done

# `DbReader.close()` is async in the UniFFI surface, so the generated class
# cannot also implement `AutoCloseable.close()`.
perl -0pi -e 's/implements AutoCloseable, DbReaderInterface/implements DbReaderInterface/' \
  "${TARGET_DIR_JAVA}/DbReader.java"
