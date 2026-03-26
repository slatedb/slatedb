#!/usr/bin/env bash
set -euo pipefail

platform="$(uname -s)"
case "${platform}" in
  Darwin)
    node_platform="darwin"
    ;;
  Linux)
    node_platform="linux"
    ;;
  CYGWIN*|MINGW*|MSYS*)
    node_platform="win32"
    ;;
  *)
    echo "unsupported platform for Node prebuild detection: ${platform}" >&2
    exit 1
    ;;
esac

arch="$(uname -m)"
case "${arch}" in
  x86_64|amd64)
    node_arch="x64"
    ;;
  i386|i686)
    node_arch="ia32"
    ;;
  arm64|aarch64)
    node_arch="arm64"
    ;;
  armv7l|armv8l|arm)
    node_arch="arm"
    ;;
  loongarch64)
    node_arch="loong64"
    ;;
  ppc64|ppc64le)
    node_arch="ppc64"
    ;;
  riscv64)
    node_arch="riscv64"
    ;;
  s390x)
    node_arch="s390x"
    ;;
  *)
    echo "unsupported architecture for Node prebuild detection: ${arch}" >&2
    exit 1
    ;;
esac

if [[ "${node_platform}" != "linux" ]]; then
  echo "${node_platform}-${node_arch}"
  exit 0
fi

if getconf GNU_LIBC_VERSION >/dev/null 2>&1; then
  libc_suffix="gnu"
elif ldd --version 2>&1 | grep -qi "musl"; then
  libc_suffix="musl"
else
  echo "unable to determine Linux libc for Node prebuild detection" >&2
  exit 1
fi

echo "${node_platform}-${node_arch}-${libc_suffix}"
