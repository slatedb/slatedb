#!/usr/bin/env bash
set -euo pipefail

# Check FlatBuffers schema evolution using `flatc --conform`.
#
# The script resolves a base revision (using the rules below), materializes the
# base copy of all schema files (schemas/*.fbs) into a temporary directory via
# `git show`, and then runs flatc conformance checks on the current schemas. It
# validates `root.fbs` and `manifest.fbs` when present in both the base and the
# working tree—`root.fbs` pulls in other files through includes, so most changes
# are covered. Files that didn’t exist at the base are skipped. If no base
# schemas can be found, the script exits successfully (nothing to compare). Any
# conformance error returned by flatc causes the script to fail.
#
# How the base is chosen (env vars):
# - `FLATBUFFERS_BASE_REF` (optional):
#   If set, this ref is used as the base for comparison. It can be a commit
#   hash, tag, or branch name (e.g., "origin/main"). If the ref is a remote
#   branch name, the script attempts to fetch it. This is the most explicit way
#   to control the base when running locally.
#
# - `GITHUB_BASE_REF` (CI: pull_request only):
#   On GitHub Actions pull_request workflows, GitHub sets `GITHUB_BASE_REF` to the
#   base branch name of the PR (e.g., "main"). When present, the script fetches
#   that branch and uses its HEAD as the base.
#   Note: The workflow should checkout with `fetch-depth: 0` to ensure history
#   is available, though this script also fetches the base branch explicitly.
#
# - Fallback when neither is set (local runs, push workflows):
#   The previous commit (HEAD^) is used as the base. This makes local testing
#   convenient when you’ve made a single commit and want to compare it against
#   its parent.
#
# If flatc finds an incompatible schema change, it exits non‑zero and this
# script fails.

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
SCHEMAS_DIR="$ROOT_DIR/schemas"
TMP_DIR="$ROOT_DIR/tmp/flatbuffers_base"
BASE_SCHEMAS_DIR="$TMP_DIR/schemas"

mkdir -p "$BASE_SCHEMAS_DIR"

echo "[flatbuffers-evolution] Detecting base ref..."

BASE_COMMIT=""
if [[ -n "${FLATBUFFERS_BASE_REF:-}" ]]; then
  # Explicit base ref from environment (commit hash, tag, or ref)
  echo "[flatbuffers-evolution] Using FLATBUFFERS_BASE_REF=$FLATBUFFERS_BASE_REF"
  # Try to resolve to a commit; fetch if it's a remote branch name
  if git rev-parse --verify -q "$FLATBUFFERS_BASE_REF^{commit}" >/dev/null; then
    BASE_COMMIT=$(git rev-parse "$FLATBUFFERS_BASE_REF^{commit}")
  else
    echo "[flatbuffers-evolution] Fetching base ref '$FLATBUFFERS_BASE_REF' from origin..."
    git fetch --no-tags --depth=1 origin "+refs/heads/${FLATBUFFERS_BASE_REF}:refs/remotes/origin/${FLATBUFFERS_BASE_REF}" || git fetch --no-tags --depth=1 origin "$FLATBUFFERS_BASE_REF"
    BASE_COMMIT=$(git rev-parse FETCH_HEAD)
  fi
elif [[ -n "${GITHUB_BASE_REF:-}" ]]; then
  # On PRs, GitHub sets GITHUB_BASE_REF to the base branch name (e.g., main)
  echo "[flatbuffers-evolution] PR base branch: $GITHUB_BASE_REF"
  echo "[flatbuffers-evolution] Fetching base branch..."
  # Try origin first (works for same-repo PRs), otherwise fetch from upstream repo (works for forks)
  if git ls-remote --exit-code origin "refs/heads/${GITHUB_BASE_REF}" >/dev/null 2>&1; then
    git fetch --no-tags --depth=1 origin "+refs/heads/${GITHUB_BASE_REF}:refs/remotes/origin/${GITHUB_BASE_REF}" || git fetch --no-tags --depth=1 origin "$GITHUB_BASE_REF"
  else
    if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
      git fetch --no-tags --depth=1 "https://github.com/${GITHUB_REPOSITORY}.git" \
        "+refs/heads/${GITHUB_BASE_REF}:refs/remotes/upstream/${GITHUB_BASE_REF}" || \
      git fetch --no-tags --depth=1 "https://github.com/${GITHUB_REPOSITORY}.git" "${GITHUB_BASE_REF}"
    else
      # Fallback to origin if GITHUB_REPOSITORY is not available
      git fetch --no-tags --depth=1 origin "+refs/heads/${GITHUB_BASE_REF}:refs/remotes/origin/${GITHUB_BASE_REF}" || git fetch --no-tags --depth=1 origin "$GITHUB_BASE_REF"
    fi
  fi
  BASE_COMMIT=$(git rev-parse FETCH_HEAD)
else
  # Fallback: previous commit on current branch
  BASE_COMMIT=$(git rev-parse HEAD^)
fi

echo "[flatbuffers-evolution] Base commit: $BASE_COMMIT"

# Helper to check existence of a file at a given commit
have_file_at_commit() {
  local commit="$1"; shift
  local path="$1"; shift
  git cat-file -e "${commit}:${path}" 2>/dev/null
}

# Extract base schema files
echo "[flatbuffers-evolution] Extracting base schemas..."
BASE_FOUND_ANY=false
for path in "$SCHEMAS_DIR"/*.fbs; do
  f=$(basename "$path")
  rel_path="schemas/${f}"
  if have_file_at_commit "$BASE_COMMIT" "$rel_path"; then
    git show "${BASE_COMMIT}:${rel_path}" >"${BASE_SCHEMAS_DIR}/${f}"
    BASE_FOUND_ANY=true
  else
    echo "[flatbuffers-evolution] Skipping ${rel_path} (did not exist at base)"
  fi
done

if [[ "$BASE_FOUND_ANY" == "false" ]]; then
  echo "[flatbuffers-evolution] No base schema files found; nothing to check."
  exit 0
fi

echo "[flatbuffers-evolution] Running flatc --conform checks..."

# Files to check for conformance. root.fbs should cover includes; also check manifest.fbs directly.
FILES_TO_CHECK=("root.fbs" "manifest.fbs")

for f in "${FILES_TO_CHECK[@]}"; do
  if [[ -f "${SCHEMAS_DIR}/${f}" ]] && [[ -f "${BASE_SCHEMAS_DIR}/${f}" ]]; then
    echo "[flatbuffers-evolution] Validating ${f} against base..."
    flatc \
      --conform "${BASE_SCHEMAS_DIR}/${f}" \
      --conform-includes "${BASE_SCHEMAS_DIR}" \
      -I "${SCHEMAS_DIR}" \
      "${SCHEMAS_DIR}/${f}"
  fi
done

echo "[flatbuffers-evolution] OK: schemas conform to base."
