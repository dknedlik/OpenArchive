#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCAL_BIN="${REPO_ROOT}/target/debug/open_archive"

if [[ -x "${LOCAL_BIN}" ]]; then
  exec "${LOCAL_BIN}" install-qdrant "$@"
fi

if command -v cargo >/dev/null 2>&1; then
  exec cargo run --bin open_archive -- install-qdrant "$@"
fi

echo "Missing OpenArchive binary and cargo. Build OpenArchive first, then rerun." >&2
exit 1
