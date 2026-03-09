#!/usr/bin/env bash
set -euo pipefail

export SCHEMA_USERNAME="open_archive_test_user"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/reset_open_archive_user.sh"
