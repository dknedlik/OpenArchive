#!/usr/bin/env bash
set -euo pipefail

export SCHEMA_USERNAME="open_archive_test_user"
export SCHEMA_PASSWORD_VAR_NAME="OPEN_ARCHIVE_TEST_PASSWORD"
export SCHEMA_PASSWORD_SECRET_ID_VAR_NAME="OPEN_ARCHIVE_TEST_PASSWORD_SECRET_ID"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/bootstrap_open_archive_user.sh"
