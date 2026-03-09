#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${1:-}"
if [[ -z "${SCRIPT_PATH}" ]]; then
  echo "Usage: $0 path/to/script.sql [sqlcl-arg ...]" >&2
  exit 2
fi

if [[ ! -f "${SCRIPT_PATH}" ]]; then
  echo "Script not found: ${SCRIPT_PATH}" >&2
  exit 2
fi

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

require oci
require sql
require base64

: "${WALLET_DIR:?WALLET_DIR must be set}"
export TNS_ADMIN="${WALLET_DIR}"
TNS_ALIAS="${TNS_ALIAS:-cleanengine_medium}"

DB_ADMIN_PASSWORD_SECRET_ID_EFFECTIVE="${DB_ADMIN_PASSWORD_SECRET_ID:-${DB_PASSWORD_SECRET_ID:-}}"
if [[ -z "${DB_ADMIN_PASSWORD_SECRET_ID_EFFECTIVE}" ]]; then
  echo "Missing ADMIN password secret id. Set DB_ADMIN_PASSWORD_SECRET_ID or DB_PASSWORD_SECRET_ID." >&2
  exit 1
fi

ADMIN_PASSWORD="$(
  oci secrets secret-bundle get \
    --secret-id "${DB_ADMIN_PASSWORD_SECRET_ID_EFFECTIVE}" \
    --query 'data."secret-bundle-content".content' \
    --raw-output 2>/dev/null | base64 --decode
)"

if [[ -z "${ADMIN_PASSWORD}" ]]; then
  echo "Fetched ADMIN password is empty" >&2
  exit 1
fi

set +x

shift
sql -s /nolog <<SQL
whenever sqlerror exit failure rollback
connect ADMIN/"${ADMIN_PASSWORD}"@${TNS_ALIAS}
@${SCRIPT_PATH}
exit
SQL
