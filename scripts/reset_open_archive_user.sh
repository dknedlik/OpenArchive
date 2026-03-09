#!/usr/bin/env bash
set -euo pipefail

SCHEMA_USERNAME="${SCHEMA_USERNAME:-open_archive_user}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_PATH="${SCRIPT_DIR}/../sql/admin/reset_open_archive_bootstrap.sql"

command -v sql >/dev/null 2>&1 || { echo "Missing required command: sql" >&2; exit 1; }
command -v oci >/dev/null 2>&1 || { echo "Missing required command: oci" >&2; exit 1; }
command -v base64 >/dev/null 2>&1 || { echo "Missing required command: base64" >&2; exit 1; }

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

SCHEMA_USERNAME_SQL="$(printf "%s" "${SCHEMA_USERNAME}" | sed "s/'/''/g")"

set +x

sql -s /nolog <<SQL
whenever sqlerror exit failure rollback
connect ADMIN/"${ADMIN_PASSWORD}"@${TNS_ALIAS}
set define off
var schema_username varchar2(128)
exec :schema_username := '${SCHEMA_USERNAME_SQL}';
set define on
@${SQL_PATH}
exit
SQL
