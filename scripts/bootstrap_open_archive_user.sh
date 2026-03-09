#!/usr/bin/env bash
set -euo pipefail

SCHEMA_USERNAME="${SCHEMA_USERNAME:-open_archive_user}"
SCHEMA_PASSWORD_VAR_NAME="${SCHEMA_PASSWORD_VAR_NAME:-OPEN_ARCHIVE_PASSWORD}"
SCHEMA_PASSWORD_SECRET_ID_VAR_NAME="${SCHEMA_PASSWORD_SECRET_ID_VAR_NAME:-OPEN_ARCHIVE_PASSWORD_SECRET_ID}"

SCHEMA_PASSWORD="${!SCHEMA_PASSWORD_VAR_NAME:-}"
SCHEMA_PASSWORD_SECRET_ID="${!SCHEMA_PASSWORD_SECRET_ID_VAR_NAME:-}"

if [[ -z "${SCHEMA_PASSWORD}" && -z "${SCHEMA_PASSWORD_SECRET_ID}" ]]; then
  echo "Set ${SCHEMA_PASSWORD_VAR_NAME} or ${SCHEMA_PASSWORD_SECRET_ID_VAR_NAME}." >&2
  exit 1
fi

if [[ -n "${SCHEMA_PASSWORD_SECRET_ID}" ]]; then
  command -v oci >/dev/null 2>&1 || { echo "Missing required command: oci" >&2; exit 1; }
  command -v base64 >/dev/null 2>&1 || { echo "Missing required command: base64" >&2; exit 1; }
  SCHEMA_PASSWORD="$(
    oci secrets secret-bundle get \
      --secret-id "${SCHEMA_PASSWORD_SECRET_ID}" \
      --query 'data."secret-bundle-content".content' \
      --raw-output 2>/dev/null | base64 --decode
  )"
fi

if [[ -z "${SCHEMA_PASSWORD}" ]]; then
  echo "Resolved ${SCHEMA_PASSWORD_VAR_NAME} is empty." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_PATH="${SCRIPT_DIR}/../sql/admin/bootstrap_open_archive_user.sql"
command -v sql >/dev/null 2>&1 || { echo "Missing required command: sql" >&2; exit 1; }

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

SCHEMA_USERNAME_SQL="$(printf "%s" "${SCHEMA_USERNAME}" | sed "s/'/''/g")"
SCHEMA_PASSWORD_SQL="$(printf "%s" "${SCHEMA_PASSWORD}" | sed "s/'/''/g")"

set +x

sql -s /nolog <<SQL
whenever sqlerror exit failure rollback
connect ADMIN/"${ADMIN_PASSWORD}"@${TNS_ALIAS}
set define off
var schema_username varchar2(128)
var schema_password varchar2(512)
exec :schema_username := '${SCHEMA_USERNAME_SQL}';
exec :schema_password := '${SCHEMA_PASSWORD_SQL}';
set define on
@${SQL_PATH}
exit
SQL
