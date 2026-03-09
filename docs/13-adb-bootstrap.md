# OpenArchive ADB Bootstrap

This project uses a CLI-first bootstrap path for Oracle ADB instead of Terraform.

The goal is to keep one-time database setup separate from application runtime:

- OCI CLI creates or retrieves Vault secrets that hold schema passwords
- SQLcl, connected as `ADMIN`, creates schema users
- the Rust CLI applies application migrations as `open_archive_user`
- ADB integration tests run against `open_archive_test_user`

## Prerequisites

- OCI CLI installed and authenticated locally
- SQLcl installed locally as `sql`
- Oracle Instant Client installed locally
- local ADB wallet available at `WALLET_DIR`
- access to the current OCI compartment, vault, and ADB instance

For this machine, the proven local pieces already exist:

- wallet: `/Users/david/.clean-engine/wallet`
- Instant Client: `/Users/david/Downloads/instantclient_23_3`

## Environment

At minimum, set:

```bash
export OCI_REGION=us-phoenix-1
export WALLET_DIR=/Users/david/.clean-engine/wallet
export TNS_ALIAS=cleanengine_medium
export TNS_ADMIN="$WALLET_DIR"
export DYLD_LIBRARY_PATH="/Users/david/Downloads/instantclient_23_3:$DYLD_LIBRARY_PATH"
```

For the existing ADB instance reused from the adjacent project, you also need:

```bash
export DB_ADMIN_PASSWORD_SECRET_ID=...
export OCI_COMPARTMENT_ID=...
export OCI_VAULT_ID=...
export OCI_VAULT_KEY_ID=...
```

If you are reusing the current local OCI project state from `../fitness-tracker`, these
values can be pulled from Terraform outputs instead of being copied manually:

```bash
export OCI_REGION=us-phoenix-1
export WALLET_DIR=/Users/david/.clean-engine/wallet
export TNS_ALIAS=cleanengine_medium
export TNS_ADMIN="$WALLET_DIR"
export DYLD_LIBRARY_PATH="/Users/david/Downloads/instantclient_23_3:$DYLD_LIBRARY_PATH"

export DB_ADMIN_PASSWORD_SECRET_ID="$(
  cd /Users/david/src/fitness-tracker/terraform &&
  terraform output -json application_config | jq -r '.db_admin_password_secret_id'
)"
export OCI_COMPARTMENT_ID="$(
  cd /Users/david/src/fitness-tracker/terraform &&
  terraform output -json application_config | jq -r '.compartment_id'
)"
export OCI_VAULT_ID="$(
  cd /Users/david/src/fitness-tracker/terraform &&
  terraform output -raw vault_id
)"
export OCI_VAULT_KEY_ID="$(
  cd /Users/david/src/fitness-tracker/terraform &&
  terraform output -raw vault_master_key_id
)"
```

## 1. Create Vault secrets for schema users

Generate passwords locally and create secrets in the current vault:

```bash
export OPEN_ARCHIVE_PASSWORD="$(openssl rand -base64 36 | tr -d '\n')"
export OPEN_ARCHIVE_TEST_PASSWORD="$(openssl rand -base64 36 | tr -d '\n')"

export OPEN_ARCHIVE_PASSWORD_SECRET_ID="$(
  oci vault secret create-base64 \
    --compartment-id "$OCI_COMPARTMENT_ID" \
    --vault-id "$OCI_VAULT_ID" \
    --key-id "$OCI_VAULT_KEY_ID" \
    --secret-name "open-archive-db-password" \
    --description "OpenArchive schema password" \
    --secret-content-content "$(printf '%s' "$OPEN_ARCHIVE_PASSWORD" | base64)" \
    --wait-for-state ACTIVE \
    --query 'data.id' \
    --raw-output
)"

export OPEN_ARCHIVE_TEST_PASSWORD_SECRET_ID="$(
  oci vault secret create-base64 \
    --compartment-id "$OCI_COMPARTMENT_ID" \
    --vault-id "$OCI_VAULT_ID" \
    --key-id "$OCI_VAULT_KEY_ID" \
    --secret-name "open-archive-test-db-password" \
    --description "OpenArchive test schema password" \
    --secret-content-content "$(printf '%s' "$OPEN_ARCHIVE_TEST_PASSWORD" | base64)" \
    --wait-for-state ACTIVE \
    --query 'data.id' \
    --raw-output
)"
```

If the secret already exists, fetch it instead:

```bash
export OPEN_ARCHIVE_PASSWORD_SECRET_ID="$(
  oci vault secret list \
    --compartment-id "$OCI_COMPARTMENT_ID" \
    --vault-id "$OCI_VAULT_ID" \
    --name "open-archive-db-password" \
    --all \
    --query 'data[0].id' \
    --raw-output
)"

export OPEN_ARCHIVE_TEST_PASSWORD_SECRET_ID="$(
  oci vault secret list \
    --compartment-id "$OCI_COMPARTMENT_ID" \
    --vault-id "$OCI_VAULT_ID" \
    --name "open-archive-test-db-password" \
    --all \
    --query 'data[0].id' \
    --raw-output
)"
```

## 2. Bootstrap the schema users

Create both schema users as `ADMIN` with the one-time bootstrap scripts:

```bash
export OPEN_ARCHIVE_PASSWORD_SECRET_ID=...
export OPEN_ARCHIVE_TEST_PASSWORD_SECRET_ID=...
export DB_ADMIN_PASSWORD_SECRET_ID=...
export WALLET_DIR=/Users/david/.clean-engine/wallet
export TNS_ALIAS=cleanengine_medium

/Users/david/src/open_archive/scripts/bootstrap_open_archive_user.sh
/Users/david/src/open_archive/scripts/bootstrap_open_archive_test_user.sh
```

These scripts:

- fetches the `ADMIN` password from Vault
- fetches the schema password from Vault or an env var
- connects to ADB via wallet-backed SQLcl
- creates the schema user if missing
- grants the minimal schema-owner privileges needed for slice one

## 3. Run Rust migrations as `open_archive_user`

After bootstrap, fetch the application user password and run the Rust CLI:

```bash
export DB_USERNAME=open_archive_user
export DB_PASSWORD="$(
  oci secrets secret-bundle get \
    --secret-id "$OPEN_ARCHIVE_PASSWORD_SECRET_ID" \
    --query 'data."secret-bundle-content".content' \
    --raw-output | base64 --decode
)"

cd /Users/david/src/open_archive
cargo run -- adb-check
cargo run -- migrate-check
cargo run -- migrate
```

## 4. Run integration tests as `open_archive_test_user`

Use the dedicated test schema for live ADB-backed integration tests. The
integration harness now resets the `oa_*` schema objects, runs Rust migrations
once at startup, and resets the schema again on process exit. It refuses to do
that unless you explicitly opt in and point it at the expected test-schema
username.

```bash
export OA_INTEGRATION_TESTS=1
export OA_ALLOW_SCHEMA_RESET=1
export OA_TEST_DB_USERNAME=open_archive_test_user
export OA_TEST_SCHEMA_USERNAME=open_archive_test_user
export OA_TEST_DB_PASSWORD="$(
  oci secrets secret-bundle get \
    --secret-id "$OPEN_ARCHIVE_TEST_PASSWORD_SECRET_ID" \
    --query 'data."secret-bundle-content".content' \
    --raw-output | base64 --decode
)"
export OA_TEST_TNS_ALIAS=cleanengine_medium
export OA_TEST_WALLET_DIR=/Users/david/.clean-engine/wallet
export TNS_ADMIN=/Users/david/.clean-engine/wallet
export DYLD_LIBRARY_PATH="/Users/david/Downloads/instantclient_23_3:${DYLD_LIBRARY_PATH:-}"

cd /Users/david/src/open_archive
cargo test --test oracle_import_write -- --ignored --nocapture --test-threads=1
```

If you need to verify the test schema is reachable before running the tests,
this should return `no pending migrations` when the credentials are correct:

```bash
export DB_USERNAME=open_archive_test_user
export DB_PASSWORD="$OA_TEST_DB_PASSWORD"
export WALLET_DIR=/Users/david/.clean-engine/wallet
export TNS_ALIAS=cleanengine_medium

cd /Users/david/src/open_archive
cargo run -- migrate
```

## Reset

To remove a schema user during early iteration:

```bash
/Users/david/src/open_archive/scripts/reset_open_archive_user.sh
/Users/david/src/open_archive/scripts/reset_open_archive_test_user.sh
```

## Notes

- `open_archive_user` is intentionally separate from `ADMIN`
- `open_archive_test_user` is intentionally separate from both `ADMIN` and `open_archive_user`
- user bootstrap is one-time database setup, not part of app startup
- application migrations run as the schema user, not as `ADMIN`
- live integration tests should use the test schema, not the main dev schema
- migration SQL lives in `/Users/david/src/open_archive/sql/migrations`
