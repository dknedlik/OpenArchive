# OpenArchive ADB Bootstrap

This project uses a CLI-first bootstrap path for Oracle ADB instead of Terraform.

The goal is to keep one-time database setup separate from application runtime:

- OCI CLI creates or retrieves the Vault secret that holds the schema password
- SQLcl, connected as `ADMIN`, creates the `open_archive_user` schema user
- the Rust CLI applies application migrations as `open_archive_user`

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

## 1. Create a Vault secret for `open_archive_user`

Generate a password locally and create a secret in the current vault:

```bash
export OPEN_ARCHIVE_PASSWORD="$(openssl rand -base64 36 | tr -d '\n')"

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
```

## 2. Bootstrap the schema user

Create the schema user as `ADMIN` with the one-time bootstrap script:

```bash
export OPEN_ARCHIVE_PASSWORD_SECRET_ID=...
export DB_ADMIN_PASSWORD_SECRET_ID=...
export WALLET_DIR=/Users/david/.clean-engine/wallet
export TNS_ALIAS=cleanengine_medium

/Users/david/src/open_archive/scripts/bootstrap_open_archive_user.sh
```

This script:

- fetches the `ADMIN` password from Vault
- fetches the `open_archive_user` password from Vault or `OPEN_ARCHIVE_PASSWORD`
- connects to ADB via wallet-backed SQLcl
- creates `open_archive_user` if missing
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

## Reset

To remove the schema user during early iteration:

```bash
/Users/david/src/open_archive/scripts/run_sqlcl_admin.sh \
  /Users/david/src/open_archive/sql/admin/reset_open_archive_bootstrap.sql
```

## Notes

- `open_archive_user` is intentionally separate from `ADMIN`
- user bootstrap is one-time database setup, not part of app startup
- application migrations run as the schema user, not as `ADMIN`
- migration SQL lives in `/Users/david/src/open_archive/sql/migrations`
