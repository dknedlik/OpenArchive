use crate::config::PostgresConfig;
use crate::error::{DbError, StorageError, StorageResult};
use crate::postgres_db;
use crate::storage::StorageTx;

pub(in crate::storage::postgres) struct PostgresStorageTx {
    pub(in crate::storage::postgres) client: postgres::Client,
    connection_string: String,
}

impl StorageTx for PostgresStorageTx {
    fn commit(&mut self) -> StorageResult<()> {
        commit_transaction(&mut self.client, &self.connection_string)
    }
}

pub(in crate::storage::postgres) fn begin_storage_tx(
    config: &PostgresConfig,
) -> StorageResult<PostgresStorageTx> {
    let mut client = postgres_db::connect(config)?;
    begin_transaction(&mut client, &config.connection_string)?;
    Ok(PostgresStorageTx {
        client,
        connection_string: config.connection_string.clone(),
    })
}

pub(in crate::storage::postgres) fn begin_transaction(
    client: &mut postgres::Client,
    connection_string: &str,
) -> StorageResult<()> {
    client
        .batch_execute("BEGIN")
        .map_err(|source| storage_db_error(connection_string, source))
}

pub(in crate::storage::postgres) fn commit_transaction(
    client: &mut postgres::Client,
    connection_string: &str,
) -> StorageResult<()> {
    client
        .batch_execute("COMMIT")
        .map_err(|source| storage_db_error(connection_string, source))
}

pub(in crate::storage::postgres) fn rollback_transaction(
    client: &mut postgres::Client,
    connection_string: &str,
) -> StorageResult<()> {
    client
        .batch_execute("ROLLBACK")
        .map_err(|source| storage_db_error(connection_string, source))
}

pub(in crate::storage::postgres) fn storage_db_error(
    connection_string: &str,
    source: postgres::Error,
) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source,
    })
}
