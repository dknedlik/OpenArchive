use crate::db;
use crate::error::{StorageError, StorageResult};
use crate::storage::StorageTx;

pub(in crate::storage::oracle) struct OracleStorageTx {
    pub(in crate::storage::oracle) conn: oracle::Connection,
}

impl StorageTx for OracleStorageTx {
    fn commit(&mut self) -> StorageResult<()> {
        commit_connection(&self.conn, "storage transaction")
    }
}

pub(in crate::storage::oracle) fn begin_storage_tx(
    config: &crate::config::OracleConfig,
) -> StorageResult<OracleStorageTx> {
    Ok(OracleStorageTx {
        conn: db::connect(config)?,
    })
}

pub(in crate::storage::oracle) fn commit_connection(
    conn: &oracle::Connection,
    operation: &'static str,
) -> StorageResult<()> {
    conn.commit()
        .map_err(|source| StorageError::Commit { operation, source })
}

pub(in crate::storage::oracle) fn rollback_connection(
    conn: &oracle::Connection,
    operation: impl Into<String>,
) -> StorageResult<()> {
    conn.rollback().map_err(|source| StorageError::Rollback {
        operation: operation.into(),
        source,
    })
}
