use crate::error::StorageResult;

use crate::storage::types::{ImportStatus, NewImport, NewImportObjectRef};
use crate::storage::StorageTx;

/// Stores the relational metadata for a copied raw payload object.
///
/// `tx` must be the same transaction used for the subsequent `insert_import`
/// call so the object reference row and import row land in the same commit.
pub trait ImportPayloadStore {
    type Tx: StorageTx;

    fn insert_payload(&self, tx: &mut Self::Tx, payload: &NewImportObjectRef) -> StorageResult<()>;
}

/// Manages the lifecycle of one import request.
pub trait ImportStore {
    type Tx: StorageTx;

    fn insert_import(&self, tx: &mut Self::Tx, import: &NewImport) -> StorageResult<()>;

    /// Update per-conversation counters at the end of the parse phase.
    fn update_import_counts(
        &self,
        tx: &mut Self::Tx,
        import_id: &str,
        count_imported: i64,
        count_failed: i64,
    ) -> StorageResult<()>;

    /// Transition the import to its terminal status and record any top-level error.
    fn complete_import(
        &self,
        tx: &mut Self::Tx,
        import_id: &str,
        status: ImportStatus,
        error_message: Option<&str>,
    ) -> StorageResult<()>;
}
