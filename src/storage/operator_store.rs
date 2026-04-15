use crate::error::StorageResult;
use crate::storage::types::ArchiveStatusSnapshot;

/// Read model for operator-facing archive status summaries.
pub trait OperatorStore: Send + Sync {
    fn load_archive_status(&self) -> StorageResult<ArchiveStatusSnapshot>;
}
