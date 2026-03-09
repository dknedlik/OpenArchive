use crate::error::StorageResult;

use crate::storage::types::NewEnrichmentJob;
use crate::storage::StorageTx;

/// Stores asynchronous enrichment jobs.
pub trait EnrichmentJobStore {
    type Tx: StorageTx;

    fn insert_job(&self, tx: &mut Self::Tx, job: &NewEnrichmentJob) -> StorageResult<()>;
}
