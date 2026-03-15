use crate::error::StorageResult;
use crate::storage::types::{
    ArtifactExtractionResult, ReconciliationDecision, RetrievalResultSet,
};

pub trait EnrichmentStateStore: Send + Sync {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()>;

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>>;

    fn save_retrieval_result_set(&self, result_set: &RetrievalResultSet) -> StorageResult<()>;

    fn load_retrieval_result_set(
        &self,
        retrieval_result_set_id: &str,
    ) -> StorageResult<Option<RetrievalResultSet>>;

    fn save_reconciliation_decisions(
        &self,
        decisions: &[ReconciliationDecision],
    ) -> StorageResult<()>;

    fn load_reconciliation_decisions(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Vec<ReconciliationDecision>>;
}
