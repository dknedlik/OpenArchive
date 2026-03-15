use crate::error::StorageResult;
use crate::storage::types::{RetrievedContextItem, RetrievalIntent};

pub trait ArchiveRetrievalStore: Send + Sync {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>>;
}
