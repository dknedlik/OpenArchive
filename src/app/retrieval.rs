use std::sync::Arc;

use crate::error::{OpenArchiveError, Result};
use crate::storage::{ArchiveRetrievalStore, RetrievalIntent, RetrievedContextItem};

pub trait ArchiveRetrievalServiceApi: Send + Sync {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> Result<Vec<RetrievedContextItem>>;
}

pub struct ArchiveRetrievalService {
    store: Arc<dyn ArchiveRetrievalStore + Send + Sync>,
}

impl ArchiveRetrievalService {
    pub fn new(store: Arc<dyn ArchiveRetrievalStore + Send + Sync>) -> Self {
        Self { store }
    }
}

impl ArchiveRetrievalServiceApi for ArchiveRetrievalService {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> Result<Vec<RetrievedContextItem>> {
        self.store
            .retrieve_for_intents(artifact_id, intents, limit_per_intent)
            .map_err(OpenArchiveError::from)
    }
}
