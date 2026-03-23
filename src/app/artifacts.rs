use std::sync::Arc;

use crate::error::StorageResult;
use crate::storage::{ArtifactListItem, ArtifactReadStore};

pub struct ArtifactQueryService {
    read_store: Arc<dyn ArtifactReadStore + Send + Sync>,
}

impl ArtifactQueryService {
    pub fn new(read_store: Arc<dyn ArtifactReadStore + Send + Sync>) -> Self {
        Self { read_store }
    }

    pub fn list_artifacts(&self) -> StorageResult<Vec<ArtifactListItem>> {
        self.read_store.list_artifacts()
    }

    pub fn list_artifacts_filtered(
        &self,
        filters: &crate::storage::types::ArtifactListFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<ArtifactListItem>> {
        let limit = limit.clamp(1, 100);
        self.read_store
            .list_artifacts_filtered(filters, limit, offset)
    }

    pub fn get_timeline(
        &self,
        filters: &crate::storage::types::TimelineFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::TimelineEntry>> {
        let limit = limit.clamp(1, 100);
        self.read_store.get_timeline(filters, limit, offset)
    }
}
