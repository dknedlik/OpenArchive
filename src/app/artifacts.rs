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
}
