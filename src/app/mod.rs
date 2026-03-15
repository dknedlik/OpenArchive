pub mod artifacts;
pub mod imports;

use std::sync::Arc;

use crate::object_store::ObjectStore;
use crate::storage::{ArtifactReadStore, ImportWriteStore};

pub struct ArchiveApplication {
    pub artifacts: artifacts::ArtifactQueryService,
    pub imports: imports::ImportApplicationService,
}

impl ArchiveApplication {
    pub fn new(
        import_store: Arc<dyn ImportWriteStore + Send + Sync>,
        read_store: Arc<dyn ArtifactReadStore + Send + Sync>,
        object_store: Arc<dyn ObjectStore + Send + Sync>,
    ) -> Self {
        Self {
            artifacts: artifacts::ArtifactQueryService::new(read_store),
            imports: imports::ImportApplicationService::new(import_store, object_store),
        }
    }
}
