pub mod artifact_detail;
pub mod artifacts;
pub mod context_pack;
pub mod imports;
pub mod search;

use std::sync::Arc;

use crate::embeddings::TextEmbedder;
use crate::object_store::ObjectStore;
use crate::storage::{
    ArchiveSearchReadStore, ArtifactContextPackReadStore, ArtifactDetailReadStore,
    ArtifactReadStore, ImportWriteStore,
};

pub struct ArchiveApplication {
    pub artifacts: artifacts::ArtifactQueryService,
    pub imports: imports::ImportApplicationService,
    pub search: Option<search::ArchiveSearchService>,
    pub artifact_detail: Option<artifact_detail::ArtifactDetailService>,
    pub context_pack: Option<context_pack::ContextPackService>,
}

impl ArchiveApplication {
    pub fn new(
        import_store: Arc<dyn ImportWriteStore + Send + Sync>,
        read_store: Arc<dyn ArtifactReadStore + Send + Sync>,
        search_read_store: Option<Arc<dyn ArchiveSearchReadStore + Send + Sync>>,
        artifact_detail_store: Option<Arc<dyn ArtifactDetailReadStore + Send + Sync>>,
        context_pack_store: Option<Arc<dyn ArtifactContextPackReadStore + Send + Sync>>,
        embedder: Option<Arc<dyn TextEmbedder>>,
        object_store: Arc<dyn ObjectStore + Send + Sync>,
    ) -> Self {
        Self {
            artifacts: artifacts::ArtifactQueryService::new(read_store),
            imports: imports::ImportApplicationService::new(import_store, object_store),
            search: search_read_store.map(search::ArchiveSearchService::new),
            artifact_detail: artifact_detail_store.map(artifact_detail::ArtifactDetailService::new),
            context_pack: context_pack_store
                .map(|store| context_pack::ContextPackService::new(store, embedder)),
        }
    }
}
