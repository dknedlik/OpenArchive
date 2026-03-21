pub mod artifact_detail;
pub mod artifacts;
pub mod context_pack;
pub mod imports;
pub mod retrieval;
pub mod search;
pub mod writeback;

use std::sync::Arc;

use crate::object_store::ObjectStore;
use crate::storage::{
    ArchiveRetrievalStore, ArchiveSearchReadStore, ArtifactContextPackReadStore,
    ArtifactDetailReadStore, ArtifactReadStore, CrossArtifactReadStore, DerivedObjectSearchStore,
    ImportWriteStore, WritebackStore,
};

pub struct ArchiveApplication {
    pub artifacts: artifacts::ArtifactQueryService,
    pub imports: imports::ImportApplicationService,
    pub retrieval: Arc<dyn retrieval::ArchiveRetrievalServiceApi>,
    pub search: Option<search::ArchiveSearchService>,
    pub artifact_detail: Option<artifact_detail::ArtifactDetailService>,
    pub context_pack: Option<context_pack::ContextPackService>,
    pub object_search: Option<search::ObjectSearchService>,
    pub writeback: Option<writeback::WritebackService>,
}

impl ArchiveApplication {
    pub fn new(
        import_store: Arc<dyn ImportWriteStore + Send + Sync>,
        read_store: Arc<dyn ArtifactReadStore + Send + Sync>,
        retrieval_store: Arc<dyn ArchiveRetrievalStore + Send + Sync>,
        search_read_store: Option<Arc<dyn ArchiveSearchReadStore + Send + Sync>>,
        artifact_detail_store: Option<Arc<dyn ArtifactDetailReadStore + Send + Sync>>,
        context_pack_store: Option<Arc<dyn ArtifactContextPackReadStore + Send + Sync>>,
        cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
        object_search_store: Option<Arc<dyn DerivedObjectSearchStore + Send + Sync>>,
        object_store: Arc<dyn ObjectStore + Send + Sync>,
        writeback_store: Option<Arc<dyn WritebackStore + Send + Sync>>,
    ) -> Self {
        let context_pack = match (context_pack_store, cross_artifact_store) {
            (Some(cp), Some(ca)) => Some(
                context_pack::ContextPackService::with_cross_artifact_store(cp, ca),
            ),
            (Some(cp), None) => Some(context_pack::ContextPackService::new(cp)),
            _ => None,
        };
        Self {
            artifacts: artifacts::ArtifactQueryService::new(read_store),
            imports: imports::ImportApplicationService::new(import_store, object_store),
            retrieval: Arc::new(retrieval::ArchiveRetrievalService::new(retrieval_store)),
            search: search_read_store.map(search::ArchiveSearchService::new),
            artifact_detail: artifact_detail_store.map(artifact_detail::ArtifactDetailService::new),
            context_pack,
            object_search: object_search_store.map(search::ObjectSearchService::new),
            writeback: writeback_store.map(writeback::WritebackService::new),
        }
    }
}
