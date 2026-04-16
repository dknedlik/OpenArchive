pub mod artifact_detail;
pub mod artifacts;
pub mod context_pack;
pub mod imports;
pub mod retrieval;
pub mod review;
pub mod search;
pub mod writeback;

use std::sync::Arc;

use crate::embedding::EmbeddingProvider;
use crate::object_store::ObjectStore;
use crate::storage::{
    ArchiveRetrievalStore, ArchiveSearchReadStore, ArtifactContextPackReadStore,
    ArtifactDetailReadStore, ArtifactReadStore, CrossArtifactReadStore, DerivedObjectSearchStore,
    ImportWriteStore, ReviewStore, WritebackStore,
};

pub struct ArchiveApplication {
    pub artifacts: artifacts::ArtifactQueryService,
    pub imports: imports::ImportApplicationService,
    pub retrieval: Arc<dyn retrieval::ArchiveRetrievalServiceApi>,
    pub search: Option<search::ArchiveSearchService>,
    pub artifact_detail: Option<artifact_detail::ArtifactDetailService>,
    pub context_pack: Option<context_pack::ContextPackService>,
    pub object_search: Option<search::ObjectSearchService>,
    pub review: Option<review::ReviewService>,
    pub writeback: Option<writeback::WritebackService>,
}

pub struct ArchiveApplicationDeps {
    pub import_store: Arc<dyn ImportWriteStore + Send + Sync>,
    pub read_store: Arc<dyn ArtifactReadStore + Send + Sync>,
    pub retrieval_store: Arc<dyn ArchiveRetrievalStore + Send + Sync>,
    pub search_read_store: Option<Arc<dyn ArchiveSearchReadStore + Send + Sync>>,
    pub artifact_detail_store: Option<Arc<dyn ArtifactDetailReadStore + Send + Sync>>,
    pub context_pack_store: Option<Arc<dyn ArtifactContextPackReadStore + Send + Sync>>,
    pub cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    pub object_search_store: Option<Arc<dyn DerivedObjectSearchStore + Send + Sync>>,
    pub review_store: Option<Arc<dyn ReviewStore + Send + Sync>>,
    pub object_search_embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    pub object_store: Arc<dyn ObjectStore + Send + Sync>,
    pub writeback_store: Option<Arc<dyn WritebackStore + Send + Sync>>,
}

impl ArchiveApplication {
    pub fn require_search(&self) -> crate::error::StorageResult<&search::ArchiveSearchService> {
        self.search
            .as_ref()
            .ok_or(crate::error::StorageError::ServiceUnavailable {
                service_name: "search",
            })
    }

    pub fn require_object_search(
        &self,
    ) -> crate::error::StorageResult<&search::ObjectSearchService> {
        self.object_search
            .as_ref()
            .ok_or(crate::error::StorageError::ServiceUnavailable {
                service_name: "object_search",
            })
    }

    pub fn require_artifact_detail(
        &self,
    ) -> crate::error::StorageResult<&artifact_detail::ArtifactDetailService> {
        self.artifact_detail
            .as_ref()
            .ok_or(crate::error::StorageError::ServiceUnavailable {
                service_name: "artifact_detail",
            })
    }

    pub fn require_context_pack(
        &self,
    ) -> crate::error::StorageResult<&context_pack::ContextPackService> {
        self.context_pack
            .as_ref()
            .ok_or(crate::error::StorageError::ServiceUnavailable {
                service_name: "context_pack",
            })
    }

    pub fn require_review(&self) -> crate::error::StorageResult<&review::ReviewService> {
        self.review
            .as_ref()
            .ok_or(crate::error::StorageError::ServiceUnavailable {
                service_name: "review",
            })
    }

    pub fn require_writeback(&self) -> crate::error::StorageResult<&writeback::WritebackService> {
        self.writeback
            .as_ref()
            .ok_or(crate::error::StorageError::ServiceUnavailable {
                service_name: "writeback",
            })
    }

    pub fn new(deps: ArchiveApplicationDeps) -> Self {
        let ArchiveApplicationDeps {
            import_store,
            read_store,
            retrieval_store,
            search_read_store,
            artifact_detail_store,
            context_pack_store,
            cross_artifact_store,
            object_search_store,
            review_store,
            object_search_embedding_provider,
            object_store,
            writeback_store,
        } = deps;
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
            search: search_read_store.map(|store| match object_search_store.clone() {
                Some(object_store) => search::ArchiveSearchService::with_semantic_fallback(
                    store,
                    object_store,
                    object_search_embedding_provider.clone(),
                ),
                None => search::ArchiveSearchService::new(store),
            }),
            artifact_detail: artifact_detail_store.map(artifact_detail::ArtifactDetailService::new),
            context_pack,
            object_search: object_search_store.map(|store| {
                search::ObjectSearchService::new(store, object_search_embedding_provider.clone())
            }),
            review: review_store.map(review::ReviewService::new),
            writeback: writeback_store.map(writeback::WritebackService::new),
        }
    }
}
