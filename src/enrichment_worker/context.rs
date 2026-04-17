use crate::config::ExtractionChunkingConfig;
use crate::embedding::EmbeddingProvider;
use crate::processor::ArtifactProcessorFactory;
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArtifactReadStore, CrossArtifactReadStore, DerivedMetadataWriteStore,
    DerivedObjectEmbeddingStore, EnrichmentJobLifecycleStore, EnrichmentStateStore,
};
use std::sync::Arc;

/// Borrowed view of all store/service/factory dependencies used by worker functions.
pub(super) struct WorkerContext<'a> {
    pub(super) job_store: &'a dyn EnrichmentJobLifecycleStore,
    pub(super) read_store: &'a dyn ArtifactReadStore,
    pub(super) state_store: &'a dyn EnrichmentStateStore,
    pub(super) derived_store: &'a dyn DerivedMetadataWriteStore,
    pub(super) cross_artifact_store: Option<&'a (dyn CrossArtifactReadStore + Send + Sync)>,
    pub(super) embedding_store: Option<&'a dyn DerivedObjectEmbeddingStore>,
    pub(super) embedding_provider: Option<&'a dyn EmbeddingProvider>,
    pub(super) processor_factory: &'a dyn ArtifactProcessorFactory,
}

/// Extraction chunking and coverage parameters.
pub(super) struct ExtractionPolicy<'a> {
    pub(super) chunking: &'a ExtractionChunkingConfig,
}

/// Owned Arc resources for long-running worker threads.
pub(super) struct WorkerResources {
    pub(super) job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub(super) read_store: Arc<dyn ArtifactReadStore>,
    pub(super) state_store: Arc<dyn EnrichmentStateStore>,
    pub(super) derived_store: Arc<dyn DerivedMetadataWriteStore>,
    pub(super) cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    pub(super) embedding_store: Option<Arc<dyn DerivedObjectEmbeddingStore>>,
    pub(super) embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    pub(super) processor_factory: Arc<dyn ArtifactProcessorFactory>,
}

impl WorkerResources {
    pub(super) fn as_context(&self) -> WorkerContext<'_> {
        WorkerContext {
            job_store: self.job_store.as_ref(),
            read_store: self.read_store.as_ref(),
            state_store: self.state_store.as_ref(),
            derived_store: self.derived_store.as_ref(),
            cross_artifact_store: self.cross_artifact_store.as_deref(),
            embedding_store: self.embedding_store.as_deref(),
            embedding_provider: self.embedding_provider.as_deref(),
            processor_factory: self.processor_factory.as_ref(),
        }
    }
}

pub struct WorkerStartConfig<'a> {
    pub http: &'a crate::config::HttpConfig,
    pub shutdown: ShutdownToken,
    pub processor_factory: Arc<dyn ArtifactProcessorFactory>,
}

pub struct EnrichmentPipelineResources {
    pub job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub read_store: Arc<dyn ArtifactReadStore>,
    pub state_store: Arc<dyn EnrichmentStateStore>,
    pub derived_store: Arc<dyn DerivedMetadataWriteStore>,
    pub cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    pub embedding_store: Option<Arc<dyn DerivedObjectEmbeddingStore>>,
    pub embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
}
