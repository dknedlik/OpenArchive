//! Composition root for provider-backed services.
//!
//! `main` should assemble application-facing traits through this module rather
//! than naming concrete providers directly.

use std::sync::Arc;

use crate::app::ArchiveApplication;
use crate::config::{AppConfig, InferenceConfig, ObjectStoreConfig, RelationalStoreConfig};
use crate::embeddings::{FastembedTextEmbedder, TextEmbedder};
use crate::error::{ConfigError, ConfigResult};
use crate::object_store::{LocalFsObjectStore, ObjectStore, S3CompatibleObjectStore};
use crate::processor::{
    AnthropicProcessorFactory, ArtifactProcessorFactory, GeminiProcessorFactory,
    GrokProcessorFactory, OpenAiProcessorFactory, StubProcessorFactory,
};
use crate::storage::{
    ArchiveRetrievalStore, ArchiveSearchReadStore, ArtifactReadStore, EnrichmentJobLifecycleStore,
    EnrichmentStateStore, ImportWriteStore, MvpRetrievalReadStore, OracleArchiveRetrievalStore,
    OracleArtifactReadStore, OracleDerivedMetadataStore, OracleEnrichmentJobStore,
    OracleImportWriteStore, PostgresArchiveRetrievalStore, PostgresArtifactReadStore,
    PostgresDerivedMetadataStore, PostgresEnrichmentJobStore, PostgresImportWriteStore,
    PostgresRetrievalReadStore,
};

pub struct ServiceBundle {
    pub app: Arc<ArchiveApplication>,
    pub read_store: Arc<dyn ArtifactReadStore>,
    pub retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    pub mvp_retrieval_read_store: Option<Arc<dyn MvpRetrievalReadStore>>,
    pub enrichment_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub state_store: Arc<dyn EnrichmentStateStore>,
    pub derived_store: Arc<dyn crate::storage::DerivedMetadataWriteStore>,
    pub processor_factory: Arc<dyn ArtifactProcessorFactory>,
}

struct SplitStageProcessorFactory {
    primary: Arc<dyn ArtifactProcessorFactory>,
    reconcile: Arc<dyn ArtifactProcessorFactory>,
}

impl ArtifactProcessorFactory for SplitStageProcessorFactory {
    fn build_preprocess_processor(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<Box<dyn crate::processor::PreprocessProcessor>, crate::processor::ProcessorError>
    {
        self.primary.build_preprocess_processor(tier)
    }

    fn build(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<Box<dyn crate::processor::ArtifactProcessor>, crate::processor::ProcessorError>
    {
        self.primary.build(tier)
    }

    fn build_reconciliation_processor(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Box<dyn crate::processor::ReconciliationProcessor>,
        crate::processor::ProcessorError,
    > {
        self.reconcile.build_reconciliation_processor(tier)
    }

    fn build_batch_processor(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Option<Box<dyn crate::processor::ArtifactBatchProcessor>>,
        crate::processor::ProcessorError,
    > {
        self.primary.build_batch_processor(tier)
    }

    fn build_preprocess_batch_processor(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Option<Box<dyn crate::processor::PreprocessBatchProcessor>>,
        crate::processor::ProcessorError,
    > {
        self.primary.build_preprocess_batch_processor(tier)
    }

    fn build_reconciliation_batch_processor(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Option<Box<dyn crate::processor::ReconciliationBatchProcessor>>,
        crate::processor::ProcessorError,
    > {
        self.reconcile.build_reconciliation_batch_processor(tier)
    }

    fn build_extraction_submitter(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Option<Box<dyn crate::processor::ExtractionBatchSubmitter>>,
        crate::processor::ProcessorError,
    > {
        self.primary.build_extraction_submitter(tier)
    }

    fn build_preprocess_submitter(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Option<Box<dyn crate::processor::PreprocessBatchSubmitter>>,
        crate::processor::ProcessorError,
    > {
        self.primary.build_preprocess_submitter(tier)
    }

    fn build_reconciliation_submitter(
        &self,
        tier: crate::storage::EnrichmentTier,
    ) -> Result<
        Option<Box<dyn crate::processor::ReconciliationBatchSubmitter>>,
        crate::processor::ProcessorError,
    > {
        self.reconcile.build_reconciliation_submitter(tier)
    }
}

fn build_processor_factory(
    inference: &InferenceConfig,
) -> ConfigResult<Arc<dyn ArtifactProcessorFactory>> {
    match inference {
        InferenceConfig::Stub => Ok(Arc::new(StubProcessorFactory)),
        InferenceConfig::OpenAi(openai) => Ok(Arc::new(
            OpenAiProcessorFactory::new(openai.clone())
                .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
        )),
        InferenceConfig::Gemini(gemini) => Ok(Arc::new(
            GeminiProcessorFactory::new(gemini.clone())
                .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
        )),
        InferenceConfig::Anthropic(anthropic) => Ok(Arc::new(
            AnthropicProcessorFactory::new(anthropic.clone())
                .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
        )),
        InferenceConfig::Grok(grok) => Ok(Arc::new(
            GrokProcessorFactory::new(grok.clone())
                .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
        )),
    }
}

pub fn build_service_bundle(config: &AppConfig) -> ConfigResult<ServiceBundle> {
    let object_store: Arc<dyn ObjectStore + Send + Sync> = match &config.object_store {
        ObjectStoreConfig::LocalFs(local_fs) => {
            Arc::new(LocalFsObjectStore::new(local_fs.root.clone()))
        }
        ObjectStoreConfig::S3Compatible(s3) => {
            Arc::new(S3CompatibleObjectStore::new(s3.clone()).map_err(|err| {
                ConfigError::InvalidObjectStoreConfig {
                    message: err.to_string(),
                }
            })?)
        }
    };

    match &config.relational_store {
        RelationalStoreConfig::Postgres(pg_config) => {
            let embedder: Option<Arc<dyn TextEmbedder>> = if config.embeddings.enabled {
                Some(Arc::new(FastembedTextEmbedder::new(&config.embeddings)))
            } else {
                None
            };
            let import_store: Arc<dyn ImportWriteStore + Send + Sync> =
                Arc::new(PostgresImportWriteStore::new(pg_config.clone()));
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> =
                Arc::new(PostgresArtifactReadStore::new(pg_config.clone()));
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> =
                Arc::new(PostgresArchiveRetrievalStore::new(pg_config.clone()));
            let mvp_retrieval_store_impl =
                Arc::new(PostgresRetrievalReadStore::new(pg_config.clone()));
            let mvp_retrieval_read_store: Arc<dyn MvpRetrievalReadStore> =
                mvp_retrieval_store_impl.clone();
            let search_read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync> =
                mvp_retrieval_store_impl.clone();
            let artifact_detail_store: Arc<
                dyn crate::storage::ArtifactDetailReadStore + Send + Sync,
            > = mvp_retrieval_store_impl.clone();
            let primary_factory = build_processor_factory(&config.inference)?;
            let processor_factory: Arc<dyn ArtifactProcessorFactory> =
                if let Some(reconcile_inference) = &config.reconcile_inference {
                    Arc::new(SplitStageProcessorFactory {
                        primary: Arc::clone(&primary_factory),
                        reconcile: build_processor_factory(reconcile_inference)?,
                    })
                } else {
                    primary_factory
                };
            let context_pack_store: Arc<
                dyn crate::storage::ArtifactContextPackReadStore + Send + Sync,
            > = mvp_retrieval_store_impl.clone();
            let app = Arc::new(ArchiveApplication::new(
                Arc::clone(&import_store),
                Arc::clone(&read_store),
                Some(search_read_store),
                Some(artifact_detail_store),
                Some(context_pack_store),
                embedder,
                Arc::clone(&object_store),
            ));
            Ok(ServiceBundle {
                app,
                read_store,
                retrieval_store,
                mvp_retrieval_read_store: Some(mvp_retrieval_read_store),
                enrichment_store: Arc::new(PostgresEnrichmentJobStore::new(pg_config.clone())),
                state_store: Arc::new(PostgresDerivedMetadataStore::new(pg_config.clone())),
                derived_store: Arc::new(PostgresDerivedMetadataStore::new(pg_config.clone())),
                processor_factory,
            })
        }
        RelationalStoreConfig::Oracle(db_config) => {
            let embedder: Option<Arc<dyn TextEmbedder>> = if config.embeddings.enabled {
                Some(Arc::new(FastembedTextEmbedder::new(&config.embeddings)))
            } else {
                None
            };
            let import_store: Arc<dyn ImportWriteStore + Send + Sync> =
                Arc::new(OracleImportWriteStore::new(db_config.clone()));
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> =
                Arc::new(OracleArtifactReadStore::new(db_config.clone()));
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> =
                Arc::new(OracleArchiveRetrievalStore::new(db_config.clone()));
            let primary_factory = build_processor_factory(&config.inference)?;
            let processor_factory: Arc<dyn ArtifactProcessorFactory> =
                if let Some(reconcile_inference) = &config.reconcile_inference {
                    Arc::new(SplitStageProcessorFactory {
                        primary: Arc::clone(&primary_factory),
                        reconcile: build_processor_factory(reconcile_inference)?,
                    })
                } else {
                    primary_factory
                };
            let app = Arc::new(ArchiveApplication::new(
                Arc::clone(&import_store),
                Arc::clone(&read_store),
                None,
                None,
                None,
                embedder,
                Arc::clone(&object_store),
            ));
            Ok(ServiceBundle {
                app,
                read_store,
                retrieval_store,
                mvp_retrieval_read_store: None,
                enrichment_store: Arc::new(OracleEnrichmentJobStore::new(db_config.clone())),
                state_store: Arc::new(OracleDerivedMetadataStore::new(db_config.clone())),
                derived_store: Arc::new(OracleDerivedMetadataStore::new(db_config.clone())),
                processor_factory,
            })
        }
    }
}

/// Temporary helper for legacy commands that still talk directly to the Oracle
/// migration and probe code paths. This should disappear once those commands
/// become provider-backed too.
pub fn require_oracle_db_config(config: &AppConfig) -> ConfigResult<&crate::config::OracleConfig> {
    config
        .relational_store
        .oracle_config()
        .ok_or(ConfigError::InvalidEnumEnv {
            key: "OA_RELATIONAL_STORE",
            value: "unknown".to_string(),
            expected: "oracle",
        })
}
