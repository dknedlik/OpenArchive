//! Composition root for provider-backed services.
//!
//! `main` should assemble application-facing traits through this module rather
//! than naming concrete providers directly.

use std::sync::Arc;

use crate::app::ArchiveApplication;
use crate::config::{AppConfig, InferenceConfig, ObjectStoreConfig, RelationalStoreConfig};
use crate::error::{ConfigError, ConfigResult};
use crate::object_store::{LocalFsObjectStore, ObjectStore, S3CompatibleObjectStore};
use crate::processor::{
    AnthropicProcessorFactory, ArtifactProcessorFactory, GeminiProcessorFactory,
    GrokProcessorFactory, OpenAiProcessorFactory, StubProcessorFactory,
};
use crate::storage::{
    ArchiveRetrievalStore, ArtifactReadStore, EnrichmentJobLifecycleStore, EnrichmentStateStore,
    ImportWriteStore, OracleDerivedMetadataStore, OracleEnrichmentJobStore, OracleImportWriteStore,
    PostgresDerivedMetadataStore, PostgresEnrichmentJobStore, PostgresImportWriteStore,
};

pub struct ServiceBundle {
    pub app: Arc<ArchiveApplication>,
    pub read_store: Arc<dyn ArtifactReadStore>,
    pub retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    pub enrichment_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub state_store: Arc<dyn EnrichmentStateStore>,
    pub derived_store: Arc<dyn crate::storage::DerivedMetadataWriteStore>,
    pub processor_factory: Arc<dyn ArtifactProcessorFactory>,
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
            let archive_store_impl = Arc::new(PostgresImportWriteStore::new(pg_config.clone()));
            let import_store: Arc<dyn ImportWriteStore + Send + Sync> = archive_store_impl.clone();
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> = archive_store_impl.clone();
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> = archive_store_impl.clone();
            let processor_factory: Arc<dyn ArtifactProcessorFactory> = match &config.inference {
                InferenceConfig::Stub => Arc::new(StubProcessorFactory),
                InferenceConfig::OpenAi(openai) => Arc::new(
                    OpenAiProcessorFactory::new(openai.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
                InferenceConfig::Gemini(gemini) => Arc::new(
                    GeminiProcessorFactory::new(gemini.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
                InferenceConfig::Anthropic(anthropic) => Arc::new(
                    AnthropicProcessorFactory::new(anthropic.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
                InferenceConfig::Grok(grok) => Arc::new(
                    GrokProcessorFactory::new(grok.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
            };
            let app = Arc::new(ArchiveApplication::new(
                Arc::clone(&import_store),
                Arc::clone(&read_store),
                Arc::clone(&object_store),
            ));
            Ok(ServiceBundle {
                app,
                read_store,
                retrieval_store,
                enrichment_store: Arc::new(PostgresEnrichmentJobStore::new(pg_config.clone())),
                state_store: Arc::new(PostgresDerivedMetadataStore::new(pg_config.clone())),
                derived_store: Arc::new(PostgresDerivedMetadataStore::new(pg_config.clone())),
                processor_factory,
            })
        }
        RelationalStoreConfig::Oracle(db_config) => {
            let archive_store_impl = Arc::new(OracleImportWriteStore::new(db_config.clone()));
            let import_store: Arc<dyn ImportWriteStore + Send + Sync> = archive_store_impl.clone();
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> = archive_store_impl.clone();
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> = archive_store_impl.clone();
            let processor_factory: Arc<dyn ArtifactProcessorFactory> = match &config.inference {
                InferenceConfig::Stub => Arc::new(StubProcessorFactory),
                InferenceConfig::OpenAi(openai) => Arc::new(
                    OpenAiProcessorFactory::new(openai.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
                InferenceConfig::Gemini(gemini) => Arc::new(
                    GeminiProcessorFactory::new(gemini.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
                InferenceConfig::Anthropic(anthropic) => Arc::new(
                    AnthropicProcessorFactory::new(anthropic.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
                InferenceConfig::Grok(grok) => Arc::new(
                    GrokProcessorFactory::new(grok.clone())
                        .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
                ),
            };
            let app = Arc::new(ArchiveApplication::new(
                Arc::clone(&import_store),
                Arc::clone(&read_store),
                Arc::clone(&object_store),
            ));
            Ok(ServiceBundle {
                app,
                read_store,
                retrieval_store,
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
