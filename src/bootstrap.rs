//! Composition root for provider-backed services.
//!
//! `main` should assemble application-facing traits through this module rather
//! than naming concrete providers directly.

use std::sync::Arc;

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

pub trait ArchiveStore: ImportWriteStore + ArtifactReadStore + Send + Sync {}
pub trait ArchiveAppService:
    ImportWriteStore + ArtifactReadStore + ObjectStore + Send + Sync
{
}

impl<T> ArchiveStore for T where T: ImportWriteStore + ArtifactReadStore + Send + Sync {}
impl<T> ArchiveAppService for T where
    T: ImportWriteStore + ArtifactReadStore + ObjectStore + Send + Sync
{
}

struct DelegatingArchiveAppService {
    archive_store: Arc<dyn ArchiveStore>,
    object_store: Arc<dyn ObjectStore>,
}

impl ImportWriteStore for DelegatingArchiveAppService {
    fn write_import(
        &self,
        import_set: crate::storage::WriteImportSet,
    ) -> crate::error::StorageResult<crate::storage::ImportWriteResult> {
        self.archive_store.write_import(import_set)
    }
}

impl ArtifactReadStore for DelegatingArchiveAppService {
    fn list_artifacts(&self) -> crate::error::StorageResult<Vec<crate::storage::ArtifactListItem>> {
        self.archive_store.list_artifacts()
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> crate::error::StorageResult<Option<crate::storage::LoadedArtifactForEnrichment>> {
        self.archive_store.load_artifact_for_enrichment(artifact_id)
    }
}

impl ObjectStore for DelegatingArchiveAppService {
    fn put_object(
        &self,
        object: crate::object_store::NewObject,
    ) -> crate::error::ObjectStoreResult<crate::object_store::PutObjectResult> {
        self.object_store.put_object(object)
    }

    fn get_object_bytes(
        &self,
        object: &crate::object_store::StoredObject,
    ) -> crate::error::ObjectStoreResult<Vec<u8>> {
        self.object_store.get_object_bytes(object)
    }

    fn delete_object(
        &self,
        object: &crate::object_store::StoredObject,
    ) -> crate::error::ObjectStoreResult<()> {
        self.object_store.delete_object(object)
    }
}

pub struct ServiceBundle {
    pub archive_service: Arc<dyn ArchiveAppService>,
    pub read_store: Arc<dyn ArtifactReadStore>,
    pub retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    pub enrichment_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub state_store: Arc<dyn EnrichmentStateStore>,
    pub derived_store: Arc<dyn crate::storage::DerivedMetadataWriteStore>,
    pub processor_factory: Arc<dyn ArtifactProcessorFactory>,
}

pub fn build_service_bundle(config: &AppConfig) -> ConfigResult<ServiceBundle> {
    let object_store: Arc<dyn ObjectStore> = match &config.object_store {
        ObjectStoreConfig::LocalFs(local_fs) => Arc::new(LocalFsObjectStore::new(local_fs.root.clone())),
        ObjectStoreConfig::S3Compatible(s3) => Arc::new(
            S3CompatibleObjectStore::new(s3.clone()).map_err(|err| ConfigError::InvalidObjectStoreConfig {
                message: err.to_string(),
            })?,
        ),
    };

    match &config.relational_store {
        RelationalStoreConfig::Postgres(pg_config) => {
            let archive_store_impl = Arc::new(PostgresImportWriteStore::new(pg_config.clone()));
            let archive_store: Arc<dyn ArchiveStore> = archive_store_impl.clone();
            let read_store: Arc<dyn ArtifactReadStore> = archive_store_impl.clone();
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> = archive_store_impl.clone();
            let processor_factory: Arc<dyn ArtifactProcessorFactory> = match &config.inference {
                InferenceConfig::Stub => Arc::new(StubProcessorFactory),
                InferenceConfig::OpenAi(openai) => Arc::new(
                    OpenAiProcessorFactory::new(openai.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
                InferenceConfig::Gemini(gemini) => Arc::new(
                    GeminiProcessorFactory::new(gemini.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
                InferenceConfig::Anthropic(anthropic) => Arc::new(
                    AnthropicProcessorFactory::new(anthropic.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
                InferenceConfig::Grok(grok) => Arc::new(
                    GrokProcessorFactory::new(grok.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
            };
            Ok(ServiceBundle {
                archive_service: Arc::new(DelegatingArchiveAppService {
                    archive_store: Arc::clone(&archive_store),
                    object_store,
                }),
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
            let archive_store: Arc<dyn ArchiveStore> = archive_store_impl.clone();
            let read_store: Arc<dyn ArtifactReadStore> = archive_store_impl.clone();
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> = archive_store_impl.clone();
            let processor_factory: Arc<dyn ArtifactProcessorFactory> = match &config.inference {
                InferenceConfig::Stub => Arc::new(StubProcessorFactory),
                InferenceConfig::OpenAi(openai) => Arc::new(
                    OpenAiProcessorFactory::new(openai.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
                InferenceConfig::Gemini(gemini) => Arc::new(
                    GeminiProcessorFactory::new(gemini.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
                InferenceConfig::Anthropic(anthropic) => Arc::new(
                    AnthropicProcessorFactory::new(anthropic.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
                InferenceConfig::Grok(grok) => Arc::new(
                    GrokProcessorFactory::new(grok.clone()).map_err(|message| {
                        ConfigError::InvalidInferenceConfig { message }
                    })?,
                ),
            };
            Ok(ServiceBundle {
                archive_service: Arc::new(DelegatingArchiveAppService {
                    archive_store: Arc::clone(&archive_store),
                    object_store,
                }),
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
