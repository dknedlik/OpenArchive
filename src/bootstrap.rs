//! Composition root for provider-backed services.
//!
//! `main` should assemble application-facing traits through this module rather
//! than naming concrete providers directly.

use std::sync::Arc;

use crate::app::{ArchiveApplication, ArchiveApplicationDeps};
use crate::config::{
    AppConfig, InferenceConfig, ObjectStoreConfig, RelationalStoreConfig, VectorStoreConfig,
};
use crate::embedding::{build_embedding_provider, EmbeddingProvider};
use crate::error::{ConfigError, ConfigResult};
use crate::object_store::{LocalFsObjectStore, ObjectStore, S3CompatibleObjectStore};
use crate::postgres_db;
use crate::processor::{
    AnthropicProcessorFactory, ArtifactProcessorFactory, GeminiProcessorFactory,
    GrokProcessorFactory, OciProcessorFactory, OpenAiProcessorFactory, StubProcessorFactory,
};
use crate::storage::{
    ArchiveRetrievalStore, ArchiveSearchReadStore, ArtifactReadStore,
    CompositeDerivedObjectSearchStore, EnrichmentJobLifecycleStore, EnrichmentStateStore,
    ImportWriteStore, OperatorStore, OracleArchiveRetrievalStore, OracleArtifactReadStore,
    OracleDerivedMetadataStore, OracleEnrichmentJobStore, OracleImportWriteStore,
    OracleOperatorStore, PostgresArtifactReadStore, PostgresDerivedMetadataStore,
    PostgresDerivedObjectEmbeddingStore, PostgresEnrichmentJobStore, PostgresImportWriteStore,
    PostgresOperatorStore, PostgresRetrievalReadStore, PostgresWritebackStore,
    SqliteArtifactReadStore, SqliteDerivedMetadataStore, SqliteEnrichmentJobStore,
    SqliteImportWriteStore, SqliteOperatorStore, SqliteRetrievalReadStore, SqliteWritebackStore,
};
use crate::vector::QdrantVectorStore;

type CrossArtifactStoreArc = Arc<dyn crate::storage::CrossArtifactReadStore + Send + Sync>;
type ObjectSearchStoreArc = Arc<dyn crate::storage::DerivedObjectSearchStore + Send + Sync>;
type EmbeddingStoreArc = Arc<dyn crate::storage::DerivedObjectEmbeddingStore>;
type VectorStoreBundle = (
    CrossArtifactStoreArc,
    ObjectSearchStoreArc,
    Option<EmbeddingStoreArc>,
);

pub struct ServiceBundle {
    pub app: Arc<ArchiveApplication>,
    pub read_store: Arc<dyn ArtifactReadStore>,
    pub enrichment_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub state_store: Arc<dyn EnrichmentStateStore>,
    pub derived_store: Arc<dyn crate::storage::DerivedMetadataWriteStore>,
    pub cross_artifact_store: Option<Arc<dyn crate::storage::CrossArtifactReadStore + Send + Sync>>,
    pub object_search_store:
        Option<Arc<dyn crate::storage::DerivedObjectSearchStore + Send + Sync>>,
    pub processor_factory: Arc<dyn ArtifactProcessorFactory>,
    pub embedding_store: Option<Arc<dyn crate::storage::DerivedObjectEmbeddingStore>>,
    pub embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    pub operator_store: Arc<dyn OperatorStore + Send + Sync>,
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
        InferenceConfig::Oci(oci) => Ok(Arc::new(
            OciProcessorFactory::new(oci.clone())
                .map_err(|message| ConfigError::InvalidInferenceConfig { message })?,
        )),
    }
}

fn validate_postgres_embedding_schema(
    pg_config: &crate::config::PostgresConfig,
    embedding_provider: Option<&Arc<dyn EmbeddingProvider>>,
) -> ConfigResult<()> {
    let Some(embedding_provider) = embedding_provider else {
        return Ok(());
    };

    let mut client =
        postgres_db::connect(pg_config).map_err(|err| ConfigError::InvalidEmbeddingConfig {
            message: format!("failed to connect for embedding schema validation: {err}"),
        })?;

    let row = client
        .query_opt(
            "SELECT format_type(a.atttypid, a.atttypmod)
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema()
               AND c.relname = 'oa_derived_object_embedding'
               AND a.attname = 'embedding'
               AND NOT a.attisdropped",
            &[],
        )
        .map_err(|err| ConfigError::InvalidEmbeddingConfig {
            message: format!("failed to inspect embedding column schema: {err}"),
        })?;

    let Some(row) = row else {
        return Ok(());
    };

    let type_name: String = row.get(0);
    let Some(configured_dimensions) = parse_vector_dimensions(&type_name) else {
        return Err(ConfigError::InvalidEmbeddingConfig {
            message: format!("unexpected embedding column type: {type_name}"),
        });
    };

    if configured_dimensions != embedding_provider.dimensions() {
        return Err(ConfigError::InvalidEmbeddingConfig {
            message: format!(
                "embedding dimension mismatch: config requests {}, but database column is {}. Changing embedding dimensions requires a schema migration and re-embedding existing derived objects.",
                embedding_provider.dimensions(),
                configured_dimensions
            ),
        });
    }

    Ok(())
}

fn parse_vector_dimensions(type_name: &str) -> Option<usize> {
    let type_name = type_name.trim();
    let prefix = "vector(";
    let suffix = ")";
    type_name
        .strip_prefix(prefix)?
        .strip_suffix(suffix)?
        .parse::<usize>()
        .ok()
}

pub fn build_service_bundle(config: &AppConfig) -> ConfigResult<ServiceBundle> {
    let configured_embedding_provider = build_embedding_provider(&config.embeddings)
        .map_err(|message| ConfigError::InvalidEmbeddingConfig { message })?;
    let embedding_provider = match config.vector_store {
        VectorStoreConfig::Disabled => None,
        _ => configured_embedding_provider.clone(),
    };
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
            if matches!(config.vector_store, VectorStoreConfig::PostgresPgVector) {
                validate_postgres_embedding_schema(pg_config, embedding_provider.as_ref())?;
            }
            let import_store: Arc<dyn ImportWriteStore + Send + Sync> =
                Arc::new(PostgresImportWriteStore::new(pg_config.clone()));
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> =
                Arc::new(PostgresArtifactReadStore::new(pg_config.clone()));
            let operator_store: Arc<dyn OperatorStore + Send + Sync> =
                Arc::new(PostgresOperatorStore::new(pg_config.clone()));
            let retrieval_impl = Arc::new(PostgresRetrievalReadStore::new(pg_config.clone()));
            let retrieval_store: Arc<dyn ArchiveRetrievalStore + Send + Sync> =
                retrieval_impl.clone();
            let search_read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync> =
                retrieval_impl.clone();
            let artifact_detail_store: Arc<
                dyn crate::storage::ArtifactDetailReadStore + Send + Sync,
            > = retrieval_impl.clone();
            let processor_factory = build_processor_factory(&config.inference)?;
            let context_pack_store: Arc<
                dyn crate::storage::ArtifactContextPackReadStore + Send + Sync,
            > = retrieval_impl.clone();
            let postgres_cross_artifact_store: Arc<
                dyn crate::storage::CrossArtifactReadStore + Send + Sync,
            > = retrieval_impl.clone();
            let postgres_object_search_store: Arc<
                dyn crate::storage::DerivedObjectSearchStore + Send + Sync,
            > = retrieval_impl.clone();
            let review_store: Arc<dyn crate::storage::ReviewStore + Send + Sync> =
                retrieval_impl.clone();
            let writeback_store: Arc<dyn crate::storage::WritebackStore + Send + Sync> =
                Arc::new(PostgresWritebackStore::new(pg_config.clone()));
            let lookup_store: Arc<dyn crate::storage::DerivedObjectLookupStore + Send + Sync> =
                retrieval_impl.clone();

            let (cross_artifact_store, object_search_store, embedding_store): VectorStoreBundle =
                match &config.vector_store {
                    VectorStoreConfig::Disabled => (
                        postgres_cross_artifact_store,
                        postgres_object_search_store,
                        None,
                    ),
                    VectorStoreConfig::PostgresPgVector => (
                        postgres_cross_artifact_store,
                        postgres_object_search_store,
                        Some(Arc::new(PostgresDerivedObjectEmbeddingStore::new(
                            pg_config.clone(),
                        ))),
                    ),
                    VectorStoreConfig::Qdrant(qdrant_config) => {
                        let qdrant_impl = Arc::new(QdrantVectorStore::new(
                            qdrant_config.as_ref().clone(),
                            embedding_provider.as_deref(),
                        )?);
                        let composite_impl = Arc::new(CompositeDerivedObjectSearchStore::new(
                            postgres_object_search_store,
                            lookup_store,
                            postgres_cross_artifact_store,
                            qdrant_impl.clone(),
                        ));
                        (composite_impl.clone(), composite_impl, Some(qdrant_impl))
                    }
                };
            let app = Arc::new(ArchiveApplication::new(ArchiveApplicationDeps {
                import_store: Arc::clone(&import_store),
                read_store: Arc::clone(&read_store),
                retrieval_store,
                search_read_store: Some(search_read_store),
                artifact_detail_store: Some(artifact_detail_store),
                context_pack_store: Some(context_pack_store),
                cross_artifact_store: Some(cross_artifact_store.clone()),
                object_search_store: Some(object_search_store.clone()),
                review_store: Some(review_store),
                object_search_embedding_provider: embedding_provider.clone(),
                object_store: Arc::clone(&object_store),
                writeback_store: Some(writeback_store),
            }));
            Ok(ServiceBundle {
                app,
                read_store,
                enrichment_store: Arc::new(PostgresEnrichmentJobStore::new(pg_config.clone())),
                state_store: Arc::new(PostgresDerivedMetadataStore::new(pg_config.clone())),
                derived_store: Arc::new(PostgresDerivedMetadataStore::new(pg_config.clone())),
                cross_artifact_store: Some(cross_artifact_store),
                object_search_store: Some(object_search_store),
                processor_factory,
                embedding_store,
                embedding_provider,
                operator_store,
            })
        }
        RelationalStoreConfig::Sqlite(sqlite_config) => {
            if matches!(config.vector_store, VectorStoreConfig::PostgresPgVector) {
                return Err(ConfigError::InvalidVectorStoreConfig {
                    message: "sqlite relational storage does not support pgvector".to_string(),
                });
            }

            let import_store: Arc<dyn ImportWriteStore + Send + Sync> =
                Arc::new(SqliteImportWriteStore::new(sqlite_config.clone()));
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> =
                Arc::new(SqliteArtifactReadStore::new(sqlite_config.clone()));
            let operator_store: Arc<dyn OperatorStore + Send + Sync> =
                Arc::new(SqliteOperatorStore::new(sqlite_config.clone()));
            let retrieval_impl = Arc::new(SqliteRetrievalReadStore::new(sqlite_config.clone()));
            let retrieval_store: Arc<dyn ArchiveRetrievalStore + Send + Sync> =
                retrieval_impl.clone();
            let search_read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync> =
                retrieval_impl.clone();
            let artifact_detail_store: Arc<
                dyn crate::storage::ArtifactDetailReadStore + Send + Sync,
            > = retrieval_impl.clone();
            let context_pack_store: Arc<
                dyn crate::storage::ArtifactContextPackReadStore + Send + Sync,
            > = retrieval_impl.clone();
            let sqlite_cross_artifact_store: Arc<
                dyn crate::storage::CrossArtifactReadStore + Send + Sync,
            > = retrieval_impl.clone();
            let sqlite_object_search_store: Arc<
                dyn crate::storage::DerivedObjectSearchStore + Send + Sync,
            > = retrieval_impl.clone();
            let lookup_store: Arc<dyn crate::storage::DerivedObjectLookupStore + Send + Sync> =
                retrieval_impl.clone();
            let review_store: Arc<dyn crate::storage::ReviewStore + Send + Sync> =
                retrieval_impl.clone();
            let writeback_store: Arc<dyn crate::storage::WritebackStore + Send + Sync> =
                Arc::new(SqliteWritebackStore::new(sqlite_config.clone()));
            let processor_factory = build_processor_factory(&config.inference)?;

            let (cross_artifact_store, object_search_store, embedding_store): VectorStoreBundle =
                match &config.vector_store {
                    VectorStoreConfig::Disabled => (
                        sqlite_cross_artifact_store,
                        sqlite_object_search_store,
                        None,
                    ),
                    VectorStoreConfig::PostgresPgVector => unreachable!("validated above"),
                    VectorStoreConfig::Qdrant(qdrant_config) => {
                        let qdrant_impl = Arc::new(QdrantVectorStore::new(
                            qdrant_config.as_ref().clone(),
                            embedding_provider.as_deref(),
                        )?);
                        let composite_impl = Arc::new(CompositeDerivedObjectSearchStore::new(
                            sqlite_object_search_store,
                            lookup_store,
                            sqlite_cross_artifact_store,
                            qdrant_impl.clone(),
                        ));
                        (composite_impl.clone(), composite_impl, Some(qdrant_impl))
                    }
                };

            let app = Arc::new(ArchiveApplication::new(ArchiveApplicationDeps {
                import_store: Arc::clone(&import_store),
                read_store: Arc::clone(&read_store),
                retrieval_store,
                search_read_store: Some(search_read_store),
                artifact_detail_store: Some(artifact_detail_store),
                context_pack_store: Some(context_pack_store),
                cross_artifact_store: Some(cross_artifact_store.clone()),
                object_search_store: Some(object_search_store.clone()),
                review_store: Some(review_store),
                object_search_embedding_provider: embedding_provider.clone(),
                object_store: Arc::clone(&object_store),
                writeback_store: Some(writeback_store),
            }));

            Ok(ServiceBundle {
                app,
                read_store,
                enrichment_store: Arc::new(SqliteEnrichmentJobStore::new(sqlite_config.clone())),
                state_store: Arc::new(SqliteDerivedMetadataStore::new(sqlite_config.clone())),
                derived_store: Arc::new(SqliteDerivedMetadataStore::new(sqlite_config.clone())),
                cross_artifact_store: Some(cross_artifact_store),
                object_search_store: Some(object_search_store),
                processor_factory,
                embedding_store,
                embedding_provider,
                operator_store,
            })
        }
        RelationalStoreConfig::Oracle(db_config) => {
            if !matches!(config.vector_store, VectorStoreConfig::Disabled) {
                return Err(ConfigError::InvalidVectorStoreConfig {
                    message:
                        "oracle relational storage does not yet support external vector providers"
                            .to_string(),
                });
            }
            let import_store: Arc<dyn ImportWriteStore + Send + Sync> =
                Arc::new(OracleImportWriteStore::new(db_config.clone()));
            let read_store: Arc<dyn ArtifactReadStore + Send + Sync> =
                Arc::new(OracleArtifactReadStore::new(db_config.clone()));
            let operator_store: Arc<dyn OperatorStore + Send + Sync> =
                Arc::new(OracleOperatorStore::new(db_config.clone()));
            let retrieval_store: Arc<dyn ArchiveRetrievalStore> =
                Arc::new(OracleArchiveRetrievalStore::new(db_config.clone()));
            let processor_factory = build_processor_factory(&config.inference)?;
            let app = Arc::new(ArchiveApplication::new(ArchiveApplicationDeps {
                import_store: Arc::clone(&import_store),
                read_store: Arc::clone(&read_store),
                retrieval_store,
                search_read_store: None,
                artifact_detail_store: None,
                context_pack_store: None,
                cross_artifact_store: None,
                object_search_store: None,
                review_store: None,
                object_search_embedding_provider: None,
                object_store: Arc::clone(&object_store),
                writeback_store: None,
            }));
            Ok(ServiceBundle {
                app,
                read_store,
                enrichment_store: Arc::new(OracleEnrichmentJobStore::new(db_config.clone())),
                state_store: Arc::new(OracleDerivedMetadataStore::new(db_config.clone())),
                derived_store: Arc::new(OracleDerivedMetadataStore::new(db_config.clone())),
                cross_artifact_store: None,
                object_search_store: None,
                processor_factory,
                embedding_store: None,
                embedding_provider: None,
                operator_store,
            })
        }
    }
}

/// Helper for Oracle-specific maintenance commands.
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

#[cfg(test)]
mod tests {
    use super::parse_vector_dimensions;

    #[test]
    fn parses_vector_dimensions_from_postgres_type_name() {
        assert_eq!(parse_vector_dimensions("vector(3072)"), Some(3072));
        assert_eq!(parse_vector_dimensions(" vector(1536) "), Some(1536));
        assert_eq!(parse_vector_dimensions("text"), None);
    }
}
