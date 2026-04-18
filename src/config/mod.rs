mod embedding;
mod env;
mod file;
mod http;
mod inference;
mod object_store;
mod pipeline;
mod relational;
mod tests;
mod vector;

use crate::error::{ConfigError, ConfigResult};
use crate::secrets::SecretStore;
use std::path::PathBuf;
use std::time::Duration;

pub use embedding::{
    EmbeddingConfig, GeminiEmbeddingConfig, OpenAiCompatibleEmbeddingConfig, OpenAiEmbeddingConfig,
    StubEmbeddingConfig,
};
use env::default_local_archive_root;
pub use env::expand_home_path;
use file::{
    ConfigFile, DatabaseSection, EmbeddingsSection, EnrichmentSection, HttpSection,
    ObjectStoreSection, VectorStoreSection,
};
pub use http::HttpConfig;
pub use inference::{
    AnthropicConfig, GeminiConfig, GrokConfig, InferenceConfig, OciConfig, OpenAiConfig,
    OpenAiReasoningEffort,
};
pub use object_store::{
    LocalFsObjectStoreConfig, ObjectStoreConfig, S3CompatibleObjectStoreConfig, S3UrlStyle,
};
pub use pipeline::{
    BatchPipelineModeConfig, DirectPipelineModeConfig, EnrichmentPipelineConfig,
    ExtractionChunkingConfig, InferenceExecutionMode, StageConfig,
};
pub use relational::{
    ConnectionPoolConfig, OracleConfig, PostgresConfig, RelationalStoreConfig, SqliteConfig,
};
pub use vector::{ManagedQdrantConfig, QdrantConfig, VectorStoreConfig};

/// Top-level runtime configuration. Provider-specific settings stay nested so
/// the composition root can switch implementations without leaking details into
/// the rest of the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub http: HttpConfig,
    pub relational_store: RelationalStoreConfig,
    pub vector_store: VectorStoreConfig,
    pub object_store: ObjectStoreConfig,
    pub inference: InferenceConfig,
    pub embeddings: EmbeddingConfig,
    pub inference_mode: InferenceExecutionMode,
}

impl AppConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let relational_store = RelationalStoreConfig::from_env()?;
        Ok(Self {
            http: HttpConfig::from_env()?,
            vector_store: VectorStoreConfig::from_env(&relational_store)?,
            relational_store,
            object_store: ObjectStoreConfig::from_env()?,
            inference: InferenceConfig::from_env()?,
            embeddings: EmbeddingConfig::from_env()?,
            inference_mode: InferenceExecutionMode::from_env()?,
        })
    }

    /// Load configuration from file, secrets store, and environment.
    ///
    /// 1. Load from ~/.open_archive/config.toml if it exists
    /// 2. Resolve secrets (API keys) from keyring/plain-file/env
    /// 3. Apply environment variable overrides (existing OA_* vars)
    /// 4. Return merged configuration
    ///
    /// Missing file is NOT an error — falls back to env-only behavior.
    pub fn load() -> ConfigResult<Self> {
        let secret_store = SecretStore::new();
        Self::load_with_secret_store(&secret_store)
    }

    /// Validate that enrichment (inference + embeddings) is configured.
    ///
    /// Returns an error with an actionable message when the inference or
    /// embedding provider is unconfigured (stub/disabled). Call this before
    /// starting any command that requires the enrichment pipeline.
    pub fn validate_enrichment_ready(&self) -> ConfigResult<()> {
        if self.inference == InferenceConfig::Stub {
            return Err(ConfigError::EnrichmentNotConfigured);
        }
        if matches!(self.embeddings, EmbeddingConfig::Disabled) {
            return Err(ConfigError::EmbeddingNotConfigured);
        }
        Ok(())
    }

    /// Load configuration with a provided secret store.
    ///
    /// This is pub(crate) for testability while keeping `load()` as the
    /// public API so callers don't need to construct SecretStore themselves.
    pub(crate) fn load_with_secret_store(secret_store: &SecretStore) -> ConfigResult<Self> {
        let config_path = Self::config_file_path()?;

        let file_config = if config_path.exists() {
            Self::load_from_file(&config_path)?
        } else {
            ConfigFile::default()
        };

        Self::merge_from_file_and_env(file_config, secret_store)
    }

    fn config_file_path() -> ConfigResult<PathBuf> {
        let home = dirs::home_dir().ok_or(ConfigError::MissingEnvWithDependency { key: "HOME" })?;
        Ok(home.join(".open_archive").join("config.toml"))
    }

    fn load_from_file(path: &std::path::Path) -> ConfigResult<ConfigFile> {
        let content = std::fs::read_to_string(path).map_err(|e| ConfigError::FileRead {
            path: path.to_path_buf(),
            source: e,
        })?;

        toml::from_str(&content).map_err(|e| ConfigError::InvalidToml {
            path: path.to_path_buf(),
            source: e,
        })
    }

    fn merge_from_file_and_env(file: ConfigFile, secret_store: &SecretStore) -> ConfigResult<Self> {
        let relational_store = if Self::has_relational_store_env_override() {
            RelationalStoreConfig::from_env()?
        } else {
            Self::relational_store_from_file_or_env(file.database)?
        };

        let object_store = if Self::has_object_store_env_override() {
            ObjectStoreConfig::from_env()?
        } else {
            Self::object_store_from_file_or_env(file.object_store)?
        };

        let vector_store = if Self::has_vector_store_env_override() {
            VectorStoreConfig::from_env(&relational_store)?
        } else {
            Self::vector_store_from_file_or_env(file.vector_store, &relational_store)?
        };

        let inference = if Self::has_inference_env_override() {
            InferenceConfig::from_env()?
        } else {
            Self::inference_from_file_or_env(file.enrichment, secret_store)?
        };

        let embeddings = if Self::has_embedding_env_override() {
            EmbeddingConfig::from_env()?
        } else {
            Self::embeddings_from_file_or_env(secret_store)?
        };

        let inference_mode = InferenceExecutionMode::from_env()?;

        let http = if Self::has_http_env_override() {
            HttpConfig::from_env()?
        } else {
            Self::http_from_file_or_env(file.http)?
        };

        Ok(Self {
            http,
            relational_store,
            vector_store,
            object_store,
            inference,
            embeddings,
            inference_mode,
        })
    }

    fn has_relational_store_env_override() -> bool {
        std::env::var("OA_RELATIONAL_STORE").is_ok()
            || std::env::var("OA_SQLITE_PATH").is_ok()
            || std::env::var("OA_POSTGRES_URL").is_ok()
            || std::env::var("OA_ORACLE_CONNECT_STRING").is_ok()
    }

    fn has_object_store_env_override() -> bool {
        std::env::var("OA_OBJECT_STORE").is_ok() || std::env::var("OA_OBJECT_STORE_ROOT").is_ok()
    }

    fn has_vector_store_env_override() -> bool {
        std::env::var("OA_VECTOR_STORE").is_ok()
            || std::env::var("OA_QDRANT_URL").is_ok()
            || std::env::var("OA_QDRANT_COLLECTION").is_ok()
    }

    fn has_inference_env_override() -> bool {
        std::env::var("OA_MODEL_PROVIDER").is_ok()
            || std::env::var("OA_GEMINI_API_KEY").is_ok()
            || std::env::var("OA_OPENAI_API_KEY").is_ok()
            || std::env::var("OA_ANTHROPIC_API_KEY").is_ok()
            || std::env::var("OA_GROK_API_KEY").is_ok()
            || std::env::var("OA_OCI_REGION").is_ok()
    }

    fn has_embedding_env_override() -> bool {
        std::env::var("OA_EMBEDDING_PROVIDER").is_ok()
            || std::env::var("OA_GEMINI_API_KEY").is_ok()
            || std::env::var("OA_OPENAI_API_KEY").is_ok()
    }

    fn has_http_env_override() -> bool {
        std::env::var("OA_HTTP_BIND").is_ok()
            || std::env::var("OA_HTTP_REQUEST_WORKERS").is_ok()
            || std::env::var("OA_ENRICHMENT_WORKERS").is_ok()
    }

    fn relational_store_from_file_or_env(
        file: Option<DatabaseSection>,
    ) -> ConfigResult<RelationalStoreConfig> {
        if let Some(db) = file {
            match db.store.as_deref() {
                Some("sqlite") => {
                    let path = match db.path {
                        Some(p) => expand_home_path(&p)?,
                        None => default_local_archive_root()?.join("open_archive.db"),
                    };
                    Ok(RelationalStoreConfig::Sqlite(SqliteConfig {
                        path,
                        busy_timeout: Duration::from_secs(30),
                    }))
                }
                Some("postgres") => Err(ConfigError::MissingEnv {
                    key: "OA_POSTGRES_URL",
                }),
                Some("oracle") => Err(ConfigError::MissingEnv {
                    key: "OA_ORACLE_CONNECT_STRING",
                }),
                Some(other) => Err(ConfigError::InvalidEnumEnv {
                    key: "database.store",
                    value: other.to_string(),
                    expected: "sqlite, postgres, oracle",
                }),
                None => Ok(RelationalStoreConfig::Sqlite(SqliteConfig {
                    path: default_local_archive_root()?.join("open_archive.db"),
                    busy_timeout: Duration::from_secs(30),
                })),
            }
        } else {
            Ok(RelationalStoreConfig::Sqlite(SqliteConfig {
                path: default_local_archive_root()?.join("open_archive.db"),
                busy_timeout: Duration::from_secs(30),
            }))
        }
    }

    fn object_store_from_file_or_env(
        file: Option<ObjectStoreSection>,
    ) -> ConfigResult<ObjectStoreConfig> {
        if let Some(os) = file {
            match os.store.as_deref() {
                Some("local_fs") => {
                    let root = match os.path {
                        Some(p) => expand_home_path(&p)?,
                        None => default_local_archive_root()?.join("objects"),
                    };
                    Ok(ObjectStoreConfig::LocalFs(LocalFsObjectStoreConfig {
                        root,
                    }))
                }
                Some("s3") => Err(ConfigError::MissingEnv {
                    key: "OA_S3_ENDPOINT",
                }),
                Some(other) => Err(ConfigError::InvalidEnumEnv {
                    key: "object_store.store",
                    value: other.to_string(),
                    expected: "local_fs, s3",
                }),
                None => Ok(ObjectStoreConfig::LocalFs(LocalFsObjectStoreConfig {
                    root: default_local_archive_root()?.join("objects"),
                })),
            }
        } else {
            Ok(ObjectStoreConfig::LocalFs(LocalFsObjectStoreConfig {
                root: default_local_archive_root()?.join("objects"),
            }))
        }
    }

    fn vector_store_from_file_or_env(
        file: Option<VectorStoreSection>,
        relational_store: &RelationalStoreConfig,
    ) -> ConfigResult<VectorStoreConfig> {
        if let Some(vs) = file {
            match vs.store.as_deref() {
                Some("disabled") => Ok(VectorStoreConfig::Disabled),
                Some("qdrant") => {
                    let url = vs
                        .url
                        .unwrap_or_else(|| "http://127.0.0.1:6333".to_string());
                    let collection_name =
                        vs.collection.unwrap_or_else(|| "open_archive".to_string());
                    let install_root = default_local_archive_root()?.join("qdrant");
                    Ok(VectorStoreConfig::Qdrant(Box::new(QdrantConfig {
                        url,
                        collection_name,
                        request_timeout: Duration::from_secs(30),
                        exact: true,
                        managed: ManagedQdrantConfig {
                            enabled: true,
                            version: "1.17.1".to_string(),
                            install_root: install_root.clone(),
                            storage_path: install_root.join("storage"),
                            log_path: install_root.join("qdrant.log"),
                            startup_timeout: Duration::from_secs(30),
                            binary_path: None,
                        },
                    })))
                }
                Some(other) => Err(ConfigError::InvalidEnumEnv {
                    key: "vector_store.store",
                    value: other.to_string(),
                    expected: "disabled, qdrant",
                }),
                None => match relational_store {
                    RelationalStoreConfig::Postgres(_) => Ok(VectorStoreConfig::PostgresPgVector),
                    _ => Self::default_qdrant_config(),
                },
            }
        } else {
            match relational_store {
                RelationalStoreConfig::Postgres(_) => Ok(VectorStoreConfig::PostgresPgVector),
                _ => Self::default_qdrant_config(),
            }
        }
    }

    fn inference_from_file_or_env(
        file: Option<EnrichmentSection>,
        secret_store: &SecretStore,
    ) -> ConfigResult<InferenceConfig> {
        if let Some(enr) = file {
            match enr.provider.as_deref() {
                Some("disabled") | Some("stub") => Ok(InferenceConfig::Stub),
                Some("gemini") => {
                    let model = enr.model.unwrap_or_else(|| "gemini-2.0-flash".to_string());
                    let api_key = secret_store.get("OA_GEMINI_API_KEY").ok_or(
                        ConfigError::MissingSecret {
                            key: "OA_GEMINI_API_KEY",
                        },
                    )?;
                    Ok(InferenceConfig::Gemini(
                        GeminiConfig::from_api_key_and_model(api_key, model)?,
                    ))
                }
                Some("openai") => {
                    let model = enr.model.unwrap_or_else(|| "gpt-4".to_string());
                    let api_key = secret_store.get("OA_OPENAI_API_KEY").ok_or(
                        ConfigError::MissingSecret {
                            key: "OA_OPENAI_API_KEY",
                        },
                    )?;
                    Ok(InferenceConfig::OpenAi(
                        OpenAiConfig::from_api_key_and_models(api_key, model.clone(), model)?,
                    ))
                }
                Some("anthropic") => {
                    let model = enr.model.unwrap_or_else(|| "claude-sonnet-4".to_string());
                    let api_key = secret_store.get("OA_ANTHROPIC_API_KEY").ok_or(
                        ConfigError::MissingSecret {
                            key: "OA_ANTHROPIC_API_KEY",
                        },
                    )?;
                    Ok(InferenceConfig::Anthropic(
                        AnthropicConfig::from_api_key_and_models(api_key, model.clone(), model)?,
                    ))
                }
                Some(other) => Err(ConfigError::InvalidEnumEnv {
                    key: "enrichment.provider",
                    value: other.to_string(),
                    expected: "disabled, gemini, openai, anthropic",
                }),
                None => Ok(InferenceConfig::Stub),
            }
        } else {
            Ok(InferenceConfig::Stub)
        }
    }

    fn http_from_file_or_env(file: Option<HttpSection>) -> ConfigResult<HttpConfig> {
        if let Some(http) = file {
            Ok(HttpConfig {
                bind_addr: http
                    .bind_addr
                    .unwrap_or_else(|| "127.0.0.1:8080".to_string()),
                request_worker_count: http.request_workers.unwrap_or(4),
                enrichment_worker_count: http.enrichment_workers.unwrap_or(2),
                enrichment_poll_interval_ms: 500,
            })
        } else {
            Ok(HttpConfig {
                bind_addr: "127.0.0.1:8080".to_string(),
                request_worker_count: 4,
                enrichment_worker_count: 2,
                enrichment_poll_interval_ms: 500,
            })
        }
    }

    fn embeddings_from_file_or_env(_secret_store: &SecretStore) -> ConfigResult<EmbeddingConfig> {
        Ok(EmbeddingConfig::Disabled)
    }

    fn default_qdrant_config() -> ConfigResult<VectorStoreConfig> {
        let install_root = default_local_archive_root()?.join("qdrant");
        Ok(VectorStoreConfig::Qdrant(Box::new(QdrantConfig {
            url: "http://127.0.0.1:6333".to_string(),
            collection_name: "open_archive".to_string(),
            request_timeout: Duration::from_secs(30),
            exact: true,
            managed: ManagedQdrantConfig {
                enabled: true,
                version: "1.17.1".to_string(),
                install_root: install_root.clone(),
                storage_path: install_root.join("storage"),
                log_path: install_root.join("qdrant.log"),
                startup_timeout: Duration::from_secs(30),
                binary_path: None,
            },
        })))
    }

    /// Write configuration to a TOML file.
    ///
    /// Serializes the current configuration to the specified path.
    /// Creates parent directories if needed.
    pub fn write_to_file(&self, path: &std::path::Path) -> ConfigResult<()> {
        let config_file = self.to_config_file();
        let toml_string = toml::to_string_pretty(&config_file)
            .map_err(|e| ConfigError::SerializeToml { source: e })?;

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ConfigError::FileRead {
                    path: parent.to_path_buf(),
                    source: e,
                })?;
            }
        }

        std::fs::write(path, toml_string).map_err(|e| ConfigError::FileRead {
            path: path.to_path_buf(),
            source: e,
        })?;

        Ok(())
    }

    /// Convert AppConfig to ConfigFile for serialization.
    fn to_config_file(&self) -> ConfigFile {
        ConfigFile {
            database: Some(DatabaseSection {
                store: Some(
                    match &self.relational_store {
                        RelationalStoreConfig::Sqlite(_) => "sqlite",
                        RelationalStoreConfig::Postgres(_) => "postgres",
                        RelationalStoreConfig::Oracle(_) => "oracle",
                    }
                    .to_string(),
                ),
                path: match &self.relational_store {
                    RelationalStoreConfig::Sqlite(s) => Some(s.path.display().to_string()),
                    _ => None,
                },
            }),
            object_store: Some(ObjectStoreSection {
                store: Some(
                    match &self.object_store {
                        ObjectStoreConfig::LocalFs(_) => "local_fs",
                        ObjectStoreConfig::S3Compatible(_) => "s3",
                    }
                    .to_string(),
                ),
                path: match &self.object_store {
                    ObjectStoreConfig::LocalFs(l) => Some(l.root.display().to_string()),
                    _ => None,
                },
            }),
            vector_store: Some(VectorStoreSection {
                store: Some(
                    match &self.vector_store {
                        VectorStoreConfig::Disabled => "disabled",
                        VectorStoreConfig::PostgresPgVector => "postgres",
                        VectorStoreConfig::Qdrant(_) => "qdrant",
                    }
                    .to_string(),
                ),
                url: match &self.vector_store {
                    VectorStoreConfig::Qdrant(q) => Some(q.url.clone()),
                    _ => None,
                },
                collection: match &self.vector_store {
                    VectorStoreConfig::Qdrant(q) => Some(q.collection_name.clone()),
                    _ => None,
                },
            }),
            enrichment: Some(EnrichmentSection {
                provider: Some(
                    match &self.inference {
                        InferenceConfig::Stub => "disabled",
                        InferenceConfig::OpenAi(_) => "openai",
                        InferenceConfig::Gemini(_) => "gemini",
                        InferenceConfig::Anthropic(_) => "anthropic",
                        InferenceConfig::Grok(_) => "grok",
                        InferenceConfig::Oci(_) => "oci",
                    }
                    .to_string(),
                ),
                model: match &self.inference {
                    InferenceConfig::Gemini(g) => Some(g.heavy_model.clone()),
                    InferenceConfig::OpenAi(o) => Some(o.heavy_model.clone()),
                    InferenceConfig::Anthropic(a) => Some(a.heavy_model.clone()),
                    InferenceConfig::Grok(g) => Some(g.heavy_model.clone()),
                    InferenceConfig::Oci(o) => Some(o.heavy_model.clone()),
                    InferenceConfig::Stub => None,
                },
            }),
            http: Some(HttpSection {
                bind_addr: Some(self.http.bind_addr.clone()),
                request_workers: Some(self.http.request_worker_count),
                enrichment_workers: Some(self.http.enrichment_worker_count),
            }),
            embeddings: Some(EmbeddingsSection {
                provider: Some(
                    match &self.embeddings {
                        EmbeddingConfig::Disabled => "disabled",
                        EmbeddingConfig::Stub(_) => "stub",
                        EmbeddingConfig::Gemini(_) => "gemini",
                        EmbeddingConfig::OpenAi(_) => "openai",
                        EmbeddingConfig::OpenAiCompatible(_) => "openai-compatible",
                    }
                    .to_string(),
                ),
                base_url: match &self.embeddings {
                    EmbeddingConfig::OpenAiCompatible(o) => Some(o.base_url.clone()),
                    _ => None,
                },
                api_key: match &self.embeddings {
                    EmbeddingConfig::OpenAiCompatible(o) => o.api_key.clone(),
                    _ => None,
                },
                model: match &self.embeddings {
                    EmbeddingConfig::Gemini(g) => Some(g.embedding_model.clone()),
                    EmbeddingConfig::OpenAi(o) => Some(o.embedding_model.clone()),
                    EmbeddingConfig::OpenAiCompatible(o) => Some(o.embedding_model.clone()),
                    EmbeddingConfig::Stub(s) => Some(s.model.clone()),
                    EmbeddingConfig::Disabled => None,
                },
                dimensions: match &self.embeddings {
                    EmbeddingConfig::Gemini(g) => Some(g.embedding_dimensions),
                    EmbeddingConfig::OpenAi(o) => Some(o.embedding_dimensions),
                    EmbeddingConfig::OpenAiCompatible(o) => Some(o.embedding_dimensions),
                    EmbeddingConfig::Stub(s) => Some(s.dimensions),
                    EmbeddingConfig::Disabled => None,
                },
            }),
        }
    }
}
