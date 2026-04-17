use crate::error::{ConfigError, ConfigResult};
use crate::secrets::SecretStore;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

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
        // Check if any relational store env vars are set - if so, use from_env
        let relational_store = if Self::has_relational_store_env_override() {
            RelationalStoreConfig::from_env()?
        } else {
            Self::relational_store_from_file_or_env(file.database)?
        };

        // Check if any object store env vars are set
        let object_store = if Self::has_object_store_env_override() {
            ObjectStoreConfig::from_env()?
        } else {
            Self::object_store_from_file_or_env(file.object_store)?
        };

        // Check if any vector store env vars are set
        let vector_store = if Self::has_vector_store_env_override() {
            VectorStoreConfig::from_env(&relational_store)?
        } else {
            Self::vector_store_from_file_or_env(file.vector_store, &relational_store)?
        };

        // Check if any inference env vars are set
        let inference = if Self::has_inference_env_override() {
            InferenceConfig::from_env()?
        } else {
            Self::inference_from_file_or_env(file.enrichment, secret_store)?
        };

        // Embeddings from env or secrets store
        let embeddings = if Self::has_embedding_env_override() {
            EmbeddingConfig::from_env()?
        } else {
            Self::embeddings_from_file_or_env(secret_store)?
        };

        // Inference mode always from env for now
        let inference_mode = InferenceExecutionMode::from_env()?;

        // Check if any HTTP env vars are set
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
        env::var("OA_RELATIONAL_STORE").is_ok()
            || env::var("OA_SQLITE_PATH").is_ok()
            || env::var("OA_POSTGRES_URL").is_ok()
            || env::var("OA_ORACLE_CONNECT_STRING").is_ok()
    }

    fn has_object_store_env_override() -> bool {
        env::var("OA_OBJECT_STORE").is_ok() || env::var("OA_OBJECT_STORE_ROOT").is_ok()
    }

    fn has_vector_store_env_override() -> bool {
        env::var("OA_VECTOR_STORE").is_ok()
            || env::var("OA_QDRANT_URL").is_ok()
            || env::var("OA_QDRANT_COLLECTION").is_ok()
    }

    fn has_inference_env_override() -> bool {
        env::var("OA_MODEL_PROVIDER").is_ok()
            || env::var("OA_GEMINI_API_KEY").is_ok()
            || env::var("OA_OPENAI_API_KEY").is_ok()
            || env::var("OA_ANTHROPIC_API_KEY").is_ok()
            || env::var("OA_GROK_API_KEY").is_ok()
            || env::var("OA_OCI_REGION").is_ok()
    }

    fn has_embedding_env_override() -> bool {
        env::var("OA_EMBEDDING_PROVIDER").is_ok()
            || env::var("OA_GEMINI_API_KEY").is_ok()
            || env::var("OA_OPENAI_API_KEY").is_ok()
    }

    fn has_http_env_override() -> bool {
        env::var("OA_HTTP_BIND").is_ok()
            || env::var("OA_HTTP_REQUEST_WORKERS").is_ok()
            || env::var("OA_ENRICHMENT_WORKERS").is_ok()
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
            // No file config, use env defaults
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
                None => {
                    // Default based on relational store
                    match relational_store {
                        RelationalStoreConfig::Postgres(_) => {
                            Ok(VectorStoreConfig::PostgresPgVector)
                        }
                        _ => Self::default_qdrant_config(),
                    }
                }
            }
        } else {
            // Default based on relational store
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
        // Default to disabled when loading from file without explicit provider
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

        // Ensure parent directory exists
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

// TOML file schema structs
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    database: Option<DatabaseSection>,
    object_store: Option<ObjectStoreSection>,
    vector_store: Option<VectorStoreSection>,
    enrichment: Option<EnrichmentSection>,
    http: Option<HttpSection>,
    embeddings: Option<EmbeddingsSection>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DatabaseSection {
    store: Option<String>,
    path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ObjectStoreSection {
    store: Option<String>,
    path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct VectorStoreSection {
    store: Option<String>,
    url: Option<String>,
    collection: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct EnrichmentSection {
    provider: Option<String>,
    model: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct EmbeddingsSection {
    provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    dimensions: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HttpSection {
    bind_addr: Option<String>,
    request_workers: Option<usize>,
    enrichment_workers: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceExecutionMode {
    Direct,
    Batch,
}

impl InferenceExecutionMode {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_INFERENCE_MODE").unwrap_or_else(|_| "batch".to_string());
        match value.as_str() {
            "direct" => Ok(Self::Direct),
            "batch" => Ok(Self::Batch),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_INFERENCE_MODE",
                value,
                expected: "direct, batch",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingConfig {
    Disabled,
    Stub(StubEmbeddingConfig),
    Gemini(GeminiEmbeddingConfig),
    OpenAi(OpenAiEmbeddingConfig),
    OpenAiCompatible(OpenAiCompatibleEmbeddingConfig),
}

impl EmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_EMBEDDING_PROVIDER").unwrap_or_else(|_| "disabled".to_string());
        match provider.as_str() {
            "disabled" => Ok(Self::Disabled),
            "stub" => Ok(Self::Stub(StubEmbeddingConfig::from_env()?)),
            "gemini" => Ok(Self::Gemini(GeminiEmbeddingConfig::from_env()?)),
            "openai" => Ok(Self::OpenAi(OpenAiEmbeddingConfig::from_env()?)),
            "openai-compatible" => Ok(Self::OpenAiCompatible(
                OpenAiCompatibleEmbeddingConfig::from_env()?,
            )),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_EMBEDDING_PROVIDER",
                value: provider,
                expected: "disabled, stub, gemini, openai, openai-compatible",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubEmbeddingConfig {
    pub model: String,
    pub dimensions: usize,
}

impl StubEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            model: env::var("OA_STUB_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "stub-embedding-small".to_string()),
            dimensions: positive_usize_env("OA_STUB_EMBEDDING_DIMENSIONS")?.unwrap_or(8),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiEmbeddingConfig {
    pub api_key: String,
    pub base_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
}

impl GeminiEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_GEMINI_API_KEY")?,
            base_url: env::var("OA_GEMINI_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".to_string()),
            embedding_model: env::var("OA_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "gemini-embedding-001".to_string()),
            embedding_dimensions: positive_usize_env("OA_EMBEDDING_DIMENSIONS")?.unwrap_or(3072),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiEmbeddingConfig {
    pub api_key: String,
    pub base_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
}

impl OpenAiEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_OPENAI_API_KEY")?,
            base_url: env::var("OA_OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            embedding_model: env::var("OA_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".to_string()),
            embedding_dimensions: positive_usize_env("OA_EMBEDDING_DIMENSIONS")?.unwrap_or(1536),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCompatibleEmbeddingConfig {
    pub api_key: Option<String>,
    pub base_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
}

impl OpenAiCompatibleEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: optional_trimmed_env("OA_OPENAI_COMPATIBLE_API_KEY"),
            base_url: required_env("OA_OPENAI_COMPATIBLE_BASE_URL")?,
            embedding_model: required_env("OA_EMBEDDING_MODEL")?,
            embedding_dimensions: positive_usize_env("OA_EMBEDDING_DIMENSIONS")?.unwrap_or(768),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalStoreConfig {
    Postgres(PostgresConfig),
    Sqlite(SqliteConfig),
    Oracle(OracleConfig),
}

impl RelationalStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_RELATIONAL_STORE").unwrap_or_else(|_| "sqlite".to_string());
        match provider.as_str() {
            "postgres" => Ok(Self::Postgres(PostgresConfig::from_env()?)),
            "sqlite" => Ok(Self::Sqlite(SqliteConfig::from_env()?)),
            "oracle" => Ok(Self::Oracle(OracleConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                value: provider,
                expected: "postgres, sqlite, oracle",
            }),
        }
    }

    pub fn oracle_config(&self) -> Option<&OracleConfig> {
        match self {
            Self::Postgres(_) => None,
            Self::Sqlite(_) => None,
            Self::Oracle(config) => Some(config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqliteConfig {
    pub path: PathBuf,
    pub busy_timeout: Duration,
}

impl SqliteConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            path: optional_path_env("OA_SQLITE_PATH")?
                .unwrap_or(default_local_archive_root()?.join("open_archive.db")),
            busy_timeout: optional_duration_env_ms("OA_SQLITE_BUSY_TIMEOUT_MS")?
                .unwrap_or_else(|| Duration::from_secs(30)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorStoreConfig {
    Disabled,
    PostgresPgVector,
    Qdrant(Box<QdrantConfig>),
}

impl VectorStoreConfig {
    pub fn from_env(relational_store: &RelationalStoreConfig) -> ConfigResult<Self> {
        let provider = env::var("OA_VECTOR_STORE").unwrap_or_else(|_| match relational_store {
            RelationalStoreConfig::Postgres(_) => "postgres".to_string(),
            RelationalStoreConfig::Sqlite(_) => "qdrant".to_string(),
            RelationalStoreConfig::Oracle(_) => "disabled".to_string(),
        });
        match provider.as_str() {
            "disabled" => Ok(Self::Disabled),
            "postgres" => Ok(Self::PostgresPgVector),
            "qdrant" => Ok(Self::Qdrant(Box::new(QdrantConfig::from_env()?))),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_VECTOR_STORE",
                value: provider,
                expected: "disabled, postgres, qdrant",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QdrantConfig {
    pub url: String,
    pub collection_name: String,
    pub request_timeout: Duration,
    pub exact: bool,
    pub managed: ManagedQdrantConfig,
}

impl QdrantConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            url: env::var("OA_QDRANT_URL").unwrap_or_else(|_| "http://127.0.0.1:6333".to_string()),
            collection_name: env::var("OA_QDRANT_COLLECTION")
                .unwrap_or_else(|_| "oa_derived_object_embeddings".to_string()),
            request_timeout: optional_duration_env_ms("OA_QDRANT_TIMEOUT_MS")?
                .unwrap_or_else(|| Duration::from_secs(30)),
            exact: bool_env("OA_QDRANT_EXACT")?.unwrap_or(true),
            managed: ManagedQdrantConfig::from_env()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedQdrantConfig {
    pub enabled: bool,
    pub version: String,
    pub install_root: PathBuf,
    pub storage_path: PathBuf,
    pub log_path: PathBuf,
    pub startup_timeout: Duration,
    pub binary_path: Option<PathBuf>,
}

impl ManagedQdrantConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let install_root = optional_path_env("OA_QDRANT_INSTALL_ROOT")?
            .unwrap_or(default_local_archive_root()?.join("qdrant"));
        Ok(Self {
            enabled: bool_env("OA_QDRANT_MANAGED")?.unwrap_or(true),
            version: optional_trimmed_env("OA_QDRANT_VERSION")
                .unwrap_or_else(|| "1.17.1".to_string()),
            storage_path: optional_path_env("OA_QDRANT_STORAGE_PATH")?
                .unwrap_or_else(|| install_root.join("storage")),
            log_path: optional_path_env("OA_QDRANT_LOG_PATH")?
                .unwrap_or_else(|| install_root.join("qdrant.log")),
            startup_timeout: optional_duration_env_ms("OA_QDRANT_STARTUP_TIMEOUT_MS")?
                .unwrap_or_else(|| Duration::from_secs(30)),
            binary_path: optional_path_env("OA_QDRANT_BINARY_PATH")?,
            install_root,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostgresConfig {
    pub connection_string: String,
}

impl PostgresConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let connection_string = env::var("OA_POSTGRES_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .map_err(|_| ConfigError::MissingEnv {
                key: "OA_POSTGRES_URL",
            })?;

        Ok(Self { connection_string })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    LocalFs(LocalFsObjectStoreConfig),
    S3Compatible(S3CompatibleObjectStoreConfig),
}

impl ObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_OBJECT_STORE").unwrap_or_else(|_| "local_fs".to_string());
        match provider.as_str() {
            "local_fs" => Ok(Self::LocalFs(LocalFsObjectStoreConfig::from_env()?)),
            "s3" => Ok(Self::S3Compatible(
                S3CompatibleObjectStoreConfig::from_env()?
            )),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OBJECT_STORE",
                value: provider,
                expected: "local_fs, s3",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFsObjectStoreConfig {
    pub root: PathBuf,
}

impl LocalFsObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let root = optional_path_env("OA_OBJECT_STORE_ROOT")?
            .unwrap_or(default_local_archive_root()?.join("objects"));

        Ok(Self { root })
    }
}

fn default_local_archive_root() -> ConfigResult<PathBuf> {
    Ok(home_dir()?.join(".open_archive"))
}

fn home_dir() -> ConfigResult<PathBuf> {
    Ok(PathBuf::from(env::var("HOME").map_err(|_| {
        ConfigError::MissingEnvWithDependency { key: "HOME" }
    })?))
}

fn optional_path_env(key: &'static str) -> ConfigResult<Option<PathBuf>> {
    optional_trimmed_env(key)
        .map(|value| expand_home_path(&value))
        .transpose()
}

pub fn expand_home_path(value: &str) -> ConfigResult<PathBuf> {
    let Some(expanded) = expand_with_home(value)? else {
        return Ok(PathBuf::from(value));
    };
    Ok(PathBuf::from(expanded))
}

fn expand_with_home(value: &str) -> ConfigResult<Option<String>> {
    let Some(home) = home_dir_string_if_needed(value)? else {
        return Ok(None);
    };
    Ok(Some(
        if value == "~" || value == "$HOME" || value == "${HOME}" {
            home
        } else if let Some(rest) = value.strip_prefix("~/") {
            format!("{home}/{rest}")
        } else if let Some(rest) = value.strip_prefix("$HOME/") {
            format!("{home}/{rest}")
        } else if let Some(rest) = value.strip_prefix("${HOME}/") {
            format!("{home}/{rest}")
        } else {
            home
        },
    ))
}

fn home_dir_string_if_needed(value: &str) -> ConfigResult<Option<String>> {
    if matches!(value, "~" | "$HOME" | "${HOME}")
        || value.starts_with("~/")
        || value.starts_with("$HOME/")
        || value.starts_with("${HOME}/")
    {
        let home = dirs::home_dir().ok_or(ConfigError::MissingEnvWithDependency { key: "HOME" })?;
        Ok(Some(home.to_string_lossy().to_string()))
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleObjectStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub key_prefix: Option<String>,
    pub url_style: S3UrlStyle,
}

impl S3CompatibleObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            endpoint: required_env("OA_S3_ENDPOINT")?,
            region: required_env("OA_S3_REGION")?,
            bucket: required_env("OA_S3_BUCKET")?,
            access_key_id: required_env("OA_S3_ACCESS_KEY_ID")?,
            secret_access_key: required_env("OA_S3_SECRET_ACCESS_KEY")?,
            key_prefix: optional_trimmed_env("OA_S3_KEY_PREFIX"),
            url_style: S3UrlStyle::from_env()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3UrlStyle {
    Path,
    VirtualHost,
}

impl S3UrlStyle {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_S3_URL_STYLE").unwrap_or_else(|_| "path".to_string());
        match value.as_str() {
            "path" => Ok(Self::Path),
            "virtual_host" => Ok(Self::VirtualHost),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_S3_URL_STYLE",
                value,
                expected: "path, virtual_host",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferenceConfig {
    Stub,
    OpenAi(OpenAiConfig),
    Gemini(GeminiConfig),
    Anthropic(AnthropicConfig),
    Grok(GrokConfig),
    Oci(OciConfig),
}

impl InferenceConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_MODEL_PROVIDER").unwrap_or_else(|_| "stub".to_string());
        Self::from_named_provider("OA_MODEL_PROVIDER", provider)
    }

    fn from_named_provider(key: &'static str, provider: String) -> ConfigResult<Self> {
        match provider.as_str() {
            "stub" => Ok(Self::Stub),
            "openai" => Ok(Self::OpenAi(OpenAiConfig::from_env()?)),
            "gemini" => Ok(Self::Gemini(GeminiConfig::from_env()?)),
            "anthropic" => Ok(Self::Anthropic(AnthropicConfig::from_env()?)),
            "grok" => Ok(Self::Grok(GrokConfig::from_env()?)),
            "oci" => Ok(Self::Oci(OciConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key,
                value: provider,
                expected: "stub, openai, gemini, anthropic, grok, oci",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
    pub batch_enabled: bool,
    pub batch_max_jobs: usize,
    pub batch_max_bytes: usize,
    pub batch_poll_interval: Duration,
}

impl GeminiConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let api_key = required_env("OA_GEMINI_API_KEY")?;
        Self::from_api_key_and_models(api_key, heavy_model, fast_model)
    }

    /// Create config from API key and model names (used when loading from secrets).
    pub fn from_api_key_and_models(
        api_key: String,
        heavy_model: String,
        fast_model: String,
    ) -> ConfigResult<Self> {
        Ok(Self {
            api_key,
            base_url: env::var("OA_GEMINI_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".to_string()),
            max_output_tokens: env::var("OA_GEMINI_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            repair_max_output_tokens: env::var("OA_GEMINI_REPAIR_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(8000),
            heavy_model,
            fast_model,
            batch_enabled: env::var("OA_GEMINI_BATCH_ENABLED")
                .ok()
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            batch_max_jobs: positive_usize_env("OA_GEMINI_BATCH_MAX_JOBS")?.unwrap_or(16),
            batch_max_bytes: positive_usize_env("OA_GEMINI_BATCH_MAX_BYTES")?.unwrap_or(1_500_000),
            batch_poll_interval: optional_duration_env_ms("OA_GEMINI_BATCH_POLL_INTERVAL_MS")?
                .unwrap_or(Duration::from_secs(5)),
        })
    }

    /// Create config from API key and single model (used when loading from file config).
    pub fn from_api_key_and_model(api_key: String, model: String) -> ConfigResult<Self> {
        Self::from_api_key_and_models(api_key, model.clone(), model)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub reasoning_effort_override: OpenAiReasoningEffort,
    pub heavy_model: String,
    pub fast_model: String,
}

impl OpenAiConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let api_key = required_env("OA_OPENAI_API_KEY")?;
        Self::from_api_key_and_models(api_key, heavy_model, fast_model)
    }

    /// Create config from API key and model names (used when loading from secrets).
    pub fn from_api_key_and_models(
        api_key: String,
        heavy_model: String,
        fast_model: String,
    ) -> ConfigResult<Self> {
        Ok(Self {
            api_key,
            base_url: env::var("OA_OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            max_output_tokens: env::var("OA_OPENAI_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            repair_max_output_tokens: env::var("OA_OPENAI_REPAIR_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(8000),
            reasoning_effort_override: OpenAiReasoningEffort::from_env()?,
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiReasoningEffort {
    Auto,
    None,
    Minimal,
    Low,
    Medium,
    High,
}

impl OpenAiReasoningEffort {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_OPENAI_REASONING_EFFORT").unwrap_or_else(|_| "auto".to_string());
        match value.as_str() {
            "auto" => Ok(Self::Auto),
            "none" => Ok(Self::None),
            "minimal" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OPENAI_REASONING_EFFORT",
                value,
                expected: "auto, none, minimal, low, medium, high",
            }),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnthropicConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
}

impl AnthropicConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let api_key = required_env("OA_ANTHROPIC_API_KEY")?;
        Self::from_api_key_and_models(api_key, heavy_model, fast_model)
    }

    /// Create config from API key and model names (used when loading from secrets).
    pub fn from_api_key_and_models(
        api_key: String,
        heavy_model: String,
        fast_model: String,
    ) -> ConfigResult<Self> {
        Ok(Self {
            api_key,
            base_url: env::var("OA_ANTHROPIC_BASE_URL")
                .unwrap_or_else(|_| "https://api.anthropic.com/v1".to_string()),
            max_output_tokens: env::var("OA_ANTHROPIC_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
}

impl GrokConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        Ok(Self {
            api_key: required_env("OA_GROK_API_KEY")?,
            base_url: env::var("OA_GROK_BASE_URL")
                .unwrap_or_else(|_| "https://api.x.ai/v1".to_string()),
            max_output_tokens: env::var("OA_GROK_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            repair_max_output_tokens: env::var("OA_GROK_REPAIR_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(8000),
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciConfig {
    pub region: String,
    pub compartment_id: String,
    pub cli_path: String,
    pub profile: Option<String>,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
}

impl OciConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let max_output_tokens = env::var("OA_OCI_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(4000);
        let repair_max_output_tokens = env::var("OA_OCI_REPAIR_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(8000);
        Ok(Self {
            region: required_env("OA_OCI_REGION")?,
            compartment_id: required_env("OA_OCI_COMPARTMENT_ID")?,
            cli_path: env::var("OA_OCI_CLI_PATH").unwrap_or_else(|_| "oci".to_string()),
            profile: optional_trimmed_env("OA_OCI_PROFILE"),
            max_output_tokens,
            repair_max_output_tokens,
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OracleConfig {
    pub connect_string: String,
    pub username: String,
    pub password: String,
    pub call_timeout: Option<Duration>,
    pub pool: ConnectionPoolConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionPoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub connection_increment: u32,
    pub stmt_cache_size: u32,
    pub get_timeout: Duration,
    pub ping_interval: Duration,
}

impl OracleConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let connect_string =
            env::var("OA_ORACLE_CONNECT_STRING").map_err(|_| ConfigError::MissingEnv {
                key: "OA_ORACLE_CONNECT_STRING",
            })?;
        let username = env::var("OA_ORACLE_USERNAME").map_err(|_| ConfigError::MissingEnv {
            key: "OA_ORACLE_USERNAME",
        })?;
        let password = env::var("OA_ORACLE_PASSWORD").map_err(|_| ConfigError::MissingEnv {
            key: "OA_ORACLE_PASSWORD",
        })?;

        Ok(Self {
            connect_string,
            username,
            password,
            call_timeout: optional_duration_env_ms("OA_ORACLE_CALL_TIMEOUT_MS")?,
            pool: ConnectionPoolConfig::from_env()?,
        })
    }
}

impl ConnectionPoolConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            min_connections: positive_u32_env("OA_DB_POOL_MIN")?.unwrap_or(1),
            max_connections: positive_u32_env("OA_DB_POOL_MAX")?.unwrap_or(8),
            connection_increment: positive_u32_env("OA_DB_POOL_INCREMENT")?.unwrap_or(1),
            stmt_cache_size: positive_u32_env("OA_DB_POOL_STMT_CACHE_SIZE")?.unwrap_or(50),
            get_timeout: optional_duration_env_ms("OA_DB_POOL_GET_TIMEOUT_MS")?
                .unwrap_or_else(|| Duration::from_secs(30)),
            ping_interval: optional_duration_env_ms("OA_DB_POOL_PING_INTERVAL_MS")?
                .unwrap_or_else(|| Duration::from_secs(60)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind_addr: String,
    pub request_worker_count: usize,
    pub enrichment_worker_count: usize,
    pub enrichment_poll_interval_ms: u64,
}

impl HttpConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let bind_addr = env::var("OA_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let request_worker_count = positive_usize_env("OA_HTTP_REQUEST_WORKERS")?.unwrap_or(4);
        let enrichment_worker_count = optional_usize_env("OA_ENRICHMENT_WORKERS")?.unwrap_or(1);
        let enrichment_poll_interval_ms = env::var("OA_ENRICHMENT_POLL_INTERVAL_MS")
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key: "OA_ENRICHMENT_POLL_INTERVAL_MS",
                        value: s,
                    })
            })
            .unwrap_or(Ok(500))?;

        Ok(Self {
            bind_addr,
            request_worker_count,
            enrichment_worker_count,
            enrichment_poll_interval_ms,
        })
    }
}

fn positive_u32_env(key: &'static str) -> ConfigResult<Option<u32>> {
    match env::var(key) {
        Ok(raw) => {
            let value = raw
                .parse::<u32>()
                .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                    key,
                    value: raw.clone(),
                })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

fn required_env(key: &'static str) -> ConfigResult<String> {
    env::var(key).map_err(|_| ConfigError::MissingEnv { key })
}

fn optional_trimmed_env(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_duration_env_ms(key: &'static str) -> ConfigResult<Option<Duration>> {
    match env::var(key) {
        Ok(raw) => {
            let value = raw
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                    key,
                    value: raw.clone(),
                })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(Duration::from_millis(value)))
        }
        Err(_) => Ok(None),
    }
}

fn bool_env(key: &'static str) -> ConfigResult<Option<bool>> {
    match env::var(key) {
        Ok(raw) => match raw.as_str() {
            "true" | "1" => Ok(Some(true)),
            "false" | "0" => Ok(Some(false)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key,
                value: raw,
                expected: "true, false, 1, 0",
            }),
        },
        Err(_) => Ok(None),
    }
}

fn positive_usize_env(key: &'static str) -> ConfigResult<Option<usize>> {
    match env::var(key) {
        Ok(raw) => {
            let value =
                raw.parse::<usize>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key,
                        value: raw.clone(),
                    })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

fn optional_usize_env(key: &'static str) -> ConfigResult<Option<usize>> {
    match env::var(key) {
        Ok(raw) => {
            let value =
                raw.parse::<usize>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key,
                        value: raw.clone(),
                    })?;
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageConfig {
    pub batch_size: usize,
    pub max_concurrent_batches: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPipelineModeConfig {
    pub extract_workers: usize,
    pub reconcile_workers: usize,
    pub embedding_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPipelineModeConfig {
    pub extract_workers: usize,
    pub extract: StageConfig,
    pub reconcile_workers: usize,
    pub reconcile: StageConfig,
    pub embedding_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionChunkingConfig {
    pub max_segments_per_chunk: usize,
    pub chunk_overlap_segments: usize,
    pub max_chars_per_chunk: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentPipelineConfig {
    pub poll_interval: Duration,
    pub direct: DirectPipelineModeConfig,
    pub batch: BatchPipelineModeConfig,
    pub chunking: ExtractionChunkingConfig,
}

impl EnrichmentPipelineConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let shared_worker_default = optional_usize_env("OA_ENRICHMENT_WORKERS")?.unwrap_or(1);
        let direct_extract_workers =
            positive_usize_env("OA_DIRECT_EXTRACT_WORKERS")?.unwrap_or(shared_worker_default);
        let direct_reconcile_workers =
            positive_usize_env("OA_DIRECT_RECONCILE_WORKERS")?.unwrap_or(shared_worker_default);
        let direct_embedding_workers =
            positive_usize_env("OA_DIRECT_EMBEDDING_WORKERS")?.unwrap_or(1);
        let batch_extract_workers =
            positive_usize_env("OA_BATCH_EXTRACT_WORKERS")?.unwrap_or(shared_worker_default);
        let batch_reconcile_workers =
            positive_usize_env("OA_BATCH_RECONCILE_WORKERS")?.unwrap_or(shared_worker_default);
        let batch_embedding_workers =
            positive_usize_env("OA_BATCH_EMBEDDING_WORKERS")?.unwrap_or(1);
        Ok(Self {
            poll_interval: optional_duration_env_ms("OA_ENRICHMENT_POLL_INTERVAL_MS")?
                .unwrap_or(Duration::from_millis(2000)),
            direct: DirectPipelineModeConfig {
                extract_workers: direct_extract_workers,
                reconcile_workers: direct_reconcile_workers,
                embedding_workers: direct_embedding_workers,
            },
            batch: BatchPipelineModeConfig {
                extract_workers: batch_extract_workers,
                extract: StageConfig {
                    batch_size: positive_usize_env("OA_BATCH_EXTRACT_BATCH_SIZE")?.unwrap_or(5),
                    max_concurrent_batches: positive_usize_env("OA_BATCH_EXTRACT_MAX_CONCURRENT")?
                        .unwrap_or(3),
                },
                reconcile_workers: batch_reconcile_workers,
                reconcile: StageConfig {
                    batch_size: positive_usize_env("OA_BATCH_RECONCILE_BATCH_SIZE")?.unwrap_or(5),
                    max_concurrent_batches: positive_usize_env(
                        "OA_BATCH_RECONCILE_MAX_CONCURRENT",
                    )?
                    .unwrap_or(2),
                },
                embedding_workers: batch_embedding_workers,
            },
            chunking: ExtractionChunkingConfig {
                max_segments_per_chunk: positive_usize_env("OA_EXTRACT_CHUNK_SEGMENTS")?
                    .unwrap_or(20),
                chunk_overlap_segments: optional_usize_env("OA_EXTRACT_CHUNK_OVERLAP")?
                    .unwrap_or(4),
                max_chars_per_chunk: positive_usize_env("OA_EXTRACT_CHUNK_MAX_CHARS")?
                    .unwrap_or(25_000),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn clear_test_env() {
        for key in [
            "OA_RELATIONAL_STORE",
            "OA_OBJECT_STORE",
            "OA_S3_ENDPOINT",
            "OA_S3_REGION",
            "OA_S3_BUCKET",
            "OA_S3_ACCESS_KEY_ID",
            "OA_S3_SECRET_ACCESS_KEY",
            "OA_S3_KEY_PREFIX",
            "OA_S3_URL_STYLE",
            "OA_MODEL_PROVIDER",
            "OA_HEAVY_MODEL",
            "OA_FAST_MODEL",
            "OA_EMBEDDING_PROVIDER",
            "OA_EMBEDDING_MODEL",
            "OA_EMBEDDING_DIMENSIONS",
            "OA_INFERENCE_MODE",
            "OA_OPENAI_API_KEY",
            "OA_OPENAI_BASE_URL",
            "OA_OPENAI_MAX_OUTPUT_TOKENS",
            "OA_OPENAI_REPAIR_MAX_OUTPUT_TOKENS",
            "OA_OPENAI_REASONING_EFFORT",
            "OA_STUB_EMBEDDING_MODEL",
            "OA_STUB_EMBEDDING_DIMENSIONS",
            "OA_GEMINI_API_KEY",
            "OA_GEMINI_BASE_URL",
            "OA_GEMINI_MAX_OUTPUT_TOKENS",
            "OA_GEMINI_REPAIR_MAX_OUTPUT_TOKENS",
            "OA_ANTHROPIC_API_KEY",
            "OA_ANTHROPIC_BASE_URL",
            "OA_ANTHROPIC_MAX_OUTPUT_TOKENS",
            "OA_GROK_API_KEY",
            "OA_GROK_BASE_URL",
            "OA_GROK_MAX_OUTPUT_TOKENS",
            "OA_GROK_REPAIR_MAX_OUTPUT_TOKENS",
            "OA_OCI_REGION",
            "OA_OCI_COMPARTMENT_ID",
            "OA_OCI_CLI_PATH",
            "OA_OCI_PROFILE",
            "OA_OCI_MAX_OUTPUT_TOKENS",
            "OA_OCI_REPAIR_MAX_OUTPUT_TOKENS",
            "OA_ORACLE_CONNECT_STRING",
            "OA_ORACLE_USERNAME",
            "OA_ORACLE_PASSWORD",
            "OA_ORACLE_CALL_TIMEOUT_MS",
            "OA_DB_POOL_MIN",
            "OA_DB_POOL_MAX",
            "OA_DB_POOL_INCREMENT",
            "OA_DB_POOL_STMT_CACHE_SIZE",
            "OA_DB_POOL_GET_TIMEOUT_MS",
            "OA_DB_POOL_PING_INTERVAL_MS",
            "OA_HTTP_BIND",
            "OA_HTTP_REQUEST_WORKERS",
            "OA_ENRICHMENT_WORKERS",
            "OA_DIRECT_EXTRACT_WORKERS",
            "OA_DIRECT_RECONCILE_WORKERS",
            "OA_DIRECT_EMBEDDING_WORKERS",
            "OA_BATCH_EXTRACT_WORKERS",
            "OA_BATCH_RECONCILE_WORKERS",
            "OA_BATCH_EMBEDDING_WORKERS",
            "OA_BATCH_EXTRACT_BATCH_SIZE",
            "OA_BATCH_EXTRACT_MAX_CONCURRENT",
            "OA_BATCH_RECONCILE_BATCH_SIZE",
            "OA_BATCH_RECONCILE_MAX_CONCURRENT",
            "OA_ENRICHMENT_POLL_INTERVAL_MS",
            "OA_EXTRACT_CHUNK_SEGMENTS",
            "OA_EXTRACT_CHUNK_OVERLAP",
            "OA_EXTRACT_CHUNK_MAX_CHARS",
            "OA_GEMINI_BATCH_ENABLED",
            "OA_GEMINI_BATCH_MAX_JOBS",
            "OA_GEMINI_BATCH_MAX_BYTES",
            "OA_GEMINI_BATCH_POLL_INTERVAL_MS",
            "OA_POSTGRES_URL",
            "OA_SQLITE_PATH",
            "OA_SQLITE_BUSY_TIMEOUT_MS",
            "OA_VECTOR_STORE",
            "OA_QDRANT_URL",
            "OA_QDRANT_COLLECTION",
            "OA_QDRANT_TIMEOUT_MS",
            "OA_QDRANT_EXACT",
            "OA_QDRANT_MANAGED",
            "OA_QDRANT_VERSION",
            "OA_QDRANT_INSTALL_ROOT",
            "OA_QDRANT_STORAGE_PATH",
            "OA_QDRANT_LOG_PATH",
            "OA_QDRANT_STARTUP_TIMEOUT_MS",
            "OA_QDRANT_BINARY_PATH",
            "OA_OPENAI_REPAIR_MAX_OUTPUT_TOKENS",
            "OA_OBJECT_STORE_ROOT",
            "DATABASE_URL",
            "OA_INFERENCE_PROVIDER",
            "OA_RECONCILE_INFERENCE_PROVIDER",
            "OA_OPENAI_STANDARD_MODEL",
            "OA_OPENAI_QUALITY_MODEL",
            "OA_OPENAI_RECONCILE_STANDARD_MODEL",
            "OA_OPENAI_RECONCILE_QUALITY_MODEL",
            "OA_OPENAI_EMBEDDING_MODEL",
            "OA_OPENAI_EMBEDDING_DIMENSIONS",
            "OA_GEMINI_STANDARD_MODEL",
            "OA_GEMINI_QUALITY_MODEL",
            "OA_GEMINI_FORMAT_STANDARD_MODEL",
            "OA_GEMINI_FORMAT_QUALITY_MODEL",
            "OA_GEMINI_RECONCILE_STANDARD_MODEL",
            "OA_GEMINI_RECONCILE_QUALITY_MODEL",
            "OA_GEMINI_EMBEDDING_MODEL",
            "OA_GEMINI_EMBEDDING_DIMENSIONS",
            "OA_ANTHROPIC_STANDARD_MODEL",
            "OA_ANTHROPIC_QUALITY_MODEL",
            "OA_ANTHROPIC_RECONCILE_STANDARD_MODEL",
            "OA_ANTHROPIC_RECONCILE_QUALITY_MODEL",
            "OA_GROK_STANDARD_MODEL",
            "OA_GROK_QUALITY_MODEL",
            "OA_GROK_RECONCILE_STANDARD_MODEL",
            "OA_GROK_RECONCILE_QUALITY_MODEL",
            "OA_OCI_STANDARD_MODEL",
            "OA_OCI_QUALITY_MODEL",
            "OA_OCI_RECONCILE_STANDARD_MODEL",
            "OA_OCI_RECONCILE_QUALITY_MODEL",
            "OA_EXTRACT_WORKERS",
            "OA_EXTRACT_BATCH_SIZE",
            "OA_EXTRACT_MAX_CONCURRENT",
            "OA_RECONCILE_WORKERS",
            "OA_RECONCILE_BATCH_SIZE",
            "OA_RECONCILE_MAX_CONCURRENT",
            "OA_EMBEDDING_WORKERS",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn app_config_defaults_to_sqlite_qdrant_and_stubbed_future_providers() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("HOME", "/tmp/open-archive-config-defaults");

        let config = AppConfig::from_env().expect("app config should load");

        assert!(matches!(
            config.relational_store,
            RelationalStoreConfig::Sqlite(_)
        ));
        let RelationalStoreConfig::Sqlite(sqlite) = config.relational_store else {
            panic!("expected sqlite config");
        };
        assert_eq!(
            sqlite.path,
            PathBuf::from("/tmp/open-archive-config-defaults/.open_archive/open_archive.db")
        );
        let ObjectStoreConfig::LocalFs(local_fs) = config.object_store else {
            panic!("expected local fs config");
        };
        assert_eq!(
            local_fs.root,
            PathBuf::from("/tmp/open-archive-config-defaults/.open_archive/objects")
        );
        let VectorStoreConfig::Qdrant(qdrant) = config.vector_store else {
            panic!("expected qdrant config");
        };
        assert_eq!(qdrant.url, "http://127.0.0.1:6333");
        assert!(qdrant.managed.enabled);
        assert_eq!(qdrant.managed.version, "1.17.1");
        assert_eq!(
            qdrant.managed.install_root,
            PathBuf::from("/tmp/open-archive-config-defaults/.open_archive/qdrant")
        );
        assert_eq!(config.inference, InferenceConfig::Stub);
        assert_eq!(config.embeddings, EmbeddingConfig::Disabled);
        assert_eq!(config.inference_mode, InferenceExecutionMode::Batch);
    }

    #[test]
    fn postgres_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "postgres");
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );

        let config = AppConfig::from_env().expect("postgres provider should load");
        assert!(matches!(
            config.relational_store,
            RelationalStoreConfig::Postgres(_)
        ));
    }

    #[test]
    fn sqlite_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "sqlite");
        std::env::set_var("OA_SQLITE_PATH", "/tmp/open_archive.sqlite");

        let config = AppConfig::from_env().expect("sqlite provider should load");
        let RelationalStoreConfig::Sqlite(sqlite) = config.relational_store else {
            panic!("expected sqlite config");
        };

        assert_eq!(sqlite.path, PathBuf::from("/tmp/open_archive.sqlite"));
        assert_eq!(sqlite.busy_timeout, Duration::from_secs(30));
        assert!(matches!(config.vector_store, VectorStoreConfig::Qdrant(_)));
    }

    #[test]
    fn sqlite_and_local_fs_paths_expand_home_prefixes() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("HOME", "/tmp/open-archive-home");
        std::env::set_var("OA_SQLITE_PATH", "${HOME}/db/open_archive.sqlite");
        std::env::set_var("OA_OBJECT_STORE_ROOT", "~/objects");

        let config = AppConfig::from_env().expect("path expansion should work");
        let RelationalStoreConfig::Sqlite(sqlite) = config.relational_store else {
            panic!("expected sqlite config");
        };
        let ObjectStoreConfig::LocalFs(local_fs) = config.object_store else {
            panic!("expected local fs config");
        };

        assert_eq!(
            sqlite.path,
            PathBuf::from("/tmp/open-archive-home/db/open_archive.sqlite")
        );
        assert_eq!(
            local_fs.root,
            PathBuf::from("/tmp/open-archive-home/objects")
        );
    }

    #[test]
    fn oracle_provider_loads_typed_pool_defaults() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "oracle");
        std::env::set_var("OA_ORACLE_CONNECT_STRING", "localhost:1521/FREEPDB1");
        std::env::set_var("OA_ORACLE_USERNAME", "oracle_user");
        std::env::set_var("OA_ORACLE_PASSWORD", "oracle_pass");

        let config = AppConfig::from_env().expect("oracle provider should load");
        let RelationalStoreConfig::Oracle(config) = config.relational_store else {
            panic!("expected oracle config");
        };

        assert_eq!(config.connect_string, "localhost:1521/FREEPDB1");
        assert_eq!(config.call_timeout, None);
        assert_eq!(config.pool.min_connections, 1);
        assert_eq!(config.pool.max_connections, 8);
        assert_eq!(config.pool.connection_increment, 1);
        assert_eq!(config.pool.stmt_cache_size, 50);
        assert_eq!(config.pool.get_timeout, Duration::from_secs(30));
        assert_eq!(config.pool.ping_interval, Duration::from_secs(60));
    }

    #[test]
    fn invalid_relational_store_provider_is_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "bogus");
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );

        let error = AppConfig::from_env().expect_err("provider should be rejected");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                ..
            }
        ));
    }

    #[test]
    fn qdrant_vector_provider_loads_with_defaults() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_VECTOR_STORE", "qdrant");

        let config = AppConfig::from_env().expect("qdrant vector provider should load");
        let VectorStoreConfig::Qdrant(qdrant) = config.vector_store else {
            panic!("expected qdrant vector config");
        };

        assert_eq!(qdrant.url, "http://127.0.0.1:6333");
        assert_eq!(qdrant.collection_name, "oa_derived_object_embeddings");
        assert_eq!(qdrant.request_timeout, Duration::from_secs(30));
        assert!(qdrant.exact);
        assert!(qdrant.managed.enabled);
        assert_eq!(qdrant.managed.version, "1.17.1");
    }

    #[test]
    fn qdrant_managed_paths_expand_home_prefixes() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("HOME", "/tmp/open-archive-qdrant-home");
        std::env::set_var("OA_RELATIONAL_STORE", "sqlite");
        std::env::set_var("OA_QDRANT_INSTALL_ROOT", "${HOME}/managed-qdrant");
        std::env::set_var("OA_QDRANT_STORAGE_PATH", "~/managed-qdrant/storage");
        std::env::set_var("OA_QDRANT_LOG_PATH", "$HOME/managed-qdrant/qdrant.log");
        std::env::set_var("OA_QDRANT_BINARY_PATH", "${HOME}/bin/qdrant");

        let config = AppConfig::from_env().expect("qdrant config should load");
        let VectorStoreConfig::Qdrant(qdrant) = config.vector_store else {
            panic!("expected qdrant vector config");
        };

        assert_eq!(
            qdrant.managed.install_root,
            PathBuf::from("/tmp/open-archive-qdrant-home/managed-qdrant")
        );
        assert_eq!(
            qdrant.managed.storage_path,
            PathBuf::from("/tmp/open-archive-qdrant-home/managed-qdrant/storage")
        );
        assert_eq!(
            qdrant.managed.log_path,
            PathBuf::from("/tmp/open-archive-qdrant-home/managed-qdrant/qdrant.log")
        );
        assert_eq!(
            qdrant.managed.binary_path,
            Some(PathBuf::from("/tmp/open-archive-qdrant-home/bin/qdrant"))
        );
    }

    #[test]
    fn s3_object_store_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_OBJECT_STORE", "s3");
        std::env::set_var("OA_S3_ENDPOINT", "http://localhost:9000");
        std::env::set_var("OA_S3_REGION", "us-east-1");
        std::env::set_var("OA_S3_BUCKET", "openarchive");
        std::env::set_var("OA_S3_ACCESS_KEY_ID", "openarchive");
        std::env::set_var("OA_S3_SECRET_ACCESS_KEY", "openarchive-secret");
        std::env::set_var("OA_S3_KEY_PREFIX", "objects");

        let config = AppConfig::from_env().expect("s3 object-store provider should load");
        let ObjectStoreConfig::S3Compatible(config) = config.object_store else {
            panic!("expected s3 object-store config");
        };

        assert_eq!(config.endpoint, "http://localhost:9000");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.bucket, "openarchive");
        assert_eq!(config.key_prefix.as_deref(), Some("objects"));
        assert_eq!(config.url_style, S3UrlStyle::Path);
    }

    #[test]
    fn openai_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "openai");
        std::env::set_var("OA_OPENAI_API_KEY", "test-key");
        std::env::set_var("OA_OPENAI_REASONING_EFFORT", "none");
        std::env::set_var("OA_HEAVY_MODEL", "gpt-4.1");
        std::env::set_var("OA_FAST_MODEL", "gpt-4.1-mini");

        let config = AppConfig::from_env().expect("openai inference provider should load");
        let InferenceConfig::OpenAi(config) = config.inference else {
            panic!("expected openai config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.openai.com/v1");
        assert_eq!(
            config.reasoning_effort_override,
            OpenAiReasoningEffort::None
        );
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.repair_max_output_tokens, 8000);
        assert_eq!(config.heavy_model, "gpt-4.1");
        assert_eq!(config.fast_model, "gpt-4.1-mini");
    }

    #[test]
    fn openai_embedding_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_EMBEDDING_PROVIDER", "openai");
        std::env::set_var("OA_OPENAI_API_KEY", "test-key");
        std::env::set_var("OA_EMBEDDING_MODEL", "text-embedding-3-small");
        std::env::set_var("OA_EMBEDDING_DIMENSIONS", "1536");

        let config = AppConfig::from_env().expect("embedding config should load");
        let EmbeddingConfig::OpenAi(config) = config.embeddings else {
            panic!("expected openai embedding config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.openai.com/v1");
        assert_eq!(config.embedding_model, "text-embedding-3-small");
        assert_eq!(config.embedding_dimensions, 1536);
    }

    #[test]
    fn gemini_embedding_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_EMBEDDING_PROVIDER", "gemini");
        std::env::set_var("OA_GEMINI_API_KEY", "test-key");
        std::env::set_var("OA_EMBEDDING_MODEL", "gemini-embedding-001");
        std::env::set_var("OA_EMBEDDING_DIMENSIONS", "3072");

        let config = AppConfig::from_env().expect("embedding config should load");
        let EmbeddingConfig::Gemini(config) = config.embeddings else {
            panic!("expected gemini embedding config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(
            config.base_url,
            "https://generativelanguage.googleapis.com/v1beta"
        );
        assert_eq!(config.embedding_model, "gemini-embedding-001");
        assert_eq!(config.embedding_dimensions, 3072);
    }

    #[test]
    fn enrichment_pipeline_defaults_include_windowed_chunking_profile() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );

        let config =
            EnrichmentPipelineConfig::from_env().expect("pipeline config should load defaults");

        assert_eq!(config.chunking.max_segments_per_chunk, 20);
        assert_eq!(config.chunking.chunk_overlap_segments, 4);
        assert_eq!(config.chunking.max_chars_per_chunk, 25_000);
        assert_eq!(config.direct.embedding_workers, 1);
        assert_eq!(config.batch.embedding_workers, 1);
    }

    #[test]
    fn enrichment_pipeline_supports_mode_specific_worker_overrides() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_DIRECT_EXTRACT_WORKERS", "7");
        std::env::set_var("OA_DIRECT_RECONCILE_WORKERS", "6");
        std::env::set_var("OA_DIRECT_EMBEDDING_WORKERS", "4");
        std::env::set_var("OA_BATCH_EXTRACT_WORKERS", "1");
        std::env::set_var("OA_BATCH_EXTRACT_BATCH_SIZE", "1");
        std::env::set_var("OA_BATCH_EXTRACT_MAX_CONCURRENT", "60");
        std::env::set_var("OA_BATCH_RECONCILE_WORKERS", "1");
        std::env::set_var("OA_BATCH_RECONCILE_BATCH_SIZE", "1");
        std::env::set_var("OA_BATCH_RECONCILE_MAX_CONCURRENT", "60");
        std::env::set_var("OA_BATCH_EMBEDDING_WORKERS", "9");

        let config =
            EnrichmentPipelineConfig::from_env().expect("pipeline config should load overrides");

        assert_eq!(config.direct.extract_workers, 7);
        assert_eq!(config.direct.reconcile_workers, 6);
        assert_eq!(config.direct.embedding_workers, 4);
        assert_eq!(config.batch.extract_workers, 1);
        assert_eq!(config.batch.extract.batch_size, 1);
        assert_eq!(config.batch.extract.max_concurrent_batches, 60);
        assert_eq!(config.batch.reconcile_workers, 1);
        assert_eq!(config.batch.reconcile.batch_size, 1);
        assert_eq!(config.batch.reconcile.max_concurrent_batches, 60);
        assert_eq!(config.batch.embedding_workers, 9);
    }

    #[test]
    fn inference_execution_mode_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_INFERENCE_MODE", "direct");

        let config = AppConfig::from_env().expect("inference execution mode should load");
        assert_eq!(config.inference_mode, InferenceExecutionMode::Direct);
    }

    #[test]
    fn gemini_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "gemini");
        std::env::set_var("OA_GEMINI_API_KEY", "test-key");
        std::env::set_var("OA_HEAVY_MODEL", "gemini-3-flash-preview");
        std::env::set_var("OA_FAST_MODEL", "gemini-2.5-flash-lite");

        let config = AppConfig::from_env().expect("gemini inference provider should load");
        let InferenceConfig::Gemini(config) = config.inference else {
            panic!("expected gemini config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(
            config.base_url,
            "https://generativelanguage.googleapis.com/v1beta"
        );
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.repair_max_output_tokens, 8000);
        assert_eq!(config.heavy_model, "gemini-3-flash-preview");
        assert_eq!(config.fast_model, "gemini-2.5-flash-lite");
    }

    #[test]
    fn anthropic_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "anthropic");
        std::env::set_var("OA_ANTHROPIC_API_KEY", "test-key");
        std::env::set_var("OA_HEAVY_MODEL", "claude-sonnet-4-20250514");
        std::env::set_var("OA_FAST_MODEL", "claude-haiku-4-5");

        let config = AppConfig::from_env().expect("anthropic inference provider should load");
        let InferenceConfig::Anthropic(config) = config.inference else {
            panic!("expected anthropic config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.anthropic.com/v1");
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.heavy_model, "claude-sonnet-4-20250514");
        assert_eq!(config.fast_model, "claude-haiku-4-5");
    }

    #[test]
    fn grok_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "grok");
        std::env::set_var("OA_GROK_API_KEY", "test-key");
        std::env::set_var("OA_HEAVY_MODEL", "grok-4-fast-reasoning");
        std::env::set_var("OA_FAST_MODEL", "grok-4-fast-non-reasoning");

        let config = AppConfig::from_env().expect("grok inference provider should load");
        let InferenceConfig::Grok(config) = config.inference else {
            panic!("expected grok config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.x.ai/v1");
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.repair_max_output_tokens, 8000);
        assert_eq!(config.heavy_model, "grok-4-fast-reasoning");
        assert_eq!(config.fast_model, "grok-4-fast-non-reasoning");
    }

    #[test]
    fn oci_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "oci");
        std::env::set_var("OA_OCI_REGION", "us-chicago-1");
        std::env::set_var("OA_OCI_COMPARTMENT_ID", "ocid1.compartment.oc1..example");
        std::env::set_var("OA_HEAVY_MODEL", "meta.llama-3.3-70b-instruct");
        std::env::set_var("OA_FAST_MODEL", "cohere.command-a-03-2025");

        let config = AppConfig::from_env().expect("oci inference provider should load");
        let InferenceConfig::Oci(config) = config.inference else {
            panic!("expected oci config");
        };

        assert_eq!(config.region, "us-chicago-1");
        assert_eq!(config.compartment_id, "ocid1.compartment.oc1..example");
        assert_eq!(config.cli_path, "oci");
        assert_eq!(config.profile, None);
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.repair_max_output_tokens, 8000);
        assert_eq!(config.heavy_model, "meta.llama-3.3-70b-instruct");
        assert_eq!(config.fast_model, "cohere.command-a-03-2025");
    }

    #[test]
    fn oci_output_tokens_preserve_requested_values() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "oci");
        std::env::set_var("OA_OCI_REGION", "us-chicago-1");
        std::env::set_var("OA_OCI_COMPARTMENT_ID", "ocid1.compartment.oc1..example");
        std::env::set_var("OA_HEAVY_MODEL", "meta.llama-3.3-70b-instruct");
        std::env::set_var("OA_FAST_MODEL", "cohere.command-a-03-2025");
        std::env::set_var("OA_OCI_MAX_OUTPUT_TOKENS", "9000");
        std::env::set_var("OA_OCI_REPAIR_MAX_OUTPUT_TOKENS", "12000");

        let config = AppConfig::from_env().expect("oci config should load with requested tokens");
        let InferenceConfig::Oci(config) = config.inference else {
            panic!("expected oci config");
        };

        assert_eq!(config.max_output_tokens, 9000);
        assert_eq!(config.repair_max_output_tokens, 12000);
    }

    #[test]
    fn legacy_worker_envs_are_ignored() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_ENRICHMENT_WORKERS", "2");
        std::env::set_var("OA_EXTRACT_WORKERS", "9");
        std::env::set_var("OA_RECONCILE_WORKERS", "8");
        std::env::set_var("OA_EMBEDDING_WORKERS", "6");
        std::env::set_var("OA_EXTRACT_BATCH_SIZE", "5");
        std::env::set_var("OA_EXTRACT_MAX_CONCURRENT", "4");

        let config = EnrichmentPipelineConfig::from_env()
            .expect("pipeline config should ignore legacy envs");

        assert_eq!(config.direct.extract_workers, 2);
        assert_eq!(config.direct.reconcile_workers, 2);
        assert_eq!(config.direct.embedding_workers, 1);
        assert_eq!(config.batch.extract_workers, 2);
        assert_eq!(config.batch.extract.batch_size, 5);
        assert_eq!(config.batch.extract.max_concurrent_batches, 3);
        assert_eq!(config.batch.reconcile_workers, 2);
        assert_eq!(config.batch.reconcile.batch_size, 5);
        assert_eq!(config.batch.reconcile.max_concurrent_batches, 2);
        assert_eq!(config.batch.embedding_workers, 1);
    }

    #[test]
    fn legacy_model_envs_do_not_satisfy_new_contract() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_MODEL_PROVIDER", "gemini");
        std::env::set_var("OA_GEMINI_API_KEY", "gemini-key");
        std::env::set_var("OA_GEMINI_STANDARD_MODEL", "gemini-3-flash-preview");
        std::env::set_var(
            "OA_GEMINI_FORMAT_STANDARD_MODEL",
            "gemini-3.1-flash-lite-preview",
        );

        let error = AppConfig::from_env().expect_err("legacy model envs should not be accepted");

        assert!(matches!(
            error,
            ConfigError::MissingEnv {
                key: "OA_HEAVY_MODEL"
            }
        ));
    }
}
