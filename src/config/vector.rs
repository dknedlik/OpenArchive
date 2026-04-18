use crate::error::{ConfigError, ConfigResult};
use std::path::PathBuf;
use std::time::Duration;

use super::env::{
    bool_env, default_local_archive_root, optional_duration_env_ms, optional_path_env,
    optional_trimmed_env,
};
use super::relational::RelationalStoreConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorStoreConfig {
    Disabled,
    PostgresPgVector,
    Qdrant(Box<QdrantConfig>),
}

impl VectorStoreConfig {
    pub fn from_env(relational_store: &RelationalStoreConfig) -> ConfigResult<Self> {
        let provider =
            std::env::var("OA_VECTOR_STORE").unwrap_or_else(|_| match relational_store {
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
            url: std::env::var("OA_QDRANT_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:6333".to_string()),
            collection_name: std::env::var("OA_QDRANT_COLLECTION")
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
