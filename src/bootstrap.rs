//! Composition root for provider-backed services.
//!
//! `main` should assemble application-facing traits through this module rather
//! than naming concrete providers directly.

use std::sync::Arc;

use crate::config::{AppConfig, RelationalStoreConfig};
use crate::error::{ConfigError, ConfigResult};
use crate::storage::{
    ArtifactReadStore, EnrichmentJobLifecycleStore, ImportWriteStore, OracleEnrichmentJobStore,
    OracleImportWriteStore,
};

pub trait ArchiveStore: ImportWriteStore + ArtifactReadStore + Send + Sync {}

impl<T> ArchiveStore for T where T: ImportWriteStore + ArtifactReadStore + Send + Sync {}

pub struct ServiceBundle {
    pub archive_store: Arc<dyn ArchiveStore>,
    pub enrichment_store: Arc<dyn EnrichmentJobLifecycleStore>,
}

pub fn build_service_bundle(config: &AppConfig) -> ConfigResult<ServiceBundle> {
    match &config.relational_store {
        RelationalStoreConfig::Oracle(db_config) => Ok(ServiceBundle {
            archive_store: Arc::new(OracleImportWriteStore::new(db_config.clone())),
            enrichment_store: Arc::new(OracleEnrichmentJobStore::new(db_config.clone())),
        }),
    }
}

/// Temporary helper for legacy commands that still talk directly to the Oracle
/// migration and probe code paths. This should disappear once those commands
/// become provider-backed too.
pub fn require_oracle_db_config(config: &AppConfig) -> ConfigResult<&crate::config::DbConfig> {
    config
        .relational_store
        .oracle_config()
        .ok_or(ConfigError::InvalidEnumEnv {
            key: "OA_RELATIONAL_STORE",
            value: "unknown".to_string(),
            expected: "oracle",
        })
}
