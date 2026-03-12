//! Composition root for provider-backed services.
//!
//! `main` should assemble application-facing traits through this module rather
//! than naming concrete providers directly.

use std::sync::Arc;

use crate::config::{AppConfig, ObjectStoreConfig, RelationalStoreConfig};
use crate::error::{ConfigError, ConfigResult};
use crate::object_store::{LocalFsObjectStore, ObjectStore, S3CompatibleObjectStore};
use crate::storage::{
    ArtifactReadStore, EnrichmentJobLifecycleStore, ImportWriteStore, OracleEnrichmentJobStore,
    OracleImportWriteStore, PostgresEnrichmentJobStore, PostgresImportWriteStore,
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
    pub enrichment_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub derived_store: Arc<dyn crate::storage::DerivedMetadataWriteStore>,
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
            let archive_store: Arc<dyn ArchiveStore> =
                Arc::new(PostgresImportWriteStore::new(pg_config.clone()));
            Ok(ServiceBundle {
                archive_service: Arc::new(DelegatingArchiveAppService {
                    archive_store: Arc::clone(&archive_store),
                    object_store,
                }),
                read_store: archive_store,
                enrichment_store: Arc::new(PostgresEnrichmentJobStore::new(pg_config.clone())),
                derived_store: Arc::new(crate::storage::PostgresDerivedMetadataStore::new(
                    pg_config.clone(),
                )),
            })
        }
        RelationalStoreConfig::Oracle(db_config) => {
            let archive_store: Arc<dyn ArchiveStore> =
                Arc::new(OracleImportWriteStore::new(db_config.clone()));
            Ok(ServiceBundle {
                archive_service: Arc::new(DelegatingArchiveAppService {
                    archive_store: Arc::clone(&archive_store),
                    object_store,
                }),
                read_store: archive_store,
                enrichment_store: Arc::new(OracleEnrichmentJobStore::new(db_config.clone())),
                derived_store: Arc::new(crate::storage::OracleDerivedMetadataStore::new(
                    db_config.clone(),
                )),
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
