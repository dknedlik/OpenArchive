use crate::config::OracleConfig;
use crate::db;
use crate::error::{StorageError, StorageResult};
use crate::storage::ArtifactReadStore;

use super::artifact;

pub struct OracleArtifactReadStore {
    config: OracleConfig,
}

impl OracleArtifactReadStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl ArtifactReadStore for OracleArtifactReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let conn = db::connect(&self.config)?;
        artifact::list_artifacts(&conn)
    }

    fn list_artifacts_filtered(
        &self,
        _filters: &crate::storage::types::ArtifactListFilters,
        _limit: usize,
        _offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        Err(StorageError::UnsupportedOperation {
            store: "oracle",
            operation: "list_artifacts_filtered",
        })
    }

    fn get_timeline(
        &self,
        _filters: &crate::storage::types::TimelineFilters,
        _limit: usize,
        _offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::TimelineEntry>> {
        Err(StorageError::UnsupportedOperation {
            store: "oracle",
            operation: "get_timeline",
        })
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<crate::storage::types::LoadedArtifactForEnrichment>> {
        let conn = db::connect(&self.config)?;
        artifact::load_artifact_for_enrichment(&conn, artifact_id)
    }
}
