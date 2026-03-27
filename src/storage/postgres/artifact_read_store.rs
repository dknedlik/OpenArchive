use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db;
use crate::storage::ArtifactReadStore;

use super::artifact;

pub struct PostgresArtifactReadStore {
    config: PostgresConfig,
}

impl PostgresArtifactReadStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl ArtifactReadStore for PostgresArtifactReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::list_artifacts(&mut client)
    }

    fn list_artifacts_filtered(
        &self,
        filters: &crate::storage::types::ArtifactListFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::list_artifacts_filtered(&mut client, filters, limit, offset)
    }

    fn get_timeline(
        &self,
        filters: &crate::storage::types::TimelineFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::TimelineEntry>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::get_timeline(&mut client, filters, limit, offset)
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<crate::storage::types::LoadedArtifactForEnrichment>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::load_artifact_for_enrichment(&mut client, artifact_id)
    }
}
