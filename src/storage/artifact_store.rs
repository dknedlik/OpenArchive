use crate::error::StorageResult;

use crate::storage::types::{LoadedArtifactForEnrichment, NewArtifact, NewParticipant};
use crate::storage::StorageTx;

/// Stores canonical artifacts and their participants.
///
/// Participants are grouped here because they are always written together with
/// their artifact and share the same per-artifact transaction boundary.
pub trait ArtifactStore {
    type Tx: StorageTx;

    fn insert_artifact(&self, tx: &mut Self::Tx, artifact: &NewArtifact) -> StorageResult<()>;

    fn insert_participant(
        &self,
        tx: &mut Self::Tx,
        participant: &NewParticipant,
    ) -> StorageResult<()>;
}

/// Read model for listing imported artifacts.
pub trait ArtifactReadStore: Send + Sync {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>>;

    fn list_artifacts_filtered(
        &self,
        filters: &crate::storage::types::ArtifactListFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>>;

    fn get_timeline(
        &self,
        filters: &crate::storage::types::TimelineFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::TimelineEntry>>;

    /// Load the ordered artifact shape the enrichment worker needs.
    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<LoadedArtifactForEnrichment>>;
}
