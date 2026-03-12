use crate::error::StorageResult;

use crate::storage::types::{LoadedConversationForEnrichment, NewArtifact, NewParticipant};
use crate::storage::StorageTx;

/// Stores conversation artifacts and their participants.
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

    /// Load the ordered conversation shape the enrichment worker needs.
    fn load_conversation_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<LoadedConversationForEnrichment>>;
}
