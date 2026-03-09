use crate::error::StorageResult;

use crate::storage::types::{NewArtifact, NewParticipant};
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
