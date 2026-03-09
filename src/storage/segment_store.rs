use anyhow::Result;

use crate::storage::types::NewSegment;
use crate::storage::StorageTx;

/// Stores canonical ordered message segments for a conversation artifact.
pub trait SegmentStore {
    type Tx: StorageTx;

    fn insert_segment(&self, tx: &mut Self::Tx, segment: &NewSegment) -> Result<()>;
}
