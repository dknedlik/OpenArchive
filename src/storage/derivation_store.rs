use crate::error::StorageResult;

use crate::storage::types::{NewDerivationRun, NewDerivedObject};
use crate::storage::writeback_store::NewArchiveLink;
use crate::storage::StorageTx;

pub trait DerivationStore {
    type Tx: StorageTx;

    fn insert_derivation_run(&self, tx: &mut Self::Tx, run: &NewDerivationRun)
        -> StorageResult<()>;

    fn insert_derived_object(
        &self,
        tx: &mut Self::Tx,
        object: &NewDerivedObject,
    ) -> StorageResult<()>;
}

#[derive(Debug)]
pub struct WriteDerivedObject {
    pub object: NewDerivedObject,
}

#[derive(Debug)]
pub struct WriteDerivationAttempt {
    pub run: NewDerivationRun,
    pub objects: Vec<WriteDerivedObject>,
    pub archive_links: Vec<NewArchiveLink>,
}

#[derive(Debug)]
pub struct DerivationWriteResult {
    pub derivation_run_id: String,
    pub derived_object_ids: Vec<String>,
}

pub trait DerivedMetadataWriteStore: Send + Sync {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult>;
}
