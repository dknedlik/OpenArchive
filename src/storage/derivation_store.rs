use crate::error::StorageResult;

use crate::storage::types::{NewDerivationRun, NewDerivedObject, NewEvidenceLink};
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

    fn insert_evidence_link(&self, tx: &mut Self::Tx, link: &NewEvidenceLink) -> StorageResult<()>;
}

#[derive(Debug)]
pub struct WriteDerivedObject {
    pub object: NewDerivedObject,
    pub evidence_links: Vec<NewEvidenceLink>,
}

#[derive(Debug)]
pub struct WriteDerivationAttempt {
    pub run: NewDerivationRun,
    pub objects: Vec<WriteDerivedObject>,
}

#[derive(Debug)]
pub struct DerivationWriteResult {
    pub derivation_run_id: String,
    pub derived_object_ids: Vec<String>,
    pub evidence_links_written: usize,
}

pub trait DerivedMetadataWriteStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult>;
}
