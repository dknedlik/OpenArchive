use crate::error::StorageResult;
use crate::storage::types::{
    NewImportedNoteAlias, NewImportedNoteLink, NewImportedNoteProperty, NewImportedNoteTag,
};
use crate::storage::StorageTx;

/// Stores canonical imported note metadata rows for document artifacts.
pub trait ImportedNoteMetadataStore {
    type Tx: StorageTx;

    fn insert_imported_note_property(
        &self,
        tx: &mut Self::Tx,
        property: &NewImportedNoteProperty,
    ) -> StorageResult<()>;

    fn insert_imported_note_tag(
        &self,
        tx: &mut Self::Tx,
        tag: &NewImportedNoteTag,
    ) -> StorageResult<()>;

    fn insert_imported_note_alias(
        &self,
        tx: &mut Self::Tx,
        alias: &NewImportedNoteAlias,
    ) -> StorageResult<()>;

    fn insert_imported_note_link(
        &self,
        tx: &mut Self::Tx,
        link: &NewImportedNoteLink,
    ) -> StorageResult<()>;
}
