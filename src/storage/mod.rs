//! Domain-facing storage interfaces for slice one.
//!
//! This module defines the write-path interface boundary. Application code
//! depends on these traits and types; Oracle-specific connection and SQL
//! concerns live in the implementation layer below this boundary.
//!
//! ## Transaction model
//!
//! `StorageTx` is the opaque transaction handle passed through all granular
//! store methods. The Oracle implementation wraps `oracle::Connection` as its
//! concrete `Tx` type. This keeps Oracle out of the domain layer while still
//! letting the granular methods share one live transaction.
//!
//! `ImportWriteStore::write_import` is the sole commit-boundary owner.
//! The expected slice-one commit strategy is:
//!
//! 1. Insert payload + import header → `tx.commit()`
//! 2. For each artifact set: insert artifact, participants, segments, job → `tx.commit()`
//! 3. Update final import status → `tx.commit()`
//!
//! Granular trait methods never call `commit()` themselves.

pub mod artifact_store;
pub mod import_store;
pub mod job_store;
pub mod oracle;
pub mod segment_store;
pub mod types;

pub use artifact_store::ArtifactStore;
pub use import_store::{ImportPayloadStore, ImportStore};
pub use job_store::EnrichmentJobStore;
pub use oracle::OracleImportWriteStore;
pub use segment_store::SegmentStore;
pub use types::{
    ArtifactClass, ArtifactStatus, EnrichmentStatus, ImportStatus, JobStatus, JobType,
    NewArtifact, NewEnrichmentJob, NewImport, NewImportPayload, NewParticipant, NewSegment,
    ParticipantRole, PayloadFormat, SegmentType, SourceType, VisibilityStatus,
};

use anyhow::Result;

// ---------------------------------------------------------------------------
// Transaction abstraction
// ---------------------------------------------------------------------------

/// Opaque transaction/session handle passed through all granular store methods.
///
/// The Oracle implementation satisfies this trait using `oracle::Connection`
/// (which carries the implicit transaction). Commit/rollback decisions stay
/// in `ImportWriteStore::write_import`, not in the granular trait methods.
pub trait StorageTx {
    fn commit(&mut self) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Composite write types
// ---------------------------------------------------------------------------

/// Everything needed to write one conversation artifact within an import.
pub struct WriteArtifactSet {
    pub artifact: NewArtifact,
    pub participants: Vec<NewParticipant>,
    pub segments: Vec<NewSegment>,
    pub job: NewEnrichmentJob,
}

/// The complete write set for one import request.
///
/// This is the unit passed to `ImportWriteStore::write_import`. All fields
/// must be fully populated before the call.
pub struct WriteImportSet {
    pub payload: NewImportPayload,
    pub import: NewImport,
    pub artifact_sets: Vec<WriteArtifactSet>,
}

/// Returned after a successful `write_import` call.
///
/// Includes the artifact IDs so the application layer can satisfy the
/// `POST /imports/chatgpt` response contract (artifact ids created) without
/// re-querying the database.
pub struct ImportWriteResult {
    pub import_id: String,
    pub import_status: ImportStatus,
    pub artifact_ids: Vec<String>,
    pub failed_artifact_ids: Vec<String>,
    pub segments_written: usize,
}

// ---------------------------------------------------------------------------
// Composite store trait (the app-facing boundary)
// ---------------------------------------------------------------------------

/// High-level write interface for one complete import.
///
/// This is the primary trait called by application code. Implementations are
/// responsible for creating and committing the underlying transaction at the
/// points described in the module-level doc comment.
pub trait ImportWriteStore {
    fn write_import(&self, import_set: WriteImportSet) -> Result<ImportWriteResult>;
}
