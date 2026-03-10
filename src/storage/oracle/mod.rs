pub mod artifact;
pub mod import;
pub mod job;
pub mod segment;

use crate::config::DbConfig;
use crate::db;
use crate::error::{StorageError, StorageResult};
use crate::storage::types::ImportStatus;
use crate::storage::{
    ArtifactIngestResult, ArtifactReadStore, ImportWriteResult, ImportWriteStore, ImportedArtifact,
    StorageTx, WriteImportSet,
};

// ---------------------------------------------------------------------------
// Transaction handle
// ---------------------------------------------------------------------------

pub struct OracleStorageTx {
    conn: oracle::Connection,
}

impl StorageTx for OracleStorageTx {
    fn commit(&mut self) -> StorageResult<()> {
        self.conn.commit().map_err(|source| StorageError::Commit {
            operation: "storage transaction",
            source,
        })
    }
}

// ---------------------------------------------------------------------------
// Store struct
// ---------------------------------------------------------------------------

pub struct OracleImportWriteStore {
    config: DbConfig,
}

impl OracleImportWriteStore {
    pub fn new(config: DbConfig) -> Self {
        Self { config }
    }
}

impl ArtifactReadStore for OracleImportWriteStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let conn = db::connect(&self.config)?;
        artifact::list_artifacts(&conn)
    }
}

// ---------------------------------------------------------------------------
// ImportWriteStore implementation
// ---------------------------------------------------------------------------

impl ImportWriteStore for OracleImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let conn = db::connect(&self.config)?;
        let mut tx = OracleStorageTx { conn };

        // ------------------------------------------------------------------
        // Phase 1: payload + import header
        // ------------------------------------------------------------------
        if let Some(existing_payload_id) =
            import::find_payload_id_by_sha256(&tx.conn, &import_set.payload.payload_sha256)?
        {
            import_set.import.payload_id = existing_payload_id;
        } else if let Err(e) = import::insert_payload(&tx.conn, &import_set.payload) {
            let _ = tx.conn.rollback();
            return Err(e);
        }
        if let Err(e) = import::insert_import(&tx.conn, &import_set.import) {
            let _ = tx.conn.rollback();
            return Err(e);
        }
        tx.commit()?;

        // ------------------------------------------------------------------
        // Phase 2: artifact sets
        // ------------------------------------------------------------------
        let import_id = import_set.import.import_id.clone();
        let mut artifacts: Vec<ImportedArtifact> =
            Vec::with_capacity(import_set.artifact_sets.len());
        let mut failed_artifact_ids: Vec<String> =
            Vec::with_capacity(import_set.artifact_sets.len());
        let mut segments_written: usize = 0;
        let mut count_imported: i64 = 0;
        let mut count_failed: i64 = 0;
        let mut artifact_errors: Vec<String> = Vec::new();

        for artifact_set in import_set.artifact_sets {
            let artifact_id = artifact_set.artifact.artifact_id.clone();
            if let Some((existing_artifact_id, enrichment_status)) =
                artifact::find_artifact_by_source_hash(
                    &tx.conn,
                    artifact_set.artifact.source_type,
                    &artifact_set.artifact.content_hash_version,
                    &artifact_set.artifact.source_conversation_hash,
                )?
            {
                artifacts.push(ImportedArtifact {
                    artifact_id: existing_artifact_id,
                    enrichment_status,
                    ingest_result: ArtifactIngestResult::AlreadyExists,
                });
                continue;
            }

            let phase2_result: StorageResult<usize> = (|| {
                artifact::insert_artifact(&tx.conn, &artifact_set.artifact)?;

                for participant in &artifact_set.participants {
                    artifact::insert_participant(&tx.conn, participant)?;
                }

                let mut seg_count = 0usize;
                for seg in &artifact_set.segments {
                    segment::insert_segment(&tx.conn, seg)?;
                    seg_count += 1;
                }

                job::insert_job(&tx.conn, &artifact_set.job)?;

                Ok(seg_count)
            })();

            match phase2_result {
                Ok(seg_count) => {
                    tx.commit()?;
                    artifacts.push(ImportedArtifact {
                        artifact_id,
                        enrichment_status: artifact_set.artifact.enrichment_status,
                        ingest_result: ArtifactIngestResult::Created,
                    });
                    segments_written += seg_count;
                    count_imported += 1;
                }
                Err(e) => {
                    tx.conn
                        .rollback()
                        .map_err(|source| StorageError::Rollback {
                            operation: format!("failed artifact set {artifact_id}"),
                            source,
                        })?;
                    failed_artifact_ids.push(artifact_id.clone());
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {e:#}", artifact_id));
                }
            }
        }

        // ------------------------------------------------------------------
        // Phase 3: update counts + complete import
        // ------------------------------------------------------------------
        let import_status = if count_failed == 0 {
            ImportStatus::Completed
        } else if count_imported == 0 {
            ImportStatus::Failed
        } else {
            ImportStatus::CompletedWithErrors
        };
        let error_message = if artifact_errors.is_empty() {
            None
        } else {
            Some(artifact_errors.join(" | "))
        };

        if let Err(e) = finalize_import(
            &mut tx,
            &import_id,
            count_imported,
            count_failed,
            import_status,
            error_message.as_deref(),
        ) {
            let _ = tx.conn.rollback();
            let recovery_message = format!("phase 3 failure after committed artifacts: {e:#}");
            recover_import_finalization(
                &self.config,
                &import_id,
                count_imported,
                count_failed,
                import_status,
                Some(recovery_message.as_str()),
            )
            .map_err(|source| StorageError::RecoverImportFinalization {
                import_id: import_id.clone(),
                source: Box::new(source),
            })?;
            return Err(e);
        }

        Ok(ImportWriteResult {
            import_id,
            import_status,
            artifacts,
            failed_artifact_ids,
            segments_written,
        })
    }
}

// ---------------------------------------------------------------------------
// Enrichment job lifecycle store
// ---------------------------------------------------------------------------

use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::types::{ClaimedJob, RetryOutcome};

pub struct OracleEnrichmentJobStore {
    config: DbConfig,
}

impl OracleEnrichmentJobStore {
    pub fn new(config: DbConfig) -> Self {
        Self { config }
    }
}

impl EnrichmentJobLifecycleStore for OracleEnrichmentJobStore {
    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_next_job(&conn, worker_id)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        job::complete_job(&conn, worker_id, job_id)
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        job::fail_job(&conn, worker_id, job_id, error_message)
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let conn = db::connect(&self.config)?;
        job::mark_job_retryable(&conn, worker_id, job_id, error_message, retry_after_seconds)
    }
}

fn finalize_import(
    tx: &mut OracleStorageTx,
    import_id: &str,
    count_imported: i64,
    count_failed: i64,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    import::finalize_import(
        &tx.conn,
        import_id,
        count_imported,
        count_failed,
        status,
        error_message,
    )?;
    tx.commit()
}

fn recover_import_finalization(
    config: &DbConfig,
    import_id: &str,
    count_imported: i64,
    count_failed: i64,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    let conn = db::connect(config)?;
    import::finalize_import(
        &conn,
        import_id,
        count_imported,
        count_failed,
        status,
        error_message,
    )?;
    conn.commit().map_err(|source| StorageError::Commit {
        operation: "import finalization recovery",
        source,
    })
}
