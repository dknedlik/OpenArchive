use log::error;

use crate::config::OracleConfig;
use crate::error::{StorageError, StorageResult};
use crate::storage::types::ImportStatus;
use crate::storage::{
    ArtifactIngestResult, ImportWriteResult, ImportWriteStore, ImportedArtifact, StorageTx,
    WriteImportSet,
};

use super::tx::{begin_storage_tx, commit_connection};
use super::{artifact, import, job, segment};

pub struct OracleImportWriteStore {
    config: OracleConfig,
}

impl OracleImportWriteStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl ImportWriteStore for OracleImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let mut tx = begin_storage_tx(&self.config)?;

        if let Some(existing_payload_object_id) =
            import::find_payload_object_id_by_sha256(&tx.conn, &import_set.payload_object.sha256)?
        {
            import_set.import.payload_object_id = existing_payload_object_id;
        } else if let Err(error) =
            import::insert_payload_object(&tx.conn, &import_set.payload_object)
        {
            if let Err(rollback_err) = tx.conn.rollback() {
                error!(
                    "Failed to rollback after payload insert error (operation=phase1_payload_insert): {}",
                    rollback_err
                );
            }
            return Err(error);
        }
        if let Err(error) = import::insert_import(&tx.conn, &import_set.import) {
            if let Err(rollback_err) = tx.conn.rollback() {
                error!(
                    "Failed to rollback after import insert error (operation=phase1_import_insert): {}",
                    rollback_err
                );
            }
            return Err(error);
        }
        tx.commit()?;

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
                Err(error) => {
                    tx.conn
                        .rollback()
                        .map_err(|source| StorageError::Rollback {
                            operation: format!("failed artifact set {artifact_id}"),
                            source: Box::new(source),
                        })?;
                    failed_artifact_ids.push(artifact_id.clone());
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {error:#}", artifact_id));
                }
            }
        }

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

        if let Err(error) = finalize_import(
            &tx.conn,
            &import_id,
            count_imported,
            count_failed,
            import_status,
            error_message.as_deref(),
        ) {
            if let Err(rollback_err) = tx.conn.rollback() {
                error!(
                    "Failed to rollback after phase 3 finalize error (import_id={}, operation=phase3_finalize_import): {}",
                    import_id, rollback_err
                );
            }
            let recovery_message = format!("phase 3 failure after committed artifacts: {error:#}");
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
            return Err(error);
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

fn finalize_import(
    conn: &oracle::Connection,
    import_id: &str,
    count_imported: i64,
    count_failed: i64,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    import::finalize_import(
        conn,
        import_id,
        count_imported,
        count_failed,
        status,
        error_message,
    )?;
    commit_connection(conn, "import finalization")
}

fn recover_import_finalization(
    config: &OracleConfig,
    import_id: &str,
    count_imported: i64,
    count_failed: i64,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    let conn = crate::db::connect(config)?;
    import::finalize_import(
        &conn,
        import_id,
        count_imported,
        count_failed,
        status,
        error_message,
    )?;
    commit_connection(&conn, "import finalization recovery")
}
