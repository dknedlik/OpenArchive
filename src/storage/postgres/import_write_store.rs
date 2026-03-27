use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::storage::types::ImportStatus;
use crate::storage::{
    ArtifactIngestResult, ImportWriteResult, ImportWriteStore, ImportedArtifact, StorageTx,
    WriteImportSet,
};

use super::tx::{begin_storage_tx, begin_transaction, rollback_transaction};
use super::{artifact, import, job, segment};

pub struct PostgresImportWriteStore {
    config: PostgresConfig,
}

impl PostgresImportWriteStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl ImportWriteStore for PostgresImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let mut tx = begin_storage_tx(&self.config)?;

        if let Some(existing_object_id) = import::find_payload_object_id_by_sha256(
            &mut tx.client,
            &import_set.payload_object.sha256,
        )? {
            import_set.import.payload_object_id = existing_object_id;
        } else {
            import::insert_payload_object(&mut tx.client, &import_set.payload_object)?;
        }
        import::insert_import(&mut tx.client, &import_set.import)?;
        tx.commit()?;

        let import_id = import_set.import.import_id.clone();
        let mut artifacts: Vec<ImportedArtifact> =
            Vec::with_capacity(import_set.artifact_sets.len());
        let mut failed_artifact_ids: Vec<String> =
            Vec::with_capacity(import_set.artifact_sets.len());
        let mut segments_written: usize = 0;
        let mut count_imported: i32 = 0;
        let mut count_failed: i32 = 0;
        let mut artifact_errors: Vec<String> = Vec::new();

        for artifact_set in import_set.artifact_sets {
            let artifact_id = artifact_set.artifact.artifact_id.clone();

            if let Some((existing_artifact_id, enrichment_status)) =
                artifact::find_artifact_by_source_hash(
                    &mut tx.client,
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

            begin_transaction(&mut tx.client, &self.config.connection_string)?;

            let phase2_result: StorageResult<usize> = (|| {
                artifact::insert_artifact(&mut tx.client, &artifact_set.artifact)?;

                for participant in &artifact_set.participants {
                    artifact::insert_participant(&mut tx.client, participant)?;
                }

                let mut seg_count = 0usize;
                for seg in &artifact_set.segments {
                    segment::insert_segment(&mut tx.client, seg)?;
                    seg_count += 1;
                }

                job::insert_job(&mut tx.client, &artifact_set.job)?;
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
                    rollback_transaction(&mut tx.client, &self.config.connection_string)?;
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

        begin_transaction(&mut tx.client, &self.config.connection_string)?;
        import::finalize_import(
            &mut tx.client,
            &import_id,
            count_imported,
            count_failed,
            import_status,
            error_message.as_deref(),
        )?;
        tx.commit()?;

        Ok(ImportWriteResult {
            import_id,
            import_status,
            artifacts,
            failed_artifact_ids,
            segments_written,
        })
    }
}
