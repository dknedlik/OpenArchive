use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::storage::types::ImportStatus;
use crate::storage::{
    ArtifactIngestResult, ImportWriteResult, ImportWriteStore, ImportedArtifact,
    NewImportedNoteLink, StorageTx, WriteImportSet,
};
use std::collections::HashMap;

use super::tx::{begin_storage_tx, begin_transaction, rollback_transaction};
use super::{artifact, import, imported_note, job, segment};

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
        let mut planned_to_actual_artifact_ids: HashMap<String, String> = HashMap::new();
        let mut deferred_links: Vec<(String, Vec<NewImportedNoteLink>)> = Vec::new();

        for artifact_set in import_set.artifact_sets {
            let planned_artifact_id = artifact_set.artifact.artifact_id.clone();

            if let Some((existing_artifact_id, enrichment_status)) =
                artifact::find_artifact_by_source_hash(
                    &mut tx.client,
                    artifact_set.artifact.source_type,
                    &artifact_set.artifact.content_hash_version,
                    &artifact_set.artifact.source_conversation_hash,
                )?
            {
                planned_to_actual_artifact_ids
                    .insert(planned_artifact_id.clone(), existing_artifact_id.clone());
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

                for property in &artifact_set.imported_note_metadata.properties {
                    imported_note::insert_imported_note_property(&mut tx.client, property)?;
                }

                for tag in &artifact_set.imported_note_metadata.tags {
                    imported_note::insert_imported_note_tag(&mut tx.client, tag)?;
                }

                for alias in &artifact_set.imported_note_metadata.aliases {
                    imported_note::insert_imported_note_alias(&mut tx.client, alias)?;
                }

                job::insert_job(&mut tx.client, &artifact_set.job)?;
                Ok(seg_count)
            })();

            match phase2_result {
                Ok(seg_count) => {
                    tx.commit()?;
                    planned_to_actual_artifact_ids
                        .insert(planned_artifact_id.clone(), planned_artifact_id.clone());
                    deferred_links.push((
                        planned_artifact_id.clone(),
                        artifact_set.imported_note_metadata.links,
                    ));
                    artifacts.push(ImportedArtifact {
                        artifact_id: planned_artifact_id,
                        enrichment_status: artifact_set.artifact.enrichment_status,
                        ingest_result: ArtifactIngestResult::Created,
                    });
                    segments_written += seg_count;
                    count_imported += 1;
                }
                Err(error) => {
                    rollback_transaction(&mut tx.client, &self.config.connection_string)?;
                    failed_artifact_ids.push(planned_artifact_id.clone());
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {error:#}", planned_artifact_id));
                }
            }
        }

        for (artifact_id, links) in deferred_links {
            if links.is_empty() {
                continue;
            }

            begin_transaction(&mut tx.client, &self.config.connection_string)?;

            let link_result: StorageResult<()> = (|| {
                for link in &links {
                    let resolved_artifact_id = link
                        .resolved_artifact_id
                        .as_ref()
                        .and_then(|artifact_id| planned_to_actual_artifact_ids.get(artifact_id))
                        .cloned();
                    let mut remapped_link = link.clone();
                    remapped_link.resolved_artifact_id = resolved_artifact_id;
                    imported_note::insert_imported_note_link(&mut tx.client, &remapped_link)?;
                }
                Ok(())
            })();

            match link_result {
                Ok(()) => tx.commit()?,
                Err(error) => {
                    rollback_transaction(&mut tx.client, &self.config.connection_string)?;
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
