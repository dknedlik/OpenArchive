use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::error::StorageError;
use crate::domain::SourceTimestamp;
use crate::storage::types::ArtifactIngestResult;
use crate::storage::{ImportWriteResult, ImportWriteStore, ImportedArtifact, WriteImportSet};

use super::links::{
    insert_imported_note_alias, insert_imported_note_link, insert_imported_note_property,
    insert_imported_note_tag, sync_structural_links_for_artifact,
};
use super::{
    db_err, open_connection, parse_enrichment_status, SqliteImportWriteStore, StorageResult,
};

impl ImportWriteStore for SqliteImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let mut connection = open_connection(&self.config)?;

        {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            let existing_object_id: Option<String> = tx
                .query_row(
                    "SELECT object_id FROM oa_object_ref WHERE sha256 = ?1 AND object_kind = 'import_payload'",
                    [&import_set.payload_object.sha256],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|source| db_err(&self.config, source))?;

            if let Some(existing_object_id) = existing_object_id {
                import_set.import.payload_object_id = existing_object_id;
            } else {
                tx.execute(
                    "INSERT INTO oa_object_ref
                     (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        import_set.payload_object.object_id,
                        "import_payload",
                        import_set.payload_object.stored_object.provider,
                        import_set.payload_object.stored_object.storage_key,
                        import_set.payload_object.stored_object.mime_type,
                        import_set.payload_object.size_bytes,
                        import_set.payload_object.sha256
                    ],
                )
                .map_err(|source| StorageError::InsertPayload {
                    payload_id: import_set.payload_object.object_id.clone(),
                    source: Box::new(source),
                })?;
            }

            tx.execute(
                "INSERT INTO oa_import
                 (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, conversation_count_detected)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    import_set.import.import_id,
                    import_set.import.source_type.as_str(),
                    import_set.import.import_status.as_str(),
                    import_set.import.payload_object_id,
                    import_set.import.source_filename,
                    import_set.import.source_content_hash,
                    import_set.import.conversation_count_detected
                ],
            )
            .map_err(|source| StorageError::InsertImport {
                import_id: import_set.import.import_id.clone(),
                source: Box::new(source),
            })?;
            tx.commit().map_err(|source| StorageError::InsertImport {
                import_id: import_set.import.import_id.clone(),
                source: Box::new(source),
            })?;
        }

        let import_id = import_set.import.import_id.clone();
        let mut artifacts = Vec::with_capacity(import_set.artifact_sets.len());
        let mut failed_artifact_ids = Vec::new();
        let mut segments_written = 0usize;
        let mut count_imported = 0i64;
        let mut count_failed = 0i64;
        let mut artifact_errors = Vec::new();
        let mut planned_to_actual_artifact_ids = std::collections::HashMap::new();
        let mut deferred_links = Vec::new();
        let mut created_artifact_ids = Vec::new();

        for artifact_set in import_set.artifact_sets {
            let planned_artifact_id = artifact_set.artifact.artifact_id.clone();
            let existing = connection
                .query_row(
                    "SELECT artifact_id, enrichment_status
                     FROM oa_artifact
                     WHERE source_type = ?1
                       AND content_hash_version = ?2
                       AND source_conversation_hash = ?3",
                    params![
                        artifact_set.artifact.source_type.as_str(),
                        artifact_set.artifact.content_hash_version,
                        artifact_set.artifact.source_conversation_hash
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            parse_enrichment_status(row.get(1)?)?,
                        ))
                    },
                )
                .optional()
                .map_err(|source| db_err(&self.config, source))?;
            if let Some((existing_artifact_id, enrichment_status)) = existing {
                planned_to_actual_artifact_ids
                    .insert(planned_artifact_id.clone(), existing_artifact_id.clone());
                artifacts.push(ImportedArtifact {
                    artifact_id: existing_artifact_id,
                    enrichment_status,
                    ingest_result: ArtifactIngestResult::AlreadyExists,
                });
                continue;
            }

            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            let phase_result = (|| -> StorageResult<usize> {
                tx.execute(
                    "INSERT INTO oa_artifact
                     (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status,
                      source_conversation_key, source_conversation_hash, title, created_at_source,
                      started_at, ended_at, primary_language, content_hash_version, content_facets_json,
                      normalization_version, error_message)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                    params![
                        artifact_set.artifact.artifact_id,
                        artifact_set.artifact.import_id,
                        artifact_set.artifact.artifact_class.as_str(),
                        artifact_set.artifact.source_type.as_str(),
                        artifact_set.artifact.artifact_status.as_str(),
                        artifact_set.artifact.enrichment_status.as_str(),
                        artifact_set.artifact.source_conversation_key,
                        artifact_set.artifact.source_conversation_hash,
                        artifact_set.artifact.title,
                        artifact_set
                            .artifact
                            .created_at_source
                            .as_ref()
                            .map(SourceTimestamp::as_str),
                        artifact_set
                            .artifact
                            .started_at
                            .as_ref()
                            .map(SourceTimestamp::as_str),
                        artifact_set
                            .artifact
                            .ended_at
                            .as_ref()
                            .map(SourceTimestamp::as_str),
                        artifact_set.artifact.primary_language,
                        artifact_set.artifact.content_hash_version,
                        artifact_set.artifact.content_facets_json,
                        artifact_set.artifact.normalization_version,
                        Option::<&str>::None
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;

                for participant in &artifact_set.participants {
                    tx.execute(
                        "INSERT INTO oa_conversation_participant
                         (participant_id, artifact_id, participant_role, display_name, provider_name, model_name, source_participant_key, sequence_no)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            participant.participant_id,
                            participant.artifact_id,
                            participant.participant_role.as_str(),
                            participant.display_name,
                            participant.provider_name,
                            participant.model_name,
                            participant.source_participant_key,
                            participant.sequence_no
                        ],
                    )
                    .map_err(|source| db_err(&self.config, source))?;
                }

                let mut seg_count = 0usize;
                for segment in &artifact_set.segments {
                    tx.execute(
                        "INSERT INTO oa_segment
                         (segment_id, artifact_id, participant_id, segment_type, source_segment_key, parent_segment_id,
                          sequence_no, created_at_source, text_content, text_content_hash, locator_json,
                          visibility_status, unsupported_content_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                        params![
                            segment.segment_id,
                            segment.artifact_id,
                            segment.participant_id,
                            segment.segment_type.as_str(),
                            segment.source_segment_key,
                            segment.parent_segment_id,
                            segment.sequence_no,
                            segment.created_at_source.as_ref().map(SourceTimestamp::as_str),
                            segment.text_content,
                            segment.text_content_hash,
                            segment.locator_json,
                            segment.visibility_status.as_str(),
                            segment.unsupported_content_json
                        ],
                    )
                    .map_err(|source| db_err(&self.config, source))?;
                    seg_count += 1;
                }

                for property in &artifact_set.imported_note_metadata.properties {
                    insert_imported_note_property(&tx, property)?;
                }
                for tag in &artifact_set.imported_note_metadata.tags {
                    insert_imported_note_tag(&tx, tag)?;
                }
                for alias in &artifact_set.imported_note_metadata.aliases {
                    insert_imported_note_alias(&tx, alias)?;
                }

                tx.execute(
                    "INSERT INTO oa_enrichment_job
                     (job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, job_status,
                      max_attempts, priority_no, required_capabilities, payload_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        artifact_set.job.job_id,
                        artifact_set.job.artifact_id,
                        artifact_set.job.job_type.as_str(),
                        artifact_set.job.enrichment_tier.as_str(),
                        artifact_set.job.spawned_by_job_id,
                        artifact_set.job.job_status.as_str(),
                        artifact_set.job.max_attempts,
                        artifact_set.job.priority_no,
                        serde_json::to_string(&artifact_set.job.required_capabilities)
                            .expect("required capabilities serializable"),
                        artifact_set.job.payload_json
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;

                Ok(seg_count)
            })();

            match phase_result {
                Ok(seg_count) => {
                    tx.commit().map_err(|source| db_err(&self.config, source))?;
                    planned_to_actual_artifact_ids
                        .insert(planned_artifact_id.clone(), planned_artifact_id.clone());
                    deferred_links.push((
                        planned_artifact_id.clone(),
                        artifact_set.imported_note_metadata.links,
                    ));
                    created_artifact_ids.push(planned_artifact_id.clone());
                    artifacts.push(ImportedArtifact {
                        artifact_id: planned_artifact_id.clone(),
                        enrichment_status: artifact_set.artifact.enrichment_status,
                        ingest_result: ArtifactIngestResult::Created,
                    });
                    segments_written += seg_count;
                    count_imported += 1;
                }
                Err(error) => {
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
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            let result = (|| -> StorageResult<()> {
                for link in &links {
                    let resolved_artifact_id = link
                        .resolved_artifact_id
                        .as_ref()
                        .and_then(|planned_id| planned_to_actual_artifact_ids.get(planned_id))
                        .cloned();
                    let mut remapped = link.clone();
                    remapped.resolved_artifact_id = resolved_artifact_id;
                    insert_imported_note_link(&tx, &remapped)?;
                }
                Ok(())
            })();
            match result {
                Ok(()) => {
                    tx.commit().map_err(|source| db_err(&self.config, source))?;
                }
                Err(error) => {
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {error:#}", artifact_id));
                }
            }
        }

        for artifact_id in &created_artifact_ids {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            match sync_structural_links_for_artifact(&tx, artifact_id) {
                Ok(()) => {
                    tx.commit().map_err(|source| db_err(&self.config, source))?;
                }
                Err(error) => {
                    count_failed += 1;
                    artifact_errors.push(format!(
                        "artifact {} structural links: {error:#}",
                        artifact_id
                    ));
                }
            }
        }

        let import_status = if count_failed == 0 {
            crate::storage::types::ImportStatus::Completed
        } else if count_imported == 0 {
            crate::storage::types::ImportStatus::Failed
        } else {
            crate::storage::types::ImportStatus::CompletedWithErrors
        };
        let error_message = if artifact_errors.is_empty() {
            None
        } else {
            Some(artifact_errors.join(" | "))
        };

        connection
            .execute(
                "UPDATE oa_import
                 SET conversation_count_imported = ?2,
                     conversation_count_failed = ?3,
                     import_status = ?4,
                     error_message = ?5,
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                 WHERE import_id = ?1",
                params![
                    import_id,
                    count_imported,
                    count_failed,
                    import_status.as_str(),
                    error_message
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;

        Ok(ImportWriteResult {
            import_id,
            import_status,
            artifacts,
            failed_artifact_ids,
            segments_written,
        })
    }
}
