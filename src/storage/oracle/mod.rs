pub mod artifact;
pub mod derivation;
pub mod import;
pub mod job;
pub mod segment;

use crate::config::OracleConfig;
use crate::db;
use crate::error::{StorageError, StorageResult};
use crate::storage::archive_retrieval_store::ArchiveRetrievalStore;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::types::ImportStatus;
use crate::storage::types::{
    ArtifactExtractionResult, ReconciliationDecision, RetrievalIntent, RetrievalResultSet,
    RetrievedContextItem,
};
use crate::storage::{
    ArtifactIngestResult, ArtifactReadStore, ImportWriteResult, ImportWriteStore, ImportedArtifact,
    StorageTx, WriteImportSet,
};
use log::error;

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
    config: OracleConfig,
}

impl OracleImportWriteStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

pub struct OracleArtifactReadStore {
    config: OracleConfig,
}

impl OracleArtifactReadStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl ArtifactReadStore for OracleArtifactReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let conn = db::connect(&self.config)?;
        artifact::list_artifacts(&conn)
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<crate::storage::types::LoadedArtifactForEnrichment>> {
        let conn = db::connect(&self.config)?;
        artifact::load_artifact_for_enrichment(&conn, artifact_id)
    }
}

pub struct OracleArchiveRetrievalStore {
    config: OracleConfig,
}

impl OracleArchiveRetrievalStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl ArchiveRetrievalStore for OracleArchiveRetrievalStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        let conn = db::connect(&self.config)?;
        let mut items = Vec::new();
        for intent in intents {
            let like_pattern = format!("%{}%", intent.query_text.to_ascii_lowercase());
            let derived_type_filter = match intent.intent_type.as_str() {
                "memory_match" => "AND d.derived_object_type = 'memory'",
                "relationship_lookup" => "AND d.derived_object_type = 'relationship'",
                "topic_lookup" => "AND d.derived_object_type IN ('classification', 'summary')",
                "contradiction_check" => "AND d.derived_object_type IN ('memory', 'relationship')",
                "entity_lookup" => "AND d.derived_object_type IN ('memory', 'relationship', 'classification', 'summary')",
                _ => "",
            };
            let sql = format!(
                "SELECT * FROM (
                    SELECT d.derived_object_type,
                           d.derived_object_id,
                           d.artifact_id,
                           d.title,
                           d.body_text
                      FROM oa_derived_object d
                     WHERE d.artifact_id <> :1
                       AND d.object_status = 'active'
                       {derived_type_filter}
                       AND (
                            LOWER(NVL(d.title, '')) LIKE :2
                            OR LOWER(NVL(d.body_text, '')) LIKE :2
                            OR LOWER(NVL(d.object_json, '')) LIKE :2
                       )
                ) WHERE ROWNUM <= :3"
            );
            let rows = conn
                .query(
                    &sql,
                    &[&artifact_id, &like_pattern, &(limit_per_intent as i64)],
                )
                .map_err(|source| StorageError::ListArtifacts { source })?;
            for row_result in rows {
                let row = row_result.map_err(|source| StorageError::ListArtifacts { source })?;
                items.push(RetrievedContextItem {
                    item_type: row.get::<_, String>(0).unwrap_or_default(),
                    object_id: row.get::<_, String>(1).unwrap_or_default(),
                    artifact_id: row.get::<_, String>(2).unwrap_or_default(),
                    title: row.get::<_, Option<String>>(3).unwrap_or(None),
                    body_text: row.get::<_, Option<String>>(4).unwrap_or(None),
                    supporting_segment_ids: Vec::new(),
                    retrieval_reason: intent.question.clone(),
                    matched_fields: vec!["title_or_body_text".to_string()],
                    rank_score: 100 - (items.len() as i32),
                });
            }
        }
        Ok(items)
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
        if let Some(existing_payload_object_id) =
            import::find_payload_object_id_by_sha256(&tx.conn, &import_set.payload_object.sha256)?
        {
            import_set.import.payload_object_id = existing_payload_object_id;
        } else if let Err(e) = import::insert_payload_object(&tx.conn, &import_set.payload_object) {
            if let Err(rollback_err) = tx.conn.rollback() {
                error!("Failed to rollback after payload insert error (operation=phase1_payload_insert): {}", rollback_err);
            }
            return Err(e);
        }
        if let Err(e) = import::insert_import(&tx.conn, &import_set.import) {
            if let Err(rollback_err) = tx.conn.rollback() {
                error!("Failed to rollback after import insert error (operation=phase1_import_insert): {}", rollback_err);
            }
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
            if let Err(rollback_err) = tx.conn.rollback() {
                error!("Failed to rollback after phase 3 finalize error (import_id={}, operation=phase3_finalize_import): {}", import_id, rollback_err);
            }
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
use crate::storage::types::{
    ClaimedJob, NewEnrichmentBatch, PersistedEnrichmentBatch, RetryOutcome,
};

pub struct OracleEnrichmentJobStore {
    config: OracleConfig,
}

impl OracleEnrichmentJobStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl EnrichmentJobLifecycleStore for OracleEnrichmentJobStore {
    fn enqueue_jobs(&self, jobs: &[crate::storage::types::NewEnrichmentJob]) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        for job in jobs {
            job::insert_job(&conn, job)?;
        }
        conn.commit().map_err(|source| StorageError::Commit {
            operation: "enqueue enrichment jobs",
            source,
        })?;
        Ok(())
    }

    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_next_job(&conn, worker_id)
    }

    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_matching_jobs(&conn, worker_id, template_job, limit)
    }

    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: crate::storage::types::JobType,
        enrichment_tier: Option<crate::storage::types::EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_jobs_by_type(&conn, worker_id, job_type, enrichment_tier, limit)
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

    fn record_batch_submission(
        &self,
        _batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn transition_batch_submission(
        &self,
        _completed_provider_batch_id: &str,
        _next_batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn complete_batch(&self, _provider_batch_id: &str) -> StorageResult<()> {
        Ok(())
    }

    fn fail_batch_record(
        &self,
        _provider_batch_id: &str,
        _error_message: &str,
    ) -> StorageResult<()> {
        Ok(())
    }

    fn load_running_batches(
        &self,
        _stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        Ok(Vec::new())
    }

    fn reconcile_stale_running_batches(&self, _stage_name: &str) -> StorageResult<usize> {
        Ok(0)
    }
}

pub struct OracleDerivedMetadataStore {
    config: OracleConfig,
}

impl OracleDerivedMetadataStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl DerivedMetadataWriteStore for OracleDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        let conn = db::connect(&self.config)?;
        let mut tx = OracleStorageTx { conn };

        derivation::validate_derivation_attempt(&tx.conn, &attempt)?;

        let result = (|| {
            derivation::supersede_active_derived_objects(&tx.conn, &attempt.run.artifact_id)?;
            derivation::insert_derivation_run(&tx.conn, &attempt.run)?;

            let mut derived_object_ids = Vec::with_capacity(attempt.objects.len());
            let mut evidence_links_written = 0usize;
            for object_write in &attempt.objects {
                derivation::insert_derived_object(&tx.conn, &object_write.object)?;
                derived_object_ids.push(object_write.object.derived_object_id.clone());

                for link in &object_write.evidence_links {
                    derivation::insert_evidence_link(&tx.conn, link)?;
                    evidence_links_written += 1;
                }
            }

            Ok(DerivationWriteResult {
                derivation_run_id: attempt.run.derivation_run_id.clone(),
                derived_object_ids,
                evidence_links_written,
            })
        })();

        match result {
            Ok(result) => {
                tx.commit()?;
                Ok(result)
            }
            Err(error) => {
                tx.conn
                    .rollback()
                    .map_err(|source| StorageError::Rollback {
                        operation: format!(
                            "failed derivation attempt {}",
                            attempt.run.derivation_run_id
                        ),
                        source,
                    })?;
                Err(error)
            }
        }
    }
}

impl EnrichmentStateStore for OracleDerivedMetadataStore {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        conn.execute(
            "MERGE INTO oa_artifact_extraction_result t
             USING (
                SELECT :1 AS extraction_result_id,
                       :2 AS artifact_id,
                       :3 AS job_id,
                       :4 AS pipeline_name,
                       :5 AS pipeline_version,
                       :6 AS result_json
                  FROM dual
             ) s
             ON (t.extraction_result_id = s.extraction_result_id)
             WHEN MATCHED THEN UPDATE SET
                 t.pipeline_name = s.pipeline_name,
                 t.pipeline_version = s.pipeline_version,
                 t.result_json = s.result_json
             WHEN NOT MATCHED THEN INSERT
                 (extraction_result_id, artifact_id, job_id, pipeline_name, pipeline_version, result_json)
                 VALUES (s.extraction_result_id, s.artifact_id, s.job_id, s.pipeline_name, s.pipeline_version, s.result_json)",
            &[
                &result.extraction_result_id,
                &result.artifact_id,
                &result.job_id,
                &result.pipeline_name,
                &result.pipeline_version,
                &serde_json::to_string(result).expect("extraction result serializable"),
            ],
        )
        .map_err(|source| StorageError::ListArtifacts { source })?;
        conn.commit().map_err(|source| StorageError::Commit {
            operation: "save extraction result",
            source,
        })?;
        Ok(())
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        let conn = db::connect(&self.config)?;
        let row = conn
            .query_row_as::<(String,)>(
                "SELECT result_json
                 FROM oa_artifact_extraction_result
                 WHERE extraction_result_id = :1",
                &[&extraction_result_id],
            )
            .map(|(json,)| json)
            .or_else(|source| match source.kind() {
                oracle::ErrorKind::NoDataFound => Ok(String::new()),
                _ => Err(StorageError::ListArtifacts { source }),
            })?;
        if row.is_empty() {
            return Ok(None);
        }
        serde_json::from_str(&row)
            .map(Some)
            .map_err(|err| StorageError::InvalidDerivationWrite {
                detail: format!(
                    "failed to deserialize extraction result {extraction_result_id}: {err}"
                ),
            })
    }

    fn save_retrieval_result_set(&self, result_set: &RetrievalResultSet) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        conn.execute(
            "MERGE INTO oa_retrieval_result_set t
             USING (
                SELECT :1 AS retrieval_result_set_id,
                       :2 AS artifact_id,
                       :3 AS job_id,
                       :4 AS extraction_result_id,
                       :5 AS pipeline_name,
                       :6 AS pipeline_version,
                       :7 AS result_json
                  FROM dual
             ) s
             ON (t.retrieval_result_set_id = s.retrieval_result_set_id)
             WHEN MATCHED THEN UPDATE SET
                 t.pipeline_name = s.pipeline_name,
                 t.pipeline_version = s.pipeline_version,
                 t.result_json = s.result_json
             WHEN NOT MATCHED THEN INSERT
                 (retrieval_result_set_id, artifact_id, job_id, extraction_result_id, pipeline_name, pipeline_version, result_json)
                 VALUES (s.retrieval_result_set_id, s.artifact_id, s.job_id, s.extraction_result_id, s.pipeline_name, s.pipeline_version, s.result_json)",
            &[
                &result_set.retrieval_result_set_id,
                &result_set.artifact_id,
                &result_set.job_id,
                &result_set.extraction_result_id,
                &result_set.pipeline_name,
                &result_set.pipeline_version,
                &serde_json::to_string(result_set).expect("retrieval result set serializable"),
            ],
        )
        .map_err(|source| StorageError::ListArtifacts { source })?;
        conn.commit().map_err(|source| StorageError::Commit {
            operation: "save retrieval result set",
            source,
        })?;
        Ok(())
    }

    fn load_retrieval_result_set(
        &self,
        retrieval_result_set_id: &str,
    ) -> StorageResult<Option<RetrievalResultSet>> {
        let conn = db::connect(&self.config)?;
        let row = conn
            .query_row_as::<(String,)>(
                "SELECT result_json
                 FROM oa_retrieval_result_set
                 WHERE retrieval_result_set_id = :1",
                &[&retrieval_result_set_id],
            )
            .map(|(json,)| json)
            .or_else(|source| match source.kind() {
                oracle::ErrorKind::NoDataFound => Ok(String::new()),
                _ => Err(StorageError::ListArtifacts { source }),
            })?;
        if row.is_empty() {
            return Ok(None);
        }
        serde_json::from_str(&row)
            .map(Some)
            .map_err(|err| StorageError::InvalidDerivationWrite {
                detail: format!(
                    "failed to deserialize retrieval result set {retrieval_result_set_id}: {err}"
                ),
            })
    }

    fn save_reconciliation_decisions(
        &self,
        decisions: &[ReconciliationDecision],
    ) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        for decision in decisions {
            conn.execute(
                "MERGE INTO oa_reconciliation_decision t
                 USING (
                    SELECT :1 AS reconciliation_decision_id,
                           :2 AS artifact_id,
                           :3 AS job_id,
                           :4 AS extraction_result_id,
                           :5 AS retrieval_result_set_id,
                           :6 AS pipeline_name,
                           :7 AS pipeline_version,
                           :8 AS decision_kind,
                           :9 AS target_kind,
                           :10 AS target_key,
                           :11 AS matched_object_id,
                           :12 AS rationale,
                           :13 AS evidence_segment_ids_json,
                           :14 AS decision_json
                      FROM dual
                 ) s
                 ON (t.reconciliation_decision_id = s.reconciliation_decision_id)
                 WHEN MATCHED THEN UPDATE SET
                     t.rationale = s.rationale,
                     t.decision_json = s.decision_json
                 WHEN NOT MATCHED THEN INSERT
                     (reconciliation_decision_id, artifact_id, job_id, extraction_result_id, retrieval_result_set_id,
                      pipeline_name, pipeline_version, decision_kind, target_kind, target_key, matched_object_id,
                      rationale, evidence_segment_ids_json, decision_json)
                     VALUES (s.reconciliation_decision_id, s.artifact_id, s.job_id, s.extraction_result_id,
                             s.retrieval_result_set_id, s.pipeline_name, s.pipeline_version, s.decision_kind,
                             s.target_kind, s.target_key, s.matched_object_id, s.rationale,
                             s.evidence_segment_ids_json, s.decision_json)",
                &[
                    &decision.reconciliation_decision_id,
                    &decision.artifact_id,
                    &decision.job_id,
                    &decision.extraction_result_id,
                    &decision.retrieval_result_set_id,
                    &decision.pipeline_name,
                    &decision.pipeline_version,
                    &serde_json::to_string(&decision.decision_kind).expect("decision kind serializable"),
                    &decision.target_kind,
                    &decision.target_key,
                    &decision.matched_object_id,
                    &decision.rationale,
                    &serde_json::to_string(&decision.evidence_segment_ids).expect("evidence ids serializable"),
                    &serde_json::to_string(decision).expect("decision serializable"),
                ],
            )
            .map_err(|source| StorageError::ListArtifacts { source })?;
        }
        conn.commit().map_err(|source| StorageError::Commit {
            operation: "save reconciliation decisions",
            source,
        })?;
        Ok(())
    }

    fn load_reconciliation_decisions(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Vec<ReconciliationDecision>> {
        let conn = db::connect(&self.config)?;
        let rows = conn
            .query(
                "SELECT decision_json
                 FROM oa_reconciliation_decision
                 WHERE extraction_result_id = :1
                 ORDER BY reconciliation_decision_id",
                &[&extraction_result_id],
            )
            .map_err(|source| StorageError::ListArtifacts { source })?;
        let mut decisions = Vec::new();
        for row_result in rows {
            let row = row_result.map_err(|source| StorageError::ListArtifacts { source })?;
            let decision_json = row
                .get::<_, String>(0)
                .map_err(|source| StorageError::ListArtifacts { source })?;
            let decision = serde_json::from_str::<ReconciliationDecision>(&decision_json).map_err(|err| {
                StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "failed to deserialize reconciliation decision for extraction result {extraction_result_id}: {err}"
                    ),
                }
            })?;
            decisions.push(decision);
        }
        Ok(decisions)
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
    config: &OracleConfig,
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
