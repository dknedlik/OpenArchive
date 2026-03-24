pub mod artifact;
pub mod derivation;
pub mod embedding;
pub mod import;
pub mod job;
pub mod retrieval;
pub mod review;
pub mod segment;
pub mod writeback;

use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db;
use crate::postgres_db::SharedPostgresClient;
use crate::storage::archive_retrieval_store::ArchiveRetrievalStore;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::embedding_store::DerivedObjectEmbeddingStore;
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::retrieval_read_store::{
    ArchiveSearchCandidate, ArchiveSearchReadStore, ArtifactContextPackMaterial,
    ArtifactContextPackReadStore, ArtifactDetailReadStore, ArtifactDetailView,
    CrossArtifactReadStore, DerivedObjectSearchResult, DerivedObjectSearchStore, GraphRelatedEntry,
    ObjectSearchFilters, RelatedDerivedObject, SearchFilters,
};
use crate::storage::review_read_store::{
    NewReviewDecision, ReviewCandidate, ReviewQueueFilters, ReviewReadStore, ReviewWriteStore,
};
use crate::storage::types::{
    ArtifactExtractionResult, ClaimedJob, NewEnrichmentBatch, PersistedEnrichmentBatch,
    ReconciliationDecision, RetrievalIntent, RetrievalResultSet, RetrievedContextItem,
    RetryOutcome,
};
use crate::storage::{
    ArtifactIngestResult, ArtifactReadStore, ImportWriteResult, ImportWriteStore, ImportedArtifact,
    StorageTx, WriteImportSet,
};

pub struct PostgresStorageTx {
    client: postgres::Client,
}

impl StorageTx for PostgresStorageTx {
    fn commit(&mut self) -> StorageResult<()> {
        self.client.batch_execute("COMMIT").map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: "transaction".to_string(),
                source,
            })
        })
    }
}

pub struct PostgresImportWriteStore {
    config: PostgresConfig,
}

impl PostgresImportWriteStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

pub struct PostgresArtifactReadStore {
    config: PostgresConfig,
}

impl PostgresArtifactReadStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl ArtifactReadStore for PostgresArtifactReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::list_artifacts(&mut client)
    }

    fn list_artifacts_filtered(
        &self,
        filters: &crate::storage::types::ArtifactListFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::list_artifacts_filtered(&mut client, filters, limit, offset)
    }

    fn get_timeline(
        &self,
        filters: &crate::storage::types::TimelineFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<crate::storage::types::TimelineEntry>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::get_timeline(&mut client, filters, limit, offset)
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<crate::storage::types::LoadedArtifactForEnrichment>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::load_artifact_for_enrichment(&mut client, artifact_id)
    }
}

pub struct PostgresArchiveRetrievalStore {
    client: SharedPostgresClient,
}

impl PostgresArchiveRetrievalStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

pub struct PostgresRetrievalReadStore {
    client: SharedPostgresClient,
}

impl PostgresRetrievalReadStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

impl ImportWriteStore for PostgresImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let mut client = postgres_db::connect(&self.config)?;
        client.batch_execute("BEGIN").map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            })
        })?;
        let mut tx = PostgresStorageTx { client };

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

            tx.client.batch_execute("BEGIN").map_err(|source| {
                crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: self.config.connection_string.clone(),
                    source,
                })
            })?;

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
                Err(e) => {
                    tx.client.batch_execute("ROLLBACK").map_err(|source| {
                        crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                            connection_string: self.config.connection_string.clone(),
                            source,
                        })
                    })?;
                    failed_artifact_ids.push(artifact_id.clone());
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {e:#}", artifact_id));
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

        tx.client.batch_execute("BEGIN").map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            })
        })?;
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

pub struct PostgresEnrichmentJobStore {
    config: PostgresConfig,
}

pub struct PostgresDerivedMetadataStore {
    config: PostgresConfig,
}

pub struct PostgresDerivedObjectEmbeddingStore {
    client: SharedPostgresClient,
}

impl PostgresDerivedMetadataStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl PostgresDerivedObjectEmbeddingStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

impl ArchiveRetrievalStore for PostgresArchiveRetrievalStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        self.client.with_client(|client| {
            retrieval::retrieve_for_intents(
                client,
                self.client.connection_string(),
                artifact_id,
                intents,
                limit_per_intent,
            )
        })
    }
}

impl ArchiveRetrievalStore for PostgresRetrievalReadStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        self.client.with_client(|client| {
            retrieval::retrieve_for_intents(
                client,
                self.client.connection_string(),
                artifact_id,
                intents,
                limit_per_intent,
            )
        })
    }
}

impl ArchiveSearchReadStore for PostgresRetrievalReadStore {
    fn search_candidates(
        &self,
        query_text: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> StorageResult<Vec<ArchiveSearchCandidate>> {
        self.client.with_client(|client| {
            retrieval::search_candidates(
                client,
                self.client.connection_string(),
                query_text,
                limit,
                filters,
            )
        })
    }
}

impl ArtifactDetailReadStore for PostgresRetrievalReadStore {
    fn load_artifact_detail(&self, artifact_id: &str) -> StorageResult<Option<ArtifactDetailView>> {
        self.client.with_client(|client| {
            retrieval::load_artifact_detail(client, self.client.connection_string(), artifact_id)
        })
    }
}

impl ArtifactContextPackReadStore for PostgresRetrievalReadStore {
    fn load_artifact_context_pack_material(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<ArtifactContextPackMaterial>> {
        self.client.with_client(|client| {
            retrieval::load_artifact_context_pack_material(
                client,
                self.client.connection_string(),
                artifact_id,
            )
        })
    }
}

impl DerivedObjectSearchStore for PostgresRetrievalReadStore {
    fn search_objects(
        &self,
        filters: &ObjectSearchFilters,
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        self.client.with_client(|client| {
            retrieval::search_objects(client, self.client.connection_string(), filters, limit)
        })
    }

    fn search_objects_by_embedding(
        &self,
        filters: &ObjectSearchFilters,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        self.client.with_client(|client| {
            retrieval::search_objects_by_embedding(
                client,
                self.client.connection_string(),
                filters,
                query_embedding,
                limit,
            )
        })
    }

    fn get_related_objects(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<GraphRelatedEntry>> {
        self.client.with_client(|client| {
            retrieval::get_related_objects(
                client,
                self.client.connection_string(),
                derived_object_id,
                limit,
            )
        })
    }
}

impl CrossArtifactReadStore for PostgresRetrievalReadStore {
    fn find_related_by_candidate_keys(
        &self,
        artifact_id: &str,
        candidate_keys: &[String],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>> {
        self.client.with_client(|client| {
            retrieval::find_related_by_candidate_keys(
                client,
                self.client.connection_string(),
                artifact_id,
                candidate_keys,
                limit,
            )
        })
    }
}

impl ReviewReadStore for PostgresRetrievalReadStore {
    fn list_review_candidates(
        &self,
        filters: &ReviewQueueFilters,
        limit: usize,
    ) -> StorageResult<Vec<ReviewCandidate>> {
        self.client.with_client(|client| {
            review::list_review_candidates(client, self.client.connection_string(), filters, limit)
        })
    }
}

impl ReviewWriteStore for PostgresRetrievalReadStore {
    fn record_review_decision(&self, decision: &NewReviewDecision) -> StorageResult<()> {
        self.client.with_client(|client| {
            review::record_review_decision(client, self.client.connection_string(), decision)
        })
    }

    fn retry_artifact_enrichment(&self, artifact_id: &str) -> StorageResult<String> {
        self.client.with_client(|client| {
            review::retry_artifact_enrichment(client, self.client.connection_string(), artifact_id)
        })
    }
}

impl EnrichmentStateStore for PostgresDerivedMetadataStore {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        client
            .execute(
                "INSERT INTO oa_artifact_extraction_result
                 (extraction_result_id, artifact_id, job_id, pipeline_name, pipeline_version, result_json)
                 VALUES ($1, $2, $3, $4, $5, $6::text::jsonb)
                 ON CONFLICT (extraction_result_id) DO UPDATE
                 SET result_json = EXCLUDED.result_json,
                     pipeline_name = EXCLUDED.pipeline_name,
                     pipeline_version = EXCLUDED.pipeline_version",
                &[
                    &result.extraction_result_id,
                    &result.artifact_id,
                    &result.job_id,
                    &result.pipeline_name,
                    &result.pipeline_version,
                    &serde_json::to_string(result).expect("extraction result serializable"),
                ],
            )
            .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            }))?;
        Ok(())
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        let mut client = postgres_db::connect(&self.config)?;
        let row = client
            .query_opt(
                "SELECT result_json::text
                 FROM oa_artifact_extraction_result
                 WHERE extraction_result_id = $1",
                &[&extraction_result_id],
            )
            .map_err(|source| {
                crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: self.config.connection_string.clone(),
                    source,
                })
            })?;
        row.map(|row| serde_json::from_str::<ArtifactExtractionResult>(&row.get::<_, String>(0)))
            .transpose()
            .map_err(|err| crate::error::StorageError::InvalidDerivationWrite {
                detail: format!(
                    "failed to deserialize extraction result {extraction_result_id}: {err}"
                ),
            })
    }

    fn save_retrieval_result_set(&self, result_set: &RetrievalResultSet) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        client
            .execute(
                "INSERT INTO oa_retrieval_result_set
                 (retrieval_result_set_id, artifact_id, job_id, extraction_result_id, pipeline_name, pipeline_version, result_json)
                 VALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb)
                 ON CONFLICT (retrieval_result_set_id) DO UPDATE
                 SET result_json = EXCLUDED.result_json,
                     pipeline_name = EXCLUDED.pipeline_name,
                     pipeline_version = EXCLUDED.pipeline_version",
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
            .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            }))?;
        Ok(())
    }

    fn load_retrieval_result_set(
        &self,
        retrieval_result_set_id: &str,
    ) -> StorageResult<Option<RetrievalResultSet>> {
        let mut client = postgres_db::connect(&self.config)?;
        let row = client
            .query_opt(
                "SELECT result_json::text
                 FROM oa_retrieval_result_set
                 WHERE retrieval_result_set_id = $1",
                &[&retrieval_result_set_id],
            )
            .map_err(|source| {
                crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: self.config.connection_string.clone(),
                    source,
                })
            })?;
        row.map(|row| serde_json::from_str::<RetrievalResultSet>(&row.get::<_, String>(0)))
            .transpose()
            .map_err(|err| crate::error::StorageError::InvalidDerivationWrite {
                detail: format!(
                    "failed to deserialize retrieval result set {retrieval_result_set_id}: {err}"
                ),
            })
    }

    fn save_reconciliation_decisions(
        &self,
        decisions: &[ReconciliationDecision],
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        for decision in decisions {
            client
                .execute(
                    "INSERT INTO oa_reconciliation_decision
                     (reconciliation_decision_id, artifact_id, job_id, extraction_result_id, retrieval_result_set_id,
                      pipeline_name, pipeline_version, decision_kind, target_kind, target_key, matched_object_id,
                      rationale, evidence_segment_ids_json, decision_json)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb, $9, $10, $11, $12, $13::text::jsonb, $14::text::jsonb)
                     ON CONFLICT (reconciliation_decision_id) DO UPDATE
                     SET decision_json = EXCLUDED.decision_json,
                         rationale = EXCLUDED.rationale",
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
                .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: self.config.connection_string.clone(),
                    source,
                }))?;
        }
        Ok(())
    }

    fn load_reconciliation_decisions(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Vec<ReconciliationDecision>> {
        let mut client = postgres_db::connect(&self.config)?;
        let rows = client
            .query(
                "SELECT decision_json::text
                 FROM oa_reconciliation_decision
                 WHERE extraction_result_id = $1
                 ORDER BY reconciliation_decision_id",
                &[&extraction_result_id],
            )
            .map_err(|source| {
                crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: self.config.connection_string.clone(),
                    source,
                })
            })?;
        rows.into_iter()
            .map(|row| {
                serde_json::from_str::<ReconciliationDecision>(&row.get::<_, String>(0)).map_err(|err| {
                    crate::error::StorageError::InvalidDerivationWrite {
                        detail: format!(
                            "failed to deserialize reconciliation decision for extraction result {extraction_result_id}: {err}"
                        ),
                    }
                })
            })
            .collect()
    }
}

impl PostgresEnrichmentJobStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl EnrichmentJobLifecycleStore for PostgresEnrichmentJobStore {
    fn enqueue_jobs(&self, jobs: &[crate::storage::types::NewEnrichmentJob]) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        client.batch_execute("BEGIN").map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            })
        })?;
        for job in jobs {
            job::insert_job(&mut client, job)?;
        }
        client.batch_execute("COMMIT").map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            })
        })?;
        Ok(())
    }

    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_next_job(&mut client, worker_id)
    }

    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_matching_jobs(&mut client, worker_id, template_job, limit)
    }

    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: crate::storage::types::JobType,
        enrichment_tier: Option<crate::storage::types::EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_jobs_by_type(&mut client, worker_id, job_type, enrichment_tier, limit)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::complete_job(&mut client, worker_id, job_id)
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::fail_job(&mut client, worker_id, job_id, error_message)
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let mut client = postgres_db::connect(&self.config)?;
        job::mark_job_retryable(
            &mut client,
            worker_id,
            job_id,
            error_message,
            retry_after_seconds,
        )
    }

    fn reschedule_running_job(
        &self,
        worker_id: &str,
        job_id: &str,
        message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::reschedule_running_job(&mut client, worker_id, job_id, message, retry_after_seconds)
    }

    fn record_batch_submission(
        &self,
        batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::record_batch_submission(&mut client, batch, jobs)
    }

    fn transition_batch_submission(
        &self,
        completed_provider_batch_id: &str,
        next_batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::transition_batch_submission(&mut client, completed_provider_batch_id, next_batch, jobs)
    }

    fn complete_batch(&self, provider_batch_id: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::complete_batch(&mut client, provider_batch_id)
    }

    fn fail_batch_record(&self, provider_batch_id: &str, error_message: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::fail_batch_record(&mut client, provider_batch_id, error_message)
    }

    fn load_running_batches(
        &self,
        stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::load_running_batches(&mut client, stage_name)
    }

    fn reconcile_stale_running_batches(&self, stage_name: &str) -> StorageResult<usize> {
        let mut client = postgres_db::connect(&self.config)?;
        job::reconcile_stale_running_batches(&mut client, stage_name)
    }

    fn reconcile_stale_running_jobs(&self, stage_name: &str) -> StorageResult<usize> {
        let mut client = postgres_db::connect(&self.config)?;
        job::reconcile_stale_running_jobs(&mut client, stage_name)
    }
}

pub struct PostgresWritebackStore {
    client: SharedPostgresClient,
}

impl PostgresWritebackStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

impl DerivedObjectEmbeddingStore for PostgresDerivedObjectEmbeddingStore {
    fn upsert_embeddings(
        &self,
        embeddings: &[crate::storage::types::NewDerivedObjectEmbedding],
    ) -> StorageResult<()> {
        self.client.with_client(|client| {
            embedding::upsert_embeddings(client, self.client.connection_string(), embeddings)
        })
    }
}

impl crate::storage::writeback_store::WritebackStore for PostgresWritebackStore {
    fn store_agent_memory(
        &self,
        memory: &crate::storage::writeback_store::NewAgentMemory,
    ) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::store_agent_memory(client, self.client.connection_string(), memory)
        })
    }

    fn store_archive_link(
        &self,
        link: &crate::storage::writeback_store::NewArchiveLink,
    ) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::store_archive_link(client, self.client.connection_string(), link)
        })
    }

    fn update_object_status(
        &self,
        update: &crate::storage::writeback_store::UpdateObjectStatus,
    ) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::update_object_status(client, self.client.connection_string(), update)
        })
    }

    fn store_agent_entity(
        &self,
        entity: &crate::storage::writeback_store::NewAgentEntity,
    ) -> StorageResult<()> {
        self.client.with_client(|client| {
            writeback::store_agent_entity(client, self.client.connection_string(), entity)
        })
    }
}

impl DerivedMetadataWriteStore for PostgresDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        let mut client = postgres_db::connect(&self.config)?;
        client.batch_execute("BEGIN").map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            })
        })?;

        let result = (|| {
            derivation::validate_derivation_attempt(
                &mut client,
                &self.config.connection_string,
                &attempt,
            )?;
            derivation::supersede_active_derived_objects(
                &mut client,
                &self.config.connection_string,
                &attempt.run.artifact_id,
            )?;
            derivation::insert_derivation_run(
                &mut client,
                &self.config.connection_string,
                &attempt.run,
            )?;

            let mut derived_object_ids = Vec::with_capacity(attempt.objects.len());
            let mut evidence_links_written = 0usize;
            for object_write in &attempt.objects {
                derivation::insert_derived_object(
                    &mut client,
                    &self.config.connection_string,
                    &object_write.object,
                )?;
                derived_object_ids.push(object_write.object.derived_object_id.clone());

                for link in &object_write.evidence_links {
                    derivation::insert_evidence_link(
                        &mut client,
                        &self.config.connection_string,
                        link,
                    )?;
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
                client.batch_execute("COMMIT").map_err(|source| {
                    crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                        connection_string: self.config.connection_string.clone(),
                        source,
                    })
                })?;
                Ok(result)
            }
            Err(error) => {
                client.batch_execute("ROLLBACK").map_err(|source| {
                    crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                        connection_string: self.config.connection_string.clone(),
                        source,
                    })
                })?;
                Err(error)
            }
        }
    }
}
