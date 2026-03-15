pub mod artifact;
pub mod derivation;
pub mod import;
pub mod job;
pub mod segment;

use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db;
use crate::storage::archive_retrieval_store::ArchiveRetrievalStore;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::types::{
    ArtifactExtractionResult, ClaimedJob, ReconciliationDecision, RetrievalIntent,
    RetrievalResultSet, RetrievedContextItem, RetryOutcome,
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

impl ArtifactReadStore for PostgresImportWriteStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::list_artifacts(&mut client)
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<crate::storage::types::LoadedArtifactForEnrichment>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::load_artifact_for_enrichment(&mut client, artifact_id)
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

impl PostgresDerivedMetadataStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl ArchiveRetrievalStore for PostgresImportWriteStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        let mut client = postgres_db::connect(&self.config)?;
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
                "SELECT d.derived_object_type,
                        d.derived_object_id,
                        d.artifact_id,
                        d.title,
                        d.body_text,
                        COALESCE(array_agg(e.segment_id ORDER BY e.evidence_rank)
                            FILTER (WHERE e.segment_id IS NOT NULL), ARRAY[]::text[]) AS segment_ids
                 FROM oa_derived_object d
                 LEFT JOIN oa_evidence_link e ON e.derived_object_id = d.derived_object_id
                 WHERE d.artifact_id <> $1
                   AND d.object_status = 'active'
                   {derived_type_filter}
                   AND (
                        lower(COALESCE(d.title, '')) LIKE $2
                        OR lower(COALESCE(d.body_text, '')) LIKE $2
                        OR lower(COALESCE(d.object_json::text, '')) LIKE $2
                   )
                 GROUP BY d.derived_object_type, d.derived_object_id, d.artifact_id, d.title, d.body_text
                 ORDER BY
                   CASE
                     WHEN lower(COALESCE(d.title, '')) LIKE $2 THEN 4
                     WHEN lower(COALESCE(d.body_text, '')) LIKE $2 THEN 3
                     WHEN lower(COALESCE(d.object_json::text, '')) LIKE $2 THEN 2
                     ELSE 1
                   END DESC,
                   d.derived_object_id
                 LIMIT $3"
            );
            let rows = client
                .query(
                    &sql,
                    &[&artifact_id, &like_pattern, &(limit_per_intent as i64)],
                )
                .map_err(|source| {
                    crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                        connection_string: self.config.connection_string.clone(),
                        source,
                    })
                })?;
            for row in rows {
                let matched_title = row.get::<_, Option<String>>(3);
                let matched_body = row.get::<_, Option<String>>(4);
                let mut matched_fields = Vec::new();
                if matched_title
                    .as_deref()
                    .map(|title| {
                        title
                            .to_ascii_lowercase()
                            .contains(&intent.query_text.to_ascii_lowercase())
                    })
                    .unwrap_or(false)
                {
                    matched_fields.push("title".to_string());
                }
                if matched_body
                    .as_deref()
                    .map(|body| {
                        body.to_ascii_lowercase()
                            .contains(&intent.query_text.to_ascii_lowercase())
                    })
                    .unwrap_or(false)
                {
                    matched_fields.push("body_text".to_string());
                }
                items.push(RetrievedContextItem {
                    item_type: row.get::<_, String>(0),
                    object_id: row.get(1),
                    artifact_id: row.get(2),
                    title: matched_title,
                    body_text: matched_body,
                    supporting_segment_ids: row.get::<_, Vec<String>>(5),
                    retrieval_reason: intent.question.clone(),
                    matched_fields,
                    rank_score: 100 - (items.len() as i32),
                });
            }
        }
        Ok(items)
    }
}

impl EnrichmentStateStore for PostgresDerivedMetadataStore {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        client
            .execute(
                "INSERT INTO oa_artifact_extraction_result
                 (extraction_result_id, artifact_id, job_id, pipeline_name, pipeline_version, result_json)
                 VALUES ($1, $2, $3, $4, $5, $6::jsonb)
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
                 VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
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
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb)
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
