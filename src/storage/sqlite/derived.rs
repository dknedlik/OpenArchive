use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::domain::SourceTimestamp;
use crate::error::StorageError;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::types::{ArtifactExtractionResult, ReconciliationDecision};

use super::job::recompute_artifact_enrichment_status_sqlite;
use super::links::sync_reconcile_links_for_archive_links;
use super::{
    db_err, deserialize_json, open_connection, with_sqlite_write_retry, SqliteDerivedMetadataStore,
    StorageResult,
};

impl EnrichmentStateStore for SqliteDerivedMetadataStore {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()> {
        with_sqlite_write_retry(&self.config, || {
            let connection = open_connection(&self.config)?;
            connection
                .execute(
                    "INSERT INTO oa_artifact_extraction_result
                     (extraction_result_id, artifact_id, job_id, pipeline_name, pipeline_version, result_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(extraction_result_id) DO UPDATE SET
                         result_json = excluded.result_json,
                         pipeline_name = excluded.pipeline_name,
                         pipeline_version = excluded.pipeline_version",
                    params![
                        result.extraction_result_id,
                        result.artifact_id,
                        result.job_id,
                        result.pipeline_name,
                        result.pipeline_version,
                        serde_json::to_string(result).expect("extraction result serializable")
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
            Ok(())
        })
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        let connection = open_connection(&self.config)?;
        let json: Option<String> = connection
            .query_row(
                "SELECT result_json FROM oa_artifact_extraction_result WHERE extraction_result_id = ?1",
                [extraction_result_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?;
        json.map(|json| deserialize_json(&json, "failed to deserialize extraction result"))
            .transpose()
    }

    fn save_reconciliation_decisions(
        &self,
        decisions: &[ReconciliationDecision],
    ) -> StorageResult<()> {
        with_sqlite_write_retry(&self.config, || {
            let mut connection = open_connection(&self.config)?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            for decision in decisions {
                tx.execute(
                    "INSERT INTO oa_reconciliation_decision
                     (reconciliation_decision_id, artifact_id, job_id, extraction_result_id, pipeline_name,
                      pipeline_version, decision_kind, target_kind, target_key, matched_object_id, rationale, decision_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                     ON CONFLICT(reconciliation_decision_id) DO UPDATE SET
                        decision_json = excluded.decision_json,
                        rationale = excluded.rationale",
                    params![
                        decision.reconciliation_decision_id,
                        decision.artifact_id,
                        decision.job_id,
                        decision.extraction_result_id,
                        decision.pipeline_name,
                        decision.pipeline_version,
                        serde_json::to_string(&decision.decision_kind).expect("decision kind serializable"),
                        decision.target_kind,
                        decision.target_key,
                        decision.matched_object_id,
                        decision.rationale,
                        serde_json::to_string(decision).expect("decision serializable")
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
            }
            tx.commit().map_err(|source| db_err(&self.config, source))?;
            Ok(())
        })
    }

    fn load_reconciliation_decisions(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Vec<ReconciliationDecision>> {
        let connection = open_connection(&self.config)?;
        let mut stmt = connection
            .prepare(
                "SELECT decision_json
                 FROM oa_reconciliation_decision
                 WHERE extraction_result_id = ?1
                 ORDER BY reconciliation_decision_id",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map([extraction_result_id], |row| row.get::<_, String>(0))
            .map_err(|source| db_err(&self.config, source))?;
        let mut decisions = Vec::new();
        for row in rows {
            decisions.push(deserialize_json(
                &row.map_err(|source| db_err(&self.config, source))?,
                "failed to deserialize reconciliation decision",
            )?);
        }
        Ok(decisions)
    }
}

impl DerivedMetadataWriteStore for SqliteDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        with_sqlite_write_retry(&self.config, || {
            let mut connection = open_connection(&self.config)?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;

            let artifact_exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM oa_artifact WHERE artifact_id = ?1)",
                    [&attempt.run.artifact_id],
                    |row| row.get(0),
                )
                .map_err(|source| db_err(&self.config, source))?;
            if !artifact_exists {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!("artifact {} does not exist", attempt.run.artifact_id),
                });
            }

            tx.execute(
                "UPDATE oa_derived_object SET object_status = 'superseded'
                 WHERE artifact_id = ?1 AND object_status = 'active'",
                [&attempt.run.artifact_id],
            )
            .map_err(|source| db_err(&self.config, source))?;

            tx.execute(
                "INSERT INTO oa_derivation_run
                 (derivation_run_id, artifact_id, job_id, run_type, pipeline_name, pipeline_version,
                  provider_name, model_name, prompt_version, run_status, input_scope_type, input_scope_json,
                  started_at, completed_at, error_message)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    attempt.run.derivation_run_id,
                    attempt.run.artifact_id,
                    attempt.run.job_id,
                    attempt.run.run_type.as_str(),
                    attempt.run.pipeline_name,
                    attempt.run.pipeline_version,
                    attempt.run.provider_name,
                    attempt.run.model_name,
                    attempt.run.prompt_version,
                    attempt.run.run_status.as_str(),
                    attempt.run.input_scope_type.as_str(),
                    attempt.run.input_scope_json,
                    attempt.run.started_at.as_str(),
                    attempt.run.completed_at.as_ref().map(SourceTimestamp::as_str),
                    attempt.run.error_message
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;

            let mut derived_object_ids = Vec::with_capacity(attempt.objects.len());
            for object_write in &attempt.objects {
                let object = &object_write.object;
                tx.execute(
                    "INSERT INTO oa_derived_object
                     (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind,
                      object_status, confidence_score, confidence_label, scope_type, scope_id, title,
                      body_text, object_json, supersedes_derived_object_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        object.derived_object_id,
                        object.artifact_id,
                        object.derivation_run_id,
                        object.payload.derived_object_type().as_str(),
                        object.origin_kind.as_str(),
                        object.object_status.as_str(),
                        object.confidence_score,
                        object.confidence_label,
                        object.scope_type.as_str(),
                        object.scope_id,
                        object.payload.title(),
                        object.payload.body_text(),
                        object.payload.object_json(),
                        object.supersedes_derived_object_id
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
                derived_object_ids.push(object.derived_object_id.clone());
            }

            for link in &attempt.archive_links {
                tx.execute(
                    "INSERT INTO oa_archive_link
                     (archive_link_id, source_object_id, target_object_id, link_type, confidence_score, rationale, origin_kind, contributed_by)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'inferred', ?7)
                     ON CONFLICT(source_object_id, target_object_id, link_type) DO UPDATE SET
                        confidence_score = COALESCE(excluded.confidence_score, oa_archive_link.confidence_score),
                        rationale = COALESCE(excluded.rationale, oa_archive_link.rationale),
                        contributed_by = COALESCE(excluded.contributed_by, oa_archive_link.contributed_by)",
                    params![
                        link.archive_link_id,
                        link.source_object_id,
                        link.target_object_id,
                        link.link_type,
                        link.confidence_score,
                        link.rationale,
                        link.contributed_by
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
            }
            sync_reconcile_links_for_archive_links(&tx, &attempt.archive_links)?;
            recompute_artifact_enrichment_status_sqlite(&tx, &attempt.run.artifact_id)?;
            tx.commit().map_err(|source| db_err(&self.config, source))?;

            Ok(DerivationWriteResult {
                derivation_run_id: attempt.run.derivation_run_id.clone(),
                derived_object_ids,
            })
        })
    }
}
