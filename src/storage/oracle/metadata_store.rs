use serde::de::DeserializeOwned;

use crate::config::OracleConfig;
use crate::db;
use crate::error::{StorageError, StorageResult};
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::types::{ArtifactExtractionResult, ReconciliationDecision, RetrievalResultSet};
use crate::storage::StorageTx;

use super::derivation;
use super::tx::{begin_storage_tx, commit_connection, rollback_connection};

pub struct OracleDerivedMetadataStore {
    config: OracleConfig,
}

impl OracleDerivedMetadataStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

fn deserialize_json<T: DeserializeOwned>(json: &str, detail: String) -> StorageResult<T> {
    serde_json::from_str(json).map_err(|err| StorageError::InvalidDerivationWrite {
        detail: format!("{detail}: {err}"),
    })
}

fn load_json_row<T: DeserializeOwned>(
    conn: &oracle::Connection,
    sql: &str,
    id: &str,
    label: &str,
) -> StorageResult<Option<T>> {
    let row = conn
        .query_row_as::<(String,)>(sql, &[&id])
        .map(|(json,)| json)
        .or_else(|source| match source.kind() {
            oracle::ErrorKind::NoDataFound => Ok(String::new()),
            _ => Err(StorageError::ListArtifacts {
                source: Box::new(source),
            }),
        })?;
    if row.is_empty() {
        return Ok(None);
    }
    deserialize_json(&row, format!("failed to deserialize {label} {id}")).map(Some)
}

impl DerivedMetadataWriteStore for OracleDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        let mut tx = begin_storage_tx(&self.config)?;

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

            for link in &attempt.archive_links {
                derivation::insert_archive_link(&tx.conn, link)?;
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
                rollback_connection(
                    &tx.conn,
                    format!(
                        "failed derivation attempt {}",
                        attempt.run.derivation_run_id
                    ),
                )?;
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
        .map_err(|source| StorageError::ListArtifacts { source: Box::new(source) })?;
        commit_connection(&conn, "save extraction result")?;
        Ok(())
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        let conn = db::connect(&self.config)?;
        load_json_row(
            &conn,
            "SELECT result_json
             FROM oa_artifact_extraction_result
             WHERE extraction_result_id = :1",
            extraction_result_id,
            "extraction result",
        )
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
        .map_err(|source| StorageError::ListArtifacts { source: Box::new(source) })?;
        commit_connection(&conn, "save retrieval result set")?;
        Ok(())
    }

    fn load_retrieval_result_set(
        &self,
        retrieval_result_set_id: &str,
    ) -> StorageResult<Option<RetrievalResultSet>> {
        let conn = db::connect(&self.config)?;
        load_json_row(
            &conn,
            "SELECT result_json
             FROM oa_retrieval_result_set
             WHERE retrieval_result_set_id = :1",
            retrieval_result_set_id,
            "retrieval result set",
        )
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
                    &serde_json::to_string(&decision.decision_kind)
                        .expect("decision kind serializable"),
                    &decision.target_kind,
                    &decision.target_key,
                    &decision.matched_object_id,
                    &decision.rationale,
                    &serde_json::to_string(&decision.evidence_segment_ids)
                        .expect("evidence ids serializable"),
                    &serde_json::to_string(decision).expect("decision serializable"),
                ],
            )
            .map_err(|source| StorageError::ListArtifacts { source: Box::new(source) })?;
        }
        commit_connection(&conn, "save reconciliation decisions")?;
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
            .map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
        let mut decisions = Vec::new();
        for row_result in rows {
            let row = row_result.map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            let decision_json =
                row.get::<_, String>(0)
                    .map_err(|source| StorageError::ListArtifacts {
                        source: Box::new(source),
                    })?;
            decisions.push(deserialize_json(
                &decision_json,
                format!(
                    "failed to deserialize reconciliation decision for extraction result {extraction_result_id}"
                ),
            )?);
        }
        Ok(decisions)
    }
}
