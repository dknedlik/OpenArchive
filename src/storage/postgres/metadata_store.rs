use serde::de::DeserializeOwned;

use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::types::{ArtifactExtractionResult, ReconciliationDecision};

use super::derivation;
use super::tx::{begin_transaction, commit_transaction, rollback_transaction, storage_db_error};

pub struct PostgresDerivedMetadataStore {
    config: PostgresConfig,
}

impl PostgresDerivedMetadataStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

fn deserialize_json<T: DeserializeOwned>(json: &str, detail: String) -> StorageResult<T> {
    serde_json::from_str(json).map_err(|err| crate::error::StorageError::InvalidDerivationWrite {
        detail: format!("{detail}: {err}"),
    })
}

fn load_json_record<T: DeserializeOwned>(
    client: &mut postgres::Client,
    connection_string: &str,
    sql: &str,
    record_id: &str,
    record_label: &str,
) -> StorageResult<Option<T>> {
    let row = client
        .query_opt(sql, &[&record_id])
        .map_err(|source| storage_db_error(connection_string, source))?;
    row.map(|row| {
        deserialize_json::<T>(
            &row.get::<_, String>(0),
            format!("failed to deserialize {record_label} {record_id}"),
        )
    })
    .transpose()
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
            .map_err(|source| storage_db_error(&self.config.connection_string, source))?;
        Ok(())
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        let mut client = postgres_db::connect(&self.config)?;
        load_json_record(
            &mut client,
            &self.config.connection_string,
            "SELECT result_json::text
             FROM oa_artifact_extraction_result
             WHERE extraction_result_id = $1",
            extraction_result_id,
            "extraction result",
        )
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
                     (reconciliation_decision_id, artifact_id, job_id, extraction_result_id,
                      pipeline_name, pipeline_version, decision_kind, target_kind, target_key, matched_object_id,
                      rationale, decision_json)
                     VALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb, $8, $9, $10, $11, $12::text::jsonb)
                     ON CONFLICT (reconciliation_decision_id) DO UPDATE
                     SET decision_json = EXCLUDED.decision_json,
                         rationale = EXCLUDED.rationale",
                    &[
                        &decision.reconciliation_decision_id,
                        &decision.artifact_id,
                        &decision.job_id,
                        &decision.extraction_result_id,
                        &decision.pipeline_name,
                        &decision.pipeline_version,
                        &serde_json::to_string(&decision.decision_kind)
                            .expect("decision kind serializable"),
                        &decision.target_kind,
                        &decision.target_key,
                        &decision.matched_object_id,
                        &decision.rationale,
                        &serde_json::to_string(decision).expect("decision serializable"),
                    ],
                )
                .map_err(|source| storage_db_error(&self.config.connection_string, source))?;
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
            .map_err(|source| storage_db_error(&self.config.connection_string, source))?;
        rows.into_iter()
            .map(|row| {
                deserialize_json::<ReconciliationDecision>(
                    &row.get::<_, String>(0),
                    format!(
                        "failed to deserialize reconciliation decision for extraction result {extraction_result_id}"
                    ),
                )
            })
            .collect()
    }
}

impl DerivedMetadataWriteStore for PostgresDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        let mut client = postgres_db::connect(&self.config)?;
        begin_transaction(&mut client, &self.config.connection_string)?;

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
            for object_write in &attempt.objects {
                derivation::insert_derived_object(
                    &mut client,
                    &self.config.connection_string,
                    &object_write.object,
                )?;
                derived_object_ids.push(object_write.object.derived_object_id.clone());
            }

            for link in &attempt.archive_links {
                derivation::insert_archive_link(&mut client, &self.config.connection_string, link)?;
            }

            Ok(DerivationWriteResult {
                derivation_run_id: attempt.run.derivation_run_id.clone(),
                derived_object_ids,
            })
        })();

        match result {
            Ok(result) => {
                commit_transaction(&mut client, &self.config.connection_string)?;
                Ok(result)
            }
            Err(error) => {
                rollback_transaction(&mut client, &self.config.connection_string)?;
                Err(error)
            }
        }
    }
}
