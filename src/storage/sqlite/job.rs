use rusqlite::types::Type;
use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::error::StorageError;
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::types::{
    ClaimedJob, EnrichmentStatus, EnrichmentTier, JobType, NewEnrichmentBatch,
    PersistedEnrichmentBatch, RetryOutcome,
};

use super::{
    db_err, open_connection, parse_enrichment_tier, parse_job_type, SqliteEnrichmentJobStore,
    StorageResult,
};

#[derive(Default)]
pub(super) struct ArtifactEnrichmentSnapshot {
    pub(super) pending_jobs: i64,
    pub(super) running_jobs: i64,
    pub(super) retryable_jobs: i64,
    pub(super) completed_jobs: i64,
    pub(super) failed_jobs: i64,
    pub(super) extraction_results: i64,
    pub(super) reconciliation_decisions: i64,
    pub(super) completed_derivation_runs: i64,
}

pub(super) fn derive_artifact_enrichment_status(
    snapshot: ArtifactEnrichmentSnapshot,
) -> EnrichmentStatus {
    let has_running = snapshot.running_jobs > 0 || snapshot.retryable_jobs > 0;
    let has_pending = snapshot.pending_jobs > 0;
    let has_failed = snapshot.failed_jobs > 0;
    let has_completed_jobs = snapshot.completed_jobs > 0;
    let has_durable_outputs = snapshot.extraction_results > 0
        || snapshot.reconciliation_decisions > 0
        || snapshot.completed_derivation_runs > 0;
    let is_fully_derived = snapshot.completed_derivation_runs > 0;

    if is_fully_derived && !has_running && !has_pending {
        return EnrichmentStatus::Completed;
    }
    if has_running {
        return if has_durable_outputs {
            EnrichmentStatus::Partial
        } else {
            EnrichmentStatus::Running
        };
    }
    if has_pending {
        return if has_durable_outputs {
            EnrichmentStatus::Partial
        } else if has_completed_jobs {
            EnrichmentStatus::Running
        } else {
            EnrichmentStatus::Pending
        };
    }
    if has_failed {
        return if has_durable_outputs {
            EnrichmentStatus::Partial
        } else {
            EnrichmentStatus::Failed
        };
    }
    if has_durable_outputs {
        return EnrichmentStatus::Partial;
    }
    if has_completed_jobs {
        return EnrichmentStatus::Running;
    }
    EnrichmentStatus::Pending
}

pub(super) fn recompute_artifact_enrichment_status_sqlite(
    connection: &rusqlite::Connection,
    artifact_id: &str,
) -> StorageResult<EnrichmentStatus> {
    let snapshot = connection
        .query_row(
            "SELECT
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'pending'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'running'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'retryable'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'completed'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'failed'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_artifact_extraction_result WHERE artifact_id = ?1), 0),
                COALESCE((SELECT COUNT(*) FROM oa_reconciliation_decision WHERE artifact_id = ?1), 0),
                COALESCE((SELECT COUNT(*) FROM oa_derivation_run WHERE artifact_id = ?1 AND run_status = 'completed'), 0)",
            [artifact_id],
            |row| {
                Ok(ArtifactEnrichmentSnapshot {
                    pending_jobs: row.get(0)?,
                    running_jobs: row.get(1)?,
                    retryable_jobs: row.get(2)?,
                    completed_jobs: row.get(3)?,
                    failed_jobs: row.get(4)?,
                    extraction_results: row.get(5)?,
                    reconciliation_decisions: row.get(6)?,
                    completed_derivation_runs: row.get(7)?,
                })
            },
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to compute enrichment snapshot for {artifact_id}: {source}"),
        })?;
    let next_status = derive_artifact_enrichment_status(snapshot);
    connection
        .execute(
            "UPDATE oa_artifact SET enrichment_status = ?2 WHERE artifact_id = ?1",
            params![artifact_id, next_status.as_str()],
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to update enrichment status for {artifact_id}: {source}"),
        })?;
    Ok(next_status)
}

pub(super) fn load_claimed_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClaimedJob> {
    let job_id: String = row.get(0)?;
    let required_capabilities_json: String = row.get(7)?;
    let required_capabilities = serde_json::from_str(&required_capabilities_json)
        .map_err(|err| rusqlite::Error::FromSqlConversionFailure(7, Type::Text, Box::new(err)))?;
    Ok(ClaimedJob {
        job_id,
        artifact_id: row.get(1)?,
        job_type: parse_job_type(row.get(2)?)?,
        enrichment_tier: parse_enrichment_tier(row.get(3)?)?,
        spawned_by_job_id: row.get(4)?,
        attempt_count: row.get(5)?,
        max_attempts: row.get(6)?,
        required_capabilities,
        payload_json: row.get(8)?,
    })
}

pub(super) fn claim_job_with_sql(
    tx: &rusqlite::Transaction<'_>,
    worker_id: &str,
    sql: &str,
    params: &[&dyn rusqlite::ToSql],
) -> StorageResult<Option<ClaimedJob>> {
    let job = tx
        .query_row(sql, params, load_claimed_job_from_row)
        .optional()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to claim job candidate: {source}"),
        })?;
    let Some(job) = job else {
        return Ok(None);
    };

    tx.execute(
        "UPDATE oa_enrichment_job
         SET job_status = 'running',
             claimed_by = ?2,
             claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
             attempt_count = attempt_count + 1
         WHERE job_id = ?1",
        params![job.job_id, worker_id],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!("failed to update claimed job {}: {source}", job.job_id),
    })?;
    recompute_artifact_enrichment_status_sqlite(tx, &job.artifact_id)?;

    Ok(Some(ClaimedJob {
        attempt_count: job.attempt_count + 1,
        ..job
    }))
}

impl EnrichmentJobLifecycleStore for SqliteEnrichmentJobStore {
    fn enqueue_jobs(&self, jobs: &[crate::storage::types::NewEnrichmentJob]) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        for job in jobs {
            tx.execute(
                "INSERT INTO oa_enrichment_job
                 (job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, job_status,
                  max_attempts, priority_no, required_capabilities, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    job.job_id,
                    job.artifact_id,
                    job.job_type.as_str(),
                    job.enrichment_tier.as_str(),
                    job.spawned_by_job_id,
                    job.job_status.as_str(),
                    job.max_attempts,
                    job.priority_no,
                    serde_json::to_string(&job.required_capabilities)
                        .expect("required capabilities serializable"),
                    job.payload_json
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
            recompute_artifact_enrichment_status_sqlite(&tx, &job.artifact_id)?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let claimed = claim_job_with_sql(
            &tx,
            worker_id,
            "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                    attempt_count, max_attempts, required_capabilities, payload_json
             FROM oa_enrichment_job
             WHERE job_status IN ('pending', 'retryable')
               AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
             ORDER BY priority_no ASC, available_at ASC, job_id ASC
             LIMIT 1",
            &[],
        )?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(claimed)
    }

    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let required_capabilities = serde_json::to_string(&template_job.required_capabilities)
            .expect("required capabilities serializable");
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let next = claim_job_with_sql(
                &tx,
                worker_id,
                "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                        attempt_count, max_attempts, required_capabilities, payload_json
                 FROM oa_enrichment_job
                 WHERE job_status IN ('pending', 'retryable')
                   AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                   AND job_type = ?1
                   AND enrichment_tier = ?2
                   AND required_capabilities = ?3
                 ORDER BY priority_no ASC, available_at ASC, job_id ASC
                 LIMIT 1",
                &[
                    &template_job.job_type.as_str(),
                    &template_job.enrichment_tier.as_str(),
                    &required_capabilities,
                ],
            )?;
            let Some(next) = next else {
                break;
            };
            claimed.push(next);
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(claimed)
    }

    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: JobType,
        enrichment_tier: Option<EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let next = if let Some(tier) = enrichment_tier {
                claim_job_with_sql(
                    &tx,
                    worker_id,
                    "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                            attempt_count, max_attempts, required_capabilities, payload_json
                     FROM oa_enrichment_job
                     WHERE job_status IN ('pending', 'retryable')
                       AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       AND job_type = ?1
                       AND enrichment_tier = ?2
                     ORDER BY priority_no ASC, available_at ASC, job_id ASC
                     LIMIT 1",
                    &[&job_type.as_str(), &tier.as_str()],
                )?
            } else {
                claim_job_with_sql(
                    &tx,
                    worker_id,
                    "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                            attempt_count, max_attempts, required_capabilities, payload_json
                     FROM oa_enrichment_job
                     WHERE job_status IN ('pending', 'retryable')
                       AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       AND job_type = ?1
                     ORDER BY priority_no ASC, available_at ASC, job_id ASC
                     LIMIT 1",
                    &[&job_type.as_str()],
                )?
            };
            let Some(next) = next else {
                break;
            };
            claimed.push(next);
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(claimed)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_id: String = tx
            .query_row(
                "SELECT artifact_id
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        tx.execute(
            "UPDATE oa_enrichment_job
             SET job_status = 'completed',
                 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                 claimed_by = NULL,
                 claimed_at = NULL
             WHERE job_id = ?1 AND claimed_by = ?2",
            params![job_id, worker_id],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_id: String = tx
            .query_row(
                "SELECT artifact_id
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        tx.execute(
            "UPDATE oa_enrichment_job
             SET job_status = 'failed',
                 last_error_message = ?3,
                 claimed_by = NULL,
                 claimed_at = NULL
             WHERE job_id = ?1 AND claimed_by = ?2",
            params![job_id, worker_id, error_message],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let row = tx
            .query_row(
                "SELECT artifact_id, attempt_count, max_attempts
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        let (artifact_id, attempt_count, max_attempts) = row;
        let outcome = if attempt_count >= max_attempts {
            tx.execute(
                "UPDATE oa_enrichment_job
                 SET job_status = 'failed',
                     last_error_message = ?3,
                     claimed_by = NULL,
                     claimed_at = NULL
                 WHERE job_id = ?1 AND claimed_by = ?2",
                params![job_id, worker_id, error_message],
            )
            .map_err(|source| db_err(&self.config, source))?;
            RetryOutcome::RetriesExhausted
        } else {
            tx.execute(
                "UPDATE oa_enrichment_job
                 SET job_status = 'retryable',
                     last_error_message = ?3,
                     available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?4),
                     claimed_by = NULL,
                     claimed_at = NULL
                 WHERE job_id = ?1 AND claimed_by = ?2",
                params![
                    job_id,
                    worker_id,
                    error_message,
                    format!("+{retry_after_seconds} seconds")
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
            RetryOutcome::Retried
        };
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(outcome)
    }

    fn reschedule_running_job(
        &self,
        worker_id: &str,
        job_id: &str,
        message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_id: String = tx
            .query_row(
                "SELECT artifact_id
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        tx.execute(
            "UPDATE oa_enrichment_job
             SET job_status = 'retryable',
                 last_error_message = ?3,
                 available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?4),
                 claimed_by = NULL,
                 claimed_at = NULL
             WHERE job_id = ?1 AND claimed_by = ?2",
            params![
                job_id,
                worker_id,
                message,
                format!("+{retry_after_seconds} seconds")
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn record_batch_submission(
        &self,
        batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "INSERT INTO oa_enrichment_batch
             (provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                batch.provider_batch_id,
                batch.provider_name,
                batch.stage_name,
                batch.phase_name,
                batch.owner_worker_id,
                batch.context_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        for (index, job) in jobs.iter().enumerate() {
            tx.execute(
                "INSERT INTO oa_enrichment_batch_job (provider_batch_id, job_id, job_order)
                 VALUES (?1, ?2, ?3)",
                params![batch.provider_batch_id, job.job_id, index as i64],
            )
            .map_err(|source| db_err(&self.config, source))?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn transition_batch_submission(
        &self,
        completed_provider_batch_id: &str,
        next_batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "UPDATE oa_enrichment_batch
             SET batch_status = 'completed',
                 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE provider_batch_id = ?1",
            [completed_provider_batch_id],
        )
        .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "INSERT INTO oa_enrichment_batch
             (provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                next_batch.provider_batch_id,
                next_batch.provider_name,
                next_batch.stage_name,
                next_batch.phase_name,
                next_batch.owner_worker_id,
                next_batch.context_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        for (index, job) in jobs.iter().enumerate() {
            tx.execute(
                "INSERT INTO oa_enrichment_batch_job (provider_batch_id, job_id, job_order)
                 VALUES (?1, ?2, ?3)",
                params![next_batch.provider_batch_id, job.job_id, index as i64],
            )
            .map_err(|source| db_err(&self.config, source))?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn complete_batch(&self, provider_batch_id: &str) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        connection
            .execute(
                "UPDATE oa_enrichment_batch
                 SET batch_status = 'completed',
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                 WHERE provider_batch_id = ?1",
                [provider_batch_id],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn fail_batch_record(&self, provider_batch_id: &str, error_message: &str) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        connection
            .execute(
                "UPDATE oa_enrichment_batch
                 SET batch_status = 'failed',
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                     last_error_message = ?2
                 WHERE provider_batch_id = ?1",
                params![provider_batch_id, error_message],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn load_running_batches(
        &self,
        stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        let connection = open_connection(&self.config)?;
        let mut batch_stmt = connection
            .prepare(
                "SELECT provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json
                 FROM oa_enrichment_batch
                 WHERE stage_name = ?1 AND batch_status = 'running'
                 ORDER BY submitted_at ASC, provider_batch_id ASC",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let batch_rows = batch_stmt
            .query_map([stage_name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })
            .map_err(|source| db_err(&self.config, source))?;

        let mut batches = Vec::new();
        for batch_row in batch_rows {
            let (
                provider_batch_id,
                provider_name,
                stage_name,
                phase_name,
                owner_worker_id,
                context_json,
            ) = batch_row.map_err(|source| db_err(&self.config, source))?;
            let mut job_stmt = connection
                .prepare(
                    "SELECT j.job_id, j.artifact_id, j.job_type, j.enrichment_tier, j.spawned_by_job_id,
                            j.attempt_count, j.max_attempts, j.required_capabilities, j.payload_json
                     FROM oa_enrichment_batch_job bj
                     JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                     WHERE bj.provider_batch_id = ?1
                     ORDER BY bj.job_order ASC",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let jobs = job_stmt
                .query_map([&provider_batch_id], load_claimed_job_from_row)
                .map_err(|source| db_err(&self.config, source))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| db_err(&self.config, source))?;
            batches.push(PersistedEnrichmentBatch {
                provider_batch_id,
                provider_name,
                stage_name,
                phase_name,
                owner_worker_id,
                context_json,
                jobs,
            });
        }
        Ok(batches)
    }

    fn reconcile_stale_running_batches(&self, stage_name: &str) -> StorageResult<usize> {
        let connection = open_connection(&self.config)?;
        let rows = connection
            .execute(
                "UPDATE oa_enrichment_batch
                 SET batch_status = 'failed',
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                     last_error_message = COALESCE(last_error_message, 'reconciled stale running batch')
                 WHERE stage_name = ?1
                   AND batch_status = 'running'
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_enrichment_batch_job bj
                        JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                        WHERE bj.provider_batch_id = oa_enrichment_batch.provider_batch_id
                          AND j.job_status = 'running'
                   )",
                [stage_name],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(rows)
    }

    fn reconcile_stale_running_jobs(&self, stage_name: &str) -> StorageResult<usize> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_ids = {
            let mut artifact_stmt = tx
                .prepare(
                    "SELECT DISTINCT j.artifact_id
                     FROM oa_enrichment_job j
                     JOIN oa_enrichment_batch_job bj ON bj.job_id = j.job_id
                     JOIN oa_enrichment_batch b ON b.provider_batch_id = bj.provider_batch_id
                     WHERE b.stage_name = ?1
                       AND b.batch_status <> 'running'
                       AND j.job_status = 'running'",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let artifact_ids = artifact_stmt
                .query_map([stage_name], |row| row.get::<_, String>(0))
                .map_err(|source| db_err(&self.config, source))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| db_err(&self.config, source))?;
            artifact_ids
        };
        let updated = tx
            .execute(
                "UPDATE oa_enrichment_job
                 SET job_status = 'retryable',
                     last_error_message = COALESCE(last_error_message, 'reconciled stale running job'),
                     available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                     claimed_by = NULL,
                     claimed_at = NULL
                 WHERE job_id IN (
                     SELECT j.job_id
                     FROM oa_enrichment_job j
                     JOIN oa_enrichment_batch_job bj ON bj.job_id = j.job_id
                     JOIN oa_enrichment_batch b ON b.provider_batch_id = bj.provider_batch_id
                     WHERE b.stage_name = ?1
                       AND b.batch_status <> 'running'
                       AND j.job_status = 'running'
                 )",
                [stage_name],
            )
            .map_err(|source| db_err(&self.config, source))?;
        for artifact_id in artifact_ids {
            recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(updated)
    }
}
