use crate::error::StorageResult;
use crate::storage::types::{
    ClaimedJob, EnrichmentTier, JobStatus, JobType, NewEnrichmentBatch, NewEnrichmentJob,
    PersistedEnrichmentBatch, RetryOutcome,
};

pub fn insert_job(client: &mut postgres::Client, j: &NewEnrichmentJob) -> StorageResult<()> {
    let job_type = j.job_type.as_str();
    let enrichment_tier = j.enrichment_tier.as_str();
    let job_status = j.job_status.as_str();
    let required_capabilities = serde_json::to_string(&j.required_capabilities)
        .expect("required capabilities serializable");
    client
        .execute(
            "INSERT INTO oa_enrichment_job \
             (job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, job_status, \
              max_attempts, priority_no, required_capabilities, payload_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text::jsonb, $10::text::jsonb)",
            &[
                &j.job_id,
                &j.artifact_id,
                &job_type,
                &enrichment_tier,
                &j.spawned_by_job_id,
                &job_status,
                &j.max_attempts,
                &j.priority_no,
                &required_capabilities,
                &j.payload_json,
            ],
        )
        .map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn claim_next_job(
    client: &mut postgres::Client,
    worker_id: &str,
) -> StorageResult<Option<ClaimedJob>> {
    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;

    let row = client
        .query_opt(
            "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                    attempt_count, max_attempts, required_capabilities::text, payload_json::text \
             FROM oa_enrichment_job \
             WHERE job_status IN ('pending', 'retryable') \
               AND available_at <= NOW() \
             ORDER BY priority_no ASC, available_at ASC \
             FOR UPDATE SKIP LOCKED \
             LIMIT 1",
            &[],
        )
        .map_err(map_pg_storage_err)?;

    let Some(row) = row else {
        client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
        return Ok(None);
    };

    let job_id: String = row.get(0);
    let artifact_id: String = row.get(1);
    let job_type_str: String = row.get(2);
    let tier_str: String = row.get(3);
    let spawned_by_job_id: Option<String> = row.get(4);
    let attempt_count: i32 = row.get(5);
    let max_attempts: i32 = row.get(6);
    let required_capabilities_json: String = row.get(7);
    let payload_json: String = row.get(8);

    let job_type = JobType::from_str(&job_type_str).ok_or_else(|| {
        crate::error::StorageError::InvalidJobType {
            job_id: job_id.clone(),
            job_type: job_type_str.clone(),
        }
    })?;
    let enrichment_tier = EnrichmentTier::from_str(&tier_str).ok_or_else(|| {
        crate::error::StorageError::InvalidEnrichmentTier {
            job_id: job_id.clone(),
            value: tier_str.clone(),
        }
    })?;
    let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
        .map_err(|err| crate::error::StorageError::InvalidJobCapabilities {
            job_id: job_id.clone(),
            detail: err.to_string(),
        })?;

    let running = JobStatus::Running.as_str();
    client
        .execute(
            "UPDATE oa_enrichment_job \
             SET job_status = $1, \
                 claimed_by = $2, \
                 claimed_at = NOW(), \
                 attempt_count = attempt_count + 1 \
             WHERE job_id = $3",
            &[&running, &worker_id, &job_id],
        )
        .map_err(map_pg_storage_err)?;

    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;

    Ok(Some(ClaimedJob {
        job_id,
        artifact_id,
        job_type,
        enrichment_tier,
        spawned_by_job_id,
        attempt_count: attempt_count + 1,
        max_attempts,
        required_capabilities,
        payload_json,
    }))
}

pub fn claim_matching_jobs(
    client: &mut postgres::Client,
    worker_id: &str,
    template_job: &ClaimedJob,
    limit: usize,
) -> StorageResult<Vec<ClaimedJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let required_capabilities = serde_json::to_string(&template_job.required_capabilities)
        .expect("required capabilities serializable");
    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;
    let mut claimed = Vec::new();

    for _ in 0..limit {
        let row = client
            .query_opt(
                "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                        attempt_count, max_attempts, required_capabilities::text, payload_json::text \
                 FROM oa_enrichment_job \
                 WHERE job_status IN ('pending', 'retryable') \
                   AND available_at <= NOW() \
                   AND job_type = $1 \
                   AND enrichment_tier = $2 \
                   AND required_capabilities::text = $3 \
                 ORDER BY priority_no ASC, available_at ASC \
                 FOR UPDATE SKIP LOCKED \
                 LIMIT 1",
                &[
                    &template_job.job_type.as_str(),
                    &template_job.enrichment_tier.as_str(),
                    &required_capabilities,
                ],
            )
            .map_err(map_pg_storage_err)?;

        let Some(row) = row else {
            break;
        };

        let job_id: String = row.get(0);
        let artifact_id: String = row.get(1);
        let job_type_str: String = row.get(2);
        let tier_str: String = row.get(3);
        let spawned_by_job_id: Option<String> = row.get(4);
        let attempt_count: i32 = row.get(5);
        let max_attempts: i32 = row.get(6);
        let required_capabilities_json: String = row.get(7);
        let payload_json: String = row.get(8);

        let job_type = JobType::from_str(&job_type_str).ok_or_else(|| {
            crate::error::StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            }
        })?;
        let enrichment_tier = EnrichmentTier::from_str(&tier_str).ok_or_else(|| {
            crate::error::StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            }
        })?;
        let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
            .map_err(|err| crate::error::StorageError::InvalidJobCapabilities {
                job_id: job_id.clone(),
                detail: err.to_string(),
            })?;

        client
            .execute(
                "UPDATE oa_enrichment_job \
                 SET job_status = $1, \
                     claimed_by = $2, \
                     claimed_at = NOW(), \
                     attempt_count = attempt_count + 1 \
                 WHERE job_id = $3",
                &[&JobStatus::Running.as_str(), &worker_id, &job_id],
            )
            .map_err(map_pg_storage_err)?;

        claimed.push(ClaimedJob {
            job_id,
            artifact_id,
            job_type,
            enrichment_tier,
            spawned_by_job_id,
            attempt_count: attempt_count + 1,
            max_attempts,
            required_capabilities,
            payload_json,
        });
    }

    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
    Ok(claimed)
}

pub fn claim_jobs_by_type(
    client: &mut postgres::Client,
    worker_id: &str,
    job_type: JobType,
    enrichment_tier: Option<EnrichmentTier>,
    limit: usize,
) -> StorageResult<Vec<ClaimedJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;
    let mut claimed = Vec::new();

    for _ in 0..limit {
        let row = if let Some(tier) = enrichment_tier {
            client
                .query_opt(
                    "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                            attempt_count, max_attempts, required_capabilities::text, payload_json::text \
                     FROM oa_enrichment_job \
                     WHERE job_status IN ('pending', 'retryable') \
                       AND available_at <= NOW() \
                       AND job_type = $1 \
                       AND enrichment_tier = $2 \
                     ORDER BY priority_no ASC, available_at ASC \
                     FOR UPDATE SKIP LOCKED \
                     LIMIT 1",
                    &[&job_type.as_str(), &tier.as_str()],
                )
                .map_err(map_pg_storage_err)?
        } else {
            client
                .query_opt(
                    "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                            attempt_count, max_attempts, required_capabilities::text, payload_json::text \
                     FROM oa_enrichment_job \
                     WHERE job_status IN ('pending', 'retryable') \
                       AND available_at <= NOW() \
                       AND job_type = $1 \
                     ORDER BY priority_no ASC, available_at ASC \
                     FOR UPDATE SKIP LOCKED \
                     LIMIT 1",
                    &[&job_type.as_str()],
                )
                .map_err(map_pg_storage_err)?
        };

        let Some(row) = row else {
            break;
        };

        let job_id: String = row.get(0);
        let artifact_id: String = row.get(1);
        let job_type_str: String = row.get(2);
        let tier_str: String = row.get(3);
        let spawned_by_job_id: Option<String> = row.get(4);
        let attempt_count: i32 = row.get(5);
        let max_attempts: i32 = row.get(6);
        let required_capabilities_json: String = row.get(7);
        let payload_json: String = row.get(8);

        let job_type = JobType::from_str(&job_type_str).ok_or_else(|| {
            crate::error::StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            }
        })?;
        let enrichment_tier = EnrichmentTier::from_str(&tier_str).ok_or_else(|| {
            crate::error::StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            }
        })?;
        let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
            .map_err(|err| crate::error::StorageError::InvalidJobCapabilities {
                job_id: job_id.clone(),
                detail: err.to_string(),
            })?;

        client
            .execute(
                "UPDATE oa_enrichment_job \
                 SET job_status = $1, \
                     claimed_by = $2, \
                     claimed_at = NOW(), \
                     attempt_count = attempt_count + 1 \
                 WHERE job_id = $3",
                &[&JobStatus::Running.as_str(), &worker_id, &job_id],
            )
            .map_err(map_pg_storage_err)?;

        claimed.push(ClaimedJob {
            job_id,
            artifact_id,
            job_type,
            enrichment_tier,
            spawned_by_job_id,
            attempt_count: attempt_count + 1,
            max_attempts,
            required_capabilities,
            payload_json,
        });
    }

    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
    Ok(claimed)
}

pub fn complete_job(
    client: &mut postgres::Client,
    worker_id: &str,
    job_id: &str,
) -> StorageResult<()> {
    update_terminal_job(
        client,
        worker_id,
        job_id,
        JobStatus::Completed.as_str(),
        None,
        "complete enrichment job",
    )
}

pub fn fail_job(
    client: &mut postgres::Client,
    worker_id: &str,
    job_id: &str,
    error_message: &str,
) -> StorageResult<()> {
    update_terminal_job(
        client,
        worker_id,
        job_id,
        JobStatus::Failed.as_str(),
        Some(error_message),
        "fail enrichment job",
    )
}

pub fn mark_job_retryable(
    client: &mut postgres::Client,
    worker_id: &str,
    job_id: &str,
    error_message: &str,
    retry_after_seconds: i64,
) -> StorageResult<RetryOutcome> {
    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;

    let row = client
        .query_opt(
            "SELECT attempt_count, max_attempts \
             FROM oa_enrichment_job \
             WHERE job_id = $1 AND job_status = 'running' AND claimed_by = $2 \
             FOR UPDATE",
            &[&job_id, &worker_id],
        )
        .map_err(map_pg_storage_err)?;
    let Some(row) = row else {
        client
            .batch_execute("ROLLBACK")
            .map_err(map_pg_storage_err)?;
        return Err(crate::error::StorageError::JobNotClaimed {
            job_id: job_id.to_string(),
            worker_id: worker_id.to_string(),
        });
    };

    let attempt_count: i32 = row.get(0);
    let max_attempts: i32 = row.get(1);

    if attempt_count >= max_attempts {
        client
            .execute(
                "UPDATE oa_enrichment_job \
                 SET job_status = $1, \
                     last_error_message = $2, \
                     claimed_by = NULL, \
                     claimed_at = NULL \
                 WHERE job_id = $3 AND claimed_by = $4",
                &[
                    &JobStatus::Failed.as_str(),
                    &error_message,
                    &job_id,
                    &worker_id,
                ],
            )
            .map_err(map_pg_storage_err)?;

        client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
        return Ok(RetryOutcome::RetriesExhausted);
    }

    client
        .execute(
            "UPDATE oa_enrichment_job \
             SET job_status = $1, \
                 last_error_message = $2, \
                 available_at = NOW() + ($3 || ' seconds')::interval, \
                 claimed_by = NULL, \
                 claimed_at = NULL \
             WHERE job_id = $4 AND claimed_by = $5",
            &[
                &JobStatus::Retryable.as_str(),
                &error_message,
                &retry_after_seconds.to_string(),
                &job_id,
                &worker_id,
            ],
        )
        .map_err(map_pg_storage_err)?;

    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
    Ok(RetryOutcome::Retried)
}

pub fn record_batch_submission(
    client: &mut postgres::Client,
    batch: &NewEnrichmentBatch,
    jobs: &[ClaimedJob],
) -> StorageResult<()> {
    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;
    insert_batch_row(client, batch)?;
    insert_batch_jobs(client, &batch.provider_batch_id, jobs)?;
    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn transition_batch_submission(
    client: &mut postgres::Client,
    completed_provider_batch_id: &str,
    next_batch: &NewEnrichmentBatch,
    jobs: &[ClaimedJob],
) -> StorageResult<()> {
    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;
    client
        .execute(
            "UPDATE oa_enrichment_batch
             SET batch_status = 'completed',
                 completed_at = NOW()
             WHERE provider_batch_id = $1 AND batch_status = 'running'",
            &[&completed_provider_batch_id],
        )
        .map_err(map_pg_storage_err)?;
    insert_batch_row(client, next_batch)?;
    insert_batch_jobs(client, &next_batch.provider_batch_id, jobs)?;
    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn complete_batch(client: &mut postgres::Client, provider_batch_id: &str) -> StorageResult<()> {
    client
        .execute(
            "UPDATE oa_enrichment_batch
             SET batch_status = 'completed',
                 completed_at = NOW()
             WHERE provider_batch_id = $1 AND batch_status = 'running'",
            &[&provider_batch_id],
        )
        .map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn fail_batch_record(
    client: &mut postgres::Client,
    provider_batch_id: &str,
    error_message: &str,
) -> StorageResult<()> {
    client
        .execute(
            "UPDATE oa_enrichment_batch
             SET batch_status = 'failed',
                 completed_at = NOW(),
                 last_error_message = $2
             WHERE provider_batch_id = $1 AND batch_status = 'running'",
            &[&provider_batch_id, &error_message],
        )
        .map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn load_running_batches(
    client: &mut postgres::Client,
    stage_name: &str,
) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
    let rows = client
        .query(
            "SELECT b.provider_batch_id,
                    b.provider_name,
                    b.stage_name,
                    b.phase_name,
                    b.owner_worker_id,
                    b.context_json::text,
                    j.job_id,
                    j.artifact_id,
                    j.job_type,
                    j.enrichment_tier,
                    j.spawned_by_job_id,
                    j.attempt_count,
                    j.max_attempts,
                    j.required_capabilities::text,
                    j.payload_json::text
             FROM oa_enrichment_batch b
             JOIN oa_enrichment_batch_job bj
               ON bj.provider_batch_id = b.provider_batch_id
             JOIN oa_enrichment_job j
               ON j.job_id = bj.job_id
             WHERE b.stage_name = $1
               AND b.batch_status = 'running'
               AND j.job_status = 'running'
             ORDER BY b.submitted_at ASC, b.provider_batch_id ASC, bj.job_order ASC",
            &[&stage_name],
        )
        .map_err(map_pg_storage_err)?;

    let mut batches: Vec<PersistedEnrichmentBatch> = Vec::new();
    let mut current_batch_id: Option<String> = None;

    for row in rows {
        let provider_batch_id: String = row.get(0);
        let provider_name: String = row.get(1);
        let row_stage_name: String = row.get(2);
        let phase_name: String = row.get(3);
        let owner_worker_id: String = row.get(4);
        let context_json: Option<String> = row.get(5);
        let job_id: String = row.get(6);
        let artifact_id: String = row.get(7);
        let job_type_str: String = row.get(8);
        let tier_str: String = row.get(9);
        let spawned_by_job_id: Option<String> = row.get(10);
        let attempt_count: i32 = row.get(11);
        let max_attempts: i32 = row.get(12);
        let required_capabilities_json: String = row.get(13);
        let payload_json: String = row.get(14);

        let job_type = JobType::from_str(&job_type_str).ok_or_else(|| {
            crate::error::StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            }
        })?;
        let enrichment_tier = EnrichmentTier::from_str(&tier_str).ok_or_else(|| {
            crate::error::StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            }
        })?;
        let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
            .map_err(|err| crate::error::StorageError::InvalidJobCapabilities {
                job_id: job_id.clone(),
                detail: err.to_string(),
            })?;

        if current_batch_id.as_deref() != Some(provider_batch_id.as_str()) {
            current_batch_id = Some(provider_batch_id.clone());
            batches.push(PersistedEnrichmentBatch {
                provider_batch_id: provider_batch_id.clone(),
                provider_name,
                stage_name: row_stage_name,
                phase_name,
                owner_worker_id,
                context_json,
                jobs: Vec::new(),
            });
        }

        batches
            .last_mut()
            .expect("batch record initialized before push")
            .jobs
            .push(ClaimedJob {
                job_id,
                artifact_id,
                job_type,
                enrichment_tier,
                spawned_by_job_id,
                attempt_count,
                max_attempts,
                required_capabilities,
                payload_json,
            });
    }

    Ok(batches)
}

pub fn reconcile_stale_running_batches(
    client: &mut postgres::Client,
    stage_name: &str,
) -> StorageResult<usize> {
    client
        .execute(
            "UPDATE oa_enrichment_batch b
             SET batch_status = CASE
                     WHEN EXISTS (
                         SELECT 1
                           FROM oa_enrichment_batch_job bj
                           JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                          WHERE bj.provider_batch_id = b.provider_batch_id
                            AND j.job_status IN ('failed', 'retryable')
                     ) THEN 'failed'
                     ELSE 'completed'
                 END,
                 completed_at = NOW(),
                 last_error_message = CASE
                     WHEN EXISTS (
                         SELECT 1
                           FROM oa_enrichment_batch_job bj
                           JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                          WHERE bj.provider_batch_id = b.provider_batch_id
                            AND j.job_status IN ('failed', 'retryable')
                     ) THEN COALESCE((
                         SELECT string_agg(
                                    left(coalesce(j.last_error_message, j.job_status), 200),
                                    ' | '
                                )
                           FROM oa_enrichment_batch_job bj
                           JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                          WHERE bj.provider_batch_id = b.provider_batch_id
                            AND j.job_status IN ('failed', 'retryable')
                     ), 'linked jobs reached terminal state')
                     ELSE last_error_message
                 END
             WHERE b.stage_name = $1
               AND b.batch_status = 'running'
               AND NOT EXISTS (
                   SELECT 1
                     FROM oa_enrichment_batch_job bj
                     JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                    WHERE bj.provider_batch_id = b.provider_batch_id
                      AND j.job_status = 'running'
               )",
            &[&stage_name],
        )
        .map(|rows| rows as usize)
        .map_err(map_pg_storage_err)
}

fn insert_batch_row(
    client: &mut postgres::Client,
    batch: &NewEnrichmentBatch,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_enrichment_batch
             (provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json)
             VALUES ($1, $2, $3, $4, $5, $6::text::jsonb)",
            &[
                &batch.provider_batch_id,
                &batch.provider_name,
                &batch.stage_name,
                &batch.phase_name,
                &batch.owner_worker_id,
                &batch.context_json,
            ],
        )
        .map_err(map_pg_storage_err)?;
    Ok(())
}

fn insert_batch_jobs(
    client: &mut postgres::Client,
    provider_batch_id: &str,
    jobs: &[ClaimedJob],
) -> StorageResult<()> {
    for (job_order, job) in jobs.iter().enumerate() {
        client
            .execute(
                "INSERT INTO oa_enrichment_batch_job
                 (provider_batch_id, job_id, job_order)
                 VALUES ($1, $2, $3)",
                &[&provider_batch_id, &job.job_id, &(job_order as i32)],
            )
            .map_err(map_pg_storage_err)?;
    }
    Ok(())
}

fn update_terminal_job(
    client: &mut postgres::Client,
    worker_id: &str,
    job_id: &str,
    status: &str,
    error_message: Option<&str>,
    _operation: &'static str,
) -> StorageResult<()> {
    client.batch_execute("BEGIN").map_err(map_pg_storage_err)?;
    let rows_updated = client
        .execute(
            "UPDATE oa_enrichment_job \
             SET job_status = $1, \
                 completed_at = CASE WHEN $1 = 'completed' THEN NOW() ELSE completed_at END, \
                 last_error_message = COALESCE($2, last_error_message), \
                 claimed_by = NULL, \
                 claimed_at = NULL \
             WHERE job_id = $3 AND job_status = 'running' AND claimed_by = $4",
            &[&status, &error_message, &job_id, &worker_id],
        )
        .map_err(map_pg_storage_err)?;
    if rows_updated == 0 {
        client
            .batch_execute("ROLLBACK")
            .map_err(map_pg_storage_err)?;
        return Err(crate::error::StorageError::JobNotClaimed {
            job_id: job_id.to_string(),
            worker_id: worker_id.to_string(),
        });
    }
    client.batch_execute("COMMIT").map_err(map_pg_storage_err)?;
    Ok(())
}

fn map_pg_storage_err(source: postgres::Error) -> crate::error::StorageError {
    crate::error::StorageError::Db(map_postgres_error(source))
}

fn map_postgres_error(source: postgres::Error) -> crate::error::DbError {
    crate::error::DbError::ConnectPostgres {
        connection_string: "postgres".to_string(),
        source,
    }
}
