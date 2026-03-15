use crate::error::StorageResult;
use crate::storage::types::{
    ClaimedJob, EnrichmentTier, JobStatus, JobType, NewEnrichmentJob, RetryOutcome,
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
