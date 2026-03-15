use crate::error::{StorageError, StorageResult};
use oracle::Connection;
use oracle::ErrorKind;

use crate::storage::types::{
    ClaimedJob, EnrichmentTier, JobStatus, JobType, NewEnrichmentJob, RetryOutcome,
};

// ---------------------------------------------------------------------------
// Insert (used during import write path)
// ---------------------------------------------------------------------------

pub fn insert_job(conn: &Connection, j: &NewEnrichmentJob) -> StorageResult<()> {
    let job_type = j.job_type.as_str();
    let enrichment_tier = j.enrichment_tier.as_str();
    let job_status = j.job_status.as_str();
    let required_capabilities = serde_json::to_string(&j.required_capabilities)
        .expect("required capabilities serializable");
    conn.execute(
        "INSERT INTO oa_enrichment_job \
         (job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, job_status, \
          max_attempts, priority_no, required_capabilities, payload_json) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)",
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
    .map_err(|source| StorageError::InsertJob {
        job_id: j.job_id.clone(),
        artifact_id: j.artifact_id.clone(),
        source,
    })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Worker lifecycle operations
// ---------------------------------------------------------------------------

/// Atomically claim the next eligible job.
///
/// Uses `FOR UPDATE SKIP LOCKED` so concurrent workers never block each
/// other. The claim SELECT and status UPDATE happen in the same transaction,
/// committed before return.
pub fn claim_next_job(conn: &Connection, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
    // Step 1: Find and lock the next eligible job.
    //
    // The ordering and SKIP LOCKED happen in the same cursor so concurrent
    // workers can move past already-locked rows instead of falsely reporting
    // an empty queue.
    let mut rows = conn
        .query(
            "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                    attempt_count, max_attempts, required_capabilities, payload_json \
             FROM oa_enrichment_job \
             WHERE job_status IN ('pending', 'retryable') \
               AND available_at <= SYSTIMESTAMP \
             ORDER BY priority_no ASC, available_at ASC \
             FOR UPDATE SKIP LOCKED",
            &[],
        )
        .map_err(|source| StorageError::ClaimJob { source })?;

    let row = match rows.next() {
        Some(row_result) => row_result.map_err(|source| StorageError::ClaimJob { source })?,
        None => return Ok(None),
    };
    let (
        job_id,
        artifact_id,
        job_type_str,
        tier_str,
        spawned_by_job_id,
        attempt_count,
        max_attempts,
        required_capabilities_json,
        payload_json,
    ) = row
        .get_as::<(
            String,
            String,
            String,
            String,
            Option<String>,
            i32,
            i32,
            String,
            String,
        )>()
        .map_err(|source| StorageError::ClaimJob { source })?;

    // Validate job_type BEFORE any update to avoid stranding invalid jobs in running state
    let job_type =
        JobType::from_str(&job_type_str).ok_or_else(|| StorageError::InvalidJobType {
            job_id: job_id.clone(),
            job_type: job_type_str.clone(),
        })?;
    let enrichment_tier =
        EnrichmentTier::from_str(&tier_str).ok_or_else(|| StorageError::InvalidEnrichmentTier {
            job_id: job_id.clone(),
            value: tier_str.clone(),
        })?;
    let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
        .map_err(|err| StorageError::InvalidJobCapabilities {
            job_id: job_id.clone(),
            detail: err.to_string(),
        })?;

    // Step 2: Update the job to running, increment attempt count, set claim fields.
    let running = JobStatus::Running.as_str();
    conn.execute(
        "UPDATE oa_enrichment_job \
         SET job_status = :1, \
             claimed_by = :2, \
             claimed_at = SYSTIMESTAMP, \
             attempt_count = attempt_count + 1 \
         WHERE job_id = :3",
        &[&running, &worker_id, &job_id],
    )
    .map_err(|source| StorageError::UpdateJobStatus {
        job_id: job_id.clone(),
        source,
    })?;

    conn.commit().map_err(|source| StorageError::Commit {
        operation: "claim enrichment job",
        source,
    })?;

    Ok(Some(ClaimedJob {
        job_id,
        artifact_id,
        job_type,
        enrichment_tier,
        spawned_by_job_id,
        // attempt_count reflects the NEW value after increment
        attempt_count: attempt_count + 1,
        max_attempts,
        required_capabilities,
        payload_json,
    }))
}

pub fn claim_matching_jobs(
    conn: &Connection,
    worker_id: &str,
    template_job: &ClaimedJob,
    limit: usize,
) -> StorageResult<Vec<ClaimedJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let required_capabilities = serde_json::to_string(&template_job.required_capabilities)
        .expect("required capabilities serializable");
    let mut claimed = Vec::new();

    for _ in 0..limit {
        let mut rows = conn
            .query(
                "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                        attempt_count, max_attempts, required_capabilities, payload_json \
                 FROM oa_enrichment_job \
                 WHERE job_status IN ('pending', 'retryable') \
                   AND available_at <= SYSTIMESTAMP \
                   AND job_type = :1 \
                   AND enrichment_tier = :2 \
                   AND required_capabilities = :3 \
                 ORDER BY priority_no ASC, available_at ASC \
                 FOR UPDATE SKIP LOCKED",
                &[
                    &template_job.job_type.as_str(),
                    &template_job.enrichment_tier.as_str(),
                    &required_capabilities,
                ],
            )
            .map_err(|source| StorageError::ClaimJob { source })?;

        let row = match rows.next() {
            Some(row_result) => row_result.map_err(|source| StorageError::ClaimJob { source })?,
            None => break,
        };
        let (
            job_id,
            artifact_id,
            job_type_str,
            tier_str,
            spawned_by_job_id,
            attempt_count,
            max_attempts,
            required_capabilities_json,
            payload_json,
        ) = row
            .get_as::<(
                String,
                String,
                String,
                String,
                Option<String>,
                i32,
                i32,
                String,
                String,
            )>()
            .map_err(|source| StorageError::ClaimJob { source })?;

        let job_type =
            JobType::from_str(&job_type_str).ok_or_else(|| StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            })?;
        let enrichment_tier =
            EnrichmentTier::from_str(&tier_str).ok_or_else(|| StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            })?;
        let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
            .map_err(|err| StorageError::InvalidJobCapabilities {
                job_id: job_id.clone(),
                detail: err.to_string(),
            })?;

        conn.execute(
            "UPDATE oa_enrichment_job \
             SET job_status = :1, \
                 claimed_by = :2, \
                 claimed_at = SYSTIMESTAMP, \
                 attempt_count = attempt_count + 1 \
             WHERE job_id = :3",
            &[&JobStatus::Running.as_str(), &worker_id, &job_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.clone(),
            source,
        })?;

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

    conn.commit().map_err(|source| StorageError::Commit {
        operation: "claim matching enrichment jobs",
        source,
    })?;

    Ok(claimed)
}

/// Claim up to `limit` eligible jobs of a specific type and optional tier.
pub fn claim_jobs_by_type(
    conn: &Connection,
    worker_id: &str,
    job_type: JobType,
    enrichment_tier: Option<EnrichmentTier>,
    limit: usize,
) -> StorageResult<Vec<ClaimedJob>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut claimed = Vec::new();

    for _ in 0..limit {
        let mut rows = if let Some(tier) = enrichment_tier {
            conn.query(
                "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                        attempt_count, max_attempts, required_capabilities, payload_json \
                 FROM oa_enrichment_job \
                 WHERE job_status IN ('pending', 'retryable') \
                   AND available_at <= SYSTIMESTAMP \
                   AND job_type = :1 \
                   AND enrichment_tier = :2 \
                 ORDER BY priority_no ASC, available_at ASC \
                 FOR UPDATE SKIP LOCKED",
                &[&job_type.as_str(), &tier.as_str()],
            )
            .map_err(|source| StorageError::ClaimJob { source })?
        } else {
            conn.query(
                "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, \
                        attempt_count, max_attempts, required_capabilities, payload_json \
                 FROM oa_enrichment_job \
                 WHERE job_status IN ('pending', 'retryable') \
                   AND available_at <= SYSTIMESTAMP \
                   AND job_type = :1 \
                 ORDER BY priority_no ASC, available_at ASC \
                 FOR UPDATE SKIP LOCKED",
                &[&job_type.as_str()],
            )
            .map_err(|source| StorageError::ClaimJob { source })?
        };

        let row = match rows.next() {
            Some(row_result) => row_result.map_err(|source| StorageError::ClaimJob { source })?,
            None => break,
        };
        let (
            job_id,
            artifact_id,
            job_type_str,
            tier_str,
            spawned_by_job_id,
            attempt_count,
            max_attempts,
            required_capabilities_json,
            payload_json,
        ) = row
            .get_as::<(
                String,
                String,
                String,
                String,
                Option<String>,
                i32,
                i32,
                String,
                String,
            )>()
            .map_err(|source| StorageError::ClaimJob { source })?;

        let job_type =
            JobType::from_str(&job_type_str).ok_or_else(|| StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            })?;
        let enrichment_tier =
            EnrichmentTier::from_str(&tier_str).ok_or_else(|| StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            })?;
        let required_capabilities: Vec<String> = serde_json::from_str(&required_capabilities_json)
            .map_err(|err| StorageError::InvalidJobCapabilities {
                job_id: job_id.clone(),
                detail: err.to_string(),
            })?;

        conn.execute(
            "UPDATE oa_enrichment_job \
             SET job_status = :1, \
                 claimed_by = :2, \
                 claimed_at = SYSTIMESTAMP, \
                 attempt_count = attempt_count + 1 \
             WHERE job_id = :3",
            &[&JobStatus::Running.as_str(), &worker_id, &job_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.clone(),
            source,
        })?;

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

    conn.commit().map_err(|source| StorageError::Commit {
        operation: "claim enrichment jobs by type",
        source,
    })?;

    Ok(claimed)
}

/// Mark a running job as successfully completed.
pub fn complete_job(conn: &Connection, worker_id: &str, job_id: &str) -> StorageResult<()> {
    lock_owned_running_job(conn, worker_id, job_id)?;

    let completed = JobStatus::Completed.as_str();
    conn.execute(
        "UPDATE oa_enrichment_job \
         SET job_status = :1, \
             completed_at = SYSTIMESTAMP, \
             claimed_by = NULL, \
             claimed_at = NULL \
         WHERE job_id = :2 AND job_status = 'running' AND claimed_by = :3",
        &[&completed, &job_id, &worker_id],
    )
    .map_err(|source| StorageError::UpdateJobStatus {
        job_id: job_id.to_string(),
        source,
    })?;

    conn.commit().map_err(|source| StorageError::Commit {
        operation: "complete enrichment job",
        source,
    })?;

    Ok(())
}

/// Mark a running job as terminally failed.
pub fn fail_job(
    conn: &Connection,
    worker_id: &str,
    job_id: &str,
    error_message: &str,
) -> StorageResult<()> {
    lock_owned_running_job(conn, worker_id, job_id)?;

    let failed = JobStatus::Failed.as_str();
    conn.execute(
        "UPDATE oa_enrichment_job \
         SET job_status = :1, \
             last_error_message = :2, \
             claimed_by = NULL, \
             claimed_at = NULL \
         WHERE job_id = :3 AND job_status = 'running' AND claimed_by = :4",
        &[&failed, &error_message, &job_id, &worker_id],
    )
    .map_err(|source| StorageError::UpdateJobStatus {
        job_id: job_id.to_string(),
        source,
    })?;

    conn.commit().map_err(|source| StorageError::Commit {
        operation: "fail enrichment job",
        source,
    })?;

    Ok(())
}

/// Attempt to mark a running job as retryable. If attempt_count >= max_attempts,
/// terminally fail it instead.
pub fn mark_job_retryable(
    conn: &Connection,
    worker_id: &str,
    job_id: &str,
    error_message: &str,
    retry_after_seconds: i64,
) -> StorageResult<RetryOutcome> {
    lock_owned_running_job(conn, worker_id, job_id)?;

    // Read current counts to decide retry vs terminal.
    let (attempt_count, max_attempts): (i32, i32) = conn
        .query_row_as::<(i32, i32)>(
            "SELECT attempt_count, max_attempts FROM oa_enrichment_job \
             WHERE job_id = :1 AND job_status = 'running' AND claimed_by = :2 \
             FOR UPDATE",
            &[&job_id, &worker_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source,
        })?;

    if attempt_count >= max_attempts {
        // Exhausted: terminal failure.
        let failed = JobStatus::Failed.as_str();
        conn.execute(
            "UPDATE oa_enrichment_job \
             SET job_status = :1, \
                 last_error_message = :2, \
                 claimed_by = NULL, \
                 claimed_at = NULL \
             WHERE job_id = :3 AND claimed_by = :4",
            &[&failed, &error_message, &job_id, &worker_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source,
        })?;

        conn.commit().map_err(|source| StorageError::Commit {
            operation: "fail exhausted enrichment job",
            source,
        })?;

        return Ok(RetryOutcome::RetriesExhausted);
    }

    // Retryable: schedule for re-claim after the delay.
    let retryable = JobStatus::Retryable.as_str();
    conn.execute(
        "UPDATE oa_enrichment_job \
         SET job_status = :1, \
             last_error_message = :2, \
             available_at = SYSTIMESTAMP + NUMTODSINTERVAL(:3, 'SECOND'), \
             claimed_by = NULL, \
             claimed_at = NULL \
         WHERE job_id = :4 AND claimed_by = :5",
        &[
            &retryable,
            &error_message,
            &retry_after_seconds,
            &job_id,
            &worker_id,
        ],
    )
    .map_err(|source| StorageError::UpdateJobStatus {
        job_id: job_id.to_string(),
        source,
    })?;

    conn.commit().map_err(|source| StorageError::Commit {
        operation: "mark enrichment job retryable",
        source,
    })?;

    Ok(RetryOutcome::Retried)
}

fn lock_owned_running_job(conn: &Connection, worker_id: &str, job_id: &str) -> StorageResult<()> {
    match conn.query_row_as::<(String,)>(
        "SELECT job_id FROM oa_enrichment_job \
         WHERE job_id = :1 AND job_status = 'running' AND claimed_by = :2 \
         FOR UPDATE",
        &[&job_id, &worker_id],
    ) {
        Ok(_) => Ok(()),
        Err(source) if source.kind() == ErrorKind::NoDataFound => {
            Err(StorageError::UpdateJobStatus {
                job_id: job_id.to_string(),
                source,
            })
        }
        Err(source) => Err(StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source,
        }),
    }
}
