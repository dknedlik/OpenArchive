use crate::error::{StorageError, StorageResult};
use oracle::Connection;
use oracle::ErrorKind;

use crate::storage::types::{
    ClaimedJob, EnrichmentStatus, EnrichmentTier, JobStatus, JobType, NewEnrichmentJob,
    RetryOutcome,
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
        source: Box::new(source),
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
        .map_err(|source| StorageError::ClaimJob {
            source: Box::new(source),
        })?;

    let row = match rows.next() {
        Some(row_result) => row_result.map_err(|source| StorageError::ClaimJob {
            source: Box::new(source),
        })?,
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
        .map_err(|source| StorageError::ClaimJob {
            source: Box::new(source),
        })?;

    // Validate job_type BEFORE any update to avoid stranding invalid jobs in running state
    let job_type = JobType::parse(&job_type_str).ok_or_else(|| StorageError::InvalidJobType {
        job_id: job_id.clone(),
        job_type: job_type_str.clone(),
    })?;
    let enrichment_tier =
        EnrichmentTier::parse(&tier_str).ok_or_else(|| StorageError::InvalidEnrichmentTier {
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
        source: Box::new(source),
    })?;

    recompute_artifact_enrichment_status(conn, &artifact_id)?;
    conn.commit().map_err(|source| StorageError::Commit {
        operation: "claim enrichment job",
        source: Box::new(source),
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
            .map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?;

        let row = match rows.next() {
            Some(row_result) => row_result.map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?,
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
            .map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?;

        let job_type =
            JobType::parse(&job_type_str).ok_or_else(|| StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            })?;
        let enrichment_tier = EnrichmentTier::parse(&tier_str).ok_or_else(|| {
            StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            }
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
            source: Box::new(source),
        })?;

        recompute_artifact_enrichment_status(conn, &artifact_id)?;
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
        source: Box::new(source),
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
            .map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?
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
            .map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?
        };

        let row = match rows.next() {
            Some(row_result) => row_result.map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?,
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
            .map_err(|source| StorageError::ClaimJob {
                source: Box::new(source),
            })?;

        let job_type =
            JobType::parse(&job_type_str).ok_or_else(|| StorageError::InvalidJobType {
                job_id: job_id.clone(),
                job_type: job_type_str.clone(),
            })?;
        let enrichment_tier = EnrichmentTier::parse(&tier_str).ok_or_else(|| {
            StorageError::InvalidEnrichmentTier {
                job_id: job_id.clone(),
                value: tier_str.clone(),
            }
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
            source: Box::new(source),
        })?;

        recompute_artifact_enrichment_status(conn, &artifact_id)?;
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
        source: Box::new(source),
    })?;

    Ok(claimed)
}

/// Mark a running job as successfully completed.
pub fn complete_job(conn: &Connection, worker_id: &str, job_id: &str) -> StorageResult<()> {
    lock_owned_running_job(conn, worker_id, job_id)?;
    let artifact_id: String = conn
        .query_row_as::<(String,)>(
            "SELECT artifact_id FROM oa_enrichment_job WHERE job_id = :1",
            &[&job_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source: Box::new(source),
        })?
        .0;

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
        source: Box::new(source),
    })?;

    recompute_artifact_enrichment_status(conn, &artifact_id)?;
    conn.commit().map_err(|source| StorageError::Commit {
        operation: "complete enrichment job",
        source: Box::new(source),
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
    let artifact_id: String = conn
        .query_row_as::<(String,)>(
            "SELECT artifact_id FROM oa_enrichment_job WHERE job_id = :1",
            &[&job_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source: Box::new(source),
        })?
        .0;

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
        source: Box::new(source),
    })?;

    recompute_artifact_enrichment_status(conn, &artifact_id)?;
    conn.commit().map_err(|source| StorageError::Commit {
        operation: "fail enrichment job",
        source: Box::new(source),
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
            source: Box::new(source),
        })?;
    let artifact_id: String = conn
        .query_row_as::<(String,)>(
            "SELECT artifact_id FROM oa_enrichment_job WHERE job_id = :1",
            &[&job_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source: Box::new(source),
        })?
        .0;

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
            source: Box::new(source),
        })?;

        recompute_artifact_enrichment_status(conn, &artifact_id)?;
        conn.commit().map_err(|source| StorageError::Commit {
            operation: "fail exhausted enrichment job",
            source: Box::new(source),
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
        source: Box::new(source),
    })?;

    recompute_artifact_enrichment_status(conn, &artifact_id)?;
    conn.commit().map_err(|source| StorageError::Commit {
        operation: "mark enrichment job retryable",
        source: Box::new(source),
    })?;

    Ok(RetryOutcome::Retried)
}

pub fn reschedule_running_job(
    conn: &Connection,
    worker_id: &str,
    job_id: &str,
    message: &str,
    retry_after_seconds: i64,
) -> StorageResult<()> {
    lock_owned_running_job(conn, worker_id, job_id)?;
    let artifact_id: String = conn
        .query_row_as::<(String,)>(
            "SELECT artifact_id FROM oa_enrichment_job WHERE job_id = :1",
            &[&job_id],
        )
        .map_err(|source| StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source: Box::new(source),
        })?
        .0;

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
            &message,
            &retry_after_seconds,
            &job_id,
            &worker_id,
        ],
    )
    .map_err(|source| StorageError::UpdateJobStatus {
        job_id: job_id.to_string(),
        source: Box::new(source),
    })?;

    recompute_artifact_enrichment_status(conn, &artifact_id)?;
    conn.commit().map_err(|source| StorageError::Commit {
        operation: "reschedule enrichment job",
        source: Box::new(source),
    })?;
    Ok(())
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
                source: Box::new(source),
            })
        }
        Err(source) => Err(StorageError::UpdateJobStatus {
            job_id: job_id.to_string(),
            source: Box::new(source),
        }),
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ArtifactEnrichmentSnapshot {
    pending_jobs: i64,
    running_jobs: i64,
    retryable_jobs: i64,
    completed_jobs: i64,
    failed_jobs: i64,
    extraction_results: i64,
    reconciliation_decisions: i64,
    completed_derivation_runs: i64,
}

fn recompute_artifact_enrichment_status(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    let snapshot = load_artifact_enrichment_snapshot(conn, artifact_id)?;
    let next_status = derive_artifact_enrichment_status(snapshot);
    conn.execute(
        "UPDATE oa_artifact SET enrichment_status = :1 WHERE artifact_id = :2",
        &[&next_status.as_str(), &artifact_id],
    )
    .map_err(|source| StorageError::ListArtifacts {
        source: Box::new(source),
    })?;
    Ok(())
}

fn load_artifact_enrichment_snapshot(
    conn: &Connection,
    artifact_id: &str,
) -> StorageResult<ArtifactEnrichmentSnapshot> {
    let row = conn
        .query_row_as::<(i64, i64, i64, i64, i64, i64, i64, i64)>(
            "SELECT
                (SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1 AND job_status = 'pending'),
                (SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1 AND job_status = 'running'),
                (SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1 AND job_status = 'retryable'),
                (SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1 AND job_status = 'completed'),
                (SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1 AND job_status = 'failed'),
                (SELECT COUNT(*) FROM oa_artifact_extraction_result WHERE artifact_id = :1),
                (SELECT COUNT(*) FROM oa_reconciliation_decision WHERE artifact_id = :1),
                (SELECT COUNT(*) FROM oa_derivation_run WHERE artifact_id = :1 AND run_status = 'completed')
             FROM dual",
            &[&artifact_id],
        )
        .map_err(|source| StorageError::ListArtifacts { source: Box::new(source) })?;
    Ok(ArtifactEnrichmentSnapshot {
        pending_jobs: row.0,
        running_jobs: row.1,
        retryable_jobs: row.2,
        completed_jobs: row.3,
        failed_jobs: row.4,
        extraction_results: row.5,
        reconciliation_decisions: row.6,
        completed_derivation_runs: row.7,
    })
}

fn derive_artifact_enrichment_status(snapshot: ArtifactEnrichmentSnapshot) -> EnrichmentStatus {
    let has_running = snapshot.running_jobs > 0 || snapshot.retryable_jobs > 0;
    let has_pending = snapshot.pending_jobs > 0;
    let has_failed = snapshot.failed_jobs > 0;
    let has_completed_jobs = snapshot.completed_jobs > 0;
    let has_durable_outputs = snapshot.extraction_results > 0
        || snapshot.reconciliation_decisions > 0
        || snapshot.completed_derivation_runs > 0;
    let is_fully_derived = snapshot.completed_derivation_runs > 0;

    if is_fully_derived && !has_running && !has_pending && !has_failed {
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
