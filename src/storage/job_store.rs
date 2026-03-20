use crate::error::StorageResult;

use crate::storage::types::{
    ClaimedJob, EnrichmentTier, JobType, NewEnrichmentBatch, NewEnrichmentJob,
    PersistedEnrichmentBatch, RetryOutcome,
};
use crate::storage::StorageTx;

/// Stores asynchronous enrichment jobs (insert-time, used during import).
pub trait EnrichmentJobStore {
    type Tx: StorageTx;

    fn insert_job(&self, tx: &mut Self::Tx, job: &NewEnrichmentJob) -> StorageResult<()>;
}

/// Worker-facing enrichment job lifecycle operations.
///
/// Each method is self-committing: it acquires a connection, performs the
/// operation, and commits before returning. This keeps the DB lock window
/// narrow and avoids holding transactions open during job execution.
///
/// ## Lifecycle
///
/// ```text
/// [pending] ──claim──▶ [running] ──complete──▶ [completed]
///                          │
///                          ├──fail──▶ [failed]  (terminal)
///                          │
///                          └──retryable──▶ [retryable] ──claim──▶ [running] ...
///                                             │
///                                     (exhausted) ──▶ [failed]
/// ```
///
/// ## Claim semantics
///
/// * `claimed_by` identifies the worker that holds the job. Set on claim,
///   cleared on completion/failure/retry.
/// * `claimed_at` is the timestamp of the claim. Set/cleared with `claimed_by`.
/// * `available_at` gates when a retryable job becomes re-claimable. A job
///   is eligible when `available_at <= SYSTIMESTAMP`.
/// * `attempt_count` is incremented on each claim (not on failure).
/// * A failure is terminal when the caller invokes `fail_job`. A failure
///   becomes terminal automatically when `mark_job_retryable` is called but
///   `attempt_count >= max_attempts`.
pub trait EnrichmentJobLifecycleStore: Sync + Send {
    /// Insert newly spawned jobs.
    fn enqueue_jobs(&self, jobs: &[NewEnrichmentJob]) -> StorageResult<()>;

    /// Atomically claim the next eligible job.
    ///
    /// Eligible: `job_status IN ('pending', 'retryable') AND available_at <= SYSTIMESTAMP`.
    /// The job is locked with `FOR UPDATE SKIP LOCKED` so concurrent workers
    /// never claim the same job. On claim, the row is updated to `running`
    /// with `claimed_by`, `claimed_at`, and `attempt_count` incremented.
    ///
    /// Returns `None` if no eligible job exists.
    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>>;

    /// Claim additional jobs that match an already-claimed template job.
    ///
    /// Implementations should only return eligible jobs with the same
    /// `job_type`, `enrichment_tier`, and equivalent required capabilities as
    /// `template_job`, up to `limit`.
    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>>;

    /// Claim up to `limit` eligible jobs of a specific type and optional tier.
    ///
    /// Like `claim_next_job` but filtered to a single `job_type` and
    /// returning multiple jobs. Used by per-stage pollers to claim a
    /// small batch of same-stage work.
    ///
    /// When `enrichment_tier` is `Some`, only jobs of that tier are claimed.
    /// When `None`, jobs of any tier are eligible (useful for tier-agnostic
    /// stages like retrieve_context).
    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: JobType,
        enrichment_tier: Option<EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>>;

    /// Mark a running job as successfully completed by the claiming worker.
    ///
    /// Sets `job_status = 'completed'`, `completed_at = SYSTIMESTAMP`.
    /// Clears `claimed_by` and `claimed_at`.
    ///
    /// The transition only succeeds when `claimed_by = worker_id`.
    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()>;

    /// Mark a running job as terminally failed (no further retries).
    ///
    /// Sets `job_status = 'failed'`, records the error message.
    /// Clears `claimed_by` and `claimed_at`.
    ///
    /// The transition only succeeds when `claimed_by = worker_id`.
    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()>;

    /// Attempt to mark a running job as retryable with a backoff delay.
    ///
    /// If `attempt_count >= max_attempts`, the job is terminally failed
    /// instead and `RetryOutcome::RetriesExhausted` is returned.
    ///
    /// Otherwise, sets `job_status = 'retryable'`, records the error,
    /// schedules `available_at` for `retry_after_seconds` in the future,
    /// and clears `claimed_by`/`claimed_at`.
    ///
    /// The transition only succeeds when `claimed_by = worker_id`.
    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome>;

    /// Persist a submitted provider batch and its claimed jobs.
    fn record_batch_submission(
        &self,
        batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()>;

    /// Persist a phase transition from one provider batch to the next.
    fn transition_batch_submission(
        &self,
        completed_provider_batch_id: &str,
        next_batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()>;

    /// Mark a persisted provider batch as terminal.
    fn complete_batch(&self, provider_batch_id: &str) -> StorageResult<()>;

    /// Mark a persisted provider batch as failed.
    fn fail_batch_record(&self, provider_batch_id: &str, error_message: &str) -> StorageResult<()>;

    /// Load in-flight provider batches for restart recovery.
    fn load_running_batches(
        &self,
        stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>>;

    /// Close persisted running batches whose linked jobs are no longer running.
    fn reconcile_stale_running_batches(&self, stage_name: &str) -> StorageResult<usize>;

    /// Requeue stale running jobs for a batch-polled stage when the owning
    /// provider batch is no longer running.
    fn reconcile_stale_running_jobs(&self, stage_name: &str) -> StorageResult<usize>;
}
