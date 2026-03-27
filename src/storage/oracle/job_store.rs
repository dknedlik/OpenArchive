use crate::config::OracleConfig;
use crate::db;
use crate::error::StorageResult;
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::types::{
    ClaimedJob, EnrichmentTier, JobType, NewEnrichmentBatch, NewEnrichmentJob,
    PersistedEnrichmentBatch, RetryOutcome,
};

use super::job;
use super::tx::commit_connection;

pub struct OracleEnrichmentJobStore {
    config: OracleConfig,
}

impl OracleEnrichmentJobStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl EnrichmentJobLifecycleStore for OracleEnrichmentJobStore {
    fn enqueue_jobs(&self, jobs: &[NewEnrichmentJob]) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        for job in jobs {
            job::insert_job(&conn, job)?;
        }
        commit_connection(&conn, "enqueue enrichment jobs")?;
        Ok(())
    }

    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_next_job(&conn, worker_id)
    }

    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_matching_jobs(&conn, worker_id, template_job, limit)
    }

    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: JobType,
        enrichment_tier: Option<EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let conn = db::connect(&self.config)?;
        job::claim_jobs_by_type(&conn, worker_id, job_type, enrichment_tier, limit)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        job::complete_job(&conn, worker_id, job_id)
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        job::fail_job(&conn, worker_id, job_id, error_message)
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let conn = db::connect(&self.config)?;
        job::mark_job_retryable(&conn, worker_id, job_id, error_message, retry_after_seconds)
    }

    fn reschedule_running_job(
        &self,
        worker_id: &str,
        job_id: &str,
        message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<()> {
        let conn = db::connect(&self.config)?;
        job::reschedule_running_job(&conn, worker_id, job_id, message, retry_after_seconds)
    }

    fn record_batch_submission(
        &self,
        _batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn transition_batch_submission(
        &self,
        _completed_provider_batch_id: &str,
        _next_batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn complete_batch(&self, _provider_batch_id: &str) -> StorageResult<()> {
        Ok(())
    }

    fn fail_batch_record(
        &self,
        _provider_batch_id: &str,
        _error_message: &str,
    ) -> StorageResult<()> {
        Ok(())
    }

    fn load_running_batches(
        &self,
        _stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        Ok(Vec::new())
    }

    fn reconcile_stale_running_batches(&self, _stage_name: &str) -> StorageResult<usize> {
        Ok(0)
    }

    fn reconcile_stale_running_jobs(&self, _stage_name: &str) -> StorageResult<usize> {
        Ok(0)
    }
}
