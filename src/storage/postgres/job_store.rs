use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db;
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::types::{
    ClaimedJob, EnrichmentTier, JobType, NewEnrichmentBatch, NewEnrichmentJob,
    PersistedEnrichmentBatch, RetryOutcome,
};

use super::job;
use super::tx::{begin_transaction, commit_transaction};

pub struct PostgresEnrichmentJobStore {
    config: PostgresConfig,
}

impl PostgresEnrichmentJobStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl EnrichmentJobLifecycleStore for PostgresEnrichmentJobStore {
    fn enqueue_jobs(&self, jobs: &[NewEnrichmentJob]) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        begin_transaction(&mut client, &self.config.connection_string)?;
        for job in jobs {
            job::insert_job(&mut client, job)?;
        }
        commit_transaction(&mut client, &self.config.connection_string)?;
        Ok(())
    }

    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_next_job(&mut client, worker_id)
    }

    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_matching_jobs(&mut client, worker_id, template_job, limit)
    }

    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: JobType,
        enrichment_tier: Option<EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_jobs_by_type(&mut client, worker_id, job_type, enrichment_tier, limit)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::complete_job(&mut client, worker_id, job_id)
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::fail_job(&mut client, worker_id, job_id, error_message)
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let mut client = postgres_db::connect(&self.config)?;
        job::mark_job_retryable(
            &mut client,
            worker_id,
            job_id,
            error_message,
            retry_after_seconds,
        )
    }

    fn reschedule_running_job(
        &self,
        worker_id: &str,
        job_id: &str,
        message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::reschedule_running_job(&mut client, worker_id, job_id, message, retry_after_seconds)
    }

    fn record_batch_submission(
        &self,
        batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::record_batch_submission(&mut client, batch, jobs)
    }

    fn transition_batch_submission(
        &self,
        completed_provider_batch_id: &str,
        next_batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::transition_batch_submission(&mut client, completed_provider_batch_id, next_batch, jobs)
    }

    fn complete_batch(&self, provider_batch_id: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::complete_batch(&mut client, provider_batch_id)
    }

    fn fail_batch_record(&self, provider_batch_id: &str, error_message: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::fail_batch_record(&mut client, provider_batch_id, error_message)
    }

    fn load_running_batches(
        &self,
        stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::load_running_batches(&mut client, stage_name)
    }

    fn reconcile_stale_running_batches(&self, stage_name: &str) -> StorageResult<usize> {
        let mut client = postgres_db::connect(&self.config)?;
        job::reconcile_stale_running_batches(&mut client, stage_name)
    }

    fn reconcile_stale_running_jobs(&self, stage_name: &str) -> StorageResult<usize> {
        let mut client = postgres_db::connect(&self.config)?;
        job::reconcile_stale_running_jobs(&mut client, stage_name)
    }
}
