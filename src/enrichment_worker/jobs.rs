use super::context::{ExtractionPolicy, WorkerContext};
use super::embedding::process_embedding_job;
use super::extract::{process_extract_job, process_extract_job_batch};
use super::reconcile::{process_reconcile_job, process_reconcile_job_batch};
use crate::config::InferenceExecutionMode;
use crate::processor::ProcessorError;
use crate::storage::{ArtifactExtractionResult, ClaimedJob, EnrichmentJobLifecycleStore};

pub(super) fn has_reconciliation_candidates(extraction_result: &ArtifactExtractionResult) -> bool {
    !(extraction_result.memories.is_empty()
        && extraction_result.entities.is_empty()
        && extraction_result.relationships.is_empty())
}

pub(super) fn process_claimed_jobs(
    worker_id: &str,
    first_job: ClaimedJob,
    ctx: &WorkerContext<'_>,
    execution_mode: InferenceExecutionMode,
    policy: &ExtractionPolicy<'_>,
) -> std::result::Result<(), String> {
    if execution_mode == InferenceExecutionMode::Batch {
        match first_job.job_type {
            crate::storage::JobType::ArtifactExtract => {
                if let Some(batch_processor) = ctx
                    .processor_factory
                    .build_batch_processor(first_job.enrichment_tier)
                    .map_err(|err| {
                        fail_job(
                            ctx.job_store,
                            worker_id,
                            &first_job.job_id,
                            "Failed to build extraction batch processor",
                            err,
                        )
                    })?
                {
                    let mut jobs = vec![first_job];
                    let additional = ctx
                        .job_store
                        .claim_matching_jobs(
                            worker_id,
                            &jobs[0],
                            batch_processor.max_batch_jobs().saturating_sub(1),
                        )
                        .map_err(|err| {
                            format!("failed to claim matching extraction jobs: {err}")
                        })?;
                    jobs.extend(additional);
                    return process_extract_job_batch(
                        worker_id,
                        jobs,
                        ctx,
                        batch_processor.as_ref(),
                        policy,
                    );
                }
            }
            crate::storage::JobType::ArtifactReconcile => {
                if let Some(batch_processor) = ctx
                    .processor_factory
                    .build_reconciliation_batch_processor(first_job.enrichment_tier)
                    .map_err(|err| {
                        fail_job(
                            ctx.job_store,
                            worker_id,
                            &first_job.job_id,
                            "Failed to build reconciliation batch processor",
                            err,
                        )
                    })?
                {
                    let mut jobs = vec![first_job];
                    let additional = ctx
                        .job_store
                        .claim_matching_jobs(
                            worker_id,
                            &jobs[0],
                            batch_processor.max_batch_jobs().saturating_sub(1),
                        )
                        .map_err(|err| {
                            format!("failed to claim matching reconciliation jobs: {err}")
                        })?;
                    jobs.extend(additional);
                    return process_reconcile_job_batch(
                        worker_id,
                        jobs,
                        ctx,
                        batch_processor.as_ref(),
                    );
                }
            }
            _ => {}
        }
    }

    process_claimed_job(worker_id, first_job, ctx, policy)
}

pub(super) fn process_claimed_job(
    worker_id: &str,
    claimed_job: ClaimedJob,
    ctx: &WorkerContext<'_>,
    policy: &ExtractionPolicy<'_>,
) -> std::result::Result<(), String> {
    match claimed_job.job_type {
        crate::storage::JobType::ArtifactExtract => {
            process_extract_job(worker_id, &claimed_job, ctx, policy)
        }
        crate::storage::JobType::ArtifactReconcile => process_reconcile_job(
            worker_id,
            &claimed_job,
            ctx,
            ctx.embedding_store.is_some() && ctx.embedding_provider.is_some(),
        ),
        crate::storage::JobType::DerivedObjectEmbed => process_embedding_job(
            worker_id,
            &claimed_job,
            ctx.job_store,
            ctx.embedding_store,
            ctx.embedding_provider,
        ),
    }
}

pub(super) fn fail_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    context: &str,
    err: impl std::fmt::Display,
) -> String {
    fail_job_message(job_store, worker_id, job_id, format!("{context}: {err}"))
}

pub(super) fn handle_processor_error(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    err: ProcessorError,
) -> String {
    if err.should_reschedule_without_attempt() {
        let message = format!("Processor execution rate-limited: {err}");
        if let Err(reschedule_err) = job_store.reschedule_running_job(
            worker_id,
            job_id,
            &message,
            err.recommended_retry_after_seconds(),
        ) {
            return format!(
                "{message}; additionally failed to reschedule job without consuming attempt: {reschedule_err}"
            );
        }
        return message;
    }

    if err.is_retryable() {
        let message = format!("Processor execution failed: {err}");
        return mark_job_retryable_message(
            job_store,
            worker_id,
            job_id,
            message,
            err.recommended_retry_after_seconds(),
        );
    }

    fail_job(
        job_store,
        worker_id,
        job_id,
        "Processor execution failed",
        err,
    )
}

pub(super) fn fail_job_message(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    message: String,
) -> String {
    if let Err(fail_err) = job_store.fail_job(worker_id, job_id, &message) {
        format!("{message}; additionally failed to mark job failed: {fail_err}")
    } else {
        message
    }
}

pub(super) fn mark_job_retryable_message(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    message: String,
    retry_after_seconds: i64,
) -> String {
    match job_store.mark_job_retryable(worker_id, job_id, &message, retry_after_seconds) {
        Ok(crate::storage::RetryOutcome::Retried) => message,
        Ok(crate::storage::RetryOutcome::RetriesExhausted) => {
            format!("{message}; retries exhausted and job marked failed")
        }
        Err(retry_err) => {
            format!("{message}; additionally failed to mark job retryable: {retry_err}")
        }
    }
}
