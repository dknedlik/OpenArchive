// ---------------------------------------------------------------------------
// Stage Poller — non-blocking batch pipeline for enrichment stages
// ---------------------------------------------------------------------------
//
// Each batch-capable pipeline stage (preprocess, extract, reconcile) gets a
// dedicated poller thread. The poller submits small batches to a provider's
// batch API, tracks in-flight batch IDs, polls for completion on subsequent
// loop iterations, and processes completed results. No thread ever blocks
// waiting for batch completion.
//
// Helper functions (new_id, build_extraction_result, etc.) are duplicated
// from enrichment_worker.rs below. TODO: consolidate via pub(crate).

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use log::{debug, error, info, warn};

use crate::processor::{
    ArtifactProcessorInput, BatchHandle, BatchPollResult, ExtractionBatchSubmitter,
    PreprocessBatchSubmitter, PreprocessProcessorInput, ProcessorError,
    ReconciliationBatchSubmitter, ReconciliationProcessorInput,
};
use crate::rate_limiter::RateLimiter;
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArtifactExtractPayload, ArtifactExtractionResult, ArtifactPreprocessPayload,
    ArtifactReadStore, ArtifactReconcilePayload, ArtifactRetrieveContextPayload, ClaimedJob,
    DerivedMetadataWriteStore, EnrichmentJobLifecycleStore, EnrichmentStateStore, EnrichmentTier,
    JobType, LoadedArtifactForEnrichment, NewEnrichmentJob, ReconciliationDecision,
    ReconciliationDecisionKind, RetrievalResultSet, SourceType, TopicThreadRef,
};

const RETRYABLE_INFERENCE_BACKOFF_SECONDS: i64 = 60;
const RATE_LIMIT_429_BACKOFF_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// InFlightContext — stage-specific state carried with each in-flight batch
// ---------------------------------------------------------------------------

/// Stage-specific context carried alongside an in-flight batch.
///
/// Each variant holds the prepared inputs and payloads needed to process
/// results when the batch completes.
enum InFlightContext {
    PreprocessPhaseOne {
        inputs: Vec<PreprocessProcessorInput>,
        payloads: Vec<ArtifactPreprocessPayload>,
    },
    PreprocessPhaseTwo {
        inputs: Vec<PreprocessProcessorInput>,
        payloads: Vec<ArtifactPreprocessPayload>,
        phase_one_data: Box<dyn std::any::Any>,
    },
    Extract {
        inputs: Vec<ArtifactProcessorInput>,
        payloads: Vec<ArtifactExtractPayload>,
    },
    Reconcile {
        inputs: Vec<ReconciliationProcessorInput>,
        payloads: Vec<ArtifactReconcilePayload>,
        extraction_results: Vec<ArtifactExtractionResult>,
        retrieval_result_sets: Vec<RetrievalResultSet>,
        loaded_artifacts: Vec<LoadedArtifactForEnrichment>,
    },
}

// ---------------------------------------------------------------------------
// InFlightBatch — tracks one in-flight provider batch
// ---------------------------------------------------------------------------

/// Tracks a single in-flight batch submitted to a provider's batch API.
pub struct InFlightBatch {
    /// Provider-returned handle for polling.
    pub handle: BatchHandle,
    /// The claimed jobs in this batch, in the same order as the inputs.
    pub jobs: Vec<ClaimedJob>,
    /// Stage-specific context needed when processing results.
    context: InFlightContext,
}

// ---------------------------------------------------------------------------
// StageBehavior — trait abstracting per-stage logic
// ---------------------------------------------------------------------------

/// Trait abstracting per-stage batch submission, polling, and result processing.
///
/// Used on a single poller thread — no `Send`/`Sync` bounds required.
pub trait StageBehavior {
    /// The job type this stage claims.
    fn job_type(&self) -> JobType;

    /// The enrichment tier this stage processes.
    ///
    /// Used to filter `claim_jobs_by_type` so that a Standard-tier poller
    /// never claims Quality-tier jobs (and vice versa).
    fn enrichment_tier(&self) -> EnrichmentTier;

    /// Human-readable name for logging.
    fn stage_name(&self) -> &'static str;

    /// Maximum jobs per batch submission.
    fn batch_size(&self) -> usize;

    /// Maximum number of concurrent in-flight batches.
    fn max_concurrent(&self) -> usize;

    /// Prepare inputs from claimed jobs and submit a batch to the provider.
    ///
    /// Jobs that fail preparation (bad payload, missing artifact, etc.) are
    /// individually failed and excluded from the batch. Returns `Err(())`
    /// only if no jobs could be submitted (all failed individually).
    fn submit(
        &self,
        jobs: Vec<ClaimedJob>,
        rate_limiter: &RateLimiter,
        read_store: &dyn ArtifactReadStore,
        state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, ()>;

    /// Poll the provider for batch completion.
    fn poll(&self, batch: &InFlightBatch) -> Result<BatchPollResult, ProcessorError>;

    /// Process a completed batch.
    ///
    /// Returns `Ok(Some(new_batch))` when a phase transition occurs (e.g.
    /// preprocess phase one -> phase two). Returns `Ok(None)` when the batch
    /// is fully processed.
    fn process_completed(
        &self,
        batch: InFlightBatch,
        data: Box<dyn std::any::Any>,
        job_store: &dyn EnrichmentJobLifecycleStore,
        state_store: &dyn EnrichmentStateStore,
        derived_store: &dyn DerivedMetadataWriteStore,
        worker_id: &str,
    ) -> Result<Option<InFlightBatch>, ()>;
}

// ---------------------------------------------------------------------------
// stage_poller_loop — the generic non-blocking poller
// ---------------------------------------------------------------------------

/// Runs the non-blocking batch poller loop for a single pipeline stage.
///
/// The loop:
/// 1. Polls all in-flight batches for completion (rate-limited).
/// 2. Processes completed/failed batches.
/// 3. Claims new jobs if capacity is available and submits a new batch.
/// 4. Sleeps for `poll_interval` before the next iteration.
pub fn stage_poller_loop(
    stage: &dyn StageBehavior,
    worker_id: String,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    rate_limiter: &RateLimiter,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!(
        "[{}] stage poller {} starting",
        stage.stage_name(),
        worker_id
    );

    let mut in_flight: HashMap<String, InFlightBatch> = HashMap::new();

    loop {
        if shutdown.is_shutdown() {
            info!(
                "[{}] stage poller {} shutting down ({} batches still in-flight)",
                stage.stage_name(),
                worker_id,
                in_flight.len()
            );
            break;
        }

        // -----------------------------------------------------------------
        // Step 1: Poll all in-flight batches
        // -----------------------------------------------------------------
        let mut completed: Vec<(String, Box<dyn std::any::Any>)> = Vec::new();
        let mut failed: Vec<(String, String)> = Vec::new();

        for (batch_id, batch) in in_flight.iter() {
            if rate_limiter.try_acquire().is_err() {
                debug!(
                    "[{}] rate limit hit while polling batch {}, skipping",
                    stage.stage_name(),
                    batch_id
                );
                continue;
            }

            match stage.poll(batch) {
                Ok(BatchPollResult::Succeeded(data)) => {
                    debug!(
                        "[{}] batch {} completed successfully",
                        stage.stage_name(),
                        batch_id
                    );
                    completed.push((batch_id.clone(), data));
                }
                Ok(BatchPollResult::Failed(msg)) => {
                    error!(
                        "[{}] batch {} failed: {}",
                        stage.stage_name(),
                        batch_id,
                        msg
                    );
                    failed.push((batch_id.clone(), msg));
                }
                Ok(BatchPollResult::Pending) => {
                    debug!(
                        "[{}] batch {} still pending",
                        stage.stage_name(),
                        batch_id
                    );
                }
                Err(ProcessorError::InferenceHttpStatus { status: 429, .. }) => {
                    warn!(
                        "[{}] 429 rate limit on batch {} poll, signaling backoff",
                        stage.stage_name(),
                        batch_id
                    );
                    rate_limiter.signal_backoff(Duration::from_secs(RATE_LIMIT_429_BACKOFF_SECS));
                }
                Err(err) => {
                    error!(
                        "[{}] error polling batch {}: {}",
                        stage.stage_name(),
                        batch_id,
                        err
                    );
                    // If the poll error itself is retryable, leave the batch
                    // in-flight for a subsequent poll attempt. Otherwise,
                    // treat as a terminal failure.
                    if !err.is_retryable() {
                        failed.push((batch_id.clone(), format!("{err}")));
                    }
                }
            }
        }

        // -----------------------------------------------------------------
        // Step 2: Process completed batches
        // -----------------------------------------------------------------
        for (batch_id, data) in completed {
            let batch = match in_flight.remove(&batch_id) {
                Some(b) => b,
                None => continue,
            };
            match stage.process_completed(batch, data, job_store, state_store, derived_store, &worker_id) {
                Ok(Some(new_batch)) => {
                    // Phase transition (preprocess phase one -> phase two).
                    let new_id = new_batch.handle.batch_id.clone();
                    debug!(
                        "[{}] phase transition: {} -> {}",
                        stage.stage_name(),
                        batch_id,
                        new_id
                    );
                    in_flight.insert(new_id, new_batch);
                }
                Ok(None) => {
                    debug!("[{}] batch {} fully processed", stage.stage_name(), batch_id);
                }
                Err(()) => {
                    // Errors already handled inside process_completed per-job.
                    debug!(
                        "[{}] batch {} processing encountered errors (handled per-job)",
                        stage.stage_name(),
                        batch_id
                    );
                }
            }
        }

        // Process failed batches: fail all jobs in each batch.
        for (batch_id, error_message) in failed {
            if let Some(batch) = in_flight.remove(&batch_id) {
                for job in batch.jobs {
                    let msg = format!(
                        "Batch {} failed: {}",
                        batch_id, error_message
                    );
                    poller_fail_job_message(job_store, &worker_id, &job.job_id, msg);
                }
            }
        }

        // -----------------------------------------------------------------
        // Step 3: Claim new jobs if capacity available
        // -----------------------------------------------------------------
        if in_flight.len() < stage.max_concurrent() {
            match job_store.claim_jobs_by_type(&worker_id, stage.job_type(), Some(stage.enrichment_tier()), stage.batch_size()) {
                Ok(jobs) if !jobs.is_empty() => {
                    debug!(
                        "[{}] claimed {} jobs",
                        stage.stage_name(),
                        jobs.len()
                    );
                    match stage.submit(jobs, rate_limiter, read_store, state_store, job_store, &worker_id) {
                        Ok(batch) => {
                            let id = batch.handle.batch_id.clone();
                            in_flight.insert(id, batch);
                        }
                        Err(()) => {
                            // All individual jobs were failed in submit().
                            debug!(
                                "[{}] all claimed jobs failed during submit preparation",
                                stage.stage_name()
                            );
                        }
                    }
                }
                Ok(_) => {
                    // No jobs available, nothing to do.
                }
                Err(err) => {
                    error!(
                        "[{}] failed to claim jobs: {}",
                        stage.stage_name(),
                        err
                    );
                }
            }
        }

        // -----------------------------------------------------------------
        // Step 4: Sleep
        // -----------------------------------------------------------------
        thread::sleep(poll_interval);
    }
}

// ---------------------------------------------------------------------------
// PreprocessStage
// ---------------------------------------------------------------------------

/// Stage behavior for the preprocessing pipeline stage.
///
/// Preprocessing is two-phase: phase one identifies topic threads, phase two
/// produces the final preprocessing output. Both phases go through the batch
/// submitter when available.
pub struct PreprocessStage {
    submitter: Box<dyn PreprocessBatchSubmitter>,
    tier: EnrichmentTier,
    batch_size: usize,
    max_concurrent: usize,
}

impl PreprocessStage {
    pub fn new(
        submitter: Box<dyn PreprocessBatchSubmitter>,
        tier: EnrichmentTier,
        batch_size: usize,
        max_concurrent: usize,
    ) -> Self {
        Self {
            submitter,
            tier,
            batch_size,
            max_concurrent,
        }
    }
}

impl StageBehavior for PreprocessStage {
    fn job_type(&self) -> JobType {
        JobType::ArtifactPreprocess
    }

    fn enrichment_tier(&self) -> EnrichmentTier {
        self.tier
    }

    fn stage_name(&self) -> &'static str {
        "preprocess"
    }

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    fn submit(
        &self,
        jobs: Vec<ClaimedJob>,
        rate_limiter: &RateLimiter,
        read_store: &dyn ArtifactReadStore,
        _state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, ()> {
        let mut valid_jobs: Vec<ClaimedJob> = Vec::new();
        let mut inputs = Vec::new();
        let mut payloads = Vec::new();

        for job in jobs {
            let payload = match ArtifactPreprocessPayload::from_json(&job.payload_json) {
                Ok(p) => p,
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to parse preprocess payload JSON", err,
                    );
                    continue;
                }
            };
            let Some(source_type) = SourceType::from_str(&payload.source_type) else {
                poller_fail_job_message(
                    job_store, worker_id, &job.job_id,
                    format!("Invalid source_type in preprocess payload: {}", payload.source_type),
                );
                continue;
            };
            let loaded = match read_store.load_artifact_for_enrichment(&job.artifact_id) {
                Ok(Some(l)) => l,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store, worker_id, &job.job_id,
                        format!("Artifact {} not found for preprocessing", job.artifact_id),
                    );
                    continue;
                }
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to load artifact for preprocessing", err,
                    );
                    continue;
                }
            };
            let input = PreprocessProcessorInput {
                artifact_id: loaded.artifact.artifact_id.clone(),
                import_id: payload.import_id.clone(),
                source_type,
                title: loaded.artifact.title.clone(),
                participants: loaded.participants,
                segments: loaded.segments,
            };
            inputs.push(input);
            payloads.push(payload);
            valid_jobs.push(job);
        }

        if valid_jobs.is_empty() {
            return Err(());
        }

        // Acquire a rate limit token before submission.
        if let Err(wait) = rate_limiter.try_acquire() {
            debug!("[preprocess] rate limit wait {:?} before submit, skipping", wait);
            // Return the jobs to the pool by marking them retryable.
            for job in &valid_jobs {
                poller_mark_retryable(job_store, worker_id, &job.job_id, "Rate limited before submit", 5);
            }
            return Err(());
        }

        match self.submitter.submit_phase_one(&inputs) {
            Ok(handle) => Ok(InFlightBatch {
                handle,
                jobs: valid_jobs,
                context: InFlightContext::PreprocessPhaseOne { inputs, payloads },
            }),
            Err(err) => {
                if matches!(err, ProcessorError::InferenceHttpStatus { status: 429, .. }) {
                    rate_limiter.signal_backoff(Duration::from_secs(RATE_LIMIT_429_BACKOFF_SECS));
                }
                for job in &valid_jobs {
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                }
                Err(())
            }
        }
    }

    fn poll(&self, batch: &InFlightBatch) -> Result<BatchPollResult, ProcessorError> {
        self.submitter.poll_batch(&batch.handle)
    }

    fn process_completed(
        &self,
        batch: InFlightBatch,
        data: Box<dyn std::any::Any>,
        job_store: &dyn EnrichmentJobLifecycleStore,
        _state_store: &dyn EnrichmentStateStore,
        _derived_store: &dyn DerivedMetadataWriteStore,
        worker_id: &str,
    ) -> Result<Option<InFlightBatch>, ()> {
        match batch.context {
            InFlightContext::PreprocessPhaseOne { inputs, payloads } => {
                // Parse phase one results, then submit phase two.
                let phase_one_data = match self.submitter.parse_phase_one(data, &inputs) {
                    Ok(d) => d,
                    Err(err) => {
                        for job in &batch.jobs {
                            poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                        }
                        return Err(());
                    }
                };

                match self.submitter.submit_phase_two(&inputs, phase_one_data.as_ref()) {
                    Ok(new_handle) => {
                        let new_batch = InFlightBatch {
                            handle: new_handle,
                            jobs: batch.jobs,
                            context: InFlightContext::PreprocessPhaseTwo {
                                inputs,
                                payloads,
                                phase_one_data,
                            },
                        };
                        Ok(Some(new_batch))
                    }
                    Err(err) => {
                        for job in &batch.jobs {
                            poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                        }
                        Err(())
                    }
                }
            }
            InFlightContext::PreprocessPhaseTwo {
                inputs,
                payloads,
                phase_one_data,
            } => {
                // Parse phase two results and process each.
                let results = self.submitter.parse_phase_two(data, &inputs, phase_one_data.as_ref());

                for ((job, (input, payload)), result) in batch
                    .jobs
                    .into_iter()
                    .zip(inputs.into_iter().zip(payloads.into_iter()))
                    .zip(results.into_iter())
                {
                    match result {
                        Ok(output) => {
                            let coverage_windows = build_preprocess_coverage_windows(
                                &job.artifact_id,
                                &input.segments,
                                &output.topic_threads,
                            );
                            let extract_job = NewEnrichmentJob {
                                job_id: new_id("job"),
                                artifact_id: job.artifact_id.clone(),
                                job_type: JobType::ArtifactExtract,
                                enrichment_tier: job.enrichment_tier,
                                spawned_by_job_id: Some(job.job_id.clone()),
                                job_status: crate::storage::JobStatus::Pending,
                                max_attempts: 3,
                                priority_no: 100,
                                required_capabilities: vec!["text".to_string()],
                                payload_json: ArtifactExtractPayload::new_v1(
                                    &job.artifact_id,
                                    &payload.import_id,
                                    input.source_type,
                                    coverage_windows,
                                    output.topic_threads,
                                )
                                .to_json(),
                            };
                            if let Err(err) = job_store.enqueue_jobs(&[extract_job]) {
                                poller_fail_job(
                                    job_store, worker_id, &job.job_id,
                                    "Failed to enqueue extraction job from preprocess", err,
                                );
                                continue;
                            }
                            if let Err(msg) = poller_complete_job(job_store, worker_id, &job.job_id) {
                                error!("[preprocess] {}", msg);
                            }
                        }
                        Err(err) => {
                            poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                        }
                    }
                }
                Ok(None)
            }
            _ => {
                error!("[preprocess] process_completed called with wrong context variant");
                Err(())
            }
        }
    }

}

// ---------------------------------------------------------------------------
// ExtractStage
// ---------------------------------------------------------------------------

/// Stage behavior for the extraction pipeline stage.
pub struct ExtractStage {
    submitter: Box<dyn ExtractionBatchSubmitter>,
    tier: EnrichmentTier,
    batch_size: usize,
    max_concurrent: usize,
}

impl ExtractStage {
    pub fn new(
        submitter: Box<dyn ExtractionBatchSubmitter>,
        tier: EnrichmentTier,
        batch_size: usize,
        max_concurrent: usize,
    ) -> Self {
        Self {
            submitter,
            tier,
            batch_size,
            max_concurrent,
        }
    }
}

impl StageBehavior for ExtractStage {
    fn job_type(&self) -> JobType {
        JobType::ArtifactExtract
    }

    fn enrichment_tier(&self) -> EnrichmentTier {
        self.tier
    }

    fn stage_name(&self) -> &'static str {
        "extract"
    }

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    fn submit(
        &self,
        jobs: Vec<ClaimedJob>,
        rate_limiter: &RateLimiter,
        read_store: &dyn ArtifactReadStore,
        _state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, ()> {
        let mut valid_jobs = Vec::new();
        let mut inputs = Vec::new();
        let mut payloads = Vec::new();

        for job in jobs {
            let payload = match ArtifactExtractPayload::from_json(&job.payload_json) {
                Ok(p) => p,
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to parse extract payload JSON", err,
                    );
                    continue;
                }
            };
            let Some(source_type) = SourceType::from_str(&payload.source_type) else {
                poller_fail_job_message(
                    job_store, worker_id, &job.job_id,
                    format!("Invalid source_type in extract payload: {}", payload.source_type),
                );
                continue;
            };

            let loaded = match read_store.load_artifact_for_enrichment(&job.artifact_id) {
                Ok(Some(l)) => l,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store, worker_id, &job.job_id,
                        format!("Artifact {} not found for extraction", job.artifact_id),
                    );
                    continue;
                }
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to load artifact for extraction", err,
                    );
                    continue;
                }
            };
            let input = ArtifactProcessorInput {
                artifact_id: loaded.artifact.artifact_id.clone(),
                import_id: payload.import_id.clone(),
                source_type,
                title: loaded.artifact.title.clone(),
                participants: loaded.participants,
                segments: loaded.segments,
            };
            inputs.push(input);
            payloads.push(payload);
            valid_jobs.push(job);
        }

        if valid_jobs.is_empty() {
            return Err(());
        }

        if let Err(wait) = rate_limiter.try_acquire() {
            debug!("[extract] rate limit wait {:?} before submit, skipping", wait);
            for job in &valid_jobs {
                poller_mark_retryable(job_store, worker_id, &job.job_id, "Rate limited before submit", 5);
            }
            return Err(());
        }

        match self.submitter.prepare_and_submit(&inputs) {
            Ok(handle) => Ok(InFlightBatch {
                handle,
                jobs: valid_jobs,
                context: InFlightContext::Extract { inputs, payloads },
            }),
            Err(err) => {
                if matches!(err, ProcessorError::InferenceHttpStatus { status: 429, .. }) {
                    rate_limiter.signal_backoff(Duration::from_secs(RATE_LIMIT_429_BACKOFF_SECS));
                }
                for job in &valid_jobs {
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                }
                Err(())
            }
        }
    }

    fn poll(&self, batch: &InFlightBatch) -> Result<BatchPollResult, ProcessorError> {
        self.submitter.poll_batch(&batch.handle)
    }

    fn process_completed(
        &self,
        batch: InFlightBatch,
        data: Box<dyn std::any::Any>,
        job_store: &dyn EnrichmentJobLifecycleStore,
        state_store: &dyn EnrichmentStateStore,
        _derived_store: &dyn DerivedMetadataWriteStore,
        worker_id: &str,
    ) -> Result<Option<InFlightBatch>, ()> {
        let (inputs, payloads) = match batch.context {
            InFlightContext::Extract { inputs, payloads } => (inputs, payloads),
            _ => {
                error!("[extract] process_completed called with wrong context variant");
                return Err(());
            }
        };

        let results = self.submitter.parse_results(data, &inputs);

        for ((job, (input, payload)), result) in batch
            .jobs
            .into_iter()
            .zip(inputs.into_iter().zip(payloads.into_iter()))
            .zip(results.into_iter())
        {
            match result {
                Ok(output) => {
                    let extraction_result = build_extraction_result(
                        &job,
                        &input,
                        &output,
                        payload.conversation_windows,
                    );
                    if let Err(err) = state_store.save_extraction_result(&extraction_result) {
                        poller_fail_job(
                            job_store, worker_id, &job.job_id,
                            "Failed to persist extraction result", err,
                        );
                        continue;
                    }
                    let retrieve_job = NewEnrichmentJob {
                        job_id: new_id("job"),
                        artifact_id: job.artifact_id.clone(),
                        job_type: JobType::ArtifactRetrieveContext,
                        enrichment_tier: job.enrichment_tier,
                        spawned_by_job_id: Some(job.job_id.clone()),
                        job_status: crate::storage::JobStatus::Pending,
                        max_attempts: 3,
                        priority_no: 100,
                        required_capabilities: vec!["archive_retrieval".to_string()],
                        payload_json: ArtifactRetrieveContextPayload::new_v1(
                            &job.artifact_id,
                            &payload.import_id,
                            input.source_type,
                            &extraction_result.extraction_result_id,
                        )
                        .to_json(),
                    };
                    if let Err(err) = job_store.enqueue_jobs(&[retrieve_job]) {
                        poller_fail_job(
                            job_store, worker_id, &job.job_id,
                            "Failed to enqueue retrieval-context job", err,
                        );
                        continue;
                    }
                    if let Err(msg) = poller_complete_job(job_store, worker_id, &job.job_id) {
                        error!("[extract] {}", msg);
                    }
                }
                Err(err) => {
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                }
            }
        }
        Ok(None)
    }

}

// ---------------------------------------------------------------------------
// ReconcileStage
// ---------------------------------------------------------------------------

/// Stage behavior for the reconciliation pipeline stage.
pub struct ReconcileStage {
    submitter: Box<dyn ReconciliationBatchSubmitter>,
    tier: EnrichmentTier,
    batch_size: usize,
    max_concurrent: usize,
}

impl ReconcileStage {
    pub fn new(
        submitter: Box<dyn ReconciliationBatchSubmitter>,
        tier: EnrichmentTier,
        batch_size: usize,
        max_concurrent: usize,
    ) -> Self {
        Self {
            submitter,
            tier,
            batch_size,
            max_concurrent,
        }
    }
}

impl StageBehavior for ReconcileStage {
    fn job_type(&self) -> JobType {
        JobType::ArtifactReconcile
    }

    fn enrichment_tier(&self) -> EnrichmentTier {
        self.tier
    }

    fn stage_name(&self) -> &'static str {
        "reconcile"
    }

    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    fn submit(
        &self,
        jobs: Vec<ClaimedJob>,
        rate_limiter: &RateLimiter,
        read_store: &dyn ArtifactReadStore,
        state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, ()> {
        let mut valid_jobs = Vec::new();
        let mut inputs = Vec::new();
        let mut payloads = Vec::new();
        let mut extraction_results = Vec::new();
        let mut retrieval_result_sets = Vec::new();
        let mut loaded_artifacts = Vec::new();

        for job in jobs {
            let payload = match ArtifactReconcilePayload::from_json(&job.payload_json) {
                Ok(p) => p,
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to parse reconcile payload JSON", err,
                    );
                    continue;
                }
            };
            let loaded = match read_store.load_artifact_for_enrichment(&job.artifact_id) {
                Ok(Some(l)) => l,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store, worker_id, &job.job_id,
                        format!("Artifact {} not found for reconciliation", job.artifact_id),
                    );
                    continue;
                }
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to load artifact for reconciliation", err,
                    );
                    continue;
                }
            };
            let extraction_result = match state_store
                .load_extraction_result(&payload.extraction_result_id)
            {
                Ok(Some(r)) => r,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store, worker_id, &job.job_id,
                        format!(
                            "Extraction result {} not found",
                            payload.extraction_result_id
                        ),
                    );
                    continue;
                }
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to load extraction result for reconciliation", err,
                    );
                    continue;
                }
            };
            let retrieval_result_set = match state_store
                .load_retrieval_result_set(&payload.retrieval_result_set_id)
            {
                Ok(Some(r)) => r,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store, worker_id, &job.job_id,
                        format!(
                            "Retrieval result set {} not found",
                            payload.retrieval_result_set_id
                        ),
                    );
                    continue;
                }
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to load retrieval result set for reconciliation", err,
                    );
                    continue;
                }
            };
            let input = match build_reconciliation_input(
                &job.artifact_id,
                &payload.source_type,
                &extraction_result,
                &retrieval_result_set,
            ) {
                Ok(i) => i,
                Err(err) => {
                    poller_fail_job(
                        job_store, worker_id, &job.job_id,
                        "Failed to build reconciliation input", err,
                    );
                    continue;
                }
            };
            inputs.push(input);
            payloads.push(payload);
            extraction_results.push(extraction_result);
            retrieval_result_sets.push(retrieval_result_set);
            loaded_artifacts.push(loaded);
            valid_jobs.push(job);
        }

        if valid_jobs.is_empty() {
            return Err(());
        }

        if let Err(wait) = rate_limiter.try_acquire() {
            debug!("[reconcile] rate limit wait {:?} before submit, skipping", wait);
            for job in &valid_jobs {
                poller_mark_retryable(job_store, worker_id, &job.job_id, "Rate limited before submit", 5);
            }
            return Err(());
        }

        match self.submitter.prepare_and_submit(&inputs) {
            Ok(handle) => Ok(InFlightBatch {
                handle,
                jobs: valid_jobs,
                context: InFlightContext::Reconcile {
                    inputs,
                    payloads,
                    extraction_results,
                    retrieval_result_sets,
                    loaded_artifacts,
                },
            }),
            Err(err) => {
                if matches!(err, ProcessorError::InferenceHttpStatus { status: 429, .. }) {
                    rate_limiter.signal_backoff(Duration::from_secs(RATE_LIMIT_429_BACKOFF_SECS));
                }
                for job in &valid_jobs {
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                }
                Err(())
            }
        }
    }

    fn poll(&self, batch: &InFlightBatch) -> Result<BatchPollResult, ProcessorError> {
        self.submitter.poll_batch(&batch.handle)
    }

    fn process_completed(
        &self,
        batch: InFlightBatch,
        data: Box<dyn std::any::Any>,
        job_store: &dyn EnrichmentJobLifecycleStore,
        state_store: &dyn EnrichmentStateStore,
        derived_store: &dyn DerivedMetadataWriteStore,
        worker_id: &str,
    ) -> Result<Option<InFlightBatch>, ()> {
        let (
            inputs,
            _payloads,
            extraction_results,
            retrieval_result_sets,
            loaded_artifacts,
        ) = match batch.context {
            InFlightContext::Reconcile {
                inputs,
                payloads,
                extraction_results,
                retrieval_result_sets,
                loaded_artifacts,
            } => (inputs, payloads, extraction_results, retrieval_result_sets, loaded_artifacts),
            _ => {
                error!("[reconcile] process_completed called with wrong context variant");
                return Err(());
            }
        };

        let results = self.submitter.parse_results(data, &inputs);

        // Zip everything together: (job, extraction_result, retrieval_result_set, loaded_artifact, result)
        let iter = batch
            .jobs
            .into_iter()
            .zip(extraction_results.into_iter())
            .zip(retrieval_result_sets.into_iter())
            .zip(loaded_artifacts.into_iter())
            .zip(results.into_iter());

        for ((((job, extraction_result, ), retrieval_result_set), loaded), result) in iter {
            match result {
                Ok(outputs) => {
                    let decisions = if extraction_result.memories.is_empty()
                        && extraction_result.relationships.is_empty()
                    {
                        vec![ReconciliationDecision {
                            reconciliation_decision_id: new_id("reconcile"),
                            artifact_id: extraction_result.artifact_id.clone(),
                            job_id: job.job_id.clone(),
                            extraction_result_id: extraction_result.extraction_result_id.clone(),
                            retrieval_result_set_id: retrieval_result_set
                                .retrieval_result_set_id
                                .clone(),
                            pipeline_name: "artifact_reconciliation".to_string(),
                            pipeline_version: "v1".to_string(),
                            decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
                            target_kind: "artifact".to_string(),
                            target_key: extraction_result.artifact_id.clone(),
                            matched_object_id: None,
                            rationale:
                                "No candidate memories or relationships were extracted for reconciliation."
                                    .to_string(),
                            evidence_segment_ids: extraction_result
                                .summary_evidence_segment_ids
                                .clone(),
                            status: "completed".to_string(),
                            error_message: None,
                        }]
                    } else {
                        build_reconciliation_decisions(
                            &job,
                            &extraction_result,
                            &retrieval_result_set,
                            outputs,
                        )
                    };
                    if let Err(err) = state_store.save_reconciliation_decisions(&decisions) {
                        poller_fail_job(
                            job_store, worker_id, &job.job_id,
                            "Failed to persist reconciliation decisions", err,
                        );
                        continue;
                    }
                    let attempt = build_derivation_attempt(
                        &job,
                        &loaded.artifact.artifact_id,
                        &extraction_result,
                        &decisions,
                    );
                    if let Err(err) = derived_store.write_derivation_attempt(attempt) {
                        poller_fail_job(
                            job_store, worker_id, &job.job_id,
                            "Failed to persist derivation output", err,
                        );
                        continue;
                    }
                    if let Err(msg) = poller_complete_job(job_store, worker_id, &job.job_id) {
                        error!("[reconcile] {}", msg);
                    }
                }
                Err(err) => {
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                }
            }
        }
        Ok(None)
    }

}

// ===========================================================================
// Helper functions — local equivalents of enrichment_worker helpers.
//
// These duplicate the helpers from enrichment_worker.rs because those are
// currently private. TODO: Once visibility is changed to pub(crate) in
// enrichment_worker.rs, replace these with re-exports.
// ===========================================================================

use crate::domain::SourceTimestamp;
use crate::processor::{
    ArtifactProcessorOutput, MemoryOutput, ReconciliationDecisionOutput, RelationshipOutput,
    SummaryOutput,
};
use crate::storage::{
    CandidateEntity, CandidateRelationship, ClassificationObjectJson, ConversationWindowRef,
    DerivationRunStatus, DerivationRunType, DerivedObjectPayload, EvidenceRole,
    ExtractedClassification, ExtractedMemory, InputScopeType, LoadedSegment, MemoryObjectJson,
    NewDerivationRun, NewDerivedObject, NewEvidenceLink, ObjectStatus, OriginKind,
    RelationshipObjectJson, RetrievalIntent,
    ScopeType, SummaryObjectJson, SupportStrength, WriteDerivationAttempt, WriteDerivedObject,
};
use rand::random;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

fn new_id(prefix: &str) -> String {
    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let entropy = random::<u64>();
    format!("{prefix}-{nanos:x}-{counter:x}-{entropy:x}")
}

fn poller_complete_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
) -> Result<(), String> {
    job_store
        .complete_job(worker_id, job_id)
        .map_err(|err| format!("failed to complete job {}: {}", job_id, err))
}

fn poller_fail_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    context: &str,
    err: impl std::fmt::Display,
) {
    poller_fail_job_message(
        job_store,
        worker_id,
        job_id,
        format!("{context}: {err}"),
    );
}

fn poller_fail_job_message(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    message: String,
) {
    if let Err(fail_err) = job_store.fail_job(worker_id, job_id, &message) {
        error!(
            "Failed to mark job {} as failed: {}; original error: {}",
            job_id, fail_err, message
        );
    }
}

fn poller_handle_processor_error(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    err: &ProcessorError,
) {
    if err.is_retryable() {
        let message = format!("Processor execution failed: {err}");
        poller_mark_retryable(
            job_store,
            worker_id,
            job_id,
            &message,
            RETRYABLE_INFERENCE_BACKOFF_SECONDS,
        );
        return;
    }

    poller_fail_job_message(
        job_store,
        worker_id,
        job_id,
        format!("Processor execution failed: {err}"),
    );
}

fn poller_mark_retryable(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    message: &str,
    retry_after_seconds: i64,
) {
    match job_store.mark_job_retryable(worker_id, job_id, message, retry_after_seconds) {
        Ok(crate::storage::RetryOutcome::Retried) => {
            debug!("Job {} marked retryable: {}", job_id, message);
        }
        Ok(crate::storage::RetryOutcome::RetriesExhausted) => {
            warn!(
                "Job {} retries exhausted, marked failed: {}",
                job_id, message
            );
        }
        Err(err) => {
            error!(
                "Failed to mark job {} as retryable: {}; original error: {}",
                job_id, err, message
            );
        }
    }
}

const PREPROCESS_WINDOW_SEGMENTS: usize = 24;
const PREPROCESS_WINDOW_OVERLAP: usize = 4;

fn build_preprocess_coverage_windows(
    artifact_id: &str,
    segments: &[LoadedSegment],
    topic_threads: &[TopicThreadRef],
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }
    let covered: HashSet<i32> = topic_threads
        .iter()
        .flat_map(|thread| thread.spans.iter())
        .flat_map(|span| span.start_sequence_no..=span.end_sequence_no)
        .collect();
    let uncovered: Vec<_> = segments
        .iter()
        .filter(|segment| !covered.contains(&segment.sequence_no))
        .cloned()
        .collect();
    if uncovered.is_empty() {
        return Vec::new();
    }

    build_contiguous_windows(artifact_id, &uncovered)
}

fn build_contiguous_windows(
    artifact_id: &str,
    segments: &[LoadedSegment],
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }
    if segments.len() <= PREPROCESS_WINDOW_SEGMENTS {
        return vec![ConversationWindowRef {
            window_id: format!("{artifact_id}:window:0"),
            label: "coverage fallback".to_string(),
            start_sequence_no: segments
                .first()
                .map(|segment| segment.sequence_no)
                .unwrap_or(0),
            end_sequence_no: segments
                .last()
                .map(|segment| segment.sequence_no)
                .unwrap_or(0),
        }];
    }

    let mut windows = Vec::new();
    let mut start = 0usize;
    let step = PREPROCESS_WINDOW_SEGMENTS
        .saturating_sub(PREPROCESS_WINDOW_OVERLAP)
        .max(1);
    while start < segments.len() {
        let end = (start + PREPROCESS_WINDOW_SEGMENTS).min(segments.len());
        let first = &segments[start];
        let last = &segments[end - 1];
        windows.push(ConversationWindowRef {
            window_id: format!("{artifact_id}:window:{}", windows.len()),
            label: format!("messages {}-{}", first.sequence_no, last.sequence_no),
            start_sequence_no: first.sequence_no,
            end_sequence_no: last.sequence_no,
        });
        if end == segments.len() {
            break;
        }
        start += step;
    }
    windows
}

fn build_extraction_result(
    claimed_job: &ClaimedJob,
    input: &ArtifactProcessorInput,
    output: &ArtifactProcessorOutput,
    _windows: Vec<ConversationWindowRef>,
) -> ArtifactExtractionResult {
    ArtifactExtractionResult {
        extraction_result_id: new_id("extract"),
        artifact_id: input.artifact_id.clone(),
        job_id: claimed_job.job_id.clone(),
        pipeline_name: output.pipeline_name.clone(),
        pipeline_version: output.pipeline_version.clone(),
        summary_title: output.summary.title.clone(),
        summary_body_text: output.summary.body_text.clone(),
        summary_evidence_segment_ids: output.summary.evidence_segment_ids.clone(),
        classifications: output
            .classifications
            .iter()
            .map(|c| ExtractedClassification {
                title: c.title.clone(),
                body_text: c.body_text.clone(),
                classification_type: c.classification_type.clone(),
                classification_value: c.classification_value.clone(),
                evidence_segment_ids: c.evidence_segment_ids.clone(),
            })
            .collect(),
        memories: output
            .memories
            .iter()
            .map(|m| ExtractedMemory {
                title: m.title.clone(),
                body_text: m.body_text.clone(),
                memory_type: m.memory_type.clone(),
                memory_scope: m.memory_scope,
                memory_scope_value: m.memory_scope_value.clone(),
                evidence_segment_ids: m.evidence_segment_ids.clone(),
            })
            .collect(),
        entities: output
            .entities
            .iter()
            .map(|e| CandidateEntity {
                entity_key: e.entity_key.clone(),
                display_name: e.display_name.clone(),
                entity_type: e.entity_type.clone(),
                evidence_segment_ids: e.evidence_segment_ids.clone(),
            })
            .collect(),
        relationships: output
            .relationships
            .iter()
            .map(|r| CandidateRelationship {
                relationship_type: r.relationship_type.clone(),
                subject_key: r.subject_key.clone(),
                object_key: r.object_key.clone(),
                title: r.title.clone(),
                body_text: r.body_text.clone(),
                confidence_label: r.confidence_label.clone(),
                evidence_segment_ids: r.evidence_segment_ids.clone(),
            })
            .collect(),
        retrieval_intents: output
            .retrieval_intents
            .iter()
            .map(|i| RetrievalIntent {
                intent_id: new_id("intent"),
                question: i.question.clone(),
                query_text: i.query_text.clone(),
                intent_type: i.intent_type.clone(),
                evidence_segment_ids: i.evidence_segment_ids.clone(),
            })
            .collect(),
        status: "completed".to_string(),
        error_message: None,
    }
}

fn build_reconciliation_input(
    artifact_id: &str,
    source_type: &str,
    extraction_result: &ArtifactExtractionResult,
    retrieval_result_set: &RetrievalResultSet,
) -> Result<ReconciliationProcessorInput, serde_json::Error> {
    Ok(ReconciliationProcessorInput {
        artifact_id: artifact_id.to_string(),
        source_type: SourceType::from_str(source_type)
            .expect("validated source_type during payload parsing"),
        summary: SummaryOutput {
            title: extraction_result.summary_title.clone(),
            body_text: extraction_result.summary_body_text.clone(),
            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
        },
        memories: extraction_result
            .memories
            .iter()
            .map(|m| MemoryOutput {
                title: m.title.clone(),
                body_text: m.body_text.clone(),
                memory_type: m.memory_type.clone(),
                memory_scope: m.memory_scope,
                memory_scope_value: m.memory_scope_value.clone(),
                evidence_segment_ids: m.evidence_segment_ids.clone(),
            })
            .collect(),
        relationships: extraction_result
            .relationships
            .iter()
            .map(|r| RelationshipOutput {
                relationship_type: r.relationship_type.clone(),
                subject_key: r.subject_key.clone(),
                object_key: r.object_key.clone(),
                title: r.title.clone(),
                body_text: r.body_text.clone(),
                confidence_label: r.confidence_label.clone(),
                evidence_segment_ids: r.evidence_segment_ids.clone(),
            })
            .collect(),
        retrieval_results_json: serde_json::to_string_pretty(retrieval_result_set)?,
    })
}

fn build_reconciliation_decisions(
    claimed_job: &ClaimedJob,
    extraction_result: &ArtifactExtractionResult,
    retrieval_result_set: &RetrievalResultSet,
    outputs: Vec<ReconciliationDecisionOutput>,
) -> Vec<ReconciliationDecision> {
    outputs
        .into_iter()
        .map(|output| ReconciliationDecision {
            reconciliation_decision_id: new_id("reconcile"),
            artifact_id: extraction_result.artifact_id.clone(),
            job_id: claimed_job.job_id.clone(),
            extraction_result_id: extraction_result.extraction_result_id.clone(),
            retrieval_result_set_id: retrieval_result_set.retrieval_result_set_id.clone(),
            pipeline_name: "artifact_reconciliation".to_string(),
            pipeline_version: "v1".to_string(),
            decision_kind: output.decision_kind,
            target_kind: output.target_kind,
            target_key: output.target_key,
            matched_object_id: output.matched_object_id,
            rationale: output.rationale,
            evidence_segment_ids: output.evidence_segment_ids,
            status: "completed".to_string(),
            error_message: None,
        })
        .collect()
}

fn memory_target_key(memory: &ExtractedMemory) -> String {
    memory
        .title
        .clone()
        .unwrap_or_else(|| memory.body_text.chars().take(64).collect())
}

fn relationship_target_key(relationship: &CandidateRelationship) -> String {
    format!(
        "{}:{}:{}",
        relationship.relationship_type, relationship.subject_key, relationship.object_key
    )
}

fn build_evidence_links(derived_object_id: &str, segment_ids: &[String]) -> Vec<NewEvidenceLink> {
    segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| NewEvidenceLink {
            evidence_link_id: new_id("evidence"),
            derived_object_id: derived_object_id.to_string(),
            segment_id: segment_id.clone(),
            evidence_role: if index == 0 {
                EvidenceRole::PrimarySupport
            } else {
                EvidenceRole::SecondarySupport
            },
            evidence_rank: (index + 1) as i64,
            support_strength: SupportStrength::Strong,
        })
        .collect()
}

fn build_derivation_attempt(
    claimed_job: &ClaimedJob,
    artifact_id: &str,
    extraction_result: &ArtifactExtractionResult,
    decisions: &[ReconciliationDecision],
) -> WriteDerivationAttempt {
    let derivation_run_id = new_id("drvrun");
    let started_at = SourceTimestamp::from(chrono::Utc::now());
    let completed_at = started_at.clone();
    let mut objects = Vec::with_capacity(
        1 + extraction_result.classifications.len() + extraction_result.memories.len(),
    );

    // Summary object
    let summary_object_id = new_id("dobj");
    objects.push(WriteDerivedObject {
        object: NewDerivedObject {
            derived_object_id: summary_object_id.clone(),
            artifact_id: artifact_id.to_string(),
            derivation_run_id: derivation_run_id.clone(),
            origin_kind: OriginKind::Deterministic,
            object_status: ObjectStatus::Active,
            confidence_score: None,
            confidence_label: None,
            scope_type: ScopeType::Artifact,
            scope_id: artifact_id.to_string(),
            supersedes_derived_object_id: None,
            payload: DerivedObjectPayload::Summary {
                title: extraction_result.summary_title.clone(),
                body_text: extraction_result.summary_body_text.clone(),
                object_json: Some(SummaryObjectJson {
                    summary_kind: Some("artifact".to_string()),
                    summary_version: Some(extraction_result.pipeline_version.clone()),
                }),
            },
        },
        evidence_links: build_evidence_links(
            &summary_object_id,
            &extraction_result.summary_evidence_segment_ids,
        ),
    });

    // Classification objects
    for classification in &extraction_result.classifications {
        let derived_object_id = new_id("dobj");
        objects.push(WriteDerivedObject {
            object: NewDerivedObject {
                derived_object_id: derived_object_id.clone(),
                artifact_id: artifact_id.to_string(),
                derivation_run_id: derivation_run_id.clone(),
                origin_kind: OriginKind::Deterministic,
                object_status: ObjectStatus::Active,
                confidence_score: None,
                confidence_label: None,
                scope_type: ScopeType::Artifact,
                scope_id: artifact_id.to_string(),
                supersedes_derived_object_id: None,
                payload: DerivedObjectPayload::Classification {
                    title: classification.title.clone(),
                    body_text: classification.body_text.clone(),
                    object_json: ClassificationObjectJson {
                        classification_type: classification.classification_type.clone(),
                        classification_value: classification.classification_value.clone(),
                    },
                },
            },
            evidence_links: build_evidence_links(
                &derived_object_id,
                &classification.evidence_segment_ids,
            ),
        });
    }

    // Memory objects (with reconciliation decisions)
    let mut attached_existing = HashSet::new();
    for memory in &extraction_result.memories {
        let decision = decisions.iter().find(|d| {
            d.target_kind == "memory" && d.target_key == memory_target_key(memory)
        });
        if let Some(decision) = decision {
            if matches!(
                decision.decision_kind,
                ReconciliationDecisionKind::AttachToExisting
                    | ReconciliationDecisionKind::StrengthenExisting
            ) {
                if let Some(existing_id) = &decision.matched_object_id {
                    attached_existing.insert(existing_id.clone());
                }
                continue;
            }
        }

        let derived_object_id = new_id("dobj");
        let supersedes_derived_object_id = decision.and_then(|d| {
            matches!(
                d.decision_kind,
                ReconciliationDecisionKind::SupersedeExisting
            )
            .then(|| d.matched_object_id.clone())
            .flatten()
        });
        objects.push(WriteDerivedObject {
            object: NewDerivedObject {
                derived_object_id: derived_object_id.clone(),
                artifact_id: artifact_id.to_string(),
                derivation_run_id: derivation_run_id.clone(),
                origin_kind: OriginKind::Inferred,
                object_status: ObjectStatus::Active,
                confidence_score: None,
                confidence_label: None,
                scope_type: memory.memory_scope,
                scope_id: memory.memory_scope_value.clone(),
                supersedes_derived_object_id,
                payload: DerivedObjectPayload::Memory {
                    title: memory.title.clone(),
                    body_text: memory.body_text.clone(),
                    object_json: MemoryObjectJson {
                        memory_type: memory.memory_type.clone(),
                        memory_scope: memory.memory_scope,
                        memory_scope_value: memory.memory_scope_value.clone(),
                    },
                },
            },
            evidence_links: build_evidence_links(
                &derived_object_id,
                &memory.evidence_segment_ids,
            ),
        });
    }

    // Relationship objects (with reconciliation decisions)
    for relationship in &extraction_result.relationships {
        let decision = decisions.iter().find(|d| {
            d.target_kind == "relationship"
                && d.target_key == relationship_target_key(relationship)
        });
        let Some(decision) = decision else {
            continue;
        };
        if matches!(
            decision.decision_kind,
            ReconciliationDecisionKind::AttachToExisting
                | ReconciliationDecisionKind::StrengthenExisting
                | ReconciliationDecisionKind::InsufficientEvidence
        ) {
            if let Some(existing_id) = &decision.matched_object_id {
                attached_existing.insert(existing_id.clone());
            }
            continue;
        }

        let derived_object_id = new_id("dobj");
        let supersedes_derived_object_id = matches!(
            decision.decision_kind,
            ReconciliationDecisionKind::SupersedeExisting
        )
        .then(|| decision.matched_object_id.clone())
        .flatten();
        let contradicts_relationship_object_id = matches!(
            decision.decision_kind,
            ReconciliationDecisionKind::ContradictsExisting
        )
        .then(|| decision.matched_object_id.clone())
        .flatten();
        objects.push(WriteDerivedObject {
            object: NewDerivedObject {
                derived_object_id: derived_object_id.clone(),
                artifact_id: artifact_id.to_string(),
                derivation_run_id: derivation_run_id.clone(),
                origin_kind: OriginKind::Inferred,
                object_status: ObjectStatus::Active,
                confidence_score: None,
                confidence_label: Some(relationship.confidence_label.clone()),
                scope_type: ScopeType::Artifact,
                scope_id: artifact_id.to_string(),
                supersedes_derived_object_id,
                payload: DerivedObjectPayload::Relationship {
                    title: relationship.title.clone(),
                    body_text: relationship.body_text.clone(),
                    object_json: RelationshipObjectJson {
                        relationship_type: relationship.relationship_type.clone(),
                        subject_key: relationship.subject_key.clone(),
                        object_key: relationship.object_key.clone(),
                        support_label: relationship.confidence_label.clone(),
                        supersedes_relationship_object_id: None,
                        contradicts_relationship_object_id,
                    },
                },
            },
            evidence_links: build_evidence_links(
                &derived_object_id,
                &relationship.evidence_segment_ids,
            ),
        });
    }

    let input_scope_json = serde_json::json!({
        "artifact_id": artifact_id,
        "extraction_result_id": extraction_result.extraction_result_id,
        "reconciled_decision_count": decisions.len(),
        "attached_existing_count": attached_existing.len()
    })
    .to_string();

    WriteDerivationAttempt {
        run: NewDerivationRun {
            derivation_run_id,
            artifact_id: artifact_id.to_string(),
            job_id: Some(claimed_job.job_id.clone()),
            run_type: DerivationRunType::ArtifactReconciliation,
            pipeline_name: "artifact_reconciliation".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some("deterministic".to_string()),
            model_name: None,
            prompt_version: None,
            run_status: DerivationRunStatus::Completed,
            input_scope_type: InputScopeType::Artifact,
            input_scope_json,
            started_at,
            completed_at: Some(completed_at),
            error_message: None,
        },
        objects,
    }
}
