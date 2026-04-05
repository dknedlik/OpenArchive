// ---------------------------------------------------------------------------
// Stage Poller — non-blocking batch pipeline for enrichment stages
// ---------------------------------------------------------------------------
//
// Each batch-capable pipeline stage gets a
// dedicated poller thread. The poller submits small batches to a provider's
// batch API, tracks in-flight batch IDs, polls for completion on subsequent
// loop iterations, and processes completed results. No thread ever blocks
// waiting for batch completion.
//
use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

use log::{debug, error, info, warn};

use crate::config::ExtractionChunkingConfig;
use crate::enrichment_worker::{
    build_derivation_attempt as worker_build_derivation_attempt,
    build_embedding_job as worker_build_embedding_job, build_extract_chunk_inputs,
    build_extraction_result as worker_build_extraction_result,
    build_reconciliation_decisions as worker_build_reconciliation_decisions,
    build_reconciliation_input as worker_build_reconciliation_input,
    complete_job as worker_complete_job, merge_chunk_outputs as worker_merge_chunk_outputs,
    new_id as worker_new_id, try_enqueue_embedding_job as worker_try_enqueue_embedding_job,
};
use crate::processor::{
    ArtifactProcessorInput, BatchHandle, BatchPollResult, ExtractionBatchSubmitter, ProcessorError,
    ReconciliationBatchSubmitter, ReconciliationProcessorInput,
};
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArtifactExtractPayload, ArtifactExtractionResult, ArtifactReadStore, ArtifactReconcilePayload,
    ClaimedJob, DerivedMetadataWriteStore, EnrichmentJobLifecycleStore, EnrichmentStateStore,
    EnrichmentTier, JobType, LoadedArtifactForEnrichment, NewEnrichmentBatch, NewEnrichmentJob,
    PersistedEnrichmentBatch, ReconciliationDecision, ReconciliationDecisionKind, SourceType,
};

/// Sentinel error returned when a stage cannot submit any jobs.
///
/// Returned from `StageBehavior::submit` and `process_completed` when no
/// jobs could be submitted (all failed individually during preparation).
#[derive(Debug)]
pub enum StageError {
    NoJobsSubmitted,
    Backoff {
        retry_after_seconds: i64,
        reason: String,
    },
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageError::NoJobsSubmitted => f.write_str("stage error: no jobs could be submitted"),
            StageError::Backoff {
                retry_after_seconds,
                reason,
            } => write!(
                f,
                "stage error: provider backoff requested for {}s: {}",
                retry_after_seconds, reason
            ),
        }
    }
}

impl std::error::Error for StageError {}

const RETRYABLE_INFERENCE_BACKOFF_SECONDS: i64 = 60;
const EXTRACT_CHUNK_WAVE_LIMIT: usize = 8;

// ---------------------------------------------------------------------------
// InFlightContext — stage-specific state carried with each in-flight batch
// ---------------------------------------------------------------------------

/// Stage-specific context carried alongside an in-flight batch.
///
/// Each variant holds the prepared inputs and payloads needed to process
/// results when the batch completes.
enum InFlightContext {
    Extract {
        groups: Vec<ExtractPreparedJob>,
    },
    Reconcile {
        inputs: Vec<ReconciliationProcessorInput>,
        payloads: Vec<ArtifactReconcilePayload>,
        extraction_results: Vec<ArtifactExtractionResult>,
        loaded_artifacts: Vec<LoadedArtifactForEnrichment>,
    },
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct ExtractPreparedJob {
    parent_input: ArtifactProcessorInput,
    payload: ArtifactExtractPayload,
    pending_chunk_inputs: Vec<ArtifactProcessorInput>,
    #[serde(default)]
    deferred_chunk_inputs: Vec<ArtifactProcessorInput>,
    #[serde(default)]
    successful_chunk_outputs: Vec<crate::processor::ArtifactProcessorOutput>,
    #[serde(default)]
    retry_count: usize,
}

// ---------------------------------------------------------------------------
// InFlightBatch — tracks one in-flight provider batch
// ---------------------------------------------------------------------------

/// Tracks a single in-flight batch submitted to a provider's batch API.
pub struct InFlightBatch {
    /// Provider-returned handle for polling.
    pub handle: BatchHandle,
    /// Job-ownership token to use when completing/failing claimed jobs.
    pub owner_worker_id: String,
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
    /// individually failed and excluded from the batch. Returns `Err(StageError)`
    /// only if no jobs could be submitted (all failed individually).
    fn submit(
        &self,
        jobs: Vec<ClaimedJob>,
        read_store: &dyn ArtifactReadStore,
        state_store: &dyn EnrichmentStateStore,
        derived_store: &dyn DerivedMetadataWriteStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, StageError>;

    /// Poll the provider for batch completion.
    fn poll(&self, batch: &InFlightBatch) -> Result<BatchPollResult, ProcessorError>;

    /// Process a completed batch.
    ///
    /// Returns `Ok(Some(new_batch))` when a phase transition occurs.
    /// Returns `Ok(None)` when the batch is fully processed.
    fn process_completed(
        &self,
        batch: InFlightBatch,
        data: Box<dyn std::any::Any>,
        job_store: &dyn EnrichmentJobLifecycleStore,
        state_store: &dyn EnrichmentStateStore,
        derived_store: &dyn DerivedMetadataWriteStore,
        worker_id: &str,
    ) -> Result<Option<InFlightBatch>, StageError>;

    /// Durable phase name used for persisted batch recovery.
    fn phase_name(&self, batch: &InFlightBatch) -> &'static str;

    /// Serialize any stage-specific recovery context needed for restart recovery.
    fn serialize_context(&self, batch: &InFlightBatch) -> Result<Option<String>, ProcessorError>;

    /// Rebuild an in-flight batch from durable storage.
    fn recover_batch(
        &self,
        persisted: PersistedEnrichmentBatch,
        read_store: &dyn ArtifactReadStore,
        state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, StageError>;
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
pub struct StagePollerContext<'a> {
    pub job_store: &'a dyn EnrichmentJobLifecycleStore,
    pub read_store: &'a dyn ArtifactReadStore,
    pub state_store: &'a dyn EnrichmentStateStore,
    pub derived_store: &'a dyn DerivedMetadataWriteStore,
}

pub fn stage_poller_loop(
    stage: &dyn StageBehavior,
    worker_id: String,
    ctx: StagePollerContext<'_>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    let StagePollerContext {
        job_store,
        read_store,
        state_store,
        derived_store,
    } = ctx;
    info!(
        "[{}] stage poller {} starting (batch_size={}, max_concurrent={}, poll_interval_ms={})",
        stage.stage_name(),
        worker_id,
        stage.batch_size(),
        stage.max_concurrent(),
        poll_interval.as_millis()
    );

    let mut in_flight: HashMap<String, InFlightBatch> = HashMap::new();
    let mut stage_backoff_until: Option<Instant> = None;
    let mut consecutive_rate_limits: u32 = 0;
    if let Err(err) = job_store.reconcile_stale_running_jobs(stage.stage_name()) {
        error!(
            "[{}] failed to reconcile stale running jobs at startup: {}",
            stage.stage_name(),
            err
        );
    }
    if let Err(err) = job_store.reconcile_stale_running_batches(stage.stage_name()) {
        error!(
            "[{}] failed to reconcile stale running batches at startup: {}",
            stage.stage_name(),
            err
        );
    }
    match job_store.load_running_batches(stage.stage_name()) {
        Ok(persisted_batches) => {
            for persisted in persisted_batches {
                let batch_id = persisted.provider_batch_id.clone();
                match stage.recover_batch(persisted, read_store, state_store, job_store, &worker_id)
                {
                    Ok(batch) => {
                        info!(
                            "[{}] recovered in-flight batch {} after startup",
                            stage.stage_name(),
                            batch_id
                        );
                        in_flight.insert(batch_id, batch);
                    }
                    Err(_) => {
                        warn!(
                            "[{}] failed to recover persisted batch {}",
                            stage.stage_name(),
                            batch_id
                        );
                    }
                }
            }
        }
        Err(err) => {
            error!(
                "[{}] failed to load persisted in-flight batches: {}",
                stage.stage_name(),
                err
            );
        }
    }

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

        if let Err(err) = job_store.reconcile_stale_running_jobs(stage.stage_name()) {
            error!(
                "[{}] failed to reconcile stale running jobs: {}",
                stage.stage_name(),
                err
            );
        }
        if let Err(err) = job_store.reconcile_stale_running_batches(stage.stage_name()) {
            error!(
                "[{}] failed to reconcile stale running batches: {}",
                stage.stage_name(),
                err
            );
        }

        let stage_backoff_active = stage_backoff_until
            .map(|until| until > Instant::now())
            .unwrap_or(false);
        if stage_backoff_active {
            debug!(
                "[{}] stage backoff active; skipping provider work this cycle",
                stage.stage_name()
            );
        }

        // -----------------------------------------------------------------
        // Step 1: Poll all in-flight batches
        // -----------------------------------------------------------------
        let mut completed: Vec<(String, Box<dyn std::any::Any>)> = Vec::new();
        let mut failed: Vec<(String, String)> = Vec::new();

        if !stage_backoff_active {
            for (batch_id, batch) in in_flight.iter() {
                debug!(
                    "[{}] polling batch {} (jobs={}, submitted_for_ms={})",
                    stage.stage_name(),
                    batch_id,
                    batch.jobs.len(),
                    batch.handle.submitted_at.elapsed().as_millis()
                );

                match stage.poll(batch) {
                    Ok(BatchPollResult::Succeeded(data)) => {
                        clear_stage_backoff(&mut stage_backoff_until, &mut consecutive_rate_limits);
                        info!(
                            "[{}] batch {} completed successfully",
                            stage.stage_name(),
                            batch_id
                        );
                        completed.push((batch_id.clone(), data));
                    }
                    Ok(BatchPollResult::Failed(msg)) => {
                        clear_stage_backoff(&mut stage_backoff_until, &mut consecutive_rate_limits);
                        error!(
                            "[{}] batch {} failed: {}",
                            stage.stage_name(),
                            batch_id,
                            msg
                        );
                        failed.push((batch_id.clone(), msg));
                    }
                    Ok(BatchPollResult::Pending) => {
                        clear_stage_backoff(&mut stage_backoff_until, &mut consecutive_rate_limits);
                        debug!("[{}] batch {} still pending", stage.stage_name(), batch_id);
                    }
                    Err(err) => {
                        error!(
                            "[{}] error polling batch {}: {}",
                            stage.stage_name(),
                            batch_id,
                            err
                        );
                        if let Some(retry_after_seconds) =
                            err.recommended_stage_backoff_seconds(consecutive_rate_limits)
                        {
                            apply_stage_backoff(
                                stage.stage_name(),
                                &mut stage_backoff_until,
                                &mut consecutive_rate_limits,
                                retry_after_seconds,
                                &format!("polling batch {}: {}", batch_id, err),
                            );
                            break;
                        }
                        // If the poll error itself is retryable, leave the batch
                        // in-flight for a subsequent poll attempt. Otherwise,
                        // treat as a terminal failure.
                        if !err.is_retryable() {
                            failed.push((batch_id.clone(), format!("{err}")));
                        }
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
            let owner_worker_id = batch.owner_worker_id.clone();
            match stage.process_completed(
                batch,
                data,
                job_store,
                state_store,
                derived_store,
                &owner_worker_id,
            ) {
                Ok(Some(new_batch)) => {
                    // Phase transition to the next provider-managed batch step.
                    let new_id = new_batch.handle.batch_id.clone();
                    let next_context = match stage.serialize_context(&new_batch) {
                        Ok(context) => context,
                        Err(err) => {
                            error!(
                                "[{}] failed to serialize phase transition context for {}: {}",
                                stage.stage_name(),
                                new_id,
                                err
                            );
                            for job in &new_batch.jobs {
                                poller_fail_job_message(
                                    job_store,
                                    &worker_id,
                                    &job.job_id,
                                    format!("Failed to persist batch transition context: {err}"),
                                );
                            }
                            continue;
                        }
                    };
                    if let Err(err) = job_store.transition_batch_submission(
                        &batch_id,
                        &NewEnrichmentBatch {
                            provider_batch_id: new_id.clone(),
                            provider_name: new_batch.handle.provider.clone(),
                            stage_name: stage.stage_name().to_string(),
                            phase_name: stage.phase_name(&new_batch).to_string(),
                            owner_worker_id: owner_worker_id.clone(),
                            context_json: next_context,
                        },
                        &new_batch.jobs,
                    ) {
                        error!(
                            "[{}] failed to persist phase transition {} -> {}: {}",
                            stage.stage_name(),
                            batch_id,
                            new_id,
                            err
                        );
                    }
                    info!(
                        "[{}] phase transition: {} -> {}",
                        stage.stage_name(),
                        batch_id,
                        new_id
                    );
                    in_flight.insert(new_id, new_batch);
                }
                Ok(None) => {
                    if let Err(err) = job_store.complete_batch(&batch_id) {
                        error!(
                            "[{}] failed to complete batch record {}: {}",
                            stage.stage_name(),
                            batch_id,
                            err
                        );
                    }
                    info!(
                        "[{}] batch {} fully processed",
                        stage.stage_name(),
                        batch_id
                    );
                }
                Err(StageError::Backoff {
                    retry_after_seconds,
                    reason,
                }) => {
                    if let Err(err) = job_store.complete_batch(&batch_id) {
                        error!(
                            "[{}] failed to close completed batch record {} after per-job errors: {}",
                            stage.stage_name(),
                            batch_id,
                            err
                        );
                    }
                    apply_stage_backoff(
                        stage.stage_name(),
                        &mut stage_backoff_until,
                        &mut consecutive_rate_limits,
                        retry_after_seconds,
                        &reason,
                    );
                    // Errors already handled inside process_completed per-job.
                    warn!(
                        "[{}] batch {} processing encountered provider backoff (handled per-job)",
                        stage.stage_name(),
                        batch_id
                    );
                }
                Err(StageError::NoJobsSubmitted) => {
                    if let Err(err) = job_store.complete_batch(&batch_id) {
                        error!(
                            "[{}] failed to close completed batch record {} after per-job errors: {}",
                            stage.stage_name(),
                            batch_id,
                            err
                        );
                    }
                    // Errors already handled inside process_completed per-job.
                    warn!(
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
                if let Err(err) = job_store.fail_batch_record(&batch_id, &error_message) {
                    error!(
                        "[{}] failed to mark batch record {} failed: {}",
                        stage.stage_name(),
                        batch_id,
                        err
                    );
                }
                for job in batch.jobs {
                    let msg = format!("Batch {} failed: {}", batch_id, error_message);
                    poller_fail_job_message(job_store, &batch.owner_worker_id, &job.job_id, msg);
                }
            }
        }

        // -----------------------------------------------------------------
        // Step 3: Claim new jobs until this stage reaches in-flight capacity.
        // -----------------------------------------------------------------
        while stage_backoff_until
            .map(|until| until <= Instant::now())
            .unwrap_or(true)
            && in_flight.len() < stage.max_concurrent()
        {
            match job_store.claim_jobs_by_type(
                &worker_id,
                stage.job_type(),
                Some(stage.enrichment_tier()),
                stage.batch_size(),
            ) {
                Ok(jobs) if !jobs.is_empty() => {
                    info!(
                        "[{}] claimed {} jobs for new batch submission",
                        stage.stage_name(),
                        jobs.len()
                    );
                    match stage.submit(
                        jobs,
                        read_store,
                        state_store,
                        derived_store,
                        job_store,
                        &worker_id,
                    ) {
                        Ok(batch) => {
                            clear_stage_backoff(
                                &mut stage_backoff_until,
                                &mut consecutive_rate_limits,
                            );
                            let id = batch.handle.batch_id.clone();
                            let context_json = match stage.serialize_context(&batch) {
                                Ok(context) => context,
                                Err(err) => {
                                    error!(
                                        "[{}] failed to serialize batch context for {}: {}",
                                        stage.stage_name(),
                                        id,
                                        err
                                    );
                                    for job in &batch.jobs {
                                        poller_fail_job_message(
                                            job_store,
                                            &worker_id,
                                            &job.job_id,
                                            format!("Failed to persist batch context: {err}"),
                                        );
                                    }
                                    continue;
                                }
                            };
                            if let Err(err) = job_store.record_batch_submission(
                                &NewEnrichmentBatch {
                                    provider_batch_id: id.clone(),
                                    provider_name: batch.handle.provider.clone(),
                                    stage_name: stage.stage_name().to_string(),
                                    phase_name: stage.phase_name(&batch).to_string(),
                                    owner_worker_id: worker_id.clone(),
                                    context_json,
                                },
                                &batch.jobs,
                            ) {
                                error!(
                                    "[{}] failed to persist submitted batch {}: {}",
                                    stage.stage_name(),
                                    id,
                                    err
                                );
                            }
                            info!(
                                "[{}] submitted batch {} (jobs={}, in_flight_before={})",
                                stage.stage_name(),
                                id,
                                batch.jobs.len(),
                                in_flight.len()
                            );
                            in_flight.insert(id, batch);
                        }
                        Err(StageError::Backoff {
                            retry_after_seconds,
                            reason,
                        }) => {
                            apply_stage_backoff(
                                stage.stage_name(),
                                &mut stage_backoff_until,
                                &mut consecutive_rate_limits,
                                retry_after_seconds,
                                &reason,
                            );
                            break;
                        }
                        Err(StageError::NoJobsSubmitted) => {
                            // All individual jobs were failed in submit().
                            warn!(
                                "[{}] all claimed jobs failed during submit preparation",
                                stage.stage_name()
                            );
                        }
                    }
                }
                Ok(_) => {
                    // No jobs available, nothing more to submit this cycle.
                    break;
                }
                Err(err) => {
                    error!("[{}] failed to claim jobs: {}", stage.stage_name(), err);
                    break;
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
// ExtractStage
// ---------------------------------------------------------------------------

/// Stage behavior for the extraction pipeline stage.
pub struct ExtractStage {
    submitter: Box<dyn ExtractionBatchSubmitter>,
    tier: EnrichmentTier,
    batch_size: usize,
    max_concurrent: usize,
    chunking: ExtractionChunkingConfig,
}

impl ExtractStage {
    pub fn new(
        submitter: Box<dyn ExtractionBatchSubmitter>,
        tier: EnrichmentTier,
        batch_size: usize,
        max_concurrent: usize,
        chunking: ExtractionChunkingConfig,
    ) -> Self {
        Self {
            submitter,
            tier,
            batch_size,
            max_concurrent,
            chunking,
        }
    }

    fn prepare_job(
        &self,
        job: &ClaimedJob,
        read_store: &dyn ArtifactReadStore,
    ) -> Result<(ArtifactProcessorInput, ArtifactExtractPayload), String> {
        let payload = ArtifactExtractPayload::from_json(&job.payload_json)
            .map_err(|err| format!("Failed to parse extract payload JSON: {err}"))?;
        let source_type = SourceType::parse(&payload.source_type).ok_or_else(|| {
            format!(
                "Invalid source_type in extract payload: {}",
                payload.source_type
            )
        })?;
        let loaded = read_store
            .load_artifact_for_enrichment(&job.artifact_id)
            .map_err(|err| format!("Failed to load artifact for extraction: {err}"))?
            .ok_or_else(|| format!("Artifact {} not found for extraction", job.artifact_id))?;
        Ok((
            ArtifactProcessorInput {
                artifact_id: loaded.artifact.artifact_id.clone(),
                import_id: payload.import_id.clone(),
                artifact_class: loaded.artifact.artifact_class,
                source_type,
                title: loaded.artifact.title.clone(),
                imported_note_metadata: payload.imported_note_metadata.clone(),
                participants: loaded.participants,
                segments: loaded.segments,
            },
            payload,
        ))
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
        read_store: &dyn ArtifactReadStore,
        _state_store: &dyn EnrichmentStateStore,
        _derived_store: &dyn DerivedMetadataWriteStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, StageError> {
        let mut valid_jobs = Vec::new();
        let mut groups = Vec::new();
        let mut submitted_chunk_count = 0usize;
        let provider_limit = self.submitter.max_batch_size();

        for job in jobs {
            let (parent_input, payload) = match self.prepare_job(&job, read_store) {
                Ok(prepared) => prepared,
                Err(message) => {
                    poller_fail_job_message(job_store, worker_id, &job.job_id, message);
                    continue;
                }
            };
            let chunk_inputs = build_extract_chunk_inputs(&parent_input, &payload, &self.chunking);
            if chunk_inputs.is_empty() {
                poller_fail_job_message(
                    job_store,
                    worker_id,
                    &job.job_id,
                    format!("Artifact {} produced no extraction chunks", job.artifact_id),
                );
                continue;
            }

            if submitted_chunk_count + chunk_inputs.len() > provider_limit {
                if submitted_chunk_count == 0 {
                    poller_fail_job_message(
                        job_store,
                        worker_id,
                        &job.job_id,
                        format!(
                            "Artifact {} expands to {} extract chunks, exceeding provider batch limit {}",
                            job.artifact_id,
                            chunk_inputs.len(),
                            provider_limit
                        ),
                    );
                } else {
                    poller_mark_retryable(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Deferred extract job because chunk-expanded batch reached provider limit",
                        1,
                    );
                }
                continue;
            }

            submitted_chunk_count += chunk_inputs.len();
            let split_at = chunk_inputs.len().min(EXTRACT_CHUNK_WAVE_LIMIT);
            let pending_chunk_inputs = chunk_inputs[..split_at].to_vec();
            let deferred_chunk_inputs = chunk_inputs[split_at..].to_vec();
            groups.push(ExtractPreparedJob {
                parent_input,
                payload,
                pending_chunk_inputs,
                deferred_chunk_inputs,
                successful_chunk_outputs: Vec::new(),
                retry_count: 0,
            });
            valid_jobs.push(job);
        }

        if valid_jobs.is_empty() {
            return Err(StageError::NoJobsSubmitted);
        }

        let flat_inputs: Vec<_> = groups
            .iter()
            .flat_map(|group| group.pending_chunk_inputs.iter().cloned())
            .collect();

        match self.submitter.prepare_and_submit(&flat_inputs) {
            Ok(handle) => Ok(InFlightBatch {
                handle,
                owner_worker_id: worker_id.to_string(),
                jobs: valid_jobs,
                context: InFlightContext::Extract { groups },
            }),
            Err(err) => {
                if matches!(err, ProcessorError::InferenceHttpStatus { status: 413, .. }) {
                    for job in &valid_jobs {
                        poller_reschedule_without_attempt(
                            job_store,
                            worker_id,
                            &job.job_id,
                            "Provider rejected extract batch body as too large; will retry in smaller waves",
                            RETRYABLE_INFERENCE_BACKOFF_SECONDS,
                        );
                    }
                } else {
                    for job in &valid_jobs {
                        poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                    }
                }
                if err.should_reschedule_without_attempt() {
                    Err(StageError::Backoff {
                        retry_after_seconds: err.recommended_retry_after_seconds(),
                        reason: format!("extract submit throttled: {err}"),
                    })
                } else {
                    Err(StageError::NoJobsSubmitted)
                }
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
    ) -> Result<Option<InFlightBatch>, StageError> {
        let groups = match batch.context {
            InFlightContext::Extract { groups } => groups,
            _ => {
                error!("[extract] process_completed called with wrong context variant");
                return Err(StageError::NoJobsSubmitted);
            }
        };

        let flat_inputs: Vec<_> = groups
            .iter()
            .flat_map(|group| group.pending_chunk_inputs.iter().cloned())
            .collect();
        let mut results = self.submitter.parse_results(data, &flat_inputs).into_iter();
        let mut next_jobs = Vec::new();
        let mut next_groups = Vec::new();

        for (job, mut group) in batch.jobs.into_iter().zip(groups.into_iter()) {
            let pending_chunk_inputs = std::mem::take(&mut group.pending_chunk_inputs);
            let mut retryable_chunk_inputs = Vec::new();
            let mut terminal_chunk_error = None;
            let mut first_retryable_error = None;
            for chunk_input in pending_chunk_inputs {
                let Some(result) = results.next() else {
                    retryable_chunk_inputs.push(chunk_input);
                    first_retryable_error.get_or_insert_with(|| ProcessorError::Message {
                        message: format!(
                            "Missing chunk result for artifact {} in extract batch",
                            job.artifact_id
                        ),
                    });
                    break;
                };
                match result {
                    Ok(output) => group.successful_chunk_outputs.push(output),
                    Err(err) => {
                        if err.is_retryable() || is_repairable_processor_error(&err) {
                            if first_retryable_error.is_none() {
                                first_retryable_error = Some(ProcessorError::Message {
                                    message: err.to_string(),
                                });
                            }
                            retryable_chunk_inputs.push(chunk_input);
                        } else {
                            terminal_chunk_error = Some(err);
                            break;
                        }
                    }
                }
            }

            if let Some(err) = terminal_chunk_error {
                poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                continue;
            }

            if !retryable_chunk_inputs.is_empty() {
                if group.retry_count + 1 >= job.max_attempts as usize {
                    let err = first_retryable_error.unwrap_or_else(|| ProcessorError::Message {
                        message: format!(
                            "extract chunk retries exhausted for artifact {}",
                            job.artifact_id
                        ),
                    });
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                    continue;
                }

                info!(
                    "[extract] artifact {} retrying {} failed chunk items while preserving {} successful chunk outputs",
                    job.artifact_id,
                    retryable_chunk_inputs.len(),
                    group.successful_chunk_outputs.len()
                );
                group.pending_chunk_inputs = retryable_chunk_inputs;
                group.retry_count += 1;
                next_jobs.push(job);
                next_groups.push(group);
                continue;
            }

            if !group.deferred_chunk_inputs.is_empty() {
                let split_at = group
                    .deferred_chunk_inputs
                    .len()
                    .min(EXTRACT_CHUNK_WAVE_LIMIT);
                let remaining = std::mem::take(&mut group.deferred_chunk_inputs);
                group.pending_chunk_inputs = remaining[..split_at].to_vec();
                group.deferred_chunk_inputs = remaining[split_at..].to_vec();
                next_jobs.push(job);
                next_groups.push(group);
                continue;
            }

            let output = if group.successful_chunk_outputs.len() == 1 {
                group
                    .successful_chunk_outputs
                    .into_iter()
                    .next()
                    .expect("single chunk output should exist")
            } else {
                worker_merge_chunk_outputs(&group.parent_input, &group.successful_chunk_outputs)
            };

            let extraction_result = worker_build_extraction_result(
                &job,
                &group.parent_input,
                &output,
                group.payload.conversation_windows,
            );
            if let Err(err) = state_store.save_extraction_result(&extraction_result) {
                poller_fail_job(
                    job_store,
                    worker_id,
                    &job.job_id,
                    "Failed to persist extraction result",
                    err,
                );
                continue;
            }
            let reconcile_job = NewEnrichmentJob {
                job_id: worker_new_id("job"),
                artifact_id: job.artifact_id.clone(),
                job_type: JobType::ArtifactReconcile,
                enrichment_tier: job.enrichment_tier,
                spawned_by_job_id: Some(job.job_id.clone()),
                job_status: crate::storage::JobStatus::Pending,
                max_attempts: 3,
                priority_no: 100,
                required_capabilities: vec!["text".to_string()],
                payload_json: ArtifactReconcilePayload::new_v1(
                    &job.artifact_id,
                    &group.payload.import_id,
                    group.parent_input.source_type,
                    &extraction_result.extraction_result_id,
                )
                .to_json(),
            };
            if let Err(err) = job_store.enqueue_jobs(&[reconcile_job]) {
                poller_fail_job(
                    job_store,
                    worker_id,
                    &job.job_id,
                    "Failed to enqueue reconciliation job",
                    err,
                );
                continue;
            }
            if let Err(msg) = worker_complete_job(job_store, worker_id, &job.job_id) {
                error!("[extract] {}", msg);
            }
        }
        if next_jobs.is_empty() {
            Ok(None)
        } else {
            let retry_inputs: Vec<_> = next_groups
                .iter()
                .flat_map(|group| group.pending_chunk_inputs.iter().cloned())
                .collect();
            match self.submitter.prepare_and_submit(&retry_inputs) {
                Ok(handle) => Ok(Some(InFlightBatch {
                    handle,
                    owner_worker_id: worker_id.to_string(),
                    jobs: next_jobs,
                    context: InFlightContext::Extract {
                        groups: next_groups,
                    },
                })),
                Err(err) => {
                    if matches!(err, ProcessorError::InferenceHttpStatus { status: 413, .. }) {
                        for job in next_jobs {
                            poller_reschedule_without_attempt(
                                job_store,
                                worker_id,
                                &job.job_id,
                                "Provider rejected extract retry batch body as too large; will retry in smaller waves",
                                RETRYABLE_INFERENCE_BACKOFF_SECONDS,
                            );
                        }
                    } else {
                        for job in next_jobs {
                            poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                        }
                    }
                    if err.should_reschedule_without_attempt() {
                        Err(StageError::Backoff {
                            retry_after_seconds: err.recommended_retry_after_seconds(),
                            reason: format!("extract retry submit throttled: {err}"),
                        })
                    } else {
                        Err(StageError::NoJobsSubmitted)
                    }
                }
            }
        }
    }

    fn phase_name(&self, _batch: &InFlightBatch) -> &'static str {
        "extract"
    }

    fn serialize_context(&self, batch: &InFlightBatch) -> Result<Option<String>, ProcessorError> {
        match &batch.context {
            InFlightContext::Extract { groups } => serde_json::to_string(groups)
                .map(Some)
                .map_err(|source| ProcessorError::SerializePrompt { source }),
            _ => Ok(None),
        }
    }

    fn recover_batch(
        &self,
        persisted: PersistedEnrichmentBatch,
        read_store: &dyn ArtifactReadStore,
        _state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, StageError> {
        let groups = if let Some(serialized) = persisted.context_json.as_deref() {
            match serde_json::from_str::<Vec<ExtractPreparedJob>>(serialized) {
                Ok(groups) => groups,
                Err(err) => {
                    error!(
                        "[extract] failed to deserialize extract recovery context for {}: {}",
                        persisted.provider_batch_id, err
                    );
                    return Err(StageError::NoJobsSubmitted);
                }
            }
        } else {
            let mut groups = Vec::new();
            for job in &persisted.jobs {
                let (parent_input, payload) = match self.prepare_job(job, read_store) {
                    Ok(prepared) => prepared,
                    Err(message) => {
                        poller_fail_job_message(job_store, worker_id, &job.job_id, message);
                        return Err(StageError::NoJobsSubmitted);
                    }
                };
                let chunk_inputs =
                    build_extract_chunk_inputs(&parent_input, &payload, &self.chunking);
                if chunk_inputs.is_empty() {
                    poller_fail_job_message(
                        job_store,
                        worker_id,
                        &job.job_id,
                        format!(
                            "Artifact {} produced no extraction chunks during recovery",
                            job.artifact_id
                        ),
                    );
                    return Err(StageError::NoJobsSubmitted);
                }
                groups.push(ExtractPreparedJob {
                    parent_input,
                    payload,
                    pending_chunk_inputs: chunk_inputs,
                    deferred_chunk_inputs: Vec::new(),
                    successful_chunk_outputs: Vec::new(),
                    retry_count: 0,
                });
            }
            groups
        };

        Ok(InFlightBatch {
            handle: BatchHandle {
                batch_id: persisted.provider_batch_id,
                provider: persisted.provider_name,
                submitted_at: std::time::Instant::now(),
            },
            owner_worker_id: persisted.owner_worker_id,
            jobs: persisted.jobs,
            context: InFlightContext::Extract { groups },
        })
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
    enqueue_embedding_jobs: bool,
}

impl ReconcileStage {
    pub fn new(
        submitter: Box<dyn ReconciliationBatchSubmitter>,
        tier: EnrichmentTier,
        batch_size: usize,
        max_concurrent: usize,
        enqueue_embedding_jobs: bool,
    ) -> Self {
        Self {
            submitter,
            tier,
            batch_size,
            max_concurrent,
            enqueue_embedding_jobs,
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
        read_store: &dyn ArtifactReadStore,
        state_store: &dyn EnrichmentStateStore,
        derived_store: &dyn DerivedMetadataWriteStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, StageError> {
        let mut valid_jobs = Vec::new();
        let mut inputs = Vec::new();
        let mut payloads = Vec::new();
        let mut extraction_results = Vec::new();
        let mut loaded_artifacts = Vec::new();

        for job in jobs {
            let payload = match ArtifactReconcilePayload::from_json(&job.payload_json) {
                Ok(p) => p,
                Err(err) => {
                    poller_fail_job(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Failed to parse reconcile payload JSON",
                        err,
                    );
                    continue;
                }
            };
            let loaded = match read_store.load_artifact_for_enrichment(&job.artifact_id) {
                Ok(Some(l)) => l,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store,
                        worker_id,
                        &job.job_id,
                        format!("Artifact {} not found for reconciliation", job.artifact_id),
                    );
                    continue;
                }
                Err(err) => {
                    poller_fail_job(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Failed to load artifact for reconciliation",
                        err,
                    );
                    continue;
                }
            };
            let extraction_result =
                match state_store.load_extraction_result(&payload.extraction_result_id) {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        poller_fail_job_message(
                            job_store,
                            worker_id,
                            &job.job_id,
                            format!(
                                "Extraction result {} not found",
                                payload.extraction_result_id
                            ),
                        );
                        continue;
                    }
                    Err(err) => {
                        poller_fail_job(
                            job_store,
                            worker_id,
                            &job.job_id,
                            "Failed to load extraction result for reconciliation",
                            err,
                        );
                        continue;
                    }
                };
            if extraction_result.memories.is_empty()
                && extraction_result.entities.is_empty()
                && extraction_result.relationships.is_empty()
            {
                if complete_reconcile_without_candidates(
                    &job,
                    &loaded,
                    &extraction_result,
                    ReconcileCompletionContext {
                        job_store,
                        state_store,
                        derived_store,
                        enqueue_embedding_jobs: self.enqueue_embedding_jobs,
                        worker_id,
                    },
                )
                .is_err()
                {
                    continue;
                }
                continue;
            }
            let input = match worker_build_reconciliation_input(
                &job.artifact_id,
                &payload.source_type,
                &extraction_result,
            ) {
                Ok(i) => i,
                Err(err) => {
                    poller_fail_job(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Failed to build reconciliation input",
                        err,
                    );
                    continue;
                }
            };
            inputs.push(input);
            payloads.push(payload);
            extraction_results.push(extraction_result);
            loaded_artifacts.push(loaded);
            valid_jobs.push(job);
        }

        if valid_jobs.is_empty() {
            return Err(StageError::NoJobsSubmitted);
        }

        match self.submitter.prepare_and_submit(&inputs) {
            Ok(handle) => Ok(InFlightBatch {
                handle,
                owner_worker_id: worker_id.to_string(),
                jobs: valid_jobs,
                context: InFlightContext::Reconcile {
                    inputs,
                    payloads,
                    extraction_results,
                    loaded_artifacts,
                },
            }),
            Err(err) => {
                for job in &valid_jobs {
                    poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                }
                if err.should_reschedule_without_attempt() {
                    Err(StageError::Backoff {
                        retry_after_seconds: err.recommended_retry_after_seconds(),
                        reason: format!("reconcile submit throttled: {err}"),
                    })
                } else {
                    Err(StageError::NoJobsSubmitted)
                }
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
    ) -> Result<Option<InFlightBatch>, StageError> {
        let (inputs, _payloads, extraction_results, loaded_artifacts) = match batch.context {
            InFlightContext::Reconcile {
                inputs,
                payloads,
                extraction_results,
                loaded_artifacts,
            } => (inputs, payloads, extraction_results, loaded_artifacts),
            _ => {
                error!("[reconcile] process_completed called with wrong context variant");
                return Err(StageError::NoJobsSubmitted);
            }
        };

        let results = self.submitter.parse_results(data, &inputs);

        // Zip everything together: (job, extraction_result, loaded_artifact, result)
        let iter = batch
            .jobs
            .into_iter()
            .zip(extraction_results)
            .zip(loaded_artifacts)
            .zip(results);

        for (((job, extraction_result), loaded), result) in iter {
            match result {
                Ok(outputs) => {
                    let decisions = if extraction_result.memories.is_empty()
                        && extraction_result.relationships.is_empty()
                    {
                        vec![ReconciliationDecision {
                            reconciliation_decision_id: worker_new_id("reconcile"),
                            artifact_id: extraction_result.artifact_id.clone(),
                            job_id: job.job_id.clone(),
                            extraction_result_id: extraction_result.extraction_result_id.clone(),
                            pipeline_name: "artifact_reconciliation".to_string(),
                            pipeline_version: "v1".to_string(),
                            decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
                            target_kind: "artifact".to_string(),
                            target_key: extraction_result.artifact_id.clone(),
                            matched_object_id: None,
                            rationale:
                                "No candidate memories, entities, or relationships were extracted for reconciliation."
                                    .to_string(),
                            evidence_segment_ids: extraction_result
                                .summary_evidence_segment_ids
                                .clone(),
                            status: "completed".to_string(),
                            error_message: None,
                        }]
                    } else {
                        worker_build_reconciliation_decisions(&job, &extraction_result, outputs)
                    };
                    if let Err(err) = state_store.save_reconciliation_decisions(&decisions) {
                        poller_fail_job(
                            job_store,
                            worker_id,
                            &job.job_id,
                            "Failed to persist reconciliation decisions",
                            err,
                        );
                        continue;
                    }
                    let attempt = worker_build_derivation_attempt(
                        &job,
                        &loaded.artifact.artifact_id,
                        &extraction_result,
                        &decisions,
                    );
                    let embedding_job = self
                        .enqueue_embedding_jobs
                        .then(|| {
                            worker_build_embedding_job(
                                &job,
                                &loaded.artifact.artifact_id,
                                &attempt.objects,
                            )
                        })
                        .flatten();
                    if let Err(err) = derived_store.write_derivation_attempt(attempt) {
                        poller_fail_job_message(
                            job_store,
                            worker_id,
                            &job.job_id,
                            format!(
                                "Failed to persist derivation output: {}; debug={err:?}",
                                err
                            ),
                        );
                        continue;
                    }
                    if let Some(embedding_job) = embedding_job.as_ref() {
                        worker_try_enqueue_embedding_job(job_store, &job.job_id, embedding_job);
                    }
                    if let Err(msg) = worker_complete_job(job_store, worker_id, &job.job_id) {
                        error!("[reconcile] {}", msg);
                    }
                }
                Err(err) => {
                    if err.is_retryable() || is_repairable_processor_error(&err) {
                        poller_mark_retryable(
                            job_store,
                            worker_id,
                            &job.job_id,
                            &format!("Processor execution failed: {err}"),
                            err.recommended_retry_after_seconds(),
                        );
                    } else {
                        poller_handle_processor_error(job_store, worker_id, &job.job_id, &err);
                    }
                }
            }
        }
        Ok(None)
    }

    fn phase_name(&self, _batch: &InFlightBatch) -> &'static str {
        "reconcile"
    }

    fn serialize_context(&self, _batch: &InFlightBatch) -> Result<Option<String>, ProcessorError> {
        Ok(None)
    }

    fn recover_batch(
        &self,
        persisted: PersistedEnrichmentBatch,
        read_store: &dyn ArtifactReadStore,
        state_store: &dyn EnrichmentStateStore,
        job_store: &dyn EnrichmentJobLifecycleStore,
        worker_id: &str,
    ) -> Result<InFlightBatch, StageError> {
        let mut inputs = Vec::new();
        let mut payloads = Vec::new();
        let mut extraction_results = Vec::new();
        let mut loaded_artifacts = Vec::new();

        for job in &persisted.jobs {
            let payload = match ArtifactReconcilePayload::from_json(&job.payload_json) {
                Ok(p) => p,
                Err(err) => {
                    poller_fail_job(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Failed to parse reconcile payload JSON during recovery",
                        err,
                    );
                    return Err(StageError::NoJobsSubmitted);
                }
            };
            let loaded = match read_store.load_artifact_for_enrichment(&job.artifact_id) {
                Ok(Some(l)) => l,
                Ok(None) => {
                    poller_fail_job_message(
                        job_store,
                        worker_id,
                        &job.job_id,
                        format!(
                            "Artifact {} not found for reconciliation recovery",
                            job.artifact_id
                        ),
                    );
                    return Err(StageError::NoJobsSubmitted);
                }
                Err(err) => {
                    poller_fail_job(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Failed to load artifact for reconciliation recovery",
                        err,
                    );
                    return Err(StageError::NoJobsSubmitted);
                }
            };
            let extraction_result =
                match state_store.load_extraction_result(&payload.extraction_result_id) {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        poller_fail_job_message(
                            job_store,
                            worker_id,
                            &job.job_id,
                            format!(
                                "Extraction result {} not found during reconciliation recovery",
                                payload.extraction_result_id
                            ),
                        );
                        return Err(StageError::NoJobsSubmitted);
                    }
                    Err(err) => {
                        poller_fail_job(
                            job_store,
                            worker_id,
                            &job.job_id,
                            "Failed to load extraction result for reconciliation recovery",
                            err,
                        );
                        return Err(StageError::NoJobsSubmitted);
                    }
                };
            let input = match worker_build_reconciliation_input(
                &job.artifact_id,
                &payload.source_type,
                &extraction_result,
            ) {
                Ok(i) => i,
                Err(err) => {
                    poller_fail_job(
                        job_store,
                        worker_id,
                        &job.job_id,
                        "Failed to build reconciliation input during recovery",
                        err,
                    );
                    return Err(StageError::NoJobsSubmitted);
                }
            };
            inputs.push(input);
            payloads.push(payload);
            extraction_results.push(extraction_result);
            loaded_artifacts.push(loaded);
        }

        Ok(InFlightBatch {
            handle: BatchHandle {
                batch_id: persisted.provider_batch_id,
                provider: persisted.provider_name,
                submitted_at: std::time::Instant::now(),
            },
            owner_worker_id: persisted.owner_worker_id,
            jobs: persisted.jobs,
            context: InFlightContext::Reconcile {
                inputs,
                payloads,
                extraction_results,
                loaded_artifacts,
            },
        })
    }
}

fn clear_stage_backoff(
    stage_backoff_until: &mut Option<Instant>,
    consecutive_rate_limits: &mut u32,
) {
    *stage_backoff_until = None;
    *consecutive_rate_limits = 0;
}

fn apply_stage_backoff(
    stage_name: &str,
    stage_backoff_until: &mut Option<Instant>,
    consecutive_rate_limits: &mut u32,
    retry_after_seconds: i64,
    reason: &str,
) {
    let retry_after_seconds = retry_after_seconds.max(1);
    let until = Instant::now()
        .checked_add(Duration::from_secs(retry_after_seconds as u64))
        .unwrap_or_else(Instant::now);
    *stage_backoff_until = Some(until);
    *consecutive_rate_limits = consecutive_rate_limits.saturating_add(1);
    warn!(
        "[{}] entering provider backoff for {}s after {}",
        stage_name, retry_after_seconds, reason
    );
}

fn poller_fail_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    context: &str,
    err: impl std::fmt::Display,
) {
    poller_fail_job_message(job_store, worker_id, job_id, format!("{context}: {err}"));
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
    if err.should_reschedule_without_attempt() {
        let message = format!("Processor execution rate-limited: {err}");
        poller_reschedule_without_attempt(
            job_store,
            worker_id,
            job_id,
            &message,
            err.recommended_retry_after_seconds(),
        );
        return;
    }

    if err.is_retryable() {
        let message = format!("Processor execution failed: {err}");
        poller_mark_retryable(
            job_store,
            worker_id,
            job_id,
            &message,
            err.recommended_retry_after_seconds(),
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

fn poller_reschedule_without_attempt(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    message: &str,
    retry_after_seconds: i64,
) {
    if let Err(err) =
        job_store.reschedule_running_job(worker_id, job_id, message, retry_after_seconds)
    {
        error!(
            "Failed to reschedule job {} without consuming attempt: {}; original error: {}",
            job_id, err, message
        );
    }
}

fn is_repairable_processor_error(err: &ProcessorError) -> bool {
    matches!(
        err,
        ProcessorError::ParseModelJson { .. } | ProcessorError::InvalidModelOutput { .. }
    )
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

struct ReconcileCompletionContext<'a> {
    job_store: &'a dyn EnrichmentJobLifecycleStore,
    state_store: &'a dyn EnrichmentStateStore,
    derived_store: &'a dyn DerivedMetadataWriteStore,
    enqueue_embedding_jobs: bool,
    worker_id: &'a str,
}

fn complete_reconcile_without_candidates(
    job: &ClaimedJob,
    loaded: &LoadedArtifactForEnrichment,
    extraction_result: &ArtifactExtractionResult,
    ctx: ReconcileCompletionContext<'_>,
) -> Result<(), ()> {
    let ReconcileCompletionContext {
        job_store,
        state_store,
        derived_store,
        enqueue_embedding_jobs,
        worker_id,
    } = ctx;
    let decisions = vec![ReconciliationDecision {
        reconciliation_decision_id: worker_new_id("reconcile"),
        artifact_id: extraction_result.artifact_id.clone(),
        job_id: job.job_id.clone(),
        extraction_result_id: extraction_result.extraction_result_id.clone(),
        pipeline_name: "artifact_reconciliation".to_string(),
        pipeline_version: "v1".to_string(),
        decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
        target_kind: "artifact".to_string(),
        target_key: extraction_result.artifact_id.clone(),
        matched_object_id: None,
        rationale:
            "No candidate memories, entities, or relationships were extracted for reconciliation."
                .to_string(),
        evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
        status: "completed".to_string(),
        error_message: None,
    }];
    if let Err(err) = state_store.save_reconciliation_decisions(&decisions) {
        poller_fail_job(
            job_store,
            worker_id,
            &job.job_id,
            "Failed to persist reconciliation decisions",
            err,
        );
        return Err(());
    }
    let attempt = worker_build_derivation_attempt(
        job,
        &loaded.artifact.artifact_id,
        extraction_result,
        &decisions,
    );
    let embedding_job = enqueue_embedding_jobs
        .then(|| worker_build_embedding_job(job, &loaded.artifact.artifact_id, &attempt.objects))
        .flatten();
    if let Err(err) = derived_store.write_derivation_attempt(attempt) {
        poller_fail_job(
            job_store,
            worker_id,
            &job.job_id,
            "Failed to write derivation attempt",
            err,
        );
        return Err(());
    }
    if let Some(embedding_job) = embedding_job.as_ref() {
        worker_try_enqueue_embedding_job(job_store, &job.job_id, embedding_job);
    }
    if let Err(msg) = worker_complete_job(job_store, worker_id, &job.job_id) {
        error!("{msg}");
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageResult;
    use crate::storage::types::{
        ArtifactListFilters, ArtifactListItem, TimelineEntry, TimelineFilters,
    };
    use crate::storage::{DerivationWriteResult, WriteDerivationAttempt};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct BackoffSubmitStage {
        submit_calls: AtomicUsize,
    }

    impl BackoffSubmitStage {
        fn new() -> Self {
            Self {
                submit_calls: AtomicUsize::new(0),
            }
        }
    }

    impl StageBehavior for BackoffSubmitStage {
        fn job_type(&self) -> JobType {
            JobType::ArtifactReconcile
        }

        fn enrichment_tier(&self) -> EnrichmentTier {
            EnrichmentTier::Default
        }

        fn stage_name(&self) -> &'static str {
            "test_backoff"
        }

        fn batch_size(&self) -> usize {
            1
        }

        fn max_concurrent(&self) -> usize {
            3
        }

        fn submit(
            &self,
            _jobs: Vec<ClaimedJob>,
            _read_store: &dyn ArtifactReadStore,
            _state_store: &dyn EnrichmentStateStore,
            _derived_store: &dyn DerivedMetadataWriteStore,
            _job_store: &dyn EnrichmentJobLifecycleStore,
            _worker_id: &str,
        ) -> Result<InFlightBatch, StageError> {
            self.submit_calls.fetch_add(1, Ordering::SeqCst);
            Err(StageError::Backoff {
                retry_after_seconds: 300,
                reason: "test throttle".to_string(),
            })
        }

        fn poll(&self, _batch: &InFlightBatch) -> Result<BatchPollResult, ProcessorError> {
            Ok(BatchPollResult::Pending)
        }

        fn process_completed(
            &self,
            _batch: InFlightBatch,
            _data: Box<dyn std::any::Any>,
            _job_store: &dyn EnrichmentJobLifecycleStore,
            _state_store: &dyn EnrichmentStateStore,
            _derived_store: &dyn DerivedMetadataWriteStore,
            _worker_id: &str,
        ) -> Result<Option<InFlightBatch>, StageError> {
            Ok(None)
        }

        fn phase_name(&self, _batch: &InFlightBatch) -> &'static str {
            "test_backoff"
        }

        fn serialize_context(
            &self,
            _batch: &InFlightBatch,
        ) -> Result<Option<String>, ProcessorError> {
            Ok(None)
        }

        fn recover_batch(
            &self,
            _persisted: PersistedEnrichmentBatch,
            _read_store: &dyn ArtifactReadStore,
            _state_store: &dyn EnrichmentStateStore,
            _job_store: &dyn EnrichmentJobLifecycleStore,
            _worker_id: &str,
        ) -> Result<InFlightBatch, StageError> {
            Err(StageError::NoJobsSubmitted)
        }
    }

    struct BackoffAwareJobStore {
        queued_jobs: Mutex<Vec<ClaimedJob>>,
        claim_calls: AtomicUsize,
    }

    impl BackoffAwareJobStore {
        fn with_jobs(count: usize) -> Self {
            let queued_jobs = (0..count)
                .map(|index| ClaimedJob {
                    job_id: format!("job-{index}"),
                    artifact_id: format!("artifact-{index}"),
                    job_type: JobType::ArtifactReconcile,
                    enrichment_tier: EnrichmentTier::Default,
                    spawned_by_job_id: None,
                    attempt_count: 1,
                    max_attempts: 3,
                    required_capabilities: Vec::new(),
                    payload_json: "{}".to_string(),
                })
                .collect();
            Self {
                queued_jobs: Mutex::new(queued_jobs),
                claim_calls: AtomicUsize::new(0),
            }
        }
    }

    impl EnrichmentJobLifecycleStore for BackoffAwareJobStore {
        fn enqueue_jobs(&self, _jobs: &[NewEnrichmentJob]) -> StorageResult<()> {
            Ok(())
        }

        fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
            Ok(None)
        }

        fn claim_matching_jobs(
            &self,
            _worker_id: &str,
            _template_job: &ClaimedJob,
            _limit: usize,
        ) -> StorageResult<Vec<ClaimedJob>> {
            Ok(Vec::new())
        }

        fn claim_jobs_by_type(
            &self,
            _worker_id: &str,
            _job_type: JobType,
            _enrichment_tier: Option<EnrichmentTier>,
            limit: usize,
        ) -> StorageResult<Vec<ClaimedJob>> {
            self.claim_calls.fetch_add(1, Ordering::SeqCst);
            let mut queued_jobs = self.queued_jobs.lock().unwrap();
            let count = limit.min(queued_jobs.len());
            Ok(queued_jobs.drain(..count).collect())
        }

        fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
            Ok(())
        }

        fn fail_job(
            &self,
            _worker_id: &str,
            _job_id: &str,
            _error_message: &str,
        ) -> StorageResult<()> {
            Ok(())
        }

        fn mark_job_retryable(
            &self,
            _worker_id: &str,
            _job_id: &str,
            _error_message: &str,
            _retry_after_seconds: i64,
        ) -> StorageResult<crate::storage::RetryOutcome> {
            Ok(crate::storage::RetryOutcome::Retried)
        }

        fn reschedule_running_job(
            &self,
            _worker_id: &str,
            _job_id: &str,
            _message: &str,
            _retry_after_seconds: i64,
        ) -> StorageResult<()> {
            Ok(())
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

    struct NullArtifactReadStore;

    impl ArtifactReadStore for NullArtifactReadStore {
        fn list_artifacts(&self) -> StorageResult<Vec<ArtifactListItem>> {
            Ok(Vec::new())
        }

        fn list_artifacts_filtered(
            &self,
            _filters: &ArtifactListFilters,
            _limit: usize,
            _offset: usize,
        ) -> StorageResult<Vec<ArtifactListItem>> {
            Ok(Vec::new())
        }

        fn get_timeline(
            &self,
            _filters: &TimelineFilters,
            _limit: usize,
            _offset: usize,
        ) -> StorageResult<Vec<TimelineEntry>> {
            Ok(Vec::new())
        }

        fn load_artifact_for_enrichment(
            &self,
            _artifact_id: &str,
        ) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
            Ok(None)
        }
    }

    struct NullEnrichmentStateStore;

    impl EnrichmentStateStore for NullEnrichmentStateStore {
        fn save_extraction_result(&self, _result: &ArtifactExtractionResult) -> StorageResult<()> {
            Ok(())
        }

        fn load_extraction_result(
            &self,
            _extraction_result_id: &str,
        ) -> StorageResult<Option<ArtifactExtractionResult>> {
            Ok(None)
        }

        fn save_reconciliation_decisions(
            &self,
            _decisions: &[ReconciliationDecision],
        ) -> StorageResult<()> {
            Ok(())
        }

        fn load_reconciliation_decisions(
            &self,
            _extraction_result_id: &str,
        ) -> StorageResult<Vec<ReconciliationDecision>> {
            Ok(Vec::new())
        }
    }

    struct NullDerivedMetadataWriteStore;

    impl DerivedMetadataWriteStore for NullDerivedMetadataWriteStore {
        fn write_derivation_attempt(
            &self,
            _attempt: WriteDerivationAttempt,
        ) -> StorageResult<DerivationWriteResult> {
            Ok(DerivationWriteResult {
                derivation_run_id: "run-1".to_string(),
                derived_object_ids: Vec::new(),
                evidence_links_written: 0,
            })
        }
    }

    #[test]
    fn throttled_submit_stops_claim_loop_for_current_cycle() {
        let stage = Arc::new(BackoffSubmitStage::new());
        let job_store = Arc::new(BackoffAwareJobStore::with_jobs(3));
        let read_store = NullArtifactReadStore;
        let state_store = NullEnrichmentStateStore;
        let derived_store = NullDerivedMetadataWriteStore;
        let shutdown = ShutdownToken::new();
        let shutdown_for_thread = shutdown.clone();
        let stage_for_thread = Arc::clone(&stage);
        let job_store_for_thread = Arc::clone(&job_store);

        let handle = thread::spawn(move || {
            stage_poller_loop(
                stage_for_thread.as_ref(),
                "test-worker".to_string(),
                StagePollerContext {
                    job_store: job_store_for_thread.as_ref(),
                    read_store: &read_store,
                    state_store: &state_store,
                    derived_store: &derived_store,
                },
                Duration::from_millis(5),
                shutdown_for_thread,
            );
        });

        thread::sleep(Duration::from_millis(30));
        shutdown.signal();
        handle.join().expect("stage poller should exit cleanly");

        assert_eq!(job_store.claim_calls.load(Ordering::SeqCst), 1);
        assert_eq!(stage.submit_calls.load(Ordering::SeqCst), 1);
    }
}
