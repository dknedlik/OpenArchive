use crate::config::{EnrichmentPipelineConfig, ExtractionChunkingConfig, InferenceExecutionMode};
use crate::domain::SourceTimestamp;
use crate::embedding::EmbeddingProvider;
use crate::error::{WorkerError, WorkerResult};
use crate::extraction_chunking::{
    build_chunk_inputs, build_coverage_windows, build_topic_thread_inputs,
};
use crate::processor::{
    cleanup_artifact_processor_output, memory_candidate_key_from_fields,
    should_shape_artifact_input, ArtifactProcessorFactory, ArtifactProcessorInput,
    ArtifactProcessorOutput, ClassificationOutput, EntityOutput, MemoryOutput, ProcessorError,
    ReconciliationProcessorInput, RelationshipOutput, StubProcessorFactory, SummaryOutput,
};
use crate::shutdown::ShutdownToken;
use crate::storage::JobType;
use crate::storage::{
    ArtifactExtractPayload, ArtifactExtractionResult, ArtifactReadStore, ArtifactReconcilePayload,
    CandidateEntity, CandidateRelationship, ClaimedJob, ClassificationObjectJson,
    ConversationWindowRef, CrossArtifactReadStore, DerivationRunStatus, DerivationRunType,
    DerivedMetadataWriteStore, DerivedObjectEmbeddingItem, DerivedObjectEmbeddingPayload,
    DerivedObjectEmbeddingStore, DerivedObjectPayload, DerivedObjectType,
    EnrichmentJobLifecycleStore, EnrichmentStateStore, EnrichmentTier, EntityObjectJson,
    EvidenceRole, ExtractedClassification, ExtractedMemory, InputScopeType, JobStatus,
    MemoryObjectJson, NewArchiveLink, NewDerivationRun, NewDerivedObject,
    NewDerivedObjectEmbedding, NewEnrichmentJob, NewEvidenceLink, ObjectStatus, OriginKind,
    ReconciliationDecision, ReconciliationDecisionKind, RelationshipObjectJson, RetrievalIntent,
    ScopeType, SummaryObjectJson, SupportStrength, WriteDerivationAttempt, WriteDerivedObject,
};
use log::{debug, error, info, warn};
use rand::random;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RETRYABLE_INFERENCE_BACKOFF_SECONDS: i64 = 60;
const RECONCILE_EMBEDDING_DETERMINISTIC_THRESHOLD: f32 = 0.90;
const RECONCILE_EMBEDDING_AMBIGUOUS_THRESHOLD: f32 = 0.70;
const RECONCILE_EMBEDDING_LOOKUP_LIMIT: usize = 3;

fn has_reconciliation_candidates(extraction_result: &ArtifactExtractionResult) -> bool {
    !(extraction_result.memories.is_empty()
        && extraction_result.entities.is_empty()
        && extraction_result.relationships.is_empty())
}

#[derive(Clone)]
struct ExtractCoveragePolicy {
    min_coverage_percent: u8,
    max_gap_fill_passes: usize,
}

/// Borrowed view of all store/service/factory dependencies used by worker functions.
struct WorkerContext<'a> {
    job_store: &'a dyn EnrichmentJobLifecycleStore,
    read_store: &'a dyn ArtifactReadStore,
    state_store: &'a dyn EnrichmentStateStore,
    derived_store: &'a dyn DerivedMetadataWriteStore,
    cross_artifact_store: Option<&'a (dyn CrossArtifactReadStore + Send + Sync)>,
    embedding_store: Option<&'a dyn DerivedObjectEmbeddingStore>,
    embedding_provider: Option<&'a dyn EmbeddingProvider>,
    processor_factory: &'a dyn ArtifactProcessorFactory,
}

/// Extraction chunking and coverage parameters.
struct ExtractionPolicy<'a> {
    chunking: &'a ExtractionChunkingConfig,
    coverage: &'a ExtractCoveragePolicy,
}

/// Owned Arc resources for long-running worker threads.
struct WorkerResources {
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    embedding_store: Option<Arc<dyn DerivedObjectEmbeddingStore>>,
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
}

impl WorkerResources {
    fn as_context(&self) -> WorkerContext<'_> {
        WorkerContext {
            job_store: self.job_store.as_ref(),
            read_store: self.read_store.as_ref(),
            state_store: self.state_store.as_ref(),
            derived_store: self.derived_store.as_ref(),
            cross_artifact_store: self.cross_artifact_store.as_deref(),
            embedding_store: self.embedding_store.as_deref(),
            embedding_provider: self.embedding_provider.as_deref(),
            processor_factory: self.processor_factory.as_ref(),
        }
    }
}

pub struct WorkerStartConfig<'a> {
    pub http: &'a crate::config::HttpConfig,
    pub shutdown: ShutdownToken,
    pub processor_factory: Arc<dyn ArtifactProcessorFactory>,
}

pub struct EnrichmentPipelineResources {
    pub job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    pub read_store: Arc<dyn ArtifactReadStore>,
    pub state_store: Arc<dyn EnrichmentStateStore>,
    pub derived_store: Arc<dyn DerivedMetadataWriteStore>,
    pub cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    pub embedding_store: Option<Arc<dyn DerivedObjectEmbeddingStore>>,
    pub embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
}

/// Worker ID format: enrichment:<pid>:<worker_index>
pub fn format_worker_id(pid: u32, worker_index: usize) -> String {
    format!("enrichment:{}:{}", pid, worker_index)
}

fn enrichment_worker(
    worker_id: String,
    res: WorkerResources,
    chunking: Arc<ExtractionChunkingConfig>,
    coverage_policy: Arc<ExtractCoveragePolicy>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!("Enrichment worker {} starting", worker_id);

    loop {
        if shutdown.is_shutdown() {
            info!("Enrichment worker {} shutting down", worker_id);
            break;
        }

        match res.job_store.claim_next_job(&worker_id) {
            Ok(Some(claimed_job)) => {
                debug!("Worker {} claimed job {}", worker_id, claimed_job.job_id);
                let ctx = res.as_context();
                let policy = ExtractionPolicy {
                    chunking: chunking.as_ref(),
                    coverage: coverage_policy.as_ref(),
                };
                if let Err(err) = process_claimed_jobs(
                    &worker_id,
                    claimed_job,
                    &ctx,
                    InferenceExecutionMode::Direct,
                    &policy,
                ) {
                    error!(
                        "Worker {} failed to process claimed work: {}",
                        worker_id, err
                    );
                }
            }
            Ok(None) => thread::sleep(poll_interval),
            Err(err) => {
                error!("Worker {} failed to claim job: {}", worker_id, err);
                thread::sleep(poll_interval);
            }
        }
    }
}

fn process_claimed_jobs(
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

fn process_claimed_job(
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

fn process_extract_job_batch(
    worker_id: &str,
    claimed_jobs: Vec<ClaimedJob>,
    ctx: &WorkerContext<'_>,
    batch_processor: &dyn crate::processor::ArtifactBatchProcessor,
    policy: &ExtractionPolicy<'_>,
) -> std::result::Result<(), String> {
    let job_store = ctx.job_store;
    let read_store = ctx.read_store;
    let state_store = ctx.state_store;
    let mut batchable = Vec::new();
    let mut fallbacks = Vec::new();

    for claimed_job in claimed_jobs {
        let payload = match ArtifactExtractPayload::from_json(&claimed_job.payload_json) {
            Ok(payload) => payload,
            Err(err) => {
                fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to parse extract payload JSON",
                    err,
                );
                continue;
            }
        };
        let Some(source_type) = crate::storage::SourceType::parse(&payload.source_type) else {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Invalid artifact source_type in extract payload: {}",
                    payload.source_type
                ),
            );
            continue;
        };
        let loaded = match read_store.load_artifact_for_enrichment(&claimed_job.artifact_id) {
            Ok(Some(loaded)) => loaded,
            Ok(None) => {
                fail_job_message(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    format!(
                        "Artifact {} not found for extraction",
                        claimed_job.artifact_id
                    ),
                );
                continue;
            }
            Err(err) => {
                fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to load artifact for extraction",
                    err,
                );
                continue;
            }
        };
        let input = ArtifactProcessorInput {
            artifact_id: loaded.artifact.artifact_id.clone(),
            import_id: payload.import_id.clone(),
            artifact_class: loaded.artifact.artifact_class,
            source_type,
            title: loaded.artifact.title.clone(),
            imported_note_metadata: payload.imported_note_metadata.clone(),
            participants: loaded.participants,
            segments: loaded.segments,
        };
        if payload.topic_threads.is_empty()
            && payload.conversation_windows.len() <= 1
            && batch_processor.can_process(&input)
        {
            batchable.push((claimed_job, payload, input));
        } else {
            fallbacks.push(claimed_job);
        }
    }

    if !batchable.is_empty() {
        let inputs: Vec<_> = batchable
            .iter()
            .map(|(_, _, input)| input.clone())
            .collect();
        let results = batch_processor.process_batch(&inputs);
        for ((claimed_job, payload, input), result) in
            batchable.into_iter().zip(results.into_iter())
        {
            match result {
                Ok(output) => {
                    let extraction_result = build_extraction_result(
                        &claimed_job,
                        &input,
                        &output,
                        payload.conversation_windows,
                    );
                    if let Err(err) = state_store.save_extraction_result(&extraction_result) {
                        let _ = fail_job(
                            job_store,
                            worker_id,
                            &claimed_job.job_id,
                            "Failed to persist extraction result",
                            err,
                        );
                        continue;
                    }
                    let reconcile_job = NewEnrichmentJob {
                        job_id: new_id("job"),
                        artifact_id: claimed_job.artifact_id.clone(),
                        job_type: crate::storage::JobType::ArtifactReconcile,
                        enrichment_tier: claimed_job.enrichment_tier,
                        spawned_by_job_id: Some(claimed_job.job_id.clone()),
                        job_status: crate::storage::JobStatus::Pending,
                        max_attempts: 3,
                        priority_no: 100,
                        required_capabilities: vec!["text".to_string()],
                        payload_json: ArtifactReconcilePayload::new_v1(
                            &claimed_job.artifact_id,
                            &payload.import_id,
                            input.source_type,
                            &extraction_result.extraction_result_id,
                        )
                        .to_json(),
                    };
                    if let Err(err) = job_store.enqueue_jobs(&[reconcile_job]) {
                        let _ = fail_job(
                            job_store,
                            worker_id,
                            &claimed_job.job_id,
                            "Failed to enqueue reconciliation job",
                            err,
                        );
                        continue;
                    }
                    complete_job(job_store, worker_id, &claimed_job.job_id)?;
                }
                Err(err) => {
                    let _ = handle_processor_error(job_store, worker_id, &claimed_job.job_id, err);
                }
            }
        }
    }

    for claimed_job in fallbacks {
        process_extract_job(worker_id, &claimed_job, ctx, policy)?;
    }

    Ok(())
}

fn process_reconcile_job_batch(
    worker_id: &str,
    claimed_jobs: Vec<ClaimedJob>,
    ctx: &WorkerContext<'_>,
    batch_processor: &dyn crate::processor::ReconciliationBatchProcessor,
) -> std::result::Result<(), String> {
    let job_store = ctx.job_store;
    let read_store = ctx.read_store;
    let state_store = ctx.state_store;
    let derived_store = ctx.derived_store;
    let mut batchable = Vec::new();
    let mut fallbacks = Vec::new();

    for claimed_job in claimed_jobs {
        let payload = match ArtifactReconcilePayload::from_json(&claimed_job.payload_json) {
            Ok(payload) => payload,
            Err(err) => {
                fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to parse reconcile payload JSON",
                    err,
                );
                continue;
            }
        };
        let loaded = match read_store.load_artifact_for_enrichment(&claimed_job.artifact_id) {
            Ok(Some(loaded)) => loaded,
            Ok(None) => {
                fail_job_message(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    format!(
                        "Artifact {} not found for reconciliation",
                        claimed_job.artifact_id
                    ),
                );
                continue;
            }
            Err(err) => {
                fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to load artifact for reconciliation",
                    err,
                );
                continue;
            }
        };
        let extraction_result =
            match state_store.load_extraction_result(&payload.extraction_result_id) {
                Ok(Some(result)) => result,
                Ok(None) => {
                    fail_job_message(
                        job_store,
                        worker_id,
                        &claimed_job.job_id,
                        format!(
                            "Extraction result {} not found",
                            payload.extraction_result_id
                        ),
                    );
                    continue;
                }
                Err(err) => {
                    fail_job(
                        job_store,
                        worker_id,
                        &claimed_job.job_id,
                        "Failed to load extraction result for reconciliation",
                        err,
                    );
                    continue;
                }
            };
        let input = match build_reconciliation_input(
            &claimed_job.artifact_id,
            &payload.source_type,
            &extraction_result,
        ) {
            Ok(input) => input,
            Err(err) => {
                fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to build reconciliation input",
                    err,
                );
                continue;
            }
        };
        let prepared = match prepare_reconciliation_work(
            &input,
            ctx.cross_artifact_store,
            ctx.embedding_provider,
        ) {
            Ok(prepared) => prepared,
            Err(err) => {
                fail_job_message(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    format!("Failed to prepare reconciliation candidates: {err}"),
                );
                continue;
            }
        };
        if let Some(ambiguous_input) = prepared.ambiguous_input {
            if batch_processor
                .estimate_size_bytes(&ambiguous_input)
                .unwrap_or(usize::MAX)
                <= batch_processor.max_batch_bytes()
            {
                batchable.push((
                    claimed_job,
                    loaded,
                    extraction_result,
                    prepared.deterministic_outputs,
                    ambiguous_input,
                ));
            } else {
                fallbacks.push(claimed_job);
            }
        } else {
            let decisions = build_reconciliation_decisions(
                &claimed_job,
                &extraction_result,
                prepared.deterministic_outputs,
            );
            if let Err(err) = state_store.save_reconciliation_decisions(&decisions) {
                let _ = fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to persist reconciliation decisions",
                    err,
                );
                continue;
            }
            let attempt = build_derivation_attempt(
                &claimed_job,
                &loaded.artifact.artifact_id,
                &extraction_result,
                &decisions,
            );
            let embedding_job = (ctx.embedding_store.is_some() && ctx.embedding_provider.is_some())
                .then(|| {
                    build_embedding_job(
                        &claimed_job,
                        &loaded.artifact.artifact_id,
                        &attempt.objects,
                    )
                })
                .flatten();
            if let Err(err) = derived_store.write_derivation_attempt(attempt) {
                let _ = fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to persist derivation output",
                    err,
                );
                continue;
            }
            if let Some(job) = embedding_job.as_ref() {
                try_enqueue_embedding_job(job_store, &claimed_job.job_id, job);
            }
            complete_job(job_store, worker_id, &claimed_job.job_id)?;
        }
    }

    if !batchable.is_empty() {
        let inputs: Vec<_> = batchable
            .iter()
            .map(|(_, _, _, _, input)| input.clone())
            .collect();
        let results = batch_processor.process_batch(&inputs);
        for ((claimed_job, loaded, extraction_result, deterministic_outputs, _), result) in
            batchable.into_iter().zip(results.into_iter())
        {
            match result {
                Ok(outputs) => {
                    let decisions = if !has_reconciliation_candidates(&extraction_result) {
                        vec![ReconciliationDecision {
                            reconciliation_decision_id: new_id("reconcile"),
                            artifact_id: extraction_result.artifact_id.clone(),
                            job_id: claimed_job.job_id.clone(),
                            extraction_result_id: extraction_result.extraction_result_id.clone(),
                            pipeline_name: "artifact_reconciliation".to_string(),
                            pipeline_version: "v1".to_string(),
                            decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
                            target_kind: "artifact".to_string(),
                            target_key: extraction_result.artifact_id.clone(),
                            matched_object_id: None,
                            rationale: "No candidate memories, entities, or relationships were extracted for reconciliation.".to_string(),
                            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
                            status: "completed".to_string(),
                            error_message: None,
                        }]
                    } else {
                        let mut combined_outputs = deterministic_outputs;
                        combined_outputs.extend(outputs);
                        build_reconciliation_decisions(
                            &claimed_job,
                            &extraction_result,
                            combined_outputs,
                        )
                    };
                    if let Err(err) = state_store.save_reconciliation_decisions(&decisions) {
                        let _ = fail_job(
                            job_store,
                            worker_id,
                            &claimed_job.job_id,
                            "Failed to persist reconciliation decisions",
                            err,
                        );
                        continue;
                    }
                    let attempt = build_derivation_attempt(
                        &claimed_job,
                        &loaded.artifact.artifact_id,
                        &extraction_result,
                        &decisions,
                    );
                    let embedding_job = (ctx.embedding_store.is_some()
                        && ctx.embedding_provider.is_some())
                    .then(|| {
                        build_embedding_job(
                            &claimed_job,
                            &loaded.artifact.artifact_id,
                            &attempt.objects,
                        )
                    })
                    .flatten();
                    if let Err(err) = derived_store.write_derivation_attempt(attempt) {
                        let _ = fail_job(
                            job_store,
                            worker_id,
                            &claimed_job.job_id,
                            "Failed to persist derivation output",
                            err,
                        );
                        continue;
                    }
                    if let Some(job) = embedding_job.as_ref() {
                        try_enqueue_embedding_job(job_store, &claimed_job.job_id, job);
                    }
                    complete_job(job_store, worker_id, &claimed_job.job_id)?;
                }
                Err(err) => {
                    let _ = handle_processor_error(job_store, worker_id, &claimed_job.job_id, err);
                }
            }
        }
    }

    for claimed_job in fallbacks {
        process_reconcile_job(worker_id, &claimed_job, ctx, false)?;
    }

    Ok(())
}

fn process_extract_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    ctx: &WorkerContext<'_>,
    policy: &ExtractionPolicy<'_>,
) -> std::result::Result<(), String> {
    let job_store = ctx.job_store;
    let read_store = ctx.read_store;
    let state_store = ctx.state_store;
    let processor_factory = ctx.processor_factory;
    let chunking = policy.chunking;
    let payload = ArtifactExtractPayload::from_json(&claimed_job.payload_json).map_err(|err| {
        fail_job(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "Failed to parse extract payload JSON",
            err,
        )
    })?;

    let loaded = read_store
        .load_artifact_for_enrichment(&claimed_job.artifact_id)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to load artifact for extraction",
                err,
            )
        })?
        .ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Artifact {} not found for extraction",
                    claimed_job.artifact_id
                ),
            )
        })?;

    let source_type = crate::storage::SourceType::parse(&payload.source_type).ok_or_else(|| {
        fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            format!(
                "Invalid artifact source_type in extract payload: {}",
                payload.source_type
            ),
        )
    })?;

    let processor_input = ArtifactProcessorInput {
        artifact_id: loaded.artifact.artifact_id.clone(),
        import_id: payload.import_id.clone(),
        artifact_class: loaded.artifact.artifact_class,
        source_type,
        title: loaded.artifact.title.clone(),
        imported_note_metadata: payload.imported_note_metadata.clone(),
        participants: loaded.participants,
        segments: loaded.segments,
    };

    let processor = processor_factory
        .build(claimed_job.enrichment_tier)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to build extraction processor",
                err,
            )
        })?;

    let chunk_inputs = build_extract_chunk_inputs(&processor_input, &payload, chunking);
    let output = if chunk_inputs.len() > 1 {
        let mut chunk_outputs = Vec::new();
        for chunk_input in chunk_inputs {
            let chunk_output = processor.process(&chunk_input).map_err(|err| {
                handle_processor_error(job_store, worker_id, &claimed_job.job_id, err)
            })?;
            chunk_outputs.push(chunk_output);
        }
        merge_chunk_outputs(&processor_input, &chunk_outputs)
    } else {
        let chunk_input = chunk_inputs
            .into_iter()
            .next()
            .expect("build_extract_chunk_inputs must return at least one chunk");
        processor
            .process(&chunk_input)
            .map_err(|err| handle_processor_error(job_store, worker_id, &claimed_job.job_id, err))?
    };
    let output = maybe_gap_fill_extraction_output(
        worker_id,
        claimed_job,
        job_store,
        processor.as_ref(),
        &processor_input,
        output,
        policy,
    )?;

    let extraction_result = build_extraction_result(
        claimed_job,
        &processor_input,
        &output,
        payload.conversation_windows,
    );
    state_store
        .save_extraction_result(&extraction_result)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to persist extraction result",
                err,
            )
        })?;

    let reconcile_job = NewEnrichmentJob {
        job_id: new_id("job"),
        artifact_id: claimed_job.artifact_id.clone(),
        job_type: crate::storage::JobType::ArtifactReconcile,
        enrichment_tier: claimed_job.enrichment_tier,
        spawned_by_job_id: Some(claimed_job.job_id.clone()),
        job_status: crate::storage::JobStatus::Pending,
        max_attempts: 3,
        priority_no: 100,
        required_capabilities: vec!["text".to_string()],
        payload_json: ArtifactReconcilePayload::new_v1(
            &claimed_job.artifact_id,
            &payload.import_id,
            source_type,
            &extraction_result.extraction_result_id,
        )
        .to_json(),
    };
    job_store.enqueue_jobs(&[reconcile_job]).map_err(|err| {
        fail_job(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "Failed to enqueue reconciliation job",
            err,
        )
    })?;

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
}

fn process_reconcile_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    ctx: &WorkerContext<'_>,
    enqueue_embedding_jobs: bool,
) -> std::result::Result<(), String> {
    let job_store = ctx.job_store;
    let read_store = ctx.read_store;
    let state_store = ctx.state_store;
    let derived_store = ctx.derived_store;
    let processor_factory = ctx.processor_factory;
    let payload =
        ArtifactReconcilePayload::from_json(&claimed_job.payload_json).map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to parse reconcile payload JSON",
                err,
            )
        })?;

    let loaded = read_store
        .load_artifact_for_enrichment(&claimed_job.artifact_id)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to load artifact for reconciliation",
                err,
            )
        })?
        .ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Artifact {} not found for reconciliation",
                    claimed_job.artifact_id
                ),
            )
        })?;
    let extraction_result = state_store
        .load_extraction_result(&payload.extraction_result_id)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to load extraction result for reconciliation",
                err,
            )
        })?
        .ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Extraction result {} not found",
                    payload.extraction_result_id
                ),
            )
        })?;
    let decisions = if !has_reconciliation_candidates(&extraction_result) {
        vec![ReconciliationDecision {
            reconciliation_decision_id: new_id("reconcile"),
            artifact_id: extraction_result.artifact_id.clone(),
            job_id: claimed_job.job_id.clone(),
            extraction_result_id: extraction_result.extraction_result_id.clone(),
            pipeline_name: "artifact_reconciliation".to_string(),
            pipeline_version: "v1".to_string(),
            decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
            target_kind: "artifact".to_string(),
            target_key: extraction_result.artifact_id.clone(),
            matched_object_id: None,
            rationale: "No candidate memories, entities, or relationships were extracted for reconciliation."
                .to_string(),
            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
            status: "completed".to_string(),
            error_message: None,
        }]
    } else {
        let input = build_reconciliation_input(
            &claimed_job.artifact_id,
            &payload.source_type,
            &extraction_result,
        )
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to build reconciliation input",
                err,
            )
        })?;
        let prepared =
            prepare_reconciliation_work(&input, ctx.cross_artifact_store, ctx.embedding_provider)
                .map_err(|err| {
                fail_job_message(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    format!("Failed to prepare reconciliation candidates: {err}"),
                )
            })?;
        let mut outputs = prepared.deterministic_outputs;
        if let Some(ambiguous_input) = prepared.ambiguous_input {
            let processor = processor_factory
                .build_reconciliation_processor(claimed_job.enrichment_tier)
                .map_err(|err| {
                    fail_job(
                        job_store,
                        worker_id,
                        &claimed_job.job_id,
                        "Failed to build reconciliation processor",
                        err,
                    )
                })?;
            let inferred_outputs = processor.reconcile(&ambiguous_input).map_err(|err| {
                handle_processor_error(job_store, worker_id, &claimed_job.job_id, err)
            })?;
            outputs.extend(inferred_outputs);
        }
        build_reconciliation_decisions(claimed_job, &extraction_result, outputs)
    };
    state_store
        .save_reconciliation_decisions(&decisions)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to persist reconciliation decisions",
                err,
            )
        })?;

    let attempt = build_derivation_attempt(
        claimed_job,
        &loaded.artifact.artifact_id,
        &extraction_result,
        &decisions,
    );
    let embedding_job = enqueue_embedding_jobs
        .then(|| build_embedding_job(claimed_job, &loaded.artifact.artifact_id, &attempt.objects))
        .flatten();
    derived_store
        .write_derivation_attempt(attempt)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to persist derivation output",
                err,
            )
        })?;
    if let Some(job) = embedding_job.as_ref() {
        try_enqueue_embedding_job(job_store, &claimed_job.job_id, job);
    }

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
}

fn process_embedding_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    embedding_store: Option<&dyn DerivedObjectEmbeddingStore>,
    embedding_provider: Option<&dyn EmbeddingProvider>,
) -> std::result::Result<(), String> {
    let payload =
        DerivedObjectEmbeddingPayload::from_json(&claimed_job.payload_json).map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to parse embedding payload JSON",
                err,
            )
        })?;

    let Some(embedding_store) = embedding_store else {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "No embedding store configured for derived object embeddings".to_string(),
        ));
    };
    let Some(embedding_provider) = embedding_provider else {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "No embedding provider configured for derived object embeddings".to_string(),
        ));
    };

    let texts = payload
        .objects
        .iter()
        .map(DerivedObjectEmbeddingItem::text_for_embedding)
        .collect::<Vec<_>>();
    let vectors = embedding_provider.embed_texts(&texts).map_err(|err| {
        let message = format!("Embedding generation failed: {err}");
        if embedding_error_is_retryable(&err) {
            mark_job_retryable_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                message,
                RETRYABLE_INFERENCE_BACKOFF_SECONDS,
            )
        } else {
            fail_job_message(job_store, worker_id, &claimed_job.job_id, message)
        }
    })?;

    if vectors.len() != payload.objects.len() {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            format!(
                "Embedding provider returned {} vectors for {} objects",
                vectors.len(),
                payload.objects.len()
            ),
        ));
    }

    let embeddings = payload
        .objects
        .iter()
        .zip(vectors.into_iter())
        .map(|(object, embedding)| {
            let derived_object_type = crate::storage::DerivedObjectType::parse(
                &object.derived_object_type,
            )
            .ok_or_else(|| {
                fail_job_message(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    format!(
                        "Invalid derived_object_type in embedding payload: {}",
                        object.derived_object_type
                    ),
                )
            })?;
            Ok(NewDerivedObjectEmbedding {
                derived_object_id: object.derived_object_id.clone(),
                artifact_id: payload.artifact_id.clone(),
                derived_object_type,
                provider_name: embedding_provider.provider_name().to_string(),
                model_name: embedding_provider.model_name().to_string(),
                content_text_hash: sha256_hex(&object.text_for_embedding()),
                embedding,
            })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;

    embedding_store
        .upsert_embeddings(&embeddings)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to persist derived object embeddings",
                err,
            )
        })?;

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
}

pub(crate) fn build_embedding_job(
    claimed_job: &ClaimedJob,
    artifact_id: &str,
    objects: &[WriteDerivedObject],
) -> Option<NewEnrichmentJob> {
    let items = objects
        .iter()
        .filter_map(|object_write| {
            let object = &object_write.object;
            if object.object_status != ObjectStatus::Active {
                return None;
            }
            let derived_object_type = object.payload.derived_object_type();
            if !derived_object_type.supports_embeddings() {
                return None;
            }
            let body_text = object
                .payload
                .body_text()
                .map(str::trim)
                .filter(|value| !value.is_empty())?
                .to_string();
            Some(DerivedObjectEmbeddingItem {
                derived_object_id: object.derived_object_id.clone(),
                derived_object_type: derived_object_type.as_str().to_string(),
                title: object.payload.title().map(ToOwned::to_owned),
                body_text,
            })
        })
        .collect::<Vec<_>>();

    if items.is_empty() {
        return None;
    }

    Some(NewEnrichmentJob {
        job_id: new_id("job"),
        artifact_id: artifact_id.to_string(),
        job_type: JobType::DerivedObjectEmbed,
        enrichment_tier: EnrichmentTier::Default,
        spawned_by_job_id: Some(claimed_job.job_id.clone()),
        job_status: JobStatus::Pending,
        max_attempts: 3,
        priority_no: 120,
        required_capabilities: vec!["text".to_string()],
        payload_json: DerivedObjectEmbeddingPayload::new_v1(artifact_id, items).to_json(),
    })
}

pub(crate) fn try_enqueue_embedding_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    parent_job_id: &str,
    embedding_job: &NewEnrichmentJob,
) {
    if let Err(err) = job_store.enqueue_jobs(std::slice::from_ref(embedding_job)) {
        warn!(
            "Failed to enqueue embedding job {} spawned by {}: {}",
            embedding_job.job_id, parent_job_id, err
        );
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn embedding_error_is_retryable(err: &crate::error::EmbeddingError) -> bool {
    match err {
        crate::error::EmbeddingError::SendRequest { .. }
        | crate::error::EmbeddingError::ReadResponse { .. } => true,
        crate::error::EmbeddingError::HttpStatus { status, .. } => *status >= 500,
        _ => false,
    }
}

pub(crate) fn build_extraction_result(
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
            .map(|classification| ExtractedClassification {
                title: classification.title.clone(),
                body_text: classification.body_text.clone(),
                classification_type: classification.classification_type.clone(),
                classification_value: classification.classification_value.clone(),
                evidence_segment_ids: classification.evidence_segment_ids.clone(),
            })
            .collect(),
        memories: output
            .memories
            .iter()
            .map(|memory| ExtractedMemory {
                candidate_key: memory.candidate_key.clone(),
                title: memory.title.clone(),
                body_text: memory.body_text.clone(),
                memory_type: memory.memory_type.clone(),
                memory_scope: memory.memory_scope,
                memory_scope_value: memory.memory_scope_value.clone(),
                evidence_segment_ids: memory.evidence_segment_ids.clone(),
            })
            .collect(),
        entities: output
            .entities
            .iter()
            .map(|entity| CandidateEntity {
                entity_key: entity.entity_key.clone(),
                display_name: entity.display_name.clone(),
                entity_type: entity.entity_type.clone(),
                evidence_segment_ids: entity.evidence_segment_ids.clone(),
            })
            .collect(),
        relationships: output
            .relationships
            .iter()
            .map(|relationship| CandidateRelationship {
                relationship_type: relationship.relationship_type.clone(),
                subject_key: relationship.subject_key.clone(),
                object_key: relationship.object_key.clone(),
                title: relationship.title.clone(),
                body_text: relationship.body_text.clone(),
                confidence_label: relationship.confidence_label.clone(),
                evidence_segment_ids: relationship.evidence_segment_ids.clone(),
            })
            .collect(),
        retrieval_intents: output
            .retrieval_intents
            .iter()
            .map(|intent| RetrievalIntent {
                intent_id: new_id("intent"),
                question: intent.question.clone(),
                query_text: intent.query_text.clone(),
                intent_type: intent.intent_type.clone(),
                evidence_segment_ids: intent.evidence_segment_ids.clone(),
            })
            .collect(),
        status: "completed".to_string(),
        error_message: None,
    }
}

pub(crate) fn merge_chunk_outputs(
    input: &ArtifactProcessorInput,
    outputs: &[ArtifactProcessorOutput],
) -> ArtifactProcessorOutput {
    let first = outputs
        .first()
        .cloned()
        .expect("merge_chunk_outputs requires at least one chunk output");
    if outputs.len() == 1 {
        return first;
    }

    let mut summary_bodies = Vec::new();
    let mut summary_titles = Vec::new();
    let mut summary_evidence = Vec::new();
    let mut classifications = BTreeMap::<(String, String), ClassificationOutput>::new();
    let mut memories = BTreeMap::<String, MemoryOutput>::new();
    let mut entities = BTreeMap::<String, EntityOutput>::new();
    let mut relationships = BTreeMap::<String, RelationshipOutput>::new();
    let mut retrieval_intents = BTreeMap::<(String, String), RetrievalIntent>::new();
    let mut importance_score = 1u8;

    for output in outputs {
        if let Some(title) = &output.summary.title {
            if !summary_titles.contains(title) {
                summary_titles.push(title.clone());
            }
        }
        if !summary_bodies.contains(&output.summary.body_text) {
            summary_bodies.push(output.summary.body_text.clone());
        }
        extend_unique(&mut summary_evidence, &output.summary.evidence_segment_ids);
        importance_score = importance_score.max(output.importance_score);

        for classification in &output.classifications {
            let key = (
                classification.classification_type.clone(),
                classification.classification_value.clone(),
            );
            if let Some(existing) = classifications.get_mut(&key) {
                merge_classification_output(existing, classification);
            } else {
                classifications.insert(key, classification.clone());
            }
        }
        for memory in &output.memories {
            let key = memory_target_key_from_output(memory);
            if let Some(existing) = memories.get_mut(&key) {
                merge_memory_output(existing, memory);
            } else {
                memories.insert(key, memory.clone());
            }
        }
        for entity in &output.entities {
            if let Some(existing) = entities.get_mut(&entity.entity_key) {
                merge_entity_output(existing, entity);
            } else {
                entities.insert(entity.entity_key.clone(), entity.clone());
            }
        }
        for relationship in &output.relationships {
            let key = relationship_target_key_from_output(relationship);
            if let Some(existing) = relationships.get_mut(&key) {
                merge_relationship_output(existing, relationship);
            } else {
                relationships.insert(key, relationship.clone());
            }
        }
        for intent in &output.retrieval_intents {
            let key = (intent.intent_type.clone(), intent.query_text.clone());
            if let Some(existing) = retrieval_intents.get_mut(&key) {
                merge_retrieval_intent(existing, intent);
            } else {
                retrieval_intents.insert(key, intent.clone());
            }
        }
    }

    cleanup_artifact_processor_output(
        input,
        ArtifactProcessorOutput {
            pipeline_name: first.pipeline_name,
            pipeline_version: first.pipeline_version,
            provider_name: first.provider_name,
            model_name: first.model_name,
            prompt_version: first.prompt_version,
            usage: None,
            summary: SummaryOutput {
                title: summary_titles
                    .first()
                    .cloned()
                    .or_else(|| input.title.clone()),
                body_text: summary_bodies.join(" "),
                evidence_segment_ids: summary_evidence,
            },
            classifications: classifications.into_values().collect(),
            memories: memories.into_values().collect(),
            entities: entities.into_values().collect(),
            relationships: relationships.into_values().collect(),
            retrieval_intents: retrieval_intents.into_values().collect(),
            importance_score,
        },
    )
}

fn collected_output_evidence_segment_ids(output: &ArtifactProcessorOutput) -> HashSet<String> {
    let mut cited = HashSet::new();
    cited.extend(output.summary.evidence_segment_ids.iter().cloned());
    for classification in &output.classifications {
        cited.extend(classification.evidence_segment_ids.iter().cloned());
    }
    for memory in &output.memories {
        cited.extend(memory.evidence_segment_ids.iter().cloned());
    }
    for entity in &output.entities {
        cited.extend(entity.evidence_segment_ids.iter().cloned());
    }
    for relationship in &output.relationships {
        cited.extend(relationship.evidence_segment_ids.iter().cloned());
    }
    for intent in &output.retrieval_intents {
        cited.extend(intent.evidence_segment_ids.iter().cloned());
    }
    cited
}

fn compute_output_coverage_percent(
    input: &ArtifactProcessorInput,
    output: &ArtifactProcessorOutput,
) -> u8 {
    if input.segments.is_empty() {
        return 100;
    }
    let cited = collected_output_evidence_segment_ids(output);
    let total = input.segments.len();
    let covered = input
        .segments
        .iter()
        .filter(|segment| cited.contains(&segment.segment_id))
        .count();
    ((covered * 100) / total) as u8
}

fn uncovered_segments_for_output(
    input: &ArtifactProcessorInput,
    output: &ArtifactProcessorOutput,
) -> Vec<crate::storage::LoadedSegment> {
    let cited = collected_output_evidence_segment_ids(output);
    input
        .segments
        .iter()
        .filter(|segment| !cited.contains(&segment.segment_id))
        .cloned()
        .collect()
}

fn maybe_gap_fill_extraction_output(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    processor: &dyn crate::processor::ArtifactProcessor,
    input: &ArtifactProcessorInput,
    output: ArtifactProcessorOutput,
    policy: &ExtractionPolicy<'_>,
) -> std::result::Result<ArtifactProcessorOutput, String> {
    let chunking = policy.chunking;
    let coverage_policy = policy.coverage;
    if coverage_policy.max_gap_fill_passes == 0 || !should_shape_artifact_input(input) {
        return Ok(output);
    }

    let mut merged_output = output;
    for pass in 0..coverage_policy.max_gap_fill_passes {
        let coverage = compute_output_coverage_percent(input, &merged_output);
        if coverage >= coverage_policy.min_coverage_percent {
            break;
        }

        let uncovered_segments = uncovered_segments_for_output(input, &merged_output);
        if uncovered_segments.is_empty() {
            break;
        }

        let uncovered_windows =
            build_coverage_windows(&input.artifact_id, &uncovered_segments, &[], chunking);
        if uncovered_windows.is_empty() {
            break;
        }

        info!(
            "Extraction gap-fill for artifact {} pass {}/{} coverage={} uncovered_segments={} windows={}",
            input.artifact_id,
            pass + 1,
            coverage_policy.max_gap_fill_passes,
            coverage,
            uncovered_segments.len(),
            uncovered_windows.len()
        );

        let mut gap_outputs = Vec::new();
        for chunk_input in build_chunk_inputs(input, &uncovered_windows, chunking) {
            let chunk_output = processor.process(&chunk_input).map_err(|err| {
                handle_processor_error(job_store, worker_id, &claimed_job.job_id, err)
            })?;
            gap_outputs.push(chunk_output);
        }
        if gap_outputs.is_empty() {
            break;
        }

        let gap_output = merge_chunk_outputs(input, &gap_outputs);
        merged_output = merge_chunk_outputs(input, &[merged_output, gap_output]);
    }

    Ok(merged_output)
}

pub(crate) fn build_extract_chunk_inputs(
    processor_input: &ArtifactProcessorInput,
    payload: &ArtifactExtractPayload,
    chunking: &ExtractionChunkingConfig,
) -> Vec<ArtifactProcessorInput> {
    if !payload.topic_threads.is_empty() {
        build_topic_thread_inputs(
            processor_input,
            &payload.topic_threads,
            &payload.conversation_windows,
            chunking,
        )
    } else if payload.conversation_windows.len() > 1 {
        build_chunk_inputs(processor_input, &payload.conversation_windows, chunking)
    } else {
        vec![processor_input.clone()]
    }
}

pub(crate) fn build_reconciliation_input(
    artifact_id: &str,
    source_type: &str,
    extraction_result: &ArtifactExtractionResult,
) -> std::result::Result<ReconciliationProcessorInput, serde_json::Error> {
    build_reconciliation_input_from_outputs(
        artifact_id,
        crate::storage::SourceType::parse(source_type)
            .expect("validated source_type during payload parsing"),
        SummaryOutput {
            title: extraction_result.summary_title.clone(),
            body_text: extraction_result.summary_body_text.clone(),
            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
        },
        extraction_result
            .memories
            .iter()
            .map(|memory| MemoryOutput {
                candidate_key: if memory.candidate_key.is_empty() {
                    memory_candidate_key_from_fields(
                        &memory.memory_type,
                        memory.memory_scope,
                        &memory.memory_scope_value,
                        memory.title.as_deref(),
                        &memory.body_text,
                    )
                } else {
                    memory.candidate_key.clone()
                },
                title: memory.title.clone(),
                body_text: memory.body_text.clone(),
                memory_type: memory.memory_type.clone(),
                memory_scope: memory.memory_scope,
                memory_scope_value: memory.memory_scope_value.clone(),
                evidence_segment_ids: memory.evidence_segment_ids.clone(),
            })
            .collect(),
        extraction_result
            .entities
            .iter()
            .map(|entity| EntityOutput {
                entity_key: entity.entity_key.clone(),
                display_name: entity.display_name.clone(),
                entity_type: entity.entity_type.clone(),
                evidence_segment_ids: entity.evidence_segment_ids.clone(),
            })
            .collect(),
        extraction_result
            .relationships
            .iter()
            .map(|relationship| RelationshipOutput {
                relationship_type: relationship.relationship_type.clone(),
                subject_key: relationship.subject_key.clone(),
                object_key: relationship.object_key.clone(),
                title: relationship.title.clone(),
                body_text: relationship.body_text.clone(),
                confidence_label: relationship.confidence_label.clone(),
                evidence_segment_ids: relationship.evidence_segment_ids.clone(),
            })
            .collect(),
        "[]".to_string(),
    )
}

fn build_reconciliation_input_from_outputs(
    artifact_id: &str,
    source_type: crate::storage::SourceType,
    summary: SummaryOutput,
    memories: Vec<MemoryOutput>,
    entities: Vec<EntityOutput>,
    relationships: Vec<RelationshipOutput>,
    retrieval_results_json: String,
) -> std::result::Result<ReconciliationProcessorInput, serde_json::Error> {
    Ok(ReconciliationProcessorInput {
        artifact_id: artifact_id.to_string(),
        source_type,
        summary,
        memories,
        entities,
        relationships,
        retrieval_results_json,
    })
}

#[derive(Clone)]
enum ReconcileCandidate {
    Memory(MemoryOutput),
    Entity(EntityOutput),
    Relationship(RelationshipOutput),
}

impl ReconcileCandidate {
    fn target_kind(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Entity(_) => "entity",
            Self::Relationship(_) => "relationship",
        }
    }

    fn target_key(&self) -> String {
        match self {
            Self::Memory(memory) => memory.candidate_key.clone(),
            Self::Entity(entity) => entity.entity_key.clone(),
            Self::Relationship(relationship) => relationship_target_key_from_output(relationship),
        }
    }

    fn derived_object_type(&self) -> DerivedObjectType {
        match self {
            Self::Memory(_) => DerivedObjectType::Memory,
            Self::Entity(_) => DerivedObjectType::Entity,
            Self::Relationship(_) => DerivedObjectType::Relationship,
        }
    }

    fn title(&self) -> Option<String> {
        match self {
            Self::Memory(memory) => memory.title.clone(),
            Self::Entity(entity) => Some(entity.display_name.clone()),
            Self::Relationship(relationship) => relationship.title.clone(),
        }
    }

    fn body_text(&self) -> String {
        match self {
            Self::Memory(memory) => memory.body_text.clone(),
            Self::Entity(entity) => format!(
                "{} is a named {} mentioned in this artifact.",
                entity.display_name, entity.entity_type
            ),
            Self::Relationship(relationship) => relationship.body_text.clone(),
        }
    }

    fn evidence_segment_ids(&self) -> Vec<String> {
        match self {
            Self::Memory(memory) => memory.evidence_segment_ids.clone(),
            Self::Entity(entity) => entity.evidence_segment_ids.clone(),
            Self::Relationship(relationship) => relationship.evidence_segment_ids.clone(),
        }
    }
}

#[derive(Clone)]
struct PreparedReconciliationWork {
    deterministic_outputs: Vec<crate::processor::ReconciliationDecisionOutput>,
    ambiguous_input: Option<ReconciliationProcessorInput>,
}

#[derive(serde::Serialize)]
struct AmbiguousReconcileMatch {
    object_id: String,
    artifact_id: String,
    similarity_score: f32,
    candidate_key: Option<String>,
    title: Option<String>,
    body_text: Option<String>,
}

#[derive(serde::Serialize)]
struct AmbiguousReconcileCase {
    target_kind: String,
    target_key: String,
    embedding_matches: Vec<AmbiguousReconcileMatch>,
}

fn build_reconciliation_candidates(
    input: &ReconciliationProcessorInput,
) -> Vec<ReconcileCandidate> {
    input
        .memories
        .iter()
        .cloned()
        .map(ReconcileCandidate::Memory)
        .chain(
            input
                .entities
                .iter()
                .cloned()
                .map(ReconcileCandidate::Entity),
        )
        .chain(
            input
                .relationships
                .iter()
                .cloned()
                .map(ReconcileCandidate::Relationship),
        )
        .collect()
}

fn build_reconciliation_input_for_candidates(
    input: &ReconciliationProcessorInput,
    candidates: Vec<ReconcileCandidate>,
    retrieval_results_json: String,
) -> std::result::Result<ReconciliationProcessorInput, serde_json::Error> {
    let mut memories = Vec::new();
    let mut entities = Vec::new();
    let mut relationships = Vec::new();

    for candidate in candidates {
        match candidate {
            ReconcileCandidate::Memory(memory) => memories.push(memory),
            ReconcileCandidate::Entity(entity) => entities.push(entity),
            ReconcileCandidate::Relationship(relationship) => relationships.push(relationship),
        }
    }

    build_reconciliation_input_from_outputs(
        &input.artifact_id,
        input.source_type,
        input.summary.clone(),
        memories,
        entities,
        relationships,
        retrieval_results_json,
    )
}

fn create_new_output(
    candidate: &ReconcileCandidate,
) -> crate::processor::ReconciliationDecisionOutput {
    crate::processor::ReconciliationDecisionOutput {
        decision_kind: ReconciliationDecisionKind::CreateNew,
        target_kind: candidate.target_kind().to_string(),
        target_key: candidate.target_key(),
        matched_object_id: None,
        rationale: "No high-confidence existing archive match was found.".to_string(),
        evidence_segment_ids: candidate.evidence_segment_ids(),
    }
}

fn attach_existing_output(
    candidate: &ReconcileCandidate,
    matched_object_id: &str,
    rationale: String,
) -> crate::processor::ReconciliationDecisionOutput {
    crate::processor::ReconciliationDecisionOutput {
        decision_kind: ReconciliationDecisionKind::AttachToExisting,
        target_kind: candidate.target_kind().to_string(),
        target_key: candidate.target_key(),
        matched_object_id: Some(matched_object_id.to_string()),
        rationale,
        evidence_segment_ids: candidate.evidence_segment_ids(),
    }
}

fn prepare_reconciliation_work(
    input: &ReconciliationProcessorInput,
    cross_artifact_store: Option<&(dyn CrossArtifactReadStore + Send + Sync)>,
    embedding_provider: Option<&dyn EmbeddingProvider>,
) -> std::result::Result<PreparedReconciliationWork, String> {
    let candidates = build_reconciliation_candidates(input);
    if candidates.is_empty() {
        return Ok(PreparedReconciliationWork {
            deterministic_outputs: Vec::new(),
            ambiguous_input: None,
        });
    }

    let mut deterministic_outputs = Vec::new();
    let mut unresolved = candidates.clone();

    if let Some(cross_artifact_store) = cross_artifact_store {
        let candidate_keys: Vec<String> = unresolved
            .iter()
            .map(|candidate| candidate.target_key())
            .collect();
        let exact_matches = cross_artifact_store
            .find_related_by_candidate_keys(
                &input.artifact_id,
                &candidate_keys,
                candidate_keys.len().saturating_mul(4).max(8),
            )
            .map_err(|err| format!("failed to load exact cross-artifact matches: {err}"))?;
        let mut still_unresolved = Vec::new();
        for candidate in unresolved {
            let maybe_exact = exact_matches.iter().find(|existing| {
                existing.derived_object_type == candidate.derived_object_type()
                    && existing
                        .candidate_key
                        .as_deref()
                        .map(str::trim)
                        .unwrap_or_default()
                        == candidate.target_key()
            });
            if let Some(existing) = maybe_exact {
                deterministic_outputs.push(attach_existing_output(
                    &candidate,
                    &existing.derived_object_id,
                    "Deterministic exact candidate_key match against an existing active derived object."
                        .to_string(),
                ));
            } else {
                still_unresolved.push(candidate);
            }
        }
        unresolved = still_unresolved;
    }

    if unresolved.is_empty() {
        return Ok(PreparedReconciliationWork {
            deterministic_outputs,
            ambiguous_input: None,
        });
    }

    let Some(embedding_provider) = embedding_provider else {
        return Ok(PreparedReconciliationWork {
            deterministic_outputs,
            ambiguous_input: Some(
                build_reconciliation_input_for_candidates(
                    input,
                    unresolved,
                    input.retrieval_results_json.clone(),
                )
                .map_err(|err| format!("failed to build fallback reconciliation input: {err}"))?,
            ),
        });
    };
    let Some(cross_artifact_store) = cross_artifact_store else {
        return Ok(PreparedReconciliationWork {
            deterministic_outputs,
            ambiguous_input: Some(
                build_reconciliation_input_for_candidates(
                    input,
                    unresolved,
                    input.retrieval_results_json.clone(),
                )
                .map_err(|err| format!("failed to build fallback reconciliation input: {err}"))?,
            ),
        });
    };

    let embedding_texts: Vec<String> = unresolved
        .iter()
        .map(|candidate| match candidate.title() {
            Some(title) if !title.trim().is_empty() => {
                format!("{title}\n\n{}", candidate.body_text())
            }
            _ => candidate.body_text(),
        })
        .collect();
    let vectors = embedding_provider
        .embed_texts(&embedding_texts)
        .map_err(|err| format!("failed to embed reconciliation candidates: {err}"))?;

    let mut ambiguous_candidates = Vec::new();
    let mut ambiguous_cases = Vec::new();

    for (candidate, vector) in unresolved.into_iter().zip(vectors.into_iter()) {
        let matches = cross_artifact_store
            .find_related_by_embedding(
                &input.artifact_id,
                candidate.derived_object_type(),
                &vector,
                RECONCILE_EMBEDDING_LOOKUP_LIMIT,
            )
            .map_err(|err| format!("failed to load embedding reconciliation matches: {err}"))?;
        let Some(top_match) = matches.first() else {
            deterministic_outputs.push(create_new_output(&candidate));
            continue;
        };
        if top_match.similarity_score >= RECONCILE_EMBEDDING_DETERMINISTIC_THRESHOLD {
            deterministic_outputs.push(attach_existing_output(
                &candidate,
                &top_match.derived_object_id,
                format!(
                    "Deterministic embedding match against an existing active derived object (similarity {:.3}).",
                    top_match.similarity_score
                ),
            ));
            continue;
        }
        if top_match.similarity_score < RECONCILE_EMBEDDING_AMBIGUOUS_THRESHOLD {
            deterministic_outputs.push(create_new_output(&candidate));
            continue;
        }

        ambiguous_cases.push(AmbiguousReconcileCase {
            target_kind: candidate.target_kind().to_string(),
            target_key: candidate.target_key(),
            embedding_matches: matches
                .iter()
                .map(|matched| AmbiguousReconcileMatch {
                    object_id: matched.derived_object_id.clone(),
                    artifact_id: matched.artifact_id.clone(),
                    similarity_score: matched.similarity_score,
                    candidate_key: matched.candidate_key.clone(),
                    title: matched.title.clone(),
                    body_text: matched.body_text.clone(),
                })
                .collect(),
        });

        ambiguous_candidates.push(candidate);
    }

    let ambiguous_input = if ambiguous_candidates.is_empty() {
        None
    } else {
        Some(
            build_reconciliation_input_for_candidates(
                input,
                ambiguous_candidates,
                serde_json::to_string_pretty(&ambiguous_cases).map_err(|err| {
                    format!("failed to serialize ambiguous reconcile cases: {err}")
                })?,
            )
            .map_err(|err| format!("failed to build ambiguous reconciliation input: {err}"))?,
        )
    };

    Ok(PreparedReconciliationWork {
        deterministic_outputs,
        ambiguous_input,
    })
}

pub(crate) fn build_reconciliation_decisions(
    claimed_job: &ClaimedJob,
    extraction_result: &ArtifactExtractionResult,
    outputs: Vec<crate::processor::ReconciliationDecisionOutput>,
) -> Vec<ReconciliationDecision> {
    outputs
        .into_iter()
        .map(|output| ReconciliationDecision {
            reconciliation_decision_id: new_id("reconcile"),
            artifact_id: extraction_result.artifact_id.clone(),
            job_id: claimed_job.job_id.clone(),
            extraction_result_id: extraction_result.extraction_result_id.clone(),
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

pub(crate) fn memory_target_key(memory: &ExtractedMemory) -> String {
    if memory.candidate_key.is_empty() {
        memory_candidate_key_from_fields(
            &memory.memory_type,
            memory.memory_scope,
            &memory.memory_scope_value,
            memory.title.as_deref(),
            &memory.body_text,
        )
    } else {
        memory.candidate_key.clone()
    }
}

fn memory_target_key_from_output(memory: &MemoryOutput) -> String {
    memory.candidate_key.clone()
}

pub(crate) fn relationship_target_key(relationship: &CandidateRelationship) -> String {
    format!(
        "{}:{}:{}",
        relationship.relationship_type, relationship.subject_key, relationship.object_key
    )
}

fn relationship_target_key_from_output(relationship: &RelationshipOutput) -> String {
    format!(
        "{}:{}:{}",
        relationship.relationship_type, relationship.subject_key, relationship.object_key
    )
}

fn entity_target_key(entity: &CandidateEntity) -> String {
    entity.entity_key.clone()
}

fn extend_unique(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn merge_classification_output(target: &mut ClassificationOutput, incoming: &ClassificationOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    target.body_text =
        prefer_richer_optional_text(target.body_text.take(), incoming.body_text.clone());
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_memory_output(target: &mut MemoryOutput, incoming: &MemoryOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_entity_output(target: &mut EntityOutput, incoming: &EntityOutput) {
    if incoming.display_name.len() > target.display_name.len() {
        target.display_name = incoming.display_name.clone();
    }
    if incoming.entity_type.len() > target.entity_type.len() {
        target.entity_type = incoming.entity_type.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_relationship_output(target: &mut RelationshipOutput, incoming: &RelationshipOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    if confidence_rank(&incoming.confidence_label) > confidence_rank(&target.confidence_label) {
        target.confidence_label = incoming.confidence_label.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_retrieval_intent(target: &mut RetrievalIntent, incoming: &RetrievalIntent) {
    if incoming.question.len() > target.question.len() {
        target.question = incoming.question.clone();
    }
    if incoming.query_text.len() > target.query_text.len() {
        target.query_text = incoming.query_text.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn prefer_richer_optional_text(
    current: Option<String>,
    incoming: Option<String>,
) -> Option<String> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => {
            if incoming.len() > current.len() {
                Some(incoming)
            } else {
                Some(current)
            }
        }
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    }
}

fn confidence_rank(label: &str) -> i32 {
    match label {
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

pub(crate) fn build_derivation_attempt(
    claimed_job: &ClaimedJob,
    artifact_id: &str,
    extraction_result: &ArtifactExtractionResult,
    decisions: &[ReconciliationDecision],
) -> WriteDerivationAttempt {
    let derivation_run_id = new_id("drvrun");
    let started_at = SourceTimestamp::from(chrono::Utc::now());
    let completed_at = started_at.clone();
    let mut objects = Vec::with_capacity(
        1 + extraction_result.classifications.len()
            + extraction_result.memories.len()
            + extraction_result.entities.len(),
    );

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

    let mut attached_existing = HashSet::new();
    for memory in &extraction_result.memories {
        let decision = decisions.iter().find(|decision| {
            decision.target_kind == "memory" && decision.target_key == memory_target_key(memory)
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
        let supersedes_derived_object_id = decision.and_then(|decision| {
            matches!(
                decision.decision_kind,
                ReconciliationDecisionKind::SupersedeExisting
            )
            .then(|| decision.matched_object_id.clone())
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
                        candidate_key: memory_target_key(memory),
                        memory_type: memory.memory_type.clone(),
                        memory_scope: memory.memory_scope,
                        memory_scope_value: memory.memory_scope_value.clone(),
                    },
                },
            },
            evidence_links: build_evidence_links(&derived_object_id, &memory.evidence_segment_ids),
        });
    }

    for entity in &extraction_result.entities {
        let decision = decisions.iter().find(|decision| {
            decision.target_kind == "entity" && decision.target_key == entity_target_key(entity)
        });
        if let Some(decision) = decision {
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
        }

        let derived_object_id = new_id("dobj");
        let supersedes_derived_object_id = decision.and_then(|decision| {
            matches!(
                decision.decision_kind,
                ReconciliationDecisionKind::SupersedeExisting
            )
            .then(|| decision.matched_object_id.clone())
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
                scope_type: ScopeType::Artifact,
                scope_id: artifact_id.to_string(),
                supersedes_derived_object_id,
                payload: DerivedObjectPayload::Entity {
                    title: Some(entity.display_name.clone()),
                    body_text: format!(
                        "{} is a named {} mentioned in this artifact.",
                        entity.display_name, entity.entity_type
                    ),
                    object_json: EntityObjectJson {
                        entity_type: entity.entity_type.clone(),
                        candidate_key: entity_target_key(entity),
                    },
                },
            },
            evidence_links: build_evidence_links(&derived_object_id, &entity.evidence_segment_ids),
        });
    }

    for relationship in &extraction_result.relationships {
        let decision = decisions.iter().find(|decision| {
            decision.target_kind == "relationship"
                && decision.target_key == relationship_target_key(relationship)
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
    let archive_links = build_reconciliation_archive_links(
        &summary_object_id,
        decisions,
        "artifact_reconciliation",
    );

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
        archive_links,
    }
}

fn build_reconciliation_archive_links(
    summary_object_id: &str,
    decisions: &[ReconciliationDecision],
    contributed_by: &str,
) -> Vec<NewArchiveLink> {
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for decision in decisions {
        if !matches!(
            decision.decision_kind,
            ReconciliationDecisionKind::AttachToExisting
                | ReconciliationDecisionKind::StrengthenExisting
        ) {
            continue;
        }

        let Some(target_object_id) = decision.matched_object_id.as_ref() else {
            continue;
        };

        let edge = (
            summary_object_id.to_string(),
            target_object_id.clone(),
            "refers_to".to_string(),
        );
        if !seen.insert(edge.clone()) {
            continue;
        }

        links.push(NewArchiveLink {
            archive_link_id: new_id("alink"),
            source_object_id: edge.0,
            target_object_id: edge.1,
            link_type: edge.2,
            confidence_score: None,
            rationale: Some(decision.rationale.clone()),
            contributed_by: Some(contributed_by.to_string()),
        });
    }

    links
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::StubEmbeddingProvider;
    use crate::error::StorageResult;
    use crate::storage::{RelatedDerivedObject, RelatedDerivedObjectEmbeddingMatch, SourceType};

    struct MockCrossArtifactReadStore {
        exact_matches: Vec<RelatedDerivedObject>,
        embedding_matches: Vec<RelatedDerivedObjectEmbeddingMatch>,
    }

    impl CrossArtifactReadStore for MockCrossArtifactReadStore {
        fn find_related_by_candidate_keys(
            &self,
            _artifact_id: &str,
            _candidate_keys: &[String],
            _limit: usize,
        ) -> StorageResult<Vec<RelatedDerivedObject>> {
            Ok(self.exact_matches.clone())
        }

        fn find_related_by_embedding(
            &self,
            _artifact_id: &str,
            _derived_object_type: DerivedObjectType,
            _query_embedding: &[f32],
            _limit: usize,
        ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>> {
            Ok(self.embedding_matches.clone())
        }
    }

    fn sample_reconciliation_input() -> ReconciliationProcessorInput {
        ReconciliationProcessorInput {
            artifact_id: "artifact-1".to_string(),
            source_type: SourceType::TextFile,
            summary: SummaryOutput {
                title: Some("Summary".to_string()),
                body_text: "Summary body".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            memories: vec![MemoryOutput {
                candidate_key: "memory-key".to_string(),
                title: Some("Memory".to_string()),
                body_text: "Memory body".to_string(),
                memory_type: "project_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: "artifact-1".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            }],
            entities: Vec::new(),
            relationships: Vec::new(),
            retrieval_results_json: "[]".to_string(),
        }
    }

    #[test]
    fn prepare_reconciliation_work_attaches_exact_candidate_key_matches() {
        let input = sample_reconciliation_input();
        let cross_store = MockCrossArtifactReadStore {
            exact_matches: vec![RelatedDerivedObject {
                derived_object_id: "dobj-9".to_string(),
                artifact_id: "artifact-2".to_string(),
                derived_object_type: DerivedObjectType::Memory,
                title: Some("Existing".to_string()),
                body_text: Some("Existing body".to_string()),
                candidate_key: Some("memory-key".to_string()),
                confidence_score: None,
            }],
            embedding_matches: Vec::new(),
        };

        let prepared = prepare_reconciliation_work(&input, Some(&cross_store), None)
            .expect("prepare work should succeed");

        assert_eq!(prepared.deterministic_outputs.len(), 1);
        assert!(prepared.ambiguous_input.is_none());
        let output = &prepared.deterministic_outputs[0];
        assert_eq!(
            output.decision_kind,
            ReconciliationDecisionKind::AttachToExisting
        );
        assert_eq!(output.target_key, "memory-key");
        assert_eq!(output.matched_object_id.as_deref(), Some("dobj-9"));
    }

    #[test]
    fn prepare_reconciliation_work_limits_model_fallback_to_ambiguous_embedding_matches() {
        let input = sample_reconciliation_input();
        let cross_store = MockCrossArtifactReadStore {
            exact_matches: Vec::new(),
            embedding_matches: vec![RelatedDerivedObjectEmbeddingMatch {
                derived_object_id: "dobj-7".to_string(),
                artifact_id: "artifact-2".to_string(),
                derived_object_type: DerivedObjectType::Memory,
                title: Some("Possible match".to_string()),
                body_text: Some("Possible body".to_string()),
                candidate_key: Some("other-key".to_string()),
                confidence_score: None,
                similarity_score: 0.82,
            }],
        };
        let embedding_provider = StubEmbeddingProvider::new("stub".to_string(), 8);

        let prepared =
            prepare_reconciliation_work(&input, Some(&cross_store), Some(&embedding_provider))
                .expect("prepare work should succeed");

        assert!(prepared.deterministic_outputs.is_empty());
        let ambiguous_input = prepared
            .ambiguous_input
            .expect("ambiguous candidate should remain for model fallback");
        assert_eq!(ambiguous_input.memories.len(), 1);
        assert!(ambiguous_input.entities.is_empty());
        assert!(ambiguous_input.relationships.is_empty());
        assert!(ambiguous_input
            .retrieval_results_json
            .contains("\"object_id\": \"dobj-7\""));
    }
}

pub(crate) fn build_evidence_links(
    derived_object_id: &str,
    segment_ids: &[String],
) -> Vec<NewEvidenceLink> {
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

fn fail_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    context: &str,
    err: impl std::fmt::Display,
) -> String {
    fail_job_message(job_store, worker_id, job_id, format!("{context}: {err}"))
}

fn handle_processor_error(
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

pub(crate) fn complete_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
) -> std::result::Result<(), String> {
    job_store
        .complete_job(worker_id, job_id)
        .map_err(|err| format!("failed to complete job {}: {}", job_id, err))
}

fn fail_job_message(
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

fn mark_job_retryable_message(
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

pub(crate) fn new_id(prefix: &str) -> String {
    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let entropy = random::<u64>();
    format!("{prefix}-{nanos:x}-{counter:x}-{entropy:x}")
}

/// Start enrichment worker pool with shutdown capability.
pub fn start_enrichment_workers(
    config: &crate::config::HttpConfig,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    _retrieval_service: Arc<dyn crate::app::retrieval::ArchiveRetrievalServiceApi>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    start_enrichment_workers_with_factory(
        WorkerStartConfig {
            http: config,
            shutdown,
            processor_factory: Arc::new(StubProcessorFactory),
        },
        EnrichmentPipelineResources {
            job_store,
            read_store,
            state_store,
            derived_store,
            cross_artifact_store: None,
            embedding_store: None,
            embedding_provider: None,
        },
    )
}

#[doc(hidden)]
pub fn start_enrichment_workers_with_factory(
    config: WorkerStartConfig<'_>,
    resources: EnrichmentPipelineResources,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let WorkerStartConfig {
        http,
        shutdown,
        processor_factory,
    } = config;
    let EnrichmentPipelineResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store: _,
        embedding_store: _,
        embedding_provider: _,
    } = resources;
    let pid = std::process::id();
    let poll_interval = Duration::from_millis(http.enrichment_poll_interval_ms);

    let workers: Vec<_> = (0..http.enrichment_worker_count)
        .map(|worker_index| {
            let worker_id = format_worker_id(pid, worker_index);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let state_store = Arc::clone(&state_store);
            let derived_store = Arc::clone(&derived_store);
            let shutdown = shutdown.clone();
            let processor_factory = Arc::clone(&processor_factory);
            let chunking = Arc::new(ExtractionChunkingConfig {
                max_segments_per_chunk: 20,
                chunk_overlap_segments: 4,
                max_chars_per_chunk: 25_000,
            });
            let coverage_policy = Arc::new(ExtractCoveragePolicy {
                min_coverage_percent: 60,
                max_gap_fill_passes: 1,
            });

            thread::Builder::new()
                .name(format!("enrichment-worker-{}", worker_index))
                .spawn(move || {
                    enrichment_worker(
                        worker_id,
                        WorkerResources {
                            job_store,
                            read_store,
                            state_store,
                            derived_store,
                            cross_artifact_store: None,
                            embedding_store: None,
                            embedding_provider: None,
                            processor_factory,
                        },
                        chunking,
                        coverage_policy,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|source| WorkerError::SpawnThread {
                    worker_kind: format!("enrichment worker {}", worker_index),
                    source: Box::new(source),
                })
        })
        .collect::<WorkerResult<Vec<_>>>()?;

    Ok(workers)
}

// ---------------------------------------------------------------------------
// Per-stage pipeline startup (replaces uniform worker pool)
// ---------------------------------------------------------------------------

/// Start the enrichment pipeline with per-stage workers.
///
/// Spawns:
/// - batch mode:
///   - extract poller threads
///   - reconcile poller threads
/// - direct mode:
///   - direct extract/reconcile worker threads
pub fn start_enrichment_pipeline(
    config: &EnrichmentPipelineConfig,
    execution_mode: InferenceExecutionMode,
    resources: EnrichmentPipelineResources,
    shutdown: ShutdownToken,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let EnrichmentPipelineResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store,
        embedding_store,
        embedding_provider,
    } = resources;
    if execution_mode == InferenceExecutionMode::Batch {
        validate_batch_execution_support(processor_factory.as_ref())?;
    }
    let pid = std::process::id();
    let chunking = Arc::new(config.chunking.clone());
    let coverage_policy = Arc::new(ExtractCoveragePolicy {
        min_coverage_percent: config.extract_min_coverage_percent,
        max_gap_fill_passes: config.extract_max_gap_fill_passes,
    });
    let poll_interval = config.poll_interval;
    let direct_resources = Arc::new(WorkerResources {
        job_store: Arc::clone(&job_store),
        read_store: Arc::clone(&read_store),
        state_store: Arc::clone(&state_store),
        derived_store: Arc::clone(&derived_store),
        cross_artifact_store: cross_artifact_store.clone(),
        embedding_store: embedding_store.clone(),
        embedding_provider: embedding_provider.clone(),
        processor_factory: Arc::clone(&processor_factory),
    });
    let mut handles = Vec::new();

    match execution_mode {
        InferenceExecutionMode::Batch => {
            let mode = &config.batch;
            handles.extend(spawn_extract_batch_workers(BatchWorkerConfig {
                pid,
                worker_count: mode.extract_workers,
                job_store: Arc::clone(&job_store),
                read_store: Arc::clone(&read_store),
                state_store: Arc::clone(&state_store),
                derived_store: Arc::clone(&derived_store),
                cross_artifact_store: cross_artifact_store.clone(),
                embedding_store: embedding_store.clone(),
                embedding_provider: embedding_provider.clone(),
                processor_factory: Arc::clone(&processor_factory),
                chunking: Some(Arc::clone(&chunking)),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
            handles.extend(spawn_reconcile_batch_workers(BatchWorkerConfig {
                pid,
                worker_count: mode.reconcile_workers,
                job_store: Arc::clone(&job_store),
                read_store: Arc::clone(&read_store),
                state_store: Arc::clone(&state_store),
                derived_store: Arc::clone(&derived_store),
                cross_artifact_store: cross_artifact_store.clone(),
                embedding_store: embedding_store.clone(),
                embedding_provider: embedding_provider.clone(),
                processor_factory: Arc::clone(&processor_factory),
                chunking: Some(Arc::clone(&chunking)),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
        }
        InferenceExecutionMode::Direct => {
            let mode = &config.direct;
            handles.extend(spawn_direct_stage_workers(DirectStageWorkerConfig {
                pid,
                stage_name: "extract",
                job_type: JobType::ArtifactExtract,
                execution_mode: InferenceExecutionMode::Direct,
                worker_count: mode.extract_workers,
                resources: Arc::clone(&direct_resources),
                chunking: Arc::clone(&chunking),
                coverage_policy: Arc::clone(&coverage_policy),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
            handles.extend(spawn_direct_stage_workers(DirectStageWorkerConfig {
                pid,
                stage_name: "reconcile",
                job_type: JobType::ArtifactReconcile,
                execution_mode: InferenceExecutionMode::Direct,
                worker_count: mode.reconcile_workers,
                resources: Arc::clone(&direct_resources),
                chunking: Arc::clone(&chunking),
                coverage_policy: Arc::clone(&coverage_policy),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
        }
    }

    let (embedding_workers, extract_workers, reconcile_workers) = match execution_mode {
        InferenceExecutionMode::Batch => (
            config.batch.embedding_workers,
            config.batch.extract_workers,
            config.batch.reconcile_workers,
        ),
        InferenceExecutionMode::Direct => (
            config.direct.embedding_workers,
            config.direct.extract_workers,
            config.direct.reconcile_workers,
        ),
    };

    if embedding_workers > 0 {
        if let (Some(embedding_store), Some(embedding_provider)) =
            (embedding_store, embedding_provider)
        {
            handles.extend(spawn_embedding_workers(
                pid,
                embedding_workers,
                Arc::clone(&job_store),
                embedding_store,
                embedding_provider,
                poll_interval,
                shutdown.clone(),
            )?);
        } else {
            info!("Embedding workers configured but embedding provider/store unavailable");
        }
    }

    info!(
        "Enrichment pipeline started: mode={:?}, extract_workers={}, reconcile_workers={}, embedding_workers={}, chunk_segments={}, chunk_overlap={}, chunk_max_chars={}",
        execution_mode,
        extract_workers,
        reconcile_workers,
        embedding_workers,
        config.chunking.max_segments_per_chunk,
        config.chunking.chunk_overlap_segments,
        config.chunking.max_chars_per_chunk
    );
    Ok(handles)
}

fn validate_batch_execution_support(
    processor_factory: &dyn ArtifactProcessorFactory,
) -> WorkerResult<()> {
    use crate::storage::EnrichmentTier;

    if processor_factory
        .build_batch_processor(EnrichmentTier::Default)?
        .is_none()
    {
        return Err(WorkerError::MissingBatchProcessor {
            stage: "extraction",
        });
    }
    if processor_factory
        .build_reconciliation_batch_processor(EnrichmentTier::Default)?
        .is_none()
    {
        return Err(WorkerError::MissingBatchProcessor {
            stage: "reconciliation",
        });
    }
    Ok(())
}

struct BatchWorkerConfig {
    pid: u32,
    worker_count: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    embedding_store: Option<Arc<dyn DerivedObjectEmbeddingStore>>,
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    chunking: Option<Arc<ExtractionChunkingConfig>>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
}

fn spawn_reconcile_batch_workers(
    config: BatchWorkerConfig,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let BatchWorkerConfig {
        pid,
        worker_count,
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store,
        embedding_store,
        embedding_provider,
        processor_factory,
        chunking,
        poll_interval,
        shutdown,
    } = config;
    let chunking = chunking.expect("reconcile batch workers require chunking placeholder");
    let coverage_policy = Arc::new(ExtractCoveragePolicy {
        min_coverage_percent: 60,
        max_gap_fill_passes: 1,
    });
    let resources = Arc::new(WorkerResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store,
        embedding_store,
        embedding_provider,
        processor_factory,
    });
    spawn_direct_stage_workers(DirectStageWorkerConfig {
        pid,
        stage_name: "reconcile",
        job_type: JobType::ArtifactReconcile,
        execution_mode: InferenceExecutionMode::Batch,
        worker_count,
        resources,
        chunking,
        coverage_policy,
        poll_interval,
        shutdown,
    })
}

fn spawn_extract_batch_workers(
    config: BatchWorkerConfig,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let BatchWorkerConfig {
        pid,
        worker_count,
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store: _,
        embedding_store: _,
        embedding_provider: _,
        processor_factory,
        chunking,
        poll_interval,
        shutdown,
    } = config;
    let chunking = chunking.expect("extract batch workers require chunking");
    let coverage_policy = Arc::new(ExtractCoveragePolicy {
        min_coverage_percent: 60,
        max_gap_fill_passes: 1,
    });
    let resources = Arc::new(WorkerResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store: None,
        embedding_store: None,
        embedding_provider: None,
        processor_factory,
    });
    spawn_direct_stage_workers(DirectStageWorkerConfig {
        pid,
        stage_name: "extract",
        job_type: JobType::ArtifactExtract,
        execution_mode: InferenceExecutionMode::Batch,
        worker_count,
        resources,
        chunking,
        coverage_policy,
        poll_interval,
        shutdown,
    })
}

struct DirectStageWorkerConfig {
    pid: u32,
    stage_name: &'static str,
    job_type: JobType,
    execution_mode: InferenceExecutionMode,
    worker_count: usize,
    resources: Arc<WorkerResources>,
    chunking: Arc<ExtractionChunkingConfig>,
    coverage_policy: Arc<ExtractCoveragePolicy>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
}

fn spawn_direct_stage_workers(
    cfg: DirectStageWorkerConfig,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    (0..cfg.worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:{}:{}", cfg.pid, cfg.stage_name, i);
            let resources = Arc::clone(&cfg.resources);
            let chunking = Arc::clone(&cfg.chunking);
            let coverage_policy = Arc::clone(&cfg.coverage_policy);
            let shutdown = cfg.shutdown.clone();
            thread::Builder::new()
                .name(format!("direct-{}-{}", cfg.stage_name, i))
                .spawn(move || {
                    let ctx = resources.as_context();
                    let policy = ExtractionPolicy {
                        chunking: chunking.as_ref(),
                        coverage: coverage_policy.as_ref(),
                    };
                    direct_stage_worker_loop(
                        worker_id,
                        cfg.job_type,
                        cfg.execution_mode,
                        &ctx,
                        &policy,
                        cfg.poll_interval,
                        shutdown,
                    );
                })
                .map_err(|source| WorkerError::SpawnThread {
                    worker_kind: format!("direct {} worker {}", cfg.stage_name, i),
                    source: Box::new(source),
                })
        })
        .collect()
}

fn direct_stage_worker_loop(
    worker_id: String,
    job_type: JobType,
    execution_mode: InferenceExecutionMode,
    ctx: &WorkerContext<'_>,
    policy: &ExtractionPolicy<'_>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!(
        "Direct stage worker {} starting for {:?}",
        worker_id, job_type
    );

    loop {
        if shutdown.is_shutdown() {
            info!("Direct stage worker {} shutting down", worker_id);
            break;
        }

        match ctx.job_store.claim_jobs_by_type(
            &worker_id,
            job_type,
            Some(EnrichmentTier::Default),
            1,
        ) {
            Ok(jobs) if !jobs.is_empty() => {
                let job = jobs.into_iter().next().expect("one claimed job");
                debug!(
                    "Direct stage worker {} claimed {} job {}",
                    worker_id,
                    job.job_type.as_str(),
                    job.job_id
                );
                if let Err(err) = process_claimed_jobs(&worker_id, job, ctx, execution_mode, policy)
                {
                    error!(
                        "Direct stage worker {} failed to process stage job: {}",
                        worker_id, err
                    );
                }
            }
            Ok(_) => thread::sleep(poll_interval),
            Err(err) => {
                error!(
                    "Direct stage worker {} failed to claim job: {}",
                    worker_id, err
                );
                thread::sleep(poll_interval);
            }
        }
    }
}

fn spawn_embedding_workers(
    pid: u32,
    worker_count: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    embedding_store: Arc<dyn DerivedObjectEmbeddingStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    (0..worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:embed:{}", pid, i);
            let job_store = Arc::clone(&job_store);
            let embedding_store = Arc::clone(&embedding_store);
            let embedding_provider = Arc::clone(&embedding_provider);
            let shutdown = shutdown.clone();

            thread::Builder::new()
                .name(format!("embed-{}", i))
                .spawn(move || {
                    embedding_worker_loop(
                        worker_id,
                        job_store.as_ref(),
                        embedding_store.as_ref(),
                        embedding_provider.as_ref(),
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|source| WorkerError::SpawnThread {
                    worker_kind: format!("embedding worker {}", i),
                    source: Box::new(source),
                })
        })
        .collect()
}

fn embedding_worker_loop(
    worker_id: String,
    job_store: &dyn EnrichmentJobLifecycleStore,
    embedding_store: &dyn DerivedObjectEmbeddingStore,
    embedding_provider: &dyn EmbeddingProvider,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!("Embedding worker {} starting", worker_id);

    loop {
        if shutdown.is_shutdown() {
            info!("Embedding worker {} shutting down", worker_id);
            break;
        }

        match job_store.claim_jobs_by_type(
            &worker_id,
            JobType::DerivedObjectEmbed,
            Some(EnrichmentTier::Default),
            1,
        ) {
            Ok(jobs) if !jobs.is_empty() => {
                let job = jobs.into_iter().next().expect("one claimed job");
                if let Err(err) = process_embedding_job(
                    &worker_id,
                    &job,
                    job_store,
                    Some(embedding_store),
                    Some(embedding_provider),
                ) {
                    error!(
                        "Embedding worker {} failed to process job: {}",
                        worker_id, err
                    );
                }
            }
            Ok(_) => thread::sleep(poll_interval),
            Err(err) => {
                error!(
                    "Embedding worker {} failed to claim job: {}",
                    worker_id, err
                );
                thread::sleep(poll_interval);
            }
        }
    }
}
