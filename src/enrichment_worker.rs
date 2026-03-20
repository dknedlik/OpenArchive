use crate::config::{EnrichmentPipelineConfig, ExtractionChunkingConfig, InferenceExecutionMode};
use crate::domain::SourceTimestamp;
use crate::extraction_chunking::{
    build_chunk_inputs, build_preprocess_coverage_windows, build_topic_thread_inputs,
};
use crate::processor::{
    ArtifactProcessorFactory, ArtifactProcessorInput, ArtifactProcessorOutput,
    ClassificationOutput, EntityOutput, MemoryOutput, PreprocessProcessorInput, ProcessorError,
    ReconciliationProcessorInput, RelationshipOutput, StubProcessorFactory, SummaryOutput,
    cleanup_artifact_processor_output, memory_candidate_key_from_fields,
    should_shape_conversation_input,
};
use crate::rate_limiter::RateLimiter;
use crate::shutdown::ShutdownToken;
use crate::stage_poller::{stage_poller_loop, ExtractStage, PreprocessStage, ReconcileStage};
use crate::storage::JobType;
use crate::storage::{
    ArchiveRetrievalStore, ArtifactExtractPayload, ArtifactExtractionResult,
    ArtifactPreprocessPayload, ArtifactReadStore, ArtifactReconcilePayload,
    ArtifactRetrieveContextPayload, CandidateEntity, CandidateRelationship, ClaimedJob,
    ClassificationObjectJson, ConversationWindowRef, DerivationRunStatus, DerivationRunType,
    DerivedMetadataWriteStore, DerivedObjectPayload, EnrichmentJobLifecycleStore,
    EnrichmentStateStore, EnrichmentTier, EvidenceRole, ExtractedClassification, ExtractedMemory,
    InputScopeType, MemoryObjectJson, NewDerivationRun, NewDerivedObject, NewEnrichmentJob,
    NewEvidenceLink, ObjectStatus, OriginKind, ReconciliationDecision, ReconciliationDecisionKind,
    RelationshipObjectJson, RetrievalIntent, RetrievalResultSet, ScopeType, SummaryObjectJson,
    SupportStrength, WriteDerivationAttempt, WriteDerivedObject,
};
use anyhow::Result;
use log::{debug, error, info};
use rand::random;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RETRYABLE_INFERENCE_BACKOFF_SECONDS: i64 = 60;

#[derive(Clone)]
struct ExtractCoveragePolicy {
    min_coverage_percent: u8,
    max_gap_fill_passes: usize,
}
/// Worker ID format: enrichment:<pid>:<worker_index>
pub fn format_worker_id(pid: u32, worker_index: usize) -> String {
    format!("enrichment:{}:{}", pid, worker_index)
}

fn enrichment_worker(
    worker_id: String,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
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

        match job_store.claim_next_job(&worker_id) {
            Ok(Some(claimed_job)) => {
                debug!("Worker {} claimed job {}", worker_id, claimed_job.job_id);
                if let Err(err) = process_claimed_jobs(
                    &worker_id,
                    claimed_job,
                    job_store.as_ref(),
                    read_store.as_ref(),
                    retrieval_store.as_ref(),
                    state_store.as_ref(),
                    derived_store.as_ref(),
                    processor_factory.as_ref(),
                    InferenceExecutionMode::Direct,
                    chunking.as_ref(),
                    coverage_policy.as_ref(),
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
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    retrieval_store: &dyn ArchiveRetrievalStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    execution_mode: InferenceExecutionMode,
    chunking: &ExtractionChunkingConfig,
    coverage_policy: &ExtractCoveragePolicy,
) -> std::result::Result<(), String> {
    if execution_mode == InferenceExecutionMode::Batch {
        match first_job.job_type {
            crate::storage::JobType::ArtifactPreprocess => {
                if let Some(batch_processor) = processor_factory
                    .build_preprocess_batch_processor(first_job.enrichment_tier)
                    .map_err(|err| {
                        fail_job(
                            job_store,
                            worker_id,
                            &first_job.job_id,
                            "Failed to build preprocess batch processor",
                            err,
                        )
                    })?
                {
                    let mut jobs = vec![first_job];
                    let additional = job_store
                        .claim_matching_jobs(
                            worker_id,
                            &jobs[0],
                            batch_processor.max_batch_jobs().saturating_sub(1),
                        )
                        .map_err(|err| {
                            format!("failed to claim matching preprocess jobs: {err}")
                        })?;
                    jobs.extend(additional);
                    return process_preprocess_job_batch(
                        worker_id,
                        jobs,
                        job_store,
                        read_store,
                        processor_factory,
                        batch_processor.as_ref(),
                        chunking,
                    );
                }
            }
            crate::storage::JobType::ArtifactExtract => {
                if let Some(batch_processor) = processor_factory
                    .build_batch_processor(first_job.enrichment_tier)
                    .map_err(|err| {
                        fail_job(
                            job_store,
                            worker_id,
                            &first_job.job_id,
                            "Failed to build extraction batch processor",
                            err,
                        )
                    })?
                {
                    let mut jobs = vec![first_job];
                    let additional = job_store
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
                        job_store,
                        read_store,
                        state_store,
                        processor_factory,
                        batch_processor.as_ref(),
                        chunking,
                        coverage_policy,
                    );
                }
            }
            crate::storage::JobType::ArtifactReconcile => {
                if let Some(batch_processor) = processor_factory
                    .build_reconciliation_batch_processor(first_job.enrichment_tier)
                    .map_err(|err| {
                        fail_job(
                            job_store,
                            worker_id,
                            &first_job.job_id,
                            "Failed to build reconciliation batch processor",
                            err,
                        )
                    })?
                {
                    let mut jobs = vec![first_job];
                    let additional = job_store
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
                        job_store,
                        read_store,
                        state_store,
                        derived_store,
                        processor_factory,
                        batch_processor.as_ref(),
                    );
                }
            }
            _ => {}
        }
    }

    process_claimed_job(
        worker_id,
        first_job,
        job_store,
        read_store,
        retrieval_store,
        state_store,
        derived_store,
        processor_factory,
        chunking,
        coverage_policy,
    )
}

fn process_claimed_job(
    worker_id: &str,
    claimed_job: ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    retrieval_store: &dyn ArchiveRetrievalStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    chunking: &ExtractionChunkingConfig,
    coverage_policy: &ExtractCoveragePolicy,
) -> std::result::Result<(), String> {
    match claimed_job.job_type {
        crate::storage::JobType::ArtifactPreprocess => process_preprocess_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            processor_factory,
            chunking,
        ),
        crate::storage::JobType::ArtifactExtract => process_extract_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            state_store,
            processor_factory,
            chunking,
            coverage_policy,
        ),
        crate::storage::JobType::ArtifactRetrieveContext => process_retrieve_context_job(
            worker_id,
            &claimed_job,
            job_store,
            retrieval_store,
            state_store,
        ),
        crate::storage::JobType::ArtifactReconcile => process_reconcile_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            state_store,
            derived_store,
            processor_factory,
        ),
    }
}

fn process_extract_job_batch(
    worker_id: &str,
    claimed_jobs: Vec<ClaimedJob>,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    state_store: &dyn EnrichmentStateStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    batch_processor: &dyn crate::processor::ArtifactBatchProcessor,
    chunking: &ExtractionChunkingConfig,
    coverage_policy: &ExtractCoveragePolicy,
) -> std::result::Result<(), String> {
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
        let Some(source_type) = crate::storage::SourceType::from_str(&payload.source_type) else {
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
            source_type,
            title: loaded.artifact.title.clone(),
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
                    let retrieve_job = NewEnrichmentJob {
                        job_id: new_id("job"),
                        artifact_id: claimed_job.artifact_id.clone(),
                        job_type: crate::storage::JobType::ArtifactRetrieveContext,
                        enrichment_tier: claimed_job.enrichment_tier,
                        spawned_by_job_id: Some(claimed_job.job_id.clone()),
                        job_status: crate::storage::JobStatus::Pending,
                        max_attempts: 3,
                        priority_no: 100,
                        required_capabilities: vec!["archive_retrieval".to_string()],
                        payload_json: ArtifactRetrieveContextPayload::new_v1(
                            &claimed_job.artifact_id,
                            &payload.import_id,
                            input.source_type,
                            &extraction_result.extraction_result_id,
                        )
                        .to_json(),
                    };
                    if let Err(err) = job_store.enqueue_jobs(&[retrieve_job]) {
                        let _ = fail_job(
                            job_store,
                            worker_id,
                            &claimed_job.job_id,
                            "Failed to enqueue retrieval-context job",
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
        process_extract_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            state_store,
            processor_factory,
            chunking,
            coverage_policy,
        )?;
    }

    Ok(())
}

fn process_reconcile_job_batch(
    worker_id: &str,
    claimed_jobs: Vec<ClaimedJob>,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    batch_processor: &dyn crate::processor::ReconciliationBatchProcessor,
) -> std::result::Result<(), String> {
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
        let retrieval_result_set =
            match state_store.load_retrieval_result_set(&payload.retrieval_result_set_id) {
                Ok(Some(result)) => result,
                Ok(None) => {
                    fail_job_message(
                        job_store,
                        worker_id,
                        &claimed_job.job_id,
                        format!(
                            "Retrieval result set {} not found",
                            payload.retrieval_result_set_id
                        ),
                    );
                    continue;
                }
                Err(err) => {
                    fail_job(
                        job_store,
                        worker_id,
                        &claimed_job.job_id,
                        "Failed to load retrieval result set for reconciliation",
                        err,
                    );
                    continue;
                }
            };
        let input = match build_reconciliation_input(
            &claimed_job.artifact_id,
            &payload.source_type,
            &extraction_result,
            &retrieval_result_set,
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
        if batch_processor
            .estimate_size_bytes(&input)
            .unwrap_or(usize::MAX)
            <= batch_processor.max_batch_bytes()
        {
            batchable.push((
                claimed_job,
                loaded,
                extraction_result,
                retrieval_result_set,
                input,
            ));
        } else {
            fallbacks.push(claimed_job);
        }
    }

    if !batchable.is_empty() {
        let inputs: Vec<_> = batchable
            .iter()
            .map(|(_, _, _, _, input)| input.clone())
            .collect();
        let results = batch_processor.process_batch(&inputs);
        for ((claimed_job, loaded, extraction_result, retrieval_result_set, _), result) in
            batchable.into_iter().zip(results.into_iter())
        {
            match result {
                Ok(outputs) => {
                    let decisions = if extraction_result.memories.is_empty()
                        && extraction_result.relationships.is_empty()
                    {
                        vec![ReconciliationDecision {
                            reconciliation_decision_id: new_id("reconcile"),
                            artifact_id: extraction_result.artifact_id.clone(),
                            job_id: claimed_job.job_id.clone(),
                            extraction_result_id: extraction_result.extraction_result_id.clone(),
                            retrieval_result_set_id: retrieval_result_set.retrieval_result_set_id.clone(),
                            pipeline_name: "artifact_reconciliation".to_string(),
                            pipeline_version: "v1".to_string(),
                            decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
                            target_kind: "artifact".to_string(),
                            target_key: extraction_result.artifact_id.clone(),
                            matched_object_id: None,
                            rationale: "No candidate memories or relationships were extracted for reconciliation.".to_string(),
                            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
                            status: "completed".to_string(),
                            error_message: None,
                        }]
                    } else {
                        build_reconciliation_decisions(
                            &claimed_job,
                            &extraction_result,
                            &retrieval_result_set,
                            outputs,
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
                    complete_job(job_store, worker_id, &claimed_job.job_id)?;
                }
                Err(err) => {
                    let _ = handle_processor_error(job_store, worker_id, &claimed_job.job_id, err);
                }
            }
        }
    }

    for claimed_job in fallbacks {
        process_reconcile_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            state_store,
            derived_store,
            processor_factory,
        )?;
    }

    Ok(())
}

fn process_preprocess_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    chunking: &ExtractionChunkingConfig,
) -> std::result::Result<(), String> {
    let payload =
        ArtifactPreprocessPayload::from_json(&claimed_job.payload_json).map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to parse preprocess payload JSON",
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
                "Failed to load artifact for preprocessing",
                err,
            )
        })?
        .ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Artifact {} not found for preprocessing",
                    claimed_job.artifact_id
                ),
            )
        })?;

    let Some(source_type) = crate::storage::SourceType::from_str(&payload.source_type) else {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            format!(
                "Invalid artifact source_type in preprocess payload: {}",
                payload.source_type
            ),
        ));
    };

    let processor = processor_factory
        .build_preprocess_processor(claimed_job.enrichment_tier)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to build preprocess processor",
                err,
            )
        })?;
    let processor_input = PreprocessProcessorInput {
        artifact_id: loaded.artifact.artifact_id.clone(),
        import_id: payload.import_id.clone(),
        source_type,
        title: loaded.artifact.title.clone(),
        participants: loaded.participants,
        segments: loaded.segments,
    };
    let preprocess_output = processor
        .segment(&processor_input)
        .map_err(|err| handle_processor_error(job_store, worker_id, &claimed_job.job_id, err))?;
    let coverage_windows = build_preprocess_coverage_windows(
        &claimed_job.artifact_id,
        &processor_input.segments,
        &preprocess_output.topic_threads,
        chunking,
    );
    let extract_job = NewEnrichmentJob {
        job_id: new_id("job"),
        artifact_id: claimed_job.artifact_id.clone(),
        job_type: crate::storage::JobType::ArtifactExtract,
        enrichment_tier: claimed_job.enrichment_tier,
        spawned_by_job_id: Some(claimed_job.job_id.clone()),
        job_status: crate::storage::JobStatus::Pending,
        max_attempts: 3,
        priority_no: 100,
        required_capabilities: vec!["text".to_string()],
        payload_json: ArtifactExtractPayload::new_v1(
            &claimed_job.artifact_id,
            &payload.import_id,
            source_type,
            coverage_windows,
            preprocess_output.topic_threads,
        )
        .to_json(),
    };

    job_store.enqueue_jobs(&[extract_job]).map_err(|err| {
        fail_job(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "Failed to enqueue extraction job from preprocess",
            err,
        )
    })?;

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
}

fn process_preprocess_job_batch(
    worker_id: &str,
    claimed_jobs: Vec<ClaimedJob>,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    batch_processor: &dyn crate::processor::PreprocessBatchProcessor,
    chunking: &ExtractionChunkingConfig,
) -> std::result::Result<(), String> {
    let mut batchable = Vec::new();
    let mut fallbacks = Vec::new();

    for claimed_job in claimed_jobs {
        let payload = match ArtifactPreprocessPayload::from_json(&claimed_job.payload_json) {
            Ok(payload) => payload,
            Err(err) => {
                fail_job(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    "Failed to parse preprocess payload JSON",
                    err,
                );
                continue;
            }
        };
        let Some(source_type) = crate::storage::SourceType::from_str(&payload.source_type) else {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Invalid artifact source_type in preprocess payload: {}",
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
                        "Artifact {} not found for preprocessing",
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
                    "Failed to load artifact for preprocessing",
                    err,
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
        if batch_processor
            .estimate_size_bytes(&input)
            .unwrap_or(usize::MAX)
            <= batch_processor.max_batch_bytes()
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
                    let coverage_windows = build_preprocess_coverage_windows(
                        &claimed_job.artifact_id,
                        &input.segments,
                        &output.topic_threads,
                        chunking,
                    );
                    let extract_job = NewEnrichmentJob {
                        job_id: new_id("job"),
                        artifact_id: claimed_job.artifact_id.clone(),
                        job_type: crate::storage::JobType::ArtifactExtract,
                        enrichment_tier: claimed_job.enrichment_tier,
                        spawned_by_job_id: Some(claimed_job.job_id.clone()),
                        job_status: crate::storage::JobStatus::Pending,
                        max_attempts: 3,
                        priority_no: 100,
                        required_capabilities: vec!["text".to_string()],
                        payload_json: ArtifactExtractPayload::new_v1(
                            &claimed_job.artifact_id,
                            &payload.import_id,
                            input.source_type,
                            coverage_windows,
                            output.topic_threads,
                        )
                        .to_json(),
                    };
                    if let Err(err) = job_store.enqueue_jobs(&[extract_job]) {
                        let _ = fail_job(
                            job_store,
                            worker_id,
                            &claimed_job.job_id,
                            "Failed to enqueue extraction job from preprocess",
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
        process_preprocess_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            processor_factory,
            chunking,
        )?;
    }

    Ok(())
}

fn process_extract_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    state_store: &dyn EnrichmentStateStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    chunking: &ExtractionChunkingConfig,
    coverage_policy: &ExtractCoveragePolicy,
) -> std::result::Result<(), String> {
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

    let source_type =
        crate::storage::SourceType::from_str(&payload.source_type).ok_or_else(|| {
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
        source_type,
        title: loaded.artifact.title.clone(),
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
        chunking,
        coverage_policy,
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

    let retrieve_job = NewEnrichmentJob {
        job_id: new_id("job"),
        artifact_id: claimed_job.artifact_id.clone(),
        job_type: crate::storage::JobType::ArtifactRetrieveContext,
        enrichment_tier: claimed_job.enrichment_tier,
        spawned_by_job_id: Some(claimed_job.job_id.clone()),
        job_status: crate::storage::JobStatus::Pending,
        max_attempts: 3,
        priority_no: 100,
        required_capabilities: vec!["archive_retrieval".to_string()],
        payload_json: ArtifactRetrieveContextPayload::new_v1(
            &claimed_job.artifact_id,
            &payload.import_id,
            source_type,
            &extraction_result.extraction_result_id,
        )
        .to_json(),
    };
    job_store.enqueue_jobs(&[retrieve_job]).map_err(|err| {
        fail_job(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "Failed to enqueue retrieval-context job",
            err,
        )
    })?;

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
}

fn process_retrieve_context_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    retrieval_store: &dyn ArchiveRetrievalStore,
    state_store: &dyn EnrichmentStateStore,
) -> std::result::Result<(), String> {
    let payload =
        ArtifactRetrieveContextPayload::from_json(&claimed_job.payload_json).map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to parse retrieve-context payload JSON",
                err,
            )
        })?;

    let extraction_result = state_store
        .load_extraction_result(&payload.extraction_result_id)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to load extraction result",
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

    let results = retrieval_store
        .retrieve_for_intents(
            &claimed_job.artifact_id,
            &extraction_result.retrieval_intents,
            8,
        )
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to retrieve archive context",
                err,
            )
        })?;

    let result_set = RetrievalResultSet {
        retrieval_result_set_id: new_id("retrieval"),
        artifact_id: claimed_job.artifact_id.clone(),
        job_id: claimed_job.job_id.clone(),
        extraction_result_id: extraction_result.extraction_result_id.clone(),
        pipeline_name: "archive_retrieval".to_string(),
        pipeline_version: "v1".to_string(),
        intents: extraction_result.retrieval_intents.clone(),
        results,
        status: "completed".to_string(),
        error_message: None,
    };

    state_store
        .save_retrieval_result_set(&result_set)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to persist retrieval result set",
                err,
            )
        })?;

    let source_type =
        crate::storage::SourceType::from_str(&payload.source_type).ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Invalid artifact source_type in retrieve-context payload: {}",
                    payload.source_type
                ),
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
        required_capabilities: vec!["text".to_string(), "archive_retrieval".to_string()],
        payload_json: ArtifactReconcilePayload::new_v1(
            &claimed_job.artifact_id,
            &payload.import_id,
            source_type,
            &extraction_result.extraction_result_id,
            &result_set.retrieval_result_set_id,
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
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
) -> std::result::Result<(), String> {
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
    let retrieval_result_set = state_store
        .load_retrieval_result_set(&payload.retrieval_result_set_id)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to load retrieval result set",
                err,
            )
        })?
        .ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Retrieval result set {} not found",
                    payload.retrieval_result_set_id
                ),
            )
        })?;

    let decisions = if extraction_result.memories.is_empty()
        && extraction_result.relationships.is_empty()
    {
        vec![ReconciliationDecision {
            reconciliation_decision_id: new_id("reconcile"),
            artifact_id: extraction_result.artifact_id.clone(),
            job_id: claimed_job.job_id.clone(),
            extraction_result_id: extraction_result.extraction_result_id.clone(),
            retrieval_result_set_id: retrieval_result_set.retrieval_result_set_id.clone(),
            pipeline_name: "artifact_reconciliation".to_string(),
            pipeline_version: "v1".to_string(),
            decision_kind: ReconciliationDecisionKind::InsufficientEvidence,
            target_kind: "artifact".to_string(),
            target_key: extraction_result.artifact_id.clone(),
            matched_object_id: None,
            rationale: "No candidate memories or relationships were extracted for reconciliation."
                .to_string(),
            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
            status: "completed".to_string(),
            error_message: None,
        }]
    } else {
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
        let input = build_reconciliation_input(
            &claimed_job.artifact_id,
            &payload.source_type,
            &extraction_result,
            &retrieval_result_set,
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
        let outputs = processor.reconcile(&input).map_err(|err| {
            handle_processor_error(job_store, worker_id, &claimed_job.job_id, err)
        })?;
        build_reconciliation_decisions(
            claimed_job,
            &extraction_result,
            &retrieval_result_set,
            outputs,
        )
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

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
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
    let mut escalate_to_frontier = false;
    let mut escalation_reasons = Vec::new();

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
        escalate_to_frontier |= output.escalate_to_frontier;
        if let Some(reason) = &output.escalation_reason {
            if !escalation_reasons.contains(reason) {
                escalation_reasons.push(reason.clone());
            }
        }

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

    cleanup_artifact_processor_output(ArtifactProcessorOutput {
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
        escalate_to_frontier,
        escalation_reason: if escalate_to_frontier && !escalation_reasons.is_empty() {
            Some(escalation_reasons.join(" "))
        } else {
            None
        },
    })
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

fn uncovered_segments_for_output<'a>(
    input: &'a ArtifactProcessorInput,
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
    chunking: &ExtractionChunkingConfig,
    coverage_policy: &ExtractCoveragePolicy,
) -> std::result::Result<ArtifactProcessorOutput, String> {
    if coverage_policy.max_gap_fill_passes == 0 || !should_shape_conversation_input(input) {
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
            build_preprocess_coverage_windows(&input.artifact_id, &uncovered_segments, &[], chunking);
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

fn build_reconciliation_input(
    artifact_id: &str,
    source_type: &str,
    extraction_result: &ArtifactExtractionResult,
    retrieval_result_set: &RetrievalResultSet,
) -> Result<ReconciliationProcessorInput, serde_json::Error> {
    Ok(ReconciliationProcessorInput {
        artifact_id: artifact_id.to_string(),
        source_type: crate::storage::SourceType::from_str(source_type)
            .expect("validated source_type during payload parsing"),
        summary: SummaryOutput {
            title: extraction_result.summary_title.clone(),
            body_text: extraction_result.summary_body_text.clone(),
            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
        },
        memories: extraction_result
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
        relationships: extraction_result
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
        retrieval_results_json: serde_json::to_string_pretty(retrieval_result_set)?,
    })
}

fn build_reconciliation_decisions(
    claimed_job: &ClaimedJob,
    extraction_result: &ArtifactExtractionResult,
    retrieval_result_set: &RetrievalResultSet,
    outputs: Vec<crate::processor::ReconciliationDecisionOutput>,
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

fn relationship_target_key(relationship: &CandidateRelationship) -> String {
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
    if err.is_retryable() {
        let message = format!("Processor execution failed: {err}");
        return mark_job_retryable_message(
            job_store,
            worker_id,
            job_id,
            message,
            RETRYABLE_INFERENCE_BACKOFF_SECONDS,
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

fn complete_job(
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

/// Start enrichment worker pool with shutdown capability.
pub fn start_enrichment_workers(
    config: &crate::config::HttpConfig,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
) -> Result<Vec<thread::JoinHandle<()>>, anyhow::Error> {
    start_enrichment_workers_with_factory(
        config,
        job_store,
        read_store,
        retrieval_store,
        state_store,
        derived_store,
        shutdown,
        Arc::new(StubProcessorFactory),
    )
}

#[doc(hidden)]
pub fn start_enrichment_workers_with_factory(
    config: &crate::config::HttpConfig,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
) -> Result<Vec<thread::JoinHandle<()>>, anyhow::Error> {
    let pid = std::process::id();
    let poll_interval = Duration::from_millis(config.enrichment_poll_interval_ms);

    let workers: Vec<_> = (0..config.enrichment_worker_count)
        .map(|worker_index| {
            let worker_id = format_worker_id(pid, worker_index);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let retrieval_store = Arc::clone(&retrieval_store);
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
                        job_store,
                        read_store,
                        retrieval_store,
                        state_store,
                        derived_store,
                        processor_factory,
                        chunking,
                        coverage_policy,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn enrichment worker thread: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(workers)
}

// ---------------------------------------------------------------------------
// Per-stage pipeline startup (replaces uniform worker pool)
// ---------------------------------------------------------------------------

/// Start the enrichment pipeline with per-stage pollers and dedicated
/// retrieve-context workers.
///
/// Spawns:
/// - batch mode:
///   - preprocess poller threads
///   - extract poller threads
///   - reconcile poller threads
/// - direct mode:
///   - direct preprocess/extract/reconcile worker threads
/// - both modes:
///   - retrieve-context worker threads
pub fn start_enrichment_pipeline(
    config: &EnrichmentPipelineConfig,
    execution_mode: InferenceExecutionMode,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
) -> Result<Vec<thread::JoinHandle<()>>> {
    if execution_mode == InferenceExecutionMode::Batch {
        validate_batch_execution_support(processor_factory.as_ref())?;
    }
    let pid = std::process::id();
    let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit_requests_per_minute));
    let chunking = Arc::new(config.chunking.clone());
    let coverage_policy = Arc::new(ExtractCoveragePolicy {
        min_coverage_percent: config.extract_min_coverage_percent,
        max_gap_fill_passes: config.extract_max_gap_fill_passes,
    });
    let poll_interval = config.poll_interval;
    let mut handles = Vec::new();

    match execution_mode {
        InferenceExecutionMode::Batch => {
            handles.extend(spawn_preprocess_batch_workers(
                pid,
                config.preprocess_workers,
                config.preprocess.batch_size,
                config.preprocess.max_concurrent_batches,
                Arc::clone(&job_store),
                Arc::clone(&read_store),
                Arc::clone(&state_store),
                Arc::clone(&derived_store),
                Arc::clone(&processor_factory),
                Arc::clone(&rate_limiter),
                Arc::clone(&chunking),
                poll_interval,
                shutdown.clone(),
            )?);
            handles.extend(spawn_extract_batch_workers(
                pid,
                config.extract_workers,
                config.extract.batch_size,
                config.extract.max_concurrent_batches,
                Arc::clone(&job_store),
                Arc::clone(&read_store),
                Arc::clone(&state_store),
                Arc::clone(&derived_store),
                Arc::clone(&processor_factory),
                Arc::clone(&rate_limiter),
                Arc::clone(&chunking),
                poll_interval,
                shutdown.clone(),
            )?);
            handles.extend(spawn_reconcile_batch_workers(
                pid,
                config.reconcile_workers,
                config.reconcile.batch_size,
                config.reconcile.max_concurrent_batches,
                Arc::clone(&job_store),
                Arc::clone(&read_store),
                Arc::clone(&state_store),
                Arc::clone(&derived_store),
                Arc::clone(&processor_factory),
                Arc::clone(&rate_limiter),
                poll_interval,
                shutdown.clone(),
            )?);
        }
        InferenceExecutionMode::Direct => {
            handles.extend(spawn_direct_stage_workers(
                pid,
                "preprocess",
                JobType::ArtifactPreprocess,
                config.preprocess_workers,
                Arc::clone(&job_store),
                Arc::clone(&read_store),
                Arc::clone(&retrieval_store),
                Arc::clone(&state_store),
                Arc::clone(&derived_store),
                Arc::clone(&processor_factory),
                Arc::clone(&chunking),
                Arc::clone(&coverage_policy),
                poll_interval,
                shutdown.clone(),
            )?);
            handles.extend(spawn_direct_stage_workers(
                pid,
                "extract",
                JobType::ArtifactExtract,
                config.extract_workers,
                Arc::clone(&job_store),
                Arc::clone(&read_store),
                Arc::clone(&retrieval_store),
                Arc::clone(&state_store),
                Arc::clone(&derived_store),
                Arc::clone(&processor_factory),
                Arc::clone(&chunking),
                Arc::clone(&coverage_policy),
                poll_interval,
                shutdown.clone(),
            )?);
            handles.extend(spawn_direct_stage_workers(
                pid,
                "reconcile",
                JobType::ArtifactReconcile,
                config.reconcile_workers,
                Arc::clone(&job_store),
                Arc::clone(&read_store),
                Arc::clone(&retrieval_store),
                Arc::clone(&state_store),
                Arc::clone(&derived_store),
                Arc::clone(&processor_factory),
                Arc::clone(&chunking),
                Arc::clone(&coverage_policy),
                poll_interval,
                shutdown.clone(),
            )?);
        }
    }

    // RetrieveContext workers (traditional blocking workers)
    for i in 0..config.retrieve_context_workers {
        let worker_id = format!("enrichment:{}:retrieve:{}", pid, i);
        let job_store = Arc::clone(&job_store);
        let retrieval_store = Arc::clone(&retrieval_store);
        let state_store = Arc::clone(&state_store);
        let shutdown = shutdown.clone();

        let h = thread::Builder::new()
            .name(format!("retrieve-context-{}", i))
            .spawn(move || {
                retrieve_context_worker_loop(
                    worker_id,
                    job_store.as_ref(),
                    retrieval_store.as_ref(),
                    state_store.as_ref(),
                    poll_interval,
                    shutdown,
                );
            })
            .map_err(|e| anyhow::anyhow!("Failed to spawn retrieve-context worker: {}", e))?;
        handles.push(h);
    }

    info!(
        "Enrichment pipeline started: mode={:?}, preprocess_workers={}, extract_workers={}, reconcile_workers={}, retrieve_context_workers={}, chunk_segments={}, chunk_overlap={}, chunk_max_chars={}",
        execution_mode,
        config.preprocess_workers,
        config.extract_workers,
        config.reconcile_workers,
        config.retrieve_context_workers
        ,
        config.chunking.max_segments_per_chunk,
        config.chunking.chunk_overlap_segments,
        config.chunking.max_chars_per_chunk
    );
    Ok(handles)
}

fn validate_batch_execution_support(
    processor_factory: &dyn ArtifactProcessorFactory,
) -> Result<()> {
    use crate::storage::EnrichmentTier;

    if processor_factory
        .build_preprocess_submitter(EnrichmentTier::Standard)?
        .is_none()
    {
        return Err(anyhow::anyhow!(
            "batch execution mode requires preprocess batch submitter support"
        ));
    }
    if processor_factory
        .build_extraction_submitter(EnrichmentTier::Standard)?
        .is_none()
    {
        return Err(anyhow::anyhow!(
            "batch execution mode requires extraction batch submitter support"
        ));
    }
    if processor_factory
        .build_reconciliation_submitter(EnrichmentTier::Standard)?
        .is_none()
    {
        return Err(anyhow::anyhow!(
            "batch execution mode requires reconciliation batch submitter support"
        ));
    }
    Ok(())
}

fn spawn_preprocess_batch_workers(
    pid: u32,
    worker_count: usize,
    batch_size: usize,
    max_concurrent: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    rate_limiter: Arc<RateLimiter>,
    chunking: Arc<ExtractionChunkingConfig>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) -> Result<Vec<thread::JoinHandle<()>>> {
    (0..worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:preprocess:{}", pid, i);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let state_store = Arc::clone(&state_store);
            let derived_store = Arc::clone(&derived_store);
            let processor_factory = Arc::clone(&processor_factory);
            let rate_limiter = Arc::clone(&rate_limiter);
            let chunking = Arc::clone(&chunking);
            let shutdown = shutdown.clone();
            thread::Builder::new()
                .name(format!("preprocess-poller-{}", i))
                .spawn(move || {
                    let submitter = processor_factory
                        .build_preprocess_submitter(crate::storage::EnrichmentTier::Standard)
                        .ok()
                        .flatten();
                    let Some(submitter) = submitter else {
                        info!("No preprocess batch submitter available; preprocess poller exiting");
                        return;
                    };
                    let stage = PreprocessStage::new(
                        submitter,
                        crate::storage::EnrichmentTier::Standard,
                        batch_size,
                        max_concurrent,
                        (*chunking).clone(),
                    );
                    stage_poller_loop(
                        &stage,
                        worker_id,
                        job_store.as_ref(),
                        read_store.as_ref(),
                        state_store.as_ref(),
                        derived_store.as_ref(),
                        &rate_limiter,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn preprocess poller: {}", e))
        })
        .collect()
}

fn spawn_reconcile_batch_workers(
    pid: u32,
    worker_count: usize,
    batch_size: usize,
    max_concurrent: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    rate_limiter: Arc<RateLimiter>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) -> Result<Vec<thread::JoinHandle<()>>> {
    (0..worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:reconcile:{}", pid, i);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let state_store = Arc::clone(&state_store);
            let derived_store = Arc::clone(&derived_store);
            let processor_factory = Arc::clone(&processor_factory);
            let rate_limiter = Arc::clone(&rate_limiter);
            let shutdown = shutdown.clone();
            thread::Builder::new()
                .name(format!("reconcile-poller-{}", i))
                .spawn(move || {
                    let submitter = processor_factory
                        .build_reconciliation_submitter(crate::storage::EnrichmentTier::Standard)
                        .ok()
                        .flatten();
                    let Some(submitter) = submitter else {
                        info!(
                            "No reconciliation batch submitter available; reconcile poller exiting"
                        );
                        return;
                    };
                    let stage = ReconcileStage::new(
                        submitter,
                        crate::storage::EnrichmentTier::Standard,
                        batch_size,
                        max_concurrent,
                    );
                    stage_poller_loop(
                        &stage,
                        worker_id,
                        job_store.as_ref(),
                        read_store.as_ref(),
                        state_store.as_ref(),
                        derived_store.as_ref(),
                        &rate_limiter,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn reconcile poller: {}", e))
        })
        .collect()
}

fn spawn_extract_batch_workers(
    pid: u32,
    worker_count: usize,
    batch_size: usize,
    max_concurrent: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    rate_limiter: Arc<RateLimiter>,
    chunking: Arc<ExtractionChunkingConfig>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) -> Result<Vec<thread::JoinHandle<()>>> {
    (0..worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:extract:{}", pid, i);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let state_store = Arc::clone(&state_store);
            let derived_store = Arc::clone(&derived_store);
            let processor_factory = Arc::clone(&processor_factory);
            let rate_limiter = Arc::clone(&rate_limiter);
            let chunking = Arc::clone(&chunking);
            let shutdown = shutdown.clone();
            thread::Builder::new()
                .name(format!("extract-poller-{}", i))
                .spawn(move || {
                    let submitter = processor_factory
                        .build_extraction_submitter(crate::storage::EnrichmentTier::Standard)
                        .ok()
                        .flatten();
                    let Some(submitter) = submitter else {
                        info!("No extract batch submitter available; extract poller exiting");
                        return;
                    };
                    let stage = ExtractStage::new(
                        submitter,
                        crate::storage::EnrichmentTier::Standard,
                        batch_size,
                        max_concurrent,
                        chunking.as_ref().clone(),
                    );
                    stage_poller_loop(
                        &stage,
                        worker_id,
                        job_store.as_ref(),
                        read_store.as_ref(),
                        state_store.as_ref(),
                        derived_store.as_ref(),
                        &rate_limiter,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn extract poller: {}", e))
        })
        .collect()
}

fn spawn_direct_stage_workers(
    pid: u32,
    stage_name: &'static str,
    job_type: JobType,
    worker_count: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    retrieval_store: Arc<dyn ArchiveRetrievalStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    chunking: Arc<ExtractionChunkingConfig>,
    coverage_policy: Arc<ExtractCoveragePolicy>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) -> Result<Vec<thread::JoinHandle<()>>> {
    (0..worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:{}:{}", pid, stage_name, i);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let retrieval_store = Arc::clone(&retrieval_store);
            let state_store = Arc::clone(&state_store);
            let derived_store = Arc::clone(&derived_store);
            let processor_factory = Arc::clone(&processor_factory);
            let chunking = Arc::clone(&chunking);
            let coverage_policy = Arc::clone(&coverage_policy);
            let shutdown = shutdown.clone();
            thread::Builder::new()
                .name(format!("direct-{}-{}", stage_name, i))
                .spawn(move || {
                    direct_stage_worker_loop(
                        worker_id,
                        job_type,
                        job_store.as_ref(),
                        read_store.as_ref(),
                        retrieval_store.as_ref(),
                        state_store.as_ref(),
                        derived_store.as_ref(),
                        processor_factory.as_ref(),
                        chunking.as_ref(),
                        coverage_policy.as_ref(),
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn direct {} worker: {}", stage_name, e))
        })
        .collect()
}

/// Dedicated worker loop for RetrieveContext jobs.
///
/// Claims one job at a time, processes it synchronously, and sleeps when
/// no jobs are available. This stage does local/DB retrieval, not provider
/// API calls, so a traditional blocking worker is appropriate.
fn direct_stage_worker_loop(
    worker_id: String,
    job_type: JobType,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    retrieval_store: &dyn ArchiveRetrievalStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
    chunking: &ExtractionChunkingConfig,
    coverage_policy: &ExtractCoveragePolicy,
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

        match job_store.claim_jobs_by_type(&worker_id, job_type, Some(EnrichmentTier::Standard), 1)
        {
            Ok(jobs) if !jobs.is_empty() => {
                let job = jobs.into_iter().next().expect("one claimed job");
                debug!(
                    "Direct stage worker {} claimed {} job {}",
                    worker_id,
                    job.job_type.as_str(),
                    job.job_id
                );
                if let Err(err) = process_claimed_job(
                    &worker_id,
                    job,
                    job_store,
                    read_store,
                    retrieval_store,
                    state_store,
                    derived_store,
                    processor_factory,
                    chunking,
                    coverage_policy,
                ) {
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

/// Dedicated worker loop for RetrieveContext jobs.
///
/// Claims one job at a time, processes it synchronously, and sleeps when
/// no jobs are available. This stage does local/DB retrieval, not provider
/// API calls, so a traditional blocking worker is appropriate.
fn retrieve_context_worker_loop(
    worker_id: String,
    job_store: &dyn EnrichmentJobLifecycleStore,
    retrieval_store: &dyn ArchiveRetrievalStore,
    state_store: &dyn EnrichmentStateStore,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!("Retrieve-context worker {} starting", worker_id);

    loop {
        if shutdown.is_shutdown() {
            info!("Retrieve-context worker {} shutting down", worker_id);
            break;
        }

        match job_store.claim_jobs_by_type(
            &worker_id,
            JobType::ArtifactRetrieveContext,
            None, // tier-agnostic: retrieve_context does DB lookups, not model calls
            1,
        ) {
            Ok(jobs) if !jobs.is_empty() => {
                let job = &jobs[0];
                debug!(
                    "Worker {} claimed retrieve-context job {}",
                    worker_id, job.job_id
                );
                if let Err(err) = process_retrieve_context_job(
                    &worker_id,
                    job,
                    job_store,
                    retrieval_store,
                    state_store,
                ) {
                    error!(
                        "Worker {} failed to process retrieve-context job: {}",
                        worker_id, err
                    );
                }
            }
            Ok(_) => thread::sleep(poll_interval),
            Err(err) => {
                error!("Worker {} failed to claim job: {}", worker_id, err);
                thread::sleep(poll_interval);
            }
        }
    }
}
