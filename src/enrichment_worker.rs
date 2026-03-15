use crate::domain::SourceTimestamp;
use crate::processor::{
    ArtifactProcessorFactory, ArtifactProcessorInput, ArtifactProcessorOutput,
    ClassificationOutput, EntityOutput, MemoryOutput, ProcessorError, ReconciliationProcessorInput,
    RelationshipOutput, StubProcessorFactory, SummaryOutput,
};
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArchiveRetrievalStore, ArtifactExtractPayload, ArtifactExtractionResult,
    ArtifactPreprocessPayload, ArtifactReadStore, ArtifactReconcilePayload,
    ArtifactRetrieveContextPayload, CandidateEntity, CandidateRelationship, ClaimedJob,
    ClassificationObjectJson, ConversationWindowRef, DerivationRunStatus, DerivationRunType,
    DerivedMetadataWriteStore, DerivedObjectPayload, EnrichmentJobLifecycleStore,
    EnrichmentStateStore, EvidenceRole, ExtractedClassification, ExtractedMemory, InputScopeType,
    MemoryObjectJson, NewDerivationRun, NewDerivedObject, NewEnrichmentJob, NewEvidenceLink,
    ObjectStatus, OriginKind, ReconciliationDecision, ReconciliationDecisionKind,
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
const PREPROCESS_WINDOW_SEGMENTS: usize = 24;
const PREPROCESS_WINDOW_OVERLAP: usize = 4;

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
                if let Err(err) = process_claimed_job(
                    &worker_id,
                    claimed_job,
                    job_store.as_ref(),
                    read_store.as_ref(),
                    retrieval_store.as_ref(),
                    state_store.as_ref(),
                    derived_store.as_ref(),
                    processor_factory.as_ref(),
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

fn process_claimed_job(
    worker_id: &str,
    claimed_job: ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    retrieval_store: &dyn ArchiveRetrievalStore,
    state_store: &dyn EnrichmentStateStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
) -> std::result::Result<(), String> {
    match claimed_job.job_type {
        crate::storage::JobType::ArtifactPreprocess => {
            process_preprocess_job(worker_id, &claimed_job, job_store, read_store)
        }
        crate::storage::JobType::ArtifactExtract => process_extract_job(
            worker_id,
            &claimed_job,
            job_store,
            read_store,
            state_store,
            processor_factory,
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

fn process_preprocess_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
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

    let windows = build_preprocess_windows(&claimed_job.artifact_id, &loaded.segments);
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
            windows,
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

fn process_extract_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    state_store: &dyn EnrichmentStateStore,
    processor_factory: &dyn ArtifactProcessorFactory,
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

    let output = if payload.conversation_windows.len() > 1 {
        let mut chunk_outputs = Vec::new();
        for chunk_input in build_chunk_inputs(&processor_input, &payload.conversation_windows) {
            let chunk_output = processor.process(&chunk_input).map_err(|err| {
                handle_processor_error(job_store, worker_id, &claimed_job.job_id, err)
            })?;
            chunk_outputs.push(chunk_output);
        }
        merge_chunk_outputs(&processor_input, &chunk_outputs)
    } else {
        processor
            .process(&processor_input)
            .map_err(|err| handle_processor_error(job_store, worker_id, &claimed_job.job_id, err))?
    };

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

fn build_preprocess_windows(
    artifact_id: &str,
    segments: &[crate::storage::LoadedSegment],
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }
    if segments.len() <= PREPROCESS_WINDOW_SEGMENTS {
        return vec![ConversationWindowRef {
            window_id: format!("{artifact_id}:window:0"),
            label: "full conversation".to_string(),
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

fn build_chunk_inputs(
    input: &ArtifactProcessorInput,
    windows: &[ConversationWindowRef],
) -> Vec<ArtifactProcessorInput> {
    windows
        .iter()
        .map(|window| ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: input.title.clone(),
            participants: input.participants.clone(),
            segments: input
                .segments
                .iter()
                .filter(|segment| {
                    segment.sequence_no >= window.start_sequence_no
                        && segment.sequence_no <= window.end_sequence_no
                })
                .cloned()
                .collect(),
        })
        .filter(|chunk| !chunk.segments.is_empty())
        .collect()
}

fn merge_chunk_outputs(
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
            classifications
                .entry((
                    classification.classification_type.clone(),
                    classification.classification_value.clone(),
                ))
                .or_insert_with(|| classification.clone());
        }
        for memory in &output.memories {
            memories
                .entry(memory_target_key_from_output(memory))
                .or_insert_with(|| memory.clone());
        }
        for entity in &output.entities {
            entities
                .entry(entity.entity_key.clone())
                .or_insert_with(|| entity.clone());
        }
        for relationship in &output.relationships {
            relationships
                .entry(relationship_target_key_from_output(relationship))
                .or_insert_with(|| relationship.clone());
        }
        for intent in &output.retrieval_intents {
            retrieval_intents
                .entry((intent.intent_type.clone(), intent.query_text.clone()))
                .or_insert_with(|| intent.clone());
        }
    }

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
        escalate_to_frontier,
        escalation_reason: if escalate_to_frontier && !escalation_reasons.is_empty() {
            Some(escalation_reasons.join(" "))
        } else {
            None
        },
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
    memory
        .title
        .clone()
        .unwrap_or_else(|| memory.body_text.chars().take(64).collect())
}

fn memory_target_key_from_output(memory: &MemoryOutput) -> String {
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
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn enrichment worker thread: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(workers)
}
