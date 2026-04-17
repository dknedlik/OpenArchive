use super::context::WorkerContext;
use super::embedding::{build_embedding_job, try_enqueue_embedding_job};
use super::jobs::{
    fail_job, fail_job_message, handle_processor_error, has_reconciliation_candidates,
};
use super::{complete_job, new_id};
use crate::domain::SourceTimestamp;
use crate::embedding::EmbeddingProvider;
use crate::processor::{
    memory_candidate_key_from_fields, ArtifactProcessorOutput, EntityOutput, MemoryOutput,
    ReconciliationProcessorInput, RelationshipOutput, SummaryOutput,
};
use crate::storage::{
    ArtifactExtractionResult, ArtifactReconcilePayload, CandidateEntity, CandidateRelationship,
    ClaimedJob, ClassificationObjectJson, CrossArtifactReadStore, DerivationRunStatus,
    DerivationRunType, DerivedObjectPayload, DerivedObjectType, ExtractedMemory, InputScopeType,
    MemoryObjectJson, NewArchiveLink, NewDerivationRun, NewDerivedObject, ObjectStatus, OriginKind,
    ReconciliationDecision, ReconciliationDecisionKind, RelatedDerivedObject,
    RelatedDerivedObjectEmbeddingMatch, RelationshipObjectJson, ScopeType, SummaryObjectJson,
    WriteDerivationAttempt, WriteDerivedObject,
};
use std::collections::{HashMap, HashSet};

const RECONCILE_EMBEDDING_DETERMINISTIC_THRESHOLD: f32 = 0.90;
const RECONCILE_EMBEDDING_AMBIGUOUS_THRESHOLD: f32 = 0.77;
const RECONCILE_EMBEDDING_LOOKUP_LIMIT: usize = 3;

pub(super) fn process_reconcile_job_batch(
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

pub(super) fn process_reconcile_job(
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

fn build_reconciliation_input(
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
            })
            .collect(),
        extraction_result
            .entities
            .iter()
            .map(|entity| EntityOutput {
                entity_key: entity.entity_key.clone(),
                display_name: entity.display_name.clone(),
                entity_type: entity.entity_type.clone(),
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
            })
            .collect(),
        "[]".to_string(),
    )
}

pub fn build_reconciliation_input_from_processor_output(
    artifact_id: &str,
    source_type: crate::storage::SourceType,
    output: &ArtifactProcessorOutput,
) -> std::result::Result<ReconciliationProcessorInput, serde_json::Error> {
    build_reconciliation_input_from_outputs(
        artifact_id,
        source_type,
        output.summary.clone(),
        output.memories.clone(),
        output.entities.clone(),
        output.relationships.clone(),
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
}

#[derive(Clone)]
pub(super) struct PreparedReconciliationWork {
    pub(super) deterministic_outputs: Vec<crate::processor::ReconciliationDecisionOutput>,
    pub(super) ambiguous_input: Option<ReconciliationProcessorInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationCandidateDisposition {
    ExactCandidateKeyMatch,
    FallbackWithoutEmbeddingProvider,
    FallbackWithoutCrossArtifactStore,
    NoEmbeddingMatches,
    DeterministicEmbeddingMatch,
    LowSimilarityCreateNew,
    AmbiguousEmbeddingMatch,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReconciliationCandidateTrace {
    pub target_kind: String,
    pub target_key: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: String,
    pub disposition: ReconciliationCandidateDisposition,
    pub exact_matches: Vec<RelatedDerivedObject>,
    pub embedding_matches: Vec<RelatedDerivedObjectEmbeddingMatch>,
    pub deterministic_output: Option<crate::processor::ReconciliationDecisionOutput>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReconciliationPreparationReport {
    pub deterministic_outputs: Vec<crate::processor::ReconciliationDecisionOutput>,
    pub ambiguous_input: Option<ReconciliationProcessorInput>,
    pub candidate_traces: Vec<ReconciliationCandidateTrace>,
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

fn build_candidate_trace(
    candidate: &ReconcileCandidate,
    disposition: ReconciliationCandidateDisposition,
    exact_matches: Vec<RelatedDerivedObject>,
    embedding_matches: Vec<RelatedDerivedObjectEmbeddingMatch>,
    deterministic_output: Option<crate::processor::ReconciliationDecisionOutput>,
) -> ReconciliationCandidateTrace {
    ReconciliationCandidateTrace {
        target_kind: candidate.target_kind().to_string(),
        target_key: candidate.target_key(),
        derived_object_type: candidate.derived_object_type(),
        title: candidate.title(),
        body_text: candidate.body_text(),
        disposition,
        exact_matches,
        embedding_matches,
        deterministic_output,
    }
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
    }
}

fn build_ambiguous_reconcile_cases(
    traces: &[ReconciliationCandidateTrace],
) -> Vec<AmbiguousReconcileCase> {
    traces
        .iter()
        .filter(|trace| {
            trace.disposition == ReconciliationCandidateDisposition::AmbiguousEmbeddingMatch
        })
        .map(|trace| AmbiguousReconcileCase {
            target_kind: trace.target_kind.clone(),
            target_key: trace.target_key.clone(),
            embedding_matches: trace
                .embedding_matches
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
        })
        .collect()
}

pub fn inspect_reconciliation_work(
    input: &ReconciliationProcessorInput,
    cross_artifact_store: Option<&(dyn CrossArtifactReadStore + Send + Sync)>,
    embedding_provider: Option<&dyn EmbeddingProvider>,
) -> std::result::Result<ReconciliationPreparationReport, String> {
    let candidates = build_reconciliation_candidates(input);
    if candidates.is_empty() {
        return Ok(ReconciliationPreparationReport {
            deterministic_outputs: Vec::new(),
            ambiguous_input: None,
            candidate_traces: Vec::new(),
        });
    }

    let mut deterministic_outputs = Vec::new();
    let mut unresolved = candidates.clone();
    let mut candidate_traces = Vec::with_capacity(candidates.len());

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
            let matching_exact = exact_matches
                .iter()
                .filter(|existing| {
                    existing.derived_object_type == candidate.derived_object_type()
                        && existing
                            .candidate_key
                            .as_deref()
                            .map(str::trim)
                            .unwrap_or_default()
                            == candidate.target_key()
                })
                .cloned()
                .collect::<Vec<_>>();
            if let Some(existing) = matching_exact.first() {
                let output = attach_existing_output(
                    &candidate,
                    &existing.derived_object_id,
                    "Deterministic exact candidate_key match against an existing active derived object."
                        .to_string(),
                );
                deterministic_outputs.push(output.clone());
                candidate_traces.push(build_candidate_trace(
                    &candidate,
                    ReconciliationCandidateDisposition::ExactCandidateKeyMatch,
                    matching_exact,
                    Vec::new(),
                    Some(output),
                ));
            } else {
                still_unresolved.push(candidate);
            }
        }
        unresolved = still_unresolved;
    }

    if unresolved.is_empty() {
        return Ok(ReconciliationPreparationReport {
            deterministic_outputs,
            ambiguous_input: None,
            candidate_traces,
        });
    }

    let Some(embedding_provider) = embedding_provider else {
        let ambiguous_input = Some(
            build_reconciliation_input_for_candidates(
                input,
                unresolved.clone(),
                input.retrieval_results_json.clone(),
            )
            .map_err(|err| format!("failed to build fallback reconciliation input: {err}"))?,
        );
        for candidate in unresolved {
            candidate_traces.push(build_candidate_trace(
                &candidate,
                ReconciliationCandidateDisposition::FallbackWithoutEmbeddingProvider,
                Vec::new(),
                Vec::new(),
                None,
            ));
        }
        return Ok(ReconciliationPreparationReport {
            deterministic_outputs,
            ambiguous_input,
            candidate_traces,
        });
    };
    let Some(cross_artifact_store) = cross_artifact_store else {
        let ambiguous_input = Some(
            build_reconciliation_input_for_candidates(
                input,
                unresolved.clone(),
                input.retrieval_results_json.clone(),
            )
            .map_err(|err| format!("failed to build fallback reconciliation input: {err}"))?,
        );
        for candidate in unresolved {
            candidate_traces.push(build_candidate_trace(
                &candidate,
                ReconciliationCandidateDisposition::FallbackWithoutCrossArtifactStore,
                Vec::new(),
                Vec::new(),
                None,
            ));
        }
        return Ok(ReconciliationPreparationReport {
            deterministic_outputs,
            ambiguous_input,
            candidate_traces,
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
            let output = create_new_output(&candidate);
            deterministic_outputs.push(output.clone());
            candidate_traces.push(build_candidate_trace(
                &candidate,
                ReconciliationCandidateDisposition::NoEmbeddingMatches,
                Vec::new(),
                Vec::new(),
                Some(output),
            ));
            continue;
        };
        if top_match.similarity_score >= RECONCILE_EMBEDDING_DETERMINISTIC_THRESHOLD {
            let output = attach_existing_output(
                &candidate,
                &top_match.derived_object_id,
                format!(
                    "Deterministic embedding match against an existing active derived object (similarity {:.3}).",
                    top_match.similarity_score
                ),
            );
            deterministic_outputs.push(output.clone());
            candidate_traces.push(build_candidate_trace(
                &candidate,
                ReconciliationCandidateDisposition::DeterministicEmbeddingMatch,
                Vec::new(),
                matches,
                Some(output),
            ));
            continue;
        }
        if top_match.similarity_score < RECONCILE_EMBEDDING_AMBIGUOUS_THRESHOLD {
            let output = create_new_output(&candidate);
            deterministic_outputs.push(output.clone());
            candidate_traces.push(build_candidate_trace(
                &candidate,
                ReconciliationCandidateDisposition::LowSimilarityCreateNew,
                Vec::new(),
                matches,
                Some(output),
            ));
            continue;
        }
        ambiguous_candidates.push(candidate);
        candidate_traces.push(build_candidate_trace(
            ambiguous_candidates
                .last()
                .expect("candidate was just pushed for ambiguity handling"),
            ReconciliationCandidateDisposition::AmbiguousEmbeddingMatch,
            Vec::new(),
            matches,
            None,
        ));
    }

    let ambiguous_input = if ambiguous_candidates.is_empty() {
        None
    } else {
        let ambiguous_cases = build_ambiguous_reconcile_cases(&candidate_traces);
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

    Ok(ReconciliationPreparationReport {
        deterministic_outputs,
        ambiguous_input,
        candidate_traces,
    })
}

pub(super) fn prepare_reconciliation_work(
    input: &ReconciliationProcessorInput,
    cross_artifact_store: Option<&(dyn CrossArtifactReadStore + Send + Sync)>,
    embedding_provider: Option<&dyn EmbeddingProvider>,
) -> std::result::Result<PreparedReconciliationWork, String> {
    let report = inspect_reconciliation_work(input, cross_artifact_store, embedding_provider)?;
    Ok(PreparedReconciliationWork {
        deterministic_outputs: report.deterministic_outputs,
        ambiguous_input: report.ambiguous_input,
    })
}

pub(super) fn build_reconciliation_decisions(
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

fn entity_target_key(entity: &CandidateEntity) -> String {
    entity.entity_key.clone()
}

pub(super) fn build_derivation_attempt(
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
        });
    }

    let mut attached_existing = HashSet::new();
    let mut candidate_object_ids = HashMap::new();
    for memory in &extraction_result.memories {
        let target_key = memory_target_key(memory);
        let decision = decisions
            .iter()
            .find(|decision| decision.target_kind == "memory" && decision.target_key == target_key);
        if decision.is_some_and(|decision| {
            matches!(
                decision.decision_kind,
                ReconciliationDecisionKind::InsufficientEvidence
            )
        }) {
            continue;
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
        if let Some(existing_id) = decision.and_then(|decision| decision.matched_object_id.as_ref())
        {
            attached_existing.insert(existing_id.clone());
        }
        candidate_object_ids.insert(
            ("memory".to_string(), target_key.clone()),
            derived_object_id.clone(),
        );
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
                        candidate_key: target_key,
                        memory_type: memory.memory_type.clone(),
                        memory_scope: memory.memory_scope,
                        memory_scope_value: memory.memory_scope_value.clone(),
                    },
                },
            },
        });
    }

    for entity in &extraction_result.entities {
        let target_key = entity_target_key(entity);
        let decision = decisions
            .iter()
            .find(|decision| decision.target_kind == "entity" && decision.target_key == target_key);
        if decision.is_some_and(|decision| {
            matches!(
                decision.decision_kind,
                ReconciliationDecisionKind::InsufficientEvidence
            )
        }) {
            continue;
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
        if let Some(existing_id) = decision.and_then(|decision| decision.matched_object_id.as_ref())
        {
            attached_existing.insert(existing_id.clone());
        }
        candidate_object_ids.insert(
            ("entity".to_string(), target_key.clone()),
            derived_object_id.clone(),
        );
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
                    object_json: crate::storage::EntityObjectJson {
                        entity_type: entity.entity_type.clone(),
                        candidate_key: target_key,
                    },
                },
            },
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
            ReconciliationDecisionKind::InsufficientEvidence
        ) {
            continue;
        }

        let target_key = relationship_target_key(relationship);
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
        if let Some(existing_id) = decision.matched_object_id.as_ref() {
            attached_existing.insert(existing_id.clone());
        }
        candidate_object_ids.insert(
            ("relationship".to_string(), target_key),
            derived_object_id.clone(),
        );
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
        &candidate_object_ids,
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
    candidate_object_ids: &HashMap<(String, String), String>,
    decisions: &[ReconciliationDecision],
    contributed_by: &str,
) -> Vec<NewArchiveLink> {
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for decision in decisions {
        let Some(link_type) = reconciliation_link_type(decision.decision_kind) else {
            continue;
        };

        let Some(target_object_id) = decision.matched_object_id.as_ref() else {
            continue;
        };
        let Some(source_object_id) = candidate_object_ids
            .get(&(decision.target_kind.clone(), decision.target_key.clone()))
            .cloned()
        else {
            continue;
        };

        let edge = (
            source_object_id,
            target_object_id.clone(),
            link_type.to_string(),
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

fn reconciliation_link_type(kind: ReconciliationDecisionKind) -> Option<&'static str> {
    match kind {
        ReconciliationDecisionKind::AttachToExisting => Some("same_as"),
        ReconciliationDecisionKind::SupersedeExisting => Some("continues"),
        ReconciliationDecisionKind::ContradictsExisting => Some("contradicts"),
        ReconciliationDecisionKind::CreateNew
        | ReconciliationDecisionKind::InsufficientEvidence => None,
    }
}
