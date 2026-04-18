use super::context::{ExtractionPolicy, WorkerContext};
use super::jobs::{fail_job, fail_job_message, handle_processor_error};
use super::{complete_job, new_id};
use crate::config::ExtractionChunkingConfig;
use crate::extraction_chunking::{build_chunk_inputs, build_topic_thread_inputs};
use crate::processor::{
    cleanup_artifact_processor_output, ArtifactProcessorInput, ArtifactProcessorOutput,
    ClassificationOutput, EntityOutput, MemoryOutput, RelationshipOutput, SummaryOutput,
};
use crate::storage::{
    ArtifactExtractPayload, ArtifactExtractionResult, CandidateEntity, CandidateRelationship,
    ClaimedJob, ConversationWindowRef, ExtractedClassification, ExtractedMemory, NewEnrichmentJob,
};
use std::collections::BTreeMap;

pub(super) fn process_extract_job_batch(
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
        for ((claimed_job, payload, input), result) in batchable.into_iter().zip(results) {
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
                        payload_json: crate::storage::ArtifactReconcilePayload::new_v1(
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

pub(super) fn process_extract_job(
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
        payload_json: crate::storage::ArtifactReconcilePayload::new_v1(
            &claimed_job.artifact_id,
            &payload.import_id,
            source_type,
            &extraction_result.extraction_result_id,
        )
        .to_json(),
    };
    ctx.job_store
        .enqueue_jobs(&[reconcile_job])
        .map_err(|err| {
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
        classifications: output
            .classifications
            .iter()
            .map(|classification| ExtractedClassification {
                title: classification.title.clone(),
                body_text: classification.body_text.clone(),
                classification_type: classification.classification_type.clone(),
                classification_value: classification.classification_value.clone(),
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
            })
            .collect(),
        entities: output
            .entities
            .iter()
            .map(|entity| CandidateEntity {
                entity_key: entity.entity_key.clone(),
                display_name: entity.display_name.clone(),
                entity_type: entity.entity_type.clone(),
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
            })
            .collect(),
        status: "completed".to_string(),
        error_message: None,
    }
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
    let mut classifications = BTreeMap::<(String, String), ClassificationOutput>::new();
    let mut memories = BTreeMap::<String, MemoryOutput>::new();
    let mut entities = BTreeMap::<String, EntityOutput>::new();
    let mut relationships = BTreeMap::<String, RelationshipOutput>::new();
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
        },
        classifications: classifications.into_values().collect(),
        memories: memories.into_values().collect(),
        entities: entities.into_values().collect(),
        relationships: relationships.into_values().collect(),
        importance_score,
    })
}

fn build_extract_chunk_inputs(
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

fn memory_target_key_from_output(memory: &MemoryOutput) -> String {
    memory.candidate_key.clone()
}

fn relationship_target_key_from_output(relationship: &RelationshipOutput) -> String {
    format!(
        "{}:{}:{}",
        relationship.relationship_type, relationship.subject_key, relationship.object_key
    )
}

fn merge_classification_output(target: &mut ClassificationOutput, incoming: &ClassificationOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    target.body_text =
        prefer_richer_optional_text(target.body_text.take(), incoming.body_text.clone());
}

fn merge_memory_output(target: &mut MemoryOutput, incoming: &MemoryOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
}

fn merge_entity_output(target: &mut EntityOutput, incoming: &EntityOutput) {
    if incoming.display_name.len() > target.display_name.len() {
        target.display_name = incoming.display_name.clone();
    }
    if incoming.entity_type.len() > target.entity_type.len() {
        target.entity_type = incoming.entity_type.clone();
    }
}

fn merge_relationship_output(target: &mut RelationshipOutput, incoming: &RelationshipOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    if confidence_rank(&incoming.confidence_label) > confidence_rank(&target.confidence_label) {
        target.confidence_label = incoming.confidence_label.clone();
    }
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
