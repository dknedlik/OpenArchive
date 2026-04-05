use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::domain::ParticipantRole;
use crate::storage::types::ScopeType;

use super::pipeline::{
    canonicalize_entity_type, normalize_candidate_key_text, normalize_optional_text,
};
use super::*;

pub(crate) const EXTRACTION_PIPELINE_PROMPT_VERSION: &str = "extraction-pipeline-v1";

pub(crate) struct ExtractionPipelineProcessor {
    pub(crate) client: Arc<dyn InferenceClient>,
    pub(crate) extract_model: String,
    pub(crate) format_model: String,
    pub(crate) stage1_max_output_tokens: Option<u32>,
    pub(crate) stage2_max_output_tokens: Option<u32>,
    pub(crate) repair_max_output_tokens: Option<u32>,
    pub(crate) pipeline_name: &'static str,
    pub(crate) provider_name: &'static str,
}

impl ArtifactProcessor for ExtractionPipelineProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        validate_input(input)?;
        let stage1_user_prompt = build_stage1_user_prompt(input)?;
        let stage1_result = match self.stage1_max_output_tokens {
            Some(max_output_tokens) => self.client.complete_text_with_max_output_tokens_override(
                &self.extract_model,
                stage1_system_prompt(),
                &stage1_user_prompt,
                max_output_tokens,
            )?,
            None => self.client.complete_text(
                &self.extract_model,
                stage1_system_prompt(),
                &stage1_user_prompt,
            )?,
        };

        let stage2_user_prompt = build_stage2_user_prompt(input, &stage1_result.output_text);
        match self.process_stage2(input, &stage1_result, &stage2_user_prompt) {
            Ok(output) => Ok(output),
            Err(error) if should_retry_with_repair(&error) => {
                let repair_prompt = build_repair_prompt(&stage2_user_prompt, &error);
                self.process_stage2_with_limit(
                    input,
                    &stage1_result,
                    &repair_prompt,
                    self.repair_max_output_tokens,
                )
            }
            Err(error) => Err(error),
        }
    }
}

impl ExtractionPipelineProcessor {
    fn process_stage2(
        &self,
        input: &ArtifactProcessorInput,
        stage1_result: &InferenceResult,
        stage2_user_prompt: &str,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        self.process_stage2_with_limit(
            input,
            stage1_result,
            stage2_user_prompt,
            self.stage2_max_output_tokens,
        )
    }

    fn process_stage2_with_limit(
        &self,
        input: &ArtifactProcessorInput,
        stage1_result: &InferenceResult,
        stage2_user_prompt: &str,
        max_output_tokens: Option<u32>,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let stage2_result = match max_output_tokens {
            Some(max_output_tokens) => self.client.complete_json_with_max_output_tokens_override(
                &self.format_model,
                stage2_system_prompt(),
                stage2_user_prompt,
                &stage2_schema(),
                max_output_tokens,
            )?,
            None => self.client.complete_json(
                &self.format_model,
                stage2_system_prompt(),
                stage2_user_prompt,
                &stage2_schema(),
            )?,
        };
        let output = parse_extraction_pipeline_output(input, &stage2_result.output_text)
            .map_err(|err| attach_output_preview(err, &stage2_result.output_text))?;

        Ok(ArtifactProcessorOutput {
            pipeline_name: self.pipeline_name.to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some(self.provider_name.to_string()),
            model_name: Some(format!("{} -> {}", self.extract_model, self.format_model)),
            prompt_version: Some(EXTRACTION_PIPELINE_PROMPT_VERSION.to_string()),
            usage: combine_usage(stage1_result.usage.clone(), stage2_result.usage),
            importance_score: estimate_importance_score(&output),
            ..output
        })
    }
}

pub(crate) struct SequentialArtifactBatchProcessor {
    processor: Box<dyn ArtifactProcessor>,
    max_batch_jobs: usize,
    max_batch_bytes: usize,
}

impl SequentialArtifactBatchProcessor {
    pub(crate) fn new(
        processor: Box<dyn ArtifactProcessor>,
        max_batch_jobs: usize,
        max_batch_bytes: usize,
    ) -> Self {
        Self {
            processor,
            max_batch_jobs,
            max_batch_bytes,
        }
    }
}

impl ArtifactBatchProcessor for SequentialArtifactBatchProcessor {
    fn max_batch_jobs(&self) -> usize {
        self.max_batch_jobs
    }

    fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    fn can_process(&self, _input: &ArtifactProcessorInput) -> bool {
        true
    }

    fn estimate_size_bytes(&self, input: &ArtifactProcessorInput) -> Result<usize, ProcessorError> {
        let metadata_bytes = input.title.as_deref().unwrap_or_default().len()
            + input.source_type.as_str().len()
            + input.artifact_class.as_str().len();
        let participant_bytes = input
            .participants
            .iter()
            .map(|participant| {
                participant.participant_id.len()
                    + participant
                        .display_name
                        .as_deref()
                        .unwrap_or_default()
                        .len()
                    + participant.external_id.as_deref().unwrap_or_default().len()
            })
            .sum::<usize>();
        let segment_bytes = input
            .segments
            .iter()
            .map(|segment| segment.text_content.len() + segment.segment_id.len())
            .sum::<usize>();
        Ok(metadata_bytes + participant_bytes + segment_bytes)
    }

    fn process_batch(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
        inputs
            .iter()
            .map(|input| self.processor.process(input))
            .collect()
    }
}

fn combine_usage(
    stage1: Option<InferenceUsage>,
    stage2: Option<InferenceUsage>,
) -> Option<InferenceUsage> {
    match (stage1, stage2) {
        (None, None) => None,
        (Some(usage), None) | (None, Some(usage)) => Some(usage),
        (Some(left), Some(right)) => Some(InferenceUsage {
            input_tokens: sum_optional(left.input_tokens, right.input_tokens),
            output_tokens: sum_optional(left.output_tokens, right.output_tokens),
            reasoning_tokens: sum_optional(left.reasoning_tokens, right.reasoning_tokens),
            total_tokens: sum_optional(left.total_tokens, right.total_tokens),
            reported_cost_micros: sum_optional(
                left.reported_cost_micros,
                right.reported_cost_micros,
            ),
        }),
    }
}

fn sum_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value),
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
    }
}

fn estimate_importance_score(output: &ArtifactProcessorOutput) -> u8 {
    if output.memories.is_empty()
        && output.entities.is_empty()
        && output.relationships.is_empty()
        && output.classifications.is_empty()
    {
        return 2;
    }
    if output.memories.iter().any(|memory| {
        matches!(
            memory.memory_type.as_str(),
            "personal_fact" | "ongoing_state"
        )
    }) {
        return 7;
    }
    if output.memories.len() >= 4 || !output.relationships.is_empty() {
        return 6;
    }
    5
}

fn build_stage1_user_prompt(input: &ArtifactProcessorInput) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptParticipant<'a> {
        participant_id: &'a str,
        participant_role: &'static str,
        display_name: Option<&'a str>,
        external_id: Option<&'a str>,
    }

    let participant_json = if input.participants.is_empty() {
        String::new()
    } else {
        let participants = input
            .participants
            .iter()
            .map(|participant| PromptParticipant {
                participant_id: &participant.participant_id,
                participant_role: participant_role_label(participant.participant_role),
                display_name: participant.display_name.as_deref(),
                external_id: participant.external_id.as_deref(),
            })
            .collect::<Vec<_>>();
        format!(
            "\nParticipants:\n{}\n",
            serde_json::to_string_pretty(&participants)
                .map_err(|source| ProcessorError::SerializePrompt { source })?
        )
    };

    let imported_note_metadata_json = input
        .imported_note_metadata
        .as_ref()
        .map(serde_json::to_string_pretty)
        .transpose()
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let imported_note_metadata_section = imported_note_metadata_json
        .map(|json| format!("\nImported note metadata:\n{json}\n"))
        .unwrap_or_default();

    Ok(format!(
        "Artifact metadata:
- artifact_id: {}
- source_type: {}
- artifact_class: {}
- title: {}
{}{}
Artifact content:
{}",
        input.artifact_id,
        input.source_type.as_str(),
        input.artifact_class.as_str(),
        input.title.as_deref().unwrap_or(""),
        participant_json,
        imported_note_metadata_section,
        render_artifact_text(input),
    ))
}

fn participant_role_label(role: ParticipantRole) -> &'static str {
    match role {
        ParticipantRole::User => "user",
        ParticipantRole::Assistant => "assistant",
        ParticipantRole::System => "system",
        ParticipantRole::Tool => "tool",
        ParticipantRole::Unknown => "unknown",
    }
}

fn render_artifact_text(input: &ArtifactProcessorInput) -> String {
    let participants: HashMap<&str, (&str, &str)> = input
        .participants
        .iter()
        .map(|participant| {
            (
                participant.participant_id.as_str(),
                (
                    participant.display_name.as_deref().unwrap_or_default(),
                    participant_role_label(participant.participant_role),
                ),
            )
        })
        .collect();

    let mut lines = Vec::with_capacity(input.segments.len());
    for segment in &input.segments {
        let mut prefix = None;
        if let Some(participant_id) = segment.participant_id.as_deref() {
            if let Some((display_name, role)) = participants.get(participant_id) {
                prefix = Some(if display_name.is_empty() {
                    format!("[{}]", role)
                } else {
                    format!("[{} | {}]", display_name, role)
                });
            }
        }
        if prefix.is_none() {
            prefix = segment
                .participant_role
                .map(participant_role_label)
                .map(|role| format!("[{}]", role));
        }

        let text = segment.text_content.trim();
        if text.is_empty() {
            continue;
        }
        lines.push(match prefix {
            Some(prefix) => format!("{prefix} {text}"),
            None => text.to_string(),
        });
    }
    lines.join("\n\n")
}

fn stage1_system_prompt() -> &'static str {
    "You are OpenArchive's extraction engine. Read the artifact and identify the highest-value things that should be retrievable later.

Write extraction notes in plain text, not JSON.

Use these top-level headings exactly:
Summary
Classifications
Memories
Entities
Relationships

General rules:
- Be accurate and conservative. Do not add unsupported claims.
- Do not use segment references or citations.
- Prefer fewer, higher-value extractions over exhaustive enumeration.
- Do not repeat yourself across memories, entities, or relationships.
- Keep memories non-overlapping. If two candidate memories describe the same underlying fact, decision, policy, history, or theme, merge them into one stronger memory.
- Split memories only when the facts are independently durable and would be useful to retrieve separately later.
- For reference documents, specifications, engineering rules, and similar non-personal documents, keep only the most durable facts and the most important named systems or technologies.
- For reference documents, do not convert each section, heading, implementation detail, or list item into a separate memory. Prefer principle-level, system-level, or policy-level memories.
- Under Classifications, Memories, Entities, and Relationships, use bullet lists only.

Formatting rules by section:
- Summary: one short paragraph.
- Classifications: one classification label per bullet. Do not combine multiple labels in one bullet.
- Memories: one memory per bullet in this exact format:
  - title: ... | body: ... | memory_type_hint: personal_fact|preference|project_fact|ongoing_state|reference
  Before emitting a memory, ask whether it stands alone or is just a supporting detail of another memory. Prefer the smallest set of memories that preserves the artifact's important durable information.
- Entities: include only named, durable, independently retrievable things such as people, organizations, named software systems, named technologies, named projects, or named medical conditions. One entity per bullet in this exact format:
  - name: ... | type: ... | note: ...
- Relationships: include only explicit or strongly supported relationships between retained named entities. Do not invent relationship chains. One relationship per bullet in this exact format:
  - subject: ... | relationship_type: ... | object: ... | note: ...
  Do not create multiple relationship bullets that restate the same underlying connection.

Never treat the following as entities unless they are clearly proper names of real retained things:
- file paths
- directory names
- commands
- role labels like User or Assistant
- abstract concepts like lineage, supersession, queue, architecture, system, project
- section names, list headings, or pipeline stages"
}

fn stage2_system_prompt() -> &'static str {
    "You are a schema formatter. Read the extraction notes and transcribe them into the required JSON schema exactly.

This is a transcription task, not a reasoning task.

Rules:
- Return valid JSON only.
- Do not add new facts, entities, relationships, or classifications.
- Do not split one bullet into multiple output objects.
- Do not infer entities from memory text.
- Do not infer relationships unless there is an explicit relationship bullet.
- One classification bullet becomes one classification object.
- One memory bullet becomes one memory object.
- One entity bullet becomes one entity object.
- One relationship bullet becomes one relationship object.
- If a section has no valid bullets, return an empty array for that section.
- Preserve the meaning of each bullet as written."
}

fn build_stage2_user_prompt(input: &ArtifactProcessorInput, stage1_notes: &str) -> String {
    format!(
        "Artifact metadata:
- artifact_id: {}
- source_type: {}
- title: {}

Formatting rules:
- Set every memory_scope to \"artifact\".
- Set every memory_scope_value to the artifact_id.
- Copy memory_type_hint directly into memory_type.
- Use only explicit entity bullets to create entities.
- Use only explicit relationship bullets to create relationships.
- Do not create entities, relationships, or classifications from the summary paragraph.
- Keep classification_type short and consistent.

Extraction notes:
{}",
        input.artifact_id,
        input.source_type.as_str(),
        input.title.as_deref().unwrap_or(""),
        stage1_notes
    )
}

fn stage2_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary",
            "classifications",
            "memories",
            "entities",
            "relationships"
        ],
        "properties": {
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "required": ["title", "body_text"],
                "properties": {
                    "title": { "type": "string" },
                    "body_text": { "type": "string" }
                }
            },
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["label", "classification_type"],
                    "properties": {
                        "label": { "type": "string" },
                        "classification_type": { "type": "string" }
                    }
                }
            },
            "memories": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "title",
                        "body_text",
                        "memory_type",
                        "memory_scope",
                        "memory_scope_value"
                    ],
                    "properties": {
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "memory_type": {
                            "type": "string",
                            "enum": [
                                "personal_fact",
                                "preference",
                                "project_fact",
                                "ongoing_state",
                                "reference"
                            ]
                        },
                        "memory_scope": {
                            "type": "string",
                            "enum": ["artifact"]
                        },
                        "memory_scope_value": { "type": "string" }
                    }
                }
            },
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["display_name", "entity_type"],
                    "properties": {
                        "display_name": { "type": "string" },
                        "entity_type": { "type": "string" }
                    }
                }
            },
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "relationship_type",
                        "subject_name",
                        "object_name",
                        "title",
                        "body_text"
                    ],
                    "properties": {
                        "relationship_type": { "type": "string" },
                        "subject_name": { "type": "string" },
                        "object_name": { "type": "string" },
                        "title": { "type": "string" },
                        "body_text": { "type": "string" }
                    }
                }
            }
        }
    })
}

#[derive(Debug, Deserialize)]
struct RedesignStage2Output {
    summary: RedesignSummary,
    classifications: Vec<RedesignClassification>,
    memories: Vec<RedesignMemory>,
    entities: Vec<RedesignEntity>,
    relationships: Vec<RedesignRelationship>,
}

#[derive(Debug, Deserialize)]
struct RedesignSummary {
    title: String,
    body_text: String,
}

#[derive(Debug, Deserialize)]
struct RedesignClassification {
    label: String,
    classification_type: String,
}

#[derive(Debug, Deserialize)]
struct RedesignMemory {
    title: String,
    body_text: String,
    memory_type: String,
    memory_scope: String,
    memory_scope_value: String,
}

#[derive(Debug, Deserialize)]
struct RedesignEntity {
    display_name: String,
    entity_type: String,
}

#[derive(Debug, Deserialize)]
struct RedesignRelationship {
    relationship_type: String,
    subject_name: String,
    object_name: String,
    title: String,
    body_text: String,
}

fn parse_extraction_pipeline_output(
    input: &ArtifactProcessorInput,
    output_text: &str,
) -> Result<ArtifactProcessorOutput, ProcessorError> {
    let parsed: RedesignStage2Output =
        serde_json::from_str(output_text).map_err(|source| ProcessorError::ParseModelJson {
            source,
            body_preview: preview(output_text),
        })?;

    if parsed
        .memories
        .iter()
        .any(|memory| memory.memory_scope != "artifact")
    {
        return Err(ProcessorError::InvalidModelOutput {
            detail: "every extraction-pipeline memory must use memory_scope=artifact".to_string(),
        });
    }

    let summary = SummaryOutput {
        title: normalize_optional_text(parsed.summary.title),
        body_text: parsed.summary.body_text.trim().to_string(),
    };

    let classifications = parsed
        .classifications
        .into_iter()
        .filter(|classification| !classification.label.trim().is_empty())
        .map(|classification| ClassificationOutput {
            title: normalize_optional_text(classification.label.clone()),
            body_text: None,
            classification_type: if classification.classification_type.trim().is_empty() {
                "topic".to_string()
            } else {
                classification.classification_type.trim().to_string()
            },
            classification_value: classification.label.trim().to_string(),
        })
        .collect::<Vec<_>>();

    let memories = dedupe_memories(
        parsed
            .memories
            .into_iter()
            .filter(|memory| !memory.body_text.trim().is_empty())
            .map(|memory| {
                let title = normalize_optional_text(memory.title);
                let body_text = memory.body_text.trim().to_string();
                let candidate_key = memory_candidate_key_from_fields(
                    memory.memory_type.trim(),
                    ScopeType::Artifact,
                    &input.artifact_id,
                    title.as_deref(),
                    &body_text,
                );
                MemoryOutput {
                    candidate_key,
                    title,
                    body_text,
                    memory_type: memory.memory_type.trim().to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: memory.memory_scope_value.trim().to_string(),
                }
            })
            .collect(),
    );

    let entities = dedupe_entities(
        parsed
            .entities
            .into_iter()
            .filter(|entity| !entity.display_name.trim().is_empty())
            .map(|entity| EntityOutput {
                entity_key: entity_key_from_display_name(
                    entity.display_name.trim(),
                    entity.entity_type.trim(),
                ),
                display_name: entity.display_name.trim().to_string(),
                entity_type: canonicalize_entity_type(&entity.entity_type),
            })
            .collect(),
    );

    let entity_name_lookup: HashMap<String, String> = entities
        .iter()
        .map(|entity| {
            (
                normalize_candidate_key_text(&entity.display_name),
                entity.entity_key.clone(),
            )
        })
        .collect();

    let relationships = dedupe_relationships(
        parsed
            .relationships
            .into_iter()
            .filter(|relationship| !relationship.relationship_type.trim().is_empty())
            .map(|relationship| {
                let subject_key = entity_name_lookup
                    .get(&normalize_candidate_key_text(&relationship.subject_name))
                    .cloned()
                    .ok_or_else(|| ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "relationship subject {:?} does not match an emitted entity",
                            relationship.subject_name
                        ),
                    })?;
                let object_key = entity_name_lookup
                    .get(&normalize_candidate_key_text(&relationship.object_name))
                    .cloned()
                    .ok_or_else(|| ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "relationship object {:?} does not match an emitted entity",
                            relationship.object_name
                        ),
                    })?;
                Ok(RelationshipOutput {
                    relationship_type: relationship.relationship_type.trim().to_string(),
                    subject_key,
                    object_key,
                    title: normalize_optional_text(relationship.title),
                    body_text: relationship.body_text.trim().to_string(),
                    confidence_label: "high".to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    );

    Ok(ArtifactProcessorOutput {
        pipeline_name: String::new(),
        pipeline_version: String::new(),
        provider_name: None,
        model_name: None,
        prompt_version: None,
        usage: None,
        summary,
        classifications,
        memories,
        entities,
        relationships,
        importance_score: 5,
    })
}

fn entity_key_from_display_name(display_name: &str, entity_type: &str) -> String {
    let normalized_name = normalize_candidate_key_text(display_name);
    let normalized_type = normalize_candidate_key_text(entity_type);
    format!("entity:{normalized_name}:{normalized_type}")
}

fn dedupe_memories(memories: Vec<MemoryOutput>) -> Vec<MemoryOutput> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for memory in memories {
        if seen.insert(memory.candidate_key.clone()) {
            deduped.push(memory);
        }
    }
    deduped
}

fn dedupe_entities(entities: Vec<EntityOutput>) -> Vec<EntityOutput> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for entity in entities {
        if seen.insert(entity.entity_key.clone()) {
            deduped.push(entity);
        }
    }
    deduped
}

fn dedupe_relationships(relationships: Vec<RelationshipOutput>) -> Vec<RelationshipOutput> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for relationship in relationships {
        let dedupe_key = format!(
            "{}|{}|{}|{}",
            normalize_candidate_key_text(&relationship.relationship_type),
            relationship.subject_key,
            relationship.object_key,
            normalize_candidate_key_text(&relationship.body_text),
        );
        if seen.insert(dedupe_key) {
            deduped.push(relationship);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use crate::storage::types::{ArtifactClass, LoadedSegment, SourceType};

    use super::*;

    fn sample_input() -> ArtifactProcessorInput {
        ArtifactProcessorInput {
            artifact_id: "artifact-1".to_string(),
            import_id: "import-1".to_string(),
            artifact_class: ArtifactClass::Conversation,
            source_type: SourceType::ChatGptExport,
            title: Some("Architecture direction".to_string()),
            imported_note_metadata: None,
            participants: Vec::new(),
            segments: vec![LoadedSegment {
                segment_id: "seg-1".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 0,
                text_content: "OpenArchive uses Rust and MCP.".to_string(),
                created_at_source: None,
                visibility_status: crate::VisibilityStatus::Visible,
            }],
        }
    }

    #[test]
    fn parse_extraction_pipeline_output_maps_entities_and_relationships() {
        let output = parse_extraction_pipeline_output(
            &sample_input(),
            &json!({
                "summary": {
                    "title": "Architecture direction",
                    "body_text": "OpenArchive is standardizing on Rust and MCP."
                },
                "classifications": [
                    {
                        "label": "Architecture Design",
                        "classification_type": "topic"
                    }
                ],
                "memories": [
                    {
                        "title": "OpenArchive uses Rust",
                        "body_text": "OpenArchive is implemented in synchronous Rust.",
                        "memory_type": "project_fact",
                        "memory_scope": "artifact",
                        "memory_scope_value": "artifact-1"
                    }
                ],
                "entities": [
                    {
                        "display_name": "OpenArchive",
                        "entity_type": "project"
                    },
                    {
                        "display_name": "Rust",
                        "entity_type": "named technology"
                    }
                ],
                "relationships": [
                    {
                        "relationship_type": "implemented_in",
                        "subject_name": "OpenArchive",
                        "object_name": "Rust",
                        "title": "Implemented in Rust",
                        "body_text": "The project is implemented in Rust."
                    }
                ]
            })
            .to_string(),
        )
        .expect("extraction pipeline output should parse");

        assert_eq!(output.memories.len(), 1);
        assert_eq!(output.entities.len(), 2);
        assert_eq!(output.relationships.len(), 1);
        assert_eq!(
            output.relationships[0].subject_key,
            output.entities[0].entity_key
        );
        assert_eq!(
            output.relationships[0].object_key,
            output.entities[1].entity_key
        );
    }
}
