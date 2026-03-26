use std::collections::HashSet;

use crate::storage::types::{RetrievalIntent, ScopeType};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ModelCandidateArtifactOutput {
    summary_draft: ModelSummary,
    classification_candidates: Vec<ModelClassification>,
    memory_candidates: Vec<ModelCandidateMemory>,
    #[serde(default)]
    entity_candidates: Vec<ModelEntity>,
    #[serde(default)]
    relationship_candidates: Vec<ModelRelationship>,
    #[serde(default)]
    retrieval_candidates: Vec<ModelRetrievalIntent>,
    importance_score: u8,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelSummary {
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelClassification {
    classification_type: String,
    classification_value: String,
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelCandidateMemory {
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
    durability_label: String,
    retrieval_value_label: String,
    consequentiality_label: String,
    temporal_scope: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelEntity {
    entity_key: String,
    display_name: String,
    entity_type: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelRelationship {
    relationship_type: String,
    subject_key: String,
    object_key: String,
    title: String,
    body_text: String,
    confidence_label: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelRetrievalIntent {
    question: String,
    query_text: String,
    intent_type: String,
    evidence_segment_ids: Vec<String>,
}

impl ModelCandidateArtifactOutput {
    fn resolve_evidence_aliases(mut self, input: &ArtifactProcessorInput) -> Self {
        let alias_map = build_segment_alias_map(input);

        for segment_id in &mut self.summary_draft.evidence_segment_ids {
            if let Some(actual) = alias_map.get(segment_id.as_str()) {
                *segment_id = actual.clone();
            }
        }

        for classification in &mut self.classification_candidates {
            for segment_id in &mut classification.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for memory in &mut self.memory_candidates {
            for segment_id in &mut memory.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for entity in &mut self.entity_candidates {
            entity.entity_type = canonicalize_entity_type(&entity.entity_type);
            for segment_id in &mut entity.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for relationship in &mut self.relationship_candidates {
            for segment_id in &mut relationship.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for intent in &mut self.retrieval_candidates {
            for segment_id in &mut intent.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        self
    }

    pub(crate) fn into_processor_output(
        self,
        input: &ArtifactProcessorInput,
        model_name: String,
        usage: Option<InferenceUsage>,
        pipeline_name: &str,
        provider_name: &str,
        prompt_version: &str,
    ) -> ArtifactProcessorOutput {
        let resolved = self.resolve_evidence_aliases(input);
        cleanup_artifact_processor_output(ArtifactProcessorOutput {
            pipeline_name: pipeline_name.to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some(provider_name.to_string()),
            model_name: Some(model_name),
            prompt_version: Some(prompt_version.to_string()),
            usage,
            summary: SummaryOutput {
                title: normalize_optional_text(resolved.summary_draft.title),
                body_text: resolved.summary_draft.body_text.trim().to_string(),
                evidence_segment_ids: resolved.summary_draft.evidence_segment_ids,
            },
            classifications: resolved
                .classification_candidates
                .into_iter()
                .map(|classification| ClassificationOutput {
                    title: normalize_optional_text(classification.title),
                    body_text: normalize_optional_text(classification.body_text),
                    classification_type: classification.classification_type,
                    classification_value: classification.classification_value,
                    evidence_segment_ids: classification.evidence_segment_ids,
                })
                .collect(),
            memories: resolved
                .memory_candidates
                .into_iter()
                .map(|memory| {
                    let title = normalize_optional_text(memory.title);
                    let body_text = memory.body_text.trim().to_string();
                    let memory_type = canonicalize_memory_type(
                        "",
                        title.as_deref().unwrap_or_default(),
                        &body_text,
                    );
                    let candidate_key = memory_candidate_key_from_fields(
                        &memory_type,
                        ScopeType::Artifact,
                        &input.artifact_id,
                        title.as_deref(),
                        &body_text,
                    );
                    MemoryOutput {
                        candidate_key,
                        title,
                        body_text,
                        memory_type,
                        memory_scope: ScopeType::Artifact,
                        memory_scope_value: input.artifact_id.clone(),
                        evidence_segment_ids: memory.evidence_segment_ids,
                    }
                })
                .collect(),
            entities: resolved
                .entity_candidates
                .into_iter()
                .map(|entity| EntityOutput {
                    entity_key: entity.entity_key.trim().to_string(),
                    display_name: entity.display_name.trim().to_string(),
                    entity_type: canonicalize_entity_type(&entity.entity_type),
                    evidence_segment_ids: entity.evidence_segment_ids,
                })
                .collect(),
            relationships: resolved
                .relationship_candidates
                .into_iter()
                .map(|relationship| RelationshipOutput {
                    relationship_type: relationship.relationship_type.trim().to_string(),
                    subject_key: relationship.subject_key.trim().to_string(),
                    object_key: relationship.object_key.trim().to_string(),
                    title: normalize_optional_text(relationship.title),
                    body_text: relationship.body_text.trim().to_string(),
                    confidence_label: relationship.confidence_label.trim().to_string(),
                    evidence_segment_ids: relationship.evidence_segment_ids,
                })
                .collect(),
            retrieval_intents: resolved
                .retrieval_candidates
                .into_iter()
                .enumerate()
                .map(|(index, intent)| RetrievalIntent {
                    intent_id: format!("intent-{}", index + 1),
                    question: intent.question.trim().to_string(),
                    query_text: intent.query_text.trim().to_string(),
                    intent_type: intent.intent_type.trim().to_string(),
                    evidence_segment_ids: intent.evidence_segment_ids,
                })
                .collect(),
            importance_score: resolved.importance_score,
        })
    }

    pub(crate) fn validate(
        mut self,
        input: &ArtifactProcessorInput,
    ) -> Result<Self, ProcessorError> {
        let valid_segment_ids_owned = allowed_artifact_evidence_refs(input);
        let valid_segment_ids: HashSet<&str> =
            valid_segment_ids_owned.iter().map(String::as_str).collect();

        validate_text_field("summary_draft.title", &self.summary_draft.title)?;
        validate_text_field("summary_draft.body_text", &self.summary_draft.body_text)?;
        validate_evidence_ids(
            "summary_draft.evidence_segment_ids",
            &self.summary_draft.evidence_segment_ids,
            &valid_segment_ids,
        )?;

        self.classification_candidates = retain_valid_items(
            self.classification_candidates,
            |index, classification| {
                validate_text_field(
                    &format!("classification_candidates[{index}].title"),
                    &classification.title,
                )?;
                validate_text_field(
                    &format!("classification_candidates[{index}].body_text"),
                    &classification.body_text,
                )?;
                validate_text_field(
                    &format!("classification_candidates[{index}].classification_value"),
                    &classification.classification_value,
                )?;
                match classification.classification_type.as_str() {
                    "topic" | "intent" => {}
                    other => {
                        return Err(ProcessorError::InvalidModelOutput {
                            detail: format!(
                                "classification_candidates[{index}].classification_type {other:?} is not allowed"
                            ),
                        })
                    }
                }
                validate_evidence_ids(
                    &format!("classification_candidates[{index}].evidence_segment_ids"),
                    &classification.evidence_segment_ids,
                    &valid_segment_ids,
                )
            },
        );

        self.memory_candidates = retain_valid_items(self.memory_candidates, |index, memory| {
            validate_text_field(&format!("memory_candidates[{index}].title"), &memory.title)?;
            validate_text_field(
                &format!("memory_candidates[{index}].body_text"),
                &memory.body_text,
            )?;
            if !matches!(memory.durability_label.as_str(), "low" | "medium" | "high") {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].durability_label {:?} is not allowed",
                        memory.durability_label
                    ),
                });
            }
            if !matches!(
                memory.retrieval_value_label.as_str(),
                "low" | "medium" | "high"
            ) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].retrieval_value_label {:?} is not allowed",
                        memory.retrieval_value_label
                    ),
                });
            }
            if !matches!(
                memory.consequentiality_label.as_str(),
                "low" | "medium" | "high"
            ) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].consequentiality_label {:?} is not allowed",
                        memory.consequentiality_label
                    ),
                });
            }
            if !matches!(
                memory.temporal_scope.as_str(),
                "ephemeral" | "time_bound" | "ongoing" | "enduring"
            ) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].temporal_scope {:?} is not allowed",
                        memory.temporal_scope
                    ),
                });
            }
            validate_evidence_ids(
                &format!("memory_candidates[{index}].evidence_segment_ids"),
                &memory.evidence_segment_ids,
                &valid_segment_ids,
            )
        });

        self.entity_candidates = retain_valid_items(self.entity_candidates, |index, entity| {
            validate_text_field(
                &format!("entity_candidates[{index}].entity_key"),
                &entity.entity_key,
            )?;
            validate_text_field(
                &format!("entity_candidates[{index}].display_name"),
                &entity.display_name,
            )?;
            validate_text_field(
                &format!("entity_candidates[{index}].entity_type"),
                &entity.entity_type,
            )?;
            validate_evidence_ids(
                &format!("entity_candidates[{index}].evidence_segment_ids"),
                &entity.evidence_segment_ids,
                &valid_segment_ids,
            )
        });

        self.relationship_candidates =
            retain_valid_items(self.relationship_candidates, |index, relationship| {
                validate_text_field(
                    &format!("relationship_candidates[{index}].relationship_type"),
                    &relationship.relationship_type,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].subject_key"),
                    &relationship.subject_key,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].object_key"),
                    &relationship.object_key,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].title"),
                    &relationship.title,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].body_text"),
                    &relationship.body_text,
                )?;
                if !matches!(
                    relationship.confidence_label.as_str(),
                    "low" | "medium" | "high"
                ) {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "relationship_candidates[{index}].confidence_label {:?} is not allowed",
                            relationship.confidence_label
                        ),
                    });
                }
                validate_evidence_ids(
                    &format!("relationship_candidates[{index}].evidence_segment_ids"),
                    &relationship.evidence_segment_ids,
                    &valid_segment_ids,
                )
            });

        self.retrieval_candidates =
            retain_valid_items(self.retrieval_candidates, |index, intent| {
                validate_text_field(
                    &format!("retrieval_candidates[{index}].question"),
                    &intent.question,
                )?;
                validate_text_field(
                    &format!("retrieval_candidates[{index}].query_text"),
                    &intent.query_text,
                )?;
                if !matches!(
                    intent.intent_type.as_str(),
                    "topic_lookup"
                        | "memory_match"
                        | "entity_lookup"
                        | "relationship_lookup"
                        | "contradiction_check"
                ) {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "retrieval_candidates[{index}].intent_type {:?} is not allowed",
                            intent.intent_type
                        ),
                    });
                }
                validate_evidence_ids(
                    &format!("retrieval_candidates[{index}].evidence_segment_ids"),
                    &intent.evidence_segment_ids,
                    &valid_segment_ids,
                )
            });

        if !(1..=10).contains(&self.importance_score) {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "importance_score {} must be between 1 and 10",
                    self.importance_score
                ),
            });
        }

        Ok(self)
    }
}
