use std::sync::Arc;

use crate::storage::types::{
    ArtifactClass, EnrichmentTier, LoadedSegment, ReconciliationDecisionKind, ScopeType, SourceType,
};

use super::*;

struct FixedInferenceClient {
    text_response: String,
    json_response: String,
}

impl InferenceClient for FixedInferenceClient {
    fn complete_text(
        &self,
        _model: &str,
        _system_prompt: &str,
        _user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        Ok(InferenceResult {
            output_text: self.text_response.clone(),
            usage: None,
        })
    }

    fn complete_json(
        &self,
        _model: &str,
        _system_prompt: &str,
        _user_prompt: &str,
        _schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        Ok(InferenceResult {
            output_text: self.json_response.clone(),
            usage: None,
        })
    }
}

struct SequenceInferenceClient {
    responses: std::sync::Mutex<Vec<String>>,
}

impl InferenceClient for SequenceInferenceClient {
    fn complete_text(
        &self,
        _model: &str,
        _system_prompt: &str,
        _user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        let mut responses = self.responses.lock().expect("sequence lock");
        if responses.is_empty() {
            return Err(ProcessorError::Message {
                message: "no more inference responses".to_string(),
            });
        }
        Ok(InferenceResult {
            output_text: responses.remove(0),
            usage: None,
        })
    }

    fn complete_json(
        &self,
        _model: &str,
        _system_prompt: &str,
        _user_prompt: &str,
        _schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        let mut responses = self.responses.lock().expect("sequence lock");
        if responses.is_empty() {
            return Err(ProcessorError::Message {
                message: "no more inference responses".to_string(),
            });
        }
        Ok(InferenceResult {
            output_text: responses.remove(0),
            usage: None,
        })
    }
}

fn sample_input() -> ArtifactProcessorInput {
    ArtifactProcessorInput {
        artifact_id: "artifact-1".to_string(),
        import_id: "import-1".to_string(),
        artifact_class: ArtifactClass::Conversation,
        source_type: SourceType::ChatGptExport,
        title: Some("Architecture direction".to_string()),
        imported_note_metadata: None,
        participants: Vec::new(),
        segments: vec![
            LoadedSegment {
                segment_id: "seg-1".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 0,
                text_content: "We should remove silent fallback.".to_string(),
                created_at_source: None,
                visibility_status: crate::VisibilityStatus::Visible,
            },
            LoadedSegment {
                segment_id: "seg-2".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 1,
                text_content: "Task 12 should use GPT-4.1 mini.".to_string(),
                created_at_source: None,
                visibility_status: crate::VisibilityStatus::Visible,
            },
        ],
    }
}

fn sample_extraction_pipeline_stage1_notes() -> String {
    "Summary
Hardening worker failure behavior and the next milestone.

Classifications
- fix_failure_handling

Memories
- title: Task 12 uses hosted inference | body: The first real pipeline uses a hosted inference provider. | memory_type_hint: project_fact

Entities

Relationships"
        .to_string()
}

fn sample_extraction_pipeline_stage2_response() -> String {
    serde_json::json!({
        "summary": {
            "title": "Candidate summary",
            "body_text": "The artifact discusses hardening worker failure behavior and the next milestone for the real pipeline."
        },
        "classifications": [
            {
                "label": "fix_failure_handling",
                "classification_type": "intent"
            }
        ],
        "memories": [
            {
                "title": "Task 12 uses hosted inference",
                "body_text": "The first real pipeline uses a hosted inference provider.",
                "memory_type": "project_fact",
                "memory_scope": "artifact",
                "memory_scope_value": "artifact-1"
            }
        ],
        "entities": [],
        "relationships": []
    })
    .to_string()
}

#[test]
fn openai_processor_parses_and_validates_output() {
    let client = Arc::new(FixedInferenceClient {
        text_response: sample_extraction_pipeline_stage1_notes(),
        json_response: sample_extraction_pipeline_stage2_response(),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Default)
        .expect("processor should build");

    let output = processor
        .process(&sample_input())
        .expect("processor should succeed");

    assert_eq!(output.pipeline_name, "openai_extraction_pipeline");
    assert_eq!(output.classifications.len(), 1);
    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.importance_score, 5);
}

#[test]
fn openai_processor_rejects_invalid_extraction_pipeline_memory_scope() {
    let client = Arc::new(FixedInferenceClient {
        text_response: sample_extraction_pipeline_stage1_notes(),
        json_response: serde_json::json!({
            "summary": {
                "title": "Bad extraction pipeline output",
                "body_text": "The model emitted an invalid memory scope."
            },
            "classifications": [
                {
                    "label": "worker_failure_handling",
                    "classification_type": "topic"
                }
            ],
            "memories": [
                {
                    "title": "Bad memory scope",
                    "body_text": "This should be rejected.",
                    "memory_type": "project_fact",
                    "memory_scope": "workspace",
                    "memory_scope_value": "artifact-1"
                }
            ],
            "entities": [],
            "relationships": []
        })
        .to_string(),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Default)
        .expect("processor should build");

    let err = processor
        .process(&sample_input())
        .expect_err("processor should fail");
    assert!(matches!(err, ProcessorError::InvalidModelOutput { .. }));
}

#[test]
fn openai_processor_retries_once_on_invalid_output_and_accepts_repair() {
    let client = Arc::new(SequenceInferenceClient {
        responses: std::sync::Mutex::new(vec![
            sample_extraction_pipeline_stage1_notes(),
            serde_json::json!({
                "summary": {
                    "title": "Bad extraction pipeline output",
                    "body_text": "The model emitted an invalid memory scope."
                },
                "classifications": [],
                "memories": [
                    {
                        "title": "Bad memory scope",
                        "body_text": "This should be rejected.",
                        "memory_type": "project_fact",
                        "memory_scope": "workspace",
                        "memory_scope_value": "artifact-1"
                    }
                ],
                "entities": [],
                "relationships": []
            })
            .to_string(),
            sample_extraction_pipeline_stage2_response(),
        ]),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Default)
        .expect("processor should build");

    let output = processor
        .process(&sample_input())
        .expect("processor should repair and succeed");

    assert_eq!(output.classifications.len(), 1);
    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.importance_score, 5);
}

#[test]
fn openai_processor_drops_invalid_objects_without_rejecting_artifact() {
    let client = Arc::new(FixedInferenceClient {
        text_response: "Summary
Logo symbol preferences.

Classifications

Memories
- title: Preferred HashBooks Logo Symbol | body: The original hash-in-book symbol should remain the preferred logo mark. | memory_type_hint: preference

Entities

Relationships"
            .to_string(),
        json_response: serde_json::json!({
            "summary": {
                "title": "Logo symbol preferences",
                "body_text": "The artifact prefers the original hash-in-book symbol."
            },
            "classifications": [],
            "memories": [
                {
                    "title": "Preferred HashBooks Logo Symbol",
                    "body_text": "The original hash-in-book symbol should remain the preferred logo mark.",
                    "memory_type": "preference",
                    "memory_scope": "artifact",
                    "memory_scope_value": "artifact-1"
                },
                {
                    "title": "Prefer original logo symbol",
                    "body_text": "   ",
                    "memory_type": "preference",
                    "memory_scope": "artifact",
                    "memory_scope_value": "artifact-1"
                }
            ],
            "entities": [],
            "relationships": []
        })
        .to_string(),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-5-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Default)
        .expect("processor should build");

    let output = processor
        .process(&sample_input())
        .expect("processor should salvage valid objects");
    assert_eq!(
        output.summary.body_text,
        "The artifact prefers the original hash-in-book symbol."
    );
    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.memories[0].memory_scope_value, "artifact-1");
}

#[test]
fn cleanup_preserves_output_counts_without_archetype_caps() {
    let output = cleanup_artifact_processor_output(ArtifactProcessorOutput {
        pipeline_name: "test".to_string(),
        pipeline_version: "v1".to_string(),
        provider_name: None,
        model_name: None,
        prompt_version: None,
        usage: None,
        summary: SummaryOutput {
            title: Some("Dashboard".to_string()),
            body_text: "Navigation and reusable query helpers.".to_string(),
        },
        classifications: (0..5)
            .map(|index| ClassificationOutput {
                title: Some(format!("Classification {index}")),
                body_text: Some("Body".to_string()),
                classification_type: "topic".to_string(),
                classification_value: format!("value-{index}"),
            })
            .collect(),
        memories: (0..5)
            .map(|index| MemoryOutput {
                candidate_key: format!("memory-{index}"),
                title: Some(format!("Memory {index}")),
                body_text: format!("Body {index}"),
                memory_type: "reference".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: "artifact-dashboard-1".to_string(),
            })
            .collect(),
        entities: (0..5)
            .map(|index| EntityOutput {
                entity_key: format!("entity-{index}"),
                display_name: format!("Entity {index}"),
                entity_type: "system".to_string(),
            })
            .collect(),
        relationships: vec![
            RelationshipOutput {
                relationship_type: "references".to_string(),
                subject_key: "entity-0".to_string(),
                object_key: "entity-1".to_string(),
                title: Some("r1".to_string()),
                body_text: "r1".to_string(),
                confidence_label: "medium".to_string(),
            },
            RelationshipOutput {
                relationship_type: "references".to_string(),
                subject_key: "entity-2".to_string(),
                object_key: "entity-3".to_string(),
                title: Some("r2".to_string()),
                body_text: "r2".to_string(),
                confidence_label: "medium".to_string(),
            },
        ],
        importance_score: 3,
    });

    assert_eq!(output.classifications.len(), 5);
    assert_eq!(output.memories.len(), 5);
    assert_eq!(output.entities.len(), 5);
    assert_eq!(output.relationships.len(), 2);
}

#[test]
fn cleanup_dedupes_and_sanitizes_outputs_generically() {
    let output = cleanup_artifact_processor_output(ArtifactProcessorOutput {
        pipeline_name: "test".to_string(),
        pipeline_version: "v1".to_string(),
        provider_name: None,
        model_name: None,
        prompt_version: None,
        usage: None,
        summary: SummaryOutput {
            title: Some("Meeting".to_string()),
            body_text: "Structured meeting note.".to_string(),
        },
        classifications: Vec::new(),
        memories: vec![
            MemoryOutput {
                candidate_key: "m1".to_string(),
                title: Some("Decision".to_string()),
                body_text: "Ship the retry fix.".to_string(),
                memory_type: "project_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: "artifact-1".to_string(),
            },
            MemoryOutput {
                candidate_key: "m1".to_string(),
                title: Some("Decision".to_string()),
                body_text: "Ship the retry fix this week.".to_string(),
                memory_type: "project_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: "artifact-1".to_string(),
            },
        ],
        entities: vec![
            EntityOutput {
                entity_key: "entity-a".to_string(),
                display_name: "Alice".to_string(),
                entity_type: "person".to_string(),
            },
            EntityOutput {
                entity_key: "entity-a-duplicate".to_string(),
                display_name: "Alice".to_string(),
                entity_type: "person".to_string(),
            },
            EntityOutput {
                entity_key: "entity-b".to_string(),
                display_name: "Worker".to_string(),
                entity_type: "system".to_string(),
            },
        ],
        relationships: vec![
            RelationshipOutput {
                relationship_type: "assigned_to".to_string(),
                subject_key: "entity-b".to_string(),
                object_key: "entity-a".to_string(),
                title: Some("Owner".to_string()),
                body_text: "Worker is assigned to Alice.".to_string(),
                confidence_label: "medium".to_string(),
            },
            RelationshipOutput {
                relationship_type: "assigned_to".to_string(),
                subject_key: "entity-b".to_string(),
                object_key: "entity-a".to_string(),
                title: Some("Owner".to_string()),
                body_text: "Worker is assigned to Alice for the retry fix.".to_string(),
                confidence_label: "high".to_string(),
            },
            RelationshipOutput {
                relationship_type: "references".to_string(),
                subject_key: "entity-b".to_string(),
                object_key: "entity-b".to_string(),
                title: Some("Self".to_string()),
                body_text: "Invalid self relationship.".to_string(),
                confidence_label: "high".to_string(),
            },
            RelationshipOutput {
                relationship_type: "references".to_string(),
                subject_key: "missing".to_string(),
                object_key: "entity-a".to_string(),
                title: Some("Missing".to_string()),
                body_text: "Invalid missing relationship.".to_string(),
                confidence_label: "high".to_string(),
            },
        ],
        importance_score: 3,
    });

    assert_eq!(output.memories.len(), 1);
    assert_eq!(
        output.memories[0].body_text,
        "Ship the retry fix this week."
    );
    assert_eq!(output.entities.len(), 2);
    assert_eq!(output.relationships.len(), 1);
    assert_eq!(output.relationships[0].confidence_label, "high");
}

fn sample_reconciliation_input() -> ReconciliationProcessorInput {
    ReconciliationProcessorInput {
        artifact_id: "artifact-1".to_string(),
        source_type: SourceType::ChatGptExport,
        summary: SummaryOutput {
            title: Some("Architecture direction".to_string()),
            body_text: "The user wants the worker to avoid silent fallback.".to_string(),
        },
        memories: vec![MemoryOutput {
            candidate_key: "memory_1".to_string(),
            title: Some("Avoid silent fallback".to_string()),
            body_text: "The worker should not silently fall back.".to_string(),
            memory_type: "project_fact".to_string(),
            memory_scope: ScopeType::Artifact,
            memory_scope_value: "artifact-1".to_string(),
        }],
        entities: vec![EntityOutput {
            entity_key: "worker".to_string(),
            display_name: "Worker".to_string(),
            entity_type: "system".to_string(),
        }],
        relationships: Vec::new(),
        retrieval_results_json: serde_json::json!({
            "objects": [
                {
                    "object_id": "obj-1",
                    "title": "Silent fallback rule",
                    "body_text": "The worker should not silently fall back."
                }
            ]
        })
        .to_string(),
    }
}

#[test]
fn reconciliation_prompt_is_narrow_comparison_prompt() {
    let input = sample_reconciliation_input();
    let prompt = build_reconciliation_prompt(&input).expect("prompt should build");

    assert!(prompt.contains("Compare ambiguous extracted candidates"));
    assert!(prompt.contains("candidate_match_options:"));
    assert!(prompt.contains("compare it only to the provided candidate_match_options"));
    assert!(!prompt.contains("retrieval_results:"));
    assert!(!prompt.contains("Reconcile extraction candidates against archive retrieval results."));
}

#[test]
fn reconciliation_normalizes_ungrounded_existing_decision_to_create_new() {
    let input = sample_reconciliation_input();
    let output = ModelReconciliationOutput {
        decisions: vec![
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::StrengthenExisting,
                target_kind: "memory".to_string(),
                target_key: "memory_1".to_string(),
                matched_object_id: None,
                rationale: "This looks like the same memory.".to_string(),
            },
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "entity".to_string(),
                target_key: "worker".to_string(),
                matched_object_id: None,
                rationale: "This entity should remain distinct.".to_string(),
            },
        ],
    };

    let decisions = output
        .into_validated_outputs(&input)
        .expect("normalization should preserve the candidate");

    assert_eq!(decisions.len(), 2);
    assert_eq!(
        decisions[0].decision_kind,
        ReconciliationDecisionKind::CreateNew
    );
    assert_eq!(decisions[0].matched_object_id, None);
}

#[test]
fn reconciliation_requires_id_for_existing_object_decisions_after_normalization() {
    let input = sample_reconciliation_input();
    let output = ModelReconciliationOutput {
        decisions: vec![
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::StrengthenExisting,
                target_kind: "memory".to_string(),
                target_key: "memory_1".to_string(),
                matched_object_id: Some("obj-1".to_string()),
                rationale: "This clearly matches the prior memory.".to_string(),
            },
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "entity".to_string(),
                target_key: "worker".to_string(),
                matched_object_id: None,
                rationale: "This entity should remain distinct.".to_string(),
            },
        ],
    };

    let decisions = output
        .into_validated_outputs(&input)
        .expect("grounded existing-object decision should remain valid");

    assert_eq!(decisions.len(), 2);
    assert_eq!(
        decisions[0].decision_kind,
        ReconciliationDecisionKind::StrengthenExisting
    );
    assert_eq!(decisions[0].matched_object_id.as_deref(), Some("obj-1"));
}

#[test]
fn reconciliation_rejects_mismatched_target_kind_for_entity_candidate() {
    let input = sample_reconciliation_input();
    let output = ModelReconciliationOutput {
        decisions: vec![
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "memory".to_string(),
                target_key: "memory_1".to_string(),
                matched_object_id: None,
                rationale: "This memory should remain distinct.".to_string(),
            },
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "memory".to_string(),
                target_key: "worker".to_string(),
                matched_object_id: None,
                rationale: "This entity is mislabeled on purpose.".to_string(),
            },
        ],
    };

    let err = output
        .into_validated_outputs(&input)
        .expect_err("entity target kind mismatch should be rejected");

    match err {
        ProcessorError::InvalidModelOutput { detail } => {
            assert!(detail.contains("target_kind"));
            assert!(detail.contains("candidate kind"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
