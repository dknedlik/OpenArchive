use std::sync::Arc;

use crate::storage::types::{
    ArtifactClass, EnrichmentTier, LoadedSegment, ReconciliationDecisionKind, ScopeType, SourceType,
};

use super::*;

struct FixedInferenceClient {
    candidate_response: String,
}

impl InferenceClient for FixedInferenceClient {
    fn complete_json(
        &self,
        _model: &str,
        _system_prompt: &str,
        _user_prompt: &str,
        _schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        Ok(InferenceResult {
            output_text: self.candidate_response.clone(),
            usage: None,
        })
    }
}

struct SequenceInferenceClient {
    responses: std::sync::Mutex<Vec<String>>,
}

impl InferenceClient for SequenceInferenceClient {
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

fn sample_document_input() -> ArtifactProcessorInput {
    ArtifactProcessorInput {
        artifact_id: "artifact-doc-1".to_string(),
        import_id: "import-doc-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::MarkdownFile,
        title: Some("Sprint planning notes".to_string()),
        participants: Vec::new(),
        segments: vec![
            LoadedSegment {
                segment_id: "seg-1".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 0,
                text_content: "Sprint planning with Alice and Bob for OpenArchive ingestion work."
                    .to_string(),
                created_at_source: None,
                visibility_status: crate::VisibilityStatus::Visible,
            },
            LoadedSegment {
                segment_id: "seg-2".to_string(),
                participant_id: None,
                participant_role: None,
                sequence_no: 1,
                text_content:
                    "Decision: ship markdown and text ingestion before docx. Alice owns parser work."
                        .to_string(),
                created_at_source: None,
                visibility_status: crate::VisibilityStatus::Visible,
            },
        ],
    }
}

fn sample_candidate_response() -> String {
    serde_json::json!({
        "summary_draft": {
            "title": "Candidate summary",
            "body_text": "The artifact discusses hardening worker failure behavior and the next milestone for the real pipeline.",
            "evidence_segment_ids": ["evidence_ref_1", "evidence_ref_2"]
        },
        "classification_candidates": [
            {
                "classification_type": "intent",
                "classification_value": "fix_failure_handling",
                "title": "Fix failure handling",
                "body_text": "The artifact is focused on hardening worker failure behavior.",
                "evidence_segment_ids": ["evidence_ref_1"]
            }
        ],
        "memory_candidates": [
            {
                "title": "Task 12 uses hosted inference",
                "body_text": "The first real pipeline uses a hosted inference provider.",
                "evidence_segment_ids": ["evidence_ref_2"],
                "durability_label": "high",
                "retrieval_value_label": "high",
                "consequentiality_label": "medium",
                "temporal_scope": "enduring"
            }
        ],
        "entity_candidates": [],
        "relationship_candidates": [],
        "retrieval_candidates": [],
        "importance_score": 8
    })
    .to_string()
}

#[test]
fn openai_processor_parses_and_validates_output() {
    let client = Arc::new(FixedInferenceClient {
        candidate_response: sample_candidate_response(),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Standard)
        .expect("processor should build");

    let output = processor
        .process(&sample_input())
        .expect("processor should succeed");

    assert_eq!(output.pipeline_name, "openai_enrichment");
    assert_eq!(output.classifications.len(), 1);
    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.summary.evidence_segment_ids, vec!["seg-1", "seg-2"]);
    assert_eq!(output.importance_score, 8);
}

#[test]
fn openai_processor_rejects_unknown_evidence_segment_ids() {
    let client = Arc::new(FixedInferenceClient {
        candidate_response: serde_json::json!({
            "summary_draft": {
                "title": "Bad evidence",
                "body_text": "The model referenced an unknown segment.",
                "evidence_segment_ids": ["evidence_ref_99"]
            },
            "classification_candidates": [
                {
                    "classification_type": "topic",
                    "classification_value": "worker_failure_handling",
                    "title": "Worker hardening",
                    "body_text": "Specific hardening work.",
                    "evidence_segment_ids": ["evidence_ref_1"]
                }
            ],
            "memory_candidates": [],
            "entity_candidates": [],
            "relationship_candidates": [],
            "retrieval_candidates": [],
            "importance_score": 5
        })
        .to_string(),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Standard)
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
            serde_json::json!({
                "summary_draft": {
                    "title": "Bad evidence",
                    "body_text": "The model referenced an unknown segment.",
                    "evidence_segment_ids": ["evidence_ref_99"]
                },
                "classification_candidates": [],
                "memory_candidates": [],
                "entity_candidates": [],
                "relationship_candidates": [],
                "retrieval_candidates": [],
                "importance_score": 5
            })
            .to_string(),
            serde_json::json!({
                "summary_draft": {
                    "title": "Hardening and next steps",
                    "body_text": "The worker should fail invalid payloads and the next milestone is the real pipeline.",
                    "evidence_segment_ids": ["evidence_ref_1", "evidence_ref_2"]
                },
                "classification_candidates": [
                    {
                        "classification_type": "intent",
                        "classification_value": "fix_failure_handling",
                        "title": "Fix failure handling",
                        "body_text": "The artifact is focused on hardening worker failure behavior.",
                        "evidence_segment_ids": ["evidence_ref_1"]
                    }
                ],
                "memory_candidates": [
                    {
                        "title": "Task 12 uses hosted inference",
                        "body_text": "The first real pipeline uses a hosted inference provider.",
                        "evidence_segment_ids": ["evidence_ref_2"],
                        "durability_label": "high",
                        "retrieval_value_label": "high",
                        "consequentiality_label": "medium",
                        "temporal_scope": "enduring"
                    }
                ],
                "entity_candidates": [],
                "relationship_candidates": [],
                "retrieval_candidates": [],
                "importance_score": 7
            })
            .to_string(),
        ]),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Standard)
        .expect("processor should build");

    let output = processor
        .process(&sample_input())
        .expect("processor should repair and succeed");

    assert_eq!(output.classifications.len(), 1);
    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.importance_score, 7);
}

#[test]
fn openai_processor_drops_invalid_objects_without_rejecting_artifact() {
    let client = Arc::new(FixedInferenceClient {
        candidate_response: serde_json::json!({
            "summary_draft": {
                "title": "Logo symbol preferences",
                "body_text": "The artifact prefers the original hash-in-book symbol.",
                "evidence_segment_ids": ["evidence_ref_1"]
            },
            "classification_candidates": [],
            "memory_candidates": [
                {
                    "title": "Preferred HashBooks Logo Symbol",
                    "body_text": "The original hash-in-book symbol should remain the preferred logo mark.",
                    "evidence_segment_ids": ["evidence_ref_1"],
                    "durability_label": "high",
                    "retrieval_value_label": "high",
                    "consequentiality_label": "medium",
                    "temporal_scope": "enduring"
                },
                {
                    "title": "Prefer original logo symbol",
                    "body_text": "The original hash-in-book symbol should remain the preferred logo mark.",
                    "evidence_segment_ids": ["evidence_ref_1"],
                    "durability_label": "high",
                    "retrieval_value_label": "high",
                    "consequentiality_label": "medium",
                    "temporal_scope": "enduring"
                }
            ],
            "entity_candidates": [],
            "relationship_candidates": [],
            "retrieval_candidates": [],
            "importance_score": 6
        })
        .to_string(),
    });
    let factory = OpenAiProcessorFactory::with_client(client, "gpt-5-mini", "gpt-5.4");
    let processor = factory
        .build(EnrichmentTier::Standard)
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
fn conversation_prompt_lists_exact_allowed_evidence_refs() {
    let prompt = build_conversation_user_prompt(&sample_input()).expect("prompt should build");

    assert!(prompt.contains("allowed evidence_ref values for this artifact"));
    assert!(prompt.contains("evidence_ref_1"));
    assert!(prompt.contains("evidence_ref_2"));
    assert!(!prompt.contains("for example s1, s2"));
}

#[test]
fn conversation_prompt_requires_discrete_personal_history_memories() {
    let prompt = build_conversation_user_prompt(&sample_input()).expect("prompt should build");

    assert!(prompt.contains("summary is not a substitute for memories"));
    assert!(prompt.contains("do not mirror the summary into memories"));
    assert!(prompt.contains("classifications are broad topical labels only"));
    assert!(prompt.contains("choose the closest memory_type from the schema"));
    assert!(prompt.contains("biographical and health history belongs in durable memories"));
}

#[test]
fn document_prompt_mentions_meeting_notes_and_entities() {
    let prompt = build_document_user_prompt(&sample_document_input()).expect("prompt should build");

    assert!(prompt.contains("artifact_class: document"));
    assert!(prompt.contains("meeting notes, call notes"));
    assert!(prompt.contains("attendees, speakers, owners"));
    assert!(prompt.contains("entities: include explicit named people"));
}

#[test]
fn canonicalize_memory_type_maps_health_history_to_personal_fact() {
    let memory_type = canonicalize_memory_type(
        "",
        "TBI and broken back history",
        "History includes a brain hemorrhage after a car accident, a second concussion, lung nodules, smoking history, and elite cycling background.",
    );

    assert_eq!(memory_type, "personal_fact");
}

fn sample_reconciliation_input() -> ReconciliationProcessorInput {
    ReconciliationProcessorInput {
        artifact_id: "artifact-1".to_string(),
        source_type: SourceType::ChatGptExport,
        summary: SummaryOutput {
            title: Some("Architecture direction".to_string()),
            body_text: "The user wants the worker to avoid silent fallback.".to_string(),
            evidence_segment_ids: vec!["seg-1".to_string()],
        },
        memories: vec![MemoryOutput {
            candidate_key: "memory_1".to_string(),
            title: Some("Avoid silent fallback".to_string()),
            body_text: "The worker should not silently fall back.".to_string(),
            memory_type: "project_fact".to_string(),
            memory_scope: ScopeType::Artifact,
            memory_scope_value: "artifact-1".to_string(),
            evidence_segment_ids: vec!["seg-1".to_string()],
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
fn reconciliation_normalizes_ungrounded_existing_decision_to_create_new() {
    let input = sample_reconciliation_input();
    let output = ModelReconciliationOutput {
        decisions: vec![ModelReconciliationDecision {
            decision_kind: ReconciliationDecisionKind::StrengthenExisting,
            target_kind: "memory".to_string(),
            target_key: "memory_1".to_string(),
            matched_object_id: None,
            rationale: "This looks like the same memory.".to_string(),
            evidence_segment_ids: vec!["seg-1".to_string()],
        }],
    };

    let decisions = output
        .into_validated_outputs(&input)
        .expect("normalization should preserve the candidate");

    assert_eq!(decisions.len(), 1);
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
        decisions: vec![ModelReconciliationDecision {
            decision_kind: ReconciliationDecisionKind::StrengthenExisting,
            target_kind: "memory".to_string(),
            target_key: "memory_1".to_string(),
            matched_object_id: Some("obj-1".to_string()),
            rationale: "This clearly matches the prior memory.".to_string(),
            evidence_segment_ids: vec!["seg-1".to_string()],
        }],
    };

    let decisions = output
        .into_validated_outputs(&input)
        .expect("grounded existing-object decision should remain valid");

    assert_eq!(decisions.len(), 1);
    assert_eq!(
        decisions[0].decision_kind,
        ReconciliationDecisionKind::StrengthenExisting
    );
    assert_eq!(decisions[0].matched_object_id.as_deref(), Some("obj-1"));
}
