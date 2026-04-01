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

fn sample_document_input() -> ArtifactProcessorInput {
    ArtifactProcessorInput {
        artifact_id: "artifact-doc-1".to_string(),
        import_id: "import-doc-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::MarkdownFile,
        title: Some("Sprint planning notes".to_string()),
        imported_note_metadata: None,
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

fn sample_dashboard_input() -> ArtifactProcessorInput {
    ArtifactProcessorInput {
        artifact_id: "artifact-dashboard-1".to_string(),
        import_id: "import-dashboard-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("Notes Dashboard".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            note_path: Some("Notes/Notes Dashboard.md".to_string()),
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content:
                "# Notes Dashboard\n```dataview\nTABLE file.link\nFROM #note\n```\n<% tp.file.cursor(1) %>"
                    .to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    }
}

fn sample_procedural_input() -> ArtifactProcessorInput {
    ArtifactProcessorInput {
        artifact_id: "artifact-proc-1".to_string(),
        import_id: "import-proc-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::MarkdownFile,
        title: Some("Import notes".to_string()),
        imported_note_metadata: None,
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content:
                "# Import notes\n1. Open Settings.\n2. Choose Importer.\n3. Import your files."
                    .to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
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
fn procedural_document_prompt_discourages_metadata_memories() {
    let prompt =
        build_document_user_prompt(&sample_procedural_input()).expect("prompt should build");

    assert!(prompt.contains("do not spend memory budget on note titles"));
    assert!(prompt.contains("do not emit memories for note metadata"));
}

#[test]
fn procedural_policy_caps_candidate_schema_sizes() {
    let schema = candidate_output_schema_wrapper(&sample_procedural_input());
    let properties = &schema["schema"]["properties"];

    assert_eq!(properties["classification_candidates"]["maxItems"], 3);
    assert_eq!(properties["memory_candidates"]["maxItems"], 6);
    assert_eq!(properties["relationship_candidates"]["maxItems"], 3);
    assert_eq!(properties["retrieval_candidates"]["maxItems"], 3);
}

#[test]
fn dashboard_policy_caps_candidate_schema_sizes() {
    let schema = candidate_output_schema_wrapper(&sample_dashboard_input());
    let properties = &schema["schema"]["properties"];

    assert_eq!(properties["classification_candidates"]["maxItems"], 3);
    assert_eq!(properties["memory_candidates"]["maxItems"], 3);
    assert_eq!(properties["entity_candidates"]["maxItems"], 3);
    assert_eq!(properties["relationship_candidates"]["maxItems"], 1);
    assert_eq!(properties["retrieval_candidates"]["maxItems"], 2);
}

#[test]
fn dashboard_policy_truncates_cleaned_outputs() {
    let input = sample_dashboard_input();
    let output = cleanup_artifact_processor_output(
        &input,
        ArtifactProcessorOutput {
            pipeline_name: "test".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: None,
            model_name: None,
            prompt_version: None,
            usage: None,
            summary: SummaryOutput {
                title: Some("Dashboard".to_string()),
                body_text: "Navigation and reusable query helpers.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            classifications: (0..5)
                .map(|index| ClassificationOutput {
                    title: Some(format!("Classification {index}")),
                    body_text: Some("Body".to_string()),
                    classification_type: "topic".to_string(),
                    classification_value: format!("value-{index}"),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                })
                .collect(),
            memories: (0..5)
                .map(|index| MemoryOutput {
                    candidate_key: format!("memory-{index}"),
                    title: Some(format!("Memory {index}")),
                    body_text: format!("Body {index}"),
                    memory_type: "reference".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                })
                .collect(),
            entities: (0..5)
                .map(|index| EntityOutput {
                    entity_key: format!("entity-{index}"),
                    display_name: format!("Entity {index}"),
                    entity_type: "system".to_string(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
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
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                RelationshipOutput {
                    relationship_type: "references".to_string(),
                    subject_key: "entity-2".to_string(),
                    object_key: "entity-3".to_string(),
                    title: Some("r2".to_string()),
                    body_text: "r2".to_string(),
                    confidence_label: "medium".to_string(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
            ],
            retrieval_intents: (0..4)
                .map(|index| crate::storage::types::RetrievalIntent {
                    intent_id: format!("intent-{index}"),
                    question: format!("Question {index}"),
                    query_text: format!("Query {index}"),
                    intent_type: "topic_lookup".to_string(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                })
                .collect(),
            importance_score: 3,
        },
    );

    assert_eq!(output.classifications.len(), 3);
    assert_eq!(output.memories.len(), 3);
    assert_eq!(output.entities.len(), 3);
    assert_eq!(output.relationships.len(), 1);
    assert_eq!(output.retrieval_intents.len(), 2);
}

#[test]
fn procedural_cleanup_drops_low_value_document_memories() {
    let input = sample_procedural_input();
    let output = cleanup_artifact_processor_output(
        &input,
        ArtifactProcessorOutput {
            pipeline_name: "test".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: None,
            model_name: None,
            prompt_version: None,
            usage: None,
            summary: SummaryOutput {
                title: Some("Import notes".to_string()),
                body_text: "How to import notes.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            classifications: Vec::new(),
            memories: vec![
                MemoryOutput {
                    candidate_key: "memory-1".to_string(),
                    title: Some("Document type".to_string()),
                    body_text: "This note is a procedural guide.".to_string(),
                    memory_type: "reference".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                MemoryOutput {
                    candidate_key: "memory-2".to_string(),
                    title: Some("Creation and modification date".to_string()),
                    body_text: "The note was created and modified on the same day.".to_string(),
                    memory_type: "reference".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                MemoryOutput {
                    candidate_key: "memory-3".to_string(),
                    title: Some("Importer workflow".to_string()),
                    body_text: "Use the importer from Settings to bring notes into the vault."
                        .to_string(),
                    memory_type: "reference".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
            ],
            entities: Vec::new(),
            relationships: Vec::new(),
            retrieval_intents: vec![crate::storage::types::RetrievalIntent {
                intent_id: "intent-1".to_string(),
                question: "What note metadata does this file have?".to_string(),
                query_text: "document type creation date".to_string(),
                intent_type: "topic_lookup".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            }],
            importance_score: 3,
        },
    );

    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.memories[0].title.as_deref(), Some("Importer workflow"));
    assert!(output.retrieval_intents.is_empty());
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

#[test]
fn procedural_documents_default_generic_memories_to_reference() {
    let input = sample_procedural_input();

    let memory_type = canonicalize_memory_type_for_input(
        &input,
        "",
        "Import workflow",
        "Use the importer to bring notes into the vault.",
    );

    assert_eq!(memory_type, "reference");
}

#[test]
fn working_notes_default_generic_memories_to_project_fact() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-work-1".to_string(),
        import_id: "import-work-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("Project meeting notes".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            properties: vec![crate::storage::types::ImportedNotePropertyRecord {
                property_key: "status".to_string(),
                value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                value_text: Some("open".to_string()),
                value_json: serde_json::Value::String("open".to_string()),
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content: "# Decision\nShip markdown and text ingestion before docx.\n\n## Action items\n- [ ] Alice owns parser work.\n- [ ] Follow up on retry behavior."
                .to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let memory_type = canonicalize_memory_type_for_input(
        &input,
        "",
        "Parser decision",
        "Ship markdown and text ingestion before docx.",
    );

    assert_eq!(memory_type, "project_fact");
}

#[test]
fn concept_reference_notes_do_not_default_generic_memories_to_ongoing_state() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-concept-1".to_string(),
        import_id: "import-concept-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("Vertical Hydroponics".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            properties: vec![crate::storage::types::ImportedNotePropertyRecord {
                property_key: "document_type".to_string(),
                value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                value_text: Some("concept".to_string()),
                value_json: serde_json::Value::String("concept".to_string()),
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content:
                "# Concept\nAn automated vertical hydroponic system for growing plants indoors.\n\n## Planning\nTBD"
                    .to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let memory_type = canonicalize_memory_type_for_input(
        &input,
        "",
        "Core concept definition",
        "An automated vertical hydroponic system for growing plants indoors.",
    );

    assert_eq!(memory_type, "reference");
}

#[test]
fn journal_cleanup_drops_dashboard_link_memories() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-journal-2".to_string(),
        import_id: "import-journal-2".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("2023-07-21".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            tags: vec![crate::storage::types::ImportedNoteTagRecord {
                raw_tag: "#journal".to_string(),
                normalized_tag: "journal".to_string(),
                tag_path: "journal".to_string(),
                source_kind: crate::storage::types::ImportedNoteTagSourceKind::Inline,
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content: "Example journal entry.".to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let output = cleanup_artifact_processor_output(
        &input,
        ArtifactProcessorOutput {
            pipeline_name: "test".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: None,
            model_name: None,
            prompt_version: None,
            usage: None,
            summary: SummaryOutput {
                title: Some("2023-07-21".to_string()),
                body_text: "Example journal entry.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            classifications: Vec::new(),
            memories: vec![
                MemoryOutput {
                    candidate_key: "memory-1".to_string(),
                    title: Some("Linked to Journal dashboard".to_string()),
                    body_text: "This entry is linked to the Journal dashboard.".to_string(),
                    memory_type: "ongoing_state".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                MemoryOutput {
                    candidate_key: "memory-2".to_string(),
                    title: Some("Example journal entry text".to_string()),
                    body_text: "My example of a journal entry.".to_string(),
                    memory_type: "ongoing_state".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
            ],
            entities: Vec::new(),
            relationships: Vec::new(),
            retrieval_intents: Vec::new(),
            importance_score: 2,
        },
    );

    assert_eq!(output.memories.len(), 1);
    assert_eq!(
        output.memories[0].title.as_deref(),
        Some("Example journal entry text")
    );
}

#[test]
fn project_dashboards_can_keep_project_facts() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-project-home-1".to_string(),
        import_id: "import-project-home-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("Top Secret Project".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            properties: vec![
                crate::storage::types::ImportedNotePropertyRecord {
                    property_key: "document_type".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("project-dashboard".to_string()),
                    value_json: serde_json::Value::String("project-dashboard".to_string()),
                },
                crate::storage::types::ImportedNotePropertyRecord {
                    property_key: "project".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("Top Secret Project".to_string()),
                    value_json: serde_json::Value::String("Top Secret Project".to_string()),
                },
            ],
            tags: vec![crate::storage::types::ImportedNoteTagRecord {
                raw_tag: "#project".to_string(),
                normalized_tag: "project".to_string(),
                tag_path: "project".to_string(),
                source_kind: crate::storage::types::ImportedNoteTagSourceKind::Inline,
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content:
                "# Top Secret Project\nDescription:: Cosmic Dawn Initiative is a NASA project deploying AI-guided spacecraft to study the universe's earliest moments.\n\n## Meetings\n```dataviewjs\nTABLE file.link\n```"
                    .to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let memory_type = canonicalize_memory_type_for_input(
        &input,
        "",
        "Project description",
        "Cosmic Dawn Initiative is a NASA project deploying AI-guided spacecraft to study the universe's earliest moments.",
    );

    assert_eq!(memory_type, "project_fact");
}

#[test]
fn dashboard_cleanup_drops_navigation_memories_but_keeps_project_fact() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-project-home-2".to_string(),
        import_id: "import-project-home-2".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("Home".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            properties: vec![
                crate::storage::types::ImportedNotePropertyRecord {
                    property_key: "document_type".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("project-dashboard".to_string()),
                    value_json: serde_json::Value::String("project-dashboard".to_string()),
                },
                crate::storage::types::ImportedNotePropertyRecord {
                    property_key: "project".to_string(),
                    value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                    value_text: Some("Top Secret Project".to_string()),
                    value_json: serde_json::Value::String("Top Secret Project".to_string()),
                },
            ],
            tags: vec![crate::storage::types::ImportedNoteTagRecord {
                raw_tag: "#project".to_string(),
                normalized_tag: "project".to_string(),
                tag_path: "project".to_string(),
                source_kind: crate::storage::types::ImportedNoteTagSourceKind::Inline,
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content:
                "# Home\nDescription:: Cosmic Dawn Initiative is a NASA project deploying AI-guided spacecraft.\n\n## Meetings\n```dataviewjs\nTABLE file.link\n```"
                    .to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let output = cleanup_artifact_processor_output(
        &input,
        ArtifactProcessorOutput {
            pipeline_name: "test".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: None,
            model_name: None,
            prompt_version: None,
            usage: None,
            summary: SummaryOutput {
                title: Some("Top Secret Project dashboard".to_string()),
                body_text: "Project home dashboard".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            classifications: Vec::new(),
            memories: vec![
                MemoryOutput {
                    candidate_key: "m1".to_string(),
                    title: Some("Project description".to_string()),
                    body_text:
                        "Cosmic Dawn Initiative is a NASA project deploying AI-guided spacecraft."
                            .to_string(),
                    memory_type: "project_fact".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                MemoryOutput {
                    candidate_key: "m2".to_string(),
                    title: Some("Dashboard sections".to_string()),
                    body_text:
                        "The home dashboard links to meetings, notes, and references for navigation."
                            .to_string(),
                    memory_type: "project_fact".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                MemoryOutput {
                    candidate_key: "m3".to_string(),
                    title: Some("Recent-item display limit".to_string()),
                    body_text: "Recent meetings and notes are limited to the latest 10 entries."
                        .to_string(),
                    memory_type: "reference".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
            ],
            entities: Vec::new(),
            relationships: Vec::new(),
            retrieval_intents: Vec::new(),
            importance_score: 4,
        },
    );

    assert_eq!(output.memories.len(), 1);
    assert_eq!(
        output.memories[0].title.as_deref(),
        Some("Project description")
    );
}

#[test]
fn working_note_cleanup_drops_imported_tag_memories() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-meeting-1".to_string(),
        import_id: "import-meeting-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("2026-03-31 meeting".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            properties: vec![crate::storage::types::ImportedNotePropertyRecord {
                property_key: "document_type".to_string(),
                value_kind: crate::storage::types::ImportedNotePropertyValueKind::String,
                value_text: Some("meeting".to_string()),
                value_json: serde_json::Value::String("meeting".to_string()),
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content: "Meeting note with one decision and one attendee.".to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let output = cleanup_artifact_processor_output(
        &input,
        ArtifactProcessorOutput {
            pipeline_name: "test".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: None,
            model_name: None,
            prompt_version: None,
            usage: None,
            summary: SummaryOutput {
                title: Some("Meeting".to_string()),
                body_text: "Structured meeting note.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            classifications: Vec::new(),
            memories: vec![
                MemoryOutput {
                    candidate_key: "m1".to_string(),
                    title: Some("Imported tags".to_string()),
                    body_text: "Imported tags include project and meeting.".to_string(),
                    memory_type: "project_fact".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
                MemoryOutput {
                    candidate_key: "m2".to_string(),
                    title: Some("Decision".to_string()),
                    body_text: "Ship the retry fix this week.".to_string(),
                    memory_type: "project_fact".to_string(),
                    memory_scope: ScopeType::Artifact,
                    memory_scope_value: input.artifact_id.clone(),
                    evidence_segment_ids: vec!["seg-1".to_string()],
                },
            ],
            entities: Vec::new(),
            relationships: Vec::new(),
            retrieval_intents: Vec::new(),
            importance_score: 3,
        },
    );

    assert_eq!(output.memories.len(), 1);
    assert_eq!(output.memories[0].title.as_deref(), Some("Decision"));
}

#[test]
fn journal_logs_default_generic_memories_to_ongoing_state() {
    let input = ArtifactProcessorInput {
        artifact_id: "artifact-journal-1".to_string(),
        import_id: "import-journal-1".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: SourceType::ObsidianVault,
        title: Some("2026-03-31".to_string()),
        imported_note_metadata: Some(crate::storage::types::ImportedNoteMetadata {
            tags: vec![crate::storage::types::ImportedNoteTagRecord {
                raw_tag: "#journal".to_string(),
                normalized_tag: "journal".to_string(),
                tag_path: "journal".to_string(),
                source_kind: crate::storage::types::ImportedNoteTagSourceKind::Inline,
            }],
            ..crate::storage::types::ImportedNoteMetadata::default()
        }),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: "seg-1".to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content: "Today I am still working through the archive migration.".to_string(),
            created_at_source: None,
            visibility_status: crate::VisibilityStatus::Visible,
        }],
    };

    let memory_type = canonicalize_memory_type_for_input(
        &input,
        "",
        "Current archive migration state",
        "Still working through the archive migration.",
    );

    assert_eq!(memory_type, "ongoing_state");
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
        entities: vec![EntityOutput {
            entity_key: "worker".to_string(),
            display_name: "Worker".to_string(),
            entity_type: "system".to_string(),
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
        decisions: vec![
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::StrengthenExisting,
                target_kind: "memory".to_string(),
                target_key: "memory_1".to_string(),
                matched_object_id: None,
                rationale: "This looks like the same memory.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "entity".to_string(),
                target_key: "worker".to_string(),
                matched_object_id: None,
                rationale: "This entity should remain distinct.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
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
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "entity".to_string(),
                target_key: "worker".to_string(),
                matched_object_id: None,
                rationale: "This entity should remain distinct.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
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
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "memory".to_string(),
                target_key: "worker".to_string(),
                matched_object_id: None,
                rationale: "This entity is mislabeled on purpose.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
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
