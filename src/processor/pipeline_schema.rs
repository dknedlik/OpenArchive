use serde_json::json;

use super::*;

pub(crate) fn candidate_output_schema_wrapper(input: &ArtifactProcessorInput) -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_artifact_candidate_extraction",
        "strict": true,
        "schema": candidate_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input))
    })
}

pub(crate) fn candidate_output_schema_with_allowed_refs(
    allowed_refs: &[String],
) -> serde_json::Value {
    let evidence_id_item = if allowed_refs.is_empty() {
        json!({ "type": "string", "minLength": 1 })
    } else {
        json!({ "type": "string", "enum": allowed_refs })
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary_draft",
            "classification_candidates",
            "memory_candidates",
            "entity_candidates",
            "relationship_candidates",
            "retrieval_candidates",
            "importance_score"
        ],
        "properties": {
            "summary_draft": {
                "type": "object",
                "additionalProperties": false,
                "required": ["title", "body_text", "evidence_segment_ids"],
                "properties": {
                    "title": { "type": "string", "minLength": 1 },
                    "body_text": { "type": "string", "minLength": 1 },
                    "evidence_segment_ids": {
                        "type": "array",
                        "minItems": 1,
                        "items": evidence_id_item.clone()
                    }
                }
            },
            "classification_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "classification_type",
                        "classification_value",
                        "title",
                        "body_text",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "classification_type": { "type": "string", "enum": ["topic", "intent"] },
                        "classification_value": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "memory_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "title",
                        "body_text",
                        "evidence_segment_ids",
                        "durability_label",
                        "retrieval_value_label",
                        "consequentiality_label",
                        "temporal_scope"
                    ],
                    "properties": {
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "durability_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "retrieval_value_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "consequentiality_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "temporal_scope": { "type": "string", "enum": ["ephemeral", "time_bound", "ongoing", "enduring"] },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "entity_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["entity_key", "display_name", "entity_type", "evidence_segment_ids"],
                    "properties": {
                        "entity_key": { "type": "string", "minLength": 1 },
                        "display_name": { "type": "string", "minLength": 1 },
                        "entity_type": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "relationship_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "relationship_type",
                        "subject_key",
                        "object_key",
                        "title",
                        "body_text",
                        "confidence_label",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "relationship_type": { "type": "string", "minLength": 1 },
                        "subject_key": { "type": "string", "minLength": 1 },
                        "object_key": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "retrieval_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["question", "query_text", "intent_type", "evidence_segment_ids"],
                    "properties": {
                        "question": { "type": "string", "minLength": 1 },
                        "query_text": { "type": "string", "minLength": 1 },
                        "intent_type": {
                            "type": "string",
                            "enum": ["topic_lookup", "memory_match", "entity_lookup", "relationship_lookup", "contradiction_check"]
                        },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "importance_score": { "type": "integer", "minimum": 1, "maximum": 10 }
        }
    })
}

pub(crate) fn parse_candidate_output(
    output_text: &str,
    input: &ArtifactProcessorInput,
) -> Result<ModelCandidateArtifactOutput, ProcessorError> {
    let parsed: ModelCandidateArtifactOutput =
        serde_json::from_str(output_text).map_err(|source| ProcessorError::ParseModelJson {
            source,
            body_preview: preview(output_text),
        })?;
    parsed.validate(input)
}

#[allow(dead_code)]
pub(crate) fn combine_usage(
    left: Option<InferenceUsage>,
    right: Option<InferenceUsage>,
) -> Option<InferenceUsage> {
    fn add(left: Option<u64>, right: Option<u64>) -> Option<u64> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left + right),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    match (left, right) {
        (None, None) => None,
        (left, right) => Some(InferenceUsage {
            input_tokens: add(
                left.as_ref().and_then(|usage| usage.input_tokens),
                right.as_ref().and_then(|usage| usage.input_tokens),
            ),
            output_tokens: add(
                left.as_ref().and_then(|usage| usage.output_tokens),
                right.as_ref().and_then(|usage| usage.output_tokens),
            ),
            reasoning_tokens: add(
                left.as_ref().and_then(|usage| usage.reasoning_tokens),
                right.as_ref().and_then(|usage| usage.reasoning_tokens),
            ),
            total_tokens: add(
                left.as_ref().and_then(|usage| usage.total_tokens),
                right.as_ref().and_then(|usage| usage.total_tokens),
            ),
            reported_cost_micros: add(
                left.as_ref().and_then(|usage| usage.reported_cost_micros),
                right.as_ref().and_then(|usage| usage.reported_cost_micros),
            ),
        }),
    }
}

#[allow(dead_code)]
pub(crate) fn structured_output_schema_wrapper(
    input: &ArtifactProcessorInput,
) -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_artifact_enrichment",
        "strict": true,
        "schema": structured_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input))
    })
}

#[allow(dead_code)]
pub fn structured_output_schema() -> serde_json::Value {
    structured_output_schema_with_allowed_refs(&[])
}

#[allow(dead_code)]
pub(crate) fn structured_output_schema_with_allowed_refs(
    allowed_refs: &[String],
) -> serde_json::Value {
    let evidence_id_item = if allowed_refs.is_empty() {
        json!({ "type": "string", "minLength": 1 })
    } else {
        json!({ "type": "string", "enum": allowed_refs })
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary",
            "classifications",
            "memories",
            "entities",
            "relationships",
            "retrieval_intents",
            "importance_score"
        ],
        "properties": {
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "required": ["title", "body_text", "evidence_segment_ids"],
                "properties": {
                    "title": { "type": "string", "minLength": 1 },
                    "body_text": { "type": "string", "minLength": 1 },
                    "evidence_segment_ids": {
                        "type": "array",
                        "minItems": 1,
                        "items": evidence_id_item.clone()
                    }
                }
            },
            "classifications": {
                "type": "array",
                "maxItems": MAX_CLASSIFICATIONS,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "classification_type",
                        "classification_value",
                        "title",
                        "body_text",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "classification_type": {
                            "type": "string",
                            "enum": ["topic", "intent"]
                        },
                        "classification_value": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "memories": {
                "type": "array",
                "maxItems": MAX_MEMORIES,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "memory_type",
                        "memory_scope",
                        "memory_scope_value",
                        "title",
                        "body_text",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "memory_type": {
                            "type": "string",
                            "enum": MEMORY_TYPE_VALUES
                        },
                        "memory_scope": {
                            "type": "string",
                            "enum": ["artifact"]
                        },
                        "memory_scope_value": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["entity_key", "display_name", "entity_type", "evidence_segment_ids"],
                    "properties": {
                        "entity_key": { "type": "string", "minLength": 1 },
                        "display_name": { "type": "string", "minLength": 1 },
                        "entity_type": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
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
                        "subject_key",
                        "object_key",
                        "title",
                        "body_text",
                        "confidence_label",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "relationship_type": { "type": "string", "minLength": 1 },
                        "subject_key": { "type": "string", "minLength": 1 },
                        "object_key": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "retrieval_intents": {
                "type": "array",
                "maxItems": MAX_RETRIEVAL_INTENTS,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["question", "query_text", "intent_type", "evidence_segment_ids"],
                    "properties": {
                        "question": { "type": "string", "minLength": 1 },
                        "query_text": { "type": "string", "minLength": 1 },
                        "intent_type": {
                            "type": "string",
                            "enum": ["topic_lookup", "memory_match", "entity_lookup", "relationship_lookup", "contradiction_check"]
                        },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item
                        }
                    }
                }
            },
            "importance_score": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10
            }
        }
    })
}

#[allow(dead_code)]
pub(crate) fn openai_structured_output_schema(input: &ArtifactProcessorInput) -> serde_json::Value {
    let mut schema =
        structured_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input));
    if let Some(memories) = schema
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|properties| properties.get_mut("memories"))
        .and_then(serde_json::Value::as_object_mut)
    {
        memories.insert("maxItems".to_string(), serde_json::json!(MAX_MEMORIES));
        if let Some(items) = memories
            .get_mut("items")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(properties) = items
                .get_mut("properties")
                .and_then(serde_json::Value::as_object_mut)
            {
                if let Some(memory_type) = properties
                    .get_mut("memory_type")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    memory_type.insert("enum".to_string(), serde_json::json!(MEMORY_TYPE_VALUES));
                }
            }
        }
    }
    schema
}

pub(crate) fn reconciliation_output_schema_wrapper() -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_reconciliation",
        "strict": true,
        "schema": reconciliation_output_schema()
    })
}

pub(crate) fn reconciliation_output_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["decision_kind", "target_kind", "target_key", "rationale", "evidence_segment_ids"],
                    "properties": {
                        "decision_kind": {
                            "type": "string",
                            "enum": [
                                "create_new",
                                "attach_to_existing",
                                "strengthen_existing",
                                "supersede_existing",
                                "contradicts_existing",
                                "insufficient_evidence"
                            ]
                        },
                        "target_kind": { "type": "string", "enum": ["memory", "relationship"] },
                        "target_key": { "type": "string", "minLength": 1 },
                        "matched_object_id": { "type": "string", "minLength": 1 },
                        "rationale": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            }
        }
    })
}
