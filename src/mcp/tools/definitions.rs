use serde_json::{json, Value};

use crate::app::artifact_detail;

use super::{REVIEW_LIMIT_CAP, SEARCH_LIMIT_CAP};

pub(in crate::mcp) fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "search_archive",
            "description": "Search the archive and return up to 50 ranked hits with match kind, snippet, and score.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query text." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": SEARCH_LIMIT_CAP, "description": "Optional result limit; values above 50 are silently clamped." },
                    "object_type": { "type": "string", "enum": ["summary", "classification", "memory", "relationship", "entity"], "description": "Filter results to a specific derived object type." },
                    "source_type": { "type": "string", "enum": ["chatgpt_export", "claude_export", "grok_export", "gemini_takeout", "text_file", "markdown_file", "obsidian_vault"], "description": "Filter results to artifacts from a specific source." },
                    "tag": { "type": "string", "description": "Optional exact normalized imported note tag filter." },
                    "alias": { "type": "string", "description": "Optional exact imported note alias filter." },
                    "path_prefix": { "type": "string", "description": "Optional imported note path prefix filter." }
                },
                "required": ["query"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "get_artifact",
            "description": "Load artifact metadata and active derived objects for one artifact id. Segments are omitted by default and can be requested in a bounded window.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "artifact_id": { "type": "string", "description": "Artifact identifier." },
                    "include_segments": { "type": "boolean", "description": "When true, include a bounded window of ordered segments. Defaults to false." },
                    "segment_offset": { "type": "integer", "minimum": 0, "description": "Zero-based starting segment offset when include_segments is true." },
                    "segment_limit": { "type": "integer", "minimum": 1, "maximum": artifact_detail::DEFAULT_SEGMENT_LIMIT, "description": "Maximum number of segments to return when include_segments is true. Values above 50 are silently clamped." }
                },
                "required": ["artifact_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "get_context_pack",
            "description": "Load the compact artifact context pack for one artifact id, including readiness and provenance.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "artifact_id": { "type": "string", "description": "Artifact identifier." }
                },
                "required": ["artifact_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "search_objects",
            "description": "Search derived objects (memories, entities, relationships, classifications) with structured filters. Uses lexical ranking by default and semantic ranking when embeddings are configured.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Optional query text for lexical and semantic ranking on object title/body." },
                    "object_type": { "type": "string", "enum": ["summary", "classification", "memory", "relationship", "entity"], "description": "Filter by derived object type." },
                    "candidate_key": { "type": "string", "description": "Exact match on the object's candidate key (entity key, memory key)." },
                    "artifact_id": { "type": "string", "description": "Scope results to a single artifact." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": SEARCH_LIMIT_CAP, "description": "Max results, default 20." }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "store_memory",
            "description": "Store a memory from the current conversation into the archive brain. Use this to persist insights, preferences, facts, or decisions that should be available to future sessions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Short title for the memory." },
                    "body_text": { "type": "string", "description": "Full memory content." },
                    "memory_type": { "type": "string", "description": "Broad retrieval-oriented type, for example: personal_fact, preference, project_fact, ongoing_state, or reference." },
                    "candidate_key": { "type": "string", "description": "Optional deduplication key for identifying related memories across artifacts." },
                    "artifact_id": { "type": "string", "description": "The artifact this memory relates to." },
                    "contributed_by": { "type": "string", "description": "Optional identifier for the contributing agent." },
                    "evidence": {
                        "type": "array",
                        "description": "Optional segment evidence supporting this memory.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "segment_id": { "type": "string" },
                                "evidence_role": { "type": "string", "enum": ["primary_support", "secondary_support", "reduction_input"] },
                                "support_strength": { "type": "string", "enum": ["strong", "medium", "weak"] }
                            },
                            "required": ["segment_id", "evidence_role", "support_strength"]
                        }
                    }
                },
                "required": ["title", "body_text", "memory_type", "artifact_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "link_objects",
            "description": "Declare a relationship between two derived objects in the archive. Use this when you discover that two objects are related (same entity, continuing memory, contradiction, etc).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_object_id": { "type": "string", "description": "ID of the source derived object." },
                    "target_object_id": { "type": "string", "description": "ID of the target derived object." },
                    "link_type": { "type": "string", "enum": ["same_as", "continues", "contradicts", "same_topic", "refers_to"], "description": "Type of relationship." },
                    "confidence_score": { "type": "number", "minimum": 0.0, "maximum": 1.0, "description": "Confidence in the link (0.0 to 1.0)." },
                    "rationale": { "type": "string", "description": "Why these objects are related." },
                    "contributed_by": { "type": "string", "description": "Optional identifier for the contributing agent." }
                },
                "required": ["source_object_id", "target_object_id", "link_type"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_artifacts",
            "description": "Browse artifacts with optional filters. Returns artifact metadata ordered by capture time (newest first).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_type": { "type": "string", "enum": ["chatgpt_export", "claude_export", "grok_export", "gemini_takeout", "text_file", "markdown_file", "obsidian_vault"], "description": "Filter to artifacts from a specific source." },
                    "enrichment_status": { "type": "string", "enum": ["pending", "running", "completed", "partial", "failed"], "description": "Filter by enrichment status." },
                    "tag": { "type": "string", "description": "Optional exact normalized imported note tag filter." },
                    "alias": { "type": "string", "description": "Optional exact imported note alias filter." },
                    "path_prefix": { "type": "string", "description": "Optional imported note path prefix filter." },
                    "captured_after": { "type": "string", "description": "ISO 8601 lower bound on capture time (inclusive)." },
                    "captured_before": { "type": "string", "description": "ISO 8601 upper bound on capture time (exclusive)." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Max results, default 20." },
                    "offset": { "type": "integer", "minimum": 0, "description": "Pagination offset, default 0." }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "update_object",
            "description": "Mark a derived object as superseded or rejected. Use this when you discover that an existing object is incorrect, outdated, or should be replaced.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "derived_object_id": { "type": "string", "description": "ID of the derived object to update." },
                    "new_status": { "type": "string", "enum": ["superseded", "rejected"], "description": "New status for the object." },
                    "replacement_object_id": { "type": "string", "description": "ID of the replacement object (only valid when superseding)." },
                    "contributed_by": { "type": "string", "description": "Optional identifier for the contributing agent." }
                },
                "required": ["derived_object_id", "new_status"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "store_entity",
            "description": "Store a canonical entity (person, project, tool, concept, etc.) in the archive. Use this to persist entities discovered during conversation analysis.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Entity name/title." },
                    "body_text": { "type": "string", "description": "Description of the entity." },
                    "entity_type": { "type": "string", "description": "Entity category, for example: person, project, tool, concept, organization, location." },
                    "artifact_id": { "type": "string", "description": "The artifact this entity relates to." },
                    "candidate_key": { "type": "string", "description": "Optional deduplication key for identifying this entity across artifacts." },
                    "contributed_by": { "type": "string", "description": "Optional identifier for the contributing agent." },
                    "evidence": {
                        "type": "array",
                        "description": "Optional segment evidence supporting this entity.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "segment_id": { "type": "string" },
                                "evidence_role": { "type": "string", "enum": ["primary_support", "secondary_support", "reduction_input"] },
                                "support_strength": { "type": "string", "enum": ["strong", "medium", "weak"] }
                            },
                            "required": ["segment_id", "evidence_role", "support_strength"]
                        }
                    }
                },
                "required": ["title", "body_text", "entity_type", "artifact_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "get_related",
            "description": "Find derived objects connected to a given object via archive links or shared evidence. Returns 1-hop graph neighbors.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "derived_object_id": { "type": "string", "description": "ID of the derived object to find relations for." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 50, "description": "Max results, default 20." }
                },
                "required": ["derived_object_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "get_timeline",
            "description": "View artifacts ordered chronologically with optional keyword and source filters. Each entry includes a summary snippet when available.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keyword": { "type": "string", "description": "Optional keyword to filter artifacts by title (full-text search)." },
                    "source_type": { "type": "string", "enum": ["chatgpt_export", "claude_export", "grok_export", "gemini_takeout", "text_file", "markdown_file", "obsidian_vault"], "description": "Filter to artifacts from a specific source." },
                    "tag": { "type": "string", "description": "Optional exact normalized imported note tag filter." },
                    "path_prefix": { "type": "string", "description": "Optional imported note path prefix filter." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Max results, default 20." },
                    "offset": { "type": "integer", "minimum": 0, "description": "Pagination offset, default 0." }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "list_review_items",
            "description": "List review queue items that need human or agent attention. Returns prioritized review items with suggested actions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kinds": {
                        "type": "array",
                        "description": "Optional review item kind filter.",
                        "items": {
                            "type": "string",
                            "enum": [
                                "artifact_needs_attention",
                                "artifact_missing_summary",
                                "object_low_confidence",
                                "candidate_key_collision",
                                "object_missing_evidence"
                            ]
                        }
                    },
                    "limit": { "type": "integer", "minimum": 1, "maximum": REVIEW_LIMIT_CAP, "description": "Max results, default 20." }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "record_review_decision",
            "description": "Record a review note, dismissal, or resolution for a review queue item.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "artifact_needs_attention",
                            "artifact_missing_summary",
                            "object_low_confidence",
                            "candidate_key_collision",
                            "object_missing_evidence"
                        ]
                    },
                    "artifact_id": { "type": "string", "description": "Artifact identifier for the review item." },
                    "derived_object_id": { "type": "string", "description": "Required for object-level review items." },
                    "decision_status": { "type": "string", "enum": ["noted", "dismissed", "resolved"] },
                    "note_text": { "type": "string", "description": "Optional note text. Required when decision_status is noted." },
                    "decided_by": { "type": "string", "description": "Optional reviewer identifier." }
                },
                "required": ["kind", "artifact_id", "decision_status"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "retry_artifact_enrichment",
            "description": "Queue a fresh enrichment extract job for an artifact when review determines the artifact should be retried.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "artifact_id": { "type": "string", "description": "Artifact identifier." }
                },
                "required": ["artifact_id"],
                "additionalProperties": false
            }
        }),
    ]
}
