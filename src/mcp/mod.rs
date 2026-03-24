use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{error, info, warn};
use rand::random;
use serde_json::{json, Value};

use crate::app::{artifact_detail, context_pack, review, search, ArchiveApplication};
use crate::storage::types::{ArtifactListFilters, TimelineFilters};
use crate::storage::writeback_store::{
    NewAgentEntity, NewAgentEvidenceLink, NewAgentMemory, NewArchiveLink, UpdateObjectStatus,
};
use crate::storage::{
    DerivedObjectType, EnrichmentStatus, EvidenceRole, ObjectSearchFilters, ObjectStatus,
    ReviewDecisionStatus, ReviewItemKind, ReviewQueueFilters, SearchFilters, SourceType,
    SupportStrength,
};

const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const SEARCH_LIMIT_CAP: usize = 50;
const REVIEW_LIMIT_CAP: usize = 100;

pub fn run_stdio_server(app: Arc<ArchiveApplication>) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut writer = io::BufWriter::new(stdout.lock());

    info!("[open-archive-mcp] stdio loop started");

    loop {
        let request = match read_jsonrpc_message(&mut reader) {
            Ok(Some(request)) => request,
            Ok(None) => {
                info!("[open-archive-mcp] stdin closed");
                break;
            }
            Err(err) => {
                error!("[open-archive-mcp] failed to read request: {err}");
                return Err(err);
            }
        };

        info!(
            "[open-archive-mcp] received method={} id={}",
            request.method,
            request
                .id
                .as_ref()
                .map(Value::to_string)
                .unwrap_or_else(|| "null".to_string())
        );

        if let Some(response) = handle_request(app.as_ref(), request) {
            let response_id = response
                .get("id")
                .cloned()
                .unwrap_or(Value::Null)
                .to_string();
            if let Err(err) = write_jsonrpc_message(&mut writer, &response) {
                error!("[open-archive-mcp] failed to write response id={response_id}: {err}");
                return Err(err);
            }
            if let Err(err) = writer.flush() {
                error!("[open-archive-mcp] failed to flush response id={response_id}: {err}");
                return Err(err);
            }
            info!("[open-archive-mcp] responded id={response_id}");
        }
    }

    Ok(())
}

fn handle_request(app: &ArchiveApplication, request: JsonRpcRequest) -> Option<Value> {
    if request.id.is_none() {
        warn!(
            "[open-archive-mcp] ignoring notification method={}",
            request.method
        );
    }

    let id = request.id?;

    let response = match request.method.as_str() {
        "initialize" => jsonrpc_result(
            id,
            json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {
                        "listChanged": false
                    }
                },
                "serverInfo": {
                    "name": "open-archive-mcp",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        ),
        "notifications/initialized" => return None,
        "ping" => jsonrpc_result(id, json!({})),
        "tools/list" => jsonrpc_result(id, json!({ "tools": tool_definitions() })),
        "tools/call" => jsonrpc_result(id, call_tool(app, request.params.unwrap_or(Value::Null))),
        _ => jsonrpc_error(id, -32601, format!("method not found: {}", request.method)),
    };

    Some(response)
}

fn tool_definitions() -> Vec<Value> {
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
                    "source_type": { "type": "string", "enum": ["chatgpt_export", "claude_export", "grok_export", "gemini_takeout", "text_file", "markdown_file"], "description": "Filter results to artifacts from a specific source." }
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
                    "source_type": { "type": "string", "enum": ["chatgpt_export", "claude_export", "grok_export", "gemini_takeout", "text_file", "markdown_file"], "description": "Filter to artifacts from a specific source." },
                    "enrichment_status": { "type": "string", "enum": ["pending", "running", "completed", "partial", "failed"], "description": "Filter by enrichment status." },
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
                    "source_type": { "type": "string", "enum": ["chatgpt_export", "claude_export", "grok_export", "gemini_takeout", "text_file", "markdown_file"], "description": "Filter to artifacts from a specific source." },
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

fn call_tool(app: &ArchiveApplication, params: Value) -> Value {
    let name = match params.get("name").and_then(Value::as_str) {
        Some(n) => n,
        None => return tool_error("invalid_params", "tools/call params.name is required"),
    };
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);

    match name {
        "search_archive" => {
            let service = match app.search.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "search_archive is unavailable for the configured provider",
                    )
                }
            };
            let query = match arguments.get("query").and_then(Value::as_str) {
                Some(q) => q,
                None => {
                    return tool_error("invalid_params", "search_archive requires a string query")
                }
            };
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(10)
                .clamp(1, SEARCH_LIMIT_CAP);
            let object_type =
                match parse_optional_enum(&arguments, "object_type", DerivedObjectType::from_str) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
            let source_type =
                match parse_optional_enum(&arguments, "source_type", SourceType::from_str) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
            match service.search(search::ArchiveSearchRequest {
                query_text: query.to_string(),
                limit,
                filters: SearchFilters {
                    object_type,
                    source_type,
                },
            }) {
                Ok(response) => tool_success(
                    serde_json::to_value(response).expect("search response serializable"),
                ),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "get_artifact" => {
            let service = match app.artifact_detail.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "get_artifact is unavailable for the configured provider",
                    )
                }
            };
            let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
                Some(id) => id,
                None => return tool_error("invalid_params", "get_artifact requires artifact_id"),
            };
            let include_segments = arguments
                .get("include_segments")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let segment_offset = arguments
                .get("segment_offset")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(0);
            let segment_limit = arguments
                .get("segment_limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(artifact_detail::DEFAULT_SEGMENT_LIMIT)
                .clamp(1, artifact_detail::DEFAULT_SEGMENT_LIMIT);
            match service.get(artifact_detail::ArtifactDetailRequest {
                artifact_id: artifact_id.to_string(),
                include_segments,
                segment_offset,
                segment_limit,
            }) {
                Ok(Some(response)) => tool_success(json!({ "found": true, "artifact": response })),
                Ok(None) => tool_success(json!({ "found": false })),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "get_context_pack" => {
            let service = match app.context_pack.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "get_context_pack is unavailable for the configured provider",
                    )
                }
            };
            let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
                Some(id) => id,
                None => {
                    return tool_error("invalid_params", "get_context_pack requires artifact_id")
                }
            };
            match service.assemble(context_pack::ContextPackRequest {
                artifact_id: artifact_id.to_string(),
            }) {
                Ok(Some(response)) => {
                    tool_success(json!({ "found": true, "context_pack": response }))
                }
                Ok(None) => tool_success(json!({ "found": false })),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "search_objects" => {
            let service = match app.object_search.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "search_objects is unavailable for the configured provider",
                    )
                }
            };
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let object_type =
                match parse_optional_enum(&arguments, "object_type", DerivedObjectType::from_str) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
            let candidate_key = arguments
                .get("candidate_key")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let artifact_id = arguments
                .get("artifact_id")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(20)
                .clamp(1, SEARCH_LIMIT_CAP);
            let filters = ObjectSearchFilters {
                query,
                object_type,
                candidate_key,
                artifact_id,
            };
            match service.search(filters, limit) {
                Ok(results) => {
                    let results_json: Vec<Value> = results.into_iter().map(|r| json!({
                        "derived_object_id": r.derived_object_id,
                        "artifact_id": r.artifact_id,
                        "derived_object_type": r.derived_object_type.as_str(),
                        "title": r.title, "body_text": r.body_text,
                        "candidate_key": r.candidate_key, "confidence_score": r.confidence_score,
                        "score": r.score,
                    })).collect();
                    tool_success(json!({ "results": results_json }))
                }
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "store_memory" => {
            let service = match app.writeback.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "store_memory is unavailable for the configured provider",
                    )
                }
            };
            let title = match arguments.get("title").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "store_memory requires title"),
            };
            let body_text = match arguments.get("body_text").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "store_memory requires body_text"),
            };
            let memory_type = match arguments.get("memory_type").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "store_memory requires memory_type"),
            };
            let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
                Some(v) => v.to_string(),
                None => return tool_error("invalid_params", "store_memory requires artifact_id"),
            };
            let candidate_key = arguments
                .get("candidate_key")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let contributed_by = arguments
                .get("contributed_by")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let evidence = match parse_evidence_array(&arguments) {
                Ok(v) => v,
                Err(e) => return e,
            };

            let derived_object_id = new_id("dobj");
            let memory = NewAgentMemory {
                derived_object_id: derived_object_id.clone(),
                artifact_id,
                title: title.to_string(),
                body_text: body_text.to_string(),
                memory_type: memory_type.to_string(),
                candidate_key,
                contributed_by,
                evidence,
            };
            match service.store_memory(memory) {
                Ok(id) => tool_success(json!({ "stored": true, "derived_object_id": id })),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "link_objects" => {
            let service = match app.writeback.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "link_objects is unavailable for the configured provider",
                    )
                }
            };
            let source_object_id = match arguments.get("source_object_id").and_then(Value::as_str) {
                Some(v) => v,
                None => {
                    return tool_error("invalid_params", "link_objects requires source_object_id")
                }
            };
            let target_object_id = match arguments.get("target_object_id").and_then(Value::as_str) {
                Some(v) => v,
                None => {
                    return tool_error("invalid_params", "link_objects requires target_object_id")
                }
            };
            let link_type = match arguments.get("link_type").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "link_objects requires link_type"),
            };
            if source_object_id == target_object_id {
                return tool_error(
                    "invalid_params",
                    "link_objects: source and target must be different objects",
                );
            }
            let rationale = arguments
                .get("rationale")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let contributed_by = arguments
                .get("contributed_by")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let archive_link_id = new_id("alink");
            let link = NewArchiveLink {
                archive_link_id: archive_link_id.clone(),
                source_object_id: source_object_id.to_string(),
                target_object_id: target_object_id.to_string(),
                link_type: link_type.to_string(),
                confidence_score: arguments.get("confidence_score").and_then(Value::as_f64),
                rationale,
                contributed_by,
            };
            match service.store_link(link) {
                Ok(id) => tool_success(json!({ "linked": true, "archive_link_id": id })),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "list_artifacts" => {
            let source_type =
                match parse_optional_enum(&arguments, "source_type", SourceType::from_str) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
            let enrichment_status = match parse_optional_enum(
                &arguments,
                "enrichment_status",
                EnrichmentStatus::from_str,
            ) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let captured_after = arguments
                .get("captured_after")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let captured_before = arguments
                .get("captured_before")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(20);
            let offset = arguments
                .get("offset")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(0);
            let filters = ArtifactListFilters {
                source_type,
                enrichment_status,
                captured_after,
                captured_before,
            };
            match app
                .artifacts
                .list_artifacts_filtered(&filters, limit, offset)
            {
                Ok(artifacts) => {
                    let artifacts_json =
                        serde_json::to_value(&artifacts).expect("artifact list serializable");
                    tool_success(json!({ "artifacts": artifacts_json }))
                }
                Err(err) => tool_storage_error(&err),
            }
        }
        "list_review_items" => {
            let service = match app.review.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "list_review_items is unavailable for the configured provider",
                    )
                }
            };
            let kinds = match parse_enum_array(&arguments, "kinds", ReviewItemKind::from_str) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(20)
                .clamp(1, REVIEW_LIMIT_CAP);
            match service.list(review::ReviewQueueRequest {
                filters: ReviewQueueFilters { kinds },
                limit,
            }) {
                Ok(response) => {
                    tool_success(serde_json::to_value(response).expect("review queue serializable"))
                }
                Err(err) => tool_app_error(&err),
            }
        }
        "record_review_decision" => {
            let service = match app.review.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "record_review_decision is unavailable for the configured provider",
                    )
                }
            };
            let kind = match parse_required_enum(&arguments, "kind", ReviewItemKind::from_str) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
                Some(v) => v.to_string(),
                None => {
                    return tool_error(
                        "invalid_params",
                        "record_review_decision requires artifact_id",
                    )
                }
            };
            let derived_object_id = arguments
                .get("derived_object_id")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let decision_status = match parse_required_enum(
                &arguments,
                "decision_status",
                ReviewDecisionStatus::from_str,
            ) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let note_text = arguments
                .get("note_text")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let decided_by = arguments
                .get("decided_by")
                .and_then(Value::as_str)
                .map(|s| s.to_string());

            match service.record_decision(review::ReviewDecisionRequest {
                kind,
                artifact_id,
                derived_object_id,
                decision_status,
                note_text,
                decided_by,
            }) {
                Ok(review_decision_id) => tool_success(
                    json!({ "recorded": true, "review_decision_id": review_decision_id }),
                ),
                Err(err) => tool_app_error(&err),
            }
        }
        "retry_artifact_enrichment" => {
            let service = match app.review.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "retry_artifact_enrichment is unavailable for the configured provider",
                    )
                }
            };
            let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
                Some(v) => v.to_string(),
                None => {
                    return tool_error(
                        "invalid_params",
                        "retry_artifact_enrichment requires artifact_id",
                    )
                }
            };
            match service.retry_artifact(review::RetryArtifactRequest { artifact_id }) {
                Ok(job_id) => tool_success(json!({ "queued": true, "job_id": job_id })),
                Err(err) => tool_app_error(&err),
            }
        }
        "update_object" => {
            let service = match app.writeback.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "update_object is unavailable for the configured provider",
                    )
                }
            };
            let derived_object_id = match arguments.get("derived_object_id").and_then(Value::as_str)
            {
                Some(v) => v.to_string(),
                None => {
                    return tool_error("invalid_params", "update_object requires derived_object_id")
                }
            };
            let new_status = match arguments.get("new_status").and_then(Value::as_str) {
                Some(v) => match ObjectStatus::from_str(v) {
                    Some(s) => s,
                    None => {
                        return tool_error("invalid_params", &format!("invalid new_status: {v}"))
                    }
                },
                None => return tool_error("invalid_params", "update_object requires new_status"),
            };
            let replacement_object_id = arguments
                .get("replacement_object_id")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let contributed_by = arguments
                .get("contributed_by")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let update = UpdateObjectStatus {
                derived_object_id,
                new_status,
                replacement_object_id,
                contributed_by,
            };
            match service.update_object_status(update) {
                Ok(()) => tool_success(json!({ "updated": true })),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "store_entity" => {
            let service = match app.writeback.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "store_entity is unavailable for the configured provider",
                    )
                }
            };
            let title = match arguments.get("title").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "store_entity requires title"),
            };
            let body_text = match arguments.get("body_text").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "store_entity requires body_text"),
            };
            let entity_type = match arguments.get("entity_type").and_then(Value::as_str) {
                Some(v) => v,
                None => return tool_error("invalid_params", "store_entity requires entity_type"),
            };
            let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
                Some(v) => v.to_string(),
                None => return tool_error("invalid_params", "store_entity requires artifact_id"),
            };
            let candidate_key = arguments
                .get("candidate_key")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let contributed_by = arguments
                .get("contributed_by")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let evidence = match parse_evidence_array(&arguments) {
                Ok(v) => v,
                Err(e) => return e,
            };
            let derived_object_id = new_id("dobj");
            let entity = NewAgentEntity {
                derived_object_id: derived_object_id.clone(),
                artifact_id,
                title: title.to_string(),
                body_text: body_text.to_string(),
                entity_type: entity_type.to_string(),
                candidate_key,
                contributed_by,
                evidence,
            };
            match service.store_entity(entity) {
                Ok(id) => tool_success(json!({ "stored": true, "derived_object_id": id })),
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "get_related" => {
            let service = match app.object_search.as_ref() {
                Some(s) => s,
                None => {
                    return tool_error(
                        "service_unavailable",
                        "get_related is unavailable for the configured provider",
                    )
                }
            };
            let derived_object_id = match arguments.get("derived_object_id").and_then(Value::as_str)
            {
                Some(v) => v,
                None => {
                    return tool_error("invalid_params", "get_related requires derived_object_id")
                }
            };
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(20);
            match service.get_related(derived_object_id, limit) {
                Ok(entries) => {
                    let entries_json =
                        serde_json::to_value(&entries).expect("related entries serializable");
                    tool_success(json!({ "related": entries_json }))
                }
                Err(err) => tool_error("internal_error", &err.to_string()),
            }
        }
        "get_timeline" => {
            let source_type =
                match parse_optional_enum(&arguments, "source_type", SourceType::from_str) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
            let keyword = arguments
                .get("keyword")
                .and_then(Value::as_str)
                .map(|s| s.to_string());
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(20);
            let offset = arguments
                .get("offset")
                .and_then(Value::as_u64)
                .map(|v| v as usize)
                .unwrap_or(0);
            let filters = TimelineFilters {
                keyword,
                source_type,
            };
            match app.artifacts.get_timeline(&filters, limit, offset) {
                Ok(entries) => {
                    let entries_json =
                        serde_json::to_value(&entries).expect("timeline serializable");
                    tool_success(json!({ "entries": entries_json }))
                }
                Err(err) => tool_storage_error(&err),
            }
        }
        _ => tool_error("invalid_params", &format!("unknown tool: {name}")),
    }
}

fn parse_optional_enum<T>(
    arguments: &Value,
    field: &str,
    parser: fn(&str) -> Option<T>,
) -> std::result::Result<Option<T>, Value> {
    match arguments.get(field).and_then(Value::as_str) {
        Some(value) => match parser(value) {
            Some(parsed) => Ok(Some(parsed)),
            None => Err(tool_error(
                "invalid_params",
                &format!("invalid {field}: {value}"),
            )),
        },
        None => Ok(None),
    }
}

fn parse_required_enum<T>(
    arguments: &Value,
    field: &str,
    parser: fn(&str) -> Option<T>,
) -> std::result::Result<T, Value> {
    match arguments.get(field).and_then(Value::as_str) {
        Some(value) => parser(value)
            .ok_or_else(|| tool_error("invalid_params", &format!("invalid {field}: {value}"))),
        None => Err(tool_error(
            "invalid_params",
            &format!("tools/call arguments.{field} is required"),
        )),
    }
}

fn parse_enum_array<T>(
    arguments: &Value,
    field: &str,
    parser: fn(&str) -> Option<T>,
) -> std::result::Result<Option<Vec<T>>, Value> {
    let Some(items) = arguments.get(field) else {
        return Ok(None);
    };
    let array = items.as_array().ok_or_else(|| {
        tool_error(
            "invalid_params",
            &format!("tools/call arguments.{field} must be an array"),
        )
    })?;
    let mut parsed = Vec::with_capacity(array.len());
    for item in array {
        let raw = item.as_str().ok_or_else(|| {
            tool_error(
                "invalid_params",
                &format!("tools/call arguments.{field} items must be strings"),
            )
        })?;
        let value = parser(raw)
            .ok_or_else(|| tool_error("invalid_params", &format!("invalid {field}: {raw}")))?;
        parsed.push(value);
    }
    Ok(Some(parsed))
}

fn parse_evidence_array(
    arguments: &Value,
) -> std::result::Result<Vec<NewAgentEvidenceLink>, Value> {
    let items = match arguments.get("evidence").and_then(Value::as_array) {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };
    let mut evidence = Vec::with_capacity(items.len());
    for item in items {
        let segment_id = match item.get("segment_id").and_then(Value::as_str) {
            Some(v) => v.to_string(),
            None => {
                return Err(tool_error(
                    "invalid_params",
                    "evidence item requires segment_id",
                ))
            }
        };
        let evidence_role = match item
            .get("evidence_role")
            .and_then(Value::as_str)
            .and_then(EvidenceRole::from_str)
        {
            Some(v) => v,
            None => {
                return Err(tool_error(
                    "invalid_params",
                    "evidence item requires valid evidence_role",
                ))
            }
        };
        let support_strength = match item
            .get("support_strength")
            .and_then(Value::as_str)
            .and_then(SupportStrength::from_str)
        {
            Some(v) => v,
            None => {
                return Err(tool_error(
                    "invalid_params",
                    "evidence item requires valid support_strength",
                ))
            }
        };
        evidence.push(NewAgentEvidenceLink {
            evidence_link_id: new_id("elink"),
            segment_id,
            evidence_role,
            support_strength,
        });
    }
    Ok(evidence)
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

fn tool_error(code: &str, message: &str) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": message
        }],
        "structuredContent": {
            "error": true,
            "code": code,
            "message": message
        },
        "isError": true
    })
}

fn tool_success(value: Value) -> Value {
    let text = summarize_tool_result(&value);
    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": value,
        "isError": false
    })
}

fn tool_storage_error(err: &crate::error::StorageError) -> Value {
    match err {
        crate::error::StorageError::UnsupportedOperation { .. } => {
            tool_error("service_unavailable", &err.to_string())
        }
        _ => tool_error("internal_error", &err.to_string()),
    }
}

fn tool_app_error(err: &crate::error::OpenArchiveError) -> Value {
    match err {
        crate::error::OpenArchiveError::Invariant(detail) => tool_error("invalid_params", detail),
        crate::error::OpenArchiveError::Storage(storage_err) => tool_storage_error(storage_err),
        _ => tool_error("internal_error", &err.to_string()),
    }
}

fn summarize_tool_result(value: &Value) -> String {
    if let Some(items) = value.get("items").and_then(Value::as_array) {
        return format!("{} review items", items.len());
    }

    if let Some(results) = value.get("results").and_then(Value::as_array) {
        return format!("{} results", results.len());
    }

    if let Some(hits) = value.get("hits").and_then(Value::as_array) {
        return format!("{} hits", hits.len());
    }

    if value
        .get("stored")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "stored".to_string();
    }

    if value
        .get("linked")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "linked".to_string();
    }

    if value
        .get("updated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "updated".to_string();
    }

    if value
        .get("recorded")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "recorded".to_string();
    }

    if value
        .get("queued")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return "queued".to_string();
    }

    if let Some(artifacts) = value.get("artifacts").and_then(Value::as_array) {
        return format!("{} artifacts", artifacts.len());
    }

    if let Some(entries) = value.get("entries").and_then(Value::as_array) {
        return format!("{} entries", entries.len());
    }

    if let Some(related) = value.get("related").and_then(Value::as_array) {
        return format!("{} related objects", related.len());
    }

    if let Some(context_pack) = value.get("context_pack").and_then(Value::as_object) {
        let summaries = context_pack
            .get("summaries")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let classifications = context_pack
            .get("classifications")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let memories = context_pack
            .get("memories")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let relationships = context_pack
            .get("relationships")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        return format!(
            "context pack: {} summaries, {} classifications, {} memories, {} relationships",
            summaries, classifications, memories, relationships
        );
    }

    if let Some(artifact) = value.get("artifact").and_then(Value::as_object) {
        let segments = artifact
            .get("segment_count")
            .and_then(Value::as_u64)
            .or_else(|| {
                artifact
                    .get("returned_segment_count")
                    .and_then(Value::as_u64)
            })
            .or_else(|| {
                artifact
                    .get("segments")
                    .and_then(Value::as_array)
                    .map(|v| v.len() as u64)
            })
            .unwrap_or(0);
        let (summaries, classifications, memories) = artifact
            .get("derived_objects")
            .and_then(Value::as_array)
            .map(|objects| {
                objects
                    .iter()
                    .fold((0_u64, 0_u64, 0_u64), |mut counts, obj| {
                        match obj
                            .get("derived_object_type")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                        {
                            "summary" => counts.0 += 1,
                            "classification" => counts.1 += 1,
                            "memory" => counts.2 += 1,
                            _ => {}
                        }
                        counts
                    })
            })
            .unwrap_or_else(|| {
                let enrichments = artifact
                    .get("enrichment")
                    .and_then(Value::as_object)
                    .and_then(|enrichment| enrichment.get("counts"))
                    .and_then(Value::as_object);
                (
                    enrichments
                        .and_then(|counts| counts.get("summaries"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    enrichments
                        .and_then(|counts| counts.get("classifications"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    enrichments
                        .and_then(|counts| counts.get("memories"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                )
            });
        return format!(
            "artifact: {} summaries, {} classifications, {} memories, {} segments",
            summaries, classifications, memories, segments
        );
    }

    if let Some(found) = value.get("found").and_then(Value::as_bool) {
        return if found {
            "found".to_string()
        } else {
            "not found".to_string()
        };
    }

    "ok".to_string()
}

fn read_jsonrpc_message(reader: &mut impl BufRead) -> io::Result<Option<JsonRpcRequest>> {
    consume_leading_blank_lines(reader)?;
    let buffer = reader.fill_buf()?;
    if buffer.is_empty() {
        return Ok(None);
    }

    if !matches!(buffer[0], b'{' | b'[') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stdio MCP expects newline-delimited JSON-RPC input",
        ));
    }

    read_jsonrpc_line_message(reader)
}

fn consume_leading_blank_lines(reader: &mut impl BufRead) -> io::Result<()> {
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(());
        }

        match buffer[0] {
            b'\r' | b'\n' => reader.consume(1),
            b' ' | b'\t' => reader.consume(1),
            _ => return Ok(()),
        }
    }
}

fn read_jsonrpc_line_message(reader: &mut impl BufRead) -> io::Result<Option<JsonRpcRequest>> {
    let mut payload = Vec::new();
    let bytes_read = reader.read_until(b'\n', &mut payload)?;
    if bytes_read == 0 {
        return Ok(None);
    }

    while matches!(payload.last(), Some(b'\r' | b'\n')) {
        payload.pop();
    }

    serde_json::from_slice(&payload).map(Some).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid newline-delimited JSON-RPC payload: {err}"),
        )
    })
}

fn write_jsonrpc_message(writer: &mut impl Write, value: &Value) -> io::Result<()> {
    let payload =
        serde_json::to_vec(value).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    writer.write_all(&payload)?;
    writer.write_all(b"\n")
}

fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn jsonrpc_error(id: Value, code: i32, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    })
}

#[derive(Debug, serde::Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::app::artifact_detail::ArtifactDetailService;
    use crate::app::context_pack::ContextPackService;
    use crate::app::retrieval::ArchiveRetrievalService;
    use crate::app::review::ReviewService;
    use crate::app::search::ArchiveSearchService;
    use crate::storage::{ArchiveRetrievalStore, RetrievalIntent, RetrievedContextItem};

    fn test_app() -> ArchiveApplication {
        let search = Some(ArchiveSearchService::new(Arc::new(MockSearchReadStore)));
        let artifact_detail = Some(ArtifactDetailService::new(Arc::new(
            MockArtifactDetailStore,
        )));
        let context_pack = Some(ContextPackService::new(Arc::new(MockContextPackStore)));
        let object_search = Some(crate::app::search::ObjectSearchService::new(
            Arc::new(MockObjectSearchStore),
            None,
        ));
        let writeback = Some(crate::app::writeback::WritebackService::new(Arc::new(
            MockWritebackStore,
        )));
        let review = Some(ReviewService::new(Arc::new(MockReviewStore)));
        ArchiveApplication {
            artifacts: crate::app::artifacts::ArtifactQueryService::new(Arc::new(
                MockArtifactReadStore,
            )),
            imports: crate::app::imports::ImportApplicationService::new(
                Arc::new(MockImportWriteStore),
                Arc::new(MockObjectStore),
            ),
            retrieval: Arc::new(ArchiveRetrievalService::new(Arc::new(MockRetrievalStore))),
            search,
            artifact_detail,
            context_pack,
            object_search,
            review,
            writeback,
        }
    }

    #[test]
    fn tools_call_returns_found_false_for_missing_artifact() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "get_artifact",
                "arguments": {
                    "artifact_id": "missing"
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(false));
        assert_eq!(
            response["structuredContent"],
            json!({
                "found": false
            })
        );
    }

    #[test]
    fn search_archive_clamps_limit() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "search_archive",
                "arguments": {
                    "query": "abc",
                    "limit": 999
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(false));
        assert!(response["structuredContent"]["hits"].is_array());
    }

    #[test]
    fn store_memory_requires_artifact_id() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "store_memory",
                "arguments": {
                    "title": "Memory",
                    "body_text": "Remember this",
                    "memory_type": "fact"
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(true));
        assert_eq!(
            response["structuredContent"]["message"],
            "store_memory requires artifact_id"
        );
    }

    #[test]
    fn link_objects_rejects_self_links() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "link_objects",
                "arguments": {
                    "source_object_id": "dobj-1",
                    "target_object_id": "dobj-1",
                    "link_type": "same_as"
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(true));
        assert_eq!(
            response["structuredContent"]["message"],
            "link_objects: source and target must be different objects"
        );
    }

    #[test]
    fn read_jsonrpc_message_accepts_newline_delimited_json() {
        let mut reader = Cursor::new(
            br#"{"jsonrpc":"2.0","id":0,"method":"initialize","params":{}}
"#
            .to_vec(),
        );

        let request = read_jsonrpc_message(&mut reader)
            .expect("line-delimited request should parse")
            .expect("request should be present");

        assert_eq!(request.method, "initialize");
        assert_eq!(request.id, Some(json!(0)));
    }

    #[test]
    fn read_jsonrpc_message_rejects_header_framed_input() {
        let payload = br#"{"jsonrpc":"2.0","id":0,"method":"initialize"}"#;
        let framed = format!("Content-Length: {}\r\n\r\n", payload.len()).into_bytes();
        let mut bytes = framed;
        bytes.extend_from_slice(payload);
        let mut reader = Cursor::new(bytes);

        let err = read_jsonrpc_message(&mut reader).expect_err("header framing should be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("newline-delimited JSON-RPC input"));
    }

    #[test]
    fn tool_success_uses_compact_summary_text() {
        let response = tool_success(json!({
            "results": [
                { "id": 1 },
                { "id": 2 }
            ]
        }));

        assert_eq!(response["content"][0]["text"], "2 results");
        assert_eq!(
            response["structuredContent"]["results"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
    }

    #[test]
    fn get_artifact_summary_text_prefers_payload_shape_over_found_flag() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "get_artifact",
                "arguments": {
                    "artifact_id": "artifact-1"
                }
            }),
        );

        assert_ne!(
            response["content"][0]["text"],
            Value::String("found".to_string())
        );
        assert_eq!(
            response["content"][0]["text"],
            Value::String(
                "artifact: 1 summaries, 1 classifications, 1 memories, 1 segments".to_string()
            )
        );
    }

    #[test]
    fn get_context_pack_summary_text_prefers_payload_shape_over_found_flag() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "get_context_pack",
                "arguments": {
                    "artifact_id": "artifact-1"
                }
            }),
        );

        assert_ne!(
            response["content"][0]["text"],
            Value::String("found".to_string())
        );
        assert_eq!(
            response["content"][0]["text"],
            Value::String(
                "context pack: 0 summaries, 0 classifications, 0 memories, 0 relationships"
                    .to_string()
            )
        );
    }

    #[test]
    fn list_review_items_returns_items_with_compact_summary_text() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "list_review_items",
                "arguments": {
                    "kinds": ["artifact_needs_attention"],
                    "limit": 10
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(false));
        assert_eq!(response["content"][0]["text"], "1 review items");
        assert_eq!(
            response["structuredContent"]["items"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn record_review_decision_surfaces_invalid_params() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "record_review_decision",
                "arguments": {
                    "kind": "artifact_needs_attention",
                    "artifact_id": "artifact-1",
                    "decision_status": "noted"
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(true));
        assert_eq!(
            response["structuredContent"]["message"],
            "review notes require non-empty note_text"
        );
    }

    #[test]
    fn retry_artifact_enrichment_returns_job_id() {
        let response = call_tool(
            &test_app(),
            json!({
                "name": "retry_artifact_enrichment",
                "arguments": {
                    "artifact_id": "artifact-9"
                }
            }),
        );

        assert_eq!(response["isError"], Value::Bool(false));
        assert_eq!(
            response["structuredContent"],
            json!({
                "queued": true,
                "job_id": "job-retry-artifact-9"
            })
        );
        assert_eq!(response["content"][0]["text"], "queued");
    }

    struct MockSearchReadStore;
    impl crate::storage::ArchiveSearchReadStore for MockSearchReadStore {
        fn search_candidates(
            &self,
            _query_text: &str,
            _limit: usize,
            _filters: &crate::storage::SearchFilters,
        ) -> crate::error::StorageResult<Vec<crate::storage::ArchiveSearchCandidate>> {
            Ok(vec![crate::storage::ArchiveSearchCandidate {
                artifact_id: "artifact-1".to_string(),
                match_record_id: "artifact-1".to_string(),
                match_kind: crate::storage::SearchCandidateKind::ArtifactTitle,
                snippet: "title".to_string(),
                score_hint: 300,
            }])
        }
    }

    struct MockRetrievalStore;
    impl ArchiveRetrievalStore for MockRetrievalStore {
        fn retrieve_for_intents(
            &self,
            _artifact_id: &str,
            _intents: &[RetrievalIntent],
            _limit_per_intent: usize,
        ) -> crate::error::StorageResult<Vec<RetrievedContextItem>> {
            Ok(Vec::new())
        }
    }

    struct MockArtifactDetailStore;
    impl crate::storage::ArtifactDetailReadStore for MockArtifactDetailStore {
        fn load_artifact_detail(
            &self,
            artifact_id: &str,
        ) -> crate::error::StorageResult<Option<crate::storage::ArtifactDetailView>> {
            if artifact_id == "missing" {
                return Ok(None);
            }
            Ok(Some(crate::storage::ArtifactDetailView {
                artifact: crate::storage::ArtifactDetailRecord {
                    artifact_id: artifact_id.to_string(),
                    title: Some("Artifact".to_string()),
                    source_type: crate::storage::SourceType::ChatGptExport,
                    enrichment_status: crate::storage::EnrichmentStatus::Completed,
                },
                segments: vec![crate::storage::ArtifactDetailSegment {
                    segment_id: "seg-1".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 1,
                    text_content: "hello".to_string(),
                }],
                derived_objects: vec![
                    crate::storage::ArtifactDetailDerivedObject {
                        derived_object_id: "sum-1".to_string(),
                        derived_object_type: crate::storage::DerivedObjectType::Summary,
                        title: Some("Summary".to_string()),
                        body_text: Some("Summary body".to_string()),
                        confidence_score: None,
                    },
                    crate::storage::ArtifactDetailDerivedObject {
                        derived_object_id: "cls-1".to_string(),
                        derived_object_type: crate::storage::DerivedObjectType::Classification,
                        title: Some("Classification".to_string()),
                        body_text: Some("Classification body".to_string()),
                        confidence_score: None,
                    },
                    crate::storage::ArtifactDetailDerivedObject {
                        derived_object_id: "mem-1".to_string(),
                        derived_object_type: crate::storage::DerivedObjectType::Memory,
                        title: Some("Memory".to_string()),
                        body_text: Some("Memory body".to_string()),
                        confidence_score: None,
                    },
                ],
            }))
        }
    }

    struct MockContextPackStore;
    impl crate::storage::ArtifactContextPackReadStore for MockContextPackStore {
        fn load_artifact_context_pack_material(
            &self,
            artifact_id: &str,
        ) -> crate::error::StorageResult<Option<crate::storage::ArtifactContextPackMaterial>>
        {
            if artifact_id == "missing" {
                return Ok(None);
            }
            Ok(Some(crate::storage::ArtifactContextPackMaterial {
                artifact: crate::storage::ArtifactDetailRecord {
                    artifact_id: artifact_id.to_string(),
                    title: Some("Artifact".to_string()),
                    source_type: crate::storage::SourceType::ChatGptExport,
                    enrichment_status: crate::storage::EnrichmentStatus::Completed,
                },
                segments: vec![],
                derived_objects: vec![],
                evidence_links: vec![],
            }))
        }
    }

    struct MockObjectSearchStore;
    impl crate::storage::DerivedObjectSearchStore for MockObjectSearchStore {
        fn search_objects(
            &self,
            _filters: &crate::storage::ObjectSearchFilters,
            _limit: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::DerivedObjectSearchResult>> {
            Ok(vec![])
        }

        fn search_objects_by_embedding(
            &self,
            _filters: &crate::storage::ObjectSearchFilters,
            _query_embedding: &[f32],
            _limit: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::DerivedObjectSearchResult>> {
            Ok(vec![])
        }

        fn get_related_objects(
            &self,
            _derived_object_id: &str,
            _limit: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::GraphRelatedEntry>> {
            Ok(vec![])
        }
    }

    struct MockArtifactReadStore;
    impl crate::storage::ArtifactReadStore for MockArtifactReadStore {
        fn list_artifacts(
            &self,
        ) -> crate::error::StorageResult<Vec<crate::storage::ArtifactListItem>> {
            Ok(vec![])
        }

        fn list_artifacts_filtered(
            &self,
            _filters: &crate::storage::ArtifactListFilters,
            _limit: usize,
            _offset: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::ArtifactListItem>> {
            Ok(vec![])
        }

        fn get_timeline(
            &self,
            _filters: &crate::storage::TimelineFilters,
            _limit: usize,
            _offset: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::TimelineEntry>> {
            Ok(vec![])
        }

        fn load_artifact_for_enrichment(
            &self,
            _artifact_id: &str,
        ) -> crate::error::StorageResult<Option<crate::storage::LoadedArtifactForEnrichment>>
        {
            Ok(None)
        }
    }

    struct MockImportWriteStore;
    impl crate::storage::ImportWriteStore for MockImportWriteStore {
        fn write_import(
            &self,
            _import_set: crate::storage::WriteImportSet,
        ) -> crate::error::StorageResult<crate::storage::ImportWriteResult> {
            unreachable!()
        }
    }

    struct MockObjectStore;
    impl crate::object_store::ObjectStore for MockObjectStore {
        fn put_object(
            &self,
            _object: crate::object_store::NewObject,
        ) -> crate::error::ObjectStoreResult<crate::object_store::PutObjectResult> {
            unreachable!()
        }

        fn get_object_bytes(
            &self,
            _object: &crate::object_store::StoredObject,
        ) -> crate::error::ObjectStoreResult<Vec<u8>> {
            unreachable!()
        }

        fn delete_object(
            &self,
            _object: &crate::object_store::StoredObject,
        ) -> crate::error::ObjectStoreResult<()> {
            unreachable!()
        }
    }

    struct MockWritebackStore;
    impl crate::storage::WritebackStore for MockWritebackStore {
        fn store_agent_memory(
            &self,
            _memory: &crate::storage::NewAgentMemory,
        ) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn store_archive_link(
            &self,
            _link: &crate::storage::NewArchiveLink,
        ) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn update_object_status(
            &self,
            _update: &crate::storage::UpdateObjectStatus,
        ) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn store_agent_entity(
            &self,
            _entity: &crate::storage::NewAgentEntity,
        ) -> crate::error::StorageResult<()> {
            Ok(())
        }
    }

    struct MockReviewStore;
    impl crate::storage::ReviewReadStore for MockReviewStore {
        fn list_review_candidates(
            &self,
            filters: &crate::storage::ReviewQueueFilters,
            limit: usize,
        ) -> crate::error::StorageResult<Vec<crate::storage::ReviewCandidate>> {
            let mut items = vec![
                crate::storage::ReviewCandidate {
                    kind: crate::storage::ReviewItemKind::ArtifactNeedsAttention,
                    artifact_id: "artifact-1".to_string(),
                    derived_object_id: None,
                    source_type: crate::storage::SourceType::ChatGptExport,
                    captured_at: "2026-03-24T10:00:00.000000+00".to_string(),
                    title: Some("Artifact failed".to_string()),
                    body_text: None,
                    derived_object_type: None,
                    candidate_key: None,
                    enrichment_status: Some(crate::storage::EnrichmentStatus::Failed),
                    confidence_score: None,
                    related_artifact_count: None,
                },
                crate::storage::ReviewCandidate {
                    kind: crate::storage::ReviewItemKind::ObjectLowConfidence,
                    artifact_id: "artifact-2".to_string(),
                    derived_object_id: Some("obj-2".to_string()),
                    source_type: crate::storage::SourceType::ClaudeExport,
                    captured_at: "2026-03-23T10:00:00.000000+00".to_string(),
                    title: Some("Weak object".to_string()),
                    body_text: Some("body".to_string()),
                    derived_object_type: Some(crate::storage::DerivedObjectType::Memory),
                    candidate_key: None,
                    enrichment_status: None,
                    confidence_score: Some(0.42),
                    related_artifact_count: None,
                },
            ];
            if let Some(kinds) = filters.kinds.as_ref() {
                items.retain(|item| kinds.contains(&item.kind));
            }
            items.truncate(limit);
            Ok(items)
        }
    }

    impl crate::storage::ReviewWriteStore for MockReviewStore {
        fn record_review_decision(
            &self,
            _decision: &crate::storage::NewReviewDecision,
        ) -> crate::error::StorageResult<()> {
            Ok(())
        }

        fn retry_artifact_enrichment(
            &self,
            artifact_id: &str,
        ) -> crate::error::StorageResult<String> {
            Ok(format!("job-retry-{artifact_id}"))
        }
    }
}
