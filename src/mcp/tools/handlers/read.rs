use serde_json::{json, Value};

use crate::app::{artifact_detail, context_pack, search, ArchiveApplication};
use crate::storage::types::{ArtifactListFilters, TimelineFilters};
use crate::storage::{
    DerivedObjectType, EnrichmentStatus, ObjectSearchFilters, SearchFilters, SourceType,
};

use super::super::parse::parse_optional_enum;
use super::super::result::{tool_error, tool_storage_error, tool_success};
use super::super::SEARCH_LIMIT_CAP;

pub(in crate::mcp::tools) fn handle_search_archive(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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
        None => return tool_error("invalid_params", "search_archive requires a string query"),
    };
    let limit = arguments
        .get("limit")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(10)
        .clamp(1, SEARCH_LIMIT_CAP);
    let object_type = match parse_optional_enum(arguments, "object_type", DerivedObjectType::parse)
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    let source_type = match parse_optional_enum(arguments, "source_type", SourceType::parse) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let tag = arguments
        .get("tag")
        .and_then(Value::as_str)
        .map(|value| value.to_lowercase());
    let alias = arguments
        .get("alias")
        .and_then(Value::as_str)
        .map(|value| value.to_lowercase());
    let path_prefix = arguments
        .get("path_prefix")
        .and_then(Value::as_str)
        .map(|value| value.to_lowercase());
    match service.search(search::ArchiveSearchRequest {
        query_text: query.to_string(),
        limit,
        filters: SearchFilters {
            object_type,
            source_type,
            tag,
            alias,
            path_prefix,
        },
    }) {
        Ok(response) => {
            tool_success(serde_json::to_value(response).expect("search response serializable"))
        }
        Err(err) => tool_error("internal_error", &err.to_string()),
    }
}

pub(in crate::mcp::tools) fn handle_get_artifact(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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

pub(in crate::mcp::tools) fn handle_get_context_pack(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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
        None => return tool_error("invalid_params", "get_context_pack requires artifact_id"),
    };
    match service.assemble(context_pack::ContextPackRequest {
        artifact_id: artifact_id.to_string(),
    }) {
        Ok(Some(response)) => tool_success(json!({ "found": true, "context_pack": response })),
        Ok(None) => tool_success(json!({ "found": false })),
        Err(err) => tool_error("internal_error", &err.to_string()),
    }
}

pub(in crate::mcp::tools) fn handle_get_note_metadata(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let service = match app.artifact_detail.as_ref() {
        Some(s) => s,
        None => {
            return tool_error(
                "service_unavailable",
                "get_note_metadata is unavailable for the configured provider",
            )
        }
    };
    let artifact_id = match arguments.get("artifact_id").and_then(Value::as_str) {
        Some(id) => id,
        None => return tool_error("invalid_params", "get_note_metadata requires artifact_id"),
    };
    match service.get(artifact_detail::ArtifactDetailRequest {
        artifact_id: artifact_id.to_string(),
        include_segments: false,
        segment_offset: 0,
        segment_limit: artifact_detail::DEFAULT_SEGMENT_LIMIT,
    }) {
        Ok(Some(response)) => tool_success(json!({
            "found": true,
            "note": {
                "artifact_id": response.artifact_id,
                "title": response.title,
                "source_type": response.source_type,
                "enrichment_status": response.enrichment_status,
                "note_path": response.note_path,
                "imported_note_metadata": response.imported_note_metadata,
                "inbound_note_links": response.inbound_note_links
            }
        })),
        Ok(None) => tool_success(json!({ "found": false })),
        Err(err) => tool_error("internal_error", &err.to_string()),
    }
}

pub(in crate::mcp::tools) fn handle_search_objects(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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
    let object_type = match parse_optional_enum(arguments, "object_type", DerivedObjectType::parse)
    {
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
            let results_json: Vec<Value> = results
                .into_iter()
                .map(|r| {
                    json!({
                        "derived_object_id": r.derived_object_id,
                        "artifact_id": r.artifact_id,
                        "derived_object_type": r.derived_object_type.as_str(),
                        "title": r.title,
                        "body_text": r.body_text,
                        "candidate_key": r.candidate_key,
                        "confidence_score": r.confidence_score,
                        "score": r.score,
                    })
                })
                .collect();
            tool_success(json!({ "results": results_json }))
        }
        Err(err) => tool_error("internal_error", &err.to_string()),
    }
}

pub(in crate::mcp::tools) fn handle_list_artifacts(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let source_type = match parse_optional_enum(arguments, "source_type", SourceType::parse) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let enrichment_status =
        match parse_optional_enum(arguments, "enrichment_status", EnrichmentStatus::parse) {
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
        tag: arguments
            .get("tag")
            .and_then(Value::as_str)
            .map(|value| value.to_lowercase()),
        alias: arguments
            .get("alias")
            .and_then(Value::as_str)
            .map(|value| value.to_lowercase()),
        path_prefix: arguments
            .get("path_prefix")
            .and_then(Value::as_str)
            .map(|value| value.to_lowercase()),
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

pub(in crate::mcp::tools) fn handle_get_related(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let service = match app.object_search.as_ref() {
        Some(s) => s,
        None => {
            return tool_error(
                "service_unavailable",
                "get_related is unavailable for the configured provider",
            )
        }
    };
    let derived_object_id = match arguments.get("derived_object_id").and_then(Value::as_str) {
        Some(v) => v,
        None => return tool_error("invalid_params", "get_related requires derived_object_id"),
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

pub(in crate::mcp::tools) fn handle_get_timeline(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let source_type = match parse_optional_enum(arguments, "source_type", SourceType::parse) {
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
        tag: arguments
            .get("tag")
            .and_then(Value::as_str)
            .map(|value| value.to_lowercase()),
        path_prefix: arguments
            .get("path_prefix")
            .and_then(Value::as_str)
            .map(|value| value.to_lowercase()),
    };
    match app.artifacts.get_timeline(&filters, limit, offset) {
        Ok(entries) => {
            let entries_json = serde_json::to_value(&entries).expect("timeline serializable");
            tool_success(json!({ "entries": entries_json }))
        }
        Err(err) => tool_storage_error(&err),
    }
}
