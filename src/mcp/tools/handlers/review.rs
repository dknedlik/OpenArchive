use serde_json::{json, Value};

use crate::app::{review, ArchiveApplication};
use crate::storage::{ReviewDecisionStatus, ReviewItemKind, ReviewQueueFilters};

use super::super::parse::{parse_enum_array, parse_required_enum};
use super::super::result::{tool_app_error, tool_error, tool_storage_error, tool_success};
use super::super::REVIEW_LIMIT_CAP;

pub(in crate::mcp::tools) fn handle_list_review_items(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let service = match app.require_review() {
        Ok(s) => s,
        Err(err) => return tool_storage_error(&err),
    };
    let kinds = match parse_enum_array(arguments, "kinds", ReviewItemKind::parse) {
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

pub(in crate::mcp::tools) fn handle_record_review_decision(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let service = match app.require_review() {
        Ok(s) => s,
        Err(err) => return tool_storage_error(&err),
    };
    let kind = match parse_required_enum(arguments, "kind", ReviewItemKind::parse) {
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
    let decision_status =
        match parse_required_enum(arguments, "decision_status", ReviewDecisionStatus::parse) {
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
        Ok(review_decision_id) => {
            tool_success(json!({ "recorded": true, "review_decision_id": review_decision_id }))
        }
        Err(err) => tool_app_error(&err),
    }
}

pub(in crate::mcp::tools) fn handle_retry_artifact_enrichment(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let service = match app.require_review() {
        Ok(s) => s,
        Err(err) => return tool_storage_error(&err),
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
