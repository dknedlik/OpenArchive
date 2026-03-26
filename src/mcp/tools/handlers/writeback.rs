use serde_json::{json, Value};

use crate::app::ArchiveApplication;
use crate::storage::writeback_store::{
    NewAgentEntity, NewAgentMemory, NewArchiveLink, UpdateObjectStatus,
};
use crate::storage::ObjectStatus;

use super::super::parse::{new_id, parse_evidence_array};
use super::super::result::{tool_error, tool_success};

pub(in crate::mcp::tools) fn handle_store_memory(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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
    let evidence = match parse_evidence_array(arguments) {
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

pub(in crate::mcp::tools) fn handle_link_objects(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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
        None => return tool_error("invalid_params", "link_objects requires source_object_id"),
    };
    let target_object_id = match arguments.get("target_object_id").and_then(Value::as_str) {
        Some(v) => v,
        None => return tool_error("invalid_params", "link_objects requires target_object_id"),
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

pub(in crate::mcp::tools) fn handle_update_object(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
    let service = match app.writeback.as_ref() {
        Some(s) => s,
        None => {
            return tool_error(
                "service_unavailable",
                "update_object is unavailable for the configured provider",
            )
        }
    };
    let derived_object_id = match arguments.get("derived_object_id").and_then(Value::as_str) {
        Some(v) => v.to_string(),
        None => return tool_error("invalid_params", "update_object requires derived_object_id"),
    };
    let new_status = match arguments.get("new_status").and_then(Value::as_str) {
        Some(v) => match ObjectStatus::from_str(v) {
            Some(s) => s,
            None => return tool_error("invalid_params", &format!("invalid new_status: {v}")),
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

pub(in crate::mcp::tools) fn handle_store_entity(
    app: &ArchiveApplication,
    arguments: &Value,
) -> Value {
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
    let evidence = match parse_evidence_array(arguments) {
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
