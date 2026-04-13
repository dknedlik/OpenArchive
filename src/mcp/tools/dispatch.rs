use serde_json::Value;

use crate::app::ArchiveApplication;

use super::handlers::{
    handle_get_artifact, handle_get_context_pack, handle_get_note_metadata, handle_get_related,
    handle_get_timeline, handle_link_objects, handle_list_artifacts, handle_list_review_items,
    handle_record_review_decision, handle_retry_artifact_enrichment, handle_search_archive,
    handle_search_objects, handle_store_entity, handle_store_memory, handle_update_object,
};
use super::result::tool_error;

pub(in crate::mcp) fn call_tool(app: &ArchiveApplication, params: Value) -> Value {
    let name = match params.get("name").and_then(Value::as_str) {
        Some(n) => n,
        None => return tool_error("invalid_params", "tools/call params.name is required"),
    };
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);

    match name {
        "search_archive" => handle_search_archive(app, &arguments),
        "get_artifact" => handle_get_artifact(app, &arguments),
        "get_context_pack" => handle_get_context_pack(app, &arguments),
        "get_note_metadata" => handle_get_note_metadata(app, &arguments),
        "search_objects" => handle_search_objects(app, &arguments),
        "store_memory" => handle_store_memory(app, &arguments),
        "link_objects" => handle_link_objects(app, &arguments),
        "list_artifacts" => handle_list_artifacts(app, &arguments),
        "update_object" => handle_update_object(app, &arguments),
        "store_entity" => handle_store_entity(app, &arguments),
        "get_related" => handle_get_related(app, &arguments),
        "get_timeline" => handle_get_timeline(app, &arguments),
        "list_review_items" => handle_list_review_items(app, &arguments),
        "record_review_decision" => handle_record_review_decision(app, &arguments),
        "retry_artifact_enrichment" => handle_retry_artifact_enrichment(app, &arguments),
        _ => tool_error("invalid_params", &format!("unknown tool: {name}")),
    }
}
