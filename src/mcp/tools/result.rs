use serde_json::{json, Value};

pub(in crate::mcp::tools) fn tool_error(code: &str, message: &str) -> Value {
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

pub(in crate::mcp) fn tool_success(value: Value) -> Value {
    let summary = summarize_tool_result(&value);
    let text = render_tool_result_text(&value, &summary);
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

fn render_tool_result_text(value: &Value, summary: &str) -> String {
    if should_include_payload_text(value) {
        match serde_json::to_string_pretty(value) {
            Ok(payload) => format!("{summary}\n{payload}"),
            Err(_) => summary.to_string(),
        }
    } else {
        summary.to_string()
    }
}

fn should_include_payload_text(value: &Value) -> bool {
    [
        "items",
        "hits",
        "results",
        "artifacts",
        "entries",
        "related",
        "artifact",
        "context_pack",
        "note",
        "found",
    ]
    .iter()
    .any(|key| value.get(key).is_some())
}

pub(in crate::mcp::tools) fn tool_storage_error(err: &crate::error::StorageError) -> Value {
    match err {
        crate::error::StorageError::UnsupportedOperation { .. } => {
            tool_error("service_unavailable", &err.to_string())
        }
        _ => tool_error("internal_error", &err.to_string()),
    }
}

pub(in crate::mcp::tools) fn tool_app_error(err: &crate::error::OpenArchiveError) -> Value {
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

    if let Some(note) = value.get("note").and_then(Value::as_object) {
        let metadata = note
            .get("imported_note_metadata")
            .and_then(Value::as_object);
        let properties = metadata
            .and_then(|value| value.get("properties"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let tags = metadata
            .and_then(|value| value.get("tags"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let aliases = metadata
            .and_then(|value| value.get("aliases"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let outbound_links = metadata
            .and_then(|value| value.get("outbound_links"))
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let inbound_links = note
            .get("inbound_note_links")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let artifact_links = note
            .get("artifact_links")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        return format!(
            "note: {} properties, {} tags, {} aliases, {} outbound links, {} inbound links, {} artifact links",
            properties, tags, aliases, outbound_links, inbound_links, artifact_links
        );
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
