use std::io::{self, BufRead, Write};
use std::sync::Arc;

use serde_json::{json, Value};

use crate::app::{artifact_detail, context_pack, search, ArchiveApplication};

const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const SEARCH_LIMIT_CAP: usize = 50;

pub fn run_stdio_server(app: Arc<ArchiveApplication>) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut writer = io::BufWriter::new(stdout.lock());

    eprintln!("[open-archive-mcp] stdio loop started");

    loop {
        let request = match read_jsonrpc_message(&mut reader) {
            Ok(Some(request)) => request,
            Ok(None) => {
                eprintln!("[open-archive-mcp] stdin closed");
                break;
            }
            Err(err) => {
                eprintln!("[open-archive-mcp] failed to read request: {err}");
                return Err(err);
            }
        };

        eprintln!(
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
                eprintln!("[open-archive-mcp] failed to write response id={response_id}: {err}");
                return Err(err);
            }
            if let Err(err) = writer.flush() {
                eprintln!("[open-archive-mcp] failed to flush response id={response_id}: {err}");
                return Err(err);
            }
            eprintln!("[open-archive-mcp] responded id={response_id}");
        }
    }

    Ok(())
}

fn handle_request(app: &ArchiveApplication, request: JsonRpcRequest) -> Option<Value> {
    if request.id.is_none() {
        eprintln!(
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
        "tools/call" => match call_tool(app, request.params.unwrap_or(Value::Null)) {
            Ok(result) => jsonrpc_result(id, result),
            Err(error) => jsonrpc_error(id, -32602, error),
        },
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
                    "limit": { "type": "integer", "minimum": 1, "maximum": SEARCH_LIMIT_CAP, "description": "Optional result limit; values above 50 are silently clamped." }
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
    ]
}

fn call_tool(app: &ArchiveApplication, params: Value) -> std::result::Result<Value, String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| "tools/call params.name is required".to_string())?;
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);

    match name {
        "search_archive" => {
            let service = app.search.as_ref().ok_or_else(|| {
                "search_archive is unavailable for the configured provider".to_string()
            })?;
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .ok_or_else(|| "search_archive requires a string query".to_string())?;
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(10)
                .clamp(1, SEARCH_LIMIT_CAP);
            let response = service
                .search(search::ArchiveSearchRequest {
                    query_text: query.to_string(),
                    limit,
                })
                .map_err(|err| err.to_string())?;
            Ok(tool_success(
                serde_json::to_value(response).expect("search response serializable"),
            ))
        }
        "get_artifact" => {
            let service = app.artifact_detail.as_ref().ok_or_else(|| {
                "get_artifact is unavailable for the configured provider".to_string()
            })?;
            let artifact_id = arguments
                .get("artifact_id")
                .and_then(Value::as_str)
                .ok_or_else(|| "get_artifact requires artifact_id".to_string())?;
            let include_segments = arguments
                .get("include_segments")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let segment_offset = arguments
                .get("segment_offset")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(0);
            let segment_limit = arguments
                .get("segment_limit")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(artifact_detail::DEFAULT_SEGMENT_LIMIT)
                .clamp(1, artifact_detail::DEFAULT_SEGMENT_LIMIT);
            let response = service
                .get(artifact_detail::ArtifactDetailRequest {
                    artifact_id: artifact_id.to_string(),
                    include_segments,
                    segment_offset,
                    segment_limit,
                })
                .map_err(|err| err.to_string())?;
            Ok(tool_success(match response {
                Some(response) => json!({ "found": true, "artifact": response }),
                None => json!({ "found": false }),
            }))
        }
        "get_context_pack" => {
            let service = app.context_pack.as_ref().ok_or_else(|| {
                "get_context_pack is unavailable for the configured provider".to_string()
            })?;
            let artifact_id = arguments
                .get("artifact_id")
                .and_then(Value::as_str)
                .ok_or_else(|| "get_context_pack requires artifact_id".to_string())?;
            let response = service
                .assemble(context_pack::ContextPackRequest {
                    artifact_id: artifact_id.to_string(),
                })
                .map_err(|err| err.to_string())?;
            Ok(tool_success(match response {
                Some(response) => json!({ "found": true, "context_pack": response }),
                None => json!({ "found": false }),
            }))
        }
        _ => Err(format!("unknown tool: {name}")),
    }
}

fn tool_success(value: Value) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": serde_json::to_string_pretty(&value).expect("tool payload serializable")
            }
        ],
        "structuredContent": value,
        "isError": false
    })
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
    use crate::app::search::ArchiveSearchService;

    fn test_app() -> ArchiveApplication {
        let search = Some(ArchiveSearchService::new(Arc::new(MockSearchReadStore)));
        let artifact_detail = Some(ArtifactDetailService::new(Arc::new(
            MockArtifactDetailStore,
        )));
        let context_pack = Some(ContextPackService::new(Arc::new(MockContextPackStore)));
        ArchiveApplication {
            artifacts: crate::app::artifacts::ArtifactQueryService::new(Arc::new(
                MockArtifactReadStore,
            )),
            imports: crate::app::imports::ImportApplicationService::new(
                Arc::new(MockImportWriteStore),
                Arc::new(MockObjectStore),
            ),
            search,
            artifact_detail,
            context_pack,
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
        )
        .expect("tool should succeed");

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
        )
        .expect("tool should succeed");

        assert_eq!(response["isError"], Value::Bool(false));
        assert!(response["structuredContent"]["hits"].is_array());
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

    struct MockSearchReadStore;
    impl crate::storage::ArchiveSearchReadStore for MockSearchReadStore {
        fn search_candidates(
            &self,
            _query_text: &str,
            _limit: usize,
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
                derived_objects: vec![],
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

    struct MockArtifactReadStore;
    impl crate::storage::ArtifactReadStore for MockArtifactReadStore {
        fn list_artifacts(
            &self,
        ) -> crate::error::StorageResult<Vec<crate::storage::ArtifactListItem>> {
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
}
