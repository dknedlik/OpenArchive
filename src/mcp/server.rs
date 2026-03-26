use std::io::{self, Write};
use std::sync::Arc;

use log::{error, info, warn};
use serde_json::{json, Value};

use crate::app::ArchiveApplication;

use super::protocol::{
    jsonrpc_error, jsonrpc_result, read_jsonrpc_message, write_jsonrpc_message, JsonRpcRequest,
};
use super::tools::{call_tool, tool_definitions};

const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

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
