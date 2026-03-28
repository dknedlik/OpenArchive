use std::io::{self, BufRead, Write};

use serde_json::{json, Value};

pub(in crate::mcp) fn read_jsonrpc_message(
    reader: &mut impl BufRead,
) -> io::Result<Option<JsonRpcRequest>> {
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

    let request: JsonRpcRequest = serde_json::from_slice(&payload).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid newline-delimited JSON-RPC payload: {err}"),
        )
    })?;
    if request.jsonrpc.as_deref() != Some("2.0") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "newline-delimited MCP input must declare jsonrpc version 2.0",
        ));
    }
    Ok(Some(request))
}

pub(in crate::mcp) fn write_jsonrpc_message(
    writer: &mut impl Write,
    value: &Value,
) -> io::Result<()> {
    let payload =
        serde_json::to_vec(value).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    writer.write_all(&payload)?;
    writer.write_all(b"\n")
}

pub(in crate::mcp) fn jsonrpc_result(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

pub(in crate::mcp) fn jsonrpc_error(id: Value, code: i32, message: String) -> Value {
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
pub(super) struct JsonRpcRequest {
    pub(super) jsonrpc: Option<String>,
    pub(super) id: Option<Value>,
    pub(super) method: String,
    pub(super) params: Option<Value>,
}
