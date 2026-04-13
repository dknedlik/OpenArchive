use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::random;
use serde_json::Value;

use super::result::tool_error;

pub(in crate::mcp::tools) fn parse_optional_enum<T>(
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

pub(in crate::mcp::tools) fn parse_required_enum<T>(
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

pub(in crate::mcp::tools) fn parse_enum_array<T>(
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

pub(in crate::mcp::tools) fn new_id(prefix: &str) -> String {
    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let entropy = random::<u64>();
    format!("{prefix}-{nanos:x}-{counter:x}-{entropy:x}")
}
