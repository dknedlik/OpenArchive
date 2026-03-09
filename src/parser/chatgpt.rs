//! ChatGPT `conversations.json` parser.
//!
//! Parses the export format into [`ParsedConversation`] structs by:
//! 1. Deserializing the JSON array into intermediate structs preserving the tree.
//! 2. Flattening each conversation tree into the active path.
//! 3. Extracting text content and recording skipped non-text parts.
//! 4. Deduplicating participants per conversation.
//! 5. Computing a deterministic content hash.
//!
//! ## Branch flattening rule
//!
//! The active path is the sequence of nodes from root to leaf that represents
//! what the user last saw in the ChatGPT UI.
//!
//! 1. When `current_node` is present, walk parent pointers back to root and
//!    reverse.
//! 2. When `current_node` is absent, walk forward from root choosing the
//!    preferred child at each branch point:
//!    - Latest `create_time` among children's messages.
//!    - Last position in the source `children` array (most recently added).
//!    - Greatest node ID by lexicographic order (deterministic tiebreaker).

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::domain::{ParticipantRole, VisibilityStatus};
use crate::error::{ParserError, ParserResult};

use super::{ParsedConversation, ParsedMessage, ParsedParticipant, SkippedContent};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse a `conversations.json` byte slice into normalized conversations.
pub fn parse_conversations(input: &[u8]) -> ParserResult<Vec<ParsedConversation>> {
    let raw: Vec<RawConversation> =
        serde_json::from_slice(input).map_err(|e| ParserError::InvalidJson {
            detail: e.to_string(),
        })?;

    if raw.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    let mut conversations = Vec::with_capacity(raw.len());
    for raw_conv in &raw {
        conversations.push(normalize_conversation(raw_conv)?);
    }
    Ok(conversations)
}

// ---------------------------------------------------------------------------
// Deserialization types (private)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RawConversation {
    id: String,
    title: Option<String>,
    create_time: Option<f64>,
    update_time: Option<f64>,
    mapping: HashMap<String, RawMappingNode>,
    current_node: Option<String>,
    default_model_slug: Option<String>,
}

#[derive(Deserialize)]
struct RawMappingNode {
    id: String,
    message: Option<RawMessage>,
    parent: Option<String>,
    #[serde(default)]
    children: Vec<String>,
}

#[derive(Deserialize)]
struct RawMessage {
    author: RawAuthor,
    create_time: Option<f64>,
    content: RawContent,
    #[serde(default)]
    metadata: serde_json::Value,
}

#[derive(Deserialize)]
struct RawAuthor {
    role: String,
    name: Option<String>,
}

#[derive(Deserialize)]
struct RawContent {
    content_type: String,
    #[serde(default)]
    parts: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

fn normalize_conversation(raw: &RawConversation) -> ParserResult<ParsedConversation> {
    let active_path = flatten_active_path(raw)?;

    // Extract content from each node on the active path.
    let mut extracted: Vec<(&RawMappingNode, ExtractedContent)> = Vec::new();
    for node in &active_path {
        if let Some(msg) = &node.message {
            extracted.push((node, extract_content(&msg.content)));
        }
    }

    // Build messages and participants, attaching skipped content as we go.
    let mut messages: Vec<ParsedMessage> = Vec::new();
    let mut pending_skipped: Vec<SkippedContent> = Vec::new();
    let mut orphaned_skipped: Vec<SkippedContent> = Vec::new();
    let mut participants_map: HashMap<String, ParsedParticipant> = HashMap::new();
    let mut participant_order: Vec<String> = Vec::new();

    for (node, content) in &extracted {
        let msg = node.message.as_ref().unwrap();
        let role = map_role(&msg.author.role);
        let author_name = msg.author.name.as_deref().unwrap_or("");
        let participant_key = format!("{}:{}", role.as_str(), author_name);

        // Register participant on first encounter.
        if !participants_map.contains_key(&participant_key) {
            let model_name = if role == ParticipantRole::Assistant {
                msg.metadata
                    .get("model_slug")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| raw.default_model_slug.clone())
            } else {
                None
            };

            let display_name = msg.author.name.clone();
            let seq = participant_order.len() as i64;
            participant_order.push(participant_key.clone());
            participants_map.insert(
                participant_key.clone(),
                ParsedParticipant {
                    source_key: participant_key.clone(),
                    role,
                    display_name,
                    model_name,
                    sequence_no: seq,
                },
            );
        }

        let has_text = !content.text.trim().is_empty();

        if has_text {
            let visibility = if role == ParticipantRole::System {
                VisibilityStatus::Hidden
            } else {
                VisibilityStatus::Visible
            };

            // Attach pending skipped content from previous unsupported-only nodes.
            let mut unsupported = std::mem::take(&mut pending_skipped);
            unsupported.extend(content.skipped.iter().cloned());

            messages.push(ParsedMessage {
                source_node_id: node.id.clone(),
                participant_key: participant_key.clone(),
                create_time: msg.create_time.and_then(epoch_to_datetime),
                text_content: content.text.clone(),
                visibility,
                unsupported_content: unsupported,
            });
        } else {
            // No text — queue skipped content for attachment to the next text segment.
            pending_skipped.extend(content.skipped.iter().cloned());
        }
    }

    // Remaining pending skipped → attach to last message or orphan.
    if !pending_skipped.is_empty() {
        if let Some(last_msg) = messages.last_mut() {
            last_msg.unsupported_content.extend(pending_skipped);
        } else {
            orphaned_skipped.extend(pending_skipped);
        }
    }

    let content_hash = compute_content_hash(&messages);

    let participants: Vec<ParsedParticipant> = participant_order
        .into_iter()
        .filter_map(|key| participants_map.remove(&key))
        .collect();

    Ok(ParsedConversation {
        source_id: raw.id.clone(),
        title: raw.title.clone(),
        create_time: raw.create_time.and_then(epoch_to_datetime),
        update_time: raw.update_time.and_then(epoch_to_datetime),
        default_model_slug: raw.default_model_slug.clone(),
        messages,
        participants,
        content_hash,
        orphaned_skipped_content: orphaned_skipped,
    })
}

// ---------------------------------------------------------------------------
// Tree flattening
// ---------------------------------------------------------------------------

/// Flatten the conversation tree into the active path: the sequence of nodes
/// from root to the current leaf that represents what the user last saw.
fn flatten_active_path<'a>(conv: &'a RawConversation) -> ParserResult<Vec<&'a RawMappingNode>> {
    if conv.mapping.is_empty() {
        return Err(ParserError::EmptyConversation {
            conversation_id: conv.id.clone(),
        });
    }

    // Strategy 1: Walk back from current_node.
    if let Some(current_id) = &conv.current_node {
        if !conv.mapping.contains_key(current_id.as_str()) {
            return Err(ParserError::MissingCurrentNode {
                conversation_id: conv.id.clone(),
                node_id: current_id.clone(),
            });
        }

        return walk_from_current_node(conv, current_id);
    }

    // Strategy 2: Forward walk from root with branch selection.
    walk_from_root(conv)
}

fn walk_from_current_node<'a>(
    conv: &'a RawConversation,
    current_id: &str,
) -> ParserResult<Vec<&'a RawMappingNode>> {
    let mut path = Vec::new();
    let mut node_id = current_id;
    let max_depth = conv.mapping.len();

    loop {
        let node = conv
            .mapping
            .get(node_id)
            .ok_or_else(|| ParserError::BrokenParentLink {
                conversation_id: conv.id.clone(),
                node_id: node_id.to_string(),
            })?;

        path.push(node);

        if path.len() > max_depth {
            return Err(ParserError::CyclicTree {
                conversation_id: conv.id.clone(),
            });
        }

        match &node.parent {
            None => break,
            Some(parent_id) => node_id = parent_id,
        }
    }

    path.reverse();
    Ok(path)
}

fn walk_from_root<'a>(conv: &'a RawConversation) -> ParserResult<Vec<&'a RawMappingNode>> {
    let root = conv
        .mapping
        .values()
        .find(|n| n.parent.is_none())
        .ok_or_else(|| ParserError::NoRoot {
            conversation_id: conv.id.clone(),
        })?;

    let mut path = vec![root];
    let mut current = root;
    let max_depth = conv.mapping.len();

    while !current.children.is_empty() {
        let next = select_child(current, &conv.mapping, &conv.id)?;
        path.push(next);
        current = next;

        if path.len() > max_depth {
            return Err(ParserError::CyclicTree {
                conversation_id: conv.id.clone(),
            });
        }
    }

    Ok(path)
}

/// Select the preferred child at a branching point.
///
/// Ordering (descending preference):
/// 1. Latest `create_time` among children's messages.
/// 2. Last position in the source `children` array (most recently added).
/// 3. Greatest node ID by lexicographic order (deterministic tiebreaker).
fn select_child<'a>(
    node: &RawMappingNode,
    mapping: &'a HashMap<String, RawMappingNode>,
    conversation_id: &str,
) -> ParserResult<&'a RawMappingNode> {
    let mut best: Option<(usize, &RawMappingNode)> = None;

    for (idx, child_id) in node.children.iter().enumerate() {
        let child = mapping
            .get(child_id)
            .ok_or_else(|| ParserError::MissingChild {
                conversation_id: conversation_id.to_string(),
                parent_id: node.id.clone(),
                child_id: child_id.clone(),
            })?;

        best = Some(match best {
            None => (idx, child),
            Some((best_idx, best_node)) => {
                if child_is_preferred(child, idx, best_node, best_idx) {
                    (idx, child)
                } else {
                    (best_idx, best_node)
                }
            }
        });
    }

    best.map(|(_, n)| n)
        .ok_or_else(|| ParserError::EmptyConversation {
            conversation_id: conversation_id.to_string(),
        })
}

/// Returns true if `candidate` should be preferred over `current_best`.
fn child_is_preferred(
    candidate: &RawMappingNode,
    candidate_idx: usize,
    current_best: &RawMappingNode,
    best_idx: usize,
) -> bool {
    let time_a = candidate.message.as_ref().and_then(|m| m.create_time);
    let time_b = current_best.message.as_ref().and_then(|m| m.create_time);

    match (time_a, time_b) {
        (Some(a), Some(b)) if a > b => return true,
        (Some(a), Some(b)) if a < b => return false,
        (Some(_), None) => return true,
        (None, Some(_)) => return false,
        _ => {} // Equal or both None — fall through to tiebreakers.
    }

    // Tiebreaker 2: later position in children array.
    if candidate_idx != best_idx {
        return candidate_idx > best_idx;
    }

    // Tiebreaker 3: lexicographic node ID.
    candidate.id > current_best.id
}

// ---------------------------------------------------------------------------
// Content extraction
// ---------------------------------------------------------------------------

struct ExtractedContent {
    text: String,
    skipped: Vec<SkippedContent>,
}

fn extract_content(content: &RawContent) -> ExtractedContent {
    match content.content_type.as_str() {
        "text" | "code" => extract_text_parts(&content.parts),
        "multimodal_text" => extract_multimodal_parts(&content.parts),
        other => ExtractedContent {
            text: String::new(),
            skipped: vec![SkippedContent {
                content_type: other.to_string(),
                part_index: 0,
                summary: format!("unsupported content type: {other}"),
            }],
        },
    }
}

fn extract_text_parts(parts: &[serde_json::Value]) -> ExtractedContent {
    let mut text_parts = Vec::new();
    let mut skipped = Vec::new();

    for (i, part) in parts.iter().enumerate() {
        if let Some(s) = part.as_str() {
            text_parts.push(s);
        } else if !part.is_null() {
            // Non-string, non-null parts in a "text" node are anomalous. Record
            // them so they surface in unsupported_content_json rather than being
            // silently lost.
            skipped.push(SkippedContent {
                content_type: "unknown".to_string(),
                part_index: i,
                summary: format!("unexpected non-string part in text content at index {i}"),
            });
        }
    }

    ExtractedContent {
        text: text_parts.join(""),
        skipped,
    }
}

fn extract_multimodal_parts(parts: &[serde_json::Value]) -> ExtractedContent {
    let mut text_parts = Vec::new();
    let mut skipped = Vec::new();

    for (i, part) in parts.iter().enumerate() {
        if let Some(s) = part.as_str() {
            text_parts.push(s);
        } else {
            let ct = part
                .get("content_type")
                .and_then(|v| v.as_str())
                .or_else(|| part.get("asset_pointer").map(|_| "asset_pointer"))
                .unwrap_or("unknown");
            skipped.push(SkippedContent {
                content_type: ct.to_string(),
                part_index: i,
                summary: format!("non-text part in multimodal_text at index {i}"),
            });
        }
    }

    ExtractedContent {
        text: text_parts.join(""),
        skipped,
    }
}

// ---------------------------------------------------------------------------
// Participant role mapping
// ---------------------------------------------------------------------------

/// Central mapping table from ChatGPT author role strings to domain roles.
fn map_role(role: &str) -> ParticipantRole {
    match role {
        "user" => ParticipantRole::User,
        "assistant" => ParticipantRole::Assistant,
        "system" => ParticipantRole::System,
        "tool" => ParticipantRole::Tool,
        _ => ParticipantRole::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Content hashing
// ---------------------------------------------------------------------------

/// Compute a deterministic SHA-256 hash of the normalized content on the active path.
///
/// Hash basis: for each emitted message segment in sequence order, append
/// `"{sequence_no}:{role}:{timestamp}:{text_content}\n"`.
///
/// This covers the slice-one idempotency inputs:
/// - ordering
/// - participant role
/// - relevant message timestamps
/// - emitted text content
///
/// It intentionally excludes unsupported content metadata.
fn compute_content_hash(messages: &[ParsedMessage]) -> String {
    let mut hasher = Sha256::new();
    for (i, msg) in messages.iter().enumerate() {
        let timestamp = msg
            .create_time
            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false))
            .unwrap_or_default();
        let role = msg
            .participant_key
            .split_once(':')
            .map(|(role, _)| role)
            .unwrap_or("unknown");

        hasher.update(format!("{i}:{role}:{timestamp}:{}\n", msg.text_content).as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn epoch_to_datetime(epoch: f64) -> Option<DateTime<Utc>> {
    if !epoch.is_finite() {
        return None;
    }
    let secs = epoch.floor() as i64;
    let nsecs = ((epoch - epoch.floor()) * 1_000_000_000.0).round() as u32;
    Utc.timestamp_opt(secs, nsecs).single()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers ----------------------------------------------------------

    /// Convenience: parse a JSON string and return the result.
    fn parse(json: &str) -> ParserResult<Vec<ParsedConversation>> {
        parse_conversations(json.as_bytes())
    }

    // -- Fixtures ---------------------------------------------------------

    /// Single linear conversation: system → user → assistant.
    fn single_linear_json() -> &'static str {
        r#"[{
          "id": "conv-1",
          "title": "Linear Chat",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "current_node": "msg-3",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {
              "id": "root",
              "message": null,
              "parent": null,
              "children": ["msg-1"]
            },
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "system", "name": null},
                "create_time": 1700000000.0,
                "content": {"content_type": "text", "parts": ["You are a helpful assistant."]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["msg-2"]
            },
            "msg-2": {
              "id": "msg-2",
              "message": {
                "id": "msg-2",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["Hello!"]},
                "metadata": {}
              },
              "parent": "msg-1",
              "children": ["msg-3"]
            },
            "msg-3": {
              "id": "msg-3",
              "message": {
                "id": "msg-3",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Hi! How can I help?"]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-2",
              "children": []
            }
          },
          "moderation_results": []
        }]"#
    }

    /// Two conversations in one export.
    fn multi_conversation_json() -> &'static str {
        r#"[
          {
            "id": "conv-a",
            "title": "First",
            "create_time": 1700000000.0,
            "update_time": 1700000010.0,
            "current_node": "a-msg",
            "default_model_slug": "gpt-4o",
            "mapping": {
              "a-root": {"id": "a-root", "message": null, "parent": null, "children": ["a-msg"]},
              "a-msg": {
                "id": "a-msg",
                "message": {
                  "id": "a-msg",
                  "author": {"role": "user", "name": null},
                  "create_time": 1700000001.0,
                  "content": {"content_type": "text", "parts": ["Message A"]},
                  "metadata": {}
                },
                "parent": "a-root",
                "children": []
              }
            },
            "moderation_results": []
          },
          {
            "id": "conv-b",
            "title": "Second",
            "create_time": 1700000000.0,
            "update_time": 1700000020.0,
            "current_node": "b-msg",
            "default_model_slug": "gpt-4o",
            "mapping": {
              "b-root": {"id": "b-root", "message": null, "parent": null, "children": ["b-msg"]},
              "b-msg": {
                "id": "b-msg",
                "message": {
                  "id": "b-msg",
                  "author": {"role": "user", "name": null},
                  "create_time": 1700000002.0,
                  "content": {"content_type": "text", "parts": ["Message B"]},
                  "metadata": {}
                },
                "parent": "b-root",
                "children": []
              }
            },
            "moderation_results": []
          }
        ]"#
    }

    /// Conversation with a branch: user → {response-a, response-b}.
    /// `current_node` points to response-b.
    fn branching_with_current_node_json() -> &'static str {
        r#"[{
          "id": "conv-branch",
          "title": "Branch Test",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "current_node": "response-b",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["What is Rust?"]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["response-a", "response-b"]
            },
            "response-a": {
              "id": "response-a",
              "message": {
                "id": "response-a",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Rust is a systems language."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-1",
              "children": []
            },
            "response-b": {
              "id": "response-b",
              "message": {
                "id": "response-b",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000025.0,
                "content": {"content_type": "text", "parts": ["Rust is a memory-safe language."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-1",
              "children": []
            }
          },
          "moderation_results": []
        }]"#
    }

    /// Same branching structure but no current_node.
    /// response-b has a later timestamp so the heuristic should pick it.
    fn branching_no_current_node_json() -> &'static str {
        r#"[{
          "id": "conv-fallback",
          "title": "Fallback Test",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["What is Rust?"]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["response-a", "response-b"]
            },
            "response-a": {
              "id": "response-a",
              "message": {
                "id": "response-a",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Rust is a systems language."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-1",
              "children": []
            },
            "response-b": {
              "id": "response-b",
              "message": {
                "id": "response-b",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000025.0,
                "content": {"content_type": "text", "parts": ["Rust is a memory-safe language."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-1",
              "children": []
            }
          },
          "moderation_results": []
        }]"#
    }

    /// Tied timestamps, no current_node — should use child array position.
    /// response-b is second in children array, so it wins.
    fn branching_tied_timestamps_json() -> &'static str {
        r#"[{
          "id": "conv-tied",
          "title": "Tied Timestamps",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["Question"]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["response-a", "response-b"]
            },
            "response-a": {
              "id": "response-a",
              "message": {
                "id": "response-a",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Answer A"]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-1",
              "children": []
            },
            "response-b": {
              "id": "response-b",
              "message": {
                "id": "response-b",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Answer B"]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-1",
              "children": []
            }
          },
          "moderation_results": []
        }]"#
    }

    /// Conversation with unsupported content:
    /// user text → user multimodal (text+image) → tool execution_output → assistant text
    fn unsupported_content_json() -> &'static str {
        r#"[{
          "id": "conv-unsupported",
          "title": "Unsupported Content Test",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "current_node": "msg-4",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["Plain text message"]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["msg-2"]
            },
            "msg-2": {
              "id": "msg-2",
              "message": {
                "id": "msg-2",
                "author": {"role": "user", "name": null},
                "create_time": 1700000020.0,
                "content": {
                  "content_type": "multimodal_text",
                  "parts": [
                    "Look at this image: ",
                    {"asset_pointer": "file-service://file-abc123", "content_type": "image/png", "width": 800, "height": 600}
                  ]
                },
                "metadata": {}
              },
              "parent": "msg-1",
              "children": ["msg-3"]
            },
            "msg-3": {
              "id": "msg-3",
              "message": {
                "id": "msg-3",
                "author": {"role": "tool", "name": "python"},
                "create_time": 1700000030.0,
                "content": {"content_type": "execution_output", "parts": ["<result object>"]},
                "metadata": {}
              },
              "parent": "msg-2",
              "children": ["msg-4"]
            },
            "msg-4": {
              "id": "msg-4",
              "message": {
                "id": "msg-4",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000040.0,
                "content": {"content_type": "text", "parts": ["Here is the analysis."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-3",
              "children": []
            }
          },
          "moderation_results": []
        }]"#
    }

    /// Conversation with duplicate participants:
    /// system → user → assistant → tool:browser → assistant → tool:browser
    fn participant_dedup_json() -> &'static str {
        r#"[{
          "id": "conv-dedup",
          "title": "Participant Dedup",
          "create_time": 1700000000.0,
          "update_time": 1700000200.0,
          "current_node": "msg-6",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "system", "name": null},
                "create_time": 1700000000.0,
                "content": {"content_type": "text", "parts": ["System prompt."]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["msg-2"]
            },
            "msg-2": {
              "id": "msg-2",
              "message": {
                "id": "msg-2",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["Search for Rust docs."]},
                "metadata": {}
              },
              "parent": "msg-1",
              "children": ["msg-3"]
            },
            "msg-3": {
              "id": "msg-3",
              "message": {
                "id": "msg-3",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Let me search."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-2",
              "children": ["msg-4"]
            },
            "msg-4": {
              "id": "msg-4",
              "message": {
                "id": "msg-4",
                "author": {"role": "tool", "name": "browser"},
                "create_time": 1700000030.0,
                "content": {"content_type": "text", "parts": ["Search results: ..."]},
                "metadata": {}
              },
              "parent": "msg-3",
              "children": ["msg-5"]
            },
            "msg-5": {
              "id": "msg-5",
              "message": {
                "id": "msg-5",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000040.0,
                "content": {"content_type": "text", "parts": ["Here are the results."]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-4",
              "children": ["msg-6"]
            },
            "msg-6": {
              "id": "msg-6",
              "message": {
                "id": "msg-6",
                "author": {"role": "tool", "name": "browser"},
                "create_time": 1700000050.0,
                "content": {"content_type": "text", "parts": ["More search results."]},
                "metadata": {}
              },
              "parent": "msg-5",
              "children": []
            }
          },
          "moderation_results": []
        }]"#
    }

    // -- Tests ------------------------------------------------------------

    #[test]
    fn single_linear_conversation() {
        let convs = parse(single_linear_json()).unwrap();
        assert_eq!(convs.len(), 1);

        let c = &convs[0];
        assert_eq!(c.source_id, "conv-1");
        assert_eq!(c.title.as_deref(), Some("Linear Chat"));
        assert!(c.create_time.is_some());
        assert!(c.update_time.is_some());
        assert_eq!(c.default_model_slug.as_deref(), Some("gpt-4o"));

        // 3 messages: system (hidden), user, assistant.
        assert_eq!(c.messages.len(), 3);

        assert_eq!(c.messages[0].source_node_id, "msg-1");
        assert_eq!(c.messages[0].text_content, "You are a helpful assistant.");
        assert_eq!(c.messages[0].visibility, VisibilityStatus::Hidden);
        assert_eq!(c.messages[0].participant_key, "system:");

        assert_eq!(c.messages[1].source_node_id, "msg-2");
        assert_eq!(c.messages[1].text_content, "Hello!");
        assert_eq!(c.messages[1].visibility, VisibilityStatus::Visible);
        assert_eq!(c.messages[1].participant_key, "user:");

        assert_eq!(c.messages[2].source_node_id, "msg-3");
        assert_eq!(c.messages[2].text_content, "Hi! How can I help?");
        assert_eq!(c.messages[2].visibility, VisibilityStatus::Visible);
        assert_eq!(c.messages[2].participant_key, "assistant:");

        // 3 participants in discovery order.
        assert_eq!(c.participants.len(), 3);
        assert_eq!(c.participants[0].role, ParticipantRole::System);
        assert_eq!(c.participants[0].sequence_no, 0);
        assert_eq!(c.participants[1].role, ParticipantRole::User);
        assert_eq!(c.participants[1].sequence_no, 1);
        assert_eq!(c.participants[2].role, ParticipantRole::Assistant);
        assert_eq!(c.participants[2].sequence_no, 2);
        assert_eq!(c.participants[2].model_name.as_deref(), Some("gpt-4o"));

        // Content hash is non-empty and deterministic.
        assert!(!c.content_hash.is_empty());
        let second = parse(single_linear_json()).unwrap();
        assert_eq!(c.content_hash, second[0].content_hash);
    }

    #[test]
    fn multi_conversation_export() {
        let convs = parse(multi_conversation_json()).unwrap();
        assert_eq!(convs.len(), 2);
        assert_eq!(convs[0].source_id, "conv-a");
        assert_eq!(convs[1].source_id, "conv-b");
        assert_eq!(convs[0].messages[0].text_content, "Message A");
        assert_eq!(convs[1].messages[0].text_content, "Message B");
    }

    #[test]
    fn branch_selection_with_current_node() {
        let convs = parse(branching_with_current_node_json()).unwrap();
        let c = &convs[0];

        assert_eq!(c.messages.len(), 2);
        assert_eq!(c.messages[0].text_content, "What is Rust?");
        // current_node points to response-b.
        assert_eq!(
            c.messages[1].text_content,
            "Rust is a memory-safe language."
        );
        assert_eq!(c.messages[1].source_node_id, "response-b");
    }

    #[test]
    fn branch_selection_fallback_latest_timestamp() {
        let convs = parse(branching_no_current_node_json()).unwrap();
        let c = &convs[0];

        assert_eq!(c.messages.len(), 2);
        // response-b has later timestamp (1700000025 > 1700000020).
        assert_eq!(
            c.messages[1].text_content,
            "Rust is a memory-safe language."
        );
        assert_eq!(c.messages[1].source_node_id, "response-b");
    }

    #[test]
    fn branch_selection_fallback_child_order_on_tied_timestamps() {
        let convs = parse(branching_tied_timestamps_json()).unwrap();
        let c = &convs[0];

        assert_eq!(c.messages.len(), 2);
        // Tied timestamps → last child in array wins → response-b.
        assert_eq!(c.messages[1].text_content, "Answer B");
        assert_eq!(c.messages[1].source_node_id, "response-b");
    }

    #[test]
    fn unsupported_content_extraction() {
        let convs = parse(unsupported_content_json()).unwrap();
        let c = &convs[0];

        // 3 text-bearing segments: plain text, multimodal text, assistant text.
        // The execution_output node (msg-3) has no text → skipped.
        assert_eq!(c.messages.len(), 3);

        // msg-1: plain text, no skipped content.
        assert_eq!(c.messages[0].text_content, "Plain text message");
        assert!(c.messages[0].unsupported_content.is_empty());

        // msg-2: multimodal text with image → text extracted, image skipped.
        assert_eq!(c.messages[1].text_content, "Look at this image: ");
        assert_eq!(c.messages[1].unsupported_content.len(), 1);
        assert_eq!(
            c.messages[1].unsupported_content[0].content_type,
            "image/png"
        );
        assert_eq!(c.messages[1].unsupported_content[0].part_index, 1);

        // msg-4 (assistant text): the execution_output's skipped content is
        // forward-attached here.
        assert_eq!(c.messages[2].text_content, "Here is the analysis.");
        assert_eq!(c.messages[2].unsupported_content.len(), 1);
        assert_eq!(
            c.messages[2].unsupported_content[0].content_type,
            "execution_output"
        );
    }

    #[test]
    fn participant_role_mapping_and_dedup() {
        let convs = parse(participant_dedup_json()).unwrap();
        let c = &convs[0];

        // 6 messages from 4 unique participants.
        assert_eq!(c.messages.len(), 6);
        assert_eq!(c.participants.len(), 4);

        // Discovery order: system, user, assistant, tool:browser.
        assert_eq!(c.participants[0].role, ParticipantRole::System);
        assert_eq!(c.participants[0].source_key, "system:");
        assert!(c.participants[0].display_name.is_none());

        assert_eq!(c.participants[1].role, ParticipantRole::User);
        assert_eq!(c.participants[1].source_key, "user:");

        assert_eq!(c.participants[2].role, ParticipantRole::Assistant);
        assert_eq!(c.participants[2].source_key, "assistant:");
        assert_eq!(c.participants[2].model_name.as_deref(), Some("gpt-4o"));

        assert_eq!(c.participants[3].role, ParticipantRole::Tool);
        assert_eq!(c.participants[3].source_key, "tool:browser");
        assert_eq!(c.participants[3].display_name.as_deref(), Some("browser"));
        assert!(c.participants[3].model_name.is_none());

        // Verify dedup: both tool:browser messages reference the same participant.
        assert_eq!(c.messages[3].participant_key, "tool:browser");
        assert_eq!(c.messages[5].participant_key, "tool:browser");
    }

    #[test]
    fn content_hash_stable_across_unsupported_content_changes() {
        // Parse the linear conversation.
        let baseline = parse(single_linear_json()).unwrap();
        let baseline_hash = &baseline[0].content_hash;

        // Build a version with identical text but added unsupported content
        // on the user message.
        let modified = r#"[{
          "id": "conv-1",
          "title": "Linear Chat",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "current_node": "msg-3",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "system", "name": null},
                "create_time": 1700000000.0,
                "content": {"content_type": "text", "parts": ["You are a helpful assistant."]},
                "metadata": {}
              },
              "parent": "root",
              "children": ["msg-2"]
            },
            "msg-2": {
              "id": "msg-2",
              "message": {
                "id": "msg-2",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {
                  "content_type": "multimodal_text",
                  "parts": [
                    "Hello!",
                    {"asset_pointer": "file-service://extra-image", "content_type": "image/jpeg"}
                  ]
                },
                "metadata": {}
              },
              "parent": "msg-1",
              "children": ["msg-3"]
            },
            "msg-3": {
              "id": "msg-3",
              "message": {
                "id": "msg-3",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000020.0,
                "content": {"content_type": "text", "parts": ["Hi! How can I help?"]},
                "metadata": {"model_slug": "gpt-4o"}
              },
              "parent": "msg-2",
              "children": []
            }
          },
          "moderation_results": []
        }]"#;

        let modified_convs = parse(modified).unwrap();
        // Same text content → same hash, even though msg-2 now has an image part.
        assert_eq!(baseline_hash, &modified_convs[0].content_hash);
    }

    #[test]
    fn empty_export_is_error() {
        let result = parse("[]");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParserError::EmptyExport));
    }

    #[test]
    fn invalid_json_is_error() {
        let result = parse("not json");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ParserError::InvalidJson { .. }
        ));
    }

    #[test]
    fn empty_mapping_is_error() {
        let json = r#"[{
          "id": "conv-empty",
          "title": null,
          "create_time": null,
          "update_time": null,
          "current_node": null,
          "default_model_slug": null,
          "mapping": {},
          "moderation_results": []
        }]"#;
        let result = parse(json);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ParserError::EmptyConversation { .. }
        ));
    }

    #[test]
    fn missing_current_node_is_error() {
        let json = r#"[{
          "id": "conv-missing-current",
          "title": "Missing Current Node",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "current_node": "missing-node",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "user", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "text", "parts": ["Hello"]},
                "metadata": {}
              },
              "parent": "root",
              "children": []
            }
          },
          "moderation_results": []
        }]"#;

        let result = parse(json);
        assert!(matches!(
            result.unwrap_err(),
            ParserError::MissingCurrentNode { .. }
        ));
    }

    #[test]
    fn epoch_to_datetime_conversion() {
        let dt = epoch_to_datetime(1700000000.123456).unwrap();
        assert_eq!(dt.timestamp(), 1700000000);
        // Sub-second precision preserved.
        assert!(dt.timestamp_subsec_nanos() > 0);

        // Non-finite returns None.
        assert!(epoch_to_datetime(f64::NAN).is_none());
        assert!(epoch_to_datetime(f64::INFINITY).is_none());
    }

    #[test]
    fn orphaned_skipped_content_when_no_text_segments() {
        // All messages are unsupported → no text segments, skipped content orphaned.
        let json = r#"[{
          "id": "conv-all-unsupported",
          "title": "All Unsupported",
          "create_time": 1700000000.0,
          "update_time": 1700000100.0,
          "current_node": "msg-1",
          "default_model_slug": "gpt-4o",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["msg-1"]},
            "msg-1": {
              "id": "msg-1",
              "message": {
                "id": "msg-1",
                "author": {"role": "assistant", "name": null},
                "create_time": 1700000010.0,
                "content": {"content_type": "execution_output", "parts": ["<output>"]},
                "metadata": {}
              },
              "parent": "root",
              "children": []
            }
          },
          "moderation_results": []
        }]"#;

        let convs = parse(json).unwrap();
        let c = &convs[0];
        assert!(c.messages.is_empty());
        assert_eq!(c.orphaned_skipped_content.len(), 1);
        assert_eq!(
            c.orphaned_skipped_content[0].content_type,
            "execution_output"
        );
    }
}
