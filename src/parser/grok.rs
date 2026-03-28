//! Grok export parser.

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::domain::{ParticipantRole, VisibilityStatus};
use crate::error::{ParserError, ParserResult};

use super::{ParsedConversation, ParsedMessage, ParsedParticipant, SkippedContent};

pub const GROK_NORMALIZATION_VERSION: &str = "grok-v1";
pub const GROK_CONTENT_HASH_VERSION: &str = "grok-v1";
pub const GROK_CONTENT_FACETS: &[&str] = &[
    "messages",
    "text",
    "participants",
    "timestamps",
    "markup_stripped",
];

pub fn parse_conversations(input: &[u8]) -> ParserResult<Vec<ParsedConversation>> {
    let raw: RawExport = serde_json::from_slice(input).map_err(|e| ParserError::InvalidJson {
        detail: e.to_string(),
    })?;

    if raw.conversations.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    let conversations = raw
        .conversations
        .iter()
        .filter_map(normalize_conversation)
        .collect::<Vec<_>>();

    if conversations.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    Ok(conversations)
}

#[derive(Deserialize)]
struct RawExport {
    #[serde(default)]
    conversations: Vec<RawConversationEnvelope>,
}

#[derive(Deserialize)]
struct RawConversationEnvelope {
    conversation: RawConversation,
    #[serde(default)]
    responses: Vec<RawResponseEnvelope>,
}

#[derive(Deserialize)]
struct RawConversation {
    id: String,
    title: Option<String>,
    summary: Option<String>,
    create_time: Option<String>,
    modify_time: Option<String>,
}

#[derive(Deserialize)]
struct RawResponseEnvelope {
    response: RawResponse,
}

#[derive(Deserialize)]
struct RawResponse {
    #[serde(rename = "_id")]
    id: String,
    message: Option<String>,
    sender: Option<String>,
    create_time: Option<serde_json::Value>,
    model: Option<String>,
    #[serde(default)]
    web_search_results: Vec<serde_json::Value>,
}

fn normalize_conversation(raw: &RawConversationEnvelope) -> Option<ParsedConversation> {
    let mut participants_by_key = HashMap::new();
    let mut participant_order = Vec::new();
    let mut messages = Vec::new();

    for response in &raw.responses {
        let text = normalize_message_text(response.response.message.as_deref().unwrap_or(""));
        if text.text.trim().is_empty() {
            continue;
        }

        let role = map_sender(response.response.sender.as_deref().unwrap_or("unknown"));
        let participant_key = participant_key(role, response.response.model.as_deref());
        if !participants_by_key.contains_key(&participant_key) {
            let sequence_no = participant_order.len() as i64;
            participant_order.push(participant_key.clone());
            participants_by_key.insert(
                participant_key.clone(),
                ParsedParticipant {
                    source_key: participant_key.clone(),
                    role,
                    display_name: participant_display_name(role),
                    model_name: response.response.model.clone(),
                    sequence_no,
                },
            );
        }

        let mut unsupported = text.skipped;
        if !response.response.web_search_results.is_empty() {
            unsupported.push(SkippedContent {
                content_type: "web_search_results".to_string(),
                part_index: unsupported.len(),
                summary: format!(
                    "{} web search result(s) omitted",
                    response.response.web_search_results.len()
                ),
            });
        }

        messages.push(ParsedMessage {
            source_node_id: response.response.id.clone(),
            participant_key,
            create_time: parse_grok_datetime(response.response.create_time.as_ref()),
            text_content: text.text,
            visibility: if role == ParticipantRole::System {
                VisibilityStatus::Hidden
            } else {
                VisibilityStatus::Visible
            },
            unsupported_content: unsupported,
        });
    }

    if messages.is_empty() {
        return None;
    }

    let participants = participant_order
        .into_iter()
        .filter_map(|key| participants_by_key.remove(&key))
        .collect::<Vec<_>>();

    Some(ParsedConversation {
        source_id: raw.conversation.id.clone(),
        title: non_empty(raw.conversation.title.as_deref())
            .or_else(|| non_empty(raw.conversation.summary.as_deref())),
        create_time: parse_iso_datetime(raw.conversation.create_time.as_deref()),
        update_time: parse_iso_datetime(raw.conversation.modify_time.as_deref()),
        default_model_slug: participants
            .iter()
            .find(|participant| participant.role == ParticipantRole::Assistant)
            .and_then(|participant| participant.model_name.clone()),
        content_hash: compute_content_hash(&messages),
        messages,
        participants,
        orphaned_skipped_content: Vec::new(),
    })
}

struct NormalizedText {
    text: String,
    skipped: Vec<SkippedContent>,
}

fn normalize_message_text(input: &str) -> NormalizedText {
    let mut output = String::with_capacity(input.len());
    let mut skipped = Vec::new();
    let mut idx = 0usize;
    let bytes = input.as_bytes();

    while idx < bytes.len() {
        if bytes[idx] == b'<' {
            if let Some(rel_end) = input[idx..].find('>') {
                let end = idx + rel_end;
                let tag = &input[idx + 1..end];
                if tag.starts_with("grok:render")
                    || tag.starts_with("/grok:render")
                    || tag.starts_with("xai:")
                    || tag.starts_with("/xai:")
                {
                    skipped.push(SkippedContent {
                        content_type: "inline_markup".to_string(),
                        part_index: skipped.len(),
                        summary: summarize_text(tag),
                    });
                    idx = end + 1;
                    continue;
                }
            }
        }

        if let Some(ch) = input[idx..].chars().next() {
            output.push(ch);
            idx += ch.len_utf8();
        } else {
            break;
        }
    }

    NormalizedText {
        text: output.trim().to_string(),
        skipped,
    }
}

fn participant_key(role: ParticipantRole, model_name: Option<&str>) -> String {
    match model_name {
        Some(model) if !model.trim().is_empty() => format!("{}:{model}", role.as_str()),
        _ => format!("{}:", role.as_str()),
    }
}

fn participant_display_name(role: ParticipantRole) -> Option<String> {
    match role {
        ParticipantRole::User => Some("Grok User".to_string()),
        ParticipantRole::Assistant => Some("Grok".to_string()),
        ParticipantRole::System => Some("System".to_string()),
        ParticipantRole::Tool => Some("Tool".to_string()),
        ParticipantRole::Unknown => None,
    }
}

fn map_sender(sender: &str) -> ParticipantRole {
    match sender.to_ascii_lowercase().as_str() {
        "human" | "user" => ParticipantRole::User,
        "assistant" => ParticipantRole::Assistant,
        "system" => ParticipantRole::System,
        "tool" => ParticipantRole::Tool,
        _ => ParticipantRole::Unknown,
    }
}

fn parse_iso_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn parse_grok_datetime(value: Option<&serde_json::Value>) -> Option<DateTime<Utc>> {
    let raw = value?;
    if let Some(as_str) = raw.as_str() {
        return parse_iso_datetime(Some(as_str));
    }

    let millis = raw
        .get("$date")
        .and_then(|date| date.get("$numberLong"))
        .and_then(|n| n.as_str())
        .and_then(|n| n.parse::<i64>().ok())?;

    let seconds = millis.div_euclid(1_000);
    let nanos = (millis.rem_euclid(1_000) as u32) * 1_000_000;
    Utc.timestamp_opt(seconds, nanos).single()
}

fn compute_content_hash(messages: &[ParsedMessage]) -> String {
    let mut hasher = Sha256::new();
    for (index, message) in messages.iter().enumerate() {
        let timestamp = message
            .create_time
            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false))
            .unwrap_or_default();
        let role = message
            .participant_key
            .split_once(':')
            .map(|(role, _)| role)
            .unwrap_or("unknown");
        hasher.update(format!("{index}:{role}:{timestamp}:{}\n", message.text_content).as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn summarize_text(text: &str) -> String {
    const MAX_LEN: usize = 80;
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= MAX_LEN {
        compact
    } else {
        format!("{}...", &compact[..MAX_LEN])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_grok_fixture_and_strips_inline_markup() {
        let payload = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/grok_export.json"
        ));
        let conversations = parse_conversations(payload).unwrap();
        assert_eq!(conversations.len(), 1);
        let conversation = &conversations[0];
        assert_eq!(conversation.messages.len(), 2);
        assert!(conversation.messages[1]
            .text_content
            .contains("both Anthropic and xAI"));
        assert!(conversation.messages[1]
            .unsupported_content
            .iter()
            .any(|item| item.content_type == "inline_markup"));
        assert!(conversation.messages[1]
            .unsupported_content
            .iter()
            .any(|item| item.content_type == "web_search_results"));
    }

    #[test]
    fn skips_empty_conversations_in_export() {
        let payload = br#"{
          "conversations": [
            {
              "conversation": {
                "id": "empty-conv",
                "create_time": "2026-03-15T19:19:26.203416Z",
                "modify_time": "2026-03-15T19:19:47.011477Z",
                "title": "Empty",
                "summary": ""
              },
              "responses": []
            },
            {
              "conversation": {
                "id": "real-conv",
                "create_time": "2026-03-15T19:19:26.203416Z",
                "modify_time": "2026-03-15T19:19:47.011477Z",
                "title": "Real",
                "summary": ""
              },
              "responses": [
                {
                  "response": {
                    "_id": "msg-1",
                    "message": "hello",
                    "sender": "human",
                    "create_time": {"$date":{"$numberLong":"1773602366234"}},
                    "model": "grok-4"
                  }
                }
              ]
            }
          ]
        }"#;

        let conversations = parse_conversations(payload).unwrap();
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].source_id, "real-conv");
    }
}
