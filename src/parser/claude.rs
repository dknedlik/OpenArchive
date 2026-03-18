//! Claude export parser.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::domain::{ParticipantRole, VisibilityStatus};
use crate::error::{ParserError, ParserResult};

use super::{ParsedConversation, ParsedMessage, ParsedParticipant, SkippedContent};

pub const CLAUDE_NORMALIZATION_VERSION: &str = "claude-v1";
pub const CLAUDE_CONTENT_HASH_VERSION: &str = "claude-v1";
pub const CLAUDE_CONTENT_FACETS: &[&str] = &[
    "messages",
    "text",
    "participants",
    "timestamps",
    "unsupported_content",
];

pub fn parse_conversations(input: &[u8]) -> ParserResult<Vec<ParsedConversation>> {
    let raw: Vec<RawConversation> =
        serde_json::from_slice(input).map_err(|e| ParserError::InvalidJson {
            detail: e.to_string(),
        })?;

    if raw.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    let conversations = raw
        .iter()
        .filter_map(normalize_conversation)
        .collect::<Vec<_>>();

    if conversations.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    Ok(conversations)
}

#[derive(Deserialize)]
struct RawConversation {
    uuid: String,
    name: Option<String>,
    summary: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    #[serde(default)]
    chat_messages: Vec<Option<RawMessage>>,
}

#[derive(Deserialize)]
struct RawMessage {
    uuid: String,
    text: Option<String>,
    #[serde(default)]
    content: Vec<RawContentPart>,
    sender: Option<String>,
    created_at: Option<String>,
    #[serde(default)]
    attachments: Vec<serde_json::Value>,
    #[serde(default)]
    files: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawContentPart {
    #[serde(rename = "type")]
    part_type: String,
    text: Option<String>,
    thinking: Option<String>,
}

fn normalize_conversation(raw: &RawConversation) -> Option<ParsedConversation> {
    let mut messages = Vec::new();
    let mut orphaned_skipped = Vec::new();
    let mut participants_by_key = HashMap::new();
    let mut participant_order = Vec::new();

    for maybe_message in &raw.chat_messages {
        let Some(message) = maybe_message else {
            continue;
        };

        let extracted = extract_message_content(message);
        if extracted.text.trim().is_empty() {
            orphaned_skipped.extend(extracted.skipped);
            continue;
        }

        let role = map_sender(message.sender.as_deref().unwrap_or("unknown"));
        let display_name = participant_display_name(role);
        let participant_key = format!(
            "{}:{}",
            role.as_str(),
            display_name.as_deref().unwrap_or_default()
        );

        if !participants_by_key.contains_key(&participant_key) {
            let sequence_no = participant_order.len() as i64;
            participant_order.push(participant_key.clone());
            participants_by_key.insert(
                participant_key.clone(),
                ParsedParticipant {
                    source_key: participant_key.clone(),
                    role,
                    display_name,
                    model_name: None,
                    sequence_no,
                },
            );
        }

        messages.push(ParsedMessage {
            source_node_id: message.uuid.clone(),
            participant_key,
            create_time: parse_datetime(message.created_at.as_deref()),
            text_content: extracted.text,
            visibility: if role == ParticipantRole::System {
                VisibilityStatus::Hidden
            } else {
                VisibilityStatus::Visible
            },
            unsupported_content: extracted.skipped,
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
        source_id: raw.uuid.clone(),
        title: non_empty(raw.name.as_deref())
            .map(str::to_string)
            .or_else(|| non_empty(raw.summary.as_deref()).map(str::to_string)),
        create_time: parse_datetime(raw.created_at.as_deref()),
        update_time: parse_datetime(raw.updated_at.as_deref()),
        default_model_slug: None,
        content_hash: compute_content_hash(&messages),
        messages,
        participants,
        orphaned_skipped_content: orphaned_skipped,
    })
}

struct ExtractedMessageContent {
    text: String,
    skipped: Vec<SkippedContent>,
}

fn extract_message_content(message: &RawMessage) -> ExtractedMessageContent {
    let mut text_parts = Vec::new();
    let mut skipped = Vec::new();

    for (index, part) in message.content.iter().enumerate() {
        match part.part_type.as_str() {
            "text" => {
                if let Some(text) = non_empty(part.text.as_deref()) {
                    text_parts.push(text.to_string());
                }
            }
            "thinking" => skipped.push(SkippedContent {
                content_type: "thinking".to_string(),
                part_index: index,
                summary: "thinking block omitted from normalized text".to_string(),
            }),
            other => skipped.push(SkippedContent {
                content_type: other.to_string(),
                part_index: index,
                summary: format!("unsupported Claude content type: {other}"),
            }),
        }

        if part.part_type == "thinking" {
            if let Some(thinking) = non_empty(part.thinking.as_deref()) {
                skipped.push(SkippedContent {
                    content_type: "thinking_text".to_string(),
                    part_index: index,
                    summary: summarize_text(thinking),
                });
            }
        }
    }

    if text_parts.is_empty() {
        if let Some(text) = non_empty(message.text.as_deref()) {
            text_parts.push(text.to_string());
        }
    }

    if !message.attachments.is_empty() {
        skipped.push(SkippedContent {
            content_type: "attachments".to_string(),
            part_index: skipped.len(),
            summary: format!(
                "{} attachment reference(s) omitted",
                message.attachments.len()
            ),
        });
    }

    if !message.files.is_empty() {
        skipped.push(SkippedContent {
            content_type: "files".to_string(),
            part_index: skipped.len(),
            summary: format!("{} file reference(s) omitted", message.files.len()),
        });
    }

    ExtractedMessageContent {
        text: text_parts.join("\n\n"),
        skipped,
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

fn participant_display_name(role: ParticipantRole) -> Option<String> {
    match role {
        ParticipantRole::User => Some("Claude User".to_string()),
        ParticipantRole::Assistant => Some("Claude".to_string()),
        ParticipantRole::System => Some("System".to_string()),
        ParticipantRole::Tool => Some("Tool".to_string()),
        ParticipantRole::Unknown => None,
    }
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

fn parse_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
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
    fn parses_claude_fixture_with_thinking_and_files() {
        let payload = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/claude_export.json"
        ));
        let conversations = parse_conversations(payload).unwrap();
        assert_eq!(conversations.len(), 1);
        let conversation = &conversations[0];
        assert_eq!(conversation.source_id, "claude-conv-1");
        assert_eq!(conversation.messages.len(), 2);
        assert_eq!(conversation.participants.len(), 2);
        assert_eq!(conversation.messages[1].unsupported_content.len(), 3);
        assert!(conversation.messages[1]
            .unsupported_content
            .iter()
            .any(|item| item.content_type == "thinking"));
        assert!(conversation.messages[1]
            .unsupported_content
            .iter()
            .any(|item| item.content_type == "files"));
    }

    #[test]
    fn skips_empty_conversations_in_export() {
        let payload = br#"[
          {
            "uuid": "empty-conv",
            "name": "Empty",
            "summary": "",
            "created_at": "2026-01-17T14:23:32.807041Z",
            "updated_at": "2026-01-17T14:24:52.521684Z",
            "chat_messages": []
          },
          {
            "uuid": "real-conv",
            "name": "Real",
            "summary": "",
            "created_at": "2026-01-17T14:23:32.807041Z",
            "updated_at": "2026-01-17T14:24:52.521684Z",
            "chat_messages": [
              {
                "uuid": "msg-1",
                "text": "hello",
                "content": [
                  {
                    "type": "text",
                    "text": "hello"
                  }
                ],
                "sender": "human",
                "created_at": "2026-01-17T14:23:33.472056Z",
                "updated_at": "2026-01-17T14:23:33.472056Z",
                "attachments": [],
                "files": []
              }
            ]
          }
        ]"#;

        let conversations = parse_conversations(payload).unwrap();
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].source_id, "real-conv");
    }
}
