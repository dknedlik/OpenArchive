//! Gemini Takeout activity parser.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::domain::{ParticipantRole, VisibilityStatus};
use crate::error::{ParserError, ParserResult};

use super::{ParsedConversation, ParsedMessage, ParsedParticipant, SkippedContent};

pub const GEMINI_NORMALIZATION_VERSION: &str = "gemini-v1";
pub const GEMINI_CONTENT_HASH_VERSION: &str = "gemini-v1";
pub const GEMINI_CONTENT_FACETS: &[&str] =
    &["activity_entry", "html_text", "participants", "timestamps"];

pub fn parse_conversations(input: &[u8]) -> ParserResult<Vec<ParsedConversation>> {
    let raw: Vec<RawActivityItem> =
        serde_json::from_slice(input).map_err(|e| ParserError::InvalidJson {
            detail: e.to_string(),
        })?;

    if raw.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    let conversations = raw
        .iter()
        .enumerate()
        .filter_map(|(index, item)| normalize_item(index, item))
        .collect::<Vec<_>>();

    if conversations.is_empty() {
        return Err(ParserError::EmptyExport);
    }

    Ok(conversations)
}

#[derive(Deserialize)]
struct RawActivityItem {
    title: Option<String>,
    time: Option<String>,
    #[serde(default, rename = "safeHtmlItem")]
    safe_html_item: Vec<SafeHtmlItem>,
    #[serde(default, rename = "attachedFiles")]
    attached_files: Vec<serde_json::Value>,
    #[serde(rename = "imageFile")]
    image_file: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct SafeHtmlItem {
    html: Option<String>,
}

fn normalize_item(index: usize, raw: &RawActivityItem) -> Option<ParsedConversation> {
    let assistant_text = extract_html_text(raw);
    if assistant_text.trim().is_empty() {
        return None;
    }

    let source_id = format!("gemini-activity-{}", index + 1);
    let create_time = parse_datetime(raw.time.as_deref());
    let update_time = create_time;
    let title = non_empty(raw.title.as_deref());
    let prompt = title.as_deref().and_then(extract_prompt_from_title);

    let mut participants_by_key = HashMap::new();
    let mut participant_order = Vec::new();
    let mut messages = Vec::new();

    if let Some(prompt_text) = prompt {
        let participant_key = "user:Gemini User".to_string();
        participant_order.push(participant_key.clone());
        participants_by_key.insert(
            participant_key.clone(),
            ParsedParticipant {
                source_key: participant_key.clone(),
                role: ParticipantRole::User,
                display_name: Some("Gemini User".to_string()),
                model_name: None,
                sequence_no: 0,
            },
        );
        messages.push(ParsedMessage {
            source_node_id: format!("{source_id}-prompt"),
            participant_key,
            create_time,
            text_content: prompt_text.to_string(),
            visibility: VisibilityStatus::Visible,
            unsupported_content: Vec::new(),
        });
    }

    let assistant_key = "assistant:Gemini".to_string();
    if !participants_by_key.contains_key(&assistant_key) {
        let sequence_no = participant_order.len() as i64;
        participant_order.push(assistant_key.clone());
        participants_by_key.insert(
            assistant_key.clone(),
            ParsedParticipant {
                source_key: assistant_key.clone(),
                role: ParticipantRole::Assistant,
                display_name: Some("Gemini".to_string()),
                model_name: Some("gemini".to_string()),
                sequence_no,
            },
        );
    }

    let mut unsupported = Vec::new();
    if !raw.attached_files.is_empty() {
        unsupported.push(SkippedContent {
            content_type: "attached_files".to_string(),
            part_index: unsupported.len(),
            summary: format!(
                "{} attached file reference(s) omitted",
                raw.attached_files.len()
            ),
        });
    }
    if raw.image_file.is_some() {
        unsupported.push(SkippedContent {
            content_type: "image_file".to_string(),
            part_index: unsupported.len(),
            summary: "image file reference omitted".to_string(),
        });
    }

    messages.push(ParsedMessage {
        source_node_id: format!("{source_id}-response"),
        participant_key: assistant_key,
        create_time,
        text_content: assistant_text,
        visibility: VisibilityStatus::Visible,
        unsupported_content: unsupported,
    });

    let participants = participant_order
        .into_iter()
        .filter_map(|key| participants_by_key.remove(&key))
        .collect::<Vec<_>>();

    Some(ParsedConversation {
        source_id,
        title,
        create_time,
        update_time,
        default_model_slug: Some("gemini".to_string()),
        content_hash: compute_content_hash(&messages),
        messages,
        participants,
        orphaned_skipped_content: Vec::new(),
    })
}

fn extract_html_text(raw: &RawActivityItem) -> String {
    raw.safe_html_item
        .iter()
        .filter_map(|item| item.html.as_deref())
        .map(html_to_text)
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn html_to_text(input: &str) -> String {
    let mut text = String::new();
    let mut idx = 0usize;
    while idx < input.len() {
        let rest = &input[idx..];
        if rest.starts_with("<br") {
            if let Some(end) = rest.find('>') {
                text.push('\n');
                idx += end + 1;
                continue;
            }
        }
        if rest.starts_with("<li") {
            if let Some(end) = rest.find('>') {
                text.push_str("- ");
                idx += end + 1;
                continue;
            }
        }
        if rest.starts_with("</p")
            || rest.starts_with("</div")
            || rest.starts_with("</li")
            || rest.starts_with("</tr")
            || rest.starts_with("</h1")
            || rest.starts_with("</h2")
            || rest.starts_with("</h3")
            || rest.starts_with("</h4")
            || rest.starts_with("</h5")
            || rest.starts_with("</h6")
            || rest.starts_with("</blockquote")
            || rest.starts_with("<hr")
        {
            if let Some(end) = rest.find('>') {
                text.push('\n');
                idx += end + 1;
                continue;
            }
        }
        if rest.starts_with('<') {
            if let Some(end) = rest.find('>') {
                idx += end + 1;
                continue;
            }
        }
        if rest.starts_with('&') {
            if let Some((decoded, consumed)) = decode_entity(rest) {
                text.push_str(decoded);
                idx += consumed;
                continue;
            }
        }
        if let Some(ch) = rest.chars().next() {
            text.push(ch);
            idx += ch.len_utf8();
        } else {
            break;
        }
    }

    collapse_text(&text)
}

fn decode_entity(input: &str) -> Option<(&'static str, usize)> {
    let mappings = [
        ("&quot;", "\""),
        ("&#39;", "'"),
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&nbsp;", " "),
    ];
    for (entity, decoded) in mappings {
        if input.starts_with(entity) {
            return Some((decoded, entity.len()));
        }
    }
    None
}

fn collapse_text(text: &str) -> String {
    let mut lines = Vec::new();
    for line in text.lines() {
        let compact = line.split_whitespace().collect::<Vec<_>>().join(" ");
        if !compact.is_empty() {
            lines.push(compact);
        }
    }
    lines.join("\n")
}

fn extract_prompt_from_title(title: &str) -> Option<&str> {
    title.strip_prefix("Prompted ").map(str::trim)
}

fn parse_datetime(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gemini_fixture_and_converts_html_to_text() {
        let payload = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_export.json"
        ));
        let conversations = parse_conversations(payload).unwrap();
        assert_eq!(conversations.len(), 1);
        let conversation = &conversations[0];
        assert_eq!(conversation.messages.len(), 2);
        assert!(conversation.messages[1]
            .text_content
            .contains("The \"best\" sweetener for tea"));
        assert!(conversation.messages[1].text_content.contains("- Splenda:"));
    }

    #[test]
    fn skips_items_with_no_usable_text() {
        let payload = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_empty_export.json"
        ));
        let err = parse_conversations(payload).unwrap_err();
        assert!(matches!(err, ParserError::EmptyExport));
    }
}
