//! Shared domain vocabulary for slice one.
//!
//! These types are used across parser and storage layers and should not be
//! anchored under `storage/` even though some are persisted directly.

use chrono::{DateTime, SecondsFormat, Utc};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SourceTimestamp(String);

impl SourceTimestamp {
    pub fn parse_rfc3339(input: &str) -> Result<Self, chrono::ParseError> {
        let parsed = DateTime::parse_from_rfc3339(input)?;
        Ok(Self::from(parsed.with_timezone(&Utc)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<DateTime<Utc>> for SourceTimestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value.to_rfc3339_opts(SecondsFormat::Nanos, false))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantRole {
    User,
    Assistant,
    System,
    Tool,
    Unknown,
}

impl ParticipantRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            ParticipantRole::User => "user",
            ParticipantRole::Assistant => "assistant",
            ParticipantRole::System => "system",
            ParticipantRole::Tool => "tool",
            ParticipantRole::Unknown => "unknown",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            "system" => Some(Self::System),
            "tool" => Some(Self::Tool),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisibilityStatus {
    Visible,
    Hidden,
    SkippedUnsupported,
}

impl VisibilityStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            VisibilityStatus::Visible => "visible",
            VisibilityStatus::Hidden => "hidden",
            VisibilityStatus::SkippedUnsupported => "skipped_unsupported",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "visible" => Some(Self::Visible),
            "hidden" => Some(Self::Hidden),
            "skipped_unsupported" => Some(Self::SkippedUnsupported),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SourceTimestamp;

    #[test]
    fn source_timestamp_normalizes_to_utc_rfc3339() {
        let ts = SourceTimestamp::parse_rfc3339("2026-03-08T22:27:12.3055-05:00").unwrap();
        assert_eq!(ts.as_str(), "2026-03-09T03:27:12.305500000+00:00");
    }
}
