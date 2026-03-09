/// Domain vocabulary enums and "New" structs for the slice-one write path.
///
/// These types are domain-shaped. They do not expose Oracle types or SQL concerns.
///
/// Nullability follows the live DDL in V001. Fields that are NOT NULL in the
/// schema are required (`String`, `i64`, etc.); nullable columns become
/// `Option<_>`. Source timestamps use `Option<SourceTimestamp>` so RFC3339
/// format and timezone are validated at parse time rather than at DB binding.
/// Server-assigned timestamps (created_at, captured_at, etc.) are omitted —
/// the DB sets them via DEFAULT SYSTIMESTAMP.

use chrono::{DateTime, SecondsFormat, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
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

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    ChatGptExport,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::ChatGptExport => "chatgpt_export",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    ChatGptExportZip,
    ChatGptExportJson,
    ChatGptExportCanonicalJson,
}

impl PayloadFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            PayloadFormat::ChatGptExportZip => "chatgpt_export_zip",
            PayloadFormat::ChatGptExportJson => "chatgpt_export_json",
            PayloadFormat::ChatGptExportCanonicalJson => "chatgpt_export_canonical_json",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportStatus {
    Pending,
    Parsing,
    Completed,
    CompletedWithErrors,
    Failed,
}

impl ImportStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ImportStatus::Pending => "pending",
            ImportStatus::Parsing => "parsing",
            ImportStatus::Completed => "completed",
            ImportStatus::CompletedWithErrors => "completed_with_errors",
            ImportStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactClass {
    Conversation,
}

impl ArtifactClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactClass::Conversation => "conversation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStatus {
    Captured,
    Normalized,
    Failed,
}

impl ArtifactStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactStatus::Captured => "captured",
            ArtifactStatus::Normalized => "normalized",
            ArtifactStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentStatus {
    Pending,
    Running,
    Completed,
    Partial,
    Failed,
}

impl EnrichmentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EnrichmentStatus::Pending => "pending",
            EnrichmentStatus::Running => "running",
            EnrichmentStatus::Completed => "completed",
            EnrichmentStatus::Partial => "partial",
            EnrichmentStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentType {
    Message,
    MessageWindow,
}

impl SegmentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SegmentType::Message => "message",
            SegmentType::MessageWindow => "message_window",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    ConversationEnrichment,
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobType::ConversationEnrichment => "conversation_enrichment",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Partial,
    Failed,
    Retryable,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Partial => "partial",
            JobStatus::Failed => "failed",
            JobStatus::Retryable => "retryable",
        }
    }
}

// ---------------------------------------------------------------------------
// "New" structs — data needed to create one row, minus server-set fields
// ---------------------------------------------------------------------------

/// Data required to create one oa_import_payload row.
/// All fields match the NOT NULL DDL columns; server-set created_at is omitted.
#[derive(Debug)]
pub struct NewImportPayload {
    pub payload_id: String,
    pub payload_format: PayloadFormat,
    pub payload_mime_type: String,
    pub payload_bytes: Vec<u8>,
    pub payload_size_bytes: i64,
    /// SHA-256 hex digest. Unique constraint enforces payload-level deduplication.
    pub payload_sha256: String,
}

/// Data required to create one oa_import row.
#[derive(Debug)]
pub struct NewImport {
    pub import_id: String,
    pub source_type: SourceType,
    pub import_status: ImportStatus,
    /// FK to oa_import_payload. Must be inserted before the import row.
    pub payload_id: String,
    /// Original filename from the upload, nullable in DDL.
    pub source_filename: Option<String>,
    /// SHA-256 hex digest of the raw source bytes. NOT NULL in DDL.
    pub source_content_hash: String,
    /// Best-effort conversation count at intake time. DDL DEFAULT 0 NOT NULL.
    pub conversation_count_detected: i64,
}

/// Data required to create one oa_artifact row.
#[derive(Debug)]
pub struct NewArtifact {
    pub artifact_id: String,
    /// FK to oa_import
    pub import_id: String,
    pub artifact_class: ArtifactClass,
    pub source_type: SourceType,
    pub artifact_status: ArtifactStatus,
    pub enrichment_status: EnrichmentStatus,
    /// Source-native conversation identifier, nullable in DDL.
    pub source_conversation_key: Option<String>,
    /// Application-computed per-conversation hash. NOT NULL + unique constraint.
    pub source_conversation_hash: String,
    pub title: Option<String>,
    /// Timestamp from the source export, nullable in DDL.
    pub created_at_source: Option<SourceTimestamp>,
    /// Nullable in DDL.
    pub started_at: Option<SourceTimestamp>,
    /// Nullable in DDL. Must be >= started_at when both are present (DDL CHECK).
    pub ended_at: Option<SourceTimestamp>,
    pub primary_language: Option<String>,
    /// Identifies the hash algorithm and normalization basis. NOT NULL.
    pub content_hash_version: String,
    /// JSON array, e.g. `["messages","text","participants","timestamps"]`. NOT NULL in DDL.
    pub content_facets_json: String,
    pub normalization_version: String,
}

/// Data required to create one oa_conversation_participant row.
#[derive(Debug)]
pub struct NewParticipant {
    pub participant_id: String,
    /// FK to oa_artifact
    pub artifact_id: String,
    pub participant_role: ParticipantRole,
    pub display_name: Option<String>,
    pub provider_name: Option<String>,
    pub model_name: Option<String>,
    pub source_participant_key: Option<String>,
    /// Zero-based ordering within the artifact. Unique per (artifact_id, sequence_no).
    pub sequence_no: i64,
}

/// Data required to create one oa_segment row.
///
/// For slice-one `message` segments `text_content` and `text_content_hash` are
/// always provided. The DDL allows them to be NULL for non-message types but
/// enforces `text_content IS NOT NULL` for `message` via CHECK constraint.
#[derive(Debug)]
pub struct NewSegment {
    pub segment_id: String,
    /// FK to oa_artifact
    pub artifact_id: String,
    /// FK to oa_conversation_participant, nullable in DDL.
    pub participant_id: Option<String>,
    pub segment_type: SegmentType,
    pub source_segment_key: Option<String>,
    /// Self-FK, nullable in DDL.
    pub parent_segment_id: Option<String>,
    /// Zero-based ordering. Unique per (artifact_id, sequence_no, segment_type).
    pub sequence_no: i64,
    /// Timestamp from the source export, nullable in DDL.
    pub created_at_source: Option<SourceTimestamp>,
    pub text_content: String,
    pub text_content_hash: String,
    /// JSON locator, nullable in DDL; must be valid JSON when present (DDL CHECK).
    pub locator_json: Option<String>,
    pub visibility_status: VisibilityStatus,
    /// JSON blob recording skipped non-text content. Must be valid JSON when present.
    pub unsupported_content_json: Option<String>,
}

/// Data required to create one oa_enrichment_job row.
#[derive(Debug)]
pub struct NewEnrichmentJob {
    pub job_id: String,
    /// FK to oa_artifact
    pub artifact_id: String,
    pub job_type: JobType,
    pub job_status: JobStatus,
    /// DDL DEFAULT 3. Must be > 0 (DDL CHECK).
    pub max_attempts: i32,
    /// DDL DEFAULT 100. Must be >= 0 (DDL CHECK).
    pub priority_no: i32,
    /// Self-contained JSON payload for future out-of-process execution. NOT NULL in DDL.
    pub payload_json: String,
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
