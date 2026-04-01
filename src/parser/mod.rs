//! Normalized parser output types and constants for the slice-one import pipeline.
//!
//! These types are source-format-agnostic: they carry enough information to
//! populate [`WriteArtifactSet`] fields without additional interpretation, but
//! they do not reference Oracle SQL or storage implementation details.

pub mod chatgpt;
pub mod claude;
pub mod document;
pub mod gemini;
pub mod grok;
pub mod obsidian;

use chrono::{DateTime, Utc};

use crate::domain::{ParticipantRole, VisibilityStatus};

/// Normalization version for the ChatGPT text-only parser (slice one).
pub const CHATGPT_NORMALIZATION_VERSION: &str = "chatgpt-v1";

/// Content hash version, aligned with the normalization version.
pub const CHATGPT_CONTENT_HASH_VERSION: &str = "chatgpt-v1";

/// Content facets captured by the chatgpt-v1 normalization. These describe what
/// is extracted and stored, not what is included in the content hash. The hash
/// basis is defined by [`CHATGPT_CONTENT_HASH_VERSION`] (text + ordering only).
pub const CHATGPT_CONTENT_FACETS: &[&str] = &["messages", "text", "participants", "timestamps"];

/// A fully normalized conversation ready for artifact assembly.
#[derive(Debug)]
pub struct ParsedConversation {
    /// Source-native conversation ID (e.g. ChatGPT UUID).
    pub source_id: String,
    pub title: Option<String>,
    pub create_time: Option<DateTime<Utc>>,
    pub update_time: Option<DateTime<Utc>>,
    pub default_model_slug: Option<String>,
    /// Ordered message segments on the active path.
    pub messages: Vec<ParsedMessage>,
    /// Deduplicated participant list, in discovery order.
    pub participants: Vec<ParsedParticipant>,
    /// SHA-256 hex digest of the text-only content on the active path.
    /// Computed using [`CHATGPT_CONTENT_HASH_VERSION`] rules.
    pub content_hash: String,
    /// Skipped content from unsupported-only nodes that had no text-bearing
    /// neighbor to attach to. Empty in the common case.
    pub orphaned_skipped_content: Vec<SkippedContent>,
}

/// A single text-bearing message segment on the active conversation path.
#[derive(Debug)]
pub struct ParsedMessage {
    /// Source node ID from the conversation mapping.
    pub source_node_id: String,
    /// Dedup key linking this message to a [`ParsedParticipant`].
    pub participant_key: String,
    pub create_time: Option<DateTime<Utc>>,
    pub text_content: String,
    pub visibility: VisibilityStatus,
    /// Skipped non-text content from this node or from neighboring
    /// unsupported-only nodes whose content was forward-attached here.
    pub unsupported_content: Vec<SkippedContent>,
}

/// A unique participant within a single conversation.
#[derive(Debug)]
pub struct ParsedParticipant {
    /// Dedup key: `"{role}:{author_name_or_empty}"`.
    pub source_key: String,
    pub role: ParticipantRole,
    pub display_name: Option<String>,
    pub model_name: Option<String>,
    /// Zero-based discovery order within the conversation.
    pub sequence_no: i64,
}

/// Metadata about a piece of skipped non-text content.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SkippedContent {
    pub content_type: String,
    pub part_index: usize,
    pub summary: String,
}
