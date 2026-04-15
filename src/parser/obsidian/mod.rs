mod frontmatter;
mod links;
mod tags;
mod vault;

use std::collections::BTreeMap;

use crate::parser::document::ParsedDocument;
use crate::storage::{
    ImportedNoteLinkKind, ImportedNoteLinkResolutionStatus, ImportedNoteLinkTargetKind,
    ImportedNotePropertyValueKind, ImportedNoteTagSourceKind,
};

pub use frontmatter::{parse_frontmatter, ParsedFrontmatter};
pub use vault::{parse_vault_directory, parse_vault_zip, should_skip_path, VaultDirectoryFile};

/// Current manifest format version.
pub const OBSIDIAN_VAULT_MANIFEST_VERSION: u32 = 1;

/// Manifest for directory-based Obsidian vault storage.
/// Stores file metadata and content references instead of a synthetic zip.
/// Uses BTreeMap for deterministic serialization (same vault = same hash).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ObsidianVaultManifest {
    /// Version of the manifest format for future compatibility
    pub version: u32,
    /// Unix timestamp when the manifest was created (milliseconds since epoch)
    pub captured_at: i64,
    /// Map of relative file paths to file entries (BTreeMap for deterministic ordering)
    pub files: BTreeMap<String, VaultManifestFileEntry>,
}

/// Entry for a single file in the vault manifest.
/// Includes all StoredObject fields needed for retrieval.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VaultManifestFileEntry {
    /// Size of the file in bytes
    pub size: u64,
    /// Modification time as Unix timestamp (milliseconds since epoch)
    pub mtime_millis: Option<i64>,
    /// SHA-256 hash of file content for integrity verification
    pub content_hash: String,
    /// Object storage provider (e.g., "local_fs", "s3", "gcs")
    pub provider: String,
    /// Storage key/path for retrieving the object
    pub storage_key: String,
    /// MIME type of the stored content
    pub mime_type: String,
    /// Size in bytes as stored (may include overhead)
    pub stored_size_bytes: i64,
    /// SHA-256 of stored content (same as content_hash for files)
    pub stored_sha256: String,
}

pub fn normalize_tag(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_start_matches('#');
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed
        .split('/')
        .map(|part| part.trim().to_lowercase())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[derive(Debug)]
pub struct ParsedVault {
    pub notes: Vec<ParsedVaultNote>,
}

#[derive(Debug)]
pub struct ParsedVaultNote {
    pub note_path: String,
    pub title: Option<String>,
    pub document: ParsedDocument,
    pub properties: Vec<ParsedVaultProperty>,
    pub tags: Vec<ParsedVaultTag>,
    pub aliases: Vec<ParsedVaultAlias>,
    pub links: Vec<ParsedVaultLink>,
}

#[derive(Debug, Clone)]
pub struct ParsedVaultProperty {
    pub property_key: String,
    pub value_kind: ImportedNotePropertyValueKind,
    pub value_text: Option<String>,
    pub value_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ParsedVaultTag {
    pub raw_tag: String,
    pub normalized_tag: String,
    pub tag_path: String,
    pub source_kind: ImportedNoteTagSourceKind,
}

#[derive(Debug, Clone)]
pub struct ParsedVaultAlias {
    pub alias_text: String,
    pub normalized_alias: String,
}

#[derive(Debug, Clone)]
pub struct ParsedVaultLink {
    pub source_block_index: Option<usize>,
    pub link_kind: ImportedNoteLinkKind,
    pub target_kind: ImportedNoteLinkTargetKind,
    pub raw_target: String,
    pub normalized_target: Option<String>,
    pub display_text: Option<String>,
    pub target_path: Option<String>,
    pub target_heading: Option<String>,
    pub target_block: Option<String>,
    pub external_url: Option<String>,
    pub resolution_status: ImportedNoteLinkResolutionStatus,
    pub locator_json: Option<String>,
}
