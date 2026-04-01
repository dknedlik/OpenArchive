mod frontmatter;
mod links;
mod tags;
mod vault;

use crate::parser::document::ParsedDocument;
use crate::storage::{
    ImportedNoteLinkKind, ImportedNoteLinkResolutionStatus, ImportedNoteLinkTargetKind,
    ImportedNotePropertyValueKind, ImportedNoteTagSourceKind,
};

pub use vault::parse_vault_zip;
pub use frontmatter::{parse_frontmatter, ParsedFrontmatter};

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
