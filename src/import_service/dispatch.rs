use crate::error::OpenArchiveError;
use crate::object_store::ObjectStore;
use crate::parser::{self, document::DocumentFormat, ParsedConversation};
use crate::storage::{ImportWriteStore, PayloadFormat, SourceType};

use super::build::{
    build_document_import_set, build_import_set, cleanup_unreferenced_payload_object,
};
use super::{to_import_response, ImportResponse, SourceImportSpec};

pub fn import_payload<S, O>(
    store: &S,
    object_store: &O,
    source_type: SourceType,
    payload_format: PayloadFormat,
    payload_blob: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload_with_parse_bytes(
        store,
        object_store,
        source_type,
        payload_format,
        payload_blob,
        payload_blob,
    )
}

pub(super) fn import_payload_with_parse_bytes<S, O>(
    store: &S,
    object_store: &O,
    source_type: SourceType,
    payload_format: PayloadFormat,
    payload_blob: &[u8],
    parse_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    let spec = source_import_spec(source_type, payload_format)?;
    let parsed = parse_conversations_for_source(source_type, parse_bytes)?;
    let prepared = build_import_set(payload_blob, spec, parsed, object_store)?;
    let result = match store.write_import(prepared.write_set) {
        Ok(result) => result,
        Err(err) => {
            cleanup_unreferenced_payload_object(object_store, &prepared.payload_put, &err);
            return Err(err.into());
        }
    };

    Ok(to_import_response(result))
}

pub(super) fn import_document_payload<S, O>(
    store: &S,
    object_store: &O,
    source_type: SourceType,
    payload_format: PayloadFormat,
    document_format: DocumentFormat,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    let spec = source_import_spec(source_type, payload_format)?;
    let parsed = parser::document::parse_document(payload_bytes, document_format)?;
    let prepared = build_document_import_set(payload_bytes, spec, parsed, object_store)?;
    let result = match store.write_import(prepared.write_set) {
        Ok(result) => result,
        Err(err) => {
            cleanup_unreferenced_payload_object(object_store, &prepared.payload_put, &err);
            return Err(err.into());
        }
    };

    Ok(to_import_response(result))
}

pub(super) fn parse_conversations_for_source(
    source_type: SourceType,
    payload_bytes: &[u8],
) -> Result<Vec<ParsedConversation>, OpenArchiveError> {
    let parsed = match source_type {
        SourceType::ChatGptExport => parser::chatgpt::parse_conversations(payload_bytes),
        SourceType::ClaudeExport => parser::claude::parse_conversations(payload_bytes),
        SourceType::GrokExport => parser::grok::parse_conversations(payload_bytes),
        SourceType::GeminiTakeout => parser::gemini::parse_conversations(payload_bytes),
        SourceType::TextFile | SourceType::MarkdownFile | SourceType::ObsidianVault => {
            return Err(OpenArchiveError::Invariant(format!(
                "conversation parser requested for non-conversation source {}",
                source_type.as_str()
            )));
        }
    };

    parsed.map_err(OpenArchiveError::from)
}

pub(super) fn source_import_spec(
    source_type: SourceType,
    payload_format: PayloadFormat,
) -> Result<SourceImportSpec, OpenArchiveError> {
    let spec = match source_type {
        SourceType::ChatGptExport => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Conversation,
            payload_mime_type: match payload_format {
                PayloadFormat::ChatGptExportZip => "application/zip",
                PayloadFormat::ChatGptExportJson | PayloadFormat::ChatGptExportCanonicalJson => {
                    "application/json"
                }
                other => {
                    return Err(OpenArchiveError::Invariant(format!(
                        "unexpected payload_format for ChatGptExport: {other:?}"
                    )));
                }
            },
            normalization_version: parser::CHATGPT_NORMALIZATION_VERSION,
            content_hash_version: parser::CHATGPT_CONTENT_HASH_VERSION,
            content_facets: parser::CHATGPT_CONTENT_FACETS,
        },
        SourceType::ClaudeExport => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Conversation,
            payload_mime_type: "application/json",
            normalization_version: parser::claude::CLAUDE_NORMALIZATION_VERSION,
            content_hash_version: parser::claude::CLAUDE_CONTENT_HASH_VERSION,
            content_facets: parser::claude::CLAUDE_CONTENT_FACETS,
        },
        SourceType::GrokExport => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Conversation,
            payload_mime_type: "application/json",
            normalization_version: parser::grok::GROK_NORMALIZATION_VERSION,
            content_hash_version: parser::grok::GROK_CONTENT_HASH_VERSION,
            content_facets: parser::grok::GROK_CONTENT_FACETS,
        },
        SourceType::GeminiTakeout => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Conversation,
            payload_mime_type: "application/json",
            normalization_version: parser::gemini::GEMINI_NORMALIZATION_VERSION,
            content_hash_version: parser::gemini::GEMINI_CONTENT_HASH_VERSION,
            content_facets: parser::gemini::GEMINI_CONTENT_FACETS,
        },
        SourceType::TextFile => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Document,
            payload_mime_type: "text/plain",
            normalization_version: parser::document::TEXT_NORMALIZATION_VERSION,
            content_hash_version: parser::document::TEXT_CONTENT_HASH_VERSION,
            content_facets: parser::document::TEXT_CONTENT_FACETS,
        },
        SourceType::MarkdownFile => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Document,
            payload_mime_type: "text/markdown",
            normalization_version: parser::document::MARKDOWN_NORMALIZATION_VERSION,
            content_hash_version: parser::document::MARKDOWN_CONTENT_HASH_VERSION,
            content_facets: parser::document::MARKDOWN_CONTENT_FACETS,
        },
        SourceType::ObsidianVault => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: crate::storage::ArtifactClass::Document,
            payload_mime_type: "application/zip",
            normalization_version: "obsidian-v1",
            content_hash_version: parser::document::MARKDOWN_CONTENT_HASH_VERSION,
            content_facets: &[
                "blocks",
                "text",
                "headings",
                "lists",
                "code",
                "properties",
                "tags",
                "aliases",
                "links",
                "embeds",
            ],
        },
    };
    Ok(spec)
}
