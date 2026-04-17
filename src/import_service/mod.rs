mod build;
mod chatgpt;
mod conversation;
mod dispatch;
mod document;
mod ids;
mod obsidian;

#[cfg(test)]
mod tests;

use serde::Serialize;

use crate::error::OpenArchiveError;
use crate::object_store::ObjectStore;
use crate::storage::{
    ArtifactClass, ImportWriteResult, ImportWriteStore, PayloadFormat, SourceType,
};

pub use chatgpt::looks_like_chatgpt_zip;
pub use dispatch::import_payload;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ImportArtifactStatus {
    pub artifact_id: String,
    pub enrichment_status: String,
    pub ingest_result: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ImportResponse {
    pub import_id: String,
    pub import_status: String,
    pub artifacts: Vec<ImportArtifactStatus>,
}

#[derive(Debug, Clone, Copy)]
struct SourceImportSpec {
    source_type: SourceType,
    payload_format: PayloadFormat,
    artifact_class: ArtifactClass,
    payload_mime_type: &'static str,
    normalization_version: &'static str,
    content_hash_version: &'static str,
    content_facets: &'static [&'static str],
}

fn to_import_response(result: ImportWriteResult) -> ImportResponse {
    ImportResponse {
        import_id: result.import_id,
        import_status: result.import_status.as_str().to_string(),
        artifacts: result
            .artifacts
            .into_iter()
            .map(|artifact| ImportArtifactStatus {
                artifact_id: artifact.artifact_id,
                enrichment_status: artifact.enrichment_status.as_str().to_string(),
                ingest_result: artifact.ingest_result.as_str().to_string(),
            })
            .collect(),
    }
}

pub fn import_chatgpt_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    let shape = chatgpt::sniff_chatgpt_payload(payload_bytes)?;
    let format = match &shape {
        chatgpt::ChatGptPayloadShape::RawJson { .. } => PayloadFormat::ChatGptExportJson,
        chatgpt::ChatGptPayloadShape::Zip { .. } => PayloadFormat::ChatGptExportZip,
    };
    let (payload_blob, parse_input): (&[u8], &[u8]) = match &shape {
        chatgpt::ChatGptPayloadShape::RawJson { bytes } => (*bytes, *bytes),
        chatgpt::ChatGptPayloadShape::Zip {
            zip_bytes,
            conversations_json,
        } => (*zip_bytes, conversations_json.as_slice()),
    };

    dispatch::import_payload_with_parse_bytes(
        store,
        object_store,
        SourceType::ChatGptExport,
        format,
        payload_blob,
        parse_input,
    )
}

pub fn import_claude_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::ClaudeExport,
        PayloadFormat::ClaudeExportJson,
        payload_bytes,
    )
}

pub fn import_grok_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::GrokExport,
        PayloadFormat::GrokExportJson,
        payload_bytes,
    )
}

pub fn import_gemini_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    import_payload(
        store,
        object_store,
        SourceType::GeminiTakeout,
        PayloadFormat::GeminiTakeoutJson,
        payload_bytes,
    )
}

pub fn import_text_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    dispatch::import_document_payload(
        store,
        object_store,
        SourceType::TextFile,
        PayloadFormat::TextPlain,
        crate::parser::document::DocumentFormat::PlainText,
        payload_bytes,
    )
}

pub fn import_markdown_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    dispatch::import_document_payload(
        store,
        object_store,
        SourceType::MarkdownFile,
        PayloadFormat::MarkdownText,
        crate::parser::document::DocumentFormat::Markdown,
        payload_bytes,
    )
}

pub fn import_obsidian_vault_payload<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    obsidian::import_obsidian_vault_payload_impl(store, object_store, payload_bytes)
}

pub fn import_obsidian_vault_directory<S, O>(
    store: &S,
    object_store: &O,
    directory_path: &std::path::Path,
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    obsidian::import_obsidian_vault_directory_impl(store, object_store, directory_path)
}
