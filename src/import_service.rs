use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::domain::SourceTimestamp;
use crate::error::{OpenArchiveError, ParserError};
use crate::object_store::{NewObject, ObjectStore, PutObjectResult};
use crate::parser::{self, document::DocumentFormat, ParsedConversation, ParsedMessage};
use crate::storage::{
    ArtifactClass, ArtifactExtractPayload, ArtifactStatus, EnrichmentStatus, ImportStatus,
    ImportWriteStore, ImportedNoteMetadata, ImportedNoteMetadataWriteSet, JobStatus, JobType,
    NewArtifact, NewEnrichmentJob, NewImport, NewImportObjectRef, NewImportedNoteAlias,
    NewImportedNoteLink, NewImportedNoteProperty, NewImportedNoteTag, NewParticipant, NewSegment,
    PayloadFormat, SegmentType, SourceType, WriteArtifactSet, WriteImportSet,
};

/// Represents the detected shape of a ChatGPT payload.
#[derive(Debug)]
enum ChatGptPayloadShape<'a> {
    /// Raw JSON bytes (conversations.json content directly).
    RawJson { bytes: &'a [u8] },
    /// Zip archive containing conversations.json.
    Zip {
        zip_bytes: &'a [u8],
        conversations_json: Vec<u8>,
    },
}

/// Checks if bytes look like a ChatGPT export zip without extracting content.
///
/// Returns true if:
/// - Bytes start with zip local file header magic (PK\x03\x04)
/// - Archive contains an entry named conversations.json (root-level or nested)
///
/// This is a lightweight check for CLI auto-detection and routing.
pub fn looks_like_chatgpt_zip(bytes: &[u8]) -> bool {
    // Zip local file header magic: PK\x03\x04
    if bytes.len() < 4 || bytes[0..4] != *b"PK\x03\x04" {
        return false;
    }

    let reader = Cursor::new(bytes);
    let Ok(mut archive) = zip::ZipArchive::new(reader) else {
        return false;
    };

    for index in 0..archive.len() {
        let Ok(file) = archive.by_index(index) else {
            continue;
        };
        let name = file.name();
        // Root-level match
        if name.eq_ignore_ascii_case("conversations.json") {
            return true;
        }
        // Basename match anywhere in tree
        if std::path::Path::new(name)
            .file_name()
            .map(|n| n.eq_ignore_ascii_case("conversations.json"))
            .unwrap_or(false)
        {
            return true;
        }
    }

    false
}

/// Detects whether the payload is a zip archive or raw JSON.
///
/// For zip archives, extracts the conversations.json entry.
/// Returns an error if the zip is corrupt or missing conversations.json.
fn sniff_chatgpt_payload(bytes: &[u8]) -> Result<ChatGptPayloadShape<'_>, OpenArchiveError> {
    // Zip local file header magic: PK\x03\x04
    if bytes.len() < 4 || bytes[0..4] != *b"PK\x03\x04" {
        // Not a zip - treat as raw JSON
        return Ok(ChatGptPayloadShape::RawJson { bytes });
    }

    // It's a zip - now validate it and extract conversations.json
    let reader = Cursor::new(bytes);
    let mut archive =
        zip::ZipArchive::new(reader).map_err(|e| ParserError::UnsupportedPayload {
            detail: format!("corrupt zip archive: {e}"),
        })?;

    // First pass: prefer root-level match
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|e| ParserError::UnsupportedPayload {
                detail: format!("failed to read zip entry {index}: {e}"),
            })?;
        if file.name().eq_ignore_ascii_case("conversations.json") {
            let mut conversations_json = Vec::new();
            file.read_to_end(&mut conversations_json).map_err(|e| {
                ParserError::UnsupportedPayload {
                    detail: format!("failed to read conversations.json from zip: {e}"),
                }
            })?;
            return Ok(ChatGptPayloadShape::Zip {
                zip_bytes: bytes,
                conversations_json,
            });
        }
    }

    // Second pass: accept basename match anywhere in the tree
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|e| ParserError::UnsupportedPayload {
                detail: format!("failed to read zip entry {index}: {e}"),
            })?;
        let basename_matches = std::path::Path::new(file.name())
            .file_name()
            .map(|name| name.eq_ignore_ascii_case("conversations.json"))
            .unwrap_or(false);
        if basename_matches {
            let mut conversations_json = Vec::new();
            file.read_to_end(&mut conversations_json).map_err(|e| {
                ParserError::UnsupportedPayload {
                    detail: format!("failed to read conversations.json from zip: {e}"),
                }
            })?;
            return Ok(ChatGptPayloadShape::Zip {
                zip_bytes: bytes,
                conversations_json,
            });
        }
    }

    // Valid zip but no conversations.json
    Err(ParserError::UnsupportedPayload {
        detail: "chatgpt zip missing conversations.json".to_string(),
    }
    .into())
}

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

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

fn import_payload_with_parse_bytes<S, O>(
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

    Ok(ImportResponse {
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
    })
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
    let shape = sniff_chatgpt_payload(payload_bytes)?;
    let format = match &shape {
        ChatGptPayloadShape::RawJson { .. } => PayloadFormat::ChatGptExportJson,
        ChatGptPayloadShape::Zip { .. } => PayloadFormat::ChatGptExportZip,
    };
    let (payload_blob, parse_input): (&[u8], &[u8]) = match &shape {
        ChatGptPayloadShape::RawJson { bytes } => (*bytes, *bytes),
        ChatGptPayloadShape::Zip {
            zip_bytes,
            conversations_json,
        } => (*zip_bytes, conversations_json.as_slice()),
    };

    import_payload_with_parse_bytes(
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
    import_document_payload(
        store,
        object_store,
        SourceType::TextFile,
        PayloadFormat::TextPlain,
        DocumentFormat::PlainText,
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
    import_document_payload(
        store,
        object_store,
        SourceType::MarkdownFile,
        PayloadFormat::MarkdownText,
        DocumentFormat::Markdown,
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
    let spec = source_import_spec(SourceType::ObsidianVault, PayloadFormat::ObsidianVaultZip)?;
    let parsed = parser::obsidian::parse_vault_zip(payload_bytes)?;
    let prepared = build_obsidian_import_set(payload_bytes, spec, parsed, object_store)?;
    let result = match store.write_import(prepared.write_set) {
        Ok(result) => result,
        Err(err) => {
            cleanup_unreferenced_payload_object(object_store, &prepared.payload_put, &err);
            return Err(err.into());
        }
    };

    Ok(ImportResponse {
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
    })
}

fn import_document_payload<S, O>(
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

    Ok(ImportResponse {
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
    })
}

struct PreparedImportSet {
    write_set: WriteImportSet,
    payload_put: PutObjectResult,
}

fn build_import_set(
    payload_bytes: &[u8],
    spec: SourceImportSpec,
    conversations: Vec<ParsedConversation>,
    object_store: &(impl ObjectStore + ?Sized),
) -> Result<PreparedImportSet, OpenArchiveError> {
    let payload_sha256 = sha256_hex(payload_bytes);
    let payload_object_id = new_id("payload");
    let import_id = new_id("import");
    let payload_put = persist_raw_payload(
        object_store,
        &payload_object_id,
        payload_bytes,
        &payload_sha256,
        spec.payload_mime_type,
    )?;
    let content_facets_json = serde_json::to_string(spec.content_facets).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize content facets: {err}"))
    })?;

    let artifact_sets = conversations
        .into_iter()
        .map(|conversation| {
            build_artifact_set(&import_id, &content_facets_json, spec, conversation)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PreparedImportSet {
        write_set: WriteImportSet {
            payload_object: NewImportObjectRef {
                object_id: payload_object_id.clone(),
                payload_format: spec.payload_format,
                mime_type: spec.payload_mime_type.to_string(),
                size_bytes: payload_bytes.len() as i64,
                sha256: payload_sha256.clone(),
                stored_object: payload_put.stored_object.clone(),
            },
            import: NewImport {
                import_id,
                source_type: spec.source_type,
                import_status: ImportStatus::Pending,
                payload_object_id,
                source_filename: None,
                source_content_hash: payload_sha256,
                // The persisted field predates document-style imports; for an
                // Obsidian vault this records the detected note count.
                conversation_count_detected: artifact_sets.len() as i32,
            },
            artifact_sets,
        },
        payload_put,
    })
}

fn build_document_import_set(
    payload_bytes: &[u8],
    spec: SourceImportSpec,
    document: parser::document::ParsedDocument,
    object_store: &(impl ObjectStore + ?Sized),
) -> Result<PreparedImportSet, OpenArchiveError> {
    let payload_sha256 = sha256_hex(payload_bytes);
    let payload_object_id = new_id("payload");
    let import_id = new_id("import");
    let payload_put = persist_raw_payload(
        object_store,
        &payload_object_id,
        payload_bytes,
        &payload_sha256,
        spec.payload_mime_type,
    )?;
    let content_facets_json = serde_json::to_string(spec.content_facets).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize content facets: {err}"))
    })?;
    let artifact_set =
        build_document_artifact_set(&import_id, &content_facets_json, spec, document)?;

    Ok(PreparedImportSet {
        write_set: WriteImportSet {
            payload_object: NewImportObjectRef {
                object_id: payload_object_id.clone(),
                payload_format: spec.payload_format,
                mime_type: spec.payload_mime_type.to_string(),
                size_bytes: payload_bytes.len() as i64,
                sha256: payload_sha256.clone(),
                stored_object: payload_put.stored_object.clone(),
            },
            import: NewImport {
                import_id,
                source_type: spec.source_type,
                import_status: ImportStatus::Pending,
                payload_object_id,
                source_filename: None,
                source_content_hash: payload_sha256,
                conversation_count_detected: 1,
            },
            artifact_sets: vec![artifact_set],
        },
        payload_put,
    })
}

fn build_obsidian_import_set(
    payload_bytes: &[u8],
    spec: SourceImportSpec,
    vault: parser::obsidian::ParsedVault,
    object_store: &(impl ObjectStore + ?Sized),
) -> Result<PreparedImportSet, OpenArchiveError> {
    let payload_sha256 = sha256_hex(payload_bytes);
    let payload_object_id = new_id("payload");
    let import_id = new_id("import");
    let payload_put = persist_raw_payload(
        object_store,
        &payload_object_id,
        payload_bytes,
        &payload_sha256,
        spec.payload_mime_type,
    )?;
    let content_facets_json = serde_json::to_string(spec.content_facets).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize content facets: {err}"))
    })?;

    let planned_notes = vault
        .notes
        .into_iter()
        .map(|note| (new_id("artifact"), note))
        .collect::<Vec<_>>();
    let artifact_ids_by_path = planned_notes
        .iter()
        .map(|(artifact_id, note)| (note.note_path.clone(), artifact_id.clone()))
        .collect::<HashMap<_, _>>();

    let artifact_sets = planned_notes
        .into_iter()
        .map(|(artifact_id, note)| {
            build_obsidian_artifact_set(
                &import_id,
                &content_facets_json,
                spec,
                artifact_id,
                note,
                &artifact_ids_by_path,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PreparedImportSet {
        write_set: WriteImportSet {
            payload_object: NewImportObjectRef {
                object_id: payload_object_id.clone(),
                payload_format: spec.payload_format,
                mime_type: spec.payload_mime_type.to_string(),
                size_bytes: payload_bytes.len() as i64,
                sha256: payload_sha256.clone(),
                stored_object: payload_put.stored_object.clone(),
            },
            import: NewImport {
                import_id,
                source_type: spec.source_type,
                import_status: ImportStatus::Pending,
                payload_object_id,
                source_filename: None,
                source_content_hash: payload_sha256,
                conversation_count_detected: artifact_sets.len() as i32,
            },
            artifact_sets,
        },
        payload_put,
    })
}

fn persist_raw_payload(
    object_store: &(impl ObjectStore + ?Sized),
    object_id: &str,
    payload_bytes: &[u8],
    payload_sha256: &str,
    mime_type: &str,
) -> Result<PutObjectResult, OpenArchiveError> {
    object_store
        .put_object(NewObject {
            object_id: object_id.to_string(),
            namespace: "import-payloads".to_string(),
            mime_type: mime_type.to_string(),
            bytes: payload_bytes.to_vec(),
            sha256: payload_sha256.to_string(),
        })
        .map_err(OpenArchiveError::from)
}

fn cleanup_unreferenced_payload_object(
    object_store: &(impl ObjectStore + ?Sized),
    payload_put: &PutObjectResult,
    error: &crate::error::StorageError,
) {
    if !payload_put.was_created || !is_safe_precommit_import_failure(error) {
        return;
    }

    if let Err(cleanup_err) = object_store.delete_object(&payload_put.stored_object) {
        log::warn!(
            "failed to clean up unreferenced payload object {} after import write error: {}",
            payload_put.stored_object.object_id,
            cleanup_err
        );
    }
}

fn is_safe_precommit_import_failure(error: &crate::error::StorageError) -> bool {
    matches!(
        error,
        crate::error::StorageError::InsertPayload { .. }
            | crate::error::StorageError::InsertImport { .. }
    )
}

fn build_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    spec: SourceImportSpec,
    conversation: ParsedConversation,
) -> Result<WriteArtifactSet, OpenArchiveError> {
    let artifact_id = new_id("artifact");
    let mut participant_ids_by_key = HashMap::with_capacity(conversation.participants.len());

    let participants = conversation
        .participants
        .into_iter()
        .map(|participant| {
            let participant_id = new_id("participant");
            participant_ids_by_key.insert(participant.source_key.clone(), participant_id.clone());
            NewParticipant {
                participant_id,
                artifact_id: artifact_id.clone(),
                participant_role: participant.role,
                display_name: participant.display_name,
                provider_name: None,
                model_name: participant.model_name,
                source_participant_key: Some(participant.source_key),
                sequence_no: participant.sequence_no as i32,
            }
        })
        .collect::<Vec<_>>();

    let segments = conversation
        .messages
        .into_iter()
        .enumerate()
        .map(|(sequence_no, message)| {
            build_segment(&artifact_id, &participant_ids_by_key, sequence_no, message)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let created_at_source = conversation.create_time.map(SourceTimestamp::from);
    let ended_at = conversation.update_time.map(SourceTimestamp::from);

    Ok(WriteArtifactSet {
        artifact: NewArtifact {
            artifact_id: artifact_id.clone(),
            import_id: import_id.to_string(),
            artifact_class: spec.artifact_class,
            source_type: spec.source_type,
            artifact_status: ArtifactStatus::Captured,
            enrichment_status: EnrichmentStatus::Pending,
            source_conversation_key: Some(conversation.source_id.clone()),
            source_conversation_hash: conversation.content_hash,
            title: conversation.title,
            created_at_source: created_at_source.clone(),
            started_at: created_at_source,
            ended_at,
            primary_language: None,
            content_hash_version: spec.content_hash_version.to_string(),
            content_facets_json: content_facets_json.to_string(),
            normalization_version: spec.normalization_version.to_string(),
        },
        participants,
        segments,
        imported_note_metadata: ImportedNoteMetadataWriteSet::default(),
        job: NewEnrichmentJob {
            job_id: new_id("job"),
            artifact_id: artifact_id.clone(),
            job_type: JobType::ArtifactExtract,
            enrichment_tier: crate::storage::EnrichmentTier::Default,
            spawned_by_job_id: None,
            job_status: JobStatus::Pending,
            max_attempts: 3,
            priority_no: 100,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactExtractPayload::new_v1(
                &artifact_id,
                import_id,
                spec.source_type,
                None,
                Vec::new(),
                Vec::new(),
            )
            .to_json(),
        },
    })
}

fn parse_conversations_for_source(
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
            )))
        }
    };

    parsed.map_err(OpenArchiveError::from)
}

fn source_import_spec(
    source_type: SourceType,
    payload_format: PayloadFormat,
) -> Result<SourceImportSpec, OpenArchiveError> {
    let spec = match source_type {
        SourceType::ChatGptExport => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Conversation,
            payload_mime_type: match payload_format {
                PayloadFormat::ChatGptExportZip => "application/zip",
                PayloadFormat::ChatGptExportJson | PayloadFormat::ChatGptExportCanonicalJson => {
                    "application/json"
                }
                other => {
                    return Err(OpenArchiveError::Invariant(format!(
                        "unexpected payload_format for ChatGptExport: {other:?}"
                    )))
                }
            },
            normalization_version: parser::CHATGPT_NORMALIZATION_VERSION,
            content_hash_version: parser::CHATGPT_CONTENT_HASH_VERSION,
            content_facets: parser::CHATGPT_CONTENT_FACETS,
        },
        SourceType::ClaudeExport => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Conversation,
            payload_mime_type: "application/json",
            normalization_version: parser::claude::CLAUDE_NORMALIZATION_VERSION,
            content_hash_version: parser::claude::CLAUDE_CONTENT_HASH_VERSION,
            content_facets: parser::claude::CLAUDE_CONTENT_FACETS,
        },
        SourceType::GrokExport => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Conversation,
            payload_mime_type: "application/json",
            normalization_version: parser::grok::GROK_NORMALIZATION_VERSION,
            content_hash_version: parser::grok::GROK_CONTENT_HASH_VERSION,
            content_facets: parser::grok::GROK_CONTENT_FACETS,
        },
        SourceType::GeminiTakeout => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Conversation,
            payload_mime_type: "application/json",
            normalization_version: parser::gemini::GEMINI_NORMALIZATION_VERSION,
            content_hash_version: parser::gemini::GEMINI_CONTENT_HASH_VERSION,
            content_facets: parser::gemini::GEMINI_CONTENT_FACETS,
        },
        SourceType::TextFile => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Document,
            payload_mime_type: "text/plain",
            normalization_version: parser::document::TEXT_NORMALIZATION_VERSION,
            content_hash_version: parser::document::TEXT_CONTENT_HASH_VERSION,
            content_facets: parser::document::TEXT_CONTENT_FACETS,
        },
        SourceType::MarkdownFile => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Document,
            payload_mime_type: "text/markdown",
            normalization_version: parser::document::MARKDOWN_NORMALIZATION_VERSION,
            content_hash_version: parser::document::MARKDOWN_CONTENT_HASH_VERSION,
            content_facets: parser::document::MARKDOWN_CONTENT_FACETS,
        },
        SourceType::ObsidianVault => SourceImportSpec {
            source_type,
            payload_format,
            artifact_class: ArtifactClass::Document,
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

fn build_segment(
    artifact_id: &str,
    participant_ids_by_key: &HashMap<String, String>,
    sequence_no: usize,
    message: ParsedMessage,
) -> Result<NewSegment, OpenArchiveError> {
    let participant_id = participant_ids_by_key
        .get(&message.participant_key)
        .cloned()
        .ok_or_else(|| {
            OpenArchiveError::Invariant(format!(
                "missing participant id for message participant key {}",
                message.participant_key
            ))
        })?;

    let unsupported_content_json = if message.unsupported_content.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&message.unsupported_content).map_err(|err| {
                OpenArchiveError::Invariant(format!(
                    "failed to serialize unsupported content for node {}: {err}",
                    message.source_node_id
                ))
            })?,
        )
    };

    Ok(NewSegment {
        segment_id: new_id("segment"),
        artifact_id: artifact_id.to_string(),
        participant_id: Some(participant_id),
        segment_type: SegmentType::ContentBlock,
        source_segment_key: Some(message.source_node_id.clone()),
        parent_segment_id: None,
        sequence_no: sequence_no as i32,
        created_at_source: message.create_time.map(SourceTimestamp::from),
        text_content_hash: sha256_hex(message.text_content.as_bytes()),
        text_content: message.text_content,
        locator_json: Some(
            serde_json::json!({ "source_node_id": message.source_node_id }).to_string(),
        ),
        visibility_status: message.visibility,
        unsupported_content_json,
    })
}

fn build_document_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    spec: SourceImportSpec,
    document: parser::document::ParsedDocument,
) -> Result<WriteArtifactSet, OpenArchiveError> {
    let artifact_id = new_id("artifact");
    let segments = document
        .blocks
        .into_iter()
        .enumerate()
        .map(|(sequence_no, block)| NewSegment {
            segment_id: new_id("segment"),
            artifact_id: artifact_id.clone(),
            participant_id: None,
            segment_type: SegmentType::ContentBlock,
            source_segment_key: Some(format!("block-{sequence_no}")),
            parent_segment_id: None,
            sequence_no: sequence_no as i32,
            created_at_source: None,
            text_content_hash: sha256_hex(block.text_content.as_bytes()),
            text_content: block.text_content,
            locator_json: block.locator_json,
            visibility_status: crate::domain::VisibilityStatus::Visible,
            unsupported_content_json: None,
        })
        .collect();

    Ok(WriteArtifactSet {
        artifact: NewArtifact {
            artifact_id: artifact_id.clone(),
            import_id: import_id.to_string(),
            artifact_class: spec.artifact_class,
            source_type: spec.source_type,
            artifact_status: ArtifactStatus::Captured,
            enrichment_status: EnrichmentStatus::Pending,
            source_conversation_key: None,
            source_conversation_hash: document.content_hash,
            title: document.title,
            created_at_source: None,
            started_at: None,
            ended_at: None,
            primary_language: None,
            content_hash_version: spec.content_hash_version.to_string(),
            content_facets_json: content_facets_json.to_string(),
            normalization_version: spec.normalization_version.to_string(),
        },
        participants: Vec::new(),
        segments,
        imported_note_metadata: ImportedNoteMetadataWriteSet::default(),
        job: NewEnrichmentJob {
            job_id: new_id("job"),
            artifact_id: artifact_id.clone(),
            job_type: JobType::ArtifactExtract,
            enrichment_tier: crate::storage::EnrichmentTier::Default,
            spawned_by_job_id: None,
            job_status: JobStatus::Pending,
            max_attempts: 3,
            priority_no: 100,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactExtractPayload::new_v1(
                &artifact_id,
                import_id,
                spec.source_type,
                None,
                Vec::new(),
                Vec::new(),
            )
            .to_json(),
        },
    })
}

fn build_obsidian_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    spec: SourceImportSpec,
    artifact_id: String,
    note: parser::obsidian::ParsedVaultNote,
    artifact_ids_by_path: &HashMap<String, String>,
) -> Result<WriteArtifactSet, OpenArchiveError> {
    let note_identity_hash =
        obsidian_note_identity_hash(&note.note_path, &note.document.content_hash);
    let segments = note
        .document
        .blocks
        .iter()
        .enumerate()
        .map(|(sequence_no, block)| NewSegment {
            segment_id: new_id("segment"),
            artifact_id: artifact_id.clone(),
            participant_id: None,
            segment_type: SegmentType::ContentBlock,
            source_segment_key: Some(format!("block-{sequence_no}")),
            parent_segment_id: None,
            sequence_no: sequence_no as i32,
            created_at_source: None,
            text_content_hash: sha256_hex(block.text_content.as_bytes()),
            text_content: block.text_content.clone(),
            locator_json: block.locator_json.clone(),
            visibility_status: crate::domain::VisibilityStatus::Visible,
            unsupported_content_json: None,
        })
        .collect::<Vec<_>>();

    let imported_note_metadata = ImportedNoteMetadata {
        note_path: Some(note.note_path.clone()),
        properties: note
            .properties
            .iter()
            .map(|property| crate::storage::ImportedNotePropertyRecord {
                property_key: property.property_key.clone(),
                value_kind: property.value_kind,
                value_text: property.value_text.clone(),
                value_json: property.value_json.clone(),
            })
            .collect(),
        tags: note
            .tags
            .iter()
            .map(|tag| crate::storage::ImportedNoteTagRecord {
                raw_tag: tag.raw_tag.clone(),
                normalized_tag: tag.normalized_tag.clone(),
                tag_path: tag.tag_path.clone(),
                source_kind: tag.source_kind,
            })
            .collect(),
        aliases: note
            .aliases
            .iter()
            .map(|alias| crate::storage::ImportedNoteAliasRecord {
                alias_text: alias.alias_text.clone(),
                normalized_alias: alias.normalized_alias.clone(),
            })
            .collect(),
        outbound_links: note
            .links
            .iter()
            .map(|link| crate::storage::ImportedNoteLinkRecord {
                imported_note_link_id: String::new(),
                source_segment_id: None,
                link_kind: link.link_kind,
                target_kind: link.target_kind,
                raw_target: link.raw_target.clone(),
                normalized_target: link.normalized_target.clone(),
                display_text: link.display_text.clone(),
                target_path: link.target_path.clone(),
                target_heading: link.target_heading.clone(),
                target_block: link.target_block.clone(),
                external_url: link.external_url.clone(),
                resolved_artifact_id: link
                    .target_path
                    .as_ref()
                    .and_then(|path| artifact_ids_by_path.get(path))
                    .cloned(),
                resolution_status: link.resolution_status,
                locator_json: link.locator_json.clone(),
            })
            .collect(),
    };

    let metadata_write_set = ImportedNoteMetadataWriteSet {
        properties: note
            .properties
            .into_iter()
            .enumerate()
            .map(|(sequence_no, property)| NewImportedNoteProperty {
                imported_note_property_id: new_id("noteprop"),
                artifact_id: artifact_id.clone(),
                property_key: property.property_key,
                value_kind: property.value_kind,
                value_text: property.value_text,
                value_json: property.value_json.to_string(),
                sequence_no: sequence_no as i32,
            })
            .collect(),
        tags: note
            .tags
            .into_iter()
            .enumerate()
            .map(|(sequence_no, tag)| NewImportedNoteTag {
                imported_note_tag_id: new_id("notetag"),
                artifact_id: artifact_id.clone(),
                raw_tag: tag.raw_tag,
                normalized_tag: tag.normalized_tag,
                tag_path: tag.tag_path,
                source_kind: tag.source_kind,
                sequence_no: sequence_no as i32,
            })
            .collect(),
        aliases: note
            .aliases
            .into_iter()
            .enumerate()
            .map(|(sequence_no, alias)| NewImportedNoteAlias {
                imported_note_alias_id: new_id("notealias"),
                artifact_id: artifact_id.clone(),
                alias_text: alias.alias_text,
                normalized_alias: alias.normalized_alias,
                sequence_no: sequence_no as i32,
            })
            .collect(),
        links: note
            .links
            .into_iter()
            .enumerate()
            .map(|(sequence_no, link)| NewImportedNoteLink {
                imported_note_link_id: new_id("notelink"),
                artifact_id: artifact_id.clone(),
                source_segment_id: None,
                link_kind: link.link_kind,
                target_kind: link.target_kind,
                raw_target: link.raw_target,
                normalized_target: link.normalized_target,
                display_text: link.display_text,
                target_path: link.target_path.clone(),
                target_heading: link.target_heading,
                target_block: link.target_block,
                external_url: link.external_url,
                resolved_artifact_id: link
                    .target_path
                    .as_ref()
                    .and_then(|path| artifact_ids_by_path.get(path))
                    .cloned(),
                resolution_status: link.resolution_status,
                locator_json: link.locator_json,
                sequence_no: sequence_no as i32,
            })
            .collect(),
    };

    Ok(WriteArtifactSet {
        artifact: NewArtifact {
            artifact_id: artifact_id.clone(),
            import_id: import_id.to_string(),
            artifact_class: spec.artifact_class,
            source_type: spec.source_type,
            artifact_status: ArtifactStatus::Captured,
            enrichment_status: EnrichmentStatus::Pending,
            source_conversation_key: Some(note.note_path),
            source_conversation_hash: note_identity_hash,
            title: note.title,
            created_at_source: None,
            started_at: None,
            ended_at: None,
            primary_language: None,
            content_hash_version: spec.content_hash_version.to_string(),
            content_facets_json: content_facets_json.to_string(),
            normalization_version: spec.normalization_version.to_string(),
        },
        participants: Vec::new(),
        segments,
        imported_note_metadata: metadata_write_set,
        job: NewEnrichmentJob {
            job_id: new_id("job"),
            artifact_id: artifact_id.clone(),
            job_type: JobType::ArtifactExtract,
            enrichment_tier: crate::storage::EnrichmentTier::Default,
            spawned_by_job_id: None,
            job_status: JobStatus::Pending,
            max_attempts: 3,
            priority_no: 100,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactExtractPayload::new_v1(
                &artifact_id,
                import_id,
                spec.source_type,
                Some(imported_note_metadata),
                Vec::new(),
                Vec::new(),
            )
            .to_json(),
        },
    })
}

fn obsidian_note_identity_hash(note_path: &str, content_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(note_path.as_bytes());
    hasher.update([0]);
    hasher.update(content_hash.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn new_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{counter:x}")
}

fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ParserError;
    use crate::object_store::{NewObject, ObjectStore, PutObjectResult, StoredObject};
    use crate::storage::{
        ArtifactIngestResult, EnrichmentStatus, ImportWriteResult, ImportedArtifact, WriteImportSet,
    };
    use std::fs;
    use std::io::{Cursor, Write};
    use std::sync::Mutex;
    use zip::write::SimpleFileOptions;

    struct MockStore {
        captured: Mutex<Vec<WriteImportSet>>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                captured: Mutex::new(Vec::new()),
            }
        }
    }

    impl ImportWriteStore for MockStore {
        fn write_import(
            &self,
            import_set: WriteImportSet,
        ) -> crate::error::StorageResult<ImportWriteResult> {
            let artifact_ids = import_set
                .artifact_sets
                .iter()
                .map(|set| set.artifact.artifact_id.clone())
                .collect::<Vec<_>>();
            self.captured.lock().unwrap().push(import_set);
            Ok(ImportWriteResult {
                import_id: "import-test".to_string(),
                import_status: ImportStatus::Completed,
                artifacts: artifact_ids
                    .into_iter()
                    .map(|artifact_id| ImportedArtifact {
                        artifact_id,
                        enrichment_status: EnrichmentStatus::Pending,
                        ingest_result: ArtifactIngestResult::Created,
                    })
                    .collect(),
                failed_artifact_ids: Vec::new(),
                segments_written: 0,
            })
        }
    }

    struct FailingImportStore;

    impl ImportWriteStore for FailingImportStore {
        fn write_import(
            &self,
            import_set: WriteImportSet,
        ) -> crate::error::StorageResult<ImportWriteResult> {
            Err(crate::error::StorageError::InsertImport {
                import_id: import_set.import.import_id,
                source: Box::new(std::io::Error::other("synthetic precommit failure")),
            })
        }
    }

    struct MockObjectStore {
        puts: Mutex<Vec<NewObject>>,
        deletes: Mutex<Vec<StoredObject>>,
    }

    impl MockObjectStore {
        fn new() -> Self {
            Self {
                puts: Mutex::new(Vec::new()),
                deletes: Mutex::new(Vec::new()),
            }
        }
    }

    impl ObjectStore for MockObjectStore {
        fn put_object(
            &self,
            object: NewObject,
        ) -> crate::error::ObjectStoreResult<PutObjectResult> {
            let stored = StoredObject {
                object_id: object.object_id.clone(),
                provider: "mock".to_string(),
                storage_key: format!("{}/{}", object.namespace, object.sha256),
                mime_type: object.mime_type.clone(),
                size_bytes: object.bytes.len() as i64,
                sha256: object.sha256.clone(),
            };
            self.puts.lock().unwrap().push(object);
            Ok(PutObjectResult {
                stored_object: stored,
                was_created: true,
            })
        }

        fn get_object_bytes(
            &self,
            object: &StoredObject,
        ) -> crate::error::ObjectStoreResult<Vec<u8>> {
            Ok(object.storage_key.as_bytes().to_vec())
        }

        fn delete_object(&self, object: &StoredObject) -> crate::error::ObjectStoreResult<()> {
            self.deletes.lock().unwrap().push(object.clone());
            Ok(())
        }
    }

    #[test]
    fn import_chatgpt_payload_returns_compact_response() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let response = import_chatgpt_payload(
            &store,
            &object_store,
            single_conversation_export().as_bytes(),
        )
        .unwrap();

        assert_eq!(response.import_id, "import-test");
        assert_eq!(response.import_status, "completed");
        assert_eq!(response.artifacts.len(), 1);
        assert_eq!(response.artifacts[0].enrichment_status, "pending");
        assert_eq!(response.artifacts[0].ingest_result, "created");
    }

    #[test]
    fn import_chatgpt_payload_builds_multi_conversation_write_set() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        import_chatgpt_payload(
            &store,
            &object_store,
            multi_conversation_export().as_bytes(),
        )
        .unwrap();

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();
        assert_eq!(import_set.import.conversation_count_detected, 2);
        assert_eq!(import_set.artifact_sets.len(), 2);
        assert_eq!(
            import_set.import.payload_object_id,
            import_set.payload_object.object_id
        );
        assert_eq!(
            import_set.artifact_sets[0].job.job_status,
            JobStatus::Pending
        );
        assert_eq!(
            import_set.artifact_sets[1].artifact.enrichment_status,
            EnrichmentStatus::Pending
        );

        // Verify job payload matches ArtifactExtractPayload v1 contract
        for artifact_set in &import_set.artifact_sets {
            let payload =
                crate::storage::ArtifactExtractPayload::from_json(&artifact_set.job.payload_json)
                    .expect("payload must deserialize to ArtifactExtractPayload");
            assert_eq!(payload.schema_version, "1");
            assert_eq!(
                payload.source_type,
                super::SourceType::ChatGptExport.as_str()
            );
            assert!(payload.conversation_windows.is_empty());
            assert!(payload.topic_threads.is_empty());
        }
    }

    #[test]
    fn import_text_payload_builds_document_artifact_without_participants() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let response = import_text_payload(
            &store,
            &object_store,
            b"First paragraph.\n\nSecond paragraph.",
        )
        .unwrap();

        assert_eq!(response.import_status, "completed");

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::TextFile);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::TextPlain
        );
        assert_eq!(import_set.payload_object.mime_type, "text/plain");
        assert_eq!(import_set.artifact_sets.len(), 1);
        assert!(import_set.artifact_sets[0].participants.is_empty());
        assert_eq!(
            import_set.artifact_sets[0].artifact.artifact_class,
            ArtifactClass::Document
        );
        assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
    }

    #[test]
    fn import_markdown_payload_preserves_heading_title_and_markdown_mime_type() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        import_markdown_payload(
            &store,
            &object_store,
            b"# Project Notes\n\nParagraph text.\n\n- one\n- two",
        )
        .unwrap();

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::MarkdownFile);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::MarkdownText
        );
        assert_eq!(import_set.payload_object.mime_type, "text/markdown");
        assert_eq!(
            import_set.artifact_sets[0].artifact.title.as_deref(),
            Some("Project Notes")
        );
        assert_eq!(import_set.artifact_sets[0].segments.len(), 3);
    }

    #[test]
    fn import_obsidian_payload_builds_first_class_note_metadata() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response =
            import_obsidian_vault_payload(&store, &object_store, &obsidian_fixture_zip())
                .expect("obsidian import should succeed");

        assert_eq!(response.artifacts.len(), 2);

        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().expect("import set should exist");
        assert_eq!(import_set.import.source_type, SourceType::ObsidianVault);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ObsidianVaultZip
        );
        assert_eq!(import_set.payload_object.mime_type, "application/zip");

        let artifact_ids_by_path = import_set
            .artifact_sets
            .iter()
            .map(|artifact_set| {
                (
                    artifact_set
                        .artifact
                        .source_conversation_key
                        .clone()
                        .expect("note path should exist"),
                    artifact_set.artifact.artifact_id.clone(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        let inbox = import_set
            .artifact_sets
            .iter()
            .find(|artifact_set| {
                artifact_set.artifact.source_conversation_key.as_deref() == Some("Inbox.md")
            })
            .expect("Inbox.md artifact should exist");
        assert_eq!(inbox.artifact.artifact_class, ArtifactClass::Document);
        assert!(inbox.participants.is_empty());
        assert_eq!(inbox.imported_note_metadata.properties.len(), 5);
        assert_eq!(inbox.imported_note_metadata.tags.len(), 2);
        assert_eq!(inbox.imported_note_metadata.aliases.len(), 1);
        assert_eq!(inbox.imported_note_metadata.links.len(), 2);
        assert_eq!(
            inbox.imported_note_metadata.links[0]
                .resolved_artifact_id
                .as_deref(),
            artifact_ids_by_path
                .get("Projects/Acme.md")
                .map(String::as_str)
        );

        let payload = crate::storage::ArtifactExtractPayload::from_json(&inbox.job.payload_json)
            .expect("payload must deserialize");
        let note_metadata = payload
            .imported_note_metadata
            .expect("obsidian payload should include imported note metadata");
        assert_eq!(payload.source_type, SourceType::ObsidianVault.as_str());
        assert_eq!(note_metadata.note_path.as_deref(), Some("Inbox.md"));
        assert_eq!(note_metadata.tags.len(), 2);
        assert_eq!(note_metadata.aliases[0].normalized_alias, "oa inbox");
        assert_eq!(
            note_metadata.outbound_links[0]
                .resolved_artifact_id
                .as_deref(),
            artifact_ids_by_path
                .get("Projects/Acme.md")
                .map(String::as_str)
        );
    }

    #[test]
    fn obsidian_source_hash_includes_note_path_identity() {
        let first = obsidian_note_identity_hash("Inbox.md", "abc123");
        let second = obsidian_note_identity_hash("Projects/Inbox.md", "abc123");

        assert_ne!(first, second);
    }

    #[test]
    fn import_obsidian_payload_keeps_distinct_note_paths_with_same_content() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_obsidian_vault_payload(
            &store,
            &object_store,
            &obsidian_same_content_fixture_zip(),
        )
        .expect("obsidian import should succeed");

        assert_eq!(response.artifacts.len(), 2);

        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().expect("import set should exist");
        assert_eq!(import_set.artifact_sets.len(), 2);

        let mut source_hashes = import_set
            .artifact_sets
            .iter()
            .map(|artifact_set| artifact_set.artifact.source_conversation_hash.clone())
            .collect::<Vec<_>>();
        source_hashes.sort();
        source_hashes.dedup();
        assert_eq!(
            source_hashes.len(),
            2,
            "same-content notes under different paths must not collapse"
        );
    }

    fn single_conversation_export() -> &'static str {
        r#"[{
          "id": "conv-1",
          "title": "First",
          "create_time": 1710000000,
          "update_time": 1710000060,
          "current_node": "m2",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["m1"]},
            "m1": {
              "id": "m1",
              "parent": "root",
              "children": ["m2"],
              "message": {
                "author": {"role": "user", "name": "David"},
                "create_time": 1710000001,
                "content": {"content_type": "text", "parts": ["Hello"]},
                "metadata": {}
              }
            },
            "m2": {
              "id": "m2",
              "parent": "m1",
              "children": [],
              "message": {
                "author": {"role": "assistant", "name": "ChatGPT"},
                "create_time": 1710000002,
                "content": {"content_type": "text", "parts": ["Hi"]},
                "metadata": {"model_slug": "gpt-4"}
              }
            }
          },
          "default_model_slug": "gpt-4"
        }]"#
    }

    fn multi_conversation_export() -> &'static str {
        r#"[{
          "id": "conv-1",
          "title": "First",
          "create_time": 1710000000,
          "update_time": 1710000060,
          "current_node": "m1",
          "mapping": {
            "root": {"id": "root", "message": null, "parent": null, "children": ["m1"]},
            "m1": {
              "id": "m1",
              "parent": "root",
              "children": [],
              "message": {
                "author": {"role": "user", "name": "David"},
                "create_time": 1710000001,
                "content": {"content_type": "text", "parts": ["One"]},
                "metadata": {}
              }
            }
          },
          "default_model_slug": null
        },
        {
          "id": "conv-2",
          "title": "Second",
          "create_time": 1710000100,
          "update_time": 1710000160,
          "current_node": "n2",
          "mapping": {
            "root2": {"id": "root2", "message": null, "parent": null, "children": ["n1"]},
            "n1": {
              "id": "n1",
              "parent": "root2",
              "children": ["n2"],
              "message": {
                "author": {"role": "user", "name": "David"},
                "create_time": 1710000101,
                "content": {"content_type": "text", "parts": ["Two"]},
                "metadata": {}
              }
            },
            "n2": {
              "id": "n2",
              "parent": "n1",
              "children": [],
              "message": {
                "author": {"role": "assistant", "name": "ChatGPT"},
                "create_time": 1710000102,
                "content": {"content_type": "text", "parts": ["Three"]},
                "metadata": {}
              }
            }
          },
          "default_model_slug": null
        }]"#
    }

    fn obsidian_fixture_zip() -> Vec<u8> {
        let root = format!(
            "{}/tests/fixtures/obsidian_vault",
            env!("CARGO_MANIFEST_DIR")
        );
        let files = [
            ("Inbox.md", format!("{root}/Inbox.md")),
            ("Projects/Acme.md", format!("{root}/Projects/Acme.md")),
            (
                ".obsidian/workspace.json",
                format!("{root}/.obsidian/workspace.json"),
            ),
        ];
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        for (zip_path, file_path) in files {
            writer
                .start_file(zip_path, SimpleFileOptions::default())
                .expect("file should start");
            writer
                .write_all(&fs::read(file_path).expect("fixture should read"))
                .expect("fixture bytes should write");
        }
        writer.finish().expect("zip should finish").into_inner()
    }

    fn obsidian_same_content_fixture_zip() -> Vec<u8> {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        let files = [
            (
                "Templates/Header 1.md",
                "# Repeated Header\n\nShared body.\n",
            ),
            (
                "Templates/Header 2.md",
                "# Repeated Header\n\nShared body.\n",
            ),
            (".obsidian/workspace.json", "{}"),
        ];
        for (zip_path, contents) in files {
            writer
                .start_file(zip_path, SimpleFileOptions::default())
                .expect("file should start");
            writer
                .write_all(contents.as_bytes())
                .expect("fixture bytes should write");
        }
        writer.finish().expect("zip should finish").into_inner()
    }

    #[test]
    fn import_chatgpt_payload_rejects_malformed_json() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let err = import_chatgpt_payload(&store, &object_store, br#"{"bad":true}"#).unwrap_err();
        assert!(matches!(err, OpenArchiveError::Parser(_)));
        assert!(
            store.captured.lock().unwrap().is_empty(),
            "parser failures must not call the storage boundary"
        );
    }

    #[test]
    fn import_chatgpt_payload_parser_failure_does_not_mutate_prior_store_state() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        import_chatgpt_payload(
            &store,
            &object_store,
            single_conversation_export().as_bytes(),
        )
        .unwrap();

        let err = import_chatgpt_payload(&store, &object_store, br#"{"bad":true}"#).unwrap_err();
        assert!(matches!(err, OpenArchiveError::Parser(_)));

        let captured = store.captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "malformed payloads must not enqueue a second write"
        );
    }

    #[test]
    fn import_chatgpt_payload_cleans_up_new_object_on_safe_precommit_failure() {
        let store = FailingImportStore;
        let object_store = MockObjectStore::new();

        let err = import_chatgpt_payload(
            &store,
            &object_store,
            single_conversation_export().as_bytes(),
        )
        .unwrap_err();

        assert!(matches!(err, OpenArchiveError::Storage(_)));
        assert_eq!(object_store.puts.lock().unwrap().len(), 1);
        assert_eq!(object_store.deletes.lock().unwrap().len(), 1);
    }

    #[test]
    fn import_claude_payload_uses_claude_source_metadata() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_claude_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/claude_export.json"
            )),
        )
        .unwrap();

        assert_eq!(response.artifacts.len(), 1);
        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::ClaudeExport);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ClaudeExportJson
        );
        assert_eq!(
            import_set.artifact_sets[0].artifact.normalization_version,
            parser::claude::CLAUDE_NORMALIZATION_VERSION
        );
    }

    #[test]
    fn import_grok_payload_uses_grok_source_metadata() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_grok_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/grok_export.json"
            )),
        )
        .unwrap();

        assert_eq!(response.artifacts.len(), 1);
        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::GrokExport);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::GrokExportJson
        );
        assert_eq!(
            import_set.artifact_sets[0].artifact.content_hash_version,
            parser::grok::GROK_CONTENT_HASH_VERSION
        );
    }

    #[test]
    fn import_gemini_payload_skips_empty_items_and_imports_usable_entries() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_gemini_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/gemini_export.json"
            )),
        )
        .unwrap();

        assert_eq!(response.artifacts.len(), 1);
        let captured = store.captured.lock().unwrap();
        let import_set = captured.last().unwrap();
        assert_eq!(import_set.import.source_type, SourceType::GeminiTakeout);
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::GeminiTakeoutJson
        );
        assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
    }

    #[test]
    fn import_gemini_payload_rejects_payloads_without_usable_entries() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let err = import_gemini_payload(
            &store,
            &object_store,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/gemini_empty_export.json"
            )),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            OpenArchiveError::Parser(ParserError::EmptyExport)
        ));
    }

    // Tests for ChatGPT zip payload fidelity

    fn chatgpt_export_zip() -> Vec<u8> {
        let json_content = single_conversation_export();
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("conversations.json", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(json_content.as_bytes())
            .expect("content should write");
        writer.finish().expect("zip should finish").into_inner()
    }

    fn chatgpt_export_zip_with_extra_files() -> Vec<u8> {
        let json_content = single_conversation_export();
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("README.txt", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(b"This is a ChatGPT export")
            .expect("content should write");
        writer
            .start_file("conversations.json", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(json_content.as_bytes())
            .expect("content should write");
        writer.finish().expect("zip should finish").into_inner()
    }

    fn chatgpt_export_zip_missing_conversations() -> Vec<u8> {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("README.txt", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(b"Missing conversations.json")
            .expect("content should write");
        writer.finish().expect("zip should finish").into_inner()
    }

    #[test]
    fn import_chatgpt_payload_accepts_zip_export() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let zip_bytes = chatgpt_export_zip();

        let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

        assert_eq!(response.artifacts.len(), 1);

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();

        // Verify the zip format and mime type
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ChatGptExportZip
        );
        assert_eq!(import_set.payload_object.mime_type, "application/zip");

        // Verify stored blob matches input zip bytes (via sha256)
        let expected_sha256 = sha256_hex(&zip_bytes);
        assert_eq!(import_set.payload_object.sha256, expected_sha256);

        // Verify the parsed artifact matches what we'd get from raw JSON
        let artifact = &import_set.artifact_sets[0].artifact;
        assert_eq!(artifact.source_conversation_key.as_deref(), Some("conv-1"));
        assert_eq!(import_set.artifact_sets[0].segments.len(), 2);
    }

    #[test]
    fn import_chatgpt_payload_accepts_zip_with_extra_files() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let zip_bytes = chatgpt_export_zip_with_extra_files();

        let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

        assert_eq!(response.artifacts.len(), 1);

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();

        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ChatGptExportZip
        );
        assert_eq!(import_set.payload_object.mime_type, "application/zip");
    }

    #[test]
    fn import_chatgpt_payload_zip_missing_conversations_json_errors() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let zip_bytes = chatgpt_export_zip_missing_conversations();

        let err = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap_err();

        // Should return UnsupportedPayload error
        assert!(
            matches!(
                &err,
                OpenArchiveError::Parser(ParserError::UnsupportedPayload { detail })
                if detail.contains("conversations.json")
            ),
            "expected UnsupportedPayload error mentioning conversations.json, got: {err:?}"
        );

        // Verify no objects were left in the store
        assert!(object_store.puts.lock().unwrap().is_empty());
    }

    #[test]
    fn import_chatgpt_payload_zip_with_malformed_conversations_json_fails_on_parse() {
        // Create a zip with malformed JSON inside
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("conversations.json", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(br#"{"invalid": json}"#)
            .expect("content should write");
        let zip_bytes = writer.finish().expect("zip should finish").into_inner();

        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let err = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap_err();

        // Parser fails before any object is stored
        assert!(matches!(err, OpenArchiveError::Parser(_)));
        // No objects should have been stored
        assert!(object_store.puts.lock().unwrap().is_empty());
        assert!(object_store.deletes.lock().unwrap().is_empty());
    }

    #[test]
    fn import_chatgpt_payload_raw_json_still_uses_json_format() {
        let store = MockStore::new();
        let object_store = MockObjectStore::new();
        let json_bytes = single_conversation_export().as_bytes();

        let response = import_chatgpt_payload(&store, &object_store, json_bytes).unwrap();

        assert_eq!(response.artifacts.len(), 1);

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();

        // Verify raw JSON still uses ChatGptExportJson format
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ChatGptExportJson
        );
        assert_eq!(import_set.payload_object.mime_type, "application/json");

        // Verify stored blob matches input bytes
        let expected_sha256 = sha256_hex(json_bytes);
        assert_eq!(import_set.payload_object.sha256, expected_sha256);
    }

    #[test]
    fn sniff_chatgpt_payload_detects_zip_by_magic() {
        // Valid zip magic
        let zip_bytes = chatgpt_export_zip();
        assert!(zip_bytes.len() >= 4);
        assert_eq!(&zip_bytes[0..4], &[0x50, 0x4B, 0x03, 0x04]);

        let shape = sniff_chatgpt_payload(&zip_bytes).unwrap();
        assert!(
            matches!(shape, ChatGptPayloadShape::Zip { .. }),
            "should detect zip by magic bytes"
        );
    }

    #[test]
    fn sniff_chatgpt_payload_detects_raw_json() {
        let json_bytes = single_conversation_export().as_bytes();

        let shape = sniff_chatgpt_payload(json_bytes).unwrap();
        assert!(
            matches!(shape, ChatGptPayloadShape::RawJson { bytes } if bytes == json_bytes),
            "should detect raw JSON"
        );
    }

    #[test]
    fn sniff_chatgpt_payload_falls_back_to_raw_json_for_short_input() {
        // Truncated zip header (less than 4 bytes with zip magic start)
        let truncated = vec![0x50, 0x4B];

        let shape = sniff_chatgpt_payload(&truncated).unwrap();
        // Won't detect as zip because it's less than 4 bytes, will try JSON parsing
        assert!(
            matches!(shape, ChatGptPayloadShape::RawJson { .. }),
            "short input should fall back to raw JSON"
        );
    }

    #[test]
    fn sniff_chatgpt_payload_handles_empty_bytes() {
        let empty: &[u8] = b"";

        let shape = sniff_chatgpt_payload(empty).unwrap();
        assert!(
            matches!(shape, ChatGptPayloadShape::RawJson { bytes } if bytes.is_empty()),
            "empty bytes should fall back to raw JSON"
        );
    }

    #[test]
    fn sniff_chatgpt_payload_rejects_corrupt_zip_after_valid_magic() {
        // Valid zip magic followed by garbage (valid magic, corrupt body)
        let corrupt = b"PK\x03\x04garbage that is not a valid zip structure";

        let err = sniff_chatgpt_payload(corrupt).unwrap_err();
        // ZipArchive::new should fail and surface as UnsupportedPayload
        assert!(
            matches!(
                &err,
                OpenArchiveError::Parser(ParserError::UnsupportedPayload { detail })
                if detail.contains("corrupt")
            ),
            "expected corrupt zip error, got: {err:?}"
        );
    }

    // Tests for looks_like_chatgpt_zip helper

    #[test]
    fn looks_like_chatgpt_zip_detects_root_level_conversations() {
        let zip_bytes = chatgpt_export_zip();
        assert!(looks_like_chatgpt_zip(&zip_bytes));
    }

    #[test]
    fn looks_like_chatgpt_zip_detects_nested_conversations() {
        let json_content = single_conversation_export();
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "chatgpt-export-2024-01-01/conversations.json",
                SimpleFileOptions::default(),
            )
            .expect("file should start");
        writer
            .write_all(json_content.as_bytes())
            .expect("content should write");
        let zip_bytes = writer.finish().expect("zip should finish").into_inner();

        assert!(looks_like_chatgpt_zip(&zip_bytes));
    }

    #[test]
    fn looks_like_chatgpt_zip_rejects_zip_without_conversations() {
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("README.txt", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(b"No conversations here")
            .expect("content should write");
        let zip_bytes = writer.finish().expect("zip should finish").into_inner();

        assert!(!looks_like_chatgpt_zip(&zip_bytes));
    }

    #[test]
    fn looks_like_chatgpt_zip_rejects_raw_json() {
        let json_bytes = single_conversation_export().as_bytes();
        assert!(!looks_like_chatgpt_zip(json_bytes));
    }

    #[test]
    fn looks_like_chatgpt_zip_rejects_short_input() {
        let truncated = vec![0x50, 0x4B];
        assert!(!looks_like_chatgpt_zip(&truncated));
    }

    #[test]
    fn looks_like_chatgpt_zip_rejects_corrupt_zip() {
        let corrupt = b"PK\x03\x04garbage that is not a valid zip structure";
        assert!(!looks_like_chatgpt_zip(corrupt));
    }

    #[test]
    fn import_chatgpt_payload_accepts_nested_conversations_json() {
        // Create a zip with nested conversations.json (third-party export format)
        let json_content = single_conversation_export();
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "chatgpt-export-2024-01-01/conversations.json",
                SimpleFileOptions::default(),
            )
            .expect("file should start");
        writer
            .write_all(json_content.as_bytes())
            .expect("content should write");
        let zip_bytes = writer.finish().expect("zip should finish").into_inner();

        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

        assert_eq!(response.artifacts.len(), 1);

        let captured = store.captured.lock().unwrap();
        let import_set = captured.first().unwrap();

        // Verify the zip format and mime type
        assert_eq!(
            import_set.payload_object.payload_format,
            PayloadFormat::ChatGptExportZip
        );
        assert_eq!(import_set.payload_object.mime_type, "application/zip");

        // Verify the conversation was parsed correctly
        let artifact = &import_set.artifact_sets[0].artifact;
        assert_eq!(artifact.source_conversation_key.as_deref(), Some("conv-1"));
    }

    #[test]
    fn import_chatgpt_payload_prefers_root_level_over_nested() {
        // Create a zip with both root-level and nested conversations.json
        // Should prefer the root-level one
        let root_json = single_conversation_export();
        let nested_json = multi_conversation_export(); // Different content
        let cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = zip::ZipWriter::new(cursor);

        // Add nested first
        writer
            .start_file(
                "nested/folder/conversations.json",
                SimpleFileOptions::default(),
            )
            .expect("file should start");
        writer
            .write_all(nested_json.as_bytes())
            .expect("content should write");

        // Add root-level second (should be preferred)
        writer
            .start_file("conversations.json", SimpleFileOptions::default())
            .expect("file should start");
        writer
            .write_all(root_json.as_bytes())
            .expect("content should write");

        let zip_bytes = writer.finish().expect("zip should finish").into_inner();

        let store = MockStore::new();
        let object_store = MockObjectStore::new();

        let response = import_chatgpt_payload(&store, &object_store, &zip_bytes).unwrap();

        // Should get 1 artifact (from root-level single_conversation_export)
        // not 2 artifacts (from nested multi_conversation_export)
        assert_eq!(response.artifacts.len(), 1);
    }

    #[test]
    fn import_chatgpt_payload_zip_produces_same_artifacts_as_raw_json() {
        // Import as zip
        let zip_store = MockStore::new();
        let zip_object_store = MockObjectStore::new();
        let zip_bytes = chatgpt_export_zip();
        import_chatgpt_payload(&zip_store, &zip_object_store, &zip_bytes).unwrap();

        // Import as raw JSON
        let json_store = MockStore::new();
        let json_object_store = MockObjectStore::new();
        let json_bytes = single_conversation_export().as_bytes();
        import_chatgpt_payload(&json_store, &json_object_store, json_bytes).unwrap();

        // Compare artifact keys and segment counts
        let zip_captured = zip_store.captured.lock().unwrap();
        let json_captured = json_store.captured.lock().unwrap();

        assert_eq!(
            zip_captured[0].artifact_sets[0]
                .artifact
                .source_conversation_key,
            json_captured[0].artifact_sets[0]
                .artifact
                .source_conversation_key,
            "source_conversation_key should match between zip and JSON imports"
        );

        assert_eq!(
            zip_captured[0].artifact_sets[0].segments.len(),
            json_captured[0].artifact_sets[0].segments.len(),
            "segment count should match between zip and JSON imports"
        );

        // Verify different blob hashes (zip vs JSON)
        assert_ne!(
            zip_captured[0].payload_object.sha256, json_captured[0].payload_object.sha256,
            "zip and JSON should have different blob hashes"
        );
    }
}
