use std::collections::BTreeMap;

use crate::error::OpenArchiveError;
use crate::object_store::{NewObject, ObjectStore, PutObjectResult};
use crate::parser::{self, ParsedConversation};
use crate::storage::{ImportStatus, NewImport, NewImportObjectRef, WriteImportSet};

use super::conversation::build_artifact_set;
use super::document::build_document_artifact_set;
use super::ids::{new_id, sha256_hex};
use super::obsidian::build_obsidian_artifact_set;
use super::SourceImportSpec;

pub(super) struct PreparedImportSet {
    pub(super) write_set: WriteImportSet,
    pub(super) payload_put: PutObjectResult,
}

pub(super) fn build_import_set(
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
                conversation_count_detected: artifact_sets.len() as i32,
            },
            artifact_sets,
        },
        payload_put,
    })
}

pub(super) fn build_document_import_set(
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

pub(super) fn build_obsidian_import_set(
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
        .collect::<BTreeMap<_, _>>();

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

pub(super) fn persist_raw_payload(
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

pub(super) fn cleanup_unreferenced_payload_object(
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
