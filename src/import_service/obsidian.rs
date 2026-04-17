use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::error::OpenArchiveError;
use crate::object_store::ObjectStore;
use crate::parser;
use crate::storage::{
    ArtifactExtractPayload, ArtifactStatus, EnrichmentStatus, ImportStatus, ImportWriteStore,
    ImportedNoteMetadata, ImportedNoteMetadataWriteSet, JobStatus, JobType, NewArtifact,
    NewEnrichmentJob, NewImport, NewImportObjectRef, NewImportedNoteAlias, NewImportedNoteLink,
    NewImportedNoteProperty, NewImportedNoteTag, NewSegment, SegmentType, WriteArtifactSet,
    WriteImportSet,
};

use super::build::{
    build_obsidian_import_set, cleanup_unreferenced_payload_object, persist_raw_payload,
};
use super::dispatch::source_import_spec;
use super::ids::{new_id, sha256_hex};
use super::{to_import_response, ImportResponse, SourceImportSpec};

pub(super) fn import_obsidian_vault_payload_impl<S, O>(
    store: &S,
    object_store: &O,
    payload_bytes: &[u8],
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    let spec = source_import_spec(
        crate::storage::SourceType::ObsidianVault,
        crate::storage::PayloadFormat::ObsidianVaultZip,
    )?;
    let parsed = parser::obsidian::parse_vault_zip(payload_bytes)?;
    let prepared = build_obsidian_import_set(payload_bytes, spec, parsed, object_store)?;
    let result = match store.write_import(prepared.write_set) {
        Ok(result) => result,
        Err(err) => {
            cleanup_unreferenced_payload_object(object_store, &prepared.payload_put, &err);
            return Err(err.into());
        }
    };

    Ok(to_import_response(result))
}

pub(super) fn import_obsidian_vault_directory_impl<S, O>(
    store: &S,
    object_store: &O,
    directory_path: &std::path::Path,
) -> Result<ImportResponse, OpenArchiveError>
where
    S: ImportWriteStore + ?Sized,
    O: ObjectStore + ?Sized,
{
    use crate::parser::obsidian::{
        ObsidianVaultManifest, VaultDirectoryFile, VaultManifestFileEntry,
        OBSIDIAN_VAULT_MANIFEST_VERSION,
    };

    let (parsed_vault, files) = parser::obsidian::parse_vault_directory(directory_path)?;

    let mut manifest_files = BTreeMap::new();
    let mut created_blobs: Vec<crate::object_store::StoredObject> = Vec::new();

    for VaultDirectoryFile {
        relative_path,
        content,
        mtime_millis,
        ..
    } in &files
    {
        let content_bytes = content.as_bytes();
        let content_hash = sha256_hex(content_bytes);
        let blob_id = new_id("blob");

        let blob_result = object_store.put_object(crate::object_store::NewObject {
            object_id: blob_id.clone(),
            namespace: "obsidian-files".to_string(),
            mime_type: "text/markdown".to_string(),
            bytes: content_bytes.to_vec(),
            sha256: content_hash.clone(),
        })?;

        let stored = blob_result.stored_object;
        manifest_files.insert(
            relative_path.clone(),
            VaultManifestFileEntry {
                size: content_bytes.len() as u64,
                mtime_millis: *mtime_millis,
                content_hash: content_hash.clone(),
                provider: stored.provider.clone(),
                storage_key: stored.storage_key.clone(),
                mime_type: stored.mime_type.clone(),
                stored_size_bytes: stored.size_bytes,
                stored_sha256: stored.sha256.clone(),
            },
        );
        if blob_result.was_created {
            created_blobs.push(stored);
        }
    }

    let captured_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OpenArchiveError::Invariant("system time before epoch".to_string()))?
        .as_millis() as i64;

    let manifest = ObsidianVaultManifest {
        version: OBSIDIAN_VAULT_MANIFEST_VERSION,
        captured_at,
        files: manifest_files,
    };

    let import_id = new_id("import");

    let manifest_json = serde_json::to_vec_pretty(&manifest).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize manifest: {err}"))
    })?;
    let manifest_sha256 = sha256_hex(&manifest_json);
    let payload_object_id = new_id("payload");

    let payload_put = match persist_raw_payload(
        object_store,
        &payload_object_id,
        &manifest_json,
        &manifest_sha256,
        "application/json",
    ) {
        Ok(result) => result,
        Err(err) => {
            for stored_object in &created_blobs {
                let _ = object_store.delete_object(stored_object);
            }
            return Err(err);
        }
    };

    let spec = source_import_spec(
        crate::storage::SourceType::ObsidianVault,
        crate::storage::PayloadFormat::ObsidianVaultDirectory,
    )?;
    let content_facets_json = serde_json::to_string(spec.content_facets).map_err(|err| {
        OpenArchiveError::Invariant(format!("failed to serialize content facets: {err}"))
    })?;

    let planned_notes = parsed_vault
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
    let write_set = WriteImportSet {
        payload_object: NewImportObjectRef {
            object_id: payload_object_id.clone(),
            payload_format: crate::storage::PayloadFormat::ObsidianVaultDirectory,
            mime_type: "application/json".to_string(),
            size_bytes: manifest_json.len() as i64,
            sha256: manifest_sha256.clone(),
            stored_object: payload_put.stored_object.clone(),
        },
        import: NewImport {
            import_id: import_id.clone(),
            source_type: crate::storage::SourceType::ObsidianVault,
            import_status: ImportStatus::Pending,
            payload_object_id,
            source_filename: Some(
                directory_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string(),
            ),
            source_content_hash: manifest_sha256,
            conversation_count_detected: artifact_sets.len() as i32,
        },
        artifact_sets,
    };

    let result = match store.write_import(write_set) {
        Ok(result) => result,
        Err(err) => {
            cleanup_unreferenced_payload_object(object_store, &payload_put, &err);
            return Err(err.into());
        }
    };

    Ok(to_import_response(result))
}

pub(super) fn build_obsidian_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    spec: SourceImportSpec,
    artifact_id: String,
    note: parser::obsidian::ParsedVaultNote,
    artifact_ids_by_path: &BTreeMap<String, String>,
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

pub(super) fn obsidian_note_identity_hash(note_path: &str, content_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(note_path.as_bytes());
    hasher.update([0]);
    hasher.update(content_hash.as_bytes());
    format!("{:x}", hasher.finalize())
}
