use crate::error::OpenArchiveError;
use crate::parser::document::ParsedDocument;
use crate::storage::{
    ArtifactExtractPayload, ArtifactStatus, EnrichmentStatus, ImportedNoteMetadataWriteSet,
    JobStatus, JobType, NewArtifact, NewEnrichmentJob, NewSegment, SegmentType, WriteArtifactSet,
};

use super::ids::{new_id, sha256_hex};
use super::SourceImportSpec;

pub(super) fn build_document_artifact_set(
    import_id: &str,
    content_facets_json: &str,
    spec: SourceImportSpec,
    document: ParsedDocument,
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
