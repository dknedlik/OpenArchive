use std::collections::HashMap;

use crate::domain::SourceTimestamp;
use crate::error::OpenArchiveError;
use crate::parser::{ParsedConversation, ParsedMessage};
use crate::storage::{
    ArtifactExtractPayload, ArtifactStatus, EnrichmentStatus, ImportedNoteMetadataWriteSet,
    JobStatus, JobType, NewArtifact, NewEnrichmentJob, NewParticipant, NewSegment, SegmentType,
    WriteArtifactSet,
};

use super::ids::{new_id, sha256_hex};
use super::SourceImportSpec;

pub(super) fn build_artifact_set(
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
