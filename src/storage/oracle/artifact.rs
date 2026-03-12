use crate::error::{StorageError, StorageResult};
use oracle::Connection;

use crate::storage::types::{
    ArtifactListItem, EnrichmentStatus, LoadedArtifactForEnrichment, LoadedArtifactRecord,
    LoadedParticipant, LoadedSegment, NewArtifact, NewParticipant, SourceType,
};
use crate::{ParticipantRole, SourceTimestamp, VisibilityStatus};

pub fn find_artifact_by_source_hash(
    conn: &Connection,
    source_type: SourceType,
    content_hash_version: &str,
    source_conversation_hash: &str,
) -> StorageResult<Option<(String, EnrichmentStatus)>> {
    let source_type = source_type.as_str();
    let row = conn
        .query_row_as::<(String, String)>(
            "SELECT artifact_id, enrichment_status \
             FROM oa_artifact \
             WHERE source_type = :1 AND content_hash_version = :2 AND source_conversation_hash = :3",
            &[&source_type, &content_hash_version, &source_conversation_hash],
        )
        .ok();

    match row {
        Some((artifact_id, enrichment_status)) => {
            let artifact_id_clone = artifact_id.clone();
            let status = EnrichmentStatus::from_str(&enrichment_status).ok_or_else(|| {
                StorageError::InvalidEnrichmentStatus {
                    artifact_id: artifact_id_clone,
                    value: enrichment_status,
                }
            })?;
            Ok(Some((artifact_id, status)))
        }
        None => Ok(None),
    }
}

pub fn insert_artifact(conn: &Connection, a: &NewArtifact) -> StorageResult<()> {
    let artifact_class = a.artifact_class.as_str();
    let source_type = a.source_type.as_str();
    let artifact_status = a.artifact_status.as_str();
    let enrichment_status = a.enrichment_status.as_str();
    let created_at_source = a.created_at_source.as_ref().map(|ts| ts.as_str());
    let started_at = a.started_at.as_ref().map(|ts| ts.as_str());
    let ended_at = a.ended_at.as_ref().map(|ts| ts.as_str());
    conn.execute(
        "INSERT INTO oa_artifact \
         (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status, \
          source_conversation_key, source_conversation_hash, title, created_at_source, \
          started_at, ended_at, primary_language, content_hash_version, content_facets_json, \
         normalization_version) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, \
                 TO_TIMESTAMP_TZ(:10, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                 TO_TIMESTAMP_TZ(:11, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                 TO_TIMESTAMP_TZ(:12, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                 :13, :14, :15, :16)",
        &[
            &a.artifact_id,
            &a.import_id,
            &artifact_class,
            &source_type,
            &artifact_status,
            &enrichment_status,
            &a.source_conversation_key,
            &a.source_conversation_hash,
            &a.title,
            &created_at_source,
            &started_at,
            &ended_at,
            &a.primary_language,
            &a.content_hash_version,
            &a.content_facets_json,
            &a.normalization_version,
        ],
    )
    .map_err(|source| StorageError::InsertArtifact {
        artifact_id: a.artifact_id.clone(),
        source,
    })?;
    Ok(())
}

pub fn insert_participant(conn: &Connection, p: &NewParticipant) -> StorageResult<()> {
    let role = p.participant_role.as_str();
    conn.execute(
        "INSERT INTO oa_conversation_participant \
         (participant_id, artifact_id, participant_role, display_name, provider_name, \
          model_name, source_participant_key, sequence_no) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8)",
        &[
            &p.participant_id,
            &p.artifact_id,
            &role,
            &p.display_name,
            &p.provider_name,
            &p.model_name,
            &p.source_participant_key,
            &p.sequence_no,
        ],
    )
    .map_err(|source| StorageError::InsertParticipant {
        participant_id: p.participant_id.clone(),
        artifact_id: p.artifact_id.clone(),
        source,
    })?;
    Ok(())
}

pub fn list_artifacts(conn: &Connection) -> StorageResult<Vec<ArtifactListItem>> {
    let rows = conn
        .query(
            "SELECT artifact_id, \
                    title, \
                    source_type, \
                    TO_CHAR(created_at_source, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                    TO_CHAR(captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                    enrichment_status \
             FROM oa_artifact \
             ORDER BY captured_at DESC, artifact_id ASC",
            &[],
        )
        .map_err(|source| StorageError::ListArtifacts { source })?;

    let mut artifacts = Vec::new();
    for row_result in rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts { source })?;
        let artifact_id: String = row
            .get(0)
            .map_err(|source| StorageError::ListArtifacts { source })?;
        let enrichment_status: String = row
            .get(5)
            .map_err(|source| StorageError::ListArtifacts { source })?;

        let artifact_id_clone = artifact_id.clone();
        artifacts.push(ArtifactListItem {
            artifact_id,
            title: row
                .get(1)
                .map_err(|source| StorageError::ListArtifacts { source })?,
            source_type: row
                .get(2)
                .map_err(|source| StorageError::ListArtifacts { source })?,
            created_at_source: row
                .get(3)
                .map_err(|source| StorageError::ListArtifacts { source })?,
            captured_at: row
                .get(4)
                .map_err(|source| StorageError::ListArtifacts { source })?,
            enrichment_status: EnrichmentStatus::from_str(&enrichment_status).ok_or_else(|| {
                StorageError::InvalidEnrichmentStatus {
                    artifact_id: artifact_id_clone,
                    value: enrichment_status.clone(),
                }
            })?,
        });
    }

    Ok(artifacts)
}

pub fn load_artifact_for_enrichment(
    conn: &Connection,
    artifact_id: &str,
) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
    let artifact_row = conn
        .query_row_as::<(String, String, String, Option<String>)>(
            "SELECT artifact_id, import_id, source_type, title \
             FROM oa_artifact WHERE artifact_id = :1",
            &[&artifact_id],
        )
        .map(Some)
        .or_else(|source| match source.kind() {
            oracle::ErrorKind::NoDataFound => Ok(None),
            _ => Err(StorageError::ListArtifacts { source }),
        })?;

    let Some((artifact_id_value, import_id, source_type_str, title)) = artifact_row else {
        return Ok(None);
    };
    let source_type = SourceType::from_str(&source_type_str).ok_or_else(|| {
        StorageError::InvalidSourceType {
            artifact_id: artifact_id_value.clone(),
            value: source_type_str,
        }
    })?;
    let artifact = LoadedArtifactRecord {
        artifact_id: artifact_id_value,
        import_id,
        source_type,
        title,
    };

    let participant_rows = conn
        .query(
            "SELECT participant_id, participant_role, display_name, source_participant_key \
             FROM oa_conversation_participant \
             WHERE artifact_id = :1 \
             ORDER BY sequence_no ASC, participant_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| StorageError::ListArtifacts { source })?;
    let mut participants = Vec::new();
    for row_result in participant_rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts { source })?;
        let participant_id: String = row.get(0).map_err(|source| StorageError::ListArtifacts { source })?;
        let role: String = row.get(1).map_err(|source| StorageError::ListArtifacts { source })?;
        participants.push(LoadedParticipant {
            participant_id: participant_id.clone(),
            participant_role: ParticipantRole::from_str(&role).ok_or_else(|| {
                StorageError::InvalidParticipantRole {
                    participant_id,
                    value: role,
                }
            })?,
            display_name: row.get(2).map_err(|source| StorageError::ListArtifacts { source })?,
            external_id: row.get(3).map_err(|source| StorageError::ListArtifacts { source })?,
        });
    }

    let segment_rows = conn
        .query(
            "SELECT s.segment_id, s.participant_id, p.participant_role, s.sequence_no, s.text_content, \
                    TO_CHAR(s.created_at_source, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), s.visibility_status \
             FROM oa_segment s \
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id \
             WHERE s.artifact_id = :1 \
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| StorageError::ListArtifacts { source })?;
    let mut segments = Vec::new();
    for row_result in segment_rows {
        let row = row_result.map_err(|source| StorageError::ListArtifacts { source })?;
        let segment_id: String = row.get(0).map_err(|source| StorageError::ListArtifacts { source })?;
        let participant_id: Option<String> =
            row.get(1).map_err(|source| StorageError::ListArtifacts { source })?;
        let participant_role_raw: Option<String> =
            row.get(2).map_err(|source| StorageError::ListArtifacts { source })?;
        let participant_role = participant_role_raw
            .map(|value| {
                ParticipantRole::from_str(&value).ok_or_else(|| StorageError::InvalidParticipantRole {
                    participant_id: participant_id.clone().unwrap_or_else(|| "unknown".to_string()),
                    value,
                })
            })
            .transpose()?;
        let created_at_source_raw: Option<String> =
            row.get(5).map_err(|source| StorageError::ListArtifacts { source })?;
        let visibility_status_raw: String =
            row.get(6).map_err(|source| StorageError::ListArtifacts { source })?;

        segments.push(LoadedSegment {
            segment_id: segment_id.clone(),
            participant_id,
            participant_role,
            sequence_no: row.get(3).map_err(|source| StorageError::ListArtifacts { source })?,
            text_content: row.get(4).map_err(|source| StorageError::ListArtifacts { source })?,
            created_at_source: created_at_source_raw
                .map(|value| SourceTimestamp::parse_rfc3339(&value))
                .transpose()
                .map_err(|err| StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "failed to parse created_at_source for segment {}: {}",
                        segment_id, err
                    ),
                })?,
            visibility_status: VisibilityStatus::from_str(&visibility_status_raw).ok_or_else(|| {
                StorageError::InvalidVisibilityStatus {
                    segment_id,
                    value: visibility_status_raw,
                }
            })?,
        });
    }

    Ok(Some(LoadedArtifactForEnrichment {
        artifact,
        participants,
        segments,
    }))
}
