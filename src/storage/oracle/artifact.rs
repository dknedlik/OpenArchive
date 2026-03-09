use crate::error::{StorageError, StorageResult};
use oracle::Connection;

use crate::storage::types::{EnrichmentStatus, NewArtifact, NewParticipant, SourceType};

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

    Ok(row.and_then(|(artifact_id, enrichment_status)| {
        EnrichmentStatus::from_str(&enrichment_status).map(|status| (artifact_id, status))
    }))
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
