use crate::error::StorageResult;
use crate::storage::types::{
    ArtifactClass, ArtifactListItem, EnrichmentStatus, LoadedArtifactForEnrichment,
    LoadedArtifactRecord, LoadedParticipant, LoadedSegment, NewArtifact, NewParticipant,
    SourceType,
};
use crate::{ParticipantRole, SourceTimestamp, VisibilityStatus};

pub fn find_artifact_by_source_hash(
    client: &mut postgres::Client,
    source_type: SourceType,
    content_hash_version: &str,
    source_conversation_hash: &str,
) -> StorageResult<Option<(String, EnrichmentStatus)>> {
    let source_type = source_type.as_str();
    let row = client
        .query_opt(
            "SELECT artifact_id, enrichment_status \
             FROM oa_artifact \
             WHERE source_type = $1 AND content_hash_version = $2 AND source_conversation_hash = $3",
            &[&source_type, &content_hash_version, &source_conversation_hash],
        )
        .map_err(map_pg_storage_err)?;

    match row {
        Some(row) => {
            let artifact_id: String = row.get(0);
            let enrichment_status: String = row.get(1);
            let artifact_id_clone = artifact_id.clone();
            let status = EnrichmentStatus::from_str(&enrichment_status).ok_or_else(|| {
                crate::error::StorageError::InvalidEnrichmentStatus {
                    artifact_id: artifact_id_clone,
                    value: enrichment_status,
                }
            })?;
            Ok(Some((artifact_id, status)))
        }
        None => Ok(None),
    }
}

pub fn insert_artifact(client: &mut postgres::Client, a: &NewArtifact) -> StorageResult<()> {
    let artifact_class = a.artifact_class.as_str();
    let source_type = a.source_type.as_str();
    let artifact_status = a.artifact_status.as_str();
    let enrichment_status = a.enrichment_status.as_str();
    client
        .execute(
            "INSERT INTO oa_artifact \
             (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status, \
              source_conversation_key, source_conversation_hash, title, created_at_source, \
              started_at, ended_at, primary_language, content_hash_version, content_facets_json, \
              normalization_version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, \
                     $10::text::timestamptz, $11::text::timestamptz, $12::text::timestamptz, \
                     $13, $14, $15::text::jsonb, $16)",
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
                &a.created_at_source.as_ref().map(|ts| ts.as_str()),
                &a.started_at.as_ref().map(|ts| ts.as_str()),
                &a.ended_at.as_ref().map(|ts| ts.as_str()),
                &a.primary_language,
                &a.content_hash_version,
                &a.content_facets_json,
                &a.normalization_version,
            ],
        )
        .map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn insert_participant(client: &mut postgres::Client, p: &NewParticipant) -> StorageResult<()> {
    let role = p.participant_role.as_str();
    client
        .execute(
            "INSERT INTO oa_conversation_participant \
             (participant_id, artifact_id, participant_role, display_name, provider_name, \
              model_name, source_participant_key, sequence_no) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
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
        .map_err(map_pg_storage_err)?;
    Ok(())
}

pub fn list_artifacts(client: &mut postgres::Client) -> StorageResult<Vec<ArtifactListItem>> {
    let rows = client
        .query(
            "SELECT artifact_id, title, source_type, \
                    to_char(created_at_source, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), \
                    to_char(captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), \
                    enrichment_status \
             FROM oa_artifact \
             ORDER BY captured_at DESC, artifact_id ASC",
            &[],
        )
        .map_err(map_pg_storage_err)?;

    let mut artifacts = Vec::new();
    for row in rows {
        let artifact_id: String = row.get(0);
        let enrichment_status: String = row.get(5);
        let artifact_id_clone = artifact_id.clone();
        artifacts.push(ArtifactListItem {
            artifact_id,
            title: row.get(1),
            source_type: row.get(2),
            created_at_source: row.get(3),
            captured_at: row.get(4),
            enrichment_status: EnrichmentStatus::from_str(&enrichment_status).ok_or_else(|| {
                crate::error::StorageError::InvalidEnrichmentStatus {
                    artifact_id: artifact_id_clone,
                    value: enrichment_status.clone(),
                }
            })?,
        });
    }

    Ok(artifacts)
}

pub fn list_artifacts_filtered(
    client: &mut postgres::Client,
    filters: &crate::storage::types::ArtifactListFilters,
    limit: usize,
    offset: usize,
) -> StorageResult<Vec<ArtifactListItem>> {
    let source_type: Option<&str> = filters.source_type.as_ref().map(|v| v.as_str());
    let enrichment_status: Option<&str> = filters.enrichment_status.as_ref().map(|v| v.as_str());
    let captured_after: Option<&str> = filters.captured_after.as_deref();
    let captured_before: Option<&str> = filters.captured_before.as_deref();
    let limit_i64 = limit as i64;
    let offset_i64 = offset as i64;

    let rows = client
        .query(
            "SELECT artifact_id, title, source_type, \
                    to_char(created_at_source, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), \
                    to_char(captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), \
                    enrichment_status \
             FROM oa_artifact \
             WHERE ($1::text IS NULL OR source_type = $1) \
               AND ($2::text IS NULL OR enrichment_status = $2) \
               AND ($3::text IS NULL OR captured_at >= $3::timestamptz) \
               AND ($4::text IS NULL OR captured_at < $4::timestamptz) \
             ORDER BY captured_at DESC, artifact_id ASC \
             LIMIT $5 OFFSET $6",
            &[
                &source_type,
                &enrichment_status,
                &captured_after,
                &captured_before,
                &limit_i64,
                &offset_i64,
            ],
        )
        .map_err(map_pg_storage_err)?;

    let mut artifacts = Vec::new();
    for row in rows {
        let artifact_id: String = row.get(0);
        let enrichment_status_str: String = row.get(5);
        let artifact_id_clone = artifact_id.clone();
        artifacts.push(ArtifactListItem {
            artifact_id,
            title: row.get(1),
            source_type: row.get(2),
            created_at_source: row.get(3),
            captured_at: row.get(4),
            enrichment_status: EnrichmentStatus::from_str(&enrichment_status_str).ok_or_else(
                || crate::error::StorageError::InvalidEnrichmentStatus {
                    artifact_id: artifact_id_clone,
                    value: enrichment_status_str.clone(),
                },
            )?,
        });
    }

    Ok(artifacts)
}

pub fn get_timeline(
    client: &mut postgres::Client,
    filters: &crate::storage::types::TimelineFilters,
    limit: usize,
    offset: usize,
) -> StorageResult<Vec<crate::storage::types::TimelineEntry>> {
    let keyword: Option<&str> = filters.keyword.as_deref();
    let source_type: Option<&str> = filters.source_type.as_ref().map(|v| v.as_str());
    let limit_i64 = limit as i64;
    let offset_i64 = offset as i64;

    let rows = client
        .query(
            "SELECT a.artifact_id, a.title, a.source_type, \
                    to_char(a.created_at_source, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), \
                    to_char(a.captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'), \
                    a.enrichment_status, \
                    (SELECT d.body_text FROM oa_derived_object d \
                     WHERE d.artifact_id = a.artifact_id \
                       AND d.derived_object_type = 'summary' \
                       AND d.object_status = 'active' \
                     ORDER BY d.confidence_score DESC NULLS LAST \
                     LIMIT 1) \
             FROM oa_artifact a \
             WHERE ($1::text IS NULL OR a.title_tsv @@ plainto_tsquery('english', $1)) \
               AND ($2::text IS NULL OR a.source_type = $2) \
             ORDER BY COALESCE(a.created_at_source, a.captured_at) ASC \
             LIMIT $3 OFFSET $4",
            &[&keyword, &source_type, &limit_i64, &offset_i64],
        )
        .map_err(map_pg_storage_err)?;

    let mut entries = Vec::new();
    for row in rows {
        entries.push(crate::storage::types::TimelineEntry {
            artifact_id: row.get(0),
            title: row.get(1),
            source_type: row.get(2),
            created_at_source: row.get(3),
            captured_at: row.get(4),
            enrichment_status: row.get(5),
            summary_snippet: row.get(6),
        });
    }

    Ok(entries)
}

pub fn load_artifact_for_enrichment(
    client: &mut postgres::Client,
    artifact_id: &str,
) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
    let artifact_row = client
        .query_opt(
            "SELECT artifact_id, import_id, artifact_class, source_type, title \
             FROM oa_artifact WHERE artifact_id = $1",
            &[&artifact_id],
        )
        .map_err(map_pg_storage_err)?;

    let Some(artifact_row) = artifact_row else {
        return Ok(None);
    };

    let artifact_id_value: String = artifact_row.get(0);
    let import_id: String = artifact_row.get(1);
    let artifact_class_str: String = artifact_row.get(2);
    let artifact_class = ArtifactClass::from_str(&artifact_class_str).ok_or_else(|| {
        crate::error::StorageError::InvalidArtifactClass {
            artifact_id: artifact_id_value.clone(),
            value: artifact_class_str,
        }
    })?;
    let source_type_str: String = artifact_row.get(3);
    let source_type = SourceType::from_str(&source_type_str).ok_or_else(|| {
        crate::error::StorageError::InvalidSourceType {
            artifact_id: artifact_id_value.clone(),
            value: source_type_str,
        }
    })?;
    let artifact = LoadedArtifactRecord {
        artifact_id: artifact_id_value.clone(),
        import_id,
        artifact_class,
        source_type,
        title: artifact_row.get(4),
    };

    let participant_rows = client
        .query(
            "SELECT participant_id, participant_role, display_name, source_participant_key \
             FROM oa_conversation_participant \
             WHERE artifact_id = $1 \
             ORDER BY sequence_no ASC, participant_id ASC",
            &[&artifact_id],
        )
        .map_err(map_pg_storage_err)?;
    let mut participants = Vec::with_capacity(participant_rows.len());
    for row in participant_rows {
        let participant_id: String = row.get(0);
        let role: String = row.get(1);
        participants.push(LoadedParticipant {
            participant_id: participant_id.clone(),
            participant_role: ParticipantRole::from_str(&role).ok_or_else(|| {
                crate::error::StorageError::InvalidParticipantRole {
                    participant_id,
                    value: role,
                }
            })?,
            display_name: row.get(2),
            external_id: row.get(3),
        });
    }

    let segment_rows = client
        .query(
            "SELECT s.segment_id, s.participant_id, p.participant_role, s.sequence_no, s.text_content, \
                    s.created_at_source, s.visibility_status \
             FROM oa_segment s \
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id \
             WHERE s.artifact_id = $1 \
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
            &[&artifact_id],
        )
        .map_err(map_pg_storage_err)?;
    let mut segments = Vec::with_capacity(segment_rows.len());
    for row in segment_rows {
        let segment_id: String = row.get(0);
        let participant_role = row
            .get::<_, Option<String>>(2)
            .map(|value| {
                ParticipantRole::from_str(&value).ok_or_else(|| {
                    crate::error::StorageError::InvalidParticipantRole {
                        participant_id: row
                            .get::<_, Option<String>>(1)
                            .unwrap_or_else(|| "unknown".to_string()),
                        value,
                    }
                })
            })
            .transpose()?;
        let visibility_status: String = row.get(6);
        segments.push(LoadedSegment {
            segment_id: segment_id.clone(),
            participant_id: row.get(1),
            participant_role,
            sequence_no: row.get(3),
            text_content: row.get(4),
            created_at_source: row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(5)
                .map(SourceTimestamp::from),
            visibility_status: VisibilityStatus::from_str(&visibility_status).ok_or_else(|| {
                crate::error::StorageError::InvalidVisibilityStatus {
                    segment_id,
                    value: visibility_status,
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

fn map_pg_storage_err(source: postgres::Error) -> crate::error::StorageError {
    crate::error::StorageError::Db(map_postgres_error(source))
}

fn map_postgres_error(source: postgres::Error) -> crate::error::DbError {
    crate::error::DbError::ConnectPostgres {
        connection_string: "postgres".to_string(),
        source,
    }
}
