use postgres::Client;

use crate::error::{StorageError, StorageResult};
use crate::storage::retrieval_read_store::{
    ArchiveSearchCandidate, ArtifactContextDerivedObject, ArtifactContextEvidenceLink,
    ArtifactContextPackMaterial, ArtifactDetailDerivedObject, ArtifactDetailRecord,
    ArtifactDetailSegment, ArtifactDetailView, SearchCandidateKind,
};
use crate::storage::types::{
    DerivedObjectType, EnrichmentStatus, EvidenceRole, ScopeType, SourceType, SupportStrength,
};
use crate::ParticipantRole;

fn map_pg_err(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(crate::error::DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source,
    })
}

fn escape_like_query(query_text: &str) -> String {
    query_text
        .to_lowercase()
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub fn search_candidates(
    client: &mut Client,
    connection_string: &str,
    query_text: &str,
    limit: usize,
) -> StorageResult<Vec<ArchiveSearchCandidate>> {
    let like_pattern = format!("%{}%", escape_like_query(query_text));
    let limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("search limit {limit} exceeds Postgres BIGINT range"),
    })?;

    let rows = client
        .query(
            "SELECT * FROM (
                SELECT a.artifact_id AS artifact_id,
                       a.artifact_id AS match_record_id,
                       'artifact_title' AS match_kind,
                       NULL::text AS derived_object_type,
                       COALESCE(a.title, '') AS snippet,
                       300 AS score_hint
                  FROM oa_artifact a
                 WHERE lower(COALESCE(a.title, '')) LIKE $1 ESCAPE '\'
                UNION ALL
                SELECT d.artifact_id AS artifact_id,
                       d.derived_object_id AS match_record_id,
                       'derived_object' AS match_kind,
                       d.derived_object_type AS derived_object_type,
                       COALESCE(d.title, d.body_text, '') AS snippet,
                       200 AS score_hint
                  FROM oa_derived_object d
                 WHERE d.object_status = 'active'
                   AND (
                        lower(COALESCE(d.title, '')) LIKE $1 ESCAPE '\'
                        OR lower(COALESCE(d.body_text, '')) LIKE $1 ESCAPE '\'
                        OR lower(COALESCE(d.object_json::text, '')) LIKE $1 ESCAPE '\'
                   )
                UNION ALL
                SELECT s.artifact_id AS artifact_id,
                       s.segment_id AS match_record_id,
                       'segment_excerpt' AS match_kind,
                       NULL::text AS derived_object_type,
                       COALESCE(s.text_content, '') AS snippet,
                       100 AS score_hint
                  FROM oa_segment s
                 WHERE lower(COALESCE(s.text_content, '')) LIKE $1 ESCAPE '\'
            ) matches
            ORDER BY score_hint DESC, artifact_id ASC, match_record_id ASC
            LIMIT $2",
            &[&like_pattern, &limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact_id: String = row.get(0);
        let match_record_id: String = row.get(1);
        let match_kind_value: String = row.get(2);
        let snippet: String = row.get(4);
        let score_hint: i32 = row.get(5);

        let match_kind = match match_kind_value.as_str() {
            "artifact_title" => SearchCandidateKind::ArtifactTitle,
            "derived_object" => {
                let derived_object_type: String = row.get(3);
                SearchCandidateKind::DerivedObject {
                    derived_type: DerivedObjectType::from_str(&derived_object_type).ok_or_else(
                        || StorageError::InvalidDerivedObjectType {
                            artifact_id: artifact_id.clone(),
                            value: derived_object_type,
                        },
                    )?,
                }
            }
            "segment_excerpt" => SearchCandidateKind::SegmentExcerpt,
            _ => {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!("unknown search candidate kind {match_kind_value}"),
                });
            }
        };

        candidates.push(ArchiveSearchCandidate {
            artifact_id,
            match_record_id,
            match_kind,
            snippet,
            score_hint,
        });
    }

    Ok(candidates)
}

pub fn load_artifact_detail(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Option<ArtifactDetailView>> {
    let Some(artifact) = load_artifact_record(client, connection_string, artifact_id)? else {
        return Ok(None);
    };

    Ok(Some(ArtifactDetailView {
        artifact,
        segments: load_artifact_segments(client, connection_string, artifact_id)?,
        derived_objects: load_artifact_detail_derived_objects(
            client,
            connection_string,
            artifact_id,
        )?,
    }))
}

pub fn load_artifact_context_pack_material(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Option<ArtifactContextPackMaterial>> {
    let Some(artifact) = load_artifact_record(client, connection_string, artifact_id)? else {
        return Ok(None);
    };

    Ok(Some(ArtifactContextPackMaterial {
        artifact,
        segments: load_artifact_segments(client, connection_string, artifact_id)?,
        derived_objects: load_artifact_context_derived_objects(
            client,
            connection_string,
            artifact_id,
        )?,
        evidence_links: load_artifact_context_evidence_links(
            client,
            connection_string,
            artifact_id,
        )?,
    }))
}

fn load_artifact_record(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Option<ArtifactDetailRecord>> {
    let row = client
        .query_opt(
            "SELECT artifact_id, title, source_type, enrichment_status
             FROM oa_artifact
             WHERE artifact_id = $1",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let Some(row) = row else {
        return Ok(None);
    };

    let source_type: String = row.get(2);
    let enrichment_status: String = row.get(3);
    Ok(Some(ArtifactDetailRecord {
        artifact_id: row.get(0),
        title: row.get(1),
        source_type: SourceType::from_str(&source_type).ok_or_else(|| {
            StorageError::InvalidSourceType {
                artifact_id: artifact_id.to_string(),
                value: source_type,
            }
        })?,
        enrichment_status: EnrichmentStatus::from_str(&enrichment_status).ok_or_else(|| {
            StorageError::InvalidEnrichmentStatus {
                artifact_id: artifact_id.to_string(),
                value: enrichment_status,
            }
        })?,
    }))
}

fn load_artifact_segments(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactDetailSegment>> {
    let rows = client
        .query(
            "SELECT s.segment_id,
                    s.participant_id,
                    p.participant_role,
                    s.sequence_no,
                    s.text_content
             FROM oa_segment s
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id
             WHERE s.artifact_id = $1
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut segments = Vec::with_capacity(rows.len());
    for row in rows {
        let participant_id = row.get::<_, Option<String>>(1);
        let participant_role = row
            .get::<_, Option<String>>(2)
            .map(|value| {
                ParticipantRole::from_str(&value).ok_or_else(|| {
                    StorageError::InvalidParticipantRole {
                        participant_id: participant_id
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                        value,
                    }
                })
            })
            .transpose()?;

        segments.push(ArtifactDetailSegment {
            segment_id: row.get(0),
            participant_id,
            participant_role,
            sequence_no: row.get(3),
            text_content: row.get(4),
        });
    }

    Ok(segments)
}

fn load_artifact_detail_derived_objects(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactDetailDerivedObject>> {
    let rows = client
        .query(
            "SELECT derived_object_id, derived_object_type, title, body_text, confidence_score
             FROM oa_derived_object
             WHERE artifact_id = $1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut objects = Vec::with_capacity(rows.len());
    for row in rows {
        let derived_object_type: String = row.get(1);
        objects.push(ArtifactDetailDerivedObject {
            derived_object_id: row.get(0),
            derived_object_type: DerivedObjectType::from_str(&derived_object_type).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.to_string(),
                    value: derived_object_type,
                },
            )?,
            title: row.get(2),
            body_text: row.get(3),
            confidence_score: row.get(4),
        });
    }

    Ok(objects)
}

fn load_artifact_context_derived_objects(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactContextDerivedObject>> {
    let rows = client
        .query(
            "SELECT derived_object_id, derived_object_type, title, body_text, scope_id, scope_type
             FROM oa_derived_object
             WHERE artifact_id = $1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut objects = Vec::with_capacity(rows.len());
    for row in rows {
        let derived_object_type: String = row.get(1);
        objects.push(ArtifactContextDerivedObject {
            derived_object_id: row.get(0),
            derived_object_type: DerivedObjectType::from_str(&derived_object_type).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.to_string(),
                    value: derived_object_type,
                },
            )?,
            title: row.get(2),
            body_text: row.get(3),
            scope_id: row.get(4),
            scope_type: ScopeType::from_str(&row.get::<_, String>(5)).ok_or_else(|| {
                StorageError::InvalidScopeType {
                    artifact_id: artifact_id.to_string(),
                    value: row.get(5),
                }
            })?,
        });
    }

    Ok(objects)
}

fn load_artifact_context_evidence_links(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactContextEvidenceLink>> {
    let rows = client
        .query(
            "SELECT e.evidence_link_id,
                    e.derived_object_id,
                    e.segment_id,
                    e.evidence_role,
                    e.support_strength,
                    e.evidence_rank
             FROM oa_evidence_link e
             JOIN oa_derived_object d ON d.derived_object_id = e.derived_object_id
             WHERE d.artifact_id = $1 AND d.object_status = 'active'
             ORDER BY e.derived_object_id ASC, e.evidence_rank ASC, e.evidence_link_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut links = Vec::with_capacity(rows.len());
    for row in rows {
        links.push(ArtifactContextEvidenceLink {
            evidence_link_id: row.get(0),
            derived_object_id: row.get(1),
            segment_id: row.get(2),
            evidence_role: EvidenceRole::from_str(&row.get::<_, String>(3)).ok_or_else(|| {
                StorageError::InvalidEvidenceRole {
                    artifact_id: artifact_id.to_string(),
                    value: row.get(3),
                }
            })?,
            support_strength: SupportStrength::from_str(&row.get::<_, String>(4)).ok_or_else(
                || StorageError::InvalidSupportStrength {
                    artifact_id: artifact_id.to_string(),
                    value: row.get(4),
                },
            )?,
            evidence_rank: row.get(5),
        });
    }

    Ok(links)
}

#[cfg(test)]
mod tests {
    use super::escape_like_query;

    #[test]
    fn escape_like_query_uses_unicode_lowercase_and_escapes_wildcards() {
        assert_eq!(escape_like_query("Ä_%\\Test"), "ä\\_\\%\\\\test");
    }
}
