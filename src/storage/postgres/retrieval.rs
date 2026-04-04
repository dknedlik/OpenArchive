use postgres::Client;

use crate::error::{StorageError, StorageResult};
use crate::storage::postgres::{embedding::vector_literal, imported_note};
use crate::storage::retrieval_read_store::{
    ArchiveSearchCandidate, ArtifactContextDerivedObject, ArtifactContextEvidenceLink,
    ArtifactContextPackMaterial, ArtifactDetailDerivedObject, ArtifactDetailRecord,
    ArtifactDetailSegment, ArtifactDetailView, DerivedObjectSearchResult, GraphRelatedEntry,
    ObjectSearchFilters, RelatedDerivedObject, SearchCandidateKind, SearchFilters,
};
use crate::storage::types::{
    DerivedObjectType, EnrichmentStatus, EvidenceRole, RetrievalIntent, RetrievedContextItem,
    ScopeType, SourceType, SupportStrength,
};
use crate::ParticipantRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchQueryMode {
    Plain,
    Operator,
}

impl SearchQueryMode {
    fn detect(query: &str) -> Self {
        let has_symbol_operator = query.contains('"')
            || query.contains('-')
            || query.contains('|')
            || query.contains('&')
            || query.contains('!')
            || query.contains('(')
            || query.contains(')')
            || query.contains('<')
            || query.contains('>');
        if has_symbol_operator
            || query
                .split_whitespace()
                .any(|token| matches!(token, "OR" | "AND" | "NOT" | "or" | "and" | "not"))
        {
            Self::Operator
        } else {
            Self::Plain
        }
    }
}

fn map_pg_err(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(crate::error::DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source: Box::new(source),
    })
}

#[cfg(test)]
fn escape_like_query(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars().flat_map(char::to_lowercase) {
        match ch {
            '%' | '_' | '\\' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn tsquery_sql(param_idx: usize, mode: SearchQueryMode) -> String {
    match mode {
        SearchQueryMode::Operator => {
            format!("websearch_to_tsquery('english', ${param_idx})")
        }
        SearchQueryMode::Plain => format!(
            "to_tsquery('english', replace(nullif(plainto_tsquery('english', ${param_idx})::text, ''), ' & ', ' | '))"
        ),
    }
}

pub fn search_candidates(
    client: &mut Client,
    connection_string: &str,
    query_text: &str,
    limit: usize,
    filters: &SearchFilters,
) -> StorageResult<Vec<ArchiveSearchCandidate>> {
    let limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("search limit {limit} exceeds Postgres BIGINT range"),
    })?;
    let query_mode = SearchQueryMode::detect(query_text);
    let tsquery = tsquery_sql(1, query_mode);

    // Build dynamic params list. $1 is always query_text, $2 is always limit.
    let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
    params.push(&query_text);
    params.push(&limit);

    let object_type_str: String;
    let source_type_str: String;
    let tag_str: String;
    let alias_str: String;
    let path_prefix_str: String;

    // Precompute string values for filter params so they live long enough.
    if let Some(ref ot) = filters.object_type {
        object_type_str = ot.as_str().to_string();
    } else {
        object_type_str = String::new();
    }
    if let Some(ref st) = filters.source_type {
        source_type_str = st.as_str().to_string();
    } else {
        source_type_str = String::new();
    }
    if let Some(ref tag) = filters.tag {
        tag_str = tag.to_lowercase();
    } else {
        tag_str = String::new();
    }
    if let Some(ref alias) = filters.alias {
        alias_str = alias.to_lowercase();
    } else {
        alias_str = String::new();
    }
    if let Some(ref path_prefix) = filters.path_prefix {
        path_prefix_str = path_prefix.to_lowercase();
    } else {
        path_prefix_str = String::new();
    }

    let mut next_param = 3usize;
    let object_type_param: Option<usize> = if filters.object_type.is_some() {
        let idx = next_param;
        params.push(&object_type_str);
        next_param += 1;
        Some(idx)
    } else {
        None
    };
    let source_type_param: Option<usize> = if filters.source_type.is_some() {
        let idx = next_param;
        params.push(&source_type_str);
        next_param += 1;
        Some(idx)
    } else {
        None
    };
    let tag_param: Option<usize> = if filters.tag.is_some() {
        let idx = next_param;
        params.push(&tag_str);
        next_param += 1;
        Some(idx)
    } else {
        None
    };
    let alias_param: Option<usize> = if filters.alias.is_some() {
        let idx = next_param;
        params.push(&alias_str);
        next_param += 1;
        Some(idx)
    } else {
        None
    };
    let path_prefix_param: Option<usize> = if filters.path_prefix.is_some() {
        let idx = next_param;
        params.push(&path_prefix_str);
        Some(idx)
    } else {
        None
    };

    // When object_type filter is set, only include derived_object branch.
    let include_non_derived = filters.object_type.is_none();

    let mut union_branches: Vec<String> = Vec::new();

    if include_non_derived {
        let mut artifact_filter = String::new();
        if let Some(idx) = source_type_param {
            artifact_filter.push_str(&format!(" AND a.source_type = ${idx}"));
        }
        if let Some(idx) = tag_param {
            artifact_filter.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = a.artifact_id AND t.normalized_tag = ${idx})"
            ));
        }
        if let Some(idx) = alias_param {
            artifact_filter.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM oa_artifact_note_alias na WHERE na.artifact_id = a.artifact_id AND na.normalized_alias = ${idx})"
            ));
        }
        if let Some(idx) = path_prefix_param {
            artifact_filter.push_str(&format!(
                " AND lower(coalesce(a.source_conversation_key, '')) LIKE (${} || '%')",
                idx
            ));
        }

        // Artifact title branch
        union_branches.push(format!(
            "SELECT a.artifact_id AS artifact_id,
                    a.artifact_id AS match_record_id,
                    'artifact_title' AS match_kind,
                    NULL::text AS derived_object_type,
                    COALESCE(a.title, '') AS snippet,
                    240 + GREATEST(0, FLOOR(ts_rank_cd(a.title_tsv, {tsquery}) * 100)::int) AS score_hint
               FROM oa_artifact a
              WHERE a.title_tsv @@ {tsquery}{artifact_filter}"
        ));
        union_branches.push(format!(
            "SELECT a.artifact_id AS artifact_id,
                    t.artifact_note_tag_id AS match_record_id,
                    'imported_note_tag' AS match_kind,
                    NULL::text AS derived_object_type,
                    t.raw_tag AS snippet,
                    380 AS score_hint
               FROM oa_artifact_note_tag t
               JOIN oa_artifact a ON a.artifact_id = t.artifact_id
              WHERE t.normalized_tag = lower($1){artifact_filter}"
        ));
        union_branches.push(format!(
            "SELECT a.artifact_id AS artifact_id,
                    na.artifact_note_alias_id AS match_record_id,
                    'imported_note_alias' AS match_kind,
                    NULL::text AS derived_object_type,
                    na.alias_text AS snippet,
                    370 AS score_hint
               FROM oa_artifact_note_alias na
               JOIN oa_artifact a ON a.artifact_id = na.artifact_id
              WHERE na.normalized_alias = lower($1){artifact_filter}"
        ));
        union_branches.push(format!(
            "SELECT a.artifact_id AS artifact_id,
                    a.artifact_id AS match_record_id,
                    'imported_note_path' AS match_kind,
                    NULL::text AS derived_object_type,
                    COALESCE(a.source_conversation_key, '') AS snippet,
                    220 AS score_hint
               FROM oa_artifact a
              WHERE lower(coalesce(a.source_conversation_key, '')) LIKE ('%' || lower($1) || '%'){artifact_filter}"
        ));
        union_branches.push(format!(
            "SELECT a.artifact_id AS artifact_id,
                    nl.artifact_note_link_id AS match_record_id,
                    'imported_external_link' AS match_kind,
                    NULL::text AS derived_object_type,
                    COALESCE(nl.display_text, nl.external_url, nl.raw_target, '') AS snippet,
                    CASE
                        WHEN lower(coalesce(nl.external_url, '')) = lower($1) THEN 395
                        WHEN lower(coalesce(nl.external_url, '')) LIKE ('%' || lower($1) || '%') THEN 390
                        WHEN lower(coalesce(nl.display_text, '')) LIKE ('%' || lower($1) || '%') THEN 250
                        ELSE 230
                    END AS score_hint
               FROM oa_artifact_note_link nl
               JOIN oa_artifact a ON a.artifact_id = nl.artifact_id
              WHERE nl.resolution_status = 'external'
                AND (
                    lower(coalesce(nl.external_url, '')) LIKE ('%' || lower($1) || '%')
                    OR lower(coalesce(nl.display_text, '')) LIKE ('%' || lower($1) || '%')
                    OR lower(coalesce(nl.raw_target, '')) LIKE ('%' || lower($1) || '%')
                ){artifact_filter}"
        ));
    }

    // Derived object branch (always included)
    {
        let mut derived_extra = String::new();
        if let Some(idx) = object_type_param {
            derived_extra.push_str(&format!(" AND d.derived_object_type = ${idx}"));
        }
        if let Some(idx) = source_type_param {
            derived_extra.push_str(&format!(
                " AND d.artifact_id IN (SELECT a2.artifact_id FROM oa_artifact a2 WHERE a2.source_type = ${idx})"
            ));
        }
        if let Some(idx) = tag_param {
            derived_extra.push_str(&format!(
                " AND d.artifact_id IN (SELECT a2.artifact_id FROM oa_artifact_note_tag a2 WHERE a2.normalized_tag = ${idx})"
            ));
        }
        if let Some(idx) = alias_param {
            derived_extra.push_str(&format!(
                " AND d.artifact_id IN (SELECT a3.artifact_id FROM oa_artifact_note_alias a3 WHERE a3.normalized_alias = ${idx})"
            ));
        }
        if let Some(idx) = path_prefix_param {
            derived_extra.push_str(&format!(
                " AND d.artifact_id IN (SELECT a4.artifact_id FROM oa_artifact a4 WHERE lower(coalesce(a4.source_conversation_key, '')) LIKE (${} || '%'))",
                idx
            ));
        }
        union_branches.push(format!(
            "SELECT d.artifact_id AS artifact_id,
                    d.derived_object_id AS match_record_id,
                    'derived_object' AS match_kind,
                    d.derived_object_type AS derived_object_type,
                    COALESCE(d.title, d.body_text, '') AS snippet,
                    360 + GREATEST(0, FLOOR(ts_rank_cd(d.search_tsv, {tsquery}) * 100)::int) AS score_hint
               FROM oa_derived_object d
              WHERE d.object_status = 'active'
                AND d.search_tsv @@ {tsquery}{derived_extra}"
        ));
    }

    if include_non_derived {
        // Segment excerpt branch
        let mut source_join = String::new();
        if let Some(idx) = source_type_param {
            source_join.push_str(&format!(
                " AND s.artifact_id IN (SELECT a3.artifact_id FROM oa_artifact a3 WHERE a3.source_type = ${idx})"
            ));
        }
        if let Some(idx) = tag_param {
            source_join.push_str(&format!(
                " AND s.artifact_id IN (SELECT a4.artifact_id FROM oa_artifact_note_tag a4 WHERE a4.normalized_tag = ${idx})"
            ));
        }
        if let Some(idx) = alias_param {
            source_join.push_str(&format!(
                " AND s.artifact_id IN (SELECT a5.artifact_id FROM oa_artifact_note_alias a5 WHERE a5.normalized_alias = ${idx})"
            ));
        }
        if let Some(idx) = path_prefix_param {
            source_join.push_str(&format!(
                " AND s.artifact_id IN (SELECT a6.artifact_id FROM oa_artifact a6 WHERE lower(coalesce(a6.source_conversation_key, '')) LIKE (${} || '%'))",
                idx
            ));
        }
        union_branches.push(format!(
            "SELECT s.artifact_id AS artifact_id,
                    s.segment_id AS match_record_id,
                    'segment_excerpt' AS match_kind,
                    NULL::text AS derived_object_type,
                    COALESCE(s.text_content, '') AS snippet,
                    300 + GREATEST(0, FLOOR(ts_rank_cd(s.text_content_tsv, {tsquery}) * 100)::int) AS score_hint
               FROM oa_segment s
              WHERE s.text_content_tsv @@ {tsquery}{source_join}"
        ));
    }

    let sql = format!(
        "SELECT * FROM ({}) matches ORDER BY score_hint DESC, artifact_id ASC, match_record_id ASC LIMIT $2",
        union_branches.join(" UNION ALL ")
    );

    let rows = client
        .query(&sql, &params)
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
            "imported_note_tag" => SearchCandidateKind::ImportedNoteTag,
            "imported_note_alias" => SearchCandidateKind::ImportedNoteAlias,
            "imported_note_path" => SearchCandidateKind::ImportedNotePath,
            "imported_external_link" => SearchCandidateKind::ImportedExternalLink,
            "derived_object" => {
                let derived_object_type: String = row.get(3);
                SearchCandidateKind::DerivedObject {
                    derived_type: DerivedObjectType::parse(&derived_object_type).ok_or_else(
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

pub fn retrieve_for_intents(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
    intents: &[RetrievalIntent],
    limit_per_intent: usize,
) -> StorageResult<Vec<RetrievedContextItem>> {
    let limit =
        i64::try_from(limit_per_intent).map_err(|_| StorageError::InvalidDerivationWrite {
            detail: format!("retrieval limit {limit_per_intent} exceeds Postgres BIGINT range"),
        })?;
    let mut items = Vec::new();

    for intent in intents {
        let query_text = intent.query_text.trim();
        if query_text.is_empty() {
            continue;
        }
        let query_mode = SearchQueryMode::detect(query_text);
        let tsquery = tsquery_sql(2, query_mode);

        let derived_type_filter = match intent.intent_type.as_str() {
            "memory_match" => "AND d.derived_object_type = 'memory'",
            "relationship_lookup" => "AND d.derived_object_type = 'relationship'",
            "topic_lookup" => "AND d.derived_object_type IN ('classification', 'summary')",
            "contradiction_check" => "AND d.derived_object_type IN ('memory', 'relationship')",
            "entity_lookup" => "AND d.derived_object_type IN ('entity', 'memory', 'relationship', 'classification', 'summary')",
            _ => "",
        };
        let rows = client
            .query(
                &format!(
                    "SELECT d.derived_object_type,
                            d.derived_object_id,
                            d.artifact_id,
                            d.title,
                            d.body_text,
                            COALESCE(array_agg(e.segment_id ORDER BY e.evidence_rank)
                                FILTER (WHERE e.segment_id IS NOT NULL), ARRAY[]::text[]) AS segment_ids,
                            (to_tsvector('english', COALESCE(d.title, '')) @@ {tsquery}) AS title_hit,
                            (to_tsvector('english', COALESCE(d.body_text, '')) @@ {tsquery}) AS body_hit,
                            ts_rank_cd(d.search_tsv, {tsquery}) AS rank_score
                     FROM oa_derived_object d
                     LEFT JOIN oa_evidence_link e ON e.derived_object_id = d.derived_object_id
                     WHERE d.artifact_id <> $1
                       AND d.object_status = 'active'
                       {derived_type_filter}
                       AND d.search_tsv @@ {tsquery}
                     GROUP BY d.derived_object_type, d.derived_object_id, d.artifact_id, d.title, d.body_text, d.search_tsv
                     ORDER BY rank_score DESC, d.derived_object_id ASC
                     LIMIT $3"
                ),
                &[&artifact_id, &query_text, &limit],
            )
            .map_err(|source| map_pg_err(connection_string, source))?;

        for row in rows {
            let mut matched_fields = Vec::new();
            if row.get::<_, bool>(6) {
                matched_fields.push("title".to_string());
            }
            if row.get::<_, bool>(7) {
                matched_fields.push("body_text".to_string());
            }

            items.push(RetrievedContextItem {
                item_type: row.get::<_, String>(0),
                object_id: row.get(1),
                artifact_id: row.get(2),
                title: row.get(3),
                body_text: row.get(4),
                supporting_segment_ids: row.get::<_, Vec<String>>(5),
                retrieval_reason: intent.question.clone(),
                matched_fields,
                rank_score: (row.get::<_, f32>(8) * 1000.0) as i32,
            });
        }
    }

    Ok(items)
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
        imported_note_metadata: imported_note::load_imported_note_metadata(
            client,
            connection_string,
            artifact_id,
            artifact.note_path.clone(),
        )?,
        inbound_note_links: imported_note::load_inbound_note_links(
            client,
            connection_string,
            artifact_id,
        )?,
        segments: load_artifact_segments(client, connection_string, artifact_id)?,
        artifact,
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
    let note_path = artifact.note_path.clone();

    Ok(Some(ArtifactContextPackMaterial {
        artifact,
        segments: load_artifact_segments(client, connection_string, artifact_id)?,
        imported_note_metadata: imported_note::load_imported_note_metadata(
            client,
            connection_string,
            artifact_id,
            note_path,
        )?,
        inbound_note_links: imported_note::load_inbound_note_links(
            client,
            connection_string,
            artifact_id,
        )?,
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
            "SELECT artifact_id, title, source_type, enrichment_status, source_conversation_key
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
        source_type: SourceType::parse(&source_type).ok_or_else(|| {
            StorageError::InvalidSourceType {
                artifact_id: artifact_id.to_string(),
                value: source_type,
            }
        })?,
        enrichment_status: EnrichmentStatus::parse(&enrichment_status).ok_or_else(|| {
            StorageError::InvalidEnrichmentStatus {
                artifact_id: artifact_id.to_string(),
                value: enrichment_status,
            }
        })?,
        note_path: row.get(4),
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
                ParticipantRole::parse(&value).ok_or_else(|| StorageError::InvalidParticipantRole {
                    participant_id: participant_id
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string()),
                    value,
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
            "SELECT derived_object_id, derived_object_type, title, body_text,
                    confidence_score::double precision AS confidence_score
             FROM oa_derived_object
             WHERE artifact_id = $1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
            &[&artifact_id],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut objects = Vec::with_capacity(rows.len());
    for row in rows {
        let derived_object_id: String = row.get(0);
        let derived_object_type: String = row.get(1);
        objects.push(ArtifactDetailDerivedObject {
            derived_object_id: derived_object_id.clone(),
            derived_object_type: DerivedObjectType::parse(&derived_object_type).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.to_string(),
                    value: derived_object_type,
                },
            )?,
            title: row.get(2),
            body_text: row.get(3),
            confidence_score: row.try_get(4).map_err(|source| {
                StorageError::ReadDerivedObjectConfidenceScore {
                    artifact_id: artifact_id.to_string(),
                    derived_object_id: derived_object_id.clone(),
                    source: Box::new(source),
                }
            })?,
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
            "SELECT derived_object_id, derived_object_type, title, body_text, scope_id, scope_type, candidate_key
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
            derived_object_type: DerivedObjectType::parse(&derived_object_type).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.to_string(),
                    value: derived_object_type,
                },
            )?,
            title: row.get(2),
            body_text: row.get(3),
            scope_id: row.get(4),
            scope_type: ScopeType::parse(&row.get::<_, String>(5)).ok_or_else(|| {
                StorageError::InvalidScopeType {
                    artifact_id: artifact_id.to_string(),
                    value: row.get(5),
                }
            })?,
            candidate_key: row.get(6),
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
            evidence_role: EvidenceRole::parse(&row.get::<_, String>(3)).ok_or_else(|| {
                StorageError::InvalidEvidenceRole {
                    artifact_id: artifact_id.to_string(),
                    value: row.get(3),
                }
            })?,
            support_strength: SupportStrength::parse(&row.get::<_, String>(4)).ok_or_else(
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

pub fn search_objects(
    client: &mut Client,
    connection_string: &str,
    filters: &ObjectSearchFilters,
    limit: usize,
) -> StorageResult<Vec<DerivedObjectSearchResult>> {
    let limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("search limit {limit} exceeds Postgres BIGINT range"),
    })?;
    let query_mode = filters.query.as_deref().map(SearchQueryMode::detect);

    let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
    let mut next_param = 1usize;

    // Pre-compute string values so they live long enough for the borrow.
    let query_str: String = filters.query.clone().unwrap_or_default();
    let object_type_str: String = filters
        .object_type
        .as_ref()
        .map(|ot| ot.as_str().to_string())
        .unwrap_or_default();
    let candidate_key_str: String = filters.candidate_key.clone().unwrap_or_default();
    let artifact_id_str: String = filters.artifact_id.clone().unwrap_or_default();

    let mut conditions = vec!["object_status = 'active'".to_string()];

    let query_param_idx: Option<usize> = if filters.query.is_some() {
        let idx = next_param;
        params.push(&query_str);
        next_param += 1;
        let tsquery = tsquery_sql(
            idx,
            query_mode.expect("query mode should exist when query exists"),
        );
        conditions.push(format!("search_tsv @@ {tsquery}"));
        Some(idx)
    } else {
        None
    };

    if filters.object_type.is_some() {
        let idx = next_param;
        params.push(&object_type_str);
        next_param += 1;
        conditions.push(format!("derived_object_type = ${idx}"));
    }

    if filters.candidate_key.is_some() {
        let idx = next_param;
        params.push(&candidate_key_str);
        next_param += 1;
        conditions.push(format!("candidate_key = ${idx}"));
    }

    if filters.artifact_id.is_some() {
        let idx = next_param;
        params.push(&artifact_id_str);
        next_param += 1;
        conditions.push(format!("artifact_id = ${idx}"));
    }

    let limit_idx = next_param;
    params.push(&limit);

    let order_by = if let Some(idx) = query_param_idx {
        format!(
            "ts_rank_cd(search_tsv, {}) DESC",
            tsquery_sql(
                idx,
                query_mode.expect("query mode should exist when query exists")
            )
        )
    } else {
        "derived_object_id ASC".to_string()
    };
    let score_sql = if let Some(idx) = query_param_idx {
        let tsquery = tsquery_sql(
            idx,
            query_mode.expect("query mode should exist when query exists"),
        );
        format!("ts_rank_cd(search_tsv, {tsquery})::real")
    } else {
        "NULL::real".to_string()
    };

    let sql = format!(
        "SELECT derived_object_id, artifact_id, derived_object_type, title, body_text, \
                candidate_key, confidence_score::double precision, {} AS score \
         FROM oa_derived_object \
         WHERE {} \
         ORDER BY {} \
         LIMIT ${limit_idx}",
        score_sql,
        conditions.join(" AND "),
        order_by
    );

    let rows = client
        .query(&sql, &params)
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let derived_object_id: String = row.get(0);
        let artifact_id: String = row.get(1);
        let derived_object_type_str: String = row.get(2);
        results.push(DerivedObjectSearchResult {
            derived_object_id: derived_object_id.clone(),
            artifact_id: artifact_id.clone(),
            derived_object_type: DerivedObjectType::parse(&derived_object_type_str).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.clone(),
                    value: derived_object_type_str,
                },
            )?,
            title: row.get(3),
            body_text: row.get(4),
            candidate_key: row.get(5),
            confidence_score: row.try_get(6).map_err(|source| {
                StorageError::ReadDerivedObjectConfidenceScore {
                    artifact_id: artifact_id.clone(),
                    derived_object_id: derived_object_id.clone(),
                    source: Box::new(source),
                }
            })?,
            score: row.get(7),
        });
    }

    Ok(results)
}

pub fn search_objects_by_embedding(
    client: &mut Client,
    connection_string: &str,
    filters: &ObjectSearchFilters,
    query_embedding: &[f32],
    limit: usize,
) -> StorageResult<Vec<DerivedObjectSearchResult>> {
    let limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("search limit {limit} exceeds Postgres BIGINT range"),
    })?;

    let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::new();
    let mut next_param = 1usize;
    // Inline the vector literal in SQL — pgvector doesn't support TEXT parameter binding.
    let embedding_literal = vector_literal(query_embedding);

    let object_type_str: String = filters
        .object_type
        .as_ref()
        .map(|ot| ot.as_str().to_string())
        .unwrap_or_default();
    let candidate_key_str: String = filters.candidate_key.clone().unwrap_or_default();
    let artifact_id_str: String = filters.artifact_id.clone().unwrap_or_default();

    let mut conditions = vec![
        "d.object_status = 'active'".to_string(),
        "d.derived_object_type IN ('summary', 'memory', 'relationship', 'entity')".to_string(),
    ];

    if filters.object_type.is_some() {
        let idx = next_param;
        params.push(&object_type_str);
        next_param += 1;
        conditions.push(format!("d.derived_object_type = ${idx}"));
    }

    if filters.candidate_key.is_some() {
        let idx = next_param;
        params.push(&candidate_key_str);
        next_param += 1;
        conditions.push(format!("d.candidate_key = ${idx}"));
    }

    if filters.artifact_id.is_some() {
        let idx = next_param;
        params.push(&artifact_id_str);
        next_param += 1;
        conditions.push(format!("d.artifact_id = ${idx}"));
    }

    let limit_idx = next_param;
    params.push(&limit);

    let sql = format!(
        "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type, d.title, d.body_text,
                d.candidate_key, d.confidence_score::double precision,
                GREATEST(0::real, (1 - (e.embedding <=> '{embedding_literal}'::vector))::real) AS score
         FROM oa_derived_object_embedding e
         JOIN oa_derived_object d ON d.derived_object_id = e.derived_object_id
         WHERE {}
         ORDER BY e.embedding <=> '{embedding_literal}'::vector ASC, d.derived_object_id ASC
         LIMIT ${limit_idx}",
        conditions.join(" AND "),
    );

    let rows = client
        .query(&sql, &params)
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let derived_object_id: String = row.get(0);
        let artifact_id: String = row.get(1);
        let derived_object_type_str: String = row.get(2);
        results.push(DerivedObjectSearchResult {
            derived_object_id: derived_object_id.clone(),
            artifact_id: artifact_id.clone(),
            derived_object_type: DerivedObjectType::parse(&derived_object_type_str).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.clone(),
                    value: derived_object_type_str,
                },
            )?,
            title: row.get(3),
            body_text: row.get(4),
            candidate_key: row.get(5),
            confidence_score: row.try_get(6).map_err(|source| {
                StorageError::ReadDerivedObjectConfidenceScore {
                    artifact_id: artifact_id.clone(),
                    derived_object_id: derived_object_id.clone(),
                    source: Box::new(source),
                }
            })?,
            score: row.get(7),
        });
    }

    Ok(results)
}

pub fn find_related_by_candidate_keys(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
    candidate_keys: &[String],
    limit: usize,
) -> StorageResult<Vec<RelatedDerivedObject>> {
    if candidate_keys.is_empty() {
        return Ok(Vec::new());
    }

    let limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("limit {limit} exceeds Postgres BIGINT range"),
    })?;

    let rows = client
        .query(
            "SELECT derived_object_id, artifact_id, derived_object_type, title, body_text,
                    candidate_key, confidence_score::double precision
             FROM oa_derived_object
             WHERE object_status = 'active'
               AND artifact_id <> $1
               AND candidate_key = ANY($2)
             ORDER BY candidate_key ASC, derived_object_id ASC
             LIMIT $3",
            &[&artifact_id, &candidate_keys, &limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let derived_object_id: String = row.get(0);
        let row_artifact_id: String = row.get(1);
        let derived_object_type_str: String = row.get(2);
        results.push(RelatedDerivedObject {
            derived_object_id: derived_object_id.clone(),
            artifact_id: row_artifact_id.clone(),
            derived_object_type: DerivedObjectType::parse(&derived_object_type_str).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: row_artifact_id.clone(),
                    value: derived_object_type_str,
                },
            )?,
            title: row.get(3),
            body_text: row.get(4),
            candidate_key: row.get(5),
            confidence_score: row.try_get(6).map_err(|source| {
                StorageError::ReadDerivedObjectConfidenceScore {
                    artifact_id: row_artifact_id,
                    derived_object_id,
                    source: Box::new(source),
                }
            })?,
        });
    }

    Ok(results)
}

pub fn get_related_objects(
    client: &mut Client,
    connection_string: &str,
    derived_object_id: &str,
    limit: usize,
) -> StorageResult<Vec<GraphRelatedEntry>> {
    let limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("limit {limit} exceeds Postgres BIGINT range"),
    })?;

    let sql = "
        SELECT d.derived_object_id, d.artifact_id, d.derived_object_type,
               d.title, d.body_text,
               'archive_link'::text AS relation_kind,
               al.link_type, al.confidence_score::double precision, al.rationale
        FROM oa_archive_link al
        JOIN oa_derived_object d
          ON d.derived_object_id = CASE
               WHEN al.source_object_id = $1 THEN al.target_object_id
               ELSE al.source_object_id END
        WHERE (al.source_object_id = $1 OR al.target_object_id = $1)
          AND d.object_status = 'active'

        UNION ALL

        SELECT DISTINCT d.derived_object_id, d.artifact_id, d.derived_object_type,
               d.title, d.body_text,
               'shared_candidate_key'::text AS relation_kind,
               NULL::text, NULL::float8, NULL::text
        FROM oa_derived_object source
        JOIN oa_derived_object d
          ON d.candidate_key = source.candidate_key
         AND d.derived_object_id != source.derived_object_id
        WHERE source.derived_object_id = $1
          AND COALESCE(source.candidate_key, '') <> ''
          AND d.object_status = 'active'

        UNION ALL

        SELECT DISTINCT d.derived_object_id, d.artifact_id, d.derived_object_type,
               d.title, d.body_text,
               'shared_evidence'::text AS relation_kind,
               NULL::text, NULL::float8, NULL::text
        FROM oa_evidence_link el1
        JOIN oa_evidence_link el2 ON el2.segment_id = el1.segment_id AND el2.derived_object_id != $1
        JOIN oa_derived_object d ON d.derived_object_id = el2.derived_object_id
        WHERE el1.derived_object_id = $1
          AND d.object_status = 'active'

        LIMIT $2
    ";

    let rows = client
        .query(sql, &[&derived_object_id, &limit])
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut results = Vec::with_capacity(rows.len());
    let mut seen = std::collections::HashSet::new();
    for row in rows {
        let obj_id: String = row.get(0);
        if !seen.insert(obj_id.clone()) {
            continue;
        }
        let artifact_id: String = row.get(1);
        let derived_object_type_str: String = row.get(2);
        results.push(GraphRelatedEntry {
            derived_object_id: obj_id,
            artifact_id: artifact_id.clone(),
            derived_object_type: DerivedObjectType::parse(&derived_object_type_str).ok_or_else(
                || StorageError::InvalidDerivedObjectType {
                    artifact_id: artifact_id.clone(),
                    value: derived_object_type_str,
                },
            )?,
            title: row.get(3),
            body_text: row.get(4),
            relation_kind: row.get(5),
            link_type: row.get(6),
            confidence_score: row.get(7),
            rationale: row.get(8),
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::{escape_like_query, tsquery_sql, SearchQueryMode};

    #[test]
    fn escape_like_query_uses_unicode_lowercase_and_escapes_wildcards() {
        assert_eq!(escape_like_query("Ä_%\\Test"), "ä\\_\\%\\\\test");
    }

    #[test]
    fn plain_query_mode_is_default_for_natural_language_search() {
        assert_eq!(
            SearchQueryMode::detect("TBI spinal fracture injury"),
            SearchQueryMode::Plain
        );
    }

    #[test]
    fn operator_query_mode_detects_explicit_search_syntax() {
        assert_eq!(
            SearchQueryMode::detect("\"brain injury\" -smoking"),
            SearchQueryMode::Operator
        );
        assert_eq!(
            SearchQueryMode::detect("tbi OR fracture"),
            SearchQueryMode::Operator
        );
    }

    #[test]
    fn plain_query_sql_uses_disjunctive_tsquery_expression() {
        assert_eq!(
            tsquery_sql(2, SearchQueryMode::Plain),
            "to_tsquery('english', replace(nullif(plainto_tsquery('english', $2)::text, ''), ' & ', ' | '))"
        );
    }

    #[test]
    fn operator_query_sql_uses_websearch_tsquery() {
        assert_eq!(
            tsquery_sql(3, SearchQueryMode::Operator),
            "websearch_to_tsquery('english', $3)"
        );
    }
}
