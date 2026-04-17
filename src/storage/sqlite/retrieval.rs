use rusqlite::{params, params_from_iter, Connection, OptionalExtension};

use crate::error::StorageError;
use crate::storage::retrieval_read_store::{
    ArchiveSearchCandidate, ArchiveSearchReadStore, ArtifactContextDerivedObject,
    ArtifactContextPackMaterial, ArtifactContextPackReadStore, ArtifactDetailDerivedObject,
    ArtifactDetailReadStore, ArtifactDetailRecord, ArtifactDetailSegment, ArtifactDetailView,
    CrossArtifactReadStore, DerivedObjectLookupStore, DerivedObjectSearchResult,
    DerivedObjectSearchStore, GraphRelatedEntry, ObjectSearchFilters, RelatedDerivedObject,
    RelatedDerivedObjectEmbeddingMatch, SearchCandidateKind, SearchFilters,
};
use crate::storage::types::{DerivedObjectType, RetrievalIntent, RetrievedContextItem};
use crate::storage::ArchiveRetrievalStore;

use super::links::{load_artifact_links, load_imported_note_metadata, load_inbound_note_links};
use super::{
    db_err, open_connection, parse_derived_object_type, parse_scope_type, parse_source_type,
    SqliteRetrievalReadStore, StorageResult,
};

/// FTS5 query mode. `Plain` rewrites a bag-of-words query into an OR of quoted
/// tokens so that adding terms broadens results instead of eliminating them
/// (matching the spirit of Postgres `plainto_tsquery` rewritten with `|`).
/// `Operator` passes the user query straight through to FTS5 so power users can
/// write phrase queries (`"exact phrase"`), explicit operators (`AND`/`OR`/`NOT`),
/// `NEAR(...)`, prefix tokens (`foo*`), and column filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SqliteSearchQueryMode {
    Plain,
    Operator,
}

pub(super) fn detect_search_query_mode(query: &str) -> SqliteSearchQueryMode {
    let trimmed = query.trim();
    // Only route to Operator mode when the query looks intentionally written
    // for FTS5 syntax. Stray punctuation in a casual query (an unbalanced `"`
    // from a half-finished phrase, a lone `(`, etc.) would otherwise crash
    // FTS5 parsing — Plain mode escapes those characters safely.
    let quote_count = trimmed.chars().filter(|c| *c == '"').count();
    let open_paren_count = trimmed.chars().filter(|c| *c == '(').count();
    let close_paren_count = trimmed.chars().filter(|c| *c == ')').count();
    let has_quoted_phrase = quote_count >= 2
        && quote_count % 2 == 0
        && trimmed.starts_with('"')
        && trimmed.ends_with('"');
    let has_balanced_parens = open_paren_count > 0 && open_paren_count == close_paren_count;
    let has_prefix = trimmed.split_whitespace().any(|tok| {
        // Trailing `*` on a bare token is FTS5 prefix syntax. Ignore standalone
        // `*` and any token containing other punctuation we already escape.
        let bytes = tok.as_bytes();
        bytes.len() >= 2
            && bytes[bytes.len() - 1] == b'*'
            && bytes[..bytes.len() - 1]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    });
    let has_operator = trimmed
        .split_whitespace()
        .any(|tok| matches!(tok, "AND" | "OR" | "NOT" | "NEAR"));
    if has_quoted_phrase || has_balanced_parens || has_prefix || has_operator {
        SqliteSearchQueryMode::Operator
    } else {
        SqliteSearchQueryMode::Plain
    }
}

/// Build an FTS5 MATCH expression from a free-text query. Returns `None` when
/// the query reduces to no usable tokens.
///
/// In `Plain` mode each whitespace-separated token is wrapped in double quotes
/// (with embedded `"` escaped as `""`) and joined with ` OR `. Quoting disables
/// FTS5 operator parsing, so punctuation in user input is treated as a plain
/// phrase rather than a syntax error.
///
/// In `Operator` mode the query is forwarded verbatim.
pub(super) fn build_fts5_match_expression(query: &str) -> Option<String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    match detect_search_query_mode(trimmed) {
        SqliteSearchQueryMode::Operator => Some(trimmed.to_string()),
        SqliteSearchQueryMode::Plain => {
            let tokens: Vec<String> = trimmed
                .split_whitespace()
                .filter(|tok| !tok.is_empty())
                .map(|tok| format!("\"{}\"", tok.replace('"', "\"\"")))
                .collect();
            if tokens.is_empty() {
                None
            } else {
                Some(tokens.join(" OR "))
            }
        }
    }
}

/// Convert FTS5 `bm25()` (smaller / more negative is better) into a non-negative
/// integer bonus to add on top of a `score_hint` base. Mirrors the
/// `base + GREATEST(0, FLOOR(rank * scale))` shape used by the Postgres path.
pub(super) fn fts5_bm25_score_bonus(bm25: f64) -> i32 {
    let scaled = (-bm25) * 10.0;
    if !scaled.is_finite() || scaled <= 0.0 {
        0
    } else {
        scaled.round().min(i32::MAX as f64) as i32
    }
}

pub(super) fn artifact_filter_conditions(
    filters: &SearchFilters,
    artifact_alias: &str,
    values: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();
    if let Some(source_type) = filters.source_type {
        conditions.push(format!("{artifact_alias}.source_type = ?"));
        values.push(source_type.as_str().to_string());
    }
    if let Some(tag) = filters.tag.as_ref() {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = {artifact_alias}.artifact_id AND t.normalized_tag = ?)"
        ));
        values.push(tag.to_lowercase());
    }
    if let Some(alias) = filters.alias.as_ref() {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM oa_artifact_note_alias na WHERE na.artifact_id = {artifact_alias}.artifact_id AND na.normalized_alias = ?)"
        ));
        values.push(alias.to_lowercase());
    }
    if let Some(path_prefix) = filters.path_prefix.as_ref() {
        conditions.push(format!(
            "lower(coalesce({artifact_alias}.source_conversation_key, '')) LIKE ?"
        ));
        values.push(format!("{}%", path_prefix.to_lowercase()));
    }
    if conditions.is_empty() {
        "1=1".to_string()
    } else {
        conditions.join(" AND ")
    }
}

pub(super) fn load_artifact_record(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Option<ArtifactDetailRecord>> {
    use super::parse_enrichment_status;

    connection
        .query_row(
            "SELECT artifact_id, title, source_type, enrichment_status, source_conversation_key
             FROM oa_artifact
             WHERE artifact_id = ?1",
            [artifact_id],
            |row| {
                Ok(ArtifactDetailRecord {
                    artifact_id: row.get(0)?,
                    title: row.get(1)?,
                    source_type: parse_source_type(row.get(2)?)?,
                    enrichment_status: parse_enrichment_status(row.get(3)?)?,
                    note_path: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact record {artifact_id}: {source}"),
        })
}

pub(super) fn load_artifact_segments(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactDetailSegment>> {
    use super::parse_participant_role;

    let mut stmt = connection
        .prepare(
            "SELECT s.segment_id, s.participant_id, p.participant_role, s.sequence_no, s.text_content
             FROM oa_segment s
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id
             WHERE s.artifact_id = ?1
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare artifact segments query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            let participant_id: Option<String> = row.get(1)?;
            Ok(ArtifactDetailSegment {
                segment_id: row.get(0)?,
                participant_id: participant_id.clone(),
                participant_role: row
                    .get::<_, Option<String>>(2)?
                    .map(parse_participant_role)
                    .transpose()?,
                sequence_no: row.get(3)?,
                text_content: row.get(4)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact segments: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map artifact segments: {source}"),
        })
}

pub(super) fn load_artifact_detail_derived_objects(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactDetailDerivedObject>> {
    let mut stmt = connection
        .prepare(
            "SELECT derived_object_id, derived_object_type, title, body_text, confidence_score
             FROM oa_derived_object
             WHERE artifact_id = ?1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare artifact derived objects query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            Ok(ArtifactDetailDerivedObject {
                derived_object_id: row.get(0)?,
                derived_object_type: parse_derived_object_type(row.get(1)?)?,
                title: row.get(2)?,
                body_text: row.get(3)?,
                confidence_score: row.get(4)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact derived objects: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map artifact derived objects: {source}"),
        })
}

pub(super) fn load_artifact_context_derived_objects(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactContextDerivedObject>> {
    let mut stmt = connection
        .prepare(
            "SELECT derived_object_id, derived_object_type, title, body_text, scope_id, scope_type, candidate_key
             FROM oa_derived_object
             WHERE artifact_id = ?1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare context derived objects query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            Ok(ArtifactContextDerivedObject {
                derived_object_id: row.get(0)?,
                derived_object_type: parse_derived_object_type(row.get(1)?)?,
                title: row.get(2)?,
                body_text: row.get(3)?,
                scope_id: row.get(4)?,
                scope_type: parse_scope_type(row.get(5)?)?,
                candidate_key: row.get(6)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load context derived objects: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map context derived objects: {source}"),
        })
}

impl ArchiveRetrievalStore for SqliteRetrievalReadStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        let connection = open_connection(&self.config)?;
        let mut items = Vec::new();
        for intent in intents {
            let query_text = intent.query_text.trim().to_lowercase();
            if query_text.is_empty() {
                continue;
            }
            let pattern = format!("%{query_text}%");
            let derived_type_filter = match intent.intent_type.as_str() {
                "memory_match" => Some("memory"),
                "relationship_lookup" => Some("relationship"),
                "topic_lookup" => None,
                "contradiction_check" => None,
                "entity_lookup" => None,
                _ => None,
            };
            let mut stmt = connection
                .prepare(
                    "SELECT derived_object_type, derived_object_id, artifact_id, title, body_text,
                            CASE
                                WHEN lower(coalesce(title, '')) LIKE ?2 THEN 'title'
                                ELSE 'body_text'
                            END
                     FROM oa_derived_object
                     WHERE artifact_id <> ?1
                       AND object_status = 'active'
                       AND (?3 IS NULL OR derived_object_type = ?3)
                       AND (
                            lower(coalesce(title, '')) LIKE ?2 OR
                            lower(coalesce(body_text, '')) LIKE ?2
                       )
                     ORDER BY confidence_score DESC, derived_object_id ASC
                     LIMIT ?4",
                )
                .map_err(|source| StorageError::InvalidDerivationWrite {
                    detail: format!("failed to prepare retrieval query: {source}"),
                })?;
            let rows = stmt
                .query_map(
                    params![
                        artifact_id,
                        pattern,
                        derived_type_filter,
                        limit_per_intent as i64
                    ],
                    |row| {
                        Ok(RetrievedContextItem {
                            item_type: row.get(0)?,
                            object_id: row.get(1)?,
                            artifact_id: row.get(2)?,
                            title: row.get(3)?,
                            body_text: row.get(4)?,
                            supporting_segment_ids: Vec::new(),
                            retrieval_reason: intent.question.clone(),
                            matched_fields: vec![row.get(5)?],
                            rank_score: 1000,
                        })
                    },
                )
                .map_err(|source| StorageError::InvalidDerivationWrite {
                    detail: format!("failed to load retrieval items: {source}"),
                })?;
            items.extend(rows.collect::<Result<Vec<_>, _>>().map_err(|source| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("failed to map retrieval items: {source}"),
                }
            })?);
        }
        Ok(items)
    }
}

impl ArchiveSearchReadStore for SqliteRetrievalReadStore {
    fn search_candidates(
        &self,
        query_text: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> StorageResult<Vec<ArchiveSearchCandidate>> {
        let query_text = query_text.trim();
        if query_text.is_empty() {
            return Ok(Vec::new());
        }

        let connection = open_connection(&self.config)?;
        // Substring pattern is used by the lexical fallbacks for
        // tag / alias / note-path matches (those columns aren't FTS-indexed).
        let pattern = format!("%{}%", query_text.to_lowercase());
        let fts_match = build_fts5_match_expression(query_text);
        let mut candidates = Vec::new();

        if let Some(match_expr) = fts_match.as_ref() {
            let mut values: Vec<String> = vec![match_expr.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, a.title, bm25(oa_artifact_fts) AS rank_score
                 FROM oa_artifact_fts
                 JOIN oa_artifact a ON a.rowid = oa_artifact_fts.rowid
                 WHERE oa_artifact_fts MATCH ?1 AND {filter_sql}
                 ORDER BY rank_score ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    let bm25: f64 = row.get(2)?;
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(0)?,
                        match_kind: SearchCandidateKind::ArtifactTitle,
                        snippet: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        score_hint: 240 + fts5_bm25_score_bonus(bm25),
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            let mut values = vec![pattern.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, t.artifact_note_tag_id, t.raw_tag
                 FROM oa_artifact_note_tag t
                 JOIN oa_artifact a ON a.artifact_id = t.artifact_id
                 WHERE lower(t.raw_tag) LIKE ? AND {filter_sql}
                 ORDER BY t.sequence_no ASC, t.artifact_note_tag_id ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::ImportedNoteTag,
                        snippet: row.get(2)?,
                        score_hint: 380,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            let mut values = vec![pattern.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, na.artifact_note_alias_id, na.alias_text
                 FROM oa_artifact_note_alias na
                 JOIN oa_artifact a ON a.artifact_id = na.artifact_id
                 WHERE lower(na.alias_text) LIKE ? AND {filter_sql}
                 ORDER BY na.sequence_no ASC, na.artifact_note_alias_id ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::ImportedNoteAlias,
                        snippet: row.get(2)?,
                        score_hint: 340,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            let mut values = vec![pattern.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, a.artifact_id, a.source_conversation_key
                 FROM oa_artifact a
                 WHERE lower(coalesce(a.source_conversation_key, '')) LIKE ? AND {filter_sql}
                 ORDER BY a.captured_at DESC, a.artifact_id ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::ImportedNotePath,
                        snippet: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        score_hint: 300,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if let Some(match_expr) = fts_match.as_ref() {
            let mut values: Vec<String> = vec![match_expr.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            if let Some(object_type) = filters.object_type {
                values.push(object_type.as_str().to_string());
            }
            values.push(limit.to_string());
            let object_filter = if filters.object_type.is_some() {
                "AND d.derived_object_type = ?"
            } else {
                ""
            };
            let sql = format!(
                "SELECT d.artifact_id, d.derived_object_id, d.derived_object_type,
                        COALESCE(d.title, d.body_text, ''),
                        bm25(oa_derived_object_fts) AS rank_score
                 FROM oa_derived_object_fts
                 JOIN oa_derived_object d ON d.rowid = oa_derived_object_fts.rowid
                 JOIN oa_artifact a ON a.artifact_id = d.artifact_id
                 WHERE oa_derived_object_fts MATCH ?1
                   AND d.object_status = 'active'
                   AND {filter_sql}
                   {object_filter}
                 ORDER BY rank_score ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    let bm25: f64 = row.get(4)?;
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::DerivedObject {
                            derived_type: parse_derived_object_type(row.get(2)?)?,
                        },
                        snippet: row.get::<_, String>(3)?,
                        score_hint: 260 + fts5_bm25_score_bonus(bm25),
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            if let Some(match_expr) = fts_match.as_ref() {
                let mut values: Vec<String> = vec![match_expr.clone()];
                let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
                values.push(limit.to_string());
                let sql = format!(
                    "SELECT s.artifact_id, s.segment_id,
                            substr(s.text_content, 1, 240),
                            bm25(oa_segment_fts) AS rank_score
                     FROM oa_segment_fts
                     JOIN oa_segment s ON s.rowid = oa_segment_fts.rowid
                     JOIN oa_artifact a ON a.artifact_id = s.artifact_id
                     WHERE oa_segment_fts MATCH ?1 AND {filter_sql}
                     ORDER BY rank_score ASC
                     LIMIT ?"
                );
                let mut stmt = connection
                    .prepare(&sql)
                    .map_err(|source| db_err(&self.config, source))?;
                let rows = stmt
                    .query_map(params_from_iter(values.iter()), |row| {
                        let bm25: f64 = row.get(3)?;
                        Ok(ArchiveSearchCandidate {
                            artifact_id: row.get(0)?,
                            match_record_id: row.get(1)?,
                            match_kind: SearchCandidateKind::SegmentExcerpt,
                            snippet: row.get(2)?,
                            score_hint: 200 + fts5_bm25_score_bonus(bm25),
                        })
                    })
                    .map_err(|source| db_err(&self.config, source))?;
                candidates.extend(
                    rows.collect::<Result<Vec<_>, _>>()
                        .map_err(|source| db_err(&self.config, source))?,
                );
            }
        }

        candidates.sort_by(|left, right| {
            right
                .score_hint
                .cmp(&left.score_hint)
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
                .then_with(|| left.match_record_id.cmp(&right.match_record_id))
        });
        candidates.dedup_by(|left, right| left.match_record_id == right.match_record_id);
        candidates.truncate(limit);
        Ok(candidates)
    }
}

impl ArtifactDetailReadStore for SqliteRetrievalReadStore {
    fn load_artifact_detail(&self, artifact_id: &str) -> StorageResult<Option<ArtifactDetailView>> {
        let connection = open_connection(&self.config)?;
        let Some(artifact) = load_artifact_record(&connection, artifact_id)? else {
            return Ok(None);
        };
        Ok(Some(ArtifactDetailView {
            imported_note_metadata: load_imported_note_metadata(
                &connection,
                artifact_id,
                artifact.note_path.clone(),
            )?,
            inbound_note_links: load_inbound_note_links(&connection, artifact_id)?,
            artifact_links: load_artifact_links(&connection, artifact_id)?,
            segments: load_artifact_segments(&connection, artifact_id)?,
            artifact,
            derived_objects: load_artifact_detail_derived_objects(&connection, artifact_id)?,
        }))
    }
}

impl ArtifactContextPackReadStore for SqliteRetrievalReadStore {
    fn load_artifact_context_pack_material(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<ArtifactContextPackMaterial>> {
        let connection = open_connection(&self.config)?;
        let Some(artifact) = load_artifact_record(&connection, artifact_id)? else {
            return Ok(None);
        };
        Ok(Some(ArtifactContextPackMaterial {
            segments: load_artifact_segments(&connection, artifact_id)?,
            imported_note_metadata: load_imported_note_metadata(
                &connection,
                artifact_id,
                artifact.note_path.clone(),
            )?,
            inbound_note_links: load_inbound_note_links(&connection, artifact_id)?,
            artifact_links: load_artifact_links(&connection, artifact_id)?,
            derived_objects: load_artifact_context_derived_objects(&connection, artifact_id)?,
            artifact,
        }))
    }
}

impl DerivedObjectSearchStore for SqliteRetrievalReadStore {
    fn search_objects(
        &self,
        filters: &ObjectSearchFilters,
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        let connection = open_connection(&self.config)?;

        // If the caller provided a free-text query, rewrite it through the
        // shared FTS5 builder and rank with bm25. Otherwise fall back to a
        // plain confidence-ordered scan so callers that filter only by type /
        // candidate_key / artifact_id keep working.
        let normalized_query = filters
            .query
            .as_ref()
            .map(|q| q.trim())
            .filter(|q| !q.is_empty());
        let fts_match = normalized_query.and_then(build_fts5_match_expression);

        let mut values: Vec<String> = Vec::new();
        let mut conditions = vec!["d.object_status = 'active'".to_string()];

        let (from_clause, rank_select, order_clause) = if fts_match.is_some() {
            (
                "oa_derived_object_fts \
                 JOIN oa_derived_object d ON d.rowid = oa_derived_object_fts.rowid"
                    .to_string(),
                ", bm25(oa_derived_object_fts) AS rank_score".to_string(),
                "ORDER BY rank_score ASC".to_string(),
            )
        } else {
            (
                "oa_derived_object d".to_string(),
                ", NULL AS rank_score".to_string(),
                "ORDER BY COALESCE(d.confidence_score, 0.0) DESC, d.derived_object_id ASC"
                    .to_string(),
            )
        };

        if let Some(match_expr) = fts_match.as_ref() {
            conditions.push("oa_derived_object_fts MATCH ?".to_string());
            values.push(match_expr.clone());
        }
        if let Some(object_type) = filters.object_type {
            conditions.push("d.derived_object_type = ?".to_string());
            values.push(object_type.as_str().to_string());
        }
        if let Some(candidate_key) = filters.candidate_key.as_ref() {
            conditions.push("d.candidate_key = ?".to_string());
            values.push(candidate_key.clone());
        }
        if let Some(artifact_id) = filters.artifact_id.as_ref() {
            conditions.push("d.artifact_id = ?".to_string());
            values.push(artifact_id.clone());
        }
        values.push(limit.to_string());

        let sql = format!(
            "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type,
                    d.title, d.body_text, d.candidate_key, d.confidence_score
                    {rank_select}
             FROM {from_clause}
             WHERE {where_clause}
             {order_clause}
             LIMIT ?",
            rank_select = rank_select,
            from_clause = from_clause,
            where_clause = conditions.join(" AND "),
            order_clause = order_clause,
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), |row| {
                let bm25: Option<f64> = row.get(7)?;
                Ok(DerivedObjectSearchResult {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    candidate_key: row.get(5)?,
                    confidence_score: row.get(6)?,
                    // Surface a positive lexical relevance score (higher = better).
                    // `app::search::normalize_lexical_score` re-normalizes this
                    // before blending with semantic scores.
                    score: bm25.map(|value| (-value) as f32),
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn search_objects_by_embedding(
        &self,
        _filters: &ObjectSearchFilters,
        _query_embedding: &[f32],
        _limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        Err(StorageError::UnsupportedOperation {
            store: "sqlite",
            operation: "search_objects_by_embedding without external vector provider",
        })
    }

    fn get_related_objects(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<GraphRelatedEntry>> {
        let connection = open_connection(&self.config)?;
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        let mut archive_stmt = connection
            .prepare(
                "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type, d.title, d.body_text,
                        al.link_type, al.confidence_score, al.rationale
                 FROM oa_archive_link al
                 JOIN oa_derived_object d
                   ON d.derived_object_id = CASE
                        WHEN al.source_object_id = ?1 THEN al.target_object_id
                        ELSE al.source_object_id
                      END
                 WHERE (al.source_object_id = ?1 OR al.target_object_id = ?1)
                   AND d.object_status = 'active'
                 ORDER BY d.derived_object_id ASC
                 LIMIT ?2",
            )
            .map_err(|source| db_err(&self.config, source))?;

        let archive_rows = archive_stmt
            .query_map(params![derived_object_id, limit as i64], |row| {
                Ok(GraphRelatedEntry {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    relation_kind: "archive_link".to_string(),
                    link_type: row.get(5)?,
                    confidence_score: row.get(6)?,
                    rationale: row.get(7)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        for row in archive_rows {
            let entry = row.map_err(|source| db_err(&self.config, source))?;
            if seen.insert(entry.derived_object_id.clone()) {
                results.push(entry);
            }
        }

        let remaining = limit.saturating_sub(results.len());
        if remaining > 0 {
            let mut candidate_stmt = connection
                .prepare(
                    "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type, d.title, d.body_text
                     FROM oa_derived_object source
                     JOIN oa_derived_object d
                       ON d.candidate_key = source.candidate_key
                      AND d.derived_object_id <> source.derived_object_id
                     WHERE source.derived_object_id = ?1
                       AND COALESCE(source.candidate_key, '') <> ''
                       AND d.object_status = 'active'
                     ORDER BY d.derived_object_id ASC
                     LIMIT ?2",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let candidate_rows = candidate_stmt
                .query_map(params![derived_object_id, remaining as i64], |row| {
                    Ok(GraphRelatedEntry {
                        derived_object_id: row.get(0)?,
                        artifact_id: row.get(1)?,
                        derived_object_type: parse_derived_object_type(row.get(2)?)?,
                        title: row.get(3)?,
                        body_text: row.get(4)?,
                        relation_kind: "shared_candidate_key".to_string(),
                        link_type: None,
                        confidence_score: None,
                        rationale: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            for row in candidate_rows {
                let entry = row.map_err(|source| db_err(&self.config, source))?;
                if seen.insert(entry.derived_object_id.clone()) {
                    results.push(entry);
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }
}

impl DerivedObjectLookupStore for SqliteRetrievalReadStore {
    fn load_active_objects_by_ids(
        &self,
        derived_object_ids: &[String],
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        if derived_object_ids.is_empty() {
            return Ok(Vec::new());
        }
        let connection = open_connection(&self.config)?;
        let placeholders = std::iter::repeat_n("?", derived_object_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT derived_object_id, artifact_id, derived_object_type, title, body_text, candidate_key, confidence_score
             FROM oa_derived_object
             WHERE object_status = 'active'
               AND derived_object_id IN ({placeholders})
             ORDER BY derived_object_id ASC"
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(derived_object_ids.iter()), |row| {
                Ok(DerivedObjectSearchResult {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    candidate_key: row.get(5)?,
                    confidence_score: row.get(6)?,
                    score: None,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }
}

impl CrossArtifactReadStore for SqliteRetrievalReadStore {
    fn find_related_by_candidate_keys(
        &self,
        artifact_id: &str,
        candidate_keys: &[String],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>> {
        if candidate_keys.is_empty() {
            return Ok(Vec::new());
        }
        let connection = open_connection(&self.config)?;
        let placeholders = std::iter::repeat_n("?", candidate_keys.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT derived_object_id, artifact_id, derived_object_type, title, body_text, candidate_key, confidence_score
             FROM oa_derived_object
             WHERE object_status = 'active'
               AND artifact_id <> ?
               AND candidate_key IN ({placeholders})
             ORDER BY candidate_key ASC, derived_object_id ASC
             LIMIT ?"
        );
        let mut params = Vec::with_capacity(candidate_keys.len() + 2);
        params.push(artifact_id.to_string());
        params.extend(candidate_keys.iter().cloned());
        params.push(limit.to_string());
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(params.iter()), |row| {
                Ok(RelatedDerivedObject {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    candidate_key: row.get(5)?,
                    confidence_score: row.get(6)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn find_related_by_embedding(
        &self,
        _artifact_id: &str,
        _derived_object_type: DerivedObjectType,
        _query_embedding: &[f32],
        _limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>> {
        Err(StorageError::UnsupportedOperation {
            store: "sqlite",
            operation: "find_related_by_embedding without external vector provider",
        })
    }
}

#[cfg(test)]
mod fts5_helper_tests {
    use super::{
        build_fts5_match_expression, detect_search_query_mode, fts5_bm25_score_bonus,
        SqliteSearchQueryMode,
    };

    #[test]
    fn plain_query_becomes_or_of_quoted_tokens() {
        assert_eq!(
            build_fts5_match_expression("dressage horse training").as_deref(),
            Some("\"dressage\" OR \"horse\" OR \"training\"")
        );
    }

    #[test]
    fn single_token_plain_query_is_quoted() {
        assert_eq!(
            build_fts5_match_expression("dressage").as_deref(),
            Some("\"dressage\"")
        );
    }

    #[test]
    fn extra_whitespace_is_collapsed() {
        assert_eq!(
            build_fts5_match_expression("  blood   pressure  ").as_deref(),
            Some("\"blood\" OR \"pressure\"")
        );
    }

    #[test]
    fn empty_query_yields_none() {
        assert!(build_fts5_match_expression("").is_none());
        assert!(build_fts5_match_expression("   ").is_none());
    }

    #[test]
    fn embedded_double_quote_is_escaped() {
        // FTS5 escapes a literal `"` inside a quoted token by doubling it.
        assert_eq!(
            build_fts5_match_expression("foo\"bar").as_deref(),
            Some("\"foo\"\"bar\"")
        );
    }

    #[test]
    fn operator_query_passes_through_verbatim() {
        let raw = "memory AND retrieval NOT cache";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
        assert_eq!(build_fts5_match_expression(raw).as_deref(), Some(raw));
    }

    #[test]
    fn quoted_phrase_query_passes_through_verbatim() {
        let raw = "\"enrichment pipeline\"";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
        assert_eq!(build_fts5_match_expression(raw).as_deref(), Some(raw));
    }

    #[test]
    fn mixed_natural_language_with_quoted_phrase_stays_plain() {
        let raw = "Ahab's \"Strike Through the Mask\" Speech";
        assert_eq!(detect_search_query_mode(raw), SqliteSearchQueryMode::Plain);
        assert_eq!(
            build_fts5_match_expression(raw).as_deref(),
            Some("\"Ahab's\" OR \"\"\"Strike\" OR \"Through\" OR \"the\" OR \"Mask\"\"\" OR \"Speech\"")
        );
    }

    #[test]
    fn parenthesized_query_is_operator_mode() {
        let raw = "(memory OR summary) AND retrieval";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
    }

    #[test]
    fn prefix_query_is_operator_mode() {
        let raw = "dressag*";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
        assert_eq!(build_fts5_match_expression(raw).as_deref(), Some(raw));
    }

    #[test]
    fn lowercase_and_or_not_are_treated_as_plain_tokens() {
        // FTS5 only treats uppercase AND/OR/NOT as operators, so a lowercase
        // "and" should be quoted like any other plain token. This matches the
        // intent of "adding a term broadens results".
        assert_eq!(
            build_fts5_match_expression("dressage and horse").as_deref(),
            Some("\"dressage\" OR \"and\" OR \"horse\"")
        );
    }

    #[test]
    fn bm25_bonus_rewards_better_matches_with_larger_values() {
        // bm25() returns smaller (more negative) values for better matches.
        let weak = fts5_bm25_score_bonus(-0.1);
        let strong = fts5_bm25_score_bonus(-2.5);
        assert!(strong > weak, "strong={strong} should exceed weak={weak}");
    }

    #[test]
    fn bm25_bonus_clamps_non_matches_to_zero() {
        assert_eq!(fts5_bm25_score_bonus(0.0), 0);
        assert_eq!(fts5_bm25_score_bonus(1.5), 0);
        assert_eq!(fts5_bm25_score_bonus(f64::NAN), 0);
        assert_eq!(fts5_bm25_score_bonus(f64::INFINITY), 0);
    }
}
