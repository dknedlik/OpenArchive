use rusqlite::OptionalExtension;

use crate::error::StorageError;
use crate::storage::types::{
    ArchiveStatusSnapshot, ArtifactListFilters, ArtifactListItem, LoadedArtifactForEnrichment,
    LoadedArtifactRecord, LoadedParticipant, LoadedSegment, TimelineEntry, TimelineFilters,
};
use crate::storage::{
    ArtifactEnrichmentCount as ArtifactEnrichmentCountType, ArtifactReadStore,
    ArtifactSourceCount as ArtifactSourceCountType, EnrichmentJobCount as EnrichmentJobCountType,
    OperatorStore,
};

use super::links::load_imported_note_metadata;
use super::{
    db_err, from_sqlite_timestamp, open_connection, parse_artifact_class, parse_enrichment_status,
    parse_job_status, parse_participant_role, parse_source_type, parse_visibility_status,
    sqlite_count, SqliteArtifactReadStore, SqliteOperatorStore, StorageResult,
};

impl ArtifactReadStore for SqliteArtifactReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<ArtifactListItem>> {
        self.list_artifacts_filtered(&ArtifactListFilters::default(), 1000, 0)
    }

    fn list_artifacts_filtered(
        &self,
        filters: &ArtifactListFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<ArtifactListItem>> {
        use rusqlite::params_from_iter;

        let connection = open_connection(&self.config)?;
        let mut conditions = vec!["1=1".to_string()];
        let mut values: Vec<String> = Vec::new();
        if let Some(source_type) = filters.source_type {
            conditions.push("source_type = ?".to_string());
            values.push(source_type.as_str().to_string());
        }
        if let Some(status) = filters.enrichment_status {
            conditions.push("enrichment_status = ?".to_string());
            values.push(status.as_str().to_string());
        }
        if let Some(tag) = filters.tag.as_ref() {
            conditions.push(
                "EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = oa_artifact.artifact_id AND t.normalized_tag = ?)"
                    .to_string(),
            );
            values.push(tag.to_lowercase());
        }
        if let Some(alias) = filters.alias.as_ref() {
            conditions.push(
                "EXISTS (SELECT 1 FROM oa_artifact_note_alias a2 WHERE a2.artifact_id = oa_artifact.artifact_id AND a2.normalized_alias = ?)"
                    .to_string(),
            );
            values.push(alias.to_lowercase());
        }
        if let Some(path_prefix) = filters.path_prefix.as_ref() {
            conditions.push("lower(coalesce(source_conversation_key, '')) LIKE ?".to_string());
            values.push(format!("{}%", path_prefix.to_lowercase()));
        }
        if let Some(captured_after) = filters.captured_after.as_ref() {
            conditions.push("captured_at >= ?".to_string());
            values.push(captured_after.clone());
        }
        if let Some(captured_before) = filters.captured_before.as_ref() {
            conditions.push("captured_at < ?".to_string());
            values.push(captured_before.clone());
        }
        values.push(limit.to_string());
        values.push(offset.to_string());

        let sql = format!(
            "SELECT artifact_id, title, source_conversation_key, source_type, created_at_source, captured_at, enrichment_status
             FROM oa_artifact
             WHERE {}
             ORDER BY captured_at DESC, artifact_id ASC
             LIMIT ? OFFSET ?",
            conditions.join(" AND ")
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), |row| {
                Ok(ArtifactListItem {
                    artifact_id: row.get(0)?,
                    title: row.get(1)?,
                    note_path: row.get(2)?,
                    source_type: row.get(3)?,
                    created_at_source: row.get(4)?,
                    captured_at: row.get(5)?,
                    enrichment_status: parse_enrichment_status(row.get(6)?)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn get_timeline(
        &self,
        filters: &TimelineFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<TimelineEntry>> {
        use rusqlite::params_from_iter;

        let connection = open_connection(&self.config)?;
        let mut conditions = vec!["1=1".to_string()];
        let mut values: Vec<String> = Vec::new();
        if let Some(keyword) = filters.keyword.as_ref() {
            conditions.push("lower(coalesce(a.title, '')) LIKE ?".to_string());
            values.push(format!("%{}%", keyword.to_lowercase()));
        }
        if let Some(source_type) = filters.source_type {
            conditions.push("a.source_type = ?".to_string());
            values.push(source_type.as_str().to_string());
        }
        if let Some(tag) = filters.tag.as_ref() {
            conditions.push(
                "EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = a.artifact_id AND t.normalized_tag = ?)"
                    .to_string(),
            );
            values.push(tag.to_lowercase());
        }
        if let Some(path_prefix) = filters.path_prefix.as_ref() {
            conditions.push("lower(coalesce(a.source_conversation_key, '')) LIKE ?".to_string());
            values.push(format!("{}%", path_prefix.to_lowercase()));
        }
        values.push(limit.to_string());
        values.push(offset.to_string());
        let sql = format!(
            "SELECT a.artifact_id, a.title, a.source_conversation_key, a.source_type,
                    a.created_at_source, a.captured_at, a.enrichment_status,
                    (SELECT d.body_text FROM oa_derived_object d
                     WHERE d.artifact_id = a.artifact_id
                       AND d.derived_object_type = 'summary'
                       AND d.object_status = 'active'
                     ORDER BY COALESCE(d.confidence_score, 0.0) DESC, d.derived_object_id ASC
                     LIMIT 1)
             FROM oa_artifact a
             WHERE {}
             ORDER BY COALESCE(a.created_at_source, a.captured_at) ASC, a.artifact_id ASC
             LIMIT ? OFFSET ?",
            conditions.join(" AND ")
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), |row| {
                Ok(TimelineEntry {
                    artifact_id: row.get(0)?,
                    title: row.get(1)?,
                    note_path: row.get(2)?,
                    source_type: row.get(3)?,
                    created_at_source: row.get(4)?,
                    captured_at: row.get(5)?,
                    enrichment_status: row.get(6)?,
                    summary_snippet: row.get(7)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
        let connection = open_connection(&self.config)?;
        load_artifact_for_enrichment_record(&connection, artifact_id)
    }
}

impl OperatorStore for SqliteOperatorStore {
    fn load_archive_status(&self) -> StorageResult<ArchiveStatusSnapshot> {
        let connection = open_connection(&self.config)?;
        let artifact_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM oa_artifact", [], |row| row.get(0))
            .map_err(|source| db_err(&self.config, source))?;
        let mut artifact_source_stmt = connection
            .prepare(
                "SELECT source_type, COUNT(*) \
                 FROM oa_artifact \
                 GROUP BY source_type \
                 ORDER BY source_type",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let artifacts_by_source = artifact_source_stmt
            .query_map([], |row| {
                let count: i64 = row.get(1)?;
                Ok(ArtifactSourceCountType {
                    source_type: parse_source_type(row.get(0)?)?,
                    count: sqlite_count(count, 1)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|source| db_err(&self.config, source))?;

        let mut artifact_status_stmt = connection
            .prepare(
                "SELECT enrichment_status, COUNT(*) \
                 FROM oa_artifact \
                 GROUP BY enrichment_status \
                 ORDER BY enrichment_status",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let artifacts_by_enrichment_status = artifact_status_stmt
            .query_map([], |row| {
                let count: i64 = row.get(1)?;
                Ok(ArtifactEnrichmentCountType {
                    enrichment_status: parse_enrichment_status(row.get(0)?)?,
                    count: sqlite_count(count, 1)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|source| db_err(&self.config, source))?;

        let mut job_status_stmt = connection
            .prepare(
                "SELECT job_status, COUNT(*) \
                 FROM oa_enrichment_job \
                 GROUP BY job_status \
                 ORDER BY job_status",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let jobs_by_status = job_status_stmt
            .query_map([], |row| {
                let count: i64 = row.get(1)?;
                Ok(EnrichmentJobCountType {
                    job_status: parse_job_status(row.get(0)?)?,
                    count: sqlite_count(count, 1)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|source| db_err(&self.config, source))?;

        Ok(ArchiveStatusSnapshot {
            artifact_count: sqlite_count(artifact_count, 0)
                .map_err(|source| db_err(&self.config, source))?,
            artifacts_by_source,
            artifacts_by_enrichment_status,
            jobs_by_status,
        })
    }
}

fn load_artifact_for_enrichment_record(
    connection: &rusqlite::Connection,
    artifact_id: &str,
) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
    let artifact = connection
        .query_row(
            "SELECT artifact_id, import_id, artifact_class, source_type, title, source_conversation_key
             FROM oa_artifact
             WHERE artifact_id = ?1",
            [artifact_id],
            |row| {
                Ok((
                    LoadedArtifactRecord {
                        artifact_id: row.get(0)?,
                        import_id: row.get(1)?,
                        artifact_class: parse_artifact_class(row.get(2)?)?,
                        source_type: parse_source_type(row.get(3)?)?,
                        title: row.get(4)?,
                    },
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact for enrichment {artifact_id}: {source}"),
        })?;
    let Some((artifact, note_path)) = artifact else {
        return Ok(None);
    };

    let mut participant_stmt = connection
        .prepare(
            "SELECT participant_id, participant_role, display_name, source_participant_key
             FROM oa_conversation_participant
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, participant_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare participant load query: {source}"),
        })?;
    let participants = participant_stmt
        .query_map([artifact_id], |row| {
            Ok(LoadedParticipant {
                participant_id: row.get(0)?,
                participant_role: parse_participant_role(row.get(1)?)?,
                display_name: row.get(2)?,
                external_id: row.get(3)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load participants: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map participants: {source}"),
        })?;

    let mut segment_stmt = connection
        .prepare(
            "SELECT s.segment_id, s.participant_id, p.participant_role, s.sequence_no, s.text_content,
                    s.created_at_source, s.visibility_status
             FROM oa_segment s
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id
             WHERE s.artifact_id = ?1
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare segment load query: {source}"),
        })?;
    let segments = segment_stmt
        .query_map([artifact_id], |row| {
            Ok(LoadedSegment {
                segment_id: row.get(0)?,
                participant_id: row.get(1)?,
                participant_role: match row.get::<_, Option<String>>(2)? {
                    Some(value) => Some(parse_participant_role(value)?),
                    None => None,
                },
                sequence_no: row.get(3)?,
                text_content: row.get(4)?,
                created_at_source: from_sqlite_timestamp(row.get(5)?),
                visibility_status: parse_visibility_status(row.get(6)?)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load segments: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map segments: {source}"),
        })?;

    Ok(Some(LoadedArtifactForEnrichment {
        artifact,
        participants,
        segments,
        imported_note_metadata: load_imported_note_metadata(connection, artifact_id, note_path)?,
    }))
}
