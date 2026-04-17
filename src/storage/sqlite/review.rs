use rusqlite::{params, OptionalExtension, TransactionBehavior};

use crate::error::StorageError;
use crate::storage::review_read_store::{
    NewReviewDecision, ReviewCandidate, ReviewItemKind, ReviewQueueFilters, ReviewReadStore,
    ReviewWriteStore,
};
use crate::storage::types::ArtifactExtractPayload;

use super::job::recompute_artifact_enrichment_status_sqlite;
use super::{
    db_err, next_id, open_connection, parse_enrichment_status, parse_source_type,
    SqliteRetrievalReadStore, StorageResult,
};

impl ReviewReadStore for SqliteRetrievalReadStore {
    fn list_review_candidates(
        &self,
        filters: &ReviewQueueFilters,
        limit: usize,
    ) -> StorageResult<Vec<ReviewCandidate>> {
        let connection = open_connection(&self.config)?;
        let include_all = filters.kinds.as_ref().is_none_or(Vec::is_empty);
        let include_kind = |kind: ReviewItemKind| {
            include_all
                || filters
                    .kinds
                    .as_ref()
                    .is_some_and(|kinds| kinds.contains(&kind))
        };

        let mut candidates = Vec::new();

        if include_kind(ReviewItemKind::ArtifactNeedsAttention) {
            let mut stmt = connection.prepare(
                "SELECT artifact_id, source_type, captured_at, title, enrichment_status
                 FROM oa_artifact
                 WHERE enrichment_status IN ('partial', 'failed')
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:artifact_needs_attention:' || oa_artifact.artifact_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                 ORDER BY captured_at DESC, artifact_id ASC
                 LIMIT ?1",
            ).map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::ArtifactNeedsAttention,
                        artifact_id: row.get(0)?,
                        derived_object_id: None,
                        source_type: parse_source_type(row.get(1)?)?,
                        captured_at: row.get(2)?,
                        title: row.get(3)?,
                        body_text: None,
                        derived_object_type: None,
                        candidate_key: None,
                        enrichment_status: Some(parse_enrichment_status(row.get(4)?)?),
                        confidence_score: None,
                        related_artifact_count: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if include_kind(ReviewItemKind::ArtifactMissingSummary) {
            let mut stmt = connection.prepare(
                "SELECT a.artifact_id, a.source_type, a.captured_at, a.title, a.enrichment_status
                 FROM oa_artifact a
                 WHERE a.enrichment_status IN ('completed', 'partial')
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:artifact_missing_summary:' || a.artifact_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_derived_object d
                        WHERE d.artifact_id = a.artifact_id
                          AND d.derived_object_type = 'summary'
                          AND d.object_status = 'active'
                   )
                 ORDER BY a.captured_at DESC, a.artifact_id ASC
                 LIMIT ?1",
            ).map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::ArtifactMissingSummary,
                        artifact_id: row.get(0)?,
                        derived_object_id: None,
                        source_type: parse_source_type(row.get(1)?)?,
                        captured_at: row.get(2)?,
                        title: row.get(3)?,
                        body_text: None,
                        derived_object_type: None,
                        candidate_key: None,
                        enrichment_status: Some(parse_enrichment_status(row.get(4)?)?),
                        confidence_score: None,
                        related_artifact_count: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if include_kind(ReviewItemKind::ObjectLowConfidence) {
            let mut stmt = connection
                .prepare(
                    "SELECT d.derived_object_id, d.artifact_id, a.source_type, a.captured_at,
                        d.derived_object_type, d.title, d.body_text, d.confidence_score
                 FROM oa_derived_object d
                 JOIN oa_artifact a ON a.artifact_id = d.artifact_id
                 WHERE d.object_status = 'active'
                   AND d.confidence_score IS NOT NULL
                   AND d.confidence_score < 0.60
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:object_low_confidence:' || d.derived_object_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                 ORDER BY d.confidence_score ASC, a.captured_at DESC, d.derived_object_id ASC
                 LIMIT ?1",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::ObjectLowConfidence,
                        artifact_id: row.get(1)?,
                        derived_object_id: Some(row.get(0)?),
                        source_type: parse_source_type(row.get(2)?)?,
                        captured_at: row.get(3)?,
                        title: row.get(5)?,
                        body_text: row.get(6)?,
                        derived_object_type: Some(super::parse_derived_object_type(row.get(4)?)?),
                        candidate_key: None,
                        enrichment_status: None,
                        confidence_score: row.get(7)?,
                        related_artifact_count: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if include_kind(ReviewItemKind::CandidateKeyCollision) {
            let mut stmt = connection.prepare(
                "WITH collided_keys AS (
                     SELECT candidate_key, COUNT(DISTINCT artifact_id) AS artifact_count
                     FROM oa_derived_object
                     WHERE object_status = 'active'
                       AND candidate_key IS NOT NULL
                       AND candidate_key <> ''
                     GROUP BY candidate_key
                     HAVING COUNT(DISTINCT artifact_id) > 1
                 )
                 SELECT d.derived_object_id, d.artifact_id, a.source_type, a.captured_at,
                        d.derived_object_type, d.title, d.body_text, d.candidate_key, ck.artifact_count
                 FROM collided_keys ck
                 JOIN oa_derived_object d ON d.candidate_key = ck.candidate_key
                 JOIN oa_artifact a ON a.artifact_id = d.artifact_id
                 WHERE d.object_status = 'active'
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:candidate_key_collision:' || d.derived_object_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                 ORDER BY ck.artifact_count DESC, a.captured_at DESC, d.derived_object_id ASC
                 LIMIT ?1",
            ).map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::CandidateKeyCollision,
                        artifact_id: row.get(1)?,
                        derived_object_id: Some(row.get(0)?),
                        source_type: parse_source_type(row.get(2)?)?,
                        captured_at: row.get(3)?,
                        title: row.get(5)?,
                        body_text: row.get(6)?,
                        derived_object_type: Some(super::parse_derived_object_type(row.get(4)?)?),
                        candidate_key: row.get(7)?,
                        enrichment_status: None,
                        confidence_score: None,
                        related_artifact_count: Some(row.get::<_, i64>(8)? as usize),
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        Ok(candidates)
    }
}

impl ReviewWriteStore for SqliteRetrievalReadStore {
    fn record_review_decision(&self, decision: &NewReviewDecision) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        connection
            .execute(
                "INSERT INTO oa_review_decision
                 (review_decision_id, item_id, item_kind, artifact_id, derived_object_id,
                  decision_status, note_text, decided_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    decision.review_decision_id,
                    decision.item_id,
                    decision.item_kind.as_str(),
                    decision.artifact_id,
                    decision.derived_object_id,
                    decision.decision_status.as_str(),
                    decision.note_text,
                    decision.decided_by
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn retry_artifact_enrichment(&self, artifact_id: &str) -> StorageResult<String> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;

        let row = tx
            .query_row(
                "SELECT artifact_id, import_id, source_type
                 FROM oa_artifact
                 WHERE artifact_id = ?1",
                [artifact_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        parse_source_type(row.get(2)?)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?;
        let Some((artifact_id_value, import_id, source_type)) = row else {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("artifact {artifact_id} not found for retry"),
            });
        };

        let active_job_count: i64 = tx
            .query_row(
                "SELECT COUNT(*)
                 FROM oa_enrichment_job
                 WHERE artifact_id = ?1
                   AND job_status IN ('pending', 'running', 'retryable')",
                [&artifact_id_value],
                |row| row.get(0),
            )
            .map_err(|source| db_err(&self.config, source))?;
        if active_job_count > 0 {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("artifact {artifact_id} already has active enrichment work"),
            });
        }

        let job_id = next_id("job");
        let payload_json = ArtifactExtractPayload::new_v1(
            &artifact_id_value,
            &import_id,
            source_type,
            None,
            Vec::new(),
            Vec::new(),
        )
        .to_json();
        tx.execute(
            "INSERT INTO oa_enrichment_job
             (job_id, artifact_id, job_type, enrichment_tier, job_status, max_attempts,
              priority_no, required_capabilities, payload_json)
             VALUES (?1, ?2, 'artifact_extract', 'default', 'pending', 3, 100, '[\"text\"]', ?3)",
            params![job_id, artifact_id_value, payload_json],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(job_id)
    }
}
