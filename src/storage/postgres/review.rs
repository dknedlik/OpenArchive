use postgres::Client;

use crate::error::{DbError, StorageError, StorageResult};
use crate::storage::review_read_store::{
    NewReviewDecision, ReviewCandidate, ReviewItemKind, ReviewQueueFilters,
};
use crate::storage::types::{
    ArtifactExtractPayload, DerivedObjectType, EnrichmentStatus, EnrichmentTier, JobStatus,
    JobType, NewEnrichmentJob, SourceType,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

fn map_pg_err(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source: Box::new(source),
    })
}

pub fn list_review_candidates(
    client: &mut Client,
    connection_string: &str,
    filters: &ReviewQueueFilters,
    limit: usize,
) -> StorageResult<Vec<ReviewCandidate>> {
    let fetch_limit = i64::try_from(limit).map_err(|_| StorageError::InvalidDerivationWrite {
        detail: format!("review limit {limit} exceeds Postgres BIGINT range"),
    })?;
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
        candidates.extend(list_artifact_needs_attention(
            client,
            connection_string,
            fetch_limit,
        )?);
    }
    if include_kind(ReviewItemKind::ArtifactMissingSummary) {
        candidates.extend(list_artifact_missing_summary(
            client,
            connection_string,
            fetch_limit,
        )?);
    }
    if include_kind(ReviewItemKind::ObjectLowConfidence) {
        candidates.extend(list_object_low_confidence(
            client,
            connection_string,
            fetch_limit,
        )?);
    }
    if include_kind(ReviewItemKind::CandidateKeyCollision) {
        candidates.extend(list_candidate_key_collisions(
            client,
            connection_string,
            fetch_limit,
        )?);
    }
    if include_kind(ReviewItemKind::ObjectMissingEvidence) {
        candidates.extend(list_object_missing_evidence(
            client,
            connection_string,
            fetch_limit,
        )?);
    }

    Ok(candidates)
}

pub fn record_review_decision(
    client: &mut Client,
    connection_string: &str,
    decision: &NewReviewDecision,
) -> StorageResult<()> {
    let item_kind = decision.item_kind.as_str();
    let decision_status = decision.decision_status.as_str();
    client
        .execute(
            "INSERT INTO oa_review_decision
             (review_decision_id, item_id, item_kind, artifact_id, derived_object_id,
              decision_status, note_text, decided_by)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &decision.review_decision_id,
                &decision.item_id,
                &item_kind,
                &decision.artifact_id,
                &decision.derived_object_id,
                &decision_status,
                &decision.note_text,
                &decision.decided_by,
            ],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;
    Ok(())
}

pub fn retry_artifact_enrichment(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<String> {
    client
        .batch_execute("BEGIN")
        .map_err(|source| map_pg_err(connection_string, source))?;

    let result = (|| -> StorageResult<String> {
        let row = client
            .query_opt(
                "SELECT artifact_id, import_id, source_type
                 FROM oa_artifact
                 WHERE artifact_id = $1
                 FOR UPDATE",
                &[&artifact_id],
            )
            .map_err(|source| map_pg_err(connection_string, source))?;
        let Some(row) = row else {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("artifact {artifact_id} not found for retry"),
            });
        };

        let artifact_id_value: String = row.get(0);
        let import_id: String = row.get(1);
        let source_type = parse_source_type(&artifact_id_value, row.get(2))?;

        let active_job_count: i64 = client
            .query_one(
                "SELECT COUNT(*)
                 FROM oa_enrichment_job
                 WHERE artifact_id = $1
                   AND job_status IN ('pending', 'running', 'retryable')",
                &[&artifact_id_value],
            )
            .map_err(|source| map_pg_err(connection_string, source))?
            .get(0);

        if active_job_count > 0 {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("artifact {artifact_id} already has active enrichment work"),
            });
        }

        let job_id = new_id("job");
        let job = NewEnrichmentJob {
            job_id: job_id.clone(),
            artifact_id: artifact_id_value.clone(),
            job_type: JobType::ArtifactExtract,
            enrichment_tier: EnrichmentTier::Default,
            spawned_by_job_id: None,
            job_status: JobStatus::Pending,
            max_attempts: 3,
            priority_no: 100,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactExtractPayload::new_v1(
                &artifact_id_value,
                &import_id,
                source_type,
                None,
                Vec::new(),
                Vec::new(),
            )
            .to_json(),
        };
        crate::storage::postgres::job::insert_job(client, &job)?;
        crate::storage::postgres::job::recompute_artifact_enrichment_status(
            client,
            &artifact_id_value,
        )?;
        Ok(job_id)
    })();

    match result {
        Ok(job_id) => {
            client
                .batch_execute("COMMIT")
                .map_err(|source| map_pg_err(connection_string, source))?;
            Ok(job_id)
        }
        Err(err) => {
            client
                .batch_execute("ROLLBACK")
                .map_err(|source| map_pg_err(connection_string, source))?;
            Err(err)
        }
    }
}

fn list_artifact_needs_attention(
    client: &mut Client,
    connection_string: &str,
    limit: i64,
) -> StorageResult<Vec<ReviewCandidate>> {
    let rows = client
        .query(
            "SELECT artifact_id, source_type,
                    to_char(captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'),
                    title, enrichment_status
             FROM oa_artifact
             WHERE enrichment_status IN ('partial', 'failed')
               AND NOT EXISTS (
                    SELECT 1
                    FROM oa_review_decision rd
                    WHERE rd.item_id = 'review:artifact_needs_attention:' || artifact_id
                      AND rd.decision_status IN ('dismissed', 'resolved')
               )
             ORDER BY captured_at DESC, artifact_id ASC
             LIMIT $1",
            &[&limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact_id: String = row.get(0);
        let source_type = parse_source_type(&artifact_id, row.get(1))?;
        let enrichment_status = parse_enrichment_status(&artifact_id, row.get(4))?;
        candidates.push(ReviewCandidate {
            kind: ReviewItemKind::ArtifactNeedsAttention,
            artifact_id,
            derived_object_id: None,
            source_type,
            captured_at: row.get(2),
            title: row.get(3),
            body_text: None,
            derived_object_type: None,
            candidate_key: None,
            enrichment_status: Some(enrichment_status),
            confidence_score: None,
            related_artifact_count: None,
        });
    }

    Ok(candidates)
}

fn list_artifact_missing_summary(
    client: &mut Client,
    connection_string: &str,
    limit: i64,
) -> StorageResult<Vec<ReviewCandidate>> {
    let rows = client
        .query(
            "SELECT a.artifact_id, a.source_type,
                    to_char(a.captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'),
                    a.title, a.enrichment_status
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
             LIMIT $1",
            &[&limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact_id: String = row.get(0);
        let source_type = parse_source_type(&artifact_id, row.get(1))?;
        let enrichment_status = parse_enrichment_status(&artifact_id, row.get(4))?;
        candidates.push(ReviewCandidate {
            kind: ReviewItemKind::ArtifactMissingSummary,
            artifact_id,
            derived_object_id: None,
            source_type,
            captured_at: row.get(2),
            title: row.get(3),
            body_text: None,
            derived_object_type: None,
            candidate_key: None,
            enrichment_status: Some(enrichment_status),
            confidence_score: None,
            related_artifact_count: None,
        });
    }

    Ok(candidates)
}

fn list_object_low_confidence(
    client: &mut Client,
    connection_string: &str,
    limit: i64,
) -> StorageResult<Vec<ReviewCandidate>> {
    let rows = client
        .query(
            "SELECT d.derived_object_id, d.artifact_id, a.source_type,
                    to_char(a.captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'),
                    d.derived_object_type, d.title, d.body_text, d.confidence_score::double precision
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
             LIMIT $1",
            &[&limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact_id: String = row.get(1);
        candidates.push(ReviewCandidate {
            kind: ReviewItemKind::ObjectLowConfidence,
            artifact_id: artifact_id.clone(),
            derived_object_id: Some(row.get(0)),
            source_type: parse_source_type(&artifact_id, row.get(2))?,
            captured_at: row.get(3),
            title: row.get(5),
            body_text: row.get(6),
            derived_object_type: Some(parse_derived_object_type(&artifact_id, row.get(4))?),
            candidate_key: None,
            enrichment_status: None,
            confidence_score: row.try_get(7).map_err(|source| {
                StorageError::ReadDerivedObjectConfidenceScore {
                    artifact_id: artifact_id.clone(),
                    derived_object_id: row.get(0),
                    source: Box::new(source),
                }
            })?,
            related_artifact_count: None,
        });
    }

    Ok(candidates)
}

fn list_candidate_key_collisions(
    client: &mut Client,
    connection_string: &str,
    limit: i64,
) -> StorageResult<Vec<ReviewCandidate>> {
    let rows = client
        .query(
            "WITH collided_keys AS (
                 SELECT candidate_key,
                        COUNT(DISTINCT artifact_id) AS artifact_count
                 FROM oa_derived_object
                 WHERE object_status = 'active'
                   AND candidate_key IS NOT NULL
                   AND candidate_key <> ''
                 GROUP BY candidate_key
                 HAVING COUNT(DISTINCT artifact_id) > 1
             )
             SELECT d.derived_object_id, d.artifact_id, a.source_type,
                    to_char(a.captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'),
                    d.derived_object_type, d.title, d.body_text, d.candidate_key,
                    ck.artifact_count
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
             LIMIT $1",
            &[&limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact_id: String = row.get(1);
        let artifact_count: i64 = row.get(8);
        candidates.push(ReviewCandidate {
            kind: ReviewItemKind::CandidateKeyCollision,
            artifact_id: artifact_id.clone(),
            derived_object_id: Some(row.get(0)),
            source_type: parse_source_type(&artifact_id, row.get(2))?,
            captured_at: row.get(3),
            title: row.get(5),
            body_text: row.get(6),
            derived_object_type: Some(parse_derived_object_type(&artifact_id, row.get(4))?),
            candidate_key: row.get(7),
            enrichment_status: None,
            confidence_score: None,
            related_artifact_count: Some(
                usize::try_from(artifact_count).map_err(|_| StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "candidate_key collision artifact count {artifact_count} exceeds usize range"
                    ),
                })?,
            ),
        });
    }

    Ok(candidates)
}

fn list_object_missing_evidence(
    client: &mut Client,
    connection_string: &str,
    limit: i64,
) -> StorageResult<Vec<ReviewCandidate>> {
    let rows = client
        .query(
            "SELECT d.derived_object_id, d.artifact_id, a.source_type,
                    to_char(a.captured_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.USOF'),
                    d.derived_object_type, d.title, d.body_text
             FROM oa_derived_object d
             JOIN oa_artifact a ON a.artifact_id = d.artifact_id
             LEFT JOIN oa_evidence_link e ON e.derived_object_id = d.derived_object_id
             WHERE d.object_status = 'active'
               AND d.origin_kind = 'inferred'
               AND NOT EXISTS (
                    SELECT 1
                    FROM oa_review_decision rd
                    WHERE rd.item_id = 'review:object_missing_evidence:' || d.derived_object_id
                      AND rd.decision_status IN ('dismissed', 'resolved')
               )
             GROUP BY d.derived_object_id, d.artifact_id, a.source_type, a.captured_at,
                      d.derived_object_type, d.title, d.body_text
             HAVING COUNT(e.evidence_link_id) = 0
             ORDER BY a.captured_at DESC, d.derived_object_id ASC
             LIMIT $1",
            &[&limit],
        )
        .map_err(|source| map_pg_err(connection_string, source))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact_id: String = row.get(1);
        candidates.push(ReviewCandidate {
            kind: ReviewItemKind::ObjectMissingEvidence,
            artifact_id: artifact_id.clone(),
            derived_object_id: Some(row.get(0)),
            source_type: parse_source_type(&artifact_id, row.get(2))?,
            captured_at: row.get(3),
            title: row.get(5),
            body_text: row.get(6),
            derived_object_type: Some(parse_derived_object_type(&artifact_id, row.get(4))?),
            candidate_key: None,
            enrichment_status: None,
            confidence_score: None,
            related_artifact_count: None,
        });
    }

    Ok(candidates)
}

fn parse_source_type(artifact_id: &str, value: String) -> StorageResult<SourceType> {
    SourceType::parse(&value).ok_or_else(|| StorageError::InvalidSourceType {
        artifact_id: artifact_id.to_string(),
        value,
    })
}

fn parse_enrichment_status(artifact_id: &str, value: String) -> StorageResult<EnrichmentStatus> {
    EnrichmentStatus::parse(&value).ok_or_else(|| StorageError::InvalidEnrichmentStatus {
        artifact_id: artifact_id.to_string(),
        value,
    })
}

fn parse_derived_object_type(artifact_id: &str, value: String) -> StorageResult<DerivedObjectType> {
    DerivedObjectType::parse(&value).ok_or_else(|| StorageError::InvalidDerivedObjectType {
        artifact_id: artifact_id.to_string(),
        value,
    })
}

fn new_id(prefix: &str) -> String {
    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{counter:x}")
}
