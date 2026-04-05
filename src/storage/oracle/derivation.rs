use std::collections::HashSet;

use oracle::Connection;

use crate::error::{StorageError, StorageResult};
use crate::storage::derivation_store::WriteDerivationAttempt;
use crate::storage::types::{NewDerivationRun, NewDerivedObject, NewEvidenceLink, ScopeType};
use crate::storage::writeback_store::NewArchiveLink;

pub fn insert_derivation_run(conn: &Connection, run: &NewDerivationRun) -> StorageResult<()> {
    let run_type = run.run_type.as_str();
    let run_status = run.run_status.as_str();
    let input_scope_type = run.input_scope_type.as_str();
    let started_at = run.started_at.as_str();
    let completed_at = run.completed_at.as_ref().map(|ts| ts.as_str());

    conn.execute(
        "INSERT INTO oa_derivation_run \
         (derivation_run_id, artifact_id, job_id, run_type, pipeline_name, pipeline_version, \
          provider_name, model_name, prompt_version, run_status, input_scope_type, \
          input_scope_json, started_at, completed_at, error_message) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, \
                 TO_TIMESTAMP_TZ(:13, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                 TO_TIMESTAMP_TZ(:14, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), :15)",
        &[
            &run.derivation_run_id,
            &run.artifact_id,
            &run.job_id,
            &run_type,
            &run.pipeline_name,
            &run.pipeline_version,
            &run.provider_name,
            &run.model_name,
            &run.prompt_version,
            &run_status,
            &input_scope_type,
            &run.input_scope_json,
            &started_at,
            &completed_at,
            &run.error_message,
        ],
    )
    .map_err(|source| StorageError::InsertDerivationRun {
        derivation_run_id: run.derivation_run_id.clone(),
        artifact_id: run.artifact_id.clone(),
        source: Box::new(source),
    })?;

    Ok(())
}

pub fn insert_derived_object(conn: &Connection, object: &NewDerivedObject) -> StorageResult<()> {
    let derived_object_type = object.payload.derived_object_type().as_str();
    let origin_kind = object.origin_kind.as_str();
    let object_status = object.object_status.as_str();
    let scope_type = object.scope_type.as_str();
    let title = object.payload.title();
    let body_text = object.payload.body_text();
    let object_json = object.payload.object_json();

    conn.execute(
        "INSERT INTO oa_derived_object \
         (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, \
          object_status, confidence_score, confidence_label, scope_type, scope_id, title, \
          body_text, object_json, supersedes_derived_object_id) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)",
        &[
            &object.derived_object_id,
            &object.artifact_id,
            &object.derivation_run_id,
            &derived_object_type,
            &origin_kind,
            &object_status,
            &object.confidence_score,
            &object.confidence_label,
            &scope_type,
            &object.scope_id,
            &title,
            &body_text,
            &object_json,
            &object.supersedes_derived_object_id,
        ],
    )
    .map_err(|source| StorageError::InsertDerivedObject {
        derived_object_id: object.derived_object_id.clone(),
        artifact_id: object.artifact_id.clone(),
        source: Box::new(source),
    })?;

    Ok(())
}

pub fn supersede_active_derived_objects(conn: &Connection, artifact_id: &str) -> StorageResult<()> {
    conn.execute(
        "UPDATE oa_derived_object \
         SET object_status = 'superseded' \
         WHERE artifact_id = :1 AND object_status = 'active'",
        &[&artifact_id],
    )
    .map_err(|source| StorageError::UpdateDerivedObjectStatus {
        artifact_id: artifact_id.to_string(),
        source: Box::new(source),
    })?;

    Ok(())
}

pub fn insert_evidence_link(conn: &Connection, link: &NewEvidenceLink) -> StorageResult<()> {
    let evidence_role = link.evidence_role.as_str();
    let support_strength = link.support_strength.as_str();

    conn.execute(
        "INSERT INTO oa_evidence_link \
         (evidence_link_id, derived_object_id, segment_id, evidence_role, evidence_rank, support_strength) \
         VALUES (:1, :2, :3, :4, :5, :6)",
        &[
            &link.evidence_link_id,
            &link.derived_object_id,
            &link.segment_id,
            &evidence_role,
            &link.evidence_rank,
            &support_strength,
        ],
    )
    .map_err(|source| StorageError::InsertEvidenceLink {
        evidence_link_id: link.evidence_link_id.clone(),
        derived_object_id: link.derived_object_id.clone(),
        source: Box::new(source),
    })?;

    Ok(())
}

pub fn insert_archive_link(conn: &Connection, link: &NewArchiveLink) -> StorageResult<()> {
    conn.execute(
        "MERGE INTO oa_archive_link t
         USING (
            SELECT :1 AS archive_link_id,
                   :2 AS source_object_id,
                   :3 AS target_object_id,
                   :4 AS link_type,
                   :5 AS confidence_score,
                   :6 AS rationale,
                   :7 AS contributed_by
              FROM dual
         ) s
         ON (t.source_object_id = s.source_object_id
             AND t.target_object_id = s.target_object_id
             AND t.link_type = s.link_type)
         WHEN MATCHED THEN UPDATE SET
             t.confidence_score = COALESCE(s.confidence_score, t.confidence_score),
             t.rationale = COALESCE(s.rationale, t.rationale),
             t.origin_kind = COALESCE('inferred', t.origin_kind),
             t.contributed_by = COALESCE(s.contributed_by, t.contributed_by)
         WHEN NOT MATCHED THEN INSERT
             (archive_link_id, source_object_id, target_object_id, link_type,
              confidence_score, rationale, origin_kind, contributed_by)
             VALUES (s.archive_link_id, s.source_object_id, s.target_object_id, s.link_type,
                     s.confidence_score, s.rationale, 'inferred', s.contributed_by)",
        &[
            &link.archive_link_id,
            &link.source_object_id,
            &link.target_object_id,
            &link.link_type,
            &link.confidence_score,
            &link.rationale,
            &link.contributed_by,
        ],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!(
            "failed to insert archive link {}: {}",
            link.archive_link_id, source
        ),
    })?;

    Ok(())
}

pub fn validate_derivation_attempt(
    conn: &Connection,
    attempt: &WriteDerivationAttempt,
) -> StorageResult<()> {
    let artifact_id = &attempt.run.artifact_id;
    if !artifact_exists(conn, artifact_id)? {
        return Err(StorageError::InvalidDerivationWrite {
            detail: format!("artifact {} does not exist", artifact_id),
        });
    }

    let mut derived_object_ids = HashSet::with_capacity(attempt.objects.len());
    let valid_new_object_ids = attempt
        .objects
        .iter()
        .map(|object_write| object_write.object.derived_object_id.as_str())
        .collect::<HashSet<_>>();
    for object_write in &attempt.objects {
        let object = &object_write.object;
        if object.derivation_run_id != attempt.run.derivation_run_id {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "derived object {} references derivation run {} but attempt run is {}",
                    object.derived_object_id,
                    object.derivation_run_id,
                    attempt.run.derivation_run_id
                ),
            });
        }
        if object.artifact_id != *artifact_id {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "derived object {} belongs to artifact {} but attempt run artifact is {}",
                    object.derived_object_id, object.artifact_id, artifact_id
                ),
            });
        }
        if !derived_object_ids.insert(object.derived_object_id.clone()) {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("duplicate derived object id {}", object.derived_object_id),
            });
        }
        validate_scope(conn, artifact_id, object)?;

        for link in &object_write.evidence_links {
            if link.derived_object_id != object.derived_object_id {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "evidence link {} targets derived object {} but enclosing object is {}",
                        link.evidence_link_id, link.derived_object_id, object.derived_object_id
                    ),
                });
            }
            let segment_artifact_id = load_segment_artifact_id(conn, &link.segment_id)?;
            if segment_artifact_id.as_deref() != Some(artifact_id.as_str()) {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "evidence link {} references segment {} outside artifact {}",
                        link.evidence_link_id, link.segment_id, artifact_id
                    ),
                });
            }
        }
    }

    for link in &attempt.archive_links {
        if link.source_object_id == link.target_object_id {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "archive link {} must not reference the same object {}",
                    link.archive_link_id, link.source_object_id
                ),
            });
        }
        if !valid_new_object_ids.contains(link.source_object_id.as_str())
            && !derived_object_exists(conn, &link.source_object_id)?
        {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "archive link {} references missing source object {}",
                    link.archive_link_id, link.source_object_id
                ),
            });
        }
        if !valid_new_object_ids.contains(link.target_object_id.as_str())
            && !derived_object_exists(conn, &link.target_object_id)?
        {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "archive link {} references missing target object {}",
                    link.archive_link_id, link.target_object_id
                ),
            });
        }
    }

    Ok(())
}

fn validate_scope(
    conn: &Connection,
    artifact_id: &str,
    object: &NewDerivedObject,
) -> StorageResult<()> {
    match object.scope_type {
        ScopeType::Artifact => {
            if object.scope_id != artifact_id {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "artifact-scoped object {} must use scope_id {} but found {}",
                        object.derived_object_id, artifact_id, object.scope_id
                    ),
                });
            }
        }
        ScopeType::Segment => {
            let scope_artifact_id = load_segment_artifact_id(conn, &object.scope_id)?;
            if scope_artifact_id.as_deref() != Some(artifact_id) {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "segment-scoped object {} references segment {} outside artifact {}",
                        object.derived_object_id, object.scope_id, artifact_id
                    ),
                });
            }
        }
    }

    Ok(())
}

fn artifact_exists(conn: &Connection, artifact_id: &str) -> StorageResult<bool> {
    conn.query_row_as::<(String,)>(
        "SELECT artifact_id FROM oa_artifact WHERE artifact_id = :1",
        &[&artifact_id],
    )
    .map(|(found_artifact_id,)| found_artifact_id == artifact_id)
    .or_else(|source| match source.kind() {
        oracle::ErrorKind::NoDataFound => Ok(false),
        _ => Err(StorageError::ValidateArtifactOwnership {
            artifact_id: artifact_id.to_string(),
            source: Box::new(source),
        }),
    })
}

fn derived_object_exists(conn: &Connection, derived_object_id: &str) -> StorageResult<bool> {
    conn.query_row_as::<(String,)>(
        "SELECT derived_object_id FROM oa_derived_object WHERE derived_object_id = :1",
        &[&derived_object_id],
    )
    .map(|(found_derived_object_id,)| found_derived_object_id == derived_object_id)
    .or_else(|source| match source.kind() {
        oracle::ErrorKind::NoDataFound => Ok(false),
        _ => Err(StorageError::InvalidDerivationWrite {
            detail: format!(
                "failed to validate derived object {} ownership: {}",
                derived_object_id, source
            ),
        }),
    })
}

fn load_segment_artifact_id(conn: &Connection, segment_id: &str) -> StorageResult<Option<String>> {
    conn.query_row_as::<(String,)>(
        "SELECT artifact_id FROM oa_segment WHERE segment_id = :1",
        &[&segment_id],
    )
    .map(|(artifact_id,)| Some(artifact_id))
    .or_else(|source| match source.kind() {
        oracle::ErrorKind::NoDataFound => Ok(None),
        _ => Err(StorageError::ValidateSegmentOwnership {
            segment_id: segment_id.to_string(),
            source: Box::new(source),
        }),
    })
}
