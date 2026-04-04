use std::collections::HashSet;

use postgres::Client;

use crate::error::{DbError, StorageError, StorageResult};
use crate::storage::derivation_store::WriteDerivationAttempt;
use crate::storage::types::{NewDerivedObject, NewEvidenceLink, ObjectStatus, ScopeType};
use crate::storage::writeback_store::NewArchiveLink;

fn pg_error(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source: Box::new(source),
    })
}

pub fn insert_derivation_run(
    client: &mut Client,
    connection_string: &str,
    run: &crate::storage::types::NewDerivationRun,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_derivation_run \
             (derivation_run_id, artifact_id, job_id, run_type, pipeline_name, pipeline_version, \
              provider_name, model_name, prompt_version, run_status, input_scope_type, \
              input_scope_json, started_at, completed_at, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::text::jsonb, \
                     $13::text::timestamptz, $14::text::timestamptz, $15)",
            &[
                &run.derivation_run_id,
                &run.artifact_id,
                &run.job_id,
                &run.run_type.as_str(),
                &run.pipeline_name,
                &run.pipeline_version,
                &run.provider_name,
                &run.model_name,
                &run.prompt_version,
                &run.run_status.as_str(),
                &run.input_scope_type.as_str(),
                &run.input_scope_json,
                &run.started_at.as_str(),
                &run.completed_at.as_ref().map(|ts| ts.as_str()),
                &run.error_message,
            ],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(())
}

pub fn insert_derived_object(
    client: &mut Client,
    connection_string: &str,
    object: &crate::storage::types::NewDerivedObject,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_derived_object \
             (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, \
              object_status, confidence_score, confidence_label, scope_type, scope_id, title, \
              body_text, object_json, supersedes_derived_object_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7::double precision, $8, $9, $10, $11, $12, $13::text::jsonb, $14)",
            &[
                &object.derived_object_id,
                &object.artifact_id,
                &object.derivation_run_id,
                &object.payload.derived_object_type().as_str(),
                &object.origin_kind.as_str(),
                &object.object_status.as_str(),
                &object.confidence_score,
                &object.confidence_label,
                &object.scope_type.as_str(),
                &object.scope_id,
                &object.payload.title(),
                &object.payload.body_text(),
                &object.payload.object_json(),
                &object.supersedes_derived_object_id,
            ],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(())
}

pub fn supersede_active_derived_objects(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<()> {
    client
        .execute(
            "UPDATE oa_derived_object \
             SET object_status = 'superseded' \
             WHERE artifact_id = $1 AND object_status = 'active'",
            &[&artifact_id],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(())
}

pub fn insert_evidence_link(
    client: &mut Client,
    connection_string: &str,
    link: &NewEvidenceLink,
) -> StorageResult<()> {
    let evidence_rank =
        i32::try_from(link.evidence_rank).map_err(|_| StorageError::InvalidDerivationWrite {
            detail: format!(
                "evidence link {} rank {} exceeds Postgres INTEGER range",
                link.evidence_link_id, link.evidence_rank
            ),
        })?;

    client
        .execute(
            "INSERT INTO oa_evidence_link \
             (evidence_link_id, derived_object_id, segment_id, evidence_role, evidence_rank, support_strength) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &link.evidence_link_id,
                &link.derived_object_id,
                &link.segment_id,
                &link.evidence_role.as_str(),
                &evidence_rank,
                &link.support_strength.as_str(),
            ],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(())
}

pub fn insert_archive_link(
    client: &mut Client,
    connection_string: &str,
    link: &NewArchiveLink,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_archive_link \
             (archive_link_id, source_object_id, target_object_id, link_type, \
              confidence_score, rationale, origin_kind, contributed_by) \
             VALUES ($1, $2, $3, $4, $5::double precision, $6, 'inferred', $7) \
             ON CONFLICT (source_object_id, target_object_id, link_type) DO UPDATE \
             SET confidence_score = COALESCE(EXCLUDED.confidence_score, oa_archive_link.confidence_score), \
                 rationale = COALESCE(EXCLUDED.rationale, oa_archive_link.rationale), \
                 origin_kind = COALESCE(EXCLUDED.origin_kind, oa_archive_link.origin_kind), \
                 contributed_by = COALESCE(EXCLUDED.contributed_by, oa_archive_link.contributed_by)",
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
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(())
}

pub fn validate_derivation_attempt(
    client: &mut Client,
    connection_string: &str,
    attempt: &WriteDerivationAttempt,
) -> StorageResult<()> {
    let artifact_id = &attempt.run.artifact_id;
    if !artifact_exists(client, connection_string, artifact_id)? {
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
        if matches!(object.object_status, ObjectStatus::Active)
            && object_write.evidence_links.is_empty()
        {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "active derived object {} must have at least one evidence link",
                    object.derived_object_id
                ),
            });
        }
        validate_scope(client, connection_string, artifact_id, object)?;

        for link in &object_write.evidence_links {
            if link.derived_object_id != object.derived_object_id {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!(
                        "evidence link {} targets derived object {} but enclosing object is {}",
                        link.evidence_link_id, link.derived_object_id, object.derived_object_id
                    ),
                });
            }
            let segment_artifact_id =
                load_segment_artifact_id(client, connection_string, &link.segment_id)?;
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
            && !derived_object_exists(client, connection_string, &link.source_object_id)?
        {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "archive link {} references missing source object {}",
                    link.archive_link_id, link.source_object_id
                ),
            });
        }
        if !valid_new_object_ids.contains(link.target_object_id.as_str())
            && !derived_object_exists(client, connection_string, &link.target_object_id)?
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
    client: &mut Client,
    connection_string: &str,
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
            let scope_artifact_id =
                load_segment_artifact_id(client, connection_string, &object.scope_id)?;
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

fn artifact_exists(
    client: &mut Client,
    connection_string: &str,
    artifact_id: &str,
) -> StorageResult<bool> {
    let row = client
        .query_opt(
            "SELECT artifact_id FROM oa_artifact WHERE artifact_id = $1",
            &[&artifact_id],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(row.map(|row| row.get::<_, String>(0)).as_deref() == Some(artifact_id))
}

fn derived_object_exists(
    client: &mut Client,
    connection_string: &str,
    derived_object_id: &str,
) -> StorageResult<bool> {
    let row = client
        .query_opt(
            "SELECT derived_object_id FROM oa_derived_object WHERE derived_object_id = $1",
            &[&derived_object_id],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(row.map(|row| row.get::<_, String>(0)).as_deref() == Some(derived_object_id))
}

fn load_segment_artifact_id(
    client: &mut Client,
    connection_string: &str,
    segment_id: &str,
) -> StorageResult<Option<String>> {
    let row = client
        .query_opt(
            "SELECT artifact_id FROM oa_segment WHERE segment_id = $1",
            &[&segment_id],
        )
        .map_err(|source| pg_error(connection_string, source))?;

    Ok(row.map(|row| row.get::<_, String>(0)))
}
