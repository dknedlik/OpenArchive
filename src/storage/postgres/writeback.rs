use postgres::Client;

use crate::error::{DbError, StorageError, StorageResult};
use crate::storage::writeback_store::{NewAgentMemory, NewArchiveLink};

fn pg_error(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source,
    })
}

/// Insert a minimal derivation_run for agent-contributed writes, then insert
/// the derived object. Runs in a single transaction.
pub fn store_agent_memory(
    client: &mut Client,
    connection_string: &str,
    memory: &NewAgentMemory,
) -> StorageResult<()> {
    let now = chrono::Utc::now();
    let now_str = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, false);

    let derivation_run_id = format!("agentrun-{}", &memory.derived_object_id);
    let artifact_id = memory.artifact_id.as_str();
    let scope_type = "artifact";

    client
        .batch_execute("BEGIN")
        .map_err(|source| pg_error(connection_string, source))?;

    let result = (|| -> StorageResult<()> {
        // 1. Insert a lightweight derivation_run
        client
            .execute(
                "INSERT INTO oa_derivation_run \
                 (derivation_run_id, artifact_id, job_id, run_type, pipeline_name, pipeline_version, \
                  provider_name, model_name, prompt_version, run_status, input_scope_type, \
                  input_scope_json, started_at, completed_at) \
                 VALUES ($1, $2, NULL, 'agent_contributed', 'agent_writeback', '1.0', \
                         $3, NULL, NULL, 'completed', 'artifact', \
                         '{}'::jsonb, $4::text::timestamptz, $4::text::timestamptz)",
                &[
                    &derivation_run_id,
                    &artifact_id,
                    &memory.contributed_by,
                    &now_str,
                ],
            )
            .map_err(|source| pg_error(connection_string, source))?;

        // 2. Build object_json
        let object_json = serde_json::json!({
            "memory_type": memory.memory_type,
            "candidate_key": memory.candidate_key.as_deref().unwrap_or(""),
            "memory_scope": scope_type,
            "memory_scope_value": artifact_id,
        });

        // 3. Insert derived_object
        client
            .execute(
                "INSERT INTO oa_derived_object \
                 (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, \
                  object_status, confidence_score, confidence_label, scope_type, scope_id, title, \
                  body_text, object_json, supersedes_derived_object_id) \
                 VALUES ($1, $2, $3, 'memory', 'agent_contributed', \
                         'active', NULL, NULL, $4, $5, $6, $7, $8::text::jsonb, NULL)",
                &[
                    &memory.derived_object_id,
                    &artifact_id,
                    &derivation_run_id,
                    &scope_type,
                    &artifact_id,
                    &memory.title,
                    &memory.body_text,
                    &object_json.to_string(),
                ],
            )
            .map_err(|source| pg_error(connection_string, source))?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            client
                .batch_execute("COMMIT")
                .map_err(|source| pg_error(connection_string, source))?;
            Ok(())
        }
        Err(err) => {
            let _ = client.batch_execute("ROLLBACK");
            Err(err)
        }
    }
}

pub fn store_archive_link(
    client: &mut Client,
    connection_string: &str,
    link: &NewArchiveLink,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_archive_link \
             (archive_link_id, source_object_id, target_object_id, link_type, \
              confidence_score, rationale, origin_kind, contributed_by) \
             VALUES ($1, $2, $3, $4, $5::double precision, $6, 'agent_contributed', $7) \
             ON CONFLICT (source_object_id, target_object_id, link_type) DO UPDATE \
             SET confidence_score = COALESCE(EXCLUDED.confidence_score, oa_archive_link.confidence_score), \
                 rationale = COALESCE(EXCLUDED.rationale, oa_archive_link.rationale), \
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
