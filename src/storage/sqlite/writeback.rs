use rusqlite::{params, TransactionBehavior};

use crate::error::StorageError;
use crate::storage::writeback_store::{
    NewAgentEntity, NewAgentMemory, NewArchiveLink, UpdateObjectStatus, WritebackStore,
};

use super::links::sync_reconcile_links_for_archive_links;
use super::{db_err, open_connection, SqliteWritebackStore, StorageResult};

impl WritebackStore for SqliteWritebackStore {
    fn store_agent_memory(&self, memory: &NewAgentMemory) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let derivation_run_id = format!("agentrun-{}", memory.derived_object_id);
        tx.execute(
            "INSERT INTO oa_derivation_run
             (derivation_run_id, artifact_id, run_type, pipeline_name, pipeline_version, provider_name,
              run_status, input_scope_type, input_scope_json, started_at, completed_at)
             VALUES (?1, ?2, 'agent_contributed', 'agent_writeback', '1.0', ?3,
                     'completed', 'artifact', '{}', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            params![derivation_run_id, memory.artifact_id, memory.contributed_by],
        )
        .map_err(|source| db_err(&self.config, source))?;
        let object_json = serde_json::json!({
            "memory_type": memory.memory_type,
            "candidate_key": memory.candidate_key.as_deref().unwrap_or(""),
            "memory_scope": "artifact",
            "memory_scope_value": memory.artifact_id,
        })
        .to_string();
        tx.execute(
            "INSERT INTO oa_derived_object
             (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status,
              scope_type, scope_id, title, body_text, object_json)
             VALUES (?1, ?2, ?3, 'memory', 'agent_contributed', 'active', 'artifact', ?4, ?5, ?6, ?7)",
            params![
                memory.derived_object_id,
                memory.artifact_id,
                derivation_run_id,
                memory.artifact_id,
                memory.title,
                memory.body_text,
                object_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn store_archive_link(&self, link: &NewArchiveLink) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "INSERT INTO oa_archive_link
             (archive_link_id, source_object_id, target_object_id, link_type, confidence_score, rationale, origin_kind, contributed_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'agent_contributed', ?7)
             ON CONFLICT(source_object_id, target_object_id, link_type) DO UPDATE SET
                 confidence_score = COALESCE(excluded.confidence_score, oa_archive_link.confidence_score),
                 rationale = COALESCE(excluded.rationale, oa_archive_link.rationale),
                 contributed_by = COALESCE(excluded.contributed_by, oa_archive_link.contributed_by)",
            params![
                link.archive_link_id,
                link.source_object_id,
                link.target_object_id,
                link.link_type,
                link.confidence_score,
                link.rationale,
                link.contributed_by
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        sync_reconcile_links_for_archive_links(&tx, std::slice::from_ref(link))?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn update_object_status(&self, update: &UpdateObjectStatus) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        let rows = connection
            .execute(
                "UPDATE oa_derived_object
                 SET object_status = ?2,
                     supersedes_derived_object_id = ?3
                 WHERE derived_object_id = ?1
                   AND object_status = 'active'",
                params![
                    update.derived_object_id,
                    update.new_status.as_str(),
                    update.replacement_object_id
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
        if rows == 0 {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "no active derived object found with id {} to update",
                    update.derived_object_id
                ),
            });
        }
        Ok(())
    }

    fn store_agent_entity(&self, entity: &NewAgentEntity) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let derivation_run_id = format!("agentrun-{}", entity.derived_object_id);
        tx.execute(
            "INSERT INTO oa_derivation_run
             (derivation_run_id, artifact_id, run_type, pipeline_name, pipeline_version, provider_name,
              run_status, input_scope_type, input_scope_json, started_at, completed_at)
             VALUES (?1, ?2, 'agent_contributed', 'agent_writeback', '1.0', ?3,
                     'completed', 'artifact', '{}', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            params![derivation_run_id, entity.artifact_id, entity.contributed_by],
        )
        .map_err(|source| db_err(&self.config, source))?;
        let object_json = serde_json::json!({
            "entity_type": entity.entity_type,
            "candidate_key": entity.candidate_key.as_deref().unwrap_or(""),
        })
        .to_string();
        tx.execute(
            "INSERT INTO oa_derived_object
             (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status,
              scope_type, scope_id, title, body_text, object_json)
             VALUES (?1, ?2, ?3, 'entity', 'agent_contributed', 'active', 'artifact', ?4, ?5, ?6, ?7)",
            params![
                entity.derived_object_id,
                entity.artifact_id,
                derivation_run_id,
                entity.artifact_id,
                entity.title,
                entity.body_text,
                object_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }
}
