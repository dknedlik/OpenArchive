use crate::config::OracleConfig;
use crate::db;
use crate::error::{StorageError, StorageResult};
use crate::storage::archive_retrieval_store::ArchiveRetrievalStore;
use crate::storage::types::{RetrievalIntent, RetrievedContextItem};

pub struct OracleArchiveRetrievalStore {
    config: OracleConfig,
}

impl OracleArchiveRetrievalStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl ArchiveRetrievalStore for OracleArchiveRetrievalStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        let conn = db::connect(&self.config)?;
        let mut items = Vec::new();
        for intent in intents {
            let like_pattern = format!("%{}%", intent.query_text.to_ascii_lowercase());
            let derived_type_filter = match intent.intent_type.as_str() {
                "memory_match" => "AND d.derived_object_type = 'memory'",
                "relationship_lookup" => "AND d.derived_object_type = 'relationship'",
                "topic_lookup" => "AND d.derived_object_type IN ('classification', 'summary')",
                "contradiction_check" => {
                    "AND d.derived_object_type IN ('memory', 'relationship')"
                }
                "entity_lookup" => {
                    "AND d.derived_object_type IN ('memory', 'relationship', 'classification', 'summary')"
                }
                _ => "",
            };
            let sql = format!(
                "SELECT * FROM (
                    SELECT d.derived_object_type,
                           d.derived_object_id,
                           d.artifact_id,
                           d.title,
                           d.body_text
                      FROM oa_derived_object d
                     WHERE d.artifact_id <> :1
                       AND d.object_status = 'active'
                       {derived_type_filter}
                       AND (
                            LOWER(NVL(d.title, '')) LIKE :2
                            OR LOWER(NVL(d.body_text, '')) LIKE :2
                            OR LOWER(NVL(d.object_json, '')) LIKE :2
                       )
                ) WHERE ROWNUM <= :3"
            );
            let rows = conn
                .query(
                    &sql,
                    &[&artifact_id, &like_pattern, &(limit_per_intent as i64)],
                )
                .map_err(|source| StorageError::ListArtifacts {
                    source: Box::new(source),
                })?;
            for row_result in rows {
                let row = row_result.map_err(|source| StorageError::ListArtifacts {
                    source: Box::new(source),
                })?;
                items.push(RetrievedContextItem {
                    item_type: row.get::<_, String>(0).unwrap_or_default(),
                    object_id: row.get::<_, String>(1).unwrap_or_default(),
                    artifact_id: row.get::<_, String>(2).unwrap_or_default(),
                    title: row.get::<_, Option<String>>(3).unwrap_or(None),
                    body_text: row.get::<_, Option<String>>(4).unwrap_or(None),
                    supporting_segment_ids: Vec::new(),
                    retrieval_reason: intent.question.clone(),
                    matched_fields: vec!["title_or_body_text".to_string()],
                    rank_score: 100 - (items.len() as i32),
                });
            }
        }
        Ok(items)
    }
}
