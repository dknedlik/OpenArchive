use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StorageResult;
use crate::storage::retrieval_read_store::{
    CrossArtifactReadStore, DerivedObjectLookupStore, DerivedObjectSearchResult,
    DerivedObjectSearchStore, GraphRelatedEntry, ObjectSearchFilters, RelatedDerivedObject,
    RelatedDerivedObjectEmbeddingMatch, VectorSearchHit, VectorSearchStore,
};
use crate::storage::types::DerivedObjectType;

const VECTOR_FETCH_LIMIT_CAP: usize = 512;

pub struct CompositeDerivedObjectSearchStore {
    lexical_store: Arc<dyn DerivedObjectSearchStore + Send + Sync>,
    lookup_store: Arc<dyn DerivedObjectLookupStore + Send + Sync>,
    cross_artifact_store: Arc<dyn CrossArtifactReadStore + Send + Sync>,
    vector_store: Arc<dyn VectorSearchStore + Send + Sync>,
}

impl CompositeDerivedObjectSearchStore {
    pub fn new(
        lexical_store: Arc<dyn DerivedObjectSearchStore + Send + Sync>,
        lookup_store: Arc<dyn DerivedObjectLookupStore + Send + Sync>,
        cross_artifact_store: Arc<dyn CrossArtifactReadStore + Send + Sync>,
        vector_store: Arc<dyn VectorSearchStore + Send + Sync>,
    ) -> Self {
        Self {
            lexical_store,
            lookup_store,
            cross_artifact_store,
            vector_store,
        }
    }
}

impl DerivedObjectSearchStore for CompositeDerivedObjectSearchStore {
    fn search_objects(
        &self,
        filters: &ObjectSearchFilters,
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        self.lexical_store.search_objects(filters, limit)
    }

    fn search_objects_by_embedding(
        &self,
        filters: &ObjectSearchFilters,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        let hits = self.vector_store.search_by_embedding(
            filters,
            query_embedding,
            expanded_vector_limit(limit),
        )?;
        let hydrated = self.lookup_store.load_active_objects_by_ids(
            &hits
                .iter()
                .map(|hit| hit.derived_object_id.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(hydrate_vector_results(hits, hydrated, filters, limit))
    }

    fn get_related_objects(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<GraphRelatedEntry>> {
        self.lexical_store
            .get_related_objects(derived_object_id, limit)
    }
}

impl CrossArtifactReadStore for CompositeDerivedObjectSearchStore {
    fn find_related_by_candidate_keys(
        &self,
        artifact_id: &str,
        candidate_keys: &[String],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>> {
        self.cross_artifact_store
            .find_related_by_candidate_keys(artifact_id, candidate_keys, limit)
    }

    fn find_related_by_embedding(
        &self,
        artifact_id: &str,
        derived_object_type: DerivedObjectType,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>> {
        let hits = self.vector_store.find_related_by_embedding(
            artifact_id,
            derived_object_type,
            query_embedding,
            expanded_vector_limit(limit),
        )?;
        let hydrated = self.lookup_store.load_active_objects_by_ids(
            &hits
                .iter()
                .map(|hit| hit.derived_object_id.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(hydrate_related_results(
            hits,
            hydrated,
            artifact_id,
            derived_object_type,
            limit,
        ))
    }
}

fn hydrate_vector_results(
    hits: Vec<VectorSearchHit>,
    hydrated: Vec<DerivedObjectSearchResult>,
    filters: &ObjectSearchFilters,
    limit: usize,
) -> Vec<DerivedObjectSearchResult> {
    let mut hydrated_by_id = hydrated
        .into_iter()
        .map(|result| (result.derived_object_id.clone(), result))
        .collect::<HashMap<_, _>>();

    let mut results = Vec::new();
    for hit in hits {
        let Some(mut result) = hydrated_by_id.remove(&hit.derived_object_id) else {
            continue;
        };
        if !matches_object_filters(&result, filters) {
            continue;
        }
        result.score = Some(hit.score);
        results.push(result);
        if results.len() >= limit {
            break;
        }
    }
    results
}

fn hydrate_related_results(
    hits: Vec<VectorSearchHit>,
    hydrated: Vec<DerivedObjectSearchResult>,
    artifact_id: &str,
    derived_object_type: DerivedObjectType,
    limit: usize,
) -> Vec<RelatedDerivedObjectEmbeddingMatch> {
    let mut hydrated_by_id = hydrated
        .into_iter()
        .map(|result| (result.derived_object_id.clone(), result))
        .collect::<HashMap<_, _>>();

    let mut results = Vec::new();
    for hit in hits {
        let Some(result) = hydrated_by_id.remove(&hit.derived_object_id) else {
            continue;
        };
        if result.artifact_id == artifact_id || result.derived_object_type != derived_object_type {
            continue;
        }
        results.push(RelatedDerivedObjectEmbeddingMatch {
            derived_object_id: result.derived_object_id,
            artifact_id: result.artifact_id,
            derived_object_type: result.derived_object_type,
            title: result.title,
            body_text: result.body_text,
            candidate_key: result.candidate_key,
            confidence_score: result.confidence_score,
            similarity_score: hit.score,
        });
        if results.len() >= limit {
            break;
        }
    }
    results
}

fn matches_object_filters(
    result: &DerivedObjectSearchResult,
    filters: &ObjectSearchFilters,
) -> bool {
    if let Some(object_type) = filters.object_type {
        if result.derived_object_type != object_type {
            return false;
        }
    }
    if let Some(artifact_id) = filters.artifact_id.as_deref() {
        if result.artifact_id != artifact_id {
            return false;
        }
    }
    if let Some(candidate_key) = filters.candidate_key.as_deref() {
        if result.candidate_key.as_deref() != Some(candidate_key) {
            return false;
        }
    }
    true
}

fn expanded_vector_limit(limit: usize) -> usize {
    limit
        .saturating_mul(8)
        .clamp(limit.max(1), VECTOR_FETCH_LIMIT_CAP)
}
