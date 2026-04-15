use std::sync::Arc;

use log::warn;

use crate::embedding::EmbeddingProvider;
use crate::error::{OpenArchiveError, Result};
use crate::storage::{
    ArchiveSearchReadStore, DerivedObjectSearchResult, DerivedObjectSearchStore, DerivedObjectType,
    GraphRelatedEntry, ObjectSearchFilters, SearchCandidateKind, SearchFilters,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ArchiveSearchRequest {
    pub query_text: String,
    pub limit: usize,
    #[serde(skip)]
    pub filters: SearchFilters,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMatchKind {
    ArtifactTitle,
    ImportedNoteTag {
        tag_id: String,
    },
    ImportedNoteAlias {
        alias_id: String,
    },
    ImportedNotePath,
    ImportedExternalLink {
        link_id: String,
    },
    DerivedObject {
        derived_object_id: String,
        derived_type: DerivedObjectType,
    },
    SegmentExcerpt {
        segment_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ArchiveSearchHit {
    pub artifact_id: String,
    pub match_kind: SearchMatchKind,
    pub snippet: String,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ArchiveSearchResponse {
    pub hits: Vec<ArchiveSearchHit>,
}

pub struct ArchiveSearchService {
    read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync>,
    semantic_store: Option<Arc<dyn DerivedObjectSearchStore + Send + Sync>>,
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
}

impl ArchiveSearchService {
    pub fn new(read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync>) -> Self {
        Self {
            read_store,
            semantic_store: None,
            embedding_provider: None,
        }
    }

    pub fn with_semantic_fallback(
        read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync>,
        semantic_store: Arc<dyn DerivedObjectSearchStore + Send + Sync>,
        embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    ) -> Self {
        Self {
            read_store,
            semantic_store: Some(semantic_store),
            embedding_provider,
        }
    }

    pub fn search(&self, request: ArchiveSearchRequest) -> Result<ArchiveSearchResponse> {
        let query_text = request.query_text.trim();
        if query_text.is_empty() {
            return Ok(ArchiveSearchResponse { hits: Vec::new() });
        }

        let limit = request.limit.max(1);
        let candidate_limit = expanded_archive_limit(limit);
        let lexical_hits = self
            .read_store
            .search_candidates(query_text, candidate_limit, &request.filters)
            .map_err(OpenArchiveError::from)?
            .into_iter()
            .map(|candidate| ArchiveSearchHit {
                artifact_id: candidate.artifact_id,
                match_kind: match candidate.match_kind {
                    SearchCandidateKind::ArtifactTitle => SearchMatchKind::ArtifactTitle,
                    SearchCandidateKind::ImportedNoteTag => SearchMatchKind::ImportedNoteTag {
                        tag_id: candidate.match_record_id,
                    },
                    SearchCandidateKind::ImportedNoteAlias => SearchMatchKind::ImportedNoteAlias {
                        alias_id: candidate.match_record_id,
                    },
                    SearchCandidateKind::ImportedNotePath => SearchMatchKind::ImportedNotePath,
                    SearchCandidateKind::ImportedExternalLink => {
                        SearchMatchKind::ImportedExternalLink {
                            link_id: candidate.match_record_id,
                        }
                    }
                    SearchCandidateKind::DerivedObject { derived_type } => {
                        SearchMatchKind::DerivedObject {
                            derived_object_id: candidate.match_record_id,
                            derived_type,
                        }
                    }
                    SearchCandidateKind::SegmentExcerpt => SearchMatchKind::SegmentExcerpt {
                        segment_id: candidate.match_record_id,
                    },
                },
                snippet: candidate.snippet,
                score: candidate.score_hint as f32 / 100.0,
            })
            .collect::<Vec<_>>();
        let mut hits = group_archive_hits(lexical_hits);

        if should_supplement_archive_hits(&request.filters, query_text, hits.len(), limit) {
            let semantic_hits = self.semantic_archive_hits(query_text, &request.filters, limit)?;
            append_unique_hits(&mut hits, group_archive_hits(semantic_hits), limit);
        }

        hits.truncate(limit);

        Ok(ArchiveSearchResponse { hits })
    }

    fn semantic_archive_hits(
        &self,
        query_text: &str,
        filters: &SearchFilters,
        limit: usize,
    ) -> Result<Vec<ArchiveSearchHit>> {
        let Some(semantic_store) = &self.semantic_store else {
            return Ok(Vec::new());
        };
        let Some(provider) = &self.embedding_provider else {
            return Ok(Vec::new());
        };

        let mut query_vectors = match provider.embed_texts(&[query_text.to_string()]) {
            Ok(vectors) => vectors,
            Err(err) => {
                warn!(
                    "archive semantic fallback skipped after embedding failure from provider={} model={}: {}",
                    provider.provider_name(),
                    provider.model_name(),
                    err
                );
                return Ok(Vec::new());
            }
        };
        let query_vector = query_vectors.pop().unwrap_or_default();
        if query_vector.is_empty() {
            return Ok(Vec::new());
        }

        let semantic_results = semantic_store
            .search_objects_by_embedding(
                &ObjectSearchFilters {
                    query: None,
                    object_type: filters.object_type,
                    candidate_key: None,
                    artifact_id: None,
                },
                &query_vector,
                expanded_archive_limit(limit),
            )
            .map_err(OpenArchiveError::from)?;

        Ok(semantic_results
            .into_iter()
            .map(|result| {
                let snippet = archive_semantic_snippet(&result);
                ArchiveSearchHit {
                artifact_id: result.artifact_id,
                match_kind: SearchMatchKind::DerivedObject {
                    derived_object_id: result.derived_object_id,
                    derived_type: result.derived_object_type,
                },
                snippet,
                score: result.score.unwrap_or_default(),
            }})
            .collect())
    }
}

fn expanded_archive_limit(limit: usize) -> usize {
    limit.saturating_mul(8).clamp(limit.max(1), 200)
}

fn should_supplement_archive_hits(
    filters: &SearchFilters,
    query_text: &str,
    current_hits: usize,
    limit: usize,
) -> bool {
    if filters.source_type.is_some()
        || filters.tag.is_some()
        || filters.alias.is_some()
        || filters.path_prefix.is_some()
    {
        return false;
    }

    current_hits < limit && query_text.split_whitespace().count() >= 3
}

fn archive_semantic_snippet(result: &DerivedObjectSearchResult) -> String {
    result
        .title
        .clone()
        .or_else(|| result.body_text.clone())
        .unwrap_or_else(|| result.derived_object_type.as_str().to_string())
}

fn group_archive_hits(hits: Vec<ArchiveSearchHit>) -> Vec<ArchiveSearchHit> {
    use std::collections::HashMap;

    let mut best_by_artifact = HashMap::new();
    for hit in hits {
        match best_by_artifact.get_mut(&hit.artifact_id) {
            Some(existing) => {
                if archive_hit_cmp(&hit, existing).is_lt() {
                    *existing = hit;
                }
            }
            None => {
                best_by_artifact.insert(hit.artifact_id.clone(), hit);
            }
        }
    }

    let mut grouped = best_by_artifact.into_values().collect::<Vec<_>>();
    grouped.sort_by(archive_hit_cmp);
    grouped
}

fn append_unique_hits(
    hits: &mut Vec<ArchiveSearchHit>,
    extra_hits: Vec<ArchiveSearchHit>,
    limit: usize,
) {
    for hit in extra_hits {
        if hits.iter().any(|existing| existing.artifact_id == hit.artifact_id) {
            continue;
        }
        hits.push(hit);
        if hits.len() >= limit {
            break;
        }
    }
    hits.sort_by(archive_hit_cmp);
}

fn archive_hit_cmp(left: &ArchiveSearchHit, right: &ArchiveSearchHit) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.artifact_id.cmp(&right.artifact_id))
        .then_with(|| left.snippet.cmp(&right.snippet))
}

pub struct ObjectSearchService {
    store: Arc<dyn DerivedObjectSearchStore + Send + Sync>,
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
}

impl ObjectSearchService {
    pub fn new(
        store: Arc<dyn DerivedObjectSearchStore + Send + Sync>,
        embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    ) -> Self {
        Self {
            store,
            embedding_provider,
        }
    }

    pub fn search(
        &self,
        mut filters: ObjectSearchFilters,
        limit: usize,
    ) -> Result<Vec<DerivedObjectSearchResult>> {
        let limit = limit.max(1);
        filters.query = filters
            .query
            .take()
            .map(|query| query.trim().to_string())
            .filter(|query| !query.is_empty());

        let fetch_limit = limit.saturating_mul(3).min(100);
        let lexical = self
            .store
            .search_objects(&filters, fetch_limit)
            .map_err(OpenArchiveError::from)?;

        let Some(query) = filters.query.clone() else {
            return Ok(truncate_results(lexical, limit));
        };
        let Some(provider) = &self.embedding_provider else {
            return Ok(truncate_results(lexical, limit));
        };

        let mut query_vectors = match provider.embed_texts(&[query]) {
            Ok(vectors) => vectors,
            Err(err) => {
                warn!(
                    "object semantic search falling back to lexical results after embedding failure from provider={} model={}: {}",
                    provider.provider_name(),
                    provider.model_name(),
                    err
                );
                return Ok(truncate_results(lexical, limit));
            }
        };
        let query_vector = query_vectors.pop().unwrap_or_default();
        if query_vector.is_empty() {
            return Ok(truncate_results(lexical, limit));
        }
        let semantic = self
            .store
            .search_objects_by_embedding(&filters, &query_vector, fetch_limit)
            .map_err(OpenArchiveError::from)?;

        Ok(merge_results(lexical, semantic, limit))
    }

    pub fn get_related(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> Result<Vec<GraphRelatedEntry>> {
        let limit = limit.clamp(1, 50);
        self.store
            .get_related_objects(derived_object_id, limit)
            .map_err(OpenArchiveError::from)
    }
}

fn truncate_results(
    mut results: Vec<DerivedObjectSearchResult>,
    limit: usize,
) -> Vec<DerivedObjectSearchResult> {
    results.truncate(limit);
    results
}

fn merge_results(
    lexical: Vec<DerivedObjectSearchResult>,
    semantic: Vec<DerivedObjectSearchResult>,
    limit: usize,
) -> Vec<DerivedObjectSearchResult> {
    use std::collections::HashMap;
    use std::collections::HashSet;

    // Collect semantic IDs so we can tell which lexical results had no semantic match.
    let semantic_ids: HashSet<&str> = semantic
        .iter()
        .map(|r| r.derived_object_id.as_str())
        .collect();

    let mut merged: HashMap<String, DerivedObjectSearchResult> = HashMap::new();

    for mut result in lexical {
        let norm = normalize_lexical_score(result.score.unwrap_or_default());
        // Lexical-only results (no semantic match) get weighted down — their semantic
        // component is effectively 0, so they score `norm * 0.4` rather than the full value.
        result.score = if semantic_ids.contains(result.derived_object_id.as_str()) {
            Some(norm) // will be blended when we process the semantic list
        } else {
            Some(norm * 0.4)
        };
        merged.insert(result.derived_object_id.clone(), result);
    }

    for mut result in semantic {
        let semantic_score = result.score.unwrap_or_default().clamp(0.0, 1.0);
        match merged.get_mut(&result.derived_object_id) {
            Some(existing) => {
                // This entry appeared in both lists — blend the scores.
                let lexical_score = existing.score.unwrap_or_default();
                existing.score = Some((lexical_score * 0.4) + (semantic_score * 0.6));
                if existing.title.is_none() {
                    existing.title = result.title.take();
                }
                if existing.body_text.is_none() {
                    existing.body_text = result.body_text.take();
                }
            }
            None => {
                // Semantic-only: keep full semantic score — penalizing these
                // would bury the best semantic matches below lexical noise.
                result.score = Some(semantic_score);
                merged.insert(result.derived_object_id.clone(), result);
            }
        }
    }

    let mut results = merged.into_values().collect::<Vec<_>>();
    results.sort_by(|left, right| {
        right
            .score
            .unwrap_or_default()
            .total_cmp(&left.score.unwrap_or_default())
            .then_with(|| left.artifact_id.cmp(&right.artifact_id))
            .then_with(|| left.derived_object_id.cmp(&right.derived_object_id))
    });
    results.truncate(limit);
    results
}

fn normalize_lexical_score(score: f32) -> f32 {
    if score <= 0.0 {
        0.0
    } else {
        score / (score + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::{EmbeddingProvider, StubEmbeddingProvider};
    use crate::error::{EmbeddingError, EmbeddingResult, StorageResult};
    use crate::storage::retrieval_read_store::GraphRelatedEntry;
    use crate::storage::{
        ArchiveSearchCandidate, ArchiveSearchReadStore, DerivedObjectSearchStore,
    };

    struct MockSearchReadStore {
        candidates: Vec<ArchiveSearchCandidate>,
    }

    impl ArchiveSearchReadStore for MockSearchReadStore {
        fn search_candidates(
            &self,
            _query_text: &str,
            _limit: usize,
            _filters: &SearchFilters,
        ) -> StorageResult<Vec<ArchiveSearchCandidate>> {
            Ok(self.candidates.clone())
        }
    }

    struct MockObjectSearchStore;

    impl DerivedObjectSearchStore for MockObjectSearchStore {
        fn search_objects(
            &self,
            _filters: &ObjectSearchFilters,
            _limit: usize,
        ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
            Ok(vec![DerivedObjectSearchResult {
                derived_object_id: "obj-1".to_string(),
                artifact_id: "artifact-1".to_string(),
                derived_object_type: DerivedObjectType::Memory,
                title: Some("Lexical".to_string()),
                body_text: Some("lexical match".to_string()),
                candidate_key: None,
                confidence_score: None,
                score: Some(2.0),
            }])
        }

        fn search_objects_by_embedding(
            &self,
            _filters: &ObjectSearchFilters,
            _query_embedding: &[f32],
            _limit: usize,
        ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
            Ok(vec![DerivedObjectSearchResult {
                derived_object_id: "obj-1".to_string(),
                artifact_id: "artifact-1".to_string(),
                derived_object_type: DerivedObjectType::Memory,
                title: Some("Semantic".to_string()),
                body_text: Some("semantic match".to_string()),
                candidate_key: None,
                confidence_score: None,
                score: Some(0.75),
            }])
        }

        fn get_related_objects(
            &self,
            _derived_object_id: &str,
            _limit: usize,
        ) -> StorageResult<Vec<GraphRelatedEntry>> {
            Ok(Vec::new())
        }
    }

    struct FailingEmbeddingProvider;

    impl EmbeddingProvider for FailingEmbeddingProvider {
        fn provider_name(&self) -> &'static str {
            "failing"
        }

        fn model_name(&self) -> &str {
            "failing-model"
        }

        fn dimensions(&self) -> usize {
            3072
        }

        fn embed_texts(&self, _texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>> {
            Err(EmbeddingError::HttpStatus {
                status: 404,
                body_preview: "not found".to_string(),
            })
        }
    }

    #[test]
    fn search_returns_empty_results_for_blank_query() {
        let service = ArchiveSearchService::new(Arc::new(MockSearchReadStore {
            candidates: vec![ArchiveSearchCandidate {
                artifact_id: "artifact-1".to_string(),
                match_record_id: "artifact-1".to_string(),
                match_kind: SearchCandidateKind::ArtifactTitle,
                snippet: "ignored".to_string(),
                score_hint: 300,
            }],
        }));

        let response = service
            .search(ArchiveSearchRequest {
                query_text: "   ".to_string(),
                limit: 5,
                filters: SearchFilters::default(),
            })
            .expect("blank query should succeed");

        assert!(response.hits.is_empty());
    }

    #[test]
    fn search_maps_each_match_kind_and_ranks_hits() {
        let service = ArchiveSearchService::new(Arc::new(MockSearchReadStore {
            candidates: vec![
                ArchiveSearchCandidate {
                    artifact_id: "artifact-2".to_string(),
                    match_record_id: "segment-1".to_string(),
                    match_kind: SearchCandidateKind::SegmentExcerpt,
                    snippet: "segment hit".to_string(),
                    score_hint: 100,
                },
                ArchiveSearchCandidate {
                    artifact_id: "artifact-1".to_string(),
                    match_record_id: "derived-1".to_string(),
                    match_kind: SearchCandidateKind::DerivedObject {
                        derived_type: DerivedObjectType::Memory,
                    },
                    snippet: "memory hit".to_string(),
                    score_hint: 200,
                },
                ArchiveSearchCandidate {
                    artifact_id: "artifact-3".to_string(),
                    match_record_id: "artifact-3".to_string(),
                    match_kind: SearchCandidateKind::ArtifactTitle,
                    snippet: "title hit".to_string(),
                    score_hint: 300,
                },
            ],
        }));

        let response = service
            .search(ArchiveSearchRequest {
                query_text: "hit".to_string(),
                limit: 10,
                filters: SearchFilters::default(),
            })
            .expect("search should succeed");

        assert_eq!(response.hits.len(), 3);
        assert!(matches!(
            response.hits[0].match_kind,
            SearchMatchKind::ArtifactTitle
        ));
        assert!(matches!(
            response.hits[1].match_kind,
            SearchMatchKind::DerivedObject {
                derived_object_id: _,
                derived_type: DerivedObjectType::Memory
            }
        ));
        assert!(matches!(
            response.hits[2].match_kind,
            SearchMatchKind::SegmentExcerpt { segment_id: _ }
        ));
        assert_eq!(response.hits[0].score, 3.0);
        assert_eq!(response.hits[1].score, 2.0);
        assert_eq!(response.hits[2].score, 1.0);
    }

    #[test]
    fn search_applies_limit_after_ranking() {
        let service = ArchiveSearchService::new(Arc::new(MockSearchReadStore {
            candidates: vec![
                ArchiveSearchCandidate {
                    artifact_id: "artifact-1".to_string(),
                    match_record_id: "artifact-1".to_string(),
                    match_kind: SearchCandidateKind::ArtifactTitle,
                    snippet: "title hit".to_string(),
                    score_hint: 300,
                },
                ArchiveSearchCandidate {
                    artifact_id: "artifact-2".to_string(),
                    match_record_id: "derived-1".to_string(),
                    match_kind: SearchCandidateKind::DerivedObject {
                        derived_type: DerivedObjectType::Summary,
                    },
                    snippet: "summary hit".to_string(),
                    score_hint: 200,
                },
            ],
        }));

        let response = service
            .search(ArchiveSearchRequest {
                query_text: "hit".to_string(),
                limit: 1,
                filters: SearchFilters::default(),
            })
            .expect("search should succeed");

        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].artifact_id, "artifact-1");
    }

    #[test]
    fn search_groups_duplicate_artifact_hits_to_best_match() {
        let service = ArchiveSearchService::new(Arc::new(MockSearchReadStore {
            candidates: vec![
                ArchiveSearchCandidate {
                    artifact_id: "artifact-1".to_string(),
                    match_record_id: "segment-1".to_string(),
                    match_kind: SearchCandidateKind::SegmentExcerpt,
                    snippet: "segment hit".to_string(),
                    score_hint: 200,
                },
                ArchiveSearchCandidate {
                    artifact_id: "artifact-1".to_string(),
                    match_record_id: "derived-1".to_string(),
                    match_kind: SearchCandidateKind::DerivedObject {
                        derived_type: DerivedObjectType::Summary,
                    },
                    snippet: "summary hit".to_string(),
                    score_hint: 300,
                },
                ArchiveSearchCandidate {
                    artifact_id: "artifact-2".to_string(),
                    match_record_id: "artifact-2".to_string(),
                    match_kind: SearchCandidateKind::ArtifactTitle,
                    snippet: "title hit".to_string(),
                    score_hint: 250,
                },
            ],
        }));

        let response = service
            .search(ArchiveSearchRequest {
                query_text: "artifact".to_string(),
                limit: 10,
                filters: SearchFilters::default(),
            })
            .expect("search should succeed");

        assert_eq!(response.hits.len(), 2);
        assert_eq!(response.hits[0].artifact_id, "artifact-1");
        assert!(matches!(
            response.hits[0].match_kind,
            SearchMatchKind::DerivedObject {
                derived_object_id: _,
                derived_type: DerivedObjectType::Summary
            }
        ));
    }

    #[test]
    fn search_maps_imported_external_link_hits() {
        let service = ArchiveSearchService::new(Arc::new(MockSearchReadStore {
            candidates: vec![ArchiveSearchCandidate {
                artifact_id: "artifact-1".to_string(),
                match_record_id: "link-1".to_string(),
                match_kind: SearchCandidateKind::ImportedExternalLink,
                snippet: "obsidian://show-plugin?id=obsidian-importer".to_string(),
                score_hint: 390,
            }],
        }));

        let response = service
            .search(ArchiveSearchRequest {
                query_text: "obsidian-importer".to_string(),
                limit: 10,
                filters: SearchFilters::default(),
            })
            .expect("search should succeed");

        assert_eq!(response.hits.len(), 1);
        assert!(matches!(
            response.hits[0].match_kind,
            SearchMatchKind::ImportedExternalLink { ref link_id } if link_id == "link-1"
        ));
    }

    #[test]
    fn search_falls_back_to_semantic_object_hits_when_lexical_archive_hits_are_sparse() {
        let service = ArchiveSearchService::with_semantic_fallback(
            Arc::new(MockSearchReadStore { candidates: Vec::new() }),
            Arc::new(MockObjectSearchStore),
            Some(Arc::new(StubEmbeddingProvider::new("stub".to_string(), 8))),
        );

        let response = service
            .search(ArchiveSearchRequest {
                query_text: "memory retrieval question".to_string(),
                limit: 10,
                filters: SearchFilters::default(),
            })
            .expect("semantic fallback search should succeed");

        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].artifact_id, "artifact-1");
        assert!(matches!(
            response.hits[0].match_kind,
            SearchMatchKind::DerivedObject {
                derived_object_id: _,
                derived_type: DerivedObjectType::Memory
            }
        ));
    }

    #[test]
    fn object_search_merges_lexical_and_semantic_scores() {
        let service = ObjectSearchService::new(
            Arc::new(MockObjectSearchStore),
            Some(Arc::new(StubEmbeddingProvider::new("stub".to_string(), 8))),
        );

        let results = service
            .search(
                ObjectSearchFilters {
                    query: Some("memory".to_string()),
                    object_type: None,
                    candidate_key: None,
                    artifact_id: None,
                },
                10,
            )
            .expect("object search should succeed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].derived_object_id, "obj-1");
        assert!(results[0].score.unwrap_or_default() > 0.0);
    }

    #[test]
    fn object_search_falls_back_to_lexical_results_when_query_embedding_fails() {
        let service = ObjectSearchService::new(
            Arc::new(MockObjectSearchStore),
            Some(Arc::new(FailingEmbeddingProvider)),
        );

        let results = service
            .search(
                ObjectSearchFilters {
                    query: Some("memory".to_string()),
                    object_type: None,
                    candidate_key: None,
                    artifact_id: None,
                },
                10,
            )
            .expect("object search should fall back to lexical results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].derived_object_id, "obj-1");
        assert_eq!(results[0].title.as_deref(), Some("Lexical"));
    }
}
