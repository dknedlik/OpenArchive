use std::sync::Arc;

use crate::error::{OpenArchiveError, Result};
use crate::storage::{ArchiveSearchReadStore, DerivedObjectType, SearchCandidateKind};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ArchiveSearchRequest {
    pub query_text: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMatchKind {
    ArtifactTitle,
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
}

impl ArchiveSearchService {
    pub fn new(read_store: Arc<dyn ArchiveSearchReadStore + Send + Sync>) -> Self {
        Self { read_store }
    }

    pub fn search(&self, request: ArchiveSearchRequest) -> Result<ArchiveSearchResponse> {
        let query_text = request.query_text.trim();
        if query_text.is_empty() {
            return Ok(ArchiveSearchResponse { hits: Vec::new() });
        }

        let limit = request.limit.max(1);
        let mut hits = self
            .read_store
            .search_candidates(query_text, limit)
            .map_err(OpenArchiveError::from)?
            .into_iter()
            .map(|candidate| ArchiveSearchHit {
                artifact_id: candidate.artifact_id,
                match_kind: match candidate.match_kind {
                    SearchCandidateKind::ArtifactTitle => SearchMatchKind::ArtifactTitle,
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

        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
                .then_with(|| left.snippet.cmp(&right.snippet))
        });
        hits.truncate(limit);

        Ok(ArchiveSearchResponse { hits })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageResult;
    use crate::storage::{ArchiveSearchCandidate, ArchiveSearchReadStore};

    struct MockSearchReadStore {
        candidates: Vec<ArchiveSearchCandidate>,
    }

    impl ArchiveSearchReadStore for MockSearchReadStore {
        fn search_candidates(
            &self,
            _query_text: &str,
            _limit: usize,
        ) -> StorageResult<Vec<ArchiveSearchCandidate>> {
            Ok(self.candidates.clone())
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
            })
            .expect("search should succeed");

        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].artifact_id, "artifact-1");
    }
}
