use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{OpenArchiveError, Result};
use crate::storage::{
    DerivedObjectType, EnrichmentStatus, NewReviewDecision, ReviewCandidate, ReviewDecisionStatus,
    ReviewItemKind, ReviewQueueFilters, ReviewStore, SourceType,
};

const MAX_REVIEW_ITEMS: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewPriority {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAction {
    InspectArtifact,
    InspectContextPack,
    InspectRelatedObjects,
    RejectObject,
    SupersedeObject,
    LinkObjects,
    RetryArtifact,
    NoteItem,
    DismissItem,
    ResolveItem,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReviewQueueRequest {
    pub filters: ReviewQueueFilters,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ReviewQueueItem {
    pub item_id: String,
    pub kind: ReviewItemKind,
    pub priority: ReviewPriority,
    pub artifact_id: String,
    pub derived_object_id: Option<String>,
    pub source_type: SourceType,
    pub captured_at: String,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub derived_object_type: Option<DerivedObjectType>,
    pub candidate_key: Option<String>,
    pub enrichment_status: Option<EnrichmentStatus>,
    pub confidence_score: Option<f64>,
    pub related_artifact_count: Option<usize>,
    pub reason: String,
    pub recommended_actions: Vec<ReviewAction>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ReviewQueueResponse {
    pub items: Vec<ReviewQueueItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReviewDecisionRequest {
    pub kind: ReviewItemKind,
    pub artifact_id: String,
    pub derived_object_id: Option<String>,
    pub decision_status: ReviewDecisionStatus,
    pub note_text: Option<String>,
    pub decided_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RetryArtifactRequest {
    pub artifact_id: String,
}

pub struct ReviewService {
    store: Arc<dyn ReviewStore + Send + Sync>,
}

impl ReviewService {
    pub fn new(store: Arc<dyn ReviewStore + Send + Sync>) -> Self {
        Self { store }
    }

    pub fn list(&self, request: ReviewQueueRequest) -> Result<ReviewQueueResponse> {
        let limit = request.limit.clamp(1, MAX_REVIEW_ITEMS);
        let mut items = self
            .store
            .list_review_candidates(&request.filters, limit)
            .map_err(OpenArchiveError::from)?
            .into_iter()
            .map(map_candidate)
            .collect::<Vec<_>>();

        items.sort_by(|left, right| {
            priority_rank(left.priority)
                .cmp(&priority_rank(right.priority))
                .then_with(|| right.captured_at.cmp(&left.captured_at))
                .then_with(|| left.item_id.cmp(&right.item_id))
        });
        items.truncate(limit);

        Ok(ReviewQueueResponse { items })
    }

    pub fn record_decision(&self, request: ReviewDecisionRequest) -> Result<String> {
        validate_decision_request(&request)?;
        let note_text = request
            .note_text
            .map(|note| note.trim().to_string())
            .filter(|note| !note.is_empty());
        if request.decision_status == ReviewDecisionStatus::Noted && note_text.is_none() {
            return Err(OpenArchiveError::Invariant(
                "review notes require non-empty note_text".to_string(),
            ));
        }

        let review_decision_id = new_id("reviewdec");
        self.store
            .record_review_decision(&NewReviewDecision {
                review_decision_id: review_decision_id.clone(),
                item_id: build_item_id(
                    request.kind,
                    &request.artifact_id,
                    request.derived_object_id.as_deref(),
                ),
                item_kind: request.kind,
                artifact_id: request.artifact_id,
                derived_object_id: request.derived_object_id,
                decision_status: request.decision_status,
                note_text,
                decided_by: request.decided_by,
            })
            .map_err(OpenArchiveError::from)?;
        Ok(review_decision_id)
    }

    pub fn retry_artifact(&self, request: RetryArtifactRequest) -> Result<String> {
        self.store
            .retry_artifact_enrichment(&request.artifact_id)
            .map_err(OpenArchiveError::from)
    }
}

fn map_candidate(candidate: ReviewCandidate) -> ReviewQueueItem {
    ReviewQueueItem {
        item_id: build_item_id(
            candidate.kind,
            &candidate.artifact_id,
            candidate.derived_object_id.as_deref(),
        ),
        priority: priority_for_kind(candidate.kind),
        reason: reason_for_candidate(&candidate),
        recommended_actions: recommended_actions(candidate.kind),
        artifact_id: candidate.artifact_id,
        derived_object_id: candidate.derived_object_id,
        source_type: candidate.source_type,
        captured_at: candidate.captured_at,
        title: candidate.title,
        body_text: candidate.body_text,
        derived_object_type: candidate.derived_object_type,
        candidate_key: candidate.candidate_key,
        enrichment_status: candidate.enrichment_status,
        confidence_score: candidate.confidence_score,
        related_artifact_count: candidate.related_artifact_count,
        kind: candidate.kind,
    }
}

fn build_item_id(
    kind: ReviewItemKind,
    artifact_id: &str,
    derived_object_id: Option<&str>,
) -> String {
    match derived_object_id {
        Some(derived_object_id) => format!("review:{}:{derived_object_id}", kind.as_str()),
        None => format!("review:{}:{artifact_id}", kind.as_str()),
    }
}

fn validate_decision_request(request: &ReviewDecisionRequest) -> Result<()> {
    let derived_object_required = matches!(
        request.kind,
        ReviewItemKind::ObjectLowConfidence | ReviewItemKind::CandidateKeyCollision
    );
    if derived_object_required && request.derived_object_id.is_none() {
        return Err(OpenArchiveError::Invariant(format!(
            "{} decisions require derived_object_id",
            request.kind.as_str()
        )));
    }
    if !derived_object_required && request.derived_object_id.is_some() {
        return Err(OpenArchiveError::Invariant(format!(
            "{} decisions do not accept derived_object_id",
            request.kind.as_str()
        )));
    }
    Ok(())
}

fn priority_for_kind(kind: ReviewItemKind) -> ReviewPriority {
    match kind {
        ReviewItemKind::ArtifactNeedsAttention | ReviewItemKind::ArtifactMissingSummary => {
            ReviewPriority::High
        }
        ReviewItemKind::CandidateKeyCollision => ReviewPriority::Medium,
        ReviewItemKind::ObjectLowConfidence => ReviewPriority::Low,
    }
}

fn priority_rank(priority: ReviewPriority) -> u8 {
    match priority {
        ReviewPriority::High => 0,
        ReviewPriority::Medium => 1,
        ReviewPriority::Low => 2,
    }
}

fn recommended_actions(kind: ReviewItemKind) -> Vec<ReviewAction> {
    match kind {
        ReviewItemKind::ArtifactNeedsAttention => vec![
            ReviewAction::InspectArtifact,
            ReviewAction::InspectContextPack,
            ReviewAction::RetryArtifact,
            ReviewAction::NoteItem,
            ReviewAction::DismissItem,
            ReviewAction::ResolveItem,
        ],
        ReviewItemKind::ArtifactMissingSummary => vec![
            ReviewAction::InspectArtifact,
            ReviewAction::InspectContextPack,
            ReviewAction::RetryArtifact,
            ReviewAction::NoteItem,
            ReviewAction::DismissItem,
            ReviewAction::ResolveItem,
        ],
        ReviewItemKind::ObjectLowConfidence => vec![
            ReviewAction::InspectArtifact,
            ReviewAction::InspectContextPack,
            ReviewAction::RejectObject,
            ReviewAction::SupersedeObject,
            ReviewAction::NoteItem,
            ReviewAction::DismissItem,
            ReviewAction::ResolveItem,
        ],
        ReviewItemKind::CandidateKeyCollision => vec![
            ReviewAction::InspectArtifact,
            ReviewAction::InspectRelatedObjects,
            ReviewAction::LinkObjects,
            ReviewAction::SupersedeObject,
            ReviewAction::NoteItem,
            ReviewAction::DismissItem,
            ReviewAction::ResolveItem,
        ],
    }
}

fn reason_for_candidate(candidate: &ReviewCandidate) -> String {
    match candidate.kind {
        ReviewItemKind::ArtifactNeedsAttention => match candidate.enrichment_status {
            Some(EnrichmentStatus::Failed) => {
                "Artifact enrichment failed and needs inspection.".to_string()
            }
            Some(EnrichmentStatus::Partial) => {
                "Artifact enrichment is partial and may need curation.".to_string()
            }
            Some(status) => format!(
                "Artifact needs attention due to enrichment status {}.",
                status.as_str()
            ),
            None => "Artifact needs attention.".to_string(),
        },
        ReviewItemKind::ArtifactMissingSummary => {
            "Artifact enrichment finished without an active summary.".to_string()
        }
        ReviewItemKind::ObjectLowConfidence => match candidate.confidence_score {
            Some(score) => format!("Active derived object has low confidence ({score:.2})."),
            None => "Active derived object has low confidence.".to_string(),
        },
        ReviewItemKind::CandidateKeyCollision => match (
            candidate.candidate_key.as_deref(),
            candidate.related_artifact_count,
        ) {
            (Some(candidate_key), Some(count)) => {
                format!("Candidate key {candidate_key} appears across {count} artifacts.")
            }
            (Some(candidate_key), None) => {
                format!("Candidate key {candidate_key} appears on multiple artifacts.")
            }
            _ => "Candidate key appears on multiple artifacts.".to_string(),
        },
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageResult;
    use crate::storage::{ReviewStore, ReviewWriteStore};
    use std::sync::Mutex;

    struct MockReviewStore {
        candidates: Vec<ReviewCandidate>,
        recorded: Mutex<Vec<NewReviewDecision>>,
        retried_artifacts: Mutex<Vec<String>>,
    }

    impl crate::storage::ReviewReadStore for MockReviewStore {
        fn list_review_candidates(
            &self,
            _filters: &ReviewQueueFilters,
            _limit: usize,
        ) -> StorageResult<Vec<ReviewCandidate>> {
            Ok(self.candidates.clone())
        }
    }

    impl ReviewWriteStore for MockReviewStore {
        fn record_review_decision(&self, decision: &NewReviewDecision) -> StorageResult<()> {
            self.recorded.lock().unwrap().push(decision.clone());
            Ok(())
        }

        fn retry_artifact_enrichment(&self, artifact_id: &str) -> StorageResult<String> {
            self.retried_artifacts
                .lock()
                .unwrap()
                .push(artifact_id.to_string());
            Ok(format!("job-retry-{artifact_id}"))
        }
    }

    fn service_with(candidates: Vec<ReviewCandidate>) -> ReviewService {
        ReviewService::new(Arc::new(MockReviewStore {
            candidates,
            recorded: Mutex::new(Vec::new()),
            retried_artifacts: Mutex::new(Vec::new()),
        }))
    }

    #[test]
    fn list_assigns_priority_actions_and_item_ids() {
        let service = service_with(vec![
            ReviewCandidate {
                kind: ReviewItemKind::ObjectLowConfidence,
                artifact_id: "artifact-2".to_string(),
                derived_object_id: Some("obj-2".to_string()),
                source_type: SourceType::ChatGptExport,
                captured_at: "2026-03-23T10:00:00.000000+00".to_string(),
                title: Some("Weak memory".to_string()),
                body_text: Some("body".to_string()),
                derived_object_type: Some(DerivedObjectType::Memory),
                candidate_key: None,
                enrichment_status: None,
                confidence_score: Some(0.42),
                related_artifact_count: None,
            },
            ReviewCandidate {
                kind: ReviewItemKind::ArtifactNeedsAttention,
                artifact_id: "artifact-1".to_string(),
                derived_object_id: None,
                source_type: SourceType::ClaudeExport,
                captured_at: "2026-03-24T10:00:00.000000+00".to_string(),
                title: Some("Broken artifact".to_string()),
                body_text: None,
                derived_object_type: None,
                candidate_key: None,
                enrichment_status: Some(EnrichmentStatus::Failed),
                confidence_score: None,
                related_artifact_count: None,
            },
        ]);

        let response = service
            .list(ReviewQueueRequest {
                filters: ReviewQueueFilters::default(),
                limit: 10,
            })
            .unwrap();

        assert_eq!(response.items.len(), 2);
        assert_eq!(
            response.items[0].item_id,
            "review:artifact_needs_attention:artifact-1"
        );
        assert_eq!(response.items[0].priority, ReviewPriority::High);
        assert!(response.items[0]
            .recommended_actions
            .contains(&ReviewAction::RetryArtifact));
        assert_eq!(
            response.items[1].item_id,
            "review:object_low_confidence:obj-2"
        );
        assert_eq!(response.items[1].priority, ReviewPriority::Low);
        assert_eq!(
            response.items[1].reason,
            "Active derived object has low confidence (0.42)."
        );
    }

    #[test]
    fn list_truncates_after_sorting() {
        let service = service_with(vec![
            ReviewCandidate {
                kind: ReviewItemKind::ObjectLowConfidence,
                artifact_id: "artifact-low".to_string(),
                derived_object_id: Some("obj-low".to_string()),
                source_type: SourceType::ChatGptExport,
                captured_at: "2026-03-20T10:00:00.000000+00".to_string(),
                title: None,
                body_text: None,
                derived_object_type: Some(DerivedObjectType::Memory),
                candidate_key: None,
                enrichment_status: None,
                confidence_score: Some(0.2),
                related_artifact_count: None,
            },
            ReviewCandidate {
                kind: ReviewItemKind::CandidateKeyCollision,
                artifact_id: "artifact-mid".to_string(),
                derived_object_id: Some("obj-mid".to_string()),
                source_type: SourceType::ClaudeExport,
                captured_at: "2026-03-21T10:00:00.000000+00".to_string(),
                title: None,
                body_text: None,
                derived_object_type: Some(DerivedObjectType::Entity),
                candidate_key: Some("person:david".to_string()),
                enrichment_status: None,
                confidence_score: None,
                related_artifact_count: Some(2),
            },
            ReviewCandidate {
                kind: ReviewItemKind::ArtifactMissingSummary,
                artifact_id: "artifact-high".to_string(),
                derived_object_id: None,
                source_type: SourceType::GrokExport,
                captured_at: "2026-03-22T10:00:00.000000+00".to_string(),
                title: None,
                body_text: None,
                derived_object_type: None,
                candidate_key: None,
                enrichment_status: Some(EnrichmentStatus::Completed),
                confidence_score: None,
                related_artifact_count: None,
            },
        ]);

        let response = service
            .list(ReviewQueueRequest {
                filters: ReviewQueueFilters::default(),
                limit: 2,
            })
            .unwrap();

        assert_eq!(response.items.len(), 2);
        assert_eq!(
            response.items[0].kind,
            ReviewItemKind::ArtifactMissingSummary
        );
        assert_eq!(
            response.items[1].kind,
            ReviewItemKind::CandidateKeyCollision
        );
    }

    #[test]
    fn record_decision_builds_item_id_and_persists_status() {
        let store = Arc::new(MockReviewStore {
            candidates: Vec::new(),
            recorded: Mutex::new(Vec::new()),
            retried_artifacts: Mutex::new(Vec::new()),
        });
        let service = ReviewService::new(store.clone() as Arc<dyn ReviewStore + Send + Sync>);

        let decision_id = service
            .record_decision(ReviewDecisionRequest {
                kind: ReviewItemKind::ObjectLowConfidence,
                artifact_id: "artifact-1".to_string(),
                derived_object_id: Some("obj-1".to_string()),
                decision_status: ReviewDecisionStatus::Dismissed,
                note_text: Some("Reviewed and intentionally ignored.".to_string()),
                decided_by: Some("tester".to_string()),
            })
            .unwrap();

        assert!(decision_id.starts_with("reviewdec-"));
        let recorded = store.recorded.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].item_id, "review:object_low_confidence:obj-1");
        assert_eq!(recorded[0].decision_status, ReviewDecisionStatus::Dismissed);
    }

    #[test]
    fn record_decision_requires_note_for_noted_status() {
        let service = service_with(Vec::new());
        let err = service
            .record_decision(ReviewDecisionRequest {
                kind: ReviewItemKind::ArtifactNeedsAttention,
                artifact_id: "artifact-1".to_string(),
                derived_object_id: None,
                decision_status: ReviewDecisionStatus::Noted,
                note_text: None,
                decided_by: None,
            })
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("review notes require non-empty note_text"));
    }

    #[test]
    fn retry_artifact_forwards_to_store() {
        let store = Arc::new(MockReviewStore {
            candidates: Vec::new(),
            recorded: Mutex::new(Vec::new()),
            retried_artifacts: Mutex::new(Vec::new()),
        });
        let service = ReviewService::new(store.clone() as Arc<dyn ReviewStore + Send + Sync>);

        let job_id = service
            .retry_artifact(RetryArtifactRequest {
                artifact_id: "artifact-9".to_string(),
            })
            .unwrap();

        assert_eq!(job_id, "job-retry-artifact-9");
        assert_eq!(
            store.retried_artifacts.lock().unwrap().as_slice(),
            &["artifact-9".to_string()]
        );
    }
}
