use crate::error::StorageResult;
use crate::storage::types::{DerivedObjectType, EnrichmentStatus, SourceType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewItemKind {
    ArtifactNeedsAttention,
    ArtifactMissingSummary,
    ObjectLowConfidence,
    CandidateKeyCollision,
    ObjectMissingEvidence,
}

impl ReviewItemKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ArtifactNeedsAttention => "artifact_needs_attention",
            Self::ArtifactMissingSummary => "artifact_missing_summary",
            Self::ObjectLowConfidence => "object_low_confidence",
            Self::CandidateKeyCollision => "candidate_key_collision",
            Self::ObjectMissingEvidence => "object_missing_evidence",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "artifact_needs_attention" => Some(Self::ArtifactNeedsAttention),
            "artifact_missing_summary" => Some(Self::ArtifactMissingSummary),
            "object_low_confidence" => Some(Self::ObjectLowConfidence),
            "candidate_key_collision" => Some(Self::CandidateKeyCollision),
            "object_missing_evidence" => Some(Self::ObjectMissingEvidence),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReviewQueueFilters {
    pub kinds: Option<Vec<ReviewItemKind>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReviewCandidate {
    pub kind: ReviewItemKind,
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
}

pub trait ReviewReadStore: Send + Sync {
    fn list_review_candidates(
        &self,
        filters: &ReviewQueueFilters,
        limit: usize,
    ) -> StorageResult<Vec<ReviewCandidate>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewDecisionStatus {
    Noted,
    Dismissed,
    Resolved,
}

impl ReviewDecisionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Noted => "noted",
            Self::Dismissed => "dismissed",
            Self::Resolved => "resolved",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "noted" => Some(Self::Noted),
            "dismissed" => Some(Self::Dismissed),
            "resolved" => Some(Self::Resolved),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReviewDecision {
    pub review_decision_id: String,
    pub item_id: String,
    pub item_kind: ReviewItemKind,
    pub artifact_id: String,
    pub derived_object_id: Option<String>,
    pub decision_status: ReviewDecisionStatus,
    pub note_text: Option<String>,
    pub decided_by: Option<String>,
}

pub trait ReviewWriteStore: Send + Sync {
    fn record_review_decision(&self, decision: &NewReviewDecision) -> StorageResult<()>;
    fn retry_artifact_enrichment(&self, artifact_id: &str) -> StorageResult<String>;
}

pub trait ReviewStore: ReviewReadStore + ReviewWriteStore {}

impl<T> ReviewStore for T where T: ReviewReadStore + ReviewWriteStore {}
