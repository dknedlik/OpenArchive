use crate::error::StorageResult;
use crate::storage::types::{
    DerivedObjectType, EnrichmentStatus, EvidenceRole, ScopeType, SourceType, SupportStrength,
};
use crate::ParticipantRole;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilters {
    pub object_type: Option<DerivedObjectType>,
    pub source_type: Option<SourceType>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchCandidateKind {
    ArtifactTitle,
    DerivedObject { derived_type: DerivedObjectType },
    SegmentExcerpt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSearchCandidate {
    pub artifact_id: String,
    pub match_record_id: String,
    pub match_kind: SearchCandidateKind,
    pub snippet: String,
    pub score_hint: i32,
}

pub trait ArchiveSearchReadStore: Send + Sync {
    fn search_candidates(
        &self,
        query_text: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> StorageResult<Vec<ArchiveSearchCandidate>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDetailRecord {
    pub artifact_id: String,
    pub title: Option<String>,
    pub source_type: SourceType,
    pub enrichment_status: EnrichmentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDetailSegment {
    pub segment_id: String,
    pub participant_id: Option<String>,
    pub participant_role: Option<ParticipantRole>,
    pub sequence_no: i32,
    pub text_content: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactDetailDerivedObject {
    pub derived_object_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub confidence_score: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactDetailView {
    pub artifact: ArtifactDetailRecord,
    pub segments: Vec<ArtifactDetailSegment>,
    pub derived_objects: Vec<ArtifactDetailDerivedObject>,
}

pub trait ArtifactDetailReadStore: Send + Sync {
    fn load_artifact_detail(&self, artifact_id: &str) -> StorageResult<Option<ArtifactDetailView>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactContextDerivedObject {
    pub derived_object_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub scope_id: String,
    pub scope_type: ScopeType,
    pub candidate_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactContextEvidenceLink {
    pub evidence_link_id: String,
    pub derived_object_id: String,
    pub segment_id: String,
    pub evidence_role: EvidenceRole,
    pub support_strength: SupportStrength,
    pub evidence_rank: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactContextPackMaterial {
    pub artifact: ArtifactDetailRecord,
    pub segments: Vec<ArtifactDetailSegment>,
    pub derived_objects: Vec<ArtifactContextDerivedObject>,
    pub evidence_links: Vec<ArtifactContextEvidenceLink>,
}

pub trait ArtifactContextPackReadStore: Send + Sync {
    fn load_artifact_context_pack_material(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<ArtifactContextPackMaterial>>;
}

#[derive(Debug, Clone, Default)]
pub struct ObjectSearchFilters {
    pub query: Option<String>,
    pub object_type: Option<DerivedObjectType>,
    pub candidate_key: Option<String>,
    pub artifact_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DerivedObjectSearchResult {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub candidate_key: Option<String>,
    pub confidence_score: Option<f64>,
    pub score: Option<f32>,
}

pub trait DerivedObjectSearchStore: Send + Sync {
    fn search_objects(
        &self,
        filters: &ObjectSearchFilters,
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>>;

    fn search_objects_by_embedding(
        &self,
        filters: &ObjectSearchFilters,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>>;

    fn get_related_objects(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<GraphRelatedEntry>>;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct GraphRelatedEntry {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub relation_kind: String,
    pub link_type: Option<String>,
    pub confidence_score: Option<f64>,
    pub rationale: Option<String>,
}

pub trait MvpRetrievalReadStore:
    ArchiveSearchReadStore + ArtifactDetailReadStore + ArtifactContextPackReadStore
{
}

impl<T> MvpRetrievalReadStore for T where
    T: ArchiveSearchReadStore + ArtifactDetailReadStore + ArtifactContextPackReadStore
{
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelatedDerivedObject {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub candidate_key: Option<String>,
    pub confidence_score: Option<f64>,
}

pub trait CrossArtifactReadStore: Send + Sync {
    /// Given a set of candidate_keys from one artifact, find active derived objects
    /// from OTHER artifacts that share any of those keys.
    fn find_related_by_candidate_keys(
        &self,
        artifact_id: &str,
        candidate_keys: &[String],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>>;
}
