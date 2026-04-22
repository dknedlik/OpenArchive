use crate::error::StorageResult;
use crate::storage::types::{
    ArtifactLinkRecord, DerivedObjectType, EnrichmentStatus, ImportedNoteLinkRecord,
    ImportedNoteMetadata, ScopeType, SourceType,
};
use crate::ParticipantRole;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchFilters {
    pub object_type: Option<DerivedObjectType>,
    pub source_type: Option<SourceType>,
    pub tag: Option<String>,
    pub alias: Option<String>,
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchCandidateKind {
    ArtifactTitle,
    ImportedNoteTag,
    ImportedNoteAlias,
    ImportedNotePath,
    ImportedExternalLink,
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
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
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
    pub note_path: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDetailSegment {
    pub segment_id: String,
    pub participant_id: Option<String>,
    pub participant_role: Option<ParticipantRole>,
    pub sequence_no: i32,
    pub text_content: String,
    pub created_at_source: Option<String>,
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
    pub imported_note_metadata: ImportedNoteMetadata,
    pub inbound_note_links: Vec<ImportedNoteLinkRecord>,
    pub artifact_links: Vec<ArtifactLinkRecord>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactContextPackMaterial {
    pub artifact: ArtifactDetailRecord,
    pub segments: Vec<ArtifactDetailSegment>,
    pub imported_note_metadata: ImportedNoteMetadata,
    pub inbound_note_links: Vec<ImportedNoteLinkRecord>,
    pub artifact_links: Vec<ArtifactLinkRecord>,
    pub derived_objects: Vec<ArtifactContextDerivedObject>,
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

pub trait DerivedObjectLookupStore: Send + Sync {
    fn load_active_objects_by_ids(
        &self,
        derived_object_ids: &[String],
    ) -> StorageResult<Vec<DerivedObjectSearchResult>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchHit {
    pub derived_object_id: String,
    pub score: f32,
}

pub trait VectorSearchStore: Send + Sync {
    fn search_by_embedding(
        &self,
        filters: &ObjectSearchFilters,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<VectorSearchHit>>;

    fn find_related_by_embedding(
        &self,
        artifact_id: &str,
        derived_object_type: DerivedObjectType,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<VectorSearchHit>>;
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

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct RelatedDerivedObject {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub candidate_key: Option<String>,
    pub confidence_score: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct RelatedDerivedObjectEmbeddingMatch {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub candidate_key: Option<String>,
    pub confidence_score: Option<f64>,
    pub similarity_score: f32,
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

    /// Given a freshly embedded candidate, find active derived objects from
    /// OTHER artifacts of the same type ordered by embedding similarity.
    fn find_related_by_embedding(
        &self,
        artifact_id: &str,
        derived_object_type: DerivedObjectType,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>>;
}
