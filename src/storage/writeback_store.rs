use crate::error::StorageResult;
use crate::storage::types::{EvidenceRole, ObjectStatus, SupportStrength};

#[derive(Debug, Clone)]
pub struct NewAgentEvidenceLink {
    pub evidence_link_id: String,
    pub segment_id: String,
    pub evidence_role: EvidenceRole,
    pub support_strength: SupportStrength,
}

#[derive(Debug, Clone)]
pub struct UpdateObjectStatus {
    pub derived_object_id: String,
    pub new_status: ObjectStatus,
    pub replacement_object_id: Option<String>,
    pub contributed_by: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewAgentEntity {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub title: String,
    pub body_text: String,
    pub entity_type: String,
    pub candidate_key: Option<String>,
    pub contributed_by: Option<String>,
    pub evidence: Vec<NewAgentEvidenceLink>,
}

#[derive(Debug, Clone)]
pub struct NewAgentMemory {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub title: String,
    pub body_text: String,
    pub memory_type: String,
    pub candidate_key: Option<String>,
    pub contributed_by: Option<String>,
    pub evidence: Vec<NewAgentEvidenceLink>,
}

#[derive(Debug, Clone)]
pub struct NewArchiveLink {
    pub archive_link_id: String,
    pub source_object_id: String,
    pub target_object_id: String,
    pub link_type: String,
    pub confidence_score: Option<f64>,
    pub rationale: Option<String>,
    pub contributed_by: Option<String>,
}

pub trait WritebackStore: Send + Sync {
    fn store_agent_memory(&self, memory: &NewAgentMemory) -> StorageResult<()>;
    fn store_archive_link(&self, link: &NewArchiveLink) -> StorageResult<()>;
    fn update_object_status(&self, update: &UpdateObjectStatus) -> StorageResult<()>;
    fn store_agent_entity(&self, entity: &NewAgentEntity) -> StorageResult<()>;
}
