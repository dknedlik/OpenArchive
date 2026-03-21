use crate::error::StorageResult;

#[derive(Debug, Clone)]
pub struct NewAgentMemory {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub title: String,
    pub body_text: String,
    pub memory_type: String,
    pub candidate_key: Option<String>,
    pub contributed_by: Option<String>,
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
}
