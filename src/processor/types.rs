use crate::storage::types::{
    ArtifactClass, EnrichmentTier, ImportedNoteMetadata, LoadedParticipant, LoadedSegment,
    ReconciliationDecisionKind, ScopeType, SourceType,
};
use serde::{Deserialize, Serialize};

use super::pipeline::ProcessorError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactProcessorInput {
    pub artifact_id: String,
    pub import_id: String,
    pub artifact_class: ArtifactClass,
    pub source_type: SourceType,
    pub title: Option<String>,
    pub imported_note_metadata: Option<ImportedNoteMetadata>,
    pub participants: Vec<LoadedParticipant>,
    pub segments: Vec<LoadedSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SummaryOutput {
    pub title: Option<String>,
    pub body_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassificationOutput {
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub classification_type: String,
    pub classification_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryOutput {
    pub candidate_key: String,
    pub title: Option<String>,
    pub body_text: String,
    pub memory_type: String,
    pub memory_scope: ScopeType,
    pub memory_scope_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityOutput {
    pub entity_key: String,
    pub display_name: String,
    pub entity_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipOutput {
    pub relationship_type: String,
    pub subject_key: String,
    pub object_key: String,
    pub title: Option<String>,
    pub body_text: String,
    pub confidence_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactProcessorOutput {
    pub pipeline_name: String,
    pub pipeline_version: String,
    pub provider_name: Option<String>,
    pub model_name: Option<String>,
    pub prompt_version: Option<String>,
    pub usage: Option<InferenceUsage>,
    pub summary: SummaryOutput,
    pub classifications: Vec<ClassificationOutput>,
    pub memories: Vec<MemoryOutput>,
    pub entities: Vec<EntityOutput>,
    pub relationships: Vec<RelationshipOutput>,
    pub importance_score: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconciliationProcessorInput {
    pub artifact_id: String,
    pub source_type: SourceType,
    pub summary: SummaryOutput,
    pub memories: Vec<MemoryOutput>,
    pub entities: Vec<EntityOutput>,
    pub relationships: Vec<RelationshipOutput>,
    pub retrieval_results_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconciliationDecisionOutput {
    pub decision_kind: ReconciliationDecisionKind,
    pub target_kind: String,
    pub target_key: String,
    pub matched_object_id: Option<String>,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub reported_cost_micros: Option<u64>,
}

pub trait ArtifactProcessor: Send + Sync {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

pub trait ReconciliationProcessor: Send + Sync {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError>;
}

pub trait ArtifactProcessorFactory: Send + Sync {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError>;

    fn build_reconciliation_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError>;

    fn build_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ArtifactBatchProcessor>>, ProcessorError> {
        Ok(None)
    }

    fn build_reconciliation_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchProcessor>>, ProcessorError> {
        Ok(None)
    }
}

pub trait ArtifactBatchProcessor: Send + Sync {
    fn max_batch_jobs(&self) -> usize;
    fn max_batch_bytes(&self) -> usize;
    fn can_process(&self, input: &ArtifactProcessorInput) -> bool;
    fn estimate_size_bytes(&self, input: &ArtifactProcessorInput) -> Result<usize, ProcessorError>;
    fn process_batch(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>>;
}

pub trait ReconciliationBatchProcessor: Send + Sync {
    fn max_batch_jobs(&self) -> usize;
    fn max_batch_bytes(&self) -> usize;
    fn estimate_size_bytes(
        &self,
        inputs: &ReconciliationProcessorInput,
    ) -> Result<usize, ProcessorError>;
    fn process_batch(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>>;
}
