use thiserror::Error;

use crate::storage::types::{LoadedParticipant, LoadedSegment, ScopeType, SourceType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactProcessorInput {
    pub artifact_id: String,
    pub import_id: String,
    pub source_type: SourceType,
    pub title: Option<String>,
    pub participants: Vec<LoadedParticipant>,
    pub segments: Vec<LoadedSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryOutput {
    pub title: Option<String>,
    pub body_text: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassificationOutput {
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub classification_type: String,
    pub classification_value: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOutput {
    pub title: Option<String>,
    pub body_text: String,
    pub memory_type: String,
    pub memory_scope: ScopeType,
    pub memory_scope_value: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactProcessorOutput {
    pub pipeline_name: String,
    pub pipeline_version: String,
    pub summary: SummaryOutput,
    pub classifications: Vec<ClassificationOutput>,
    pub memories: Vec<MemoryOutput>,
}

pub trait ArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

#[derive(Debug, Default)]
pub struct StubProcessor;

impl ArtifactProcessor for StubProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let first_segment_id = input
            .segments
            .first()
            .map(|segment| segment.segment_id.clone())
            .ok_or_else(|| ProcessorError::InvalidInput {
                detail: format!("artifact {} has no segments to enrich", input.artifact_id),
            })?;
        let evidence_segment_ids = vec![first_segment_id];
        let segment_count = input.segments.len();
        let participant_count = input.participants.len();
        let title = input
            .title
            .clone()
            .unwrap_or_else(|| format!("Artifact {}", input.artifact_id));

        Ok(ArtifactProcessorOutput {
            pipeline_name: "stub_enrichment".to_string(),
            pipeline_version: "v1".to_string(),
            summary: SummaryOutput {
                title: Some(format!("Stub summary for {title}")),
                body_text: format!(
                    "Stub summary for artifact {} with {} segments and {} participants.",
                    input.artifact_id, segment_count, participant_count
                ),
                evidence_segment_ids: evidence_segment_ids.clone(),
            },
            classifications: vec![ClassificationOutput {
                title: Some("Source type".to_string()),
                body_text: Some(format!(
                    "Stub classification for {} based on {} segments.",
                    input.source_type.as_str(),
                    segment_count
                )),
                classification_type: "source_type".to_string(),
                classification_value: input.source_type.as_str().to_string(),
                evidence_segment_ids: evidence_segment_ids.clone(),
            }],
            memories: vec![MemoryOutput {
                title: Some("Artifact memory".to_string()),
                body_text: format!(
                    "Stub memory for artifact {} derived from {} segments.",
                    input.artifact_id, segment_count
                ),
                memory_type: "artifact_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: input.artifact_id.clone(),
                evidence_segment_ids,
            }],
        })
    }
}

pub trait ArtifactProcessorFactory: Send + Sync {
    fn build(
        &self,
        tier: crate::storage::types::EnrichmentTier,
    ) -> Result<Box<dyn ArtifactProcessor>, ProcessorError>;
}

#[derive(Debug, Default)]
pub struct StubProcessorFactory;

impl ArtifactProcessorFactory for StubProcessorFactory {
    fn build(
        &self,
        tier: crate::storage::types::EnrichmentTier,
    ) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        match tier {
            crate::storage::types::EnrichmentTier::Standard => Ok(Box::new(StubProcessor)),
            unsupported => Err(ProcessorError::UnsupportedTier {
                tier: unsupported.as_str().to_string(),
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("unsupported enrichment tier {tier}")]
    UnsupportedTier { tier: String },

    #[error("invalid processor input: {detail}")]
    InvalidInput { detail: String },

    #[error("{message}")]
    Message { message: String },
}
