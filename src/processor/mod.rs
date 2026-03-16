use std::collections::HashSet;
use std::collections::HashMap;
use std::sync::Arc;

use reqwest::blocking::{multipart, Client};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::config::{OpenAiConfig, OpenAiReasoningEffort};
use crate::storage::types::{
    EnrichmentTier, LoadedParticipant, LoadedSegment, ReconciliationDecisionKind, RetrievalIntent,
    ScopeType, SegmentSpanRef, SourceType, TopicThreadRef,
};

mod anthropic;
mod gemini;
mod grok;
pub use anthropic::AnthropicProcessorFactory;
pub use gemini::{
    GeminiBatchClient, GeminiBatchEnrichmentRequest, GeminiBatchJob, GeminiProcessorFactory,
};
pub use grok::GrokProcessorFactory;

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
pub struct PreprocessProcessorInput {
    pub artifact_id: String,
    pub import_id: String,
    pub source_type: SourceType,
    pub title: Option<String>,
    pub participants: Vec<LoadedParticipant>,
    pub segments: Vec<LoadedSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreprocessProcessorOutput {
    pub pipeline_name: String,
    pub pipeline_version: String,
    pub provider_name: Option<String>,
    pub model_name: Option<String>,
    pub prompt_version: Option<String>,
    pub usage: Option<InferenceUsage>,
    pub topic_threads: Vec<TopicThreadRef>,
    pub escalate_to_quality: bool,
    pub escalation_reason: Option<String>,
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
pub struct EntityOutput {
    pub entity_key: String,
    pub display_name: String,
    pub entity_type: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipOutput {
    pub relationship_type: String,
    pub subject_key: String,
    pub object_key: String,
    pub title: Option<String>,
    pub body_text: String,
    pub confidence_label: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub retrieval_intents: Vec<RetrievalIntent>,
    pub importance_score: u8,
    pub escalate_to_frontier: bool,
    pub escalation_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationProcessorInput {
    pub artifact_id: String,
    pub source_type: SourceType,
    pub summary: SummaryOutput,
    pub memories: Vec<MemoryOutput>,
    pub relationships: Vec<RelationshipOutput>,
    pub retrieval_results_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationDecisionOutput {
    pub decision_kind: ReconciliationDecisionKind,
    pub target_kind: String,
    pub target_key: String,
    pub matched_object_id: Option<String>,
    pub rationale: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub reported_cost_micros: Option<u64>,
}

const MAX_RETRIEVAL_INTENTS: usize = 8;

pub trait ArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

pub trait PreprocessProcessor {
    fn segment(
        &self,
        input: &PreprocessProcessorInput,
    ) -> Result<PreprocessProcessorOutput, ProcessorError>;
}

pub trait ReconciliationProcessor {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError>;
}

#[derive(Debug, Default)]
pub struct StubProcessor;

#[derive(Debug, Default)]
pub struct StubPreprocessProcessor;

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
            provider_name: Some("stub".to_string()),
            model_name: Some("stub".to_string()),
            prompt_version: Some("stub-v1".to_string()),
            usage: None,
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
                classification_type: "topic".to_string(),
                classification_value: "stub_enrichment".to_string(),
                evidence_segment_ids: evidence_segment_ids.clone(),
            }],
            memories: vec![MemoryOutput {
                title: Some("Artifact memory".to_string()),
                body_text: format!(
                    "Stub memory for artifact {} derived from {} segments.",
                    input.artifact_id, segment_count
                ),
                memory_type: "project_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: input.artifact_id.clone(),
                evidence_segment_ids,
            }],
            entities: input
                .participants
                .iter()
                .enumerate()
                .map(|(index, participant)| EntityOutput {
                    entity_key: participant
                        .display_name
                        .clone()
                        .unwrap_or_else(|| format!("participant_{index}"))
                        .to_ascii_lowercase()
                        .replace(' ', "_"),
                    display_name: participant
                        .display_name
                        .clone()
                        .unwrap_or_else(|| format!("Participant {index}")),
                    entity_type: "participant".to_string(),
                    evidence_segment_ids: vec![input.segments[0].segment_id.clone()],
                })
                .collect(),
            relationships: if input.participants.len() >= 2 {
                vec![RelationshipOutput {
                    relationship_type: "participant_interaction".to_string(),
                    subject_key: input
                        .participants
                        .first()
                        .and_then(|p| p.display_name.clone())
                        .unwrap_or_else(|| "participant_0".to_string())
                        .to_ascii_lowercase()
                        .replace(' ', "_"),
                    object_key: input
                        .participants
                        .get(1)
                        .and_then(|p| p.display_name.clone())
                        .unwrap_or_else(|| "participant_1".to_string())
                        .to_ascii_lowercase()
                        .replace(' ', "_"),
                    title: Some("Participant interaction".to_string()),
                    body_text: "The artifact captures an interaction between two participants."
                        .to_string(),
                    confidence_label: "medium".to_string(),
                    evidence_segment_ids: vec![input.segments[0].segment_id.clone()],
                }]
            } else {
                Vec::new()
            },
            retrieval_intents: vec![RetrievalIntent {
                intent_id: "intent-stub-1".to_string(),
                question: "Find prior archive context related to the artifact memory.".to_string(),
                query_text: title.clone(),
                intent_type: "memory_match".to_string(),
                evidence_segment_ids: vec![input.segments[0].segment_id.clone()],
            }],
            importance_score: 1,
            escalate_to_frontier: false,
            escalation_reason: None,
        })
    }
}

impl PreprocessProcessor for StubPreprocessProcessor {
    fn segment(
        &self,
        input: &PreprocessProcessorInput,
    ) -> Result<PreprocessProcessorOutput, ProcessorError> {
        let Some(first) = input.segments.first() else {
            return Err(ProcessorError::InvalidInput {
                detail: format!("artifact {} has no segments to preprocess", input.artifact_id),
            });
        };
        let Some(last) = input.segments.last() else {
            return Err(ProcessorError::InvalidInput {
                detail: format!("artifact {} has no segments to preprocess", input.artifact_id),
            });
        };
        Ok(PreprocessProcessorOutput {
            pipeline_name: "stub_preprocess".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some("stub".to_string()),
            model_name: Some("stub".to_string()),
            prompt_version: Some("stub-v1".to_string()),
            usage: None,
            topic_threads: vec![TopicThreadRef {
                thread_id: "thread-1".to_string(),
                label: input
                    .title
                    .clone()
                    .unwrap_or_else(|| "full conversation".to_string()),
                summary: "Stub preprocess keeps the full conversation as one topic thread.".to_string(),
                confidence_label: "medium".to_string(),
                spans: vec![SegmentSpanRef {
                    start_sequence_no: first.sequence_no,
                    end_sequence_no: last.sequence_no,
                }],
            }],
            escalate_to_quality: false,
            escalation_reason: None,
        })
    }
}

#[derive(Debug, Default)]
pub struct StubReconciliationProcessor;

impl ReconciliationProcessor for StubReconciliationProcessor {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        Ok(input
            .memories
            .iter()
            .map(|memory| ReconciliationDecisionOutput {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "memory".to_string(),
                target_key: memory
                    .title
                    .clone()
                    .unwrap_or_else(|| memory.body_text.chars().take(64).collect()),
                matched_object_id: None,
                rationale: "Stub reconciliation defaults to create_new.".to_string(),
                evidence_segment_ids: memory.evidence_segment_ids.clone(),
            })
            .chain(
                input
                    .relationships
                    .iter()
                    .map(|relationship| ReconciliationDecisionOutput {
                        decision_kind: ReconciliationDecisionKind::CreateNew,
                        target_kind: "relationship".to_string(),
                        target_key: format!(
                            "{}:{}:{}",
                            relationship.relationship_type,
                            relationship.subject_key,
                            relationship.object_key
                        ),
                        matched_object_id: None,
                        rationale: "Stub reconciliation defaults to create_new.".to_string(),
                        evidence_segment_ids: relationship.evidence_segment_ids.clone(),
                    }),
            )
            .collect())
    }
}

pub trait ArtifactProcessorFactory: Send + Sync {
    fn build_preprocess_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn PreprocessProcessor>, ProcessorError>;

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

    fn build_preprocess_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn PreprocessBatchProcessor>>, ProcessorError> {
        Ok(None)
    }

    fn build_reconciliation_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchProcessor>>, ProcessorError> {
        Ok(None)
    }

    // --- Non-blocking batch submitter builders (for per-stage pollers) ---

    fn build_extraction_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ExtractionBatchSubmitter>>, ProcessorError> {
        Ok(None)
    }

    fn build_preprocess_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn PreprocessBatchSubmitter>>, ProcessorError> {
        Ok(None)
    }

    fn build_reconciliation_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchSubmitter>>, ProcessorError> {
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

pub trait PreprocessBatchProcessor: Send + Sync {
    fn max_batch_jobs(&self) -> usize;
    fn max_batch_bytes(&self) -> usize;
    fn estimate_size_bytes(&self, input: &PreprocessProcessorInput) -> Result<usize, ProcessorError>;
    fn process_batch(
        &self,
        inputs: &[PreprocessProcessorInput],
    ) -> Vec<Result<PreprocessProcessorOutput, ProcessorError>>;
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

// ---------------------------------------------------------------------------
// Non-blocking batch submitter traits (used by per-stage pollers)
// ---------------------------------------------------------------------------

/// Handle returned by a batch submission, used for polling.
pub struct BatchHandle {
    pub batch_id: String,
    pub provider: String,
    pub submitted_at: std::time::Instant,
}

/// Result of polling an in-flight batch.
pub enum BatchPollResult {
    /// Batch is still being processed by the provider.
    Pending,
    /// Batch completed successfully. The boxed value is provider-specific
    /// completed batch data, passed back to `parse_results`.
    Succeeded(Box<dyn std::any::Any>),
    /// Batch failed terminally.
    Failed(String),
}

/// Non-blocking batch submission for extraction.
pub trait ExtractionBatchSubmitter {
    fn max_batch_size(&self) -> usize;
    fn prepare_and_submit(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError>;
    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError>;
    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>>;
}

/// Non-blocking batch submission for preprocessing (two-phase).
pub trait PreprocessBatchSubmitter {
    fn max_batch_size(&self) -> usize;
    fn submit_phase_one(
        &self,
        inputs: &[PreprocessProcessorInput],
    ) -> Result<BatchHandle, ProcessorError>;
    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError>;
    /// Parse phase-one results. Returns opaque data for phase-two submission.
    fn parse_phase_one(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[PreprocessProcessorInput],
    ) -> Result<Box<dyn std::any::Any>, ProcessorError>;
    fn submit_phase_two(
        &self,
        inputs: &[PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Result<BatchHandle, ProcessorError>;
    fn serialize_phase_one_data(
        &self,
        phase_one_data: &dyn std::any::Any,
    ) -> Result<String, ProcessorError>;
    fn deserialize_phase_one_data(
        &self,
        serialized: &str,
    ) -> Result<Box<dyn std::any::Any>, ProcessorError>;
    fn parse_phase_two(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Vec<Result<PreprocessProcessorOutput, ProcessorError>>;
}

/// Non-blocking batch submission for reconciliation.
pub trait ReconciliationBatchSubmitter {
    fn max_batch_size(&self) -> usize;
    fn prepare_and_submit(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError>;
    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError>;
    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>>;
}

trait EnrichmentStrategy: Send + Sync {
    fn prompt_version(&self) -> &'static str;
    fn system_prompt(&self) -> &'static str;
    fn process(
        &self,
        processor: &HostedArtifactProcessor,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

#[derive(Debug, Default)]
pub struct StubProcessorFactory;

impl ArtifactProcessorFactory for StubProcessorFactory {
    fn build_preprocess_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn PreprocessProcessor>, ProcessorError> {
        match tier {
            EnrichmentTier::Standard => Ok(Box::new(StubPreprocessProcessor)),
            unsupported => Err(ProcessorError::UnsupportedTier {
                tier: unsupported.as_str().to_string(),
            }),
        }
    }

    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        match tier {
            EnrichmentTier::Standard => Ok(Box::new(StubProcessor)),
            unsupported => Err(ProcessorError::UnsupportedTier {
                tier: unsupported.as_str().to_string(),
            }),
        }
    }

    fn build_reconciliation_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        match tier {
            EnrichmentTier::Standard => Ok(Box::new(StubReconciliationProcessor)),
            unsupported => Err(ProcessorError::UnsupportedTier {
                tier: unsupported.as_str().to_string(),
            }),
        }
    }
}

struct HostedArtifactProcessor {
    client: Arc<dyn InferenceClient>,
    model: String,
    pipeline_name: &'static str,
    provider_name: &'static str,
    strategy: Arc<dyn EnrichmentStrategy>,
}

struct HostedPreprocessProcessor {
    client: Arc<dyn InferenceClient>,
    model: String,
    pipeline_name: &'static str,
    provider_name: &'static str,
    prompt_version: &'static str,
}

pub(crate) struct HostedReconciliationProcessor {
    client: Arc<dyn InferenceClient>,
    model: String,
    system_prompt: &'static str,
}

impl ArtifactProcessor for HostedArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        validate_input(input)?;
        self.strategy.process(self, input)
    }
}

impl PreprocessProcessor for HostedPreprocessProcessor {
    fn segment(
        &self,
        input: &PreprocessProcessorInput,
    ) -> Result<PreprocessProcessorOutput, ProcessorError> {
        validate_preprocess_input(input)?;
        let phase_one_prompt = build_preprocess_phase_one_user_prompt(input)?;
        let phase_one_result = self.client.complete_json(
            &self.model,
            PREPROCESS_PHASE_ONE_SYSTEM_PROMPT,
            &phase_one_prompt,
            &preprocess_phase_one_schema_wrapper(),
        )?;
        let phase_one_parsed: ModelPreprocessPhaseOneOutput =
            serde_json::from_str(&phase_one_result.output_text).map_err(|source| {
                ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&phase_one_result.output_text),
                }
            })?;
        let phase_one_resolved = phase_one_parsed.resolve_and_validate(input)?;

        let phase_two_prompt = build_preprocess_phase_two_user_prompt(input, &phase_one_resolved)?;
        let phase_two_result = self.client.complete_json(
            &self.model,
            PREPROCESS_PHASE_TWO_SYSTEM_PROMPT,
            &phase_two_prompt,
            &preprocess_output_schema_wrapper(),
        )?;
        let phase_two_parsed: ModelPreprocessOutput =
            serde_json::from_str(&phase_two_result.output_text).map_err(|source| {
                ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&phase_two_result.output_text),
                }
            })?;
        let phase_two_parsed =
            phase_two_parsed.resolve_segment_aliases(input, &phase_one_resolved);
        phase_two_parsed.validate_against(input)?;
        Ok(phase_two_parsed.into_processor_output(
            input,
            self.model.clone(),
            combine_inference_usage(phase_one_result.usage, phase_two_result.usage),
            self.pipeline_name,
            self.provider_name,
            self.prompt_version,
        ))
    }
}

impl HostedArtifactProcessor {
    fn run_prompt(
        &self,
        input: &ArtifactProcessorInput,
        user_prompt: &str,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        match self.process_once(input, user_prompt) {
            Ok(output) => Ok(output),
            Err(error) if should_retry_with_repair(&error) => {
                let repair_prompt = build_repair_prompt(user_prompt, &error);
                self.process_once(input, &repair_prompt)
            }
            Err(error) => Err(error),
        }
    }

    fn process_once(
        &self,
        input: &ArtifactProcessorInput,
        user_prompt: &str,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let inference_result = self.client.complete_json(
            &self.model,
            self.strategy.system_prompt(),
            user_prompt,
            &structured_output_schema_wrapper(),
        )?;
        let parsed: ModelArtifactOutput = serde_json::from_str(&inference_result.output_text)
            .map_err(|source| ProcessorError::ParseModelJson {
                source,
                body_preview: preview(&inference_result.output_text),
            })?;
        let parsed = parsed.resolve_evidence_aliases(input);

        parsed.validate_against(input)?;
        Ok(parsed.into_processor_output(
            self.model.clone(),
            inference_result.usage,
            self.pipeline_name,
            self.provider_name,
            self.strategy.prompt_version(),
        ))
    }
}

impl HostedReconciliationProcessor {
    fn reconcile_once(
        &self,
        input: &ReconciliationProcessorInput,
        user_prompt: &str,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        let inference_result = self.client.complete_json(
            &self.model,
            self.system_prompt,
            user_prompt,
            &reconciliation_output_schema_wrapper(),
        )?;
        let parsed: ModelReconciliationOutput = serde_json::from_str(&inference_result.output_text)
            .map_err(|source| ProcessorError::ParseModelJson {
                source,
                body_preview: preview(&inference_result.output_text),
            })?;
        parsed.validate_against(input)?;
        Ok(parsed.into_outputs())
    }
}

impl ReconciliationProcessor for HostedReconciliationProcessor {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        let prompt = build_reconciliation_prompt(input)?;
        match self.reconcile_once(input, &prompt) {
            Ok(output) => Ok(output),
            Err(error) if should_retry_with_repair(&error) => {
                let repair_prompt = build_repair_prompt(&prompt, &error);
                self.reconcile_once(input, &repair_prompt)
            }
            Err(error) => Err(error),
        }
    }
}

struct ConversationEnrichmentStrategy {
    prompt_version: &'static str,
    system_prompt: &'static str,
}

impl ConversationEnrichmentStrategy {
    fn openai_default() -> Arc<dyn EnrichmentStrategy> {
        Arc::new(Self {
            prompt_version: OPENAI_PROMPT_VERSION,
            system_prompt: ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
        })
    }

    fn gemini_default() -> Arc<dyn EnrichmentStrategy> {
        Arc::new(Self {
            prompt_version: GEMINI_PROMPT_VERSION,
            system_prompt: GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
        })
    }
}

impl EnrichmentStrategy for ConversationEnrichmentStrategy {
    fn prompt_version(&self) -> &'static str {
        self.prompt_version
    }

    fn system_prompt(&self) -> &'static str {
        self.system_prompt
    }

    fn process(
        &self,
        processor: &HostedArtifactProcessor,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let prompt = build_conversation_user_prompt(input)?;
        processor.run_prompt(input, &prompt)
    }
}

pub struct OpenAiProcessorFactory {
    client: Arc<dyn InferenceClient>,
    batch_client: Option<Arc<OpenAiClient>>,
    standard_model: String,
    quality_model: String,
}

impl OpenAiProcessorFactory {
    pub fn new(config: OpenAiConfig) -> Result<Self, String> {
        let client = Arc::new(OpenAiClient::new(&config).map_err(|err| err.to_string())?);
        let quality_model = config
            .quality_model
            .clone()
            .unwrap_or_else(|| config.standard_model.clone());

        Ok(Self {
            client: client.clone(),
            batch_client: Some(client),
            standard_model: config.standard_model,
            quality_model,
        })
    }

    #[cfg(test)]
    fn with_client(
        client: Arc<dyn InferenceClient>,
        standard_model: impl Into<String>,
        quality_model: impl Into<String>,
    ) -> Self {
        Self {
            client,
            batch_client: None,
            standard_model: standard_model.into(),
            quality_model: quality_model.into(),
        }
    }
}

impl ArtifactProcessorFactory for OpenAiProcessorFactory {
    fn build_preprocess_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn PreprocessProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Box::new(HostedPreprocessProcessor {
            client: Arc::clone(&self.client),
            model,
            pipeline_name: "openai_preprocess",
            provider_name: "openai",
            prompt_version: PREPROCESS_PROMPT_VERSION,
        }))
    }

    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(HostedArtifactProcessor {
            client: Arc::clone(&self.client),
            model,
            pipeline_name: "openai_enrichment",
            provider_name: "openai",
            strategy: ConversationEnrichmentStrategy::openai_default(),
        }))
    }

    fn build_reconciliation_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Box::new(HostedReconciliationProcessor {
            client: Arc::clone(&self.client),
            model,
            system_prompt: RECONCILIATION_SYSTEM_PROMPT,
        }))
    }

    fn build_extraction_submitter(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ExtractionBatchSubmitter>>, ProcessorError> {
        let Some(client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(OpenAiExtractionSubmitter {
            client: Arc::clone(client),
            model,
        })))
    }

    fn build_preprocess_submitter(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn PreprocessBatchSubmitter>>, ProcessorError> {
        let Some(client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(OpenAiPreprocessSubmitter {
            client: Arc::clone(client),
            model,
        })))
    }

    fn build_reconciliation_submitter(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchSubmitter>>, ProcessorError> {
        let Some(client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(OpenAiReconciliationSubmitter {
            client: Arc::clone(client),
            model,
        })))
    }
}

trait InferenceClient: Send + Sync {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError>;
}

#[derive(Debug, Clone)]
struct InferenceResult {
    output_text: String,
    usage: Option<InferenceUsage>,
}

struct OpenAiClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
    reasoning_effort_override: OpenAiReasoningEffort,
}

impl OpenAiClient {
    fn new(config: &OpenAiConfig) -> Result<Self, ProcessorError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let bearer = format!("Bearer {}", config.api_key);
        let auth_value = HeaderValue::from_str(&bearer).map_err(|err| ProcessorError::Message {
            message: format!("invalid OpenAI API key header: {err}"),
        })?;
        default_headers.insert(AUTHORIZATION, auth_value);

        let client = Client::builder()
            .default_headers(default_headers)
            .build()
            .map_err(|source| ProcessorError::BuildHttpClient { source })?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            max_output_tokens: config.max_output_tokens,
            reasoning_effort_override: config.reasoning_effort_override,
        })
    }
}

impl InferenceClient for OpenAiClient {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_via_responses(model, system_prompt, user_prompt, schema)
    }
}

impl OpenAiClient {
    fn complete_via_responses(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        let request = self.client.post(format!("{}/responses", self.base_url));

        let body = OpenRouterResponsesRequest {
            model,
            max_output_tokens: self.max_output_tokens,
            reasoning: OpenRouterResponsesReasoningConfig {
                effort: self
                    .reasoning_effort_for_model(model)
                    .map(|effort| effort.as_str()),
            },
            text: OpenRouterResponsesTextConfig {
                format: schema.clone(),
            },
            input: vec![
                OpenRouterResponsesInputItem {
                    role: "system",
                    content: vec![OpenRouterResponsesContentItem {
                        item_type: "input_text",
                        text: system_prompt.to_string(),
                    }],
                },
                OpenRouterResponsesInputItem {
                    role: "user",
                    content: vec![OpenRouterResponsesContentItem {
                        item_type: "input_text",
                        text: user_prompt.to_string(),
                    }],
                },
            ],
        };

        let request_body = serde_json::to_vec(&body)
            .map_err(|source| ProcessorError::SerializePrompt { source })?;

        let response = request
            .body(request_body)
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        let status = response.status();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(ProcessorError::InferenceHttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
        }

        let parsed: OpenRouterResponsesResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(&response_text),
                }
            })?;

        let usage = parsed
            .usage
            .clone()
            .and_then(InferenceUsage::from_openrouter_usage);
        let content = parsed.flatten_text();
        if content.trim().is_empty() {
            return Err(ProcessorError::Message {
                message: "OpenAI responses returned empty content".to_string(),
            });
        }

        Ok(InferenceResult {
            output_text: content,
            usage,
        })
    }

    fn build_responses_request(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "model": model,
            "max_output_tokens": self.max_output_tokens,
            "reasoning": {
                "effort": self.reasoning_effort_for_model(model).map(|effort| effort.as_str())
            },
            "text": {
                "format": schema
            },
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": system_prompt
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": user_prompt
                        }
                    ]
                }
            ]
        })
    }

    fn submit_responses_batch(
        &self,
        display_name: Option<&str>,
        requests: &[OpenAiBatchRequest],
    ) -> Result<OpenAiBatchJob, ProcessorError> {
        let mut jsonl = String::new();
        for request in requests {
            let line = serde_json::to_string(request)
                .map_err(|source| ProcessorError::SerializePrompt { source })?;
            jsonl.push_str(&line);
            jsonl.push('\n');
        }

        let file_id = self.upload_batch_file(jsonl)?;
        let body = serde_json::json!({
            "input_file_id": file_id,
            "endpoint": "/v1/responses",
            "completion_window": "24h",
            "metadata": display_name.map(|name| serde_json::json!({ "name": name })),
        });
        let request_body = serde_json::to_vec(&body)
            .map_err(|source| ProcessorError::SerializePrompt { source })?;

        let response = self
            .client
            .post(format!("{}/batches", self.base_url))
            .body(request_body)
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        Self::parse_json_response(response)
    }

    fn upload_batch_file(&self, content: String) -> Result<String, ProcessorError> {
        let part = multipart::Part::text(content)
            .file_name("openarchive-batch.jsonl")
            .mime_str("application/jsonl")
            .map_err(|err| ProcessorError::Message {
                message: format!("invalid OpenAI batch upload mime type: {err}"),
            })?;
        let form = multipart::Form::new()
            .text("purpose", "batch")
            .part("file", part);

        let response = self
            .client
            .post(format!("{}/files", self.base_url))
            .multipart(form)
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        let file: OpenAiFileObject = Self::parse_json_response(response)?;
        Ok(file.id)
    }

    fn get_batch(&self, batch_id: &str) -> Result<OpenAiBatchJob, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/batches/{}", self.base_url, batch_id))
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        Self::parse_json_response(response)
    }

    fn read_file_text(&self, file_id: &str) -> Result<String, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/files/{}/content", self.base_url, file_id))
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        let status = response.status();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;
        if !status.is_success() {
            return Err(ProcessorError::InferenceHttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
        }
        Ok(response_text)
    }

    fn parse_json_response<T: serde::de::DeserializeOwned>(
        response: reqwest::blocking::Response,
    ) -> Result<T, ProcessorError> {
        let status = response.status();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;
        if !status.is_success() {
            return Err(ProcessorError::InferenceHttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
        }
        serde_json::from_str(&response_text).map_err(|source| ProcessorError::ParseInferenceResponse {
            source,
            body_preview: preview(&response_text),
        })
    }

    fn reasoning_effort_for_model(&self, model: &str) -> Option<OpenAiReasoningEffort> {
        if self.reasoning_effort_override != OpenAiReasoningEffort::Auto {
            return Some(self.reasoning_effort_override);
        }

        let model = model.to_ascii_lowercase();
        if model.contains("gpt-5.4") {
            return Some(OpenAiReasoningEffort::Low);
        }
        if model.contains("gpt-5-mini") {
            return Some(OpenAiReasoningEffort::Medium);
        }
        if model.contains("gpt-5-nano") {
            return Some(OpenAiReasoningEffort::Minimal);
        }
        if model.contains("gpt-5") {
            return Some(OpenAiReasoningEffort::Low);
        }

        None
    }
}

struct OpenAiExtractionSubmitter {
    client: Arc<OpenAiClient>,
    model: String,
}

struct OpenAiPreprocessSubmitter {
    client: Arc<OpenAiClient>,
    model: String,
}

struct OpenAiReconciliationSubmitter {
    client: Arc<OpenAiClient>,
    model: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenAiPhaseOneData {
    resolved: HashMap<String, Vec<PreprocessPhaseOneSpanResolved>>,
    usage: HashMap<String, Option<InferenceUsage>>,
}

impl ExtractionBatchSubmitter for OpenAiExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt = build_conversation_user_prompt(input)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.artifact_id.clone(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.model,
                    ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
                    &user_prompt,
                    &structured_output_schema(),
                ),
            });
        }
        let job = self
            .client
            .submit_responses_batch(Some("openarchive-enrichment"), &requests)?;
        Ok(BatchHandle {
            batch_id: job.id,
            provider: "openai".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        let batch = self.client.get_batch(&handle.batch_id)?;
        match batch.status.as_deref().unwrap_or_default() {
            "completed" => {
                if batch.output_file_id.is_some() {
                    Ok(BatchPollResult::Succeeded(Box::new(batch)))
                } else {
                    Ok(BatchPollResult::Failed(format!(
                        "OpenAI batch {} completed without output_file_id",
                        handle.batch_id
                    )))
                }
            }
            "failed" | "expired" | "cancelled" => Ok(BatchPollResult::Failed(format!(
                "OpenAI batch {} finished in terminal state {}",
                handle.batch_id,
                batch.status.unwrap_or_else(|| "unknown".to_string())
            ))),
            _ => Ok(BatchPollResult::Pending),
        }
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
        let batch = match completed.downcast::<OpenAiBatchJob>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs.iter().map(|_| Err(ProcessorError::Message {
                    message: "failed to downcast OpenAI extraction batch result".to_string(),
                })).collect();
            }
        };
        match parse_openai_output_file(&self.client, batch.output_file_id.as_deref(), inputs, |result, input| {
            let parsed: ModelArtifactOutput = serde_json::from_str(&result.output_text).map_err(|source| {
                ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&result.output_text),
                }
            })?;
            let parsed = parsed.resolve_evidence_aliases(input);
            parsed.validate_against(input)?;
            Ok(parsed.into_processor_output(
                self.model.clone(),
                result.usage,
                "openai_enrichment",
                "openai",
                OPENAI_PROMPT_VERSION,
            ))
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(message_processor_error(&err))).collect(),
        }
    }
}

impl PreprocessBatchSubmitter for OpenAiPreprocessSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn submit_phase_one(
        &self,
        inputs: &[PreprocessProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt = build_preprocess_phase_one_user_prompt(input)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.artifact_id.clone(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.model,
                    PREPROCESS_PHASE_ONE_SYSTEM_PROMPT,
                    &user_prompt,
                    &preprocess_phase_one_schema(),
                ),
            });
        }
        let job = self
            .client
            .submit_responses_batch(Some("openarchive-preprocess-phase-one"), &requests)?;
        Ok(BatchHandle {
            batch_id: job.id,
            provider: "openai".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        OpenAiExtractionSubmitter {
            client: Arc::clone(&self.client),
            model: self.model.clone(),
        }
        .poll_batch(handle)
    }

    fn parse_phase_one(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[PreprocessProcessorInput],
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let batch = completed.downcast::<OpenAiBatchJob>().map_err(|_| ProcessorError::Message {
            message: "failed to downcast OpenAI preprocess phase-one batch result".to_string(),
        })?;
        let outputs = parse_openai_output_file(&self.client, batch.output_file_id.as_deref(), inputs, |result, input| {
            let parsed: ModelPreprocessPhaseOneOutput =
                serde_json::from_str(&result.output_text).map_err(|source| {
                    ProcessorError::ParseModelJson {
                        source,
                        body_preview: preview(&result.output_text),
                    }
                })?;
            let resolved = parsed.resolve_and_validate(input)?;
            Ok((resolved, result.usage))
        })?;
        let mut resolved = HashMap::new();
        let mut usage = HashMap::new();
        for (input, item) in inputs.iter().zip(outputs.into_iter()) {
            let (spans, item_usage) = item?;
            resolved.insert(input.artifact_id.clone(), spans);
            usage.insert(input.artifact_id.clone(), item_usage);
        }
        Ok(Box::new(OpenAiPhaseOneData { resolved, usage }))
    }

    fn submit_phase_two(
        &self,
        inputs: &[PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Result<BatchHandle, ProcessorError> {
        let data = phase_one_data
            .downcast_ref::<OpenAiPhaseOneData>()
            .ok_or_else(|| ProcessorError::Message {
                message: "failed to downcast OpenAI preprocess phase-one data".to_string(),
            })?;
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let spans = data
                .resolved
                .get(&input.artifact_id)
                .ok_or_else(|| ProcessorError::Message {
                    message: format!("missing phase-one spans for {}", input.artifact_id),
                })?;
            let user_prompt = build_preprocess_phase_two_user_prompt(input, spans)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.artifact_id.clone(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.model,
                    PREPROCESS_PHASE_TWO_SYSTEM_PROMPT,
                    &user_prompt,
                    &preprocess_output_schema(),
                ),
            });
        }
        let job = self
            .client
            .submit_responses_batch(Some("openarchive-preprocess-phase-two"), &requests)?;
        Ok(BatchHandle {
            batch_id: job.id,
            provider: "openai".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn serialize_phase_one_data(
        &self,
        phase_one_data: &dyn std::any::Any,
    ) -> Result<String, ProcessorError> {
        let data = phase_one_data
            .downcast_ref::<OpenAiPhaseOneData>()
            .ok_or_else(|| ProcessorError::Message {
                message: "failed to downcast OpenAI preprocess phase-one data".to_string(),
            })?;
        serde_json::to_string(data).map_err(|source| ProcessorError::Message {
            message: format!("failed to serialize OpenAI preprocess phase-one data: {source}"),
        })
    }

    fn deserialize_phase_one_data(
        &self,
        serialized: &str,
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let data: OpenAiPhaseOneData =
            serde_json::from_str(serialized).map_err(|source| ProcessorError::ParseModelJson {
                source,
                body_preview: preview(serialized),
            })?;
        Ok(Box::new(data))
    }

    fn parse_phase_two(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Vec<Result<PreprocessProcessorOutput, ProcessorError>> {
        let batch = match completed.downcast::<OpenAiBatchJob>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs.iter().map(|_| Err(ProcessorError::Message {
                    message: "failed to downcast OpenAI preprocess phase-two batch result".to_string(),
                })).collect();
            }
        };
        let Some(data) = phase_one_data.downcast_ref::<OpenAiPhaseOneData>() else {
            return inputs.iter().map(|_| Err(ProcessorError::Message {
                message: "failed to downcast OpenAI preprocess phase-one data".to_string(),
            })).collect();
        };
        match parse_openai_output_file(&self.client, batch.output_file_id.as_deref(), inputs, |result, input| {
            let parsed: ModelPreprocessOutput =
                serde_json::from_str(&result.output_text).map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&result.output_text),
                })?;
            let spans = data.resolved.get(&input.artifact_id).ok_or_else(|| ProcessorError::Message {
                message: format!("missing phase-one spans for {}", input.artifact_id),
            })?;
            let parsed = parsed.resolve_segment_aliases(input, spans);
            parsed.validate_against(input)?;
            Ok(parsed.into_processor_output(
                input,
                self.model.clone(),
                combine_inference_usage(
                    data.usage.get(&input.artifact_id).cloned().unwrap_or(None),
                    result.usage,
                ),
                "openai_preprocess",
                "openai",
                PREPROCESS_PROMPT_VERSION,
            ))
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(message_processor_error(&err))).collect(),
        }
    }
}

impl ReconciliationBatchSubmitter for OpenAiReconciliationSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt = build_reconciliation_prompt(input)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.artifact_id.clone(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.model,
                    RECONCILIATION_SYSTEM_PROMPT,
                    &user_prompt,
                    &reconciliation_output_schema(),
                ),
            });
        }
        let job = self
            .client
            .submit_responses_batch(Some("openarchive-reconciliation"), &requests)?;
        Ok(BatchHandle {
            batch_id: job.id,
            provider: "openai".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        OpenAiExtractionSubmitter {
            client: Arc::clone(&self.client),
            model: self.model.clone(),
        }
        .poll_batch(handle)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>> {
        let batch = match completed.downcast::<OpenAiBatchJob>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs.iter().map(|_| Err(ProcessorError::Message {
                    message: "failed to downcast OpenAI reconciliation batch result".to_string(),
                })).collect();
            }
        };
        match parse_openai_output_file(&self.client, batch.output_file_id.as_deref(), inputs, |result, input| {
            let parsed: ModelReconciliationOutput =
                serde_json::from_str(&result.output_text).map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&result.output_text),
                })?;
            parsed.validate_against(input)?;
            Ok(parsed.into_outputs())
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(message_processor_error(&err))).collect(),
        }
    }
}

trait OpenAiBatchInput {
    fn batch_custom_id(&self) -> &str;
}

impl OpenAiBatchInput for ArtifactProcessorInput {
    fn batch_custom_id(&self) -> &str {
        &self.artifact_id
    }
}

impl OpenAiBatchInput for PreprocessProcessorInput {
    fn batch_custom_id(&self) -> &str {
        &self.artifact_id
    }
}

impl OpenAiBatchInput for ReconciliationProcessorInput {
    fn batch_custom_id(&self) -> &str {
        &self.artifact_id
    }
}

struct OpenAiBatchParsedResult {
    output_text: String,
    usage: Option<InferenceUsage>,
}

#[derive(Debug, Serialize)]
struct OpenAiBatchRequest {
    custom_id: String,
    method: String,
    url: String,
    body: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchJob {
    id: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    output_file_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiFileObject {
    id: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchResultLine {
    custom_id: String,
    #[serde(default)]
    response: Option<OpenAiBatchResponseEnvelope>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchResponseEnvelope {
    status_code: u16,
    body: OpenRouterResponsesResponse,
}

fn parse_openai_output_file<I, O, F>(
    client: &OpenAiClient,
    output_file_id: Option<&str>,
    inputs: &[I],
    mut parse: F,
) -> Result<Vec<Result<O, ProcessorError>>, ProcessorError>
where
    I: OpenAiBatchInput,
    F: FnMut(OpenAiBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let output_file_id = output_file_id.ok_or_else(|| ProcessorError::Message {
        message: "OpenAI batch missing output_file_id".to_string(),
    })?;
    let content = client.read_file_text(output_file_id)?;
    let mut parsed_by_id = HashMap::new();
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let item: OpenAiBatchResultLine = serde_json::from_str(line).map_err(|source| {
            ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(line),
            }
        })?;
        let parsed = match (item.response, item.error) {
            (Some(response), _) if response.status_code / 100 == 2 => {
                let usage = response.body.usage.clone().and_then(InferenceUsage::from_openrouter_usage);
                OpenAiBatchParsedResult {
                    output_text: response.body.flatten_text(),
                    usage,
                }
            }
            (Some(response), _) => {
                return Err(ProcessorError::Message {
                    message: format!(
                        "OpenAI batch item {} failed with status {}",
                        item.custom_id, response.status_code
                    ),
                });
            }
            (_, Some(error)) => {
                return Err(ProcessorError::Message {
                    message: format!("OpenAI batch item {} failed: {}", item.custom_id, error),
                });
            }
            _ => {
                return Err(ProcessorError::Message {
                    message: format!("OpenAI batch item {} returned no response", item.custom_id),
                });
            }
        };
        parsed_by_id.insert(item.custom_id, parsed);
    }

    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let result = parsed_by_id.remove(input.batch_custom_id()).ok_or_else(|| ProcessorError::Message {
            message: format!("OpenAI batch missing result for {}", input.batch_custom_id()),
        })?;
        outputs.push(parse(result, input));
    }
    Ok(outputs)
}

fn message_processor_error(err: &ProcessorError) -> ProcessorError {
    ProcessorError::Message {
        message: err.to_string(),
    }
}

#[derive(Debug, Serialize)]
struct OpenRouterResponsesRequest<'a> {
    model: &'a str,
    max_output_tokens: u32,
    reasoning: OpenRouterResponsesReasoningConfig<'a>,
    text: OpenRouterResponsesTextConfig,
    input: Vec<OpenRouterResponsesInputItem>,
}

#[derive(Debug, Serialize)]
struct OpenRouterResponsesTextConfig {
    format: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OpenRouterResponsesReasoningConfig<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct OpenRouterResponsesInputItem {
    role: &'static str,
    content: Vec<OpenRouterResponsesContentItem>,
}

#[derive(Debug, Serialize)]
struct OpenRouterResponsesContentItem {
    #[serde(rename = "type")]
    item_type: &'static str,
    text: String,
}

#[derive(Debug, Deserialize)]
struct OpenRouterResponsesResponse {
    #[serde(default)]
    output_text: String,
    #[serde(default)]
    output: Vec<OpenRouterResponsesOutputItem>,
    #[serde(default)]
    usage: Option<OpenRouterUsage>,
}

impl OpenRouterResponsesResponse {
    fn flatten_text(self) -> String {
        if !self.output_text.trim().is_empty() {
            return self.output_text;
        }

        let mut message_texts = Vec::new();
        let mut reasoning_texts = Vec::new();

        for item in self.output {
            match item {
                OpenRouterResponsesOutputItem::Message { content } => {
                    message_texts.extend(content.into_iter().filter_map(|content| match content {
                        OpenRouterResponsesOutputContent::OutputText { text }
                            if !text.trim().is_empty() =>
                        {
                            Some(text)
                        }
                        _ => None,
                    }));
                }
                OpenRouterResponsesOutputItem::Reasoning { summary } => {
                    reasoning_texts.extend(summary.into_iter().filter_map(|part| match part {
                        OpenRouterResponsesReasoningSummary::SummaryText { text }
                            if !text.trim().is_empty() =>
                        {
                            Some(text)
                        }
                        _ => None,
                    }));
                }
                OpenRouterResponsesOutputItem::Other => {}
            }
        }

        if !message_texts.is_empty() {
            return message_texts.join("");
        }

        reasoning_texts.join("")
    }
}

impl InferenceUsage {
    fn from_openrouter_usage(usage: OpenRouterUsage) -> Option<Self> {
        let reported_cost_micros = usage.cost.map(|cost| (cost * 1_000_000.0).round() as u64);

        if usage.input_tokens.is_none()
            && usage.output_tokens.is_none()
            && usage.total_tokens.is_none()
            && usage
                .output_tokens_details
                .as_ref()
                .and_then(|details| details.reasoning_tokens)
                .is_none()
            && reported_cost_micros.is_none()
        {
            return None;
        }

        Some(Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage
                .output_tokens_details
                .and_then(|details| details.reasoning_tokens),
            total_tokens: usage.total_tokens,
            reported_cost_micros,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenRouterResponsesOutputItem {
    #[serde(rename = "message")]
    Message {
        #[serde(default)]
        content: Vec<OpenRouterResponsesOutputContent>,
    },
    #[serde(rename = "reasoning")]
    Reasoning {
        #[serde(default)]
        summary: Vec<OpenRouterResponsesReasoningSummary>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenRouterResponsesOutputContent {
    #[serde(rename = "output_text")]
    OutputText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenRouterResponsesReasoningSummary {
    #[serde(rename = "summary_text")]
    SummaryText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenRouterUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
    #[serde(default)]
    cost: Option<f64>,
    #[serde(default)]
    output_tokens_details: Option<OpenRouterOutputTokensDetails>,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenRouterOutputTokensDetails {
    #[serde(default)]
    reasoning_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ModelArtifactOutput {
    summary: ModelSummary,
    classifications: Vec<ModelClassification>,
    memories: Vec<ModelMemory>,
    #[serde(default)]
    entities: Vec<ModelEntity>,
    #[serde(default)]
    relationships: Vec<ModelRelationship>,
    #[serde(default)]
    retrieval_intents: Vec<ModelRetrievalIntent>,
    importance_score: u8,
    escalate_to_frontier: bool,
    escalation_reason: String,
}

#[derive(Debug, Deserialize)]
struct ModelSummary {
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelClassification {
    classification_type: String,
    classification_value: String,
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelMemory {
    memory_type: String,
    memory_scope: ScopeType,
    memory_scope_value: String,
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelEntity {
    entity_key: String,
    display_name: String,
    entity_type: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelRelationship {
    relationship_type: String,
    subject_key: String,
    object_key: String,
    title: String,
    body_text: String,
    confidence_label: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelRetrievalIntent {
    question: String,
    query_text: String,
    intent_type: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelReconciliationOutput {
    decisions: Vec<ModelReconciliationDecision>,
}

#[derive(Debug, Deserialize)]
struct ModelReconciliationDecision {
    decision_kind: ReconciliationDecisionKind,
    target_kind: String,
    target_key: String,
    matched_object_id: Option<String>,
    rationale: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ModelPreprocessOutput {
    topic_threads: Vec<ModelTopicThread>,
    escalate_to_quality: bool,
    escalation_reason: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ModelPreprocessPhaseOneOutput {
    topic_spans: Vec<ModelPreprocessPhaseOneSpan>,
}

#[derive(Debug, Deserialize)]
struct ModelPreprocessPhaseOneSpan {
    span_id: String,
    label: String,
    summary: String,
    focus_key: String,
    confidence_label: String,
    start_evidence_ref: String,
    end_evidence_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreprocessPhaseOneSpanResolved {
    span_id: String,
    label: String,
    summary: String,
    focus_key: String,
    confidence_label: String,
    start_evidence_ref: String,
    end_evidence_ref: String,
    start_sequence_no: i32,
    end_sequence_no: i32,
    start_excerpt: String,
    middle_excerpt: String,
    end_excerpt: String,
}

#[derive(Debug, Deserialize)]
struct ModelTopicThread {
    thread_key: String,
    label: String,
    summary: String,
    confidence_label: String,
    spans: Vec<ModelTopicSpan>,
}

#[derive(Debug, Deserialize)]
struct ModelTopicSpan {
    start_evidence_ref: String,
    end_evidence_ref: String,
}

impl ModelArtifactOutput {
    fn resolve_evidence_aliases(mut self, input: &ArtifactProcessorInput) -> Self {
        let alias_map = build_segment_alias_map(input);

        for segment_id in &mut self.summary.evidence_segment_ids {
            if let Some(actual) = alias_map.get(segment_id.as_str()) {
                *segment_id = actual.clone();
            }
        }

        for classification in &mut self.classifications {
            for segment_id in &mut classification.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for memory in &mut self.memories {
            for segment_id in &mut memory.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for entity in &mut self.entities {
            for segment_id in &mut entity.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for relationship in &mut self.relationships {
            for segment_id in &mut relationship.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for intent in &mut self.retrieval_intents {
            for segment_id in &mut intent.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        self
    }

    fn validate_against(&self, input: &ArtifactProcessorInput) -> Result<(), ProcessorError> {
        let valid_segment_ids: HashSet<&str> = input
            .segments
            .iter()
            .map(|segment| segment.segment_id.as_str())
            .collect();

        validate_text_field("summary.title", &self.summary.title)?;
        validate_text_field("summary.body_text", &self.summary.body_text)?;
        validate_evidence_ids(
            "summary.evidence_segment_ids",
            &self.summary.evidence_segment_ids,
            &valid_segment_ids,
        )?;

        if self.classifications.len() > 2 {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} classifications; expected at most 2",
                    self.classifications.len()
                ),
            });
        }

        for (index, classification) in self.classifications.iter().enumerate() {
            validate_text_field(
                &format!("classifications[{index}].title"),
                &classification.title,
            )?;
            validate_text_field(
                &format!("classifications[{index}].body_text"),
                &classification.body_text,
            )?;
            validate_text_field(
                &format!("classifications[{index}].classification_value"),
                &classification.classification_value,
            )?;
            match classification.classification_type.as_str() {
                "topic" | "intent" => {}
                other => {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "classifications[{index}].classification_type {other:?} is not allowed"
                        ),
                    })
                }
            }
            validate_evidence_ids(
                &format!("classifications[{index}].evidence_segment_ids"),
                &classification.evidence_segment_ids,
                &valid_segment_ids,
            )?;
        }

        if self.memories.len() > 5 {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} memories; expected at most 5",
                    self.memories.len()
                ),
            });
        }

        for (index, memory) in self.memories.iter().enumerate() {
            validate_text_field(&format!("memories[{index}].title"), &memory.title)?;
            validate_text_field(&format!("memories[{index}].body_text"), &memory.body_text)?;
            match memory.memory_type.as_str() {
                "preference" | "project_fact" | "identity_fact" | "ongoing_task" | "reference" => {}
                other => {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!("memories[{index}].memory_type {other:?} is not allowed"),
                    })
                }
            }
            if memory.memory_scope_value.trim().is_empty() {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!("memories[{index}].memory_scope_value must not be empty"),
                });
            }
            validate_evidence_ids(
                &format!("memories[{index}].evidence_segment_ids"),
                &memory.evidence_segment_ids,
                &valid_segment_ids,
            )?;
        }

        for (index, entity) in self.entities.iter().enumerate() {
            validate_text_field(&format!("entities[{index}].entity_key"), &entity.entity_key)?;
            validate_text_field(
                &format!("entities[{index}].display_name"),
                &entity.display_name,
            )?;
            validate_text_field(
                &format!("entities[{index}].entity_type"),
                &entity.entity_type,
            )?;
            validate_evidence_ids(
                &format!("entities[{index}].evidence_segment_ids"),
                &entity.evidence_segment_ids,
                &valid_segment_ids,
            )?;
        }

        for (index, relationship) in self.relationships.iter().enumerate() {
            validate_text_field(
                &format!("relationships[{index}].relationship_type"),
                &relationship.relationship_type,
            )?;
            validate_text_field(
                &format!("relationships[{index}].subject_key"),
                &relationship.subject_key,
            )?;
            validate_text_field(
                &format!("relationships[{index}].object_key"),
                &relationship.object_key,
            )?;
            validate_text_field(
                &format!("relationships[{index}].title"),
                &relationship.title,
            )?;
            validate_text_field(
                &format!("relationships[{index}].body_text"),
                &relationship.body_text,
            )?;
            validate_text_field(
                &format!("relationships[{index}].confidence_label"),
                &relationship.confidence_label,
            )?;
            validate_evidence_ids(
                &format!("relationships[{index}].evidence_segment_ids"),
                &relationship.evidence_segment_ids,
                &valid_segment_ids,
            )?;
        }

        if self.retrieval_intents.len() > MAX_RETRIEVAL_INTENTS {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} retrieval intents; expected at most {}",
                    self.retrieval_intents.len(),
                    MAX_RETRIEVAL_INTENTS
                ),
            });
        }

        for (index, intent) in self.retrieval_intents.iter().enumerate() {
            validate_text_field(
                &format!("retrieval_intents[{index}].question"),
                &intent.question,
            )?;
            validate_text_field(
                &format!("retrieval_intents[{index}].query_text"),
                &intent.query_text,
            )?;
            validate_text_field(
                &format!("retrieval_intents[{index}].intent_type"),
                &intent.intent_type,
            )?;
            validate_evidence_ids(
                &format!("retrieval_intents[{index}].evidence_segment_ids"),
                &intent.evidence_segment_ids,
                &valid_segment_ids,
            )?;
        }

        if !(1..=10).contains(&self.importance_score) {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "importance_score {} must be between 1 and 10",
                    self.importance_score
                ),
            });
        }

        if self.escalate_to_frontier && self.importance_score < 8 {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "escalate_to_frontier cannot be true when importance_score is below 8"
                    .to_string(),
            });
        }

        if !self.escalate_to_frontier && !self.escalation_reason.trim().is_empty() {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "escalation_reason must be empty when escalate_to_frontier is false"
                    .to_string(),
            });
        }

        if self.escalate_to_frontier && self.escalation_reason.trim().is_empty() {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "escalation_reason must be present when escalate_to_frontier is true"
                    .to_string(),
            });
        }

        Ok(())
    }

    fn into_processor_output(
        self,
        model_name: String,
        usage: Option<InferenceUsage>,
        pipeline_name: &str,
        provider_name: &str,
        prompt_version: &str,
    ) -> ArtifactProcessorOutput {
        ArtifactProcessorOutput {
            pipeline_name: pipeline_name.to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some(provider_name.to_string()),
            model_name: Some(model_name),
            prompt_version: Some(prompt_version.to_string()),
            usage,
            summary: SummaryOutput {
                title: normalize_optional_text(self.summary.title),
                body_text: self.summary.body_text.trim().to_string(),
                evidence_segment_ids: self.summary.evidence_segment_ids,
            },
            classifications: self
                .classifications
                .into_iter()
                .map(|classification| ClassificationOutput {
                    title: normalize_optional_text(classification.title),
                    body_text: normalize_optional_text(classification.body_text),
                    classification_type: classification.classification_type,
                    classification_value: classification.classification_value,
                    evidence_segment_ids: classification.evidence_segment_ids,
                })
                .collect(),
            memories: self
                .memories
                .into_iter()
                .map(|memory| MemoryOutput {
                    title: normalize_optional_text(memory.title),
                    body_text: memory.body_text.trim().to_string(),
                    memory_type: memory.memory_type,
                    memory_scope: memory.memory_scope,
                    memory_scope_value: memory.memory_scope_value,
                    evidence_segment_ids: memory.evidence_segment_ids,
                })
                .collect(),
            entities: self
                .entities
                .into_iter()
                .map(|entity| EntityOutput {
                    entity_key: entity.entity_key,
                    display_name: entity.display_name,
                    entity_type: entity.entity_type,
                    evidence_segment_ids: entity.evidence_segment_ids,
                })
                .collect(),
            relationships: self
                .relationships
                .into_iter()
                .map(|relationship| RelationshipOutput {
                    relationship_type: relationship.relationship_type,
                    subject_key: relationship.subject_key,
                    object_key: relationship.object_key,
                    title: normalize_optional_text(relationship.title),
                    body_text: relationship.body_text.trim().to_string(),
                    confidence_label: relationship.confidence_label,
                    evidence_segment_ids: relationship.evidence_segment_ids,
                })
                .collect(),
            retrieval_intents: self
                .retrieval_intents
                .into_iter()
                .enumerate()
                .map(|(index, intent)| RetrievalIntent {
                    intent_id: format!("intent-{}", index + 1),
                    question: intent.question,
                    query_text: intent.query_text,
                    intent_type: intent.intent_type,
                    evidence_segment_ids: intent.evidence_segment_ids,
                })
                .collect(),
            importance_score: self.importance_score,
            escalate_to_frontier: self.escalate_to_frontier,
            escalation_reason: normalize_optional_text(self.escalation_reason),
        }
    }
}

impl ModelPreprocessOutput {
    fn resolve_segment_aliases(
        mut self,
        input: &PreprocessProcessorInput,
        phase_one_spans: &[PreprocessPhaseOneSpanResolved],
    ) -> Self {
        let alias_map = build_segment_alias_map_for_preprocess(input);
        let span_map: std::collections::HashMap<&str, (&str, &str)> = phase_one_spans
            .iter()
            .map(|span| {
                (
                    span.span_id.as_str(),
                    (
                        span.start_evidence_ref.as_str(),
                        span.end_evidence_ref.as_str(),
                    ),
                )
            })
            .collect();
        for thread in &mut self.topic_threads {
            for span in &mut thread.spans {
                if let Some(actual) = resolve_preprocess_evidence_ref(
                    span.start_evidence_ref.as_str(),
                    &alias_map,
                    &span_map,
                    true,
                ) {
                    span.start_evidence_ref = actual;
                }
                if let Some(actual) = resolve_preprocess_evidence_ref(
                    span.end_evidence_ref.as_str(),
                    &alias_map,
                    &span_map,
                    false,
                ) {
                    span.end_evidence_ref = actual;
                }
            }
        }
        self
    }

    fn validate_against(&self, input: &PreprocessProcessorInput) -> Result<(), ProcessorError> {
        if self.topic_threads.is_empty() {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "preprocess output must contain at least one topic thread".to_string(),
            });
        }
        if self.topic_threads.len() > 12 {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} topic threads; expected at most 12",
                    self.topic_threads.len()
                ),
            });
        }
        let valid_segments: std::collections::HashMap<&str, i32> = input
            .segments
            .iter()
            .map(|segment| (segment.segment_id.as_str(), segment.sequence_no))
            .collect();

        for (index, thread) in self.topic_threads.iter().enumerate() {
            validate_text_field(&format!("topic_threads[{index}].thread_key"), &thread.thread_key)?;
            validate_text_field(&format!("topic_threads[{index}].label"), &thread.label)?;
            validate_text_field(&format!("topic_threads[{index}].summary"), &thread.summary)?;
            match thread.confidence_label.as_str() {
                "low" | "medium" | "high" => {}
                other => {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "topic_threads[{index}].confidence_label {other:?} is not allowed"
                        ),
                    })
                }
            }
            if thread.spans.is_empty() {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!("topic_threads[{index}] must contain at least one span"),
                });
            }
            for (span_index, span) in thread.spans.iter().enumerate() {
                let Some(start) = valid_segments.get(span.start_evidence_ref.as_str()) else {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "topic_threads[{index}].spans[{span_index}].start_evidence_ref contains unknown segment id {:?}",
                            span.start_evidence_ref
                        ),
                    });
                };
                let Some(end) = valid_segments.get(span.end_evidence_ref.as_str()) else {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "topic_threads[{index}].spans[{span_index}].end_evidence_ref contains unknown segment id {:?}",
                            span.end_evidence_ref
                        ),
                    });
                };
                if start > end {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "topic_threads[{index}].spans[{span_index}] start must be <= end"
                        ),
                    });
                }
            }
        }

        if self.escalate_to_quality && self.escalation_reason.trim().is_empty() {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "escalation_reason must be present when escalate_to_quality is true"
                    .to_string(),
            });
        }

        Ok(())
    }

    fn into_processor_output(
        self,
        input: &PreprocessProcessorInput,
        model_name: String,
        usage: Option<InferenceUsage>,
        pipeline_name: &str,
        provider_name: &str,
        prompt_version: &str,
    ) -> PreprocessProcessorOutput {
        let sequence_by_segment_id: std::collections::HashMap<&str, i32> = input
            .segments
            .iter()
            .map(|segment| (segment.segment_id.as_str(), segment.sequence_no))
            .collect();
        PreprocessProcessorOutput {
            pipeline_name: pipeline_name.to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some(provider_name.to_string()),
            model_name: Some(model_name),
            prompt_version: Some(prompt_version.to_string()),
            usage,
            topic_threads: self
                .topic_threads
                .into_iter()
                .map(|thread| TopicThreadRef {
                    thread_id: thread.thread_key,
                    label: thread.label,
                    summary: thread.summary,
                    confidence_label: thread.confidence_label,
                    spans: thread
                        .spans
                        .into_iter()
                        .map(|span| SegmentSpanRef {
                            start_sequence_no: *sequence_by_segment_id
                                .get(span.start_evidence_ref.as_str())
                                .expect("preprocess span start validated"),
                            end_sequence_no: *sequence_by_segment_id
                                .get(span.end_evidence_ref.as_str())
                                .expect("preprocess span end validated"),
                        })
                        .collect(),
                })
                .collect(),
            escalate_to_quality: self.escalate_to_quality,
            escalation_reason: normalize_optional_text(self.escalation_reason),
        }
    }
}

impl ModelPreprocessPhaseOneOutput {
    pub(crate) fn resolve_and_validate(
        self,
        input: &PreprocessProcessorInput,
    ) -> Result<Vec<PreprocessPhaseOneSpanResolved>, ProcessorError> {
        if self.topic_spans.is_empty() {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "phase-one preprocess output must contain at least one topic span"
                    .to_string(),
            });
        }
        let valid_segments: std::collections::HashMap<&str, i32> = input
            .segments
            .iter()
            .map(|segment| (segment.segment_id.as_str(), segment.sequence_no))
            .collect();
        let alias_map = build_segment_alias_map_for_preprocess(input);
        let mut resolved = Vec::new();
        for (index, span) in self.topic_spans.into_iter().enumerate() {
            validate_text_field(&format!("topic_spans[{index}].span_id"), &span.span_id)?;
            validate_text_field(&format!("topic_spans[{index}].label"), &span.label)?;
            validate_text_field(&format!("topic_spans[{index}].summary"), &span.summary)?;
            validate_text_field(&format!("topic_spans[{index}].focus_key"), &span.focus_key)?;
            match span.confidence_label.as_str() {
                "low" | "medium" | "high" => {}
                other => {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "topic_spans[{index}].confidence_label {other:?} is not allowed"
                        ),
                    })
                }
            }
            let start_ref = alias_map
                .get(span.start_evidence_ref.as_str())
                .cloned()
                .unwrap_or(span.start_evidence_ref);
            let end_ref = alias_map
                .get(span.end_evidence_ref.as_str())
                .cloned()
                .unwrap_or(span.end_evidence_ref);
            let Some(start) = valid_segments.get(start_ref.as_str()) else {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "topic_spans[{index}].start_evidence_ref contains unknown segment id {:?}",
                        start_ref
                    ),
                });
            };
            let Some(end) = valid_segments.get(end_ref.as_str()) else {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "topic_spans[{index}].end_evidence_ref contains unknown segment id {:?}",
                        end_ref
                    ),
                });
            };
            if start > end {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!("topic_spans[{index}] start must be <= end"),
                });
            }
            resolved.push(PreprocessPhaseOneSpanResolved {
                span_id: span.span_id,
                label: span.label,
                summary: span.summary,
                focus_key: span.focus_key,
                confidence_label: span.confidence_label,
                start_evidence_ref: start_ref,
                end_evidence_ref: end_ref,
                start_sequence_no: *start,
                end_sequence_no: *end,
                start_excerpt: excerpt_for_sequence(input, *start),
                middle_excerpt: excerpt_for_sequence(input, *start + ((*end - *start) / 2)),
                end_excerpt: excerpt_for_sequence(input, *end),
            });
        }
        resolved.sort_by_key(|span| span.start_sequence_no);
        Ok(fill_preprocess_phase_one_gaps(resolved, input))
    }
}

impl ModelReconciliationOutput {
    fn validate_against(&self, input: &ReconciliationProcessorInput) -> Result<(), ProcessorError> {
        let valid_evidence_ids: HashSet<&str> = input
            .summary
            .evidence_segment_ids
            .iter()
            .chain(
                input
                    .memories
                    .iter()
                    .flat_map(|memory| memory.evidence_segment_ids.iter()),
            )
            .chain(
                input
                    .relationships
                    .iter()
                    .flat_map(|relationship| relationship.evidence_segment_ids.iter()),
            )
            .map(String::as_str)
            .collect();
        let valid_targets: HashSet<String> = input
            .memories
            .iter()
            .map(|memory| {
                memory
                    .title
                    .clone()
                    .unwrap_or_else(|| memory.body_text.chars().take(64).collect())
            })
            .chain(input.relationships.iter().map(|relationship| {
                format!(
                    "{}:{}:{}",
                    relationship.relationship_type,
                    relationship.subject_key,
                    relationship.object_key
                )
            }))
            .collect();
        let mut seen_targets = HashSet::new();

        for (index, decision) in self.decisions.iter().enumerate() {
            validate_text_field(
                &format!("decisions[{index}].target_kind"),
                &decision.target_kind,
            )?;
            validate_text_field(
                &format!("decisions[{index}].target_key"),
                &decision.target_key,
            )?;
            validate_text_field(
                &format!("decisions[{index}].rationale"),
                &decision.rationale,
            )?;
            if !valid_targets.contains(&decision.target_key) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].target_key {:?} does not match a candidate",
                        decision.target_key
                    ),
                });
            }
            if !seen_targets.insert(decision.target_key.clone()) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].target_key {:?} is duplicated",
                        decision.target_key
                    ),
                });
            }
            validate_evidence_ids(
                &format!("decisions[{index}].evidence_segment_ids"),
                &decision.evidence_segment_ids,
                &valid_evidence_ids,
            )?;
        }

        if seen_targets != valid_targets {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "reconciliation output must provide exactly one decision for each candidate memory or relationship"
                    .to_string(),
            });
        }

        Ok(())
    }

    fn into_outputs(self) -> Vec<ReconciliationDecisionOutput> {
        self.decisions
            .into_iter()
            .map(|decision| ReconciliationDecisionOutput {
                decision_kind: decision.decision_kind,
                target_kind: decision.target_kind,
                target_key: decision.target_key,
                matched_object_id: decision.matched_object_id,
                rationale: decision.rationale,
                evidence_segment_ids: decision.evidence_segment_ids,
            })
            .collect()
    }
}

fn validate_input(input: &ArtifactProcessorInput) -> Result<(), ProcessorError> {
    if input.segments.is_empty() {
        return Err(ProcessorError::InvalidInput {
            detail: format!("artifact {} has no segments to enrich", input.artifact_id),
        });
    }

    Ok(())
}

fn validate_preprocess_input(input: &PreprocessProcessorInput) -> Result<(), ProcessorError> {
    if input.segments.is_empty() {
        return Err(ProcessorError::InvalidInput {
            detail: format!("artifact {} has no segments to preprocess", input.artifact_id),
        });
    }
    Ok(())
}

fn validate_text_field(field: &str, value: &str) -> Result<(), ProcessorError> {
    if value.trim().is_empty() {
        return Err(ProcessorError::InvalidModelOutput {
            detail: format!("{field} must not be empty"),
        });
    }

    Ok(())
}

fn validate_evidence_ids(
    field: &str,
    evidence_segment_ids: &[String],
    valid_segment_ids: &HashSet<&str>,
) -> Result<(), ProcessorError> {
    if evidence_segment_ids.is_empty() {
        return Err(ProcessorError::InvalidModelOutput {
            detail: format!("{field} must contain at least one segment id"),
        });
    }

    for segment_id in evidence_segment_ids {
        if !valid_segment_ids.contains(segment_id.as_str()) {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!("{field} contains unknown segment id {segment_id:?}"),
            });
        }
    }

    Ok(())
}

fn normalize_optional_text(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub(crate) fn combine_inference_usage(
    first: Option<InferenceUsage>,
    second: Option<InferenceUsage>,
) -> Option<InferenceUsage> {
    match (first, second) {
        (None, None) => None,
        (first, second) => Some(InferenceUsage {
            input_tokens: sum_usage_field(first.as_ref().and_then(|u| u.input_tokens), second.as_ref().and_then(|u| u.input_tokens)),
            output_tokens: sum_usage_field(first.as_ref().and_then(|u| u.output_tokens), second.as_ref().and_then(|u| u.output_tokens)),
            reasoning_tokens: sum_usage_field(first.as_ref().and_then(|u| u.reasoning_tokens), second.as_ref().and_then(|u| u.reasoning_tokens)),
            total_tokens: sum_usage_field(first.as_ref().and_then(|u| u.total_tokens), second.as_ref().and_then(|u| u.total_tokens)),
            reported_cost_micros: sum_usage_field(first.as_ref().and_then(|u| u.reported_cost_micros), second.as_ref().and_then(|u| u.reported_cost_micros)),
        }),
    }
}

fn sum_usage_field(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, None) => None,
        (left, right) => Some(left.unwrap_or(0) + right.unwrap_or(0)),
    }
}

fn excerpt_for_sequence(input: &PreprocessProcessorInput, sequence_no: i32) -> String {
    input
        .segments
        .iter()
        .find(|segment| segment.sequence_no == sequence_no)
        .map(|segment| segment.text_content.replace('\n', " "))
        .unwrap_or_default()
        .chars()
        .take(160)
        .collect()
}

fn fill_preprocess_phase_one_gaps(
    mut spans: Vec<PreprocessPhaseOneSpanResolved>,
    input: &PreprocessProcessorInput,
) -> Vec<PreprocessPhaseOneSpanResolved> {
    let covered: HashSet<i32> = spans
        .iter()
        .flat_map(|span| span.start_sequence_no..=span.end_sequence_no)
        .collect();
    for segment in &input.segments {
        if covered.contains(&segment.sequence_no) {
            continue;
        }
        let excerpt = excerpt_for_sequence(input, segment.sequence_no);
        spans.push(PreprocessPhaseOneSpanResolved {
            span_id: format!("gap-{}", segment.sequence_no),
            label: "Gap recovery".to_string(),
            summary: "Recovered uncovered tail or transition segment.".to_string(),
            focus_key: "gap recovery".to_string(),
            confidence_label: "low".to_string(),
            start_evidence_ref: segment.segment_id.clone(),
            end_evidence_ref: segment.segment_id.clone(),
            start_sequence_no: segment.sequence_no,
            end_sequence_no: segment.sequence_no,
            start_excerpt: excerpt.clone(),
            middle_excerpt: excerpt.clone(),
            end_excerpt: excerpt,
        });
    }
    spans.sort_by_key(|span| span.start_sequence_no);
    spans
}

pub(crate) fn build_conversation_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        participant_role: &'a str,
        text: &'a str,
    }

    let prompt_segments: Vec<_> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: segment_alias(index),
            participant_role: segment
                .participant_role
                .map(|role| role.as_str())
                .unwrap_or("unknown"),
            text: segment.text_content.as_str(),
        })
        .collect();

    let segments_json = serde_json::to_string_pretty(&prompt_segments)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;

    Ok(format!(
        "Input artifact:\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         \n\
         Output guidance:\n\
         - summary: 2-4 compact sentences\n\
         - classifications: prefer 0 or 1\n\
         - memories: only durable engineering facts or explicit next steps\n\
         - entities: include only explicit named people, projects, systems, or organizations with durable retrieval value\n\
         - relationships: include only explicit supported links between emitted entities or durable facts\n\
         - retrieval_intents: ask archive-only follow-up questions when duplicate detection, prior-state matching, or contradiction checks matter\n\
         - importance: be conservative\n\
         - evidence_segment_ids must use only the evidence_ref values shown in segments (for example s1, s2)\n\
         - cite the minimum evidence needed; prefer 1-2 refs per item\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        segments_json = segments_json,
    ))
}

pub(crate) fn build_preprocess_phase_one_user_prompt(
    input: &PreprocessProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        sequence_no: i32,
        participant_role: &'a str,
        text: &'a str,
    }
    let prompt_segments: Vec<_> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: segment_alias(index),
            sequence_no: segment.sequence_no,
            participant_role: segment
                .participant_role
                .map(|role| role.as_str())
                .unwrap_or("unknown"),
            text: segment.text_content.as_str(),
        })
        .collect();
    let segments_json = serde_json::to_string_pretty(&prompt_segments)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    Ok(format!(
        "Create contiguous local topic spans for this conversation.\n\
artifact_id: {artifact_id}\n\
source_type: {source_type}\n\
title: {title}\n\
\n\
segments:\n\
{segments_json}\n\
\n\
ID contract:\n\
- start_evidence_ref and end_evidence_ref must be copied exactly from the evidence_ref values shown in segments.\n\
- Never invent, rename, transform, expand, or combine refs.\n\
- Never output segment ids, artifact ids, sequence numbers, span ids, or strings like segment-*, seg-*, span-*, s1_s2.\n",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        segments_json = segments_json,
    ))
}

pub(crate) fn build_preprocess_phase_two_user_prompt(
    input: &PreprocessProcessorInput,
    spans: &[PreprocessPhaseOneSpanResolved],
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSpan<'a> {
        span_id: &'a str,
        label: &'a str,
        summary: &'a str,
        focus_key: &'a str,
        confidence_label: &'a str,
        start_evidence_ref: &'a str,
        end_evidence_ref: &'a str,
        start_sequence_no: i32,
        end_sequence_no: i32,
        start_excerpt: &'a str,
        middle_excerpt: &'a str,
        end_excerpt: &'a str,
    }
    let prompt_spans: Vec<_> = spans
        .iter()
        .map(|span| PromptSpan {
            span_id: span.span_id.as_str(),
            label: span.label.as_str(),
            summary: span.summary.as_str(),
            focus_key: span.focus_key.as_str(),
            confidence_label: span.confidence_label.as_str(),
            start_evidence_ref: span.start_evidence_ref.as_str(),
            end_evidence_ref: span.end_evidence_ref.as_str(),
            start_sequence_no: span.start_sequence_no,
            end_sequence_no: span.end_sequence_no,
            start_excerpt: span.start_excerpt.as_str(),
            middle_excerpt: span.middle_excerpt.as_str(),
            end_excerpt: span.end_excerpt.as_str(),
        })
        .collect();
    let spans_json = serde_json::to_string_pretty(&prompt_spans)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let mut allowed_refs = spans
        .iter()
        .flat_map(|span| {
            [
                span.start_evidence_ref.clone(),
                span.end_evidence_ref.clone(),
            ]
        })
        .collect::<Vec<_>>();
    allowed_refs.sort();
    allowed_refs.dedup();
    Ok(format!(
        "Merge local topic spans into durable topic threads for this conversation.\n\
artifact_id: {artifact_id}\n\
source_type: {source_type}\n\
title: {title}\n\
\n\
Merge criteria:\n\
- Merge spans only when they clearly continue the same specific question, decision, troubleshooting thread, workflow, or entity-specific investigation.\n\
- Use focus_key as a strong identity hint. Same broad domain with different focus_key values should usually stay separate.\n\
- Do NOT merge spans merely because they are in the same broad domain, product area, or subject family.\n\
- Distinguish separate subthreads inside the same domain.\n\
- Prefer keeping two threads separate when unsure.\n\
\n\
ID contract:\n\
- In topic_threads.spans, start_evidence_ref and end_evidence_ref must be copied exactly from the start_evidence_ref/end_evidence_ref values shown in the spans list.\n\
- Allowed refs for this artifact: {allowed_refs_json}\n\
- Never output span_id in those fields.\n\
- Never invent, rename, transform, expand, or combine refs.\n\
- Never output segment ids, artifact ids, sequence numbers, or strings like segment-*, seg-*, span-*, s1_s2.\n\
\n\
spans:\n\
{spans_json}\n",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        allowed_refs_json = serde_json::to_string(&allowed_refs)
            .map_err(|source| ProcessorError::SerializePrompt { source })?,
        spans_json = spans_json,
    ))
}

pub(crate) fn should_shape_conversation_input(input: &ArtifactProcessorInput) -> bool {
    let char_count: usize = input
        .segments
        .iter()
        .map(|segment| segment.text_content.len())
        .sum();
    input.segments.len() >= 60 || char_count > 100_000
}

pub(crate) fn build_reconciliation_prompt(
    input: &ReconciliationProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct CandidateMemory<'a> {
        target_key: String,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }

    #[derive(Serialize)]
    struct CandidateRelationship<'a> {
        target_key: String,
        relationship_type: &'a str,
        subject_key: &'a str,
        object_key: &'a str,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }

    let memories: Vec<_> = input
        .memories
        .iter()
        .map(|memory| CandidateMemory {
            target_key: memory
                .title
                .clone()
                .unwrap_or_else(|| memory.body_text.chars().take(64).collect()),
            title: memory.title.as_deref(),
            body_text: memory.body_text.as_str(),
            evidence_segment_ids: &memory.evidence_segment_ids,
        })
        .collect();
    let relationships: Vec<_> = input
        .relationships
        .iter()
        .map(|relationship| CandidateRelationship {
            target_key: format!(
                "{}:{}:{}",
                relationship.relationship_type, relationship.subject_key, relationship.object_key
            ),
            relationship_type: relationship.relationship_type.as_str(),
            subject_key: relationship.subject_key.as_str(),
            object_key: relationship.object_key.as_str(),
            title: relationship.title.as_deref(),
            body_text: relationship.body_text.as_str(),
            evidence_segment_ids: &relationship.evidence_segment_ids,
        })
        .collect();

    let memories_json = serde_json::to_string_pretty(&memories)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    let relationships_json = serde_json::to_string_pretty(&relationships)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;

    Ok(format!(
        "Reconcile extraction candidates against archive retrieval results.\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         summary: {summary}\n\
         \n\
         candidate_memories:\n\
         {memories_json}\n\
         \n\
         candidate_relationships:\n\
         {relationships_json}\n\
         \n\
         retrieval_results:\n\
         {retrieval_results_json}\n\
         \n\
         Output one decision per candidate memory or relationship.\n\
         target_key must match a candidate target_key exactly.\n\
         target_kind must be memory or relationship.\n",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        summary = input.summary.body_text,
        memories_json = memories_json,
        relationships_json = relationships_json,
        retrieval_results_json = input.retrieval_results_json,
    ))
}

fn preview(value: &str) -> String {
    value.chars().take(240).collect()
}

fn build_segment_alias_map(
    input: &ArtifactProcessorInput,
) -> std::collections::HashMap<String, String> {
    input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| (segment_alias(index), segment.segment_id.clone()))
        .collect()
}

fn build_segment_alias_map_for_preprocess(
    input: &PreprocessProcessorInput,
) -> std::collections::HashMap<String, String> {
    let mut aliases = std::collections::HashMap::new();
    for (index, segment) in input.segments.iter().enumerate() {
        let ordinal = index + 1;
        let actual = segment.segment_id.clone();
        for alias in [
            format!("s{ordinal}"),
            format!("s_{ordinal}"),
            format!("s-{ordinal}"),
            format!("seg{ordinal}"),
            format!("seg_{ordinal}"),
            format!("seg-{ordinal}"),
            format!("segment{ordinal}"),
            format!("segment_{ordinal}"),
            format!("segment-{ordinal}"),
            format!("span{ordinal}"),
            format!("span_{ordinal}"),
            format!("span-{ordinal}"),
        ] {
            aliases.insert(alias, actual.clone());
        }
    }
    aliases
}

fn segment_alias(index: usize) -> String {
    format!("s{}", index + 1)
}

fn resolve_preprocess_evidence_ref(
    raw: &str,
    alias_map: &std::collections::HashMap<String, String>,
    span_map: &std::collections::HashMap<&str, (&str, &str)>,
    pick_start: bool,
) -> Option<String> {
    if let Some(actual) = alias_map.get(raw) {
        return Some(actual.clone());
    }
    if let Some((start, end)) = span_map.get(raw) {
        return Some(if pick_start { *start } else { *end }.to_string());
    }

    let aliases = extract_segment_aliases(raw);
    if aliases.is_empty() {
        return None;
    }

    let selected = if pick_start {
        aliases.first()
    } else {
        aliases.last()
    }?;
    alias_map.get(selected.as_str()).cloned()
}

fn extract_segment_aliases(raw: &str) -> Vec<String> {
    let lowered = raw.to_ascii_lowercase();
    let bytes = lowered.as_bytes();
    let mut aliases = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b's' {
            index += 1;
            continue;
        }
        let start = index;
        index += 1;
        let digit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if index > digit_start {
            aliases.push(lowered[start..index].to_string());
        }
    }
    aliases
}

fn should_retry_with_repair(error: &ProcessorError) -> bool {
    matches!(
        error,
        ProcessorError::ParseModelJson { .. } | ProcessorError::InvalidModelOutput { .. }
    )
}

fn build_repair_prompt(original_prompt: &str, error: &ProcessorError) -> String {
    format!(
        "{original_prompt}\n\nYour previous response was invalid.\n\
Return valid JSON only, matching the required schema exactly.\n\
Do not add explanation or markdown.\n\
Previous output problem: {}\n",
        error.compact_reason()
    )
}

pub(crate) fn structured_output_schema_wrapper() -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_artifact_enrichment",
        "strict": true,
        "schema": structured_output_schema()
    })
}

pub(crate) fn preprocess_output_schema_wrapper() -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_preprocess_segmentation",
        "strict": true,
        "schema": preprocess_output_schema()
    })
}

pub(crate) fn preprocess_phase_one_schema_wrapper() -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_preprocess_phase_one",
        "strict": true,
        "schema": preprocess_phase_one_schema()
    })
}

pub(crate) fn preprocess_phase_one_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["topic_spans"],
        "properties": {
            "topic_spans": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["span_id", "label", "summary", "focus_key", "confidence_label", "start_evidence_ref", "end_evidence_ref"],
                    "properties": {
                        "span_id": { "type": "string", "minLength": 1 },
                        "label": { "type": "string", "minLength": 1 },
                        "summary": { "type": "string", "minLength": 1 },
                        "focus_key": { "type": "string", "minLength": 1 },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "start_evidence_ref": { "type": "string", "minLength": 1 },
                        "end_evidence_ref": { "type": "string", "minLength": 1 }
                    }
                }
            }
        }
    })
}

pub(crate) fn preprocess_output_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["topic_threads", "escalate_to_quality", "escalation_reason"],
        "properties": {
            "topic_threads": {
                "type": "array",
                "minItems": 1,
                "maxItems": 12,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["thread_key", "label", "summary", "confidence_label", "spans"],
                    "properties": {
                        "thread_key": { "type": "string", "minLength": 1 },
                        "label": { "type": "string", "minLength": 1 },
                        "summary": { "type": "string", "minLength": 1 },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "spans": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 8,
                            "items": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["start_evidence_ref", "end_evidence_ref"],
                                "properties": {
                                    "start_evidence_ref": { "type": "string", "minLength": 1 },
                                    "end_evidence_ref": { "type": "string", "minLength": 1 }
                                }
                            }
                        }
                    }
                }
            },
            "escalate_to_quality": { "type": "boolean" },
            "escalation_reason": { "type": "string" }
        }
    })
}

pub(crate) fn structured_output_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary",
            "classifications",
            "memories",
            "entities",
            "relationships",
            "retrieval_intents",
            "importance_score",
            "escalate_to_frontier",
            "escalation_reason"
        ],
        "properties": {
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "required": ["title", "body_text", "evidence_segment_ids"],
                "properties": {
                    "title": { "type": "string", "minLength": 1 },
                    "body_text": { "type": "string", "minLength": 1 },
                    "evidence_segment_ids": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "type": "string", "minLength": 1 }
                    }
                }
            },
            "classifications": {
                "type": "array",
                "maxItems": 2,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "classification_type",
                        "classification_value",
                        "title",
                        "body_text",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "classification_type": {
                            "type": "string",
                            "enum": ["topic", "intent"]
                        },
                        "classification_value": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            },
            "memories": {
                "type": "array",
                "maxItems": 5,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "memory_type",
                        "memory_scope",
                        "memory_scope_value",
                        "title",
                        "body_text",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "memory_type": {
                            "type": "string",
                            "enum": [
                                "preference",
                                "project_fact",
                                "identity_fact",
                                "ongoing_task",
                                "reference"
                            ]
                        },
                        "memory_scope": {
                            "type": "string",
                            "enum": ["artifact"]
                        },
                        "memory_scope_value": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            },
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["entity_key", "display_name", "entity_type", "evidence_segment_ids"],
                    "properties": {
                        "entity_key": { "type": "string", "minLength": 1 },
                        "display_name": { "type": "string", "minLength": 1 },
                        "entity_type": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            },
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "relationship_type",
                        "subject_key",
                        "object_key",
                        "title",
                        "body_text",
                        "confidence_label",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "relationship_type": { "type": "string", "minLength": 1 },
                        "subject_key": { "type": "string", "minLength": 1 },
                        "object_key": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            },
            "retrieval_intents": {
                "type": "array",
                "maxItems": MAX_RETRIEVAL_INTENTS,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["question", "query_text", "intent_type", "evidence_segment_ids"],
                    "properties": {
                        "question": { "type": "string", "minLength": 1 },
                        "query_text": { "type": "string", "minLength": 1 },
                        "intent_type": {
                            "type": "string",
                            "enum": ["topic_lookup", "memory_match", "entity_lookup", "relationship_lookup", "contradiction_check"]
                        },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            },
            "importance_score": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10
            },
            "escalate_to_frontier": { "type": "boolean" },
            "escalation_reason": { "type": "string" }
        }
    })
}

pub(crate) fn reconciliation_output_schema_wrapper() -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_reconciliation",
        "strict": true,
        "schema": reconciliation_output_schema()
    })
}

pub(crate) fn reconciliation_output_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["decision_kind", "target_kind", "target_key", "rationale", "evidence_segment_ids"],
                    "properties": {
                        "decision_kind": {
                            "type": "string",
                            "enum": [
                                "create_new",
                                "attach_to_existing",
                                "strengthen_existing",
                                "supersede_existing",
                                "contradicts_existing",
                                "insufficient_evidence"
                            ]
                        },
                        "target_kind": { "type": "string", "enum": ["memory", "relationship"] },
                        "target_key": { "type": "string", "minLength": 1 },
                        "matched_object_id": { "type": "string", "minLength": 1 },
                        "rationale": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": { "type": "string", "minLength": 1 }
                        }
                    }
                }
            }
        }
    })
}

const OPENAI_PROMPT_VERSION: &str = "openai-strict-v1";
pub(crate) const GEMINI_PROMPT_VERSION: &str = "gemini-strict-v5";
const PREPROCESS_PROMPT_VERSION: &str = "preprocess-v3";
pub(crate) const PREPROCESS_PHASE_ONE_SYSTEM_PROMPT: &str = "You are OpenArchive's local segmentation engine. Return ONLY valid JSON.\n\nRules:\n1. Partition the conversation into contiguous topic_spans.\n2. Every segment must be covered exactly once.\n3. Keep spans local and contiguous. Do not merge non-contiguous returns in this phase.\n4. Split when the active question, decision, troubleshooting target, work item, or named subject changes in a meaningful way.\n5. Do not create tiny spans for brief clarifications unless the thread actually changes.\n6. Labels must be short and specific.\n7. Summaries must be one short sentence.\n8. focus_key must be a short phrase naming the concrete thread target, such as the specific question, workflow, entity, or decision under discussion.\n9. start_evidence_ref and end_evidence_ref must be copied exactly from the provided evidence_ref strings.\n10. Never invent, transform, expand, or combine ids.\n11. Never output values like segment-*, seg-*, span-*, artifact ids, or sequence numbers in evidence ref fields.\n12. Output valid JSON only.";
pub(crate) const PREPROCESS_PHASE_TWO_SYSTEM_PROMPT: &str = "You are OpenArchive's thread consolidation engine. Return ONLY valid JSON.\n\nRules:\n1. Merge local spans into durable topic_threads.\n2. Revisited topics should reuse one thread with multiple spans.\n3. Do not merge spans on broad domain overlap alone.\n4. Only merge when the later span clearly resumes the same specific thread of work, question, workflow, or investigation.\n5. Treat focus_key as a strong identity signal; different focus_key values usually imply different threads unless the excerpts show a clear return to the same exact thread.\n6. Prefer separate threads when unsure.\n7. In topic_threads.spans, start_evidence_ref and end_evidence_ref must be copied exactly from the provided spans.\n8. Never output span_id in evidence ref fields.\n9. Never invent, transform, expand, or combine ids.\n10. Never output values like segment-*, seg-*, span-*, artifact ids, or sequence numbers in evidence ref fields.\n11. Summaries must be one short sentence.\n12. Output valid JSON only.";
const ARTIFACT_EXTRACTION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict extraction engine. Read one artifact and return ONLY valid JSON.\n\nReturn exactly these sections:\n- summary\n- classifications\n- memories\n- entities\n- relationships\n- retrieval_intents\n- importance_score\n- escalate_to_frontier\n- escalation_reason\n\nRules:\n1. Output valid JSON only. No markdown or extra text.\n2. Every summary, classification, memory, entity, relationship, and retrieval_intent must cite real evidence_segment_ids from the artifact.\n3. Do not use prior archive knowledge. Reason only from the provided artifact.\n4. Prefer explicit uncertainty over guessed continuity with prior brain state.\n5. Use retrieval_intents only for questions that require archive lookups to resolve ambiguity, duplicate detection, contradiction checks, or prior-state matching.\n6. Prefer fewer, stronger memories over weak ones.\n7. Emit entities only when there is explicit artifact support.\n8. Emit relationships only when the artifact explicitly supports the link.\n9. Memories must use only these memory_type values: preference, project_fact, identity_fact, ongoing_task, reference.\n10. relationship confidence_label must be low, medium, or high.\n11. retrieval_intent intent_type must be one of topic_lookup, memory_match, entity_lookup, relationship_lookup, contradiction_check.\n12. memory_scope is always artifact and memory_scope_value is the artifact_id.\n13. Summary must be 2-4 compact sentences focused on lasting engineering substance.\n14. Low-signal artifacts should usually have low importance and no classifications.\n15. escalate_to_frontier may be true only when importance_score >= 8 and the artifact would materially benefit from a higher-quality pass.\n16. escalation_reason must be empty when escalate_to_frontier is false and one short sentence when true.";

pub(crate) const GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict extraction engine for Gemini. Read one artifact and return ONLY valid JSON.\n\nReturn exactly these sections:\n- summary\n- classifications\n- memories\n- entities\n- relationships\n- retrieval_intents\n- importance_score\n- escalate_to_frontier\n- escalation_reason\n\nRules:\n1. Output valid JSON only. No markdown or extra text.\n2. Every emitted item must cite real evidence_segment_ids from the artifact.\n3. Do not invent facts, intentions, preferences, identities, entities, or commitments.\n4. Keep the output compact, but do not overcompress rich artifacts.\n5. Use retrieval_intents to ask for archive lookups when prior-state matching, contradiction checks, or duplicate detection matter.\n6. Do not guess prior continuity; emit retrieval_intents instead.\n7. Emit relationships only when the artifact explicitly supports the link.\n8. relationship confidence_label must be low, medium, or high.\n9. memory_scope is always artifact and memory_scope_value is the artifact_id.\n10. Low-signal artifacts should usually have low importance and no classifications.\n11. escalate_to_frontier may be true only when importance_score >= 8 and the artifact would materially benefit from a higher-quality pass.\n12. escalation_reason must be empty when escalate_to_frontier is false and one short sentence when true.";

pub(crate) const RECONCILIATION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict reconciliation engine. Use only the provided extraction candidates, retrieval results, and source evidence. Return ONLY valid JSON.\n\nRules:\n1. Prefer no merge over a weak merge.\n2. Choose create_new when the archive evidence is insufficient.\n3. Use attach_to_existing or strengthen_existing only when the retrieved object clearly matches the candidate.\n4. Use supersede_existing only when the new artifact clearly updates or replaces prior state.\n5. Use contradicts_existing only when the artifact clearly conflicts with retrieved prior state.\n6. Never merge identities, projects, or relationships on vague topical overlap.\n7. Every decision must cite real evidence_segment_ids from the extraction candidates.\n8. Output valid JSON only.";

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("unsupported enrichment tier {tier}")]
    UnsupportedTier { tier: String },

    #[error("invalid processor input: {detail}")]
    InvalidInput { detail: String },

    #[error("failed to serialize processor prompt")]
    SerializePrompt {
        #[source]
        source: serde_json::Error,
    },

    #[error("failed to build inference HTTP client")]
    BuildHttpClient {
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to send inference request")]
    SendInferenceRequest {
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to read inference response body")]
    ReadInferenceResponse {
        #[source]
        source: reqwest::Error,
    },

    #[error("inference returned HTTP status {status}: {body_preview}")]
    InferenceHttpStatus { status: u16, body_preview: String },

    #[error("failed to parse inference response JSON: {body_preview}")]
    ParseInferenceResponse {
        #[source]
        source: serde_json::Error,
        body_preview: String,
    },

    #[error("failed to parse model output JSON: {body_preview}")]
    ParseModelJson {
        #[source]
        source: serde_json::Error,
        body_preview: String,
    },

    #[error("invalid model output: {detail}")]
    InvalidModelOutput { detail: String },

    #[error("{message}")]
    Message { message: String },
}

impl ProcessorError {
    fn compact_reason(&self) -> String {
        match self {
            ProcessorError::ParseModelJson { body_preview, .. } => {
                format!("response was not valid JSON; preview: {body_preview}")
            }
            ProcessorError::InvalidModelOutput { detail } => detail.clone(),
            other => other.to_string(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            ProcessorError::SendInferenceRequest { .. }
            | ProcessorError::ReadInferenceResponse { .. }
            | ProcessorError::ParseInferenceResponse { .. } => true,
            ProcessorError::InferenceHttpStatus { status, .. } => {
                matches!(*status, 408 | 409 | 425 | 429 | 500..=599)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedInferenceClient {
        response: String,
    }

    impl InferenceClient for FixedInferenceClient {
        fn complete_json(
            &self,
            _model: &str,
            _system_prompt: &str,
            _user_prompt: &str,
            _schema: &serde_json::Value,
        ) -> Result<InferenceResult, ProcessorError> {
            Ok(InferenceResult {
                output_text: self.response.clone(),
                usage: None,
            })
        }
    }

    struct SequenceInferenceClient {
        responses: std::sync::Mutex<Vec<String>>,
    }

    impl InferenceClient for SequenceInferenceClient {
        fn complete_json(
            &self,
            _model: &str,
            _system_prompt: &str,
            _user_prompt: &str,
            _schema: &serde_json::Value,
        ) -> Result<InferenceResult, ProcessorError> {
            let mut responses = self.responses.lock().expect("sequence lock");
            if responses.is_empty() {
                return Err(ProcessorError::Message {
                    message: "no more inference responses".to_string(),
                });
            }
            Ok(InferenceResult {
                output_text: responses.remove(0),
                usage: None,
            })
        }
    }

    fn sample_input() -> ArtifactProcessorInput {
        ArtifactProcessorInput {
            artifact_id: "artifact-1".to_string(),
            import_id: "import-1".to_string(),
            source_type: SourceType::ChatGptExport,
            title: Some("Architecture direction".to_string()),
            participants: Vec::new(),
            segments: vec![
                LoadedSegment {
                    segment_id: "seg-1".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 0,
                    text_content: "We should remove silent fallback.".to_string(),
                    created_at_source: None,
                    visibility_status: crate::VisibilityStatus::Visible,
                },
                LoadedSegment {
                    segment_id: "seg-2".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 1,
                    text_content: "Task 12 should use GPT-4.1 mini.".to_string(),
                    created_at_source: None,
                    visibility_status: crate::VisibilityStatus::Visible,
                },
            ],
        }
    }

    #[test]
    fn openai_processor_parses_and_validates_output() {
        let client = Arc::new(FixedInferenceClient {
            response: serde_json::json!({
                "summary": {
                    "title": "Hardening and next steps",
                    "body_text": "The worker should fail invalid payloads and the next milestone is the real pipeline.",
                    "evidence_segment_ids": ["seg-1", "seg-2"]
                },
                "classifications": [
                    {
                        "classification_type": "intent",
                        "classification_value": "fix_failure_handling",
                        "title": "Fix failure handling",
                        "body_text": "The artifact is focused on hardening worker failure behavior.",
                        "evidence_segment_ids": ["seg-1"]
                    }
                ],
                "memories": [
                    {
                        "memory_type": "project_fact",
                        "memory_scope": "artifact",
                        "memory_scope_value": "artifact-1",
                        "title": "Task 12 uses hosted inference",
                        "body_text": "The first real pipeline uses a hosted inference provider.",
                        "evidence_segment_ids": ["seg-2"]
                    }
                ],
                "entities": [],
                "relationships": [],
                "retrieval_intents": [],
                "importance_score": 8,
                "escalate_to_frontier": true,
                "escalation_reason": "High-value implementation direction with durable engineering impact."
            })
            .to_string(),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory
            .build(EnrichmentTier::Standard)
            .expect("processor should build");

        let output = processor
            .process(&sample_input())
            .expect("processor should succeed");

        assert_eq!(output.pipeline_name, "openai_enrichment");
        assert_eq!(output.classifications.len(), 1);
        assert_eq!(output.memories.len(), 1);
        assert_eq!(output.summary.evidence_segment_ids, vec!["seg-1", "seg-2"]);
        assert_eq!(output.importance_score, 8);
        assert!(output.escalate_to_frontier);
    }

    #[test]
    fn openai_preprocess_processor_parses_topic_threads_and_resolves_aliases() {
        let client = Arc::new(SequenceInferenceClient {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({
                    "topic_spans": [
                        {
                            "span_id": "span-1",
                            "label": "Fallback hardening",
                            "summary": "The conversation focuses on fallback removal.",
                            "focus_key": "fallback removal",
                            "confidence_label": "high",
                            "start_evidence_ref": "s1",
                            "end_evidence_ref": "s1"
                        },
                        {
                            "span_id": "span-2",
                            "label": "Model selection",
                            "summary": "The conversation briefly discusses model choice.",
                            "focus_key": "model choice",
                            "confidence_label": "medium",
                            "start_evidence_ref": "s2",
                            "end_evidence_ref": "s2"
                        }
                    ]
                })
                .to_string(),
                serde_json::json!({
                    "topic_threads": [
                        {
                            "thread_key": "fallback-hardening",
                            "label": "Fallback hardening",
                            "summary": "Conversation returns to the fallback-removal topic across multiple turns.",
                            "confidence_label": "high",
                            "spans": [
                                {
                                    "start_evidence_ref": "s1",
                                    "end_evidence_ref": "s1"
                                },
                                {
                                    "start_evidence_ref": "s2",
                                    "end_evidence_ref": "s2"
                                }
                            ]
                        }
                    ],
                    "escalate_to_quality": false,
                    "escalation_reason": ""
                })
                .to_string(),
            ]),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory
            .build_preprocess_processor(EnrichmentTier::Standard)
            .expect("preprocess processor should build");

        let output = processor
            .segment(&PreprocessProcessorInput {
                artifact_id: "artifact-1".to_string(),
                import_id: "import-1".to_string(),
                source_type: SourceType::ChatGptExport,
                title: Some("Architecture direction".to_string()),
                participants: Vec::new(),
                segments: sample_input().segments,
            })
            .expect("preprocess processor should succeed");

        assert_eq!(output.pipeline_name, "openai_preprocess");
        assert_eq!(output.topic_threads.len(), 1);
        assert_eq!(output.topic_threads[0].spans.len(), 2);
        assert_eq!(output.topic_threads[0].spans[0].start_sequence_no, 0);
        assert_eq!(output.topic_threads[0].spans[1].end_sequence_no, 1);
        assert!(!output.escalate_to_quality);
    }

    #[test]
    fn openai_processor_rejects_unknown_evidence_segment_ids() {
        let client = Arc::new(FixedInferenceClient {
            response: serde_json::json!({
                "summary": {
                    "title": "Bad evidence",
                    "body_text": "The model referenced an unknown segment.",
                    "evidence_segment_ids": ["seg-99"]
                },
                "classifications": [
                    {
                        "classification_type": "topic",
                        "classification_value": "worker_failure_handling",
                        "title": "Worker hardening",
                        "body_text": "Specific hardening work.",
                        "evidence_segment_ids": ["seg-1"]
                    }
                ],
                "memories": [],
                "entities": [],
                "relationships": [],
                "retrieval_intents": [],
                "importance_score": 5,
                "escalate_to_frontier": false,
                "escalation_reason": ""
            })
            .to_string(),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory
            .build(EnrichmentTier::Standard)
            .expect("processor should build");

        let err = processor
            .process(&sample_input())
            .expect_err("processor should fail");
        assert!(matches!(err, ProcessorError::InvalidModelOutput { .. }));
    }

    #[test]
    fn openai_processor_retries_once_on_invalid_output_and_accepts_repair() {
        let client = Arc::new(SequenceInferenceClient {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({
                    "summary": {
                        "title": "Bad evidence",
                        "body_text": "The model referenced an unknown segment.",
                        "evidence_segment_ids": ["seg-99"]
                    },
                    "classifications": [],
                    "memories": [],
                    "entities": [],
                    "relationships": [],
                    "retrieval_intents": [],
                    "importance_score": 5,
                    "escalate_to_frontier": false,
                    "escalation_reason": ""
                })
                .to_string(),
                serde_json::json!({
                    "summary": {
                        "title": "Hardening and next steps",
                        "body_text": "The worker should fail invalid payloads and the next milestone is the real pipeline.",
                        "evidence_segment_ids": ["seg-1", "seg-2"]
                    },
                    "classifications": [
                        {
                            "classification_type": "intent",
                            "classification_value": "fix_failure_handling",
                            "title": "Fix failure handling",
                            "body_text": "The artifact is focused on hardening worker failure behavior.",
                            "evidence_segment_ids": ["seg-1"]
                        }
                    ],
                    "memories": [
                        {
                            "memory_type": "project_fact",
                            "memory_scope": "artifact",
                            "memory_scope_value": "artifact-1",
                            "title": "Task 12 uses hosted inference",
                            "body_text": "The first real pipeline uses a hosted inference provider.",
                            "evidence_segment_ids": ["seg-2"]
                        }
                    ],
                    "entities": [],
                    "relationships": [],
                    "retrieval_intents": [],
                    "importance_score": 7,
                    "escalate_to_frontier": false,
                    "escalation_reason": ""
                })
                .to_string(),
            ]),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory
            .build(EnrichmentTier::Standard)
            .expect("processor should build");

        let output = processor
            .process(&sample_input())
            .expect("processor should repair and succeed");

        assert_eq!(output.classifications.len(), 1);
        assert_eq!(output.memories.len(), 1);
        assert_eq!(output.importance_score, 7);
        assert!(!output.escalate_to_frontier);
    }
}
