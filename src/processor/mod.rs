use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reqwest::blocking::{multipart, Client};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::config::{OpenAiConfig, OpenAiReasoningEffort};
use crate::storage::types::{
    ArtifactClass, EnrichmentTier, LoadedParticipant, LoadedSegment, ReconciliationDecisionKind,
    RetrievalIntent, ScopeType, SourceType,
};

mod anthropic;
mod gemini;
mod grok;
pub use anthropic::AnthropicProcessorFactory;
pub use gemini::{
    GeminiBatchClient, GeminiBatchEnrichmentRequest, GeminiBatchJob, GeminiProcessorFactory,
};
pub use grok::GrokProcessorFactory;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactProcessorInput {
    pub artifact_id: String,
    pub import_id: String,
    pub artifact_class: ArtifactClass,
    pub source_type: SourceType,
    pub title: Option<String>,
    pub participants: Vec<LoadedParticipant>,
    pub segments: Vec<LoadedSegment>,
}

pub(crate) fn artifact_processor_batch_custom_id(input: &ArtifactProcessorInput) -> String {
    let mut hasher = Sha256::new();
    if let Some(title) = &input.title {
        hasher.update(title.as_bytes());
    }
    for segment in &input.segments {
        hasher.update(segment.segment_id.as_bytes());
        hasher.update([0]);
    }
    let digest = format!("{:x}", hasher.finalize());
    let first_sequence_no = input
        .segments
        .first()
        .map(|segment| segment.sequence_no)
        .unwrap_or_default();
    let last_sequence_no = input
        .segments
        .last()
        .map(|segment| segment.sequence_no)
        .unwrap_or_default();
    format!(
        "{}:extract:{}-{}:{}",
        input.artifact_id,
        first_sequence_no,
        last_sequence_no,
        &digest[..12]
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SummaryOutput {
    pub title: Option<String>,
    pub body_text: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClassificationOutput {
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub classification_type: String,
    pub classification_value: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryOutput {
    pub candidate_key: String,
    pub title: Option<String>,
    pub body_text: String,
    pub memory_type: String,
    pub memory_scope: ScopeType,
    pub memory_scope_value: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityOutput {
    pub entity_key: String,
    pub display_name: String,
    pub entity_type: String,
    pub evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipOutput {
    pub relationship_type: String,
    pub subject_key: String,
    pub object_key: String,
    pub title: Option<String>,
    pub body_text: String,
    pub confidence_label: String,
    pub evidence_segment_ids: Vec<String>,
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
    pub retrieval_intents: Vec<RetrievalIntent>,
    pub importance_score: u8,
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

const MAX_CLASSIFICATIONS: usize = 5;
const MAX_MEMORIES: usize = 15;
const MAX_RETRIEVAL_INTENTS: usize = 8;
const MEMORY_TYPE_VALUES: [&str; 5] = [
    "personal_fact",
    "preference",
    "project_fact",
    "ongoing_state",
    "reference",
];

pub trait ArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

pub trait ReconciliationProcessor {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError>;
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
                candidate_key: memory_candidate_key_from_fields(
                    "project_fact",
                    ScopeType::Artifact,
                    &input.artifact_id,
                    Some("Artifact memory"),
                    &format!(
                        "Stub memory for artifact {} derived from {} segments.",
                        input.artifact_id, segment_count
                    ),
                ),
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
                target_key: memory.candidate_key.clone(),
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

    // --- Non-blocking batch submitter builders (for per-stage pollers) ---

    fn build_extraction_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ExtractionBatchSubmitter>>, ProcessorError> {
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
    fn process(
        &self,
        processor: &HostedArtifactProcessor,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError>;
}

#[derive(Debug, Default)]
pub struct StubProcessorFactory;

impl ArtifactProcessorFactory for StubProcessorFactory {
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
        let candidate_result = self.client.complete_json(
            &self.model,
            candidate_system_prompt(input),
            user_prompt,
            &candidate_output_schema_wrapper(input),
        )?;
        let candidate = parse_candidate_output(&candidate_result.output_text, input)
            .map_err(|err| attach_output_preview(err, &candidate_result.output_text))?;
        Ok(candidate.into_processor_output(
            input,
            self.model.clone(),
            candidate_result.usage,
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
        parsed.into_validated_outputs(input)
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
}

impl ConversationEnrichmentStrategy {
    fn anthropic_default() -> Arc<dyn EnrichmentStrategy> {
        Arc::new(Self {
            prompt_version: ANTHROPIC_PROMPT_VERSION,
        })
    }
}

impl EnrichmentStrategy for ConversationEnrichmentStrategy {
    fn prompt_version(&self) -> &'static str {
        self.prompt_version
    }

    fn process(
        &self,
        processor: &HostedArtifactProcessor,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let prompt = build_two_phase_candidate_user_prompt(input)?;
        processor.run_prompt(input, &prompt)
    }
}

pub struct OpenAiProcessorFactory {
    client: Arc<OpenAiClient>,
    batch_client: Option<Arc<OpenAiClient>>,
    extract_max_output_tokens: u32,
    extract_repair_max_output_tokens: u32,
    standard_model: String,
    quality_model: String,
    reconcile_standard_model: String,
    reconcile_quality_model: String,
}

impl OpenAiProcessorFactory {
    pub fn new(config: OpenAiConfig) -> Result<Self, String> {
        let client = Arc::new(OpenAiClient::new(&config).map_err(|err| err.to_string())?);
        let quality_model = config
            .quality_model
            .clone()
            .unwrap_or_else(|| config.standard_model.clone());
        let reconcile_quality_model = config
            .reconcile_quality_model
            .clone()
            .or_else(|| config.quality_model.clone())
            .unwrap_or_else(|| config.reconcile_standard_model.clone());

        Ok(Self {
            client: client.clone(),
            batch_client: Some(client),
            extract_max_output_tokens: config.max_output_tokens,
            extract_repair_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            standard_model: config.standard_model,
            quality_model,
            reconcile_standard_model: config.reconcile_standard_model,
            reconcile_quality_model,
        })
    }

    #[cfg(test)]
    fn with_client(
        client: Arc<dyn InferenceClient>,
        standard_model: impl Into<String>,
        quality_model: impl Into<String>,
    ) -> Self {
        let standard_model = standard_model.into();
        let quality_model = quality_model.into();
        Self {
            client: Arc::new(OpenAiClient::for_tests(client)),
            batch_client: None,
            extract_max_output_tokens: 4000,
            extract_repair_max_output_tokens: 8000,
            standard_model: standard_model.clone(),
            quality_model: quality_model.clone(),
            reconcile_standard_model: standard_model,
            reconcile_quality_model: quality_model,
        }
    }
}

impl ArtifactProcessorFactory for OpenAiProcessorFactory {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(OpenAiArtifactProcessor {
            client: Arc::clone(&self.client),
            candidate_model: model,
            max_output_tokens: self.extract_max_output_tokens,
            repair_max_output_tokens: self.extract_repair_max_output_tokens,
        }))
    }

    fn build_reconciliation_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.reconcile_standard_model.clone(),
            EnrichmentTier::Quality => self.reconcile_quality_model.clone(),
        };
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(HostedReconciliationProcessor {
            client,
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
            candidate_model: model,
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
            EnrichmentTier::Standard => self.reconcile_standard_model.clone(),
            EnrichmentTier::Quality => self.reconcile_quality_model.clone(),
        };
        Ok(Some(Box::new(OpenAiReconciliationSubmitter {
            client: Arc::clone(client),
            model,
        })))
    }
}

pub(crate) trait InferenceClient: Send + Sync {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError>;
}

#[derive(Debug, Clone)]
pub(crate) struct InferenceResult {
    output_text: String,
    usage: Option<InferenceUsage>,
}

struct OpenAiClient {
    delegate: Option<Arc<dyn InferenceClient>>,
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
            .timeout(Duration::from_secs(180))
            .build()
            .map_err(|source| ProcessorError::BuildHttpClient { source })?;

        Ok(Self {
            delegate: None,
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            max_output_tokens: config.max_output_tokens,
            reasoning_effort_override: config.reasoning_effort_override,
        })
    }

    #[cfg(test)]
    fn for_tests(delegate: Arc<dyn InferenceClient>) -> Self {
        Self {
            delegate: Some(delegate),
            client: Client::builder().build().expect("test http client"),
            base_url: "https://api.openai.com/v1".to_string(),
            max_output_tokens: 4000,
            reasoning_effort_override: OpenAiReasoningEffort::Auto,
        }
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
        if let Some(delegate) = &self.delegate {
            return delegate.complete_json(model, system_prompt, user_prompt, schema);
        }
        self.complete_via_responses(model, system_prompt, user_prompt, schema)
    }
}

impl OpenAiClient {
    fn complete_json_with_max_output_tokens(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        if let Some(delegate) = &self.delegate {
            return delegate.complete_json(model, system_prompt, user_prompt, schema);
        }
        self.complete_via_responses_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            max_output_tokens,
        )
    }

    fn responses_text_format(schema: &serde_json::Value) -> serde_json::Value {
        let format_type = schema
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match format_type {
            "json_schema" => {
                let mut wrapped = schema.clone();
                if let Some(inner) = wrapped.get_mut("schema") {
                    Self::normalize_schema_for_openai(inner);
                }
                wrapped
            }
            "json_object" | "text" => schema.clone(),
            _ => {
                let mut normalized = schema.clone();
                Self::normalize_schema_for_openai(&mut normalized);
                serde_json::json!({
                    "type": "json_schema",
                    "name": "openarchive_structured_output",
                    "strict": true,
                    "schema": normalized,
                })
            }
        }
    }

    fn normalize_schema_for_openai(schema: &mut serde_json::Value) {
        match schema {
            serde_json::Value::Object(map) => {
                let required_names: std::collections::HashSet<String> = map
                    .get("required")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(properties) = map
                    .get_mut("properties")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    let property_names: Vec<String> = properties.keys().cloned().collect();
                    for property_name in &property_names {
                        if let Some(property_schema) = properties.get_mut(property_name) {
                            Self::normalize_schema_for_openai(property_schema);
                            if !required_names.contains(property_name) {
                                Self::make_schema_nullable(property_schema);
                            }
                        }
                    }
                    map.insert(
                        "required".to_string(),
                        serde_json::Value::Array(
                            property_names
                                .into_iter()
                                .map(serde_json::Value::String)
                                .collect(),
                        ),
                    );
                }

                if let Some(items) = map.get_mut("items") {
                    Self::normalize_schema_for_openai(items);
                }

                if let Some(schema) = map.get_mut("schema") {
                    Self::normalize_schema_for_openai(schema);
                }

                if let Some(any_of) = map
                    .get_mut("anyOf")
                    .and_then(serde_json::Value::as_array_mut)
                {
                    for variant in any_of {
                        Self::normalize_schema_for_openai(variant);
                    }
                }

                if let Some(one_of) = map
                    .get_mut("oneOf")
                    .and_then(serde_json::Value::as_array_mut)
                {
                    for variant in one_of {
                        Self::normalize_schema_for_openai(variant);
                    }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    Self::normalize_schema_for_openai(item);
                }
            }
            _ => {}
        }
    }

    fn make_schema_nullable(schema: &mut serde_json::Value) {
        let Some(map) = schema.as_object_mut() else {
            return;
        };
        match map.get_mut("type") {
            Some(serde_json::Value::String(existing)) => {
                if existing != "null" {
                    let original = existing.clone();
                    *schema = serde_json::json!({
                        "anyOf": [
                            { "type": original },
                            { "type": "null" }
                        ]
                    });
                }
            }
            Some(serde_json::Value::Array(items)) => {
                let has_null = items.iter().any(|item| item.as_str() == Some("null"));
                if !has_null {
                    items.push(serde_json::Value::String("null".to_string()));
                }
            }
            _ => {
                if !map.contains_key("anyOf") {
                    let original = schema.clone();
                    *schema = serde_json::json!({
                        "anyOf": [
                            original,
                            { "type": "null" }
                        ]
                    });
                }
            }
        }
    }

    fn complete_via_responses(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_via_responses_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            self.max_output_tokens,
        )
    }

    fn complete_via_responses_with_max_output_tokens(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        let request = self.client.post(format!("{}/responses", self.base_url));

        let body = OpenRouterResponsesRequest {
            model,
            max_output_tokens,
            reasoning: self.reasoning_effort_for_model(model).map(|effort| {
                OpenRouterResponsesReasoningConfig {
                    effort: Some(effort.as_str()),
                }
            }),
            text: OpenRouterResponsesTextConfig {
                format: Self::responses_text_format(schema),
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
        let mut body = serde_json::json!({
            "model": model,
            "max_output_tokens": self.max_output_tokens,
            "text": {
                "format": Self::responses_text_format(schema)
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
        });
        if let Some(effort) = self.reasoning_effort_for_model(model) {
            body["reasoning"] = serde_json::json!({
                "effort": effort.as_str()
            });
        }
        body
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
        serde_json::from_str(&response_text).map_err(|source| {
            ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(&response_text),
            }
        })
    }

    fn reasoning_effort_for_model(&self, model: &str) -> Option<OpenAiReasoningEffort> {
        if model.to_ascii_lowercase().starts_with("gpt-4.1") {
            return None;
        }
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

struct OpenAiArtifactProcessor {
    client: Arc<OpenAiClient>,
    candidate_model: String,
    max_output_tokens: u32,
    repair_max_output_tokens: u32,
}

impl ArtifactProcessor for OpenAiArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        validate_input(input)?;
        let prompt = build_two_phase_candidate_user_prompt(input)?;
        match self.process_once(input, &prompt, self.max_output_tokens) {
            Ok(output) => Ok(output),
            Err(error) if should_retry_with_repair(&error) => {
                let repair_prompt = build_repair_prompt(&prompt, &error);
                self.process_once(input, &repair_prompt, self.repair_max_output_tokens)
            }
            Err(error) => Err(error),
        }
    }
}

impl OpenAiArtifactProcessor {
    fn process_once(
        &self,
        input: &ArtifactProcessorInput,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let candidate_result = self.client.complete_json_with_max_output_tokens(
            &self.candidate_model,
            candidate_system_prompt(input),
            user_prompt,
            &candidate_output_schema_wrapper(input),
            max_output_tokens.max(TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS),
        )?;
        let candidate = parse_candidate_output(&candidate_result.output_text, input)
            .map_err(|err| attach_output_preview(err, &candidate_result.output_text))?;
        Ok(candidate.into_processor_output(
            input,
            self.candidate_model.clone(),
            candidate_result.usage,
            "openai_enrichment",
            "openai",
            OPENAI_PROMPT_VERSION,
        ))
    }
}

struct OpenAiExtractionSubmitter {
    client: Arc<OpenAiClient>,
    candidate_model: String,
}

struct OpenAiReconciliationSubmitter {
    client: Arc<OpenAiClient>,
    model: String,
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
            let user_prompt = build_two_phase_candidate_user_prompt(input)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.batch_custom_id(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.candidate_model,
                    candidate_system_prompt(input),
                    &user_prompt,
                    &candidate_output_schema_wrapper(input),
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
                        "OpenAI batch {} completed without output_file_id (error_file_id={})",
                        handle.batch_id,
                        batch.error_file_id.as_deref().unwrap_or("none")
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
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast OpenAI extraction batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        let candidate_results = parse_openai_output_file(
            &self.client,
            batch.output_file_id.as_deref(),
            inputs,
            |result, input| {
                let candidate = parse_candidate_output(&result.output_text, input)
                    .map_err(|err| attach_output_preview(err, &result.output_text))?;
                Ok((candidate, result.usage))
            },
        );
        if candidate_results.iter().any(Result::is_err) {
            return candidate_results
                .into_iter()
                .map(|result| {
                    result
                        .map(|_| unreachable!("candidate error already checked"))
                        .map_err(|err| ProcessorError::Message {
                            message: err.to_string(),
                        })
                })
                .collect();
        }
        candidate_results
            .into_iter()
            .zip(inputs.iter())
            .map(|(result, input)| match result {
                Ok((candidate, usage)) => Ok(candidate.into_processor_output(
                    input,
                    self.candidate_model.clone(),
                    usage,
                    "openai_enrichment",
                    "openai",
                    OPENAI_PROMPT_VERSION,
                )),
                Err(err) => Err(ProcessorError::Message {
                    message: err.to_string(),
                }),
            })
            .collect()
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
            candidate_model: self.model.clone(),
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
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast OpenAI reconciliation batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        parse_openai_output_file(
            &self.client,
            batch.output_file_id.as_deref(),
            inputs,
            |result, input| {
                let parsed: ModelReconciliationOutput = serde_json::from_str(&result.output_text)
                    .map_err(|source| {
                    ProcessorError::ParseModelJson {
                        source,
                        body_preview: preview(&result.output_text),
                    }
                })?;
                parsed.into_validated_outputs(input)
            },
        )
    }
}

trait OpenAiBatchInput {
    fn batch_custom_id(&self) -> String;
}

impl OpenAiBatchInput for ArtifactProcessorInput {
    fn batch_custom_id(&self) -> String {
        artifact_processor_batch_custom_id(self)
    }
}

impl OpenAiBatchInput for ReconciliationProcessorInput {
    fn batch_custom_id(&self) -> String {
        self.artifact_id.clone()
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
    #[serde(default)]
    error_file_id: Option<String>,
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
) -> Vec<Result<O, ProcessorError>>
where
    I: OpenAiBatchInput,
    F: FnMut(OpenAiBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let Some(output_file_id) = output_file_id else {
        let err = ProcessorError::Message {
            message: "OpenAI batch missing output_file_id".to_string(),
        };
        return inputs
            .iter()
            .map(|_| Err(message_processor_error(&err)))
            .collect();
    };
    let content = match client.read_file_text(output_file_id) {
        Ok(content) => content,
        Err(err) => {
            return inputs
                .iter()
                .map(|_| Err(message_processor_error(&err)))
                .collect()
        }
    };
    let mut parsed_by_id = HashMap::new();
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let item: OpenAiBatchResultLine = match serde_json::from_str(line) {
            Ok(item) => item,
            Err(source) => {
                let err = ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(line),
                };
                return inputs
                    .iter()
                    .map(|_| Err(message_processor_error(&err)))
                    .collect();
            }
        };
        let custom_id = item.custom_id.clone();
        let item_result = match (item.response, item.error) {
            (Some(response), _) if response.status_code / 100 == 2 => {
                let usage = response
                    .body
                    .usage
                    .clone()
                    .and_then(InferenceUsage::from_openrouter_usage);
                Ok(OpenAiBatchParsedResult {
                    output_text: response.body.flatten_text(),
                    usage,
                })
            }
            (Some(response), _) => Err(ProcessorError::Message {
                message: format!(
                    "OpenAI batch item {} failed with status {}",
                    custom_id, response.status_code
                ),
            }),
            (_, Some(error)) => Err(ProcessorError::Message {
                message: format!("OpenAI batch item {} failed: {}", custom_id, error),
            }),
            _ => Err(ProcessorError::Message {
                message: format!("OpenAI batch item {} returned no response", custom_id),
            }),
        };
        parsed_by_id.insert(custom_id, item_result);
    }

    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let custom_id = input.batch_custom_id();
        match parsed_by_id.remove(&custom_id) {
            Some(Ok(result)) => outputs.push(parse(result, input)),
            Some(Err(err)) => outputs.push(Err(err)),
            None => outputs.push(Err(ProcessorError::Message {
                message: format!("OpenAI batch missing result for {}", custom_id),
            })),
        }
    }
    outputs
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
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<OpenRouterResponsesReasoningConfig<'a>>,
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ModelCandidateArtifactOutput {
    summary_draft: ModelSummary,
    classification_candidates: Vec<ModelClassification>,
    memory_candidates: Vec<ModelCandidateMemory>,
    #[serde(default)]
    entity_candidates: Vec<ModelEntity>,
    #[serde(default)]
    relationship_candidates: Vec<ModelRelationship>,
    #[serde(default)]
    retrieval_candidates: Vec<ModelRetrievalIntent>,
    importance_score: u8,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelSummary {
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelClassification {
    classification_type: String,
    classification_value: String,
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelCandidateMemory {
    title: String,
    body_text: String,
    evidence_segment_ids: Vec<String>,
    durability_label: String,
    retrieval_value_label: String,
    consequentiality_label: String,
    temporal_scope: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelEntity {
    entity_key: String,
    display_name: String,
    entity_type: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelRelationship {
    relationship_type: String,
    subject_key: String,
    object_key: String,
    title: String,
    body_text: String,
    confidence_label: String,
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[cfg(any())]
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
            memory.memory_type =
                canonicalize_memory_type(&memory.memory_type, &memory.title, &memory.body_text);
            for segment_id in &mut memory.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for entity in &mut self.entities {
            entity.entity_type = canonicalize_entity_type(&entity.entity_type);
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

    fn validate_and_salvage(
        mut self,
        input: &ArtifactProcessorInput,
    ) -> Result<Self, ProcessorError> {
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

        if self.classifications.len() > MAX_CLASSIFICATIONS {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} classifications; expected at most {}",
                    self.classifications.len(),
                    MAX_CLASSIFICATIONS
                ),
            });
        }

        self.classifications = retain_valid_items(self.classifications, |index, classification| {
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
            )
        });

        if self.memories.len() > MAX_MEMORIES {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} memories; expected at most {}",
                    self.memories.len(),
                    MAX_MEMORIES
                ),
            });
        }

        self.memories = retain_valid_items(self.memories, |index, memory| {
            validate_text_field(&format!("memories[{index}].title"), &memory.title)?;
            validate_text_field(&format!("memories[{index}].body_text"), &memory.body_text)?;
            if !MEMORY_TYPE_VALUES.contains(&memory.memory_type.as_str()) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memories[{index}].memory_type {:?} is not allowed",
                        memory.memory_type
                    ),
                });
            }
            if memory.memory_scope_value.trim().is_empty() {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!("memories[{index}].memory_scope_value must not be empty"),
                });
            }
            if memory.memory_scope == ScopeType::Artifact
                && memory.memory_scope_value != input.artifact_id
            {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memories[{index}].memory_scope_value must equal artifact_id {:?}, got {:?}",
                        input.artifact_id, memory.memory_scope_value
                    ),
                });
            }
            validate_evidence_ids(
                &format!("memories[{index}].evidence_segment_ids"),
                &memory.evidence_segment_ids,
                &valid_segment_ids,
            )
        });

        self.entities = retain_valid_items(self.entities, |index, entity| {
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
            )
        });

        self.relationships = retain_valid_items(self.relationships, |index, relationship| {
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
            )
        });

        if self.retrieval_intents.len() > MAX_RETRIEVAL_INTENTS {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "model returned {} retrieval intents; expected at most {}",
                    self.retrieval_intents.len(),
                    MAX_RETRIEVAL_INTENTS
                ),
            });
        }

        self.retrieval_intents = retain_valid_items(self.retrieval_intents, |index, intent| {
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
            )
        });

        if !(1..=10).contains(&self.importance_score) {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "importance_score {} must be between 1 and 10",
                    self.importance_score
                ),
            });
        }

        Ok(self)
    }

    fn into_processor_output(
        self,
        model_name: String,
        usage: Option<InferenceUsage>,
        pipeline_name: &str,
        provider_name: &str,
        prompt_version: &str,
    ) -> ArtifactProcessorOutput {
        cleanup_artifact_processor_output(ArtifactProcessorOutput {
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
                .map(|memory| {
                    let title = normalize_optional_text(memory.title);
                    let body_text = memory.body_text.trim().to_string();
                    let memory_type = canonicalize_memory_type(
                        &memory.memory_type,
                        title.as_deref().unwrap_or_default(),
                        &body_text,
                    );
                    let candidate_key = memory_candidate_key_from_fields(
                        &memory_type,
                        memory.memory_scope,
                        &memory.memory_scope_value,
                        title.as_deref(),
                        &body_text,
                    );
                    MemoryOutput {
                        candidate_key,
                        title,
                        body_text,
                        memory_type,
                        memory_scope: memory.memory_scope,
                        memory_scope_value: memory.memory_scope_value,
                        evidence_segment_ids: memory.evidence_segment_ids,
                    }
                })
                .collect(),
            entities: self
                .entities
                .into_iter()
                .map(|entity| EntityOutput {
                    entity_key: entity.entity_key,
                    display_name: entity.display_name,
                    entity_type: canonicalize_entity_type(&entity.entity_type),
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
        })
    }
}

impl ModelCandidateArtifactOutput {
    fn resolve_evidence_aliases(mut self, input: &ArtifactProcessorInput) -> Self {
        let alias_map = build_segment_alias_map(input);

        for segment_id in &mut self.summary_draft.evidence_segment_ids {
            if let Some(actual) = alias_map.get(segment_id.as_str()) {
                *segment_id = actual.clone();
            }
        }

        for classification in &mut self.classification_candidates {
            for segment_id in &mut classification.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for memory in &mut self.memory_candidates {
            for segment_id in &mut memory.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for entity in &mut self.entity_candidates {
            entity.entity_type = canonicalize_entity_type(&entity.entity_type);
            for segment_id in &mut entity.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for relationship in &mut self.relationship_candidates {
            for segment_id in &mut relationship.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        for intent in &mut self.retrieval_candidates {
            for segment_id in &mut intent.evidence_segment_ids {
                if let Some(actual) = alias_map.get(segment_id.as_str()) {
                    *segment_id = actual.clone();
                }
            }
        }

        self
    }

    fn into_processor_output(
        self,
        input: &ArtifactProcessorInput,
        model_name: String,
        usage: Option<InferenceUsage>,
        pipeline_name: &str,
        provider_name: &str,
        prompt_version: &str,
    ) -> ArtifactProcessorOutput {
        let resolved = self.resolve_evidence_aliases(input);
        cleanup_artifact_processor_output(ArtifactProcessorOutput {
            pipeline_name: pipeline_name.to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some(provider_name.to_string()),
            model_name: Some(model_name),
            prompt_version: Some(prompt_version.to_string()),
            usage,
            summary: SummaryOutput {
                title: normalize_optional_text(resolved.summary_draft.title),
                body_text: resolved.summary_draft.body_text.trim().to_string(),
                evidence_segment_ids: resolved.summary_draft.evidence_segment_ids,
            },
            classifications: resolved
                .classification_candidates
                .into_iter()
                .map(|classification| ClassificationOutput {
                    title: normalize_optional_text(classification.title),
                    body_text: normalize_optional_text(classification.body_text),
                    classification_type: classification.classification_type,
                    classification_value: classification.classification_value,
                    evidence_segment_ids: classification.evidence_segment_ids,
                })
                .collect(),
            memories: resolved
                .memory_candidates
                .into_iter()
                .map(|memory| {
                    let title = normalize_optional_text(memory.title);
                    let body_text = memory.body_text.trim().to_string();
                    let memory_type = canonicalize_memory_type(
                        "",
                        title.as_deref().unwrap_or_default(),
                        &body_text,
                    );
                    let candidate_key = memory_candidate_key_from_fields(
                        &memory_type,
                        ScopeType::Artifact,
                        &input.artifact_id,
                        title.as_deref(),
                        &body_text,
                    );
                    MemoryOutput {
                        candidate_key,
                        title,
                        body_text,
                        memory_type,
                        memory_scope: ScopeType::Artifact,
                        memory_scope_value: input.artifact_id.clone(),
                        evidence_segment_ids: memory.evidence_segment_ids,
                    }
                })
                .collect(),
            entities: resolved
                .entity_candidates
                .into_iter()
                .map(|entity| EntityOutput {
                    entity_key: entity.entity_key.trim().to_string(),
                    display_name: entity.display_name.trim().to_string(),
                    entity_type: canonicalize_entity_type(&entity.entity_type),
                    evidence_segment_ids: entity.evidence_segment_ids,
                })
                .collect(),
            relationships: resolved
                .relationship_candidates
                .into_iter()
                .map(|relationship| RelationshipOutput {
                    relationship_type: relationship.relationship_type.trim().to_string(),
                    subject_key: relationship.subject_key.trim().to_string(),
                    object_key: relationship.object_key.trim().to_string(),
                    title: normalize_optional_text(relationship.title),
                    body_text: relationship.body_text.trim().to_string(),
                    confidence_label: relationship.confidence_label.trim().to_string(),
                    evidence_segment_ids: relationship.evidence_segment_ids,
                })
                .collect(),
            retrieval_intents: resolved
                .retrieval_candidates
                .into_iter()
                .enumerate()
                .map(|(index, intent)| RetrievalIntent {
                    intent_id: format!("intent-{}", index + 1),
                    question: intent.question.trim().to_string(),
                    query_text: intent.query_text.trim().to_string(),
                    intent_type: intent.intent_type.trim().to_string(),
                    evidence_segment_ids: intent.evidence_segment_ids,
                })
                .collect(),
            importance_score: resolved.importance_score,
        })
    }

    fn validate(mut self, input: &ArtifactProcessorInput) -> Result<Self, ProcessorError> {
        let valid_segment_ids_owned = allowed_artifact_evidence_refs(input);
        let valid_segment_ids: HashSet<&str> =
            valid_segment_ids_owned.iter().map(String::as_str).collect();

        validate_text_field("summary_draft.title", &self.summary_draft.title)?;
        validate_text_field("summary_draft.body_text", &self.summary_draft.body_text)?;
        validate_evidence_ids(
            "summary_draft.evidence_segment_ids",
            &self.summary_draft.evidence_segment_ids,
            &valid_segment_ids,
        )?;

        self.classification_candidates = retain_valid_items(
            self.classification_candidates,
            |index, classification| {
                validate_text_field(
                    &format!("classification_candidates[{index}].title"),
                    &classification.title,
                )?;
                validate_text_field(
                    &format!("classification_candidates[{index}].body_text"),
                    &classification.body_text,
                )?;
                validate_text_field(
                    &format!("classification_candidates[{index}].classification_value"),
                    &classification.classification_value,
                )?;
                match classification.classification_type.as_str() {
                    "topic" | "intent" => {}
                    other => {
                        return Err(ProcessorError::InvalidModelOutput {
                            detail: format!(
                                "classification_candidates[{index}].classification_type {other:?} is not allowed"
                            ),
                        })
                    }
                }
                validate_evidence_ids(
                    &format!("classification_candidates[{index}].evidence_segment_ids"),
                    &classification.evidence_segment_ids,
                    &valid_segment_ids,
                )
            },
        );

        self.memory_candidates = retain_valid_items(self.memory_candidates, |index, memory| {
            validate_text_field(&format!("memory_candidates[{index}].title"), &memory.title)?;
            validate_text_field(
                &format!("memory_candidates[{index}].body_text"),
                &memory.body_text,
            )?;
            if !matches!(memory.durability_label.as_str(), "low" | "medium" | "high") {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].durability_label {:?} is not allowed",
                        memory.durability_label
                    ),
                });
            }
            if !matches!(
                memory.retrieval_value_label.as_str(),
                "low" | "medium" | "high"
            ) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].retrieval_value_label {:?} is not allowed",
                        memory.retrieval_value_label
                    ),
                });
            }
            if !matches!(
                memory.consequentiality_label.as_str(),
                "low" | "medium" | "high"
            ) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].consequentiality_label {:?} is not allowed",
                        memory.consequentiality_label
                    ),
                });
            }
            if !matches!(
                memory.temporal_scope.as_str(),
                "ephemeral" | "time_bound" | "ongoing" | "enduring"
            ) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "memory_candidates[{index}].temporal_scope {:?} is not allowed",
                        memory.temporal_scope
                    ),
                });
            }
            validate_evidence_ids(
                &format!("memory_candidates[{index}].evidence_segment_ids"),
                &memory.evidence_segment_ids,
                &valid_segment_ids,
            )
        });

        self.entity_candidates = retain_valid_items(self.entity_candidates, |index, entity| {
            validate_text_field(
                &format!("entity_candidates[{index}].entity_key"),
                &entity.entity_key,
            )?;
            validate_text_field(
                &format!("entity_candidates[{index}].display_name"),
                &entity.display_name,
            )?;
            validate_text_field(
                &format!("entity_candidates[{index}].entity_type"),
                &entity.entity_type,
            )?;
            validate_evidence_ids(
                &format!("entity_candidates[{index}].evidence_segment_ids"),
                &entity.evidence_segment_ids,
                &valid_segment_ids,
            )
        });

        self.relationship_candidates =
            retain_valid_items(self.relationship_candidates, |index, relationship| {
                validate_text_field(
                    &format!("relationship_candidates[{index}].relationship_type"),
                    &relationship.relationship_type,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].subject_key"),
                    &relationship.subject_key,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].object_key"),
                    &relationship.object_key,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].title"),
                    &relationship.title,
                )?;
                validate_text_field(
                    &format!("relationship_candidates[{index}].body_text"),
                    &relationship.body_text,
                )?;
                if !matches!(
                    relationship.confidence_label.as_str(),
                    "low" | "medium" | "high"
                ) {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "relationship_candidates[{index}].confidence_label {:?} is not allowed",
                            relationship.confidence_label
                        ),
                    });
                }
                validate_evidence_ids(
                    &format!("relationship_candidates[{index}].evidence_segment_ids"),
                    &relationship.evidence_segment_ids,
                    &valid_segment_ids,
                )
            });

        self.retrieval_candidates =
            retain_valid_items(self.retrieval_candidates, |index, intent| {
                validate_text_field(
                    &format!("retrieval_candidates[{index}].question"),
                    &intent.question,
                )?;
                validate_text_field(
                    &format!("retrieval_candidates[{index}].query_text"),
                    &intent.query_text,
                )?;
                if !matches!(
                    intent.intent_type.as_str(),
                    "topic_lookup"
                        | "memory_match"
                        | "entity_lookup"
                        | "relationship_lookup"
                        | "contradiction_check"
                ) {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "retrieval_candidates[{index}].intent_type {:?} is not allowed",
                            intent.intent_type
                        ),
                    });
                }
                validate_evidence_ids(
                    &format!("retrieval_candidates[{index}].evidence_segment_ids"),
                    &intent.evidence_segment_ids,
                    &valid_segment_ids,
                )
            });

        if !(1..=10).contains(&self.importance_score) {
            return Err(ProcessorError::InvalidModelOutput {
                detail: format!(
                    "importance_score {} must be between 1 and 10",
                    self.importance_score
                ),
            });
        }

        Ok(self)
    }
}

pub(crate) fn cleanup_artifact_processor_output(
    mut output: ArtifactProcessorOutput,
) -> ArtifactProcessorOutput {
    output.memories = dedupe_memory_outputs(output.memories);
    output.entities = dedupe_entity_outputs(output.entities);
    output.relationships = sanitize_relationship_outputs(output.relationships, &output.entities);
    output.retrieval_intents = dedupe_retrieval_intents(output.retrieval_intents);
    output
}

fn dedupe_memory_outputs(memories: Vec<MemoryOutput>) -> Vec<MemoryOutput> {
    let mut deduped = Vec::<MemoryOutput>::new();
    for memory in memories {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_memory_outputs(existing, &memory))
        {
            merge_memory_outputs(existing, &memory);
        } else {
            deduped.push(memory);
        }
    }
    deduped
}

fn dedupe_entity_outputs(entities: Vec<EntityOutput>) -> Vec<EntityOutput> {
    let mut deduped = Vec::<EntityOutput>::new();
    for entity in entities {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_entity_outputs(existing, &entity))
        {
            merge_entity_outputs(existing, &entity);
        } else {
            deduped.push(entity);
        }
    }
    deduped
}

fn sanitize_relationship_outputs(
    relationships: Vec<RelationshipOutput>,
    entities: &[EntityOutput],
) -> Vec<RelationshipOutput> {
    let entity_keys: HashSet<&str> = entities
        .iter()
        .map(|entity| entity.entity_key.as_str())
        .collect();
    let mut deduped = Vec::<RelationshipOutput>::new();
    for relationship in relationships {
        if relationship.subject_key == relationship.object_key {
            continue;
        }
        if !entity_keys.contains(relationship.subject_key.as_str())
            || !entity_keys.contains(relationship.object_key.as_str())
        {
            continue;
        }
        if !matches!(
            relationship.confidence_label.as_str(),
            "low" | "medium" | "high"
        ) {
            continue;
        }
        if let Some(existing) = deduped.iter_mut().find(|existing| {
            existing.relationship_type == relationship.relationship_type
                && existing.subject_key == relationship.subject_key
                && existing.object_key == relationship.object_key
        }) {
            merge_relationship_outputs(existing, &relationship);
        } else {
            deduped.push(relationship);
        }
    }
    deduped
}

fn dedupe_retrieval_intents(intents: Vec<RetrievalIntent>) -> Vec<RetrievalIntent> {
    let mut deduped = Vec::<RetrievalIntent>::new();
    for intent in intents {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| should_merge_retrieval_intents(existing, &intent))
        {
            merge_retrieval_intents(existing, &intent);
        } else {
            deduped.push(intent);
        }
    }
    deduped
}

fn should_merge_memory_outputs(left: &MemoryOutput, right: &MemoryOutput) -> bool {
    if left.candidate_key == right.candidate_key {
        return true;
    }
    if left.memory_type != right.memory_type
        || left.memory_scope != right.memory_scope
        || left.memory_scope_value != right.memory_scope_value
    {
        return false;
    }

    let left_title = normalize_merge_text(left.title.as_deref().unwrap_or_default());
    let right_title = normalize_merge_text(right.title.as_deref().unwrap_or_default());
    if !left_title.is_empty() && left_title == right_title {
        return text_overlap_score(&left.body_text, &right.body_text) >= 0.82;
    }

    text_overlap_score(&left.body_text, &right.body_text) >= 0.9
}

fn should_merge_entity_outputs(left: &EntityOutput, right: &EntityOutput) -> bool {
    if normalize_merge_text(&left.entity_key) == normalize_merge_text(&right.entity_key) {
        return true;
    }
    normalize_merge_text(&left.display_name) == normalize_merge_text(&right.display_name)
}

fn should_merge_retrieval_intents(left: &RetrievalIntent, right: &RetrievalIntent) -> bool {
    if left.intent_type != right.intent_type {
        return false;
    }
    let left_query = normalize_merge_text(&left.query_text);
    let right_query = normalize_merge_text(&right.query_text);
    let left_question = normalize_merge_text(&left.question);
    let right_question = normalize_merge_text(&right.question);
    left_query == right_query
        || (!left_question.is_empty() && left_question == right_question)
        || text_overlap_score(&left.query_text, &right.query_text) >= 0.88
}

fn merge_memory_outputs(target: &mut MemoryOutput, incoming: &MemoryOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_entity_outputs(target: &mut EntityOutput, incoming: &EntityOutput) {
    if incoming.display_name.len() > target.display_name.len() {
        target.display_name = incoming.display_name.clone();
    }
    if incoming.entity_type.len() > target.entity_type.len() {
        target.entity_type = incoming.entity_type.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_relationship_outputs(target: &mut RelationshipOutput, incoming: &RelationshipOutput) {
    target.title = prefer_richer_optional_text(target.title.take(), incoming.title.clone());
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    if confidence_rank(&incoming.confidence_label) > confidence_rank(&target.confidence_label) {
        target.confidence_label = incoming.confidence_label.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_retrieval_intents(target: &mut RetrievalIntent, incoming: &RetrievalIntent) {
    if incoming.question.len() > target.question.len() {
        target.question = incoming.question.clone();
    }
    if incoming.query_text.len() > target.query_text.len() {
        target.query_text = incoming.query_text.clone();
    }
    extend_unique_strings(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn extend_unique_strings(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn normalize_merge_text(value: &str) -> String {
    normalize_candidate_key_text(value)
        .split_whitespace()
        .map(singularize_merge_token)
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn singularize_merge_token(token: &str) -> String {
    if token.len() > 4 && token.ends_with("ies") {
        format!("{}y", &token[..token.len() - 3])
    } else if token.len() > 4 && token.ends_with('s') && !token.ends_with("ss") {
        token[..token.len() - 1].to_string()
    } else {
        token.to_string()
    }
}

fn text_overlap_score(left: &str, right: &str) -> f32 {
    let left = normalize_merge_text(left);
    let right = normalize_merge_text(right);
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    if left == right || left.contains(&right) || right.contains(&left) {
        return 1.0;
    }

    let left_tokens: HashSet<&str> = left.split_whitespace().collect();
    let right_tokens: HashSet<&str> = right.split_whitespace().collect();
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }
    let overlap = left_tokens.intersection(&right_tokens).count() as f32;
    let smaller = left_tokens.len().min(right_tokens.len()) as f32;
    overlap / smaller
}

fn canonicalize_memory_type(raw: &str, title: &str, body_text: &str) -> String {
    let raw = normalize_candidate_key_text(raw);
    let title = normalize_candidate_key_text(title);
    let body = normalize_candidate_key_text(body_text);
    let combined = format!("{title} {body}");

    if matches!(
        raw.as_str(),
        "personal fact"
            | "personal_fact"
            | "identity fact"
            | "identity_fact"
            | "fact"
            | "experience"
            | "biographical fact"
            | "health fact"
    ) {
        return "personal_fact".to_string();
    }
    if matches!(raw.as_str(), "preference" | "preferences") {
        return "preference".to_string();
    }
    if matches!(
        raw.as_str(),
        "ongoing task"
            | "ongoing_task"
            | "ongoing state"
            | "ongoing_state"
            | "current state"
            | "task"
    ) {
        return "ongoing_state".to_string();
    }
    if matches!(raw.as_str(), "project fact" | "project_fact" | "decision") {
        return "project_fact".to_string();
    }
    if matches!(
        raw.as_str(),
        "reference" | "reference material" | "reference_material" | "skill"
    ) {
        return "reference".to_string();
    }

    if combined.contains("user weigh")
        || combined.contains("body weight")
        || combined.contains("weight history")
        || combined.contains("user age")
        || combined.contains("cholesterol")
        || combined.contains("ldl")
        || combined.contains("apob")
        || combined.contains("lp a")
        || combined.contains("egfr")
        || combined.contains("tbi")
        || combined.contains("concussion")
        || combined.contains("brain hemorrhage")
        || combined.contains("hemorrhage")
        || combined.contains("lung nodule")
        || combined.contains("smoking history")
        || combined.contains("pack year")
        || combined.contains("fracture")
        || combined.contains("broken back")
        || combined.contains("broke my back")
        || combined.contains("injur")
        || combined.contains("elite cyclist")
        || combined.contains("cycling history")
        || combined.contains("racing year")
        || combined.contains("university of")
        || combined.contains("lab result")
    {
        return "personal_fact".to_string();
    }

    if combined.contains("prefer")
        || combined.contains("avoid")
        || combined.contains("likes ")
        || combined.contains("dislike")
        || combined.contains("favorite")
        || combined.contains("prefers ")
    {
        return "preference".to_string();
    }

    if combined.contains("protocol")
        || combined.contains("plan")
        || combined.contains("schedule")
        || combined.contains("rotation")
        || combined.contains("transition")
        || combined.contains("trial")
        || combined.contains("current ")
        || combined.contains("currently ")
        || combined.contains("taking ")
        || combined.contains("supply")
        || combined.contains("dose")
        || combined.contains("dosing")
        || combined.contains("arriv")
        || combined.contains("week")
        || combined.contains("daily ")
    {
        return "ongoing_state".to_string();
    }

    if combined.contains("program")
        || combined.contains("system")
        || combined.contains("workflow")
        || combined.contains("architecture")
        || combined.contains("clean engine")
        || combined.contains("project")
        || combined.contains("decision")
        || combined.contains("should ")
        || combined.contains("must ")
    {
        return "project_fact".to_string();
    }

    if combined.contains("reference")
        || combined.contains("cheatsheet")
        || combined.contains("cheat sheet")
        || combined.contains("script")
        || combined.contains("template")
        || combined.contains("guide")
    {
        return "reference".to_string();
    }

    match raw.as_str() {
        "" => "personal_fact".to_string(),
        other if other.contains("preference") => "preference".to_string(),
        other if other.contains("personal") || other.contains("identity") => {
            "personal_fact".to_string()
        }
        other if other.contains("project") || other.contains("decision") => {
            "project_fact".to_string()
        }
        other if other.contains("ongoing") || other.contains("state") || other.contains("task") => {
            "ongoing_state".to_string()
        }
        other if other.contains("reference") => "reference".to_string(),
        _ => "personal_fact".to_string(),
    }
}

fn canonicalize_entity_type(raw: &str) -> String {
    let raw = normalize_candidate_key_text(raw);
    match raw.as_str() {
        "brand" | "organization brand" => "brand".to_string(),
        "organization" | "company" | "retailer" | "store" => "organization".to_string(),
        "food product" | "food product or source" | "food source" | "product or ingredient" => {
            "food_product".to_string()
        }
        "supplement form" | "supplement" => "supplement_form".to_string(),
        "laboratory marker" | "biomarker" => "biomarker".to_string(),
        "exercise program" | "training type" | "exercise type" => "training_type".to_string(),
        "medication" | "medication or brand name mention" => "medication".to_string(),
        "quality standard" => "quality_standard".to_string(),
        "" => "entity".to_string(),
        _ => raw.replace(' ', "_"),
    }
}

fn prefer_richer_optional_text(
    current: Option<String>,
    incoming: Option<String>,
) -> Option<String> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => {
            if incoming.len() > current.len() {
                Some(incoming)
            } else {
                Some(current)
            }
        }
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    }
}

fn confidence_rank(label: &str) -> i32 {
    match label {
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

impl ModelReconciliationOutput {
    fn normalize_ungrounded_existing_decisions(mut self) -> Self {
        for decision in &mut self.decisions {
            if requires_matched_object_id(decision.decision_kind)
                && decision
                    .matched_object_id
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                // Reconciliation is only allowed to merge when it can point at a
                // concrete existing object. Otherwise preserve the extracted
                // candidate by treating it as create_new.
                decision.decision_kind = ReconciliationDecisionKind::CreateNew;
                decision.matched_object_id = None;
            }
        }
        self
    }

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
            .map(|memory| memory.candidate_key.clone())
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
            if requires_matched_object_id(decision.decision_kind)
                && decision
                    .matched_object_id
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].matched_object_id is required for {:?}",
                        decision.decision_kind
                    ),
                });
            }
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

    fn into_validated_outputs(
        self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        let normalized = self.normalize_ungrounded_existing_decisions();
        normalized.validate_against(input)?;
        Ok(normalized.into_outputs())
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

fn requires_matched_object_id(kind: ReconciliationDecisionKind) -> bool {
    matches!(
        kind,
        ReconciliationDecisionKind::AttachToExisting
            | ReconciliationDecisionKind::StrengthenExisting
            | ReconciliationDecisionKind::SupersedeExisting
            | ReconciliationDecisionKind::ContradictsExisting
    )
}

fn validate_input(input: &ArtifactProcessorInput) -> Result<(), ProcessorError> {
    if input.segments.is_empty() {
        return Err(ProcessorError::InvalidInput {
            detail: format!("artifact {} has no segments to enrich", input.artifact_id),
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

fn retain_valid_items<T>(
    items: Vec<T>,
    mut validate: impl FnMut(usize, &T) -> Result<(), ProcessorError>,
) -> Vec<T> {
    items
        .into_iter()
        .enumerate()
        .filter_map(|(index, item)| validate(index, &item).ok().map(|_| item))
        .collect()
}

fn normalize_optional_text(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_candidate_key_text(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_was_space = true;

    for ch in value.chars().flat_map(char::to_lowercase) {
        let ch = if ch.is_ascii_alphanumeric() || ch.is_whitespace() {
            ch
        } else {
            ' '
        };
        if ch.is_whitespace() {
            if !previous_was_space {
                normalized.push(' ');
                previous_was_space = true;
            }
        } else {
            normalized.push(ch);
            previous_was_space = false;
        }
    }

    normalized.trim().to_string()
}

fn hex_prefix(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub fn memory_candidate_key_from_fields(
    memory_type: &str,
    memory_scope: ScopeType,
    memory_scope_value: &str,
    title: Option<&str>,
    body_text: &str,
) -> String {
    let canonical = format!(
        "{}|{}|{}|{}|{}",
        normalize_candidate_key_text(memory_type),
        memory_scope.as_str(),
        normalize_candidate_key_text(memory_scope_value),
        normalize_candidate_key_text(title.unwrap_or_default()),
        normalize_candidate_key_text(body_text),
    );
    let digest = Sha256::digest(canonical.as_bytes());
    format!("mem:{}", hex_prefix(&digest[..16]))
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
         artifact_class: conversation\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         \n\
         Output guidance:\n\
         - summary: 2-4 compact sentences covering the main topics discussed\n\
         - summary is not a substitute for memories; memories capture discrete durable facts for retrieval, while the summary is a narrative synthesis\n\
         - do not mirror the summary into memories or the memories into the summary mechanically\n\
         - if a durable fact could answer a later retrieval question on its own, emit it as a memory whether or not the summary also mentions it\n\
         - classifications: 1-3 covering the major domains of the conversation\n\
         - classifications are broad topical labels only; do not use them instead of memories for durable facts\n\
         - memories: extract every durable personal fact, preference, project fact, ongoing state, or reusable reference that could be independently retrieved later. A long or rich conversation should produce many memories, not a summary of a few. Categories to look for: preferences, history, decisions made, conclusions reached, ongoing state, goals, constraints, health and medical context, biographical facts, and named workflows or systems.\n\
         - choose the closest memory_type from the schema; use the broad available categories and do not invent narrower domain-specific subtypes\n\
         - biographical and health history belongs in durable memories, not only in summary prose\n\
         - entities: include all explicit named people, projects, systems, organizations, and domain-specific proper nouns with durable retrieval value\n\
         - relationships: include explicit supported links between emitted entities or durable facts\n\
         - retrieval_intents: ask archive-only follow-up questions when duplicate detection, prior-state matching, or contradiction checks matter\n\
         - importance: be conservative\n\
         - evidence_segment_ids must use only the evidence_ref values shown in segments\n\
         - allowed evidence_ref values for this artifact: {allowed_refs_json}\n\
         - copy evidence_ref values exactly as shown, for example evidence_ref_1 and evidence_ref_2\n\
         - never invent, abbreviate, transform, or combine evidence refs\n\
         - cite every segment that directly supports each derived object, not just the primary one\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        segments_json = segments_json,
         allowed_refs_json = serde_json::to_string(&allowed_artifact_evidence_refs(input))
            .map_err(|source| ProcessorError::SerializePrompt { source })?,
    ))
}

pub(crate) fn build_document_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        text: &'a str,
    }

    let prompt_segments: Vec<_> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: segment_alias(index),
            text: segment.text_content.as_str(),
        })
        .collect();

    let segments_json = serde_json::to_string_pretty(&prompt_segments)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;

    Ok(format!(
        "Input artifact:\n\
         artifact_class: document\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         \n\
         Output guidance:\n\
         - summarize the document, not a dialogue; capture its purpose, key claims, decisions, action items, and durable references\n\
         - first infer what kind of document this is from the text itself, such as meeting notes, call notes, work log, research notes, design notes, journal entry, or general reference material, and adapt extraction accordingly\n\
         - summary is not a substitute for memories; memories should stay atomic and independently retrievable later\n\
         - classifications should capture document form and durable topical lenses, not one-off details\n\
         - memories: extract every durable fact, decision, commitment, requirement, constraint, ongoing state, action item owner, reference detail, or background context that could matter in future retrieval\n\
         - if the document appears to be meeting notes or a call log, treat attendees, speakers, owners, discussed systems, and recorded decisions as likely entity and relationship candidates when explicitly supported\n\
         - entities: include explicit named people, teams, organizations, projects, systems, repositories, vendors, and domain-specific proper nouns with likely future retrieval value\n\
         - relationships: include explicit or strongly supported links such as ownership, participation, dependency, decision impact, coordination, or affiliation between emitted entities\n\
         - retrieval_intents: ask archive-only follow-up questions that would help connect this document to prior decisions, prior states, duplicate notes, or related entities\n\
         - importance: be conservative, but documents with durable project context, commitments, or personal history can still be high value\n\
         - evidence_segment_ids must use only the evidence_ref values shown in segments\n\
         - allowed evidence_ref values for this artifact: {allowed_refs_json}\n\
         - copy evidence_ref values exactly as shown, for example evidence_ref_1 and evidence_ref_2\n\
         - never invent, abbreviate, transform, or combine evidence refs\n\
         - cite every segment that directly supports each derived object, not just the primary one\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        segments_json = segments_json,
        allowed_refs_json = serde_json::to_string(&allowed_artifact_evidence_refs(input))
            .map_err(|source| ProcessorError::SerializePrompt { source })?,
    ))
}

pub(crate) fn build_candidate_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    match input.artifact_class {
        ArtifactClass::Conversation => build_conversation_user_prompt(input),
        ArtifactClass::Document => build_document_user_prompt(input),
    }
}

pub(crate) fn should_shape_artifact_input(input: &ArtifactProcessorInput) -> bool {
    if input.artifact_class == ArtifactClass::Document {
        return false;
    }

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
            target_key: memory.candidate_key.clone(),
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
         target_kind must be memory or relationship.\n\
         If decision_kind is attach_to_existing, strengthen_existing, supersede_existing, or contradicts_existing,\n\
         matched_object_id must be copied exactly from a retrieved object.\n\
         If you cannot name an exact matched_object_id, choose create_new instead.\n",
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

pub(crate) fn allowed_artifact_evidence_refs(input: &ArtifactProcessorInput) -> Vec<String> {
    input
        .segments
        .iter()
        .enumerate()
        .map(|(index, _)| segment_alias(index))
        .collect()
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

fn segment_alias(index: usize) -> String {
    format!("evidence_ref_{}", index + 1)
}

fn attach_output_preview(err: ProcessorError, output_text: &str) -> ProcessorError {
    match err {
        ProcessorError::InvalidModelOutput { detail } => ProcessorError::InvalidModelOutput {
            detail: format!("{detail}; output preview: {}", preview(output_text)),
        },
        other => other,
    }
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

pub(crate) const TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS: u32 = 7000;
#[allow(dead_code)]
pub(crate) const TWO_PHASE_FINALIZER_MAX_OUTPUT_TOKENS: u32 = 4000;

pub(crate) fn candidate_system_prompt(input: &ArtifactProcessorInput) -> &'static str {
    match input.artifact_class {
        ArtifactClass::Conversation => CONVERSATION_CANDIDATE_SYSTEM_PROMPT,
        ArtifactClass::Document => DOCUMENT_CANDIDATE_SYSTEM_PROMPT,
    }
}

pub(crate) fn build_two_phase_candidate_user_prompt(
    input: &ArtifactProcessorInput,
) -> Result<String, ProcessorError> {
    build_candidate_user_prompt(input)
}

#[allow(dead_code)]
pub(crate) fn build_two_phase_finalizer_prompt(
    artifact_id: &str,
    candidate: &ModelCandidateArtifactOutput,
) -> Result<String, ProcessorError> {
    let candidate_json = serde_json::to_string_pretty(candidate)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;
    Ok(format!(
        "Finalize this extraction candidate bundle into the target schema.\n\
         artifact_id: {artifact_id}\n\
         \n\
         Candidate bundle:\n\
         {candidate_json}\n\
         \n\
         Rules:\n\
         - Use only facts and evidence refs present in the candidate bundle.\n\
         - Do not invent new memories, entities, relationships, classifications, or retrieval intents.\n\
         - Merge overlapping candidates and drop non-durable or redundant items.\n\
         - Prefer specific, retrieval-worthy memories over broad umbrella memories.\n\
         - Choose memory_type only from: personal_fact, preference, project_fact, ongoing_state, reference.\n\
         - Preserve evidence refs exactly as provided.\n\
         - Fix mistyped or weak candidates where possible.\n\
         - Output valid JSON only.\n"
    ))
}

pub(crate) fn candidate_output_schema_wrapper(input: &ArtifactProcessorInput) -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_artifact_candidate_extraction",
        "strict": true,
        "schema": candidate_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input))
    })
}

pub(crate) fn candidate_output_schema_with_allowed_refs(
    allowed_refs: &[String],
) -> serde_json::Value {
    let evidence_id_item = if allowed_refs.is_empty() {
        json!({ "type": "string", "minLength": 1 })
    } else {
        json!({ "type": "string", "enum": allowed_refs })
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary_draft",
            "classification_candidates",
            "memory_candidates",
            "entity_candidates",
            "relationship_candidates",
            "retrieval_candidates",
            "importance_score"
        ],
        "properties": {
            "summary_draft": {
                "type": "object",
                "additionalProperties": false,
                "required": ["title", "body_text", "evidence_segment_ids"],
                "properties": {
                    "title": { "type": "string", "minLength": 1 },
                    "body_text": { "type": "string", "minLength": 1 },
                    "evidence_segment_ids": {
                        "type": "array",
                        "minItems": 1,
                        "items": evidence_id_item.clone()
                    }
                }
            },
            "classification_candidates": {
                "type": "array",
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
                        "classification_type": { "type": "string", "enum": ["topic", "intent"] },
                        "classification_value": { "type": "string", "minLength": 1 },
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "memory_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "title",
                        "body_text",
                        "evidence_segment_ids",
                        "durability_label",
                        "retrieval_value_label",
                        "consequentiality_label",
                        "temporal_scope"
                    ],
                    "properties": {
                        "title": { "type": "string", "minLength": 1 },
                        "body_text": { "type": "string", "minLength": 1 },
                        "durability_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "retrieval_value_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "consequentiality_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "temporal_scope": { "type": "string", "enum": ["ephemeral", "time_bound", "ongoing", "enduring"] },
                        "evidence_segment_ids": {
                            "type": "array",
                            "minItems": 1,
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "entity_candidates": {
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
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "relationship_candidates": {
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
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "retrieval_candidates": {
                "type": "array",
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
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "importance_score": { "type": "integer", "minimum": 1, "maximum": 10 }
        }
    })
}

pub(crate) fn parse_candidate_output(
    output_text: &str,
    input: &ArtifactProcessorInput,
) -> Result<ModelCandidateArtifactOutput, ProcessorError> {
    let parsed: ModelCandidateArtifactOutput =
        serde_json::from_str(output_text).map_err(|source| ProcessorError::ParseModelJson {
            source,
            body_preview: preview(output_text),
        })?;
    parsed.validate(input)
}

#[cfg(any())]
pub(crate) fn parse_final_artifact_output(
    output_text: &str,
    input: &ArtifactProcessorInput,
) -> Result<ModelArtifactOutput, ProcessorError> {
    let parsed: ModelArtifactOutput =
        serde_json::from_str(output_text).map_err(|source| ProcessorError::ParseModelJson {
            source,
            body_preview: preview(output_text),
        })?;
    let parsed = parsed.resolve_evidence_aliases(input);
    parsed.validate_and_salvage(input)
}

#[allow(dead_code)]
pub(crate) fn combine_usage(
    left: Option<InferenceUsage>,
    right: Option<InferenceUsage>,
) -> Option<InferenceUsage> {
    fn add(left: Option<u64>, right: Option<u64>) -> Option<u64> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left + right),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        }
    }

    match (left, right) {
        (None, None) => None,
        (left, right) => Some(InferenceUsage {
            input_tokens: add(
                left.as_ref().and_then(|usage| usage.input_tokens),
                right.as_ref().and_then(|usage| usage.input_tokens),
            ),
            output_tokens: add(
                left.as_ref().and_then(|usage| usage.output_tokens),
                right.as_ref().and_then(|usage| usage.output_tokens),
            ),
            reasoning_tokens: add(
                left.as_ref().and_then(|usage| usage.reasoning_tokens),
                right.as_ref().and_then(|usage| usage.reasoning_tokens),
            ),
            total_tokens: add(
                left.as_ref().and_then(|usage| usage.total_tokens),
                right.as_ref().and_then(|usage| usage.total_tokens),
            ),
            reported_cost_micros: add(
                left.as_ref().and_then(|usage| usage.reported_cost_micros),
                right.as_ref().and_then(|usage| usage.reported_cost_micros),
            ),
        }),
    }
}

#[allow(dead_code)]
pub(crate) fn structured_output_schema_wrapper(
    input: &ArtifactProcessorInput,
) -> serde_json::Value {
    json!({
        "type": "json_schema",
        "name": "openarchive_artifact_enrichment",
        "strict": true,
        "schema": structured_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input))
    })
}

#[allow(dead_code)]
pub fn structured_output_schema() -> serde_json::Value {
    structured_output_schema_with_allowed_refs(&[])
}

#[allow(dead_code)]
pub(crate) fn structured_output_schema_with_allowed_refs(
    allowed_refs: &[String],
) -> serde_json::Value {
    let evidence_id_item = if allowed_refs.is_empty() {
        json!({ "type": "string", "minLength": 1 })
    } else {
        json!({ "type": "string", "enum": allowed_refs })
    };
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
            "importance_score"
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
                        "items": evidence_id_item.clone()
                    }
                }
            },
            "classifications": {
                "type": "array",
                "maxItems": MAX_CLASSIFICATIONS,
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
                            "items": evidence_id_item.clone()
                        }
                    }
                }
            },
            "memories": {
                "type": "array",
                "maxItems": MAX_MEMORIES,
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
                            "enum": MEMORY_TYPE_VALUES
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
                            "items": evidence_id_item.clone()
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
                            "items": evidence_id_item.clone()
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
                            "items": evidence_id_item.clone()
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
                            "items": evidence_id_item
                        }
                    }
                }
            },
            "importance_score": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10
            }
        }
    })
}

#[allow(dead_code)]
pub(crate) fn openai_structured_output_schema(input: &ArtifactProcessorInput) -> serde_json::Value {
    let mut schema =
        structured_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input));
    if let Some(memories) = schema
        .get_mut("properties")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|properties| properties.get_mut("memories"))
        .and_then(serde_json::Value::as_object_mut)
    {
        memories.insert("maxItems".to_string(), serde_json::json!(MAX_MEMORIES));
        if let Some(items) = memories
            .get_mut("items")
            .and_then(serde_json::Value::as_object_mut)
        {
            if let Some(properties) = items
                .get_mut("properties")
                .and_then(serde_json::Value::as_object_mut)
            {
                if let Some(memory_type) = properties
                    .get_mut("memory_type")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    memory_type.insert("enum".to_string(), serde_json::json!(MEMORY_TYPE_VALUES));
                }
            }
        }
    }
    schema
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

const OPENAI_PROMPT_VERSION: &str = "openai-two-phase-v2";
pub(crate) const ANTHROPIC_PROMPT_VERSION: &str = "anthropic-two-phase-v2";
pub(crate) const GEMINI_PROMPT_VERSION: &str = "gemini-two-phase-v2";
pub(crate) const GROK_PROMPT_VERSION: &str = "grok-two-phase-v2";

pub(crate) const CONVERSATION_CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine for conversational artifacts. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent, human-readable synthesis of the conversation's central topics, actors, stakes, and outcomes. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, constraint, decision, preference, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later. It is acceptable to include candidates that may later be pruned.\n- relationship_candidates: Emit explicit or strongly supported relationships between emitted entities. It is acceptable to include related candidates that may later be simplified.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization.";
pub(crate) const DOCUMENT_CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine for document artifacts. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent synthesis of the document's purpose, main claims, decisions, action items, and durable context. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses such as document form and durable topical domains. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, requirement, commitment, action item, constraint, decision, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later, including named people, teams, projects, systems, organizations, vendors, or other proper nouns explicitly supported by the document.\n- relationship_candidates: Emit explicit or strongly supported relationships between emitted entities, including ownership, participation, coordination, dependency, affiliation, or decision impact when supported.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization, including meeting notes, call logs, and reference documents.";

#[allow(dead_code)]
pub(crate) const TWO_PHASE_FINALIZER_SYSTEM_PROMPT: &str = "You are OpenArchive's extraction finalizer. Use only the provided candidate bundle and return ONLY valid JSON in the requested final schema.\n\nGeneral rules:\n1. Do not invent facts, evidence refs, or objects not supported by the candidate bundle.\n2. Preserve evidence refs exactly.\n3. Do not treat any count limit as a target. Keep the strongest distinct items that belong; do not pad, and do not collapse unrelated facts merely to be shorter.\n4. When several candidates are individually valid, prefer the set with higher long-term retrieval value, clearer future usefulness, and greater downstream consequence.\n5. The candidate bundle is already a first-pass extraction. Pruning is optional, not required. If the candidate count is already within the schema limit, you do not need to remove anything. Drop or merge only when candidates are genuine duplicates, clear near-duplicates, unsupported by the bundle, or clearly low-value and local to this artifact.\n\nObject guidance:\n- summary: Keep it coherent and central. Preserve the main story of the artifact without turning the summary into a dump of all candidates.\n- classifications: Keep only useful browse or filter lenses. Remove generic, weak, or redundant classifications.\n- memories: Keep distinct, durable, retrieval-worthy memories. Use the candidate labels for durability, retrieval value, consequentiality, and temporal scope as editorial signals, not rigid rules. Candidates marked high on durability, retrieval value, or consequentiality should usually be retained unless they duplicate another stronger candidate. Deprioritize transient logistics, one-off setup details, low-value inventories, and items whose future usefulness is narrowly local to this artifact. Keep separate memories when candidates would answer meaningfully different future retrieval questions, even if they are related or appear close together in the source. Merge only genuine duplicates or near-duplicates. Do not collapse multiple important facts into an umbrella memory when keeping them separate would improve future retrieval.\n- entities: Keep only salient entities that stand on their own as future retrieval targets, but be conservative about dropping supported entities when they anchor retained memories, relationships, or retrieval intents.\n- relationships: Keep semantically clean, well-supported relationships between retained entities. Prefer retaining a supported relationship over dropping it merely for brevity.\n- retrieval_intents: Keep questions that are likely future lookups or follow-ups, not paraphrases of the summary. Prefer preserving intents that map to retained memories or relationships.\n\nSchema guidance:\n- Fix bad type assignments and choose memory_type only from the allowed schema values.\n- Output valid JSON only.";

pub(crate) const RECONCILIATION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict reconciliation engine. Use only the provided extraction candidates, retrieval results, and source evidence. Return ONLY valid JSON.\n\nRules:\n1. Prefer no merge over a weak merge.\n2. Choose create_new when the archive evidence is insufficient.\n3. Use attach_to_existing or strengthen_existing only when the retrieved object clearly matches the candidate.\n4. Use supersede_existing only when the new artifact clearly updates or replaces prior state.\n5. Use contradicts_existing only when the artifact clearly conflicts with retrieved prior state.\n6. Never merge identities, projects, or relationships on vague topical overlap.\n7. Every decision must cite real evidence_segment_ids from the extraction candidates.\n8. Any decision that uses an existing-object action must include the exact matched_object_id from retrieval results.\n9. If you cannot name an exact matched_object_id, choose create_new.\n10. Output valid JSON only.";

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
        candidate_response: String,
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
                output_text: self.candidate_response.clone(),
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
            artifact_class: ArtifactClass::Conversation,
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

    fn sample_document_input() -> ArtifactProcessorInput {
        ArtifactProcessorInput {
            artifact_id: "artifact-doc-1".to_string(),
            import_id: "import-doc-1".to_string(),
            artifact_class: ArtifactClass::Document,
            source_type: SourceType::MarkdownFile,
            title: Some("Sprint planning notes".to_string()),
            participants: Vec::new(),
            segments: vec![
                LoadedSegment {
                    segment_id: "seg-1".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 0,
                    text_content: "Sprint planning with Alice and Bob for OpenArchive ingestion work."
                        .to_string(),
                    created_at_source: None,
                    visibility_status: crate::VisibilityStatus::Visible,
                },
                LoadedSegment {
                    segment_id: "seg-2".to_string(),
                    participant_id: None,
                    participant_role: None,
                    sequence_no: 1,
                    text_content:
                        "Decision: ship markdown and text ingestion before docx. Alice owns parser work."
                            .to_string(),
                    created_at_source: None,
                    visibility_status: crate::VisibilityStatus::Visible,
                },
            ],
        }
    }

    fn sample_candidate_response() -> String {
        serde_json::json!({
            "summary_draft": {
                "title": "Candidate summary",
                "body_text": "The artifact discusses hardening worker failure behavior and the next milestone for the real pipeline.",
                "evidence_segment_ids": ["evidence_ref_1", "evidence_ref_2"]
            },
            "classification_candidates": [
                {
                    "classification_type": "intent",
                    "classification_value": "fix_failure_handling",
                    "title": "Fix failure handling",
                    "body_text": "The artifact is focused on hardening worker failure behavior.",
                    "evidence_segment_ids": ["evidence_ref_1"]
                }
            ],
            "memory_candidates": [
                {
                    "title": "Task 12 uses hosted inference",
                    "body_text": "The first real pipeline uses a hosted inference provider.",
                    "evidence_segment_ids": ["evidence_ref_2"],
                    "durability_label": "high",
                    "retrieval_value_label": "high",
                    "consequentiality_label": "medium",
                    "temporal_scope": "enduring"
                }
            ],
            "entity_candidates": [],
            "relationship_candidates": [],
            "retrieval_candidates": [],
            "importance_score": 8
        })
        .to_string()
    }

    #[test]
    fn openai_processor_parses_and_validates_output() {
        let client = Arc::new(FixedInferenceClient {
            candidate_response: sample_candidate_response(),
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
    }

    #[test]
    fn openai_processor_rejects_unknown_evidence_segment_ids() {
        let client = Arc::new(FixedInferenceClient {
            candidate_response: serde_json::json!({
                "summary_draft": {
                    "title": "Bad evidence",
                    "body_text": "The model referenced an unknown segment.",
                    "evidence_segment_ids": ["evidence_ref_99"]
                },
                "classification_candidates": [
                    {
                        "classification_type": "topic",
                        "classification_value": "worker_failure_handling",
                        "title": "Worker hardening",
                        "body_text": "Specific hardening work.",
                        "evidence_segment_ids": ["evidence_ref_1"]
                    }
                ],
                "memory_candidates": [],
                "entity_candidates": [],
                "relationship_candidates": [],
                "retrieval_candidates": [],
                "importance_score": 5
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
                    "summary_draft": {
                        "title": "Bad evidence",
                        "body_text": "The model referenced an unknown segment.",
                        "evidence_segment_ids": ["evidence_ref_99"]
                    },
                    "classification_candidates": [],
                    "memory_candidates": [],
                    "entity_candidates": [],
                    "relationship_candidates": [],
                    "retrieval_candidates": [],
                    "importance_score": 5
                })
                .to_string(),
                serde_json::json!({
                    "summary_draft": {
                        "title": "Hardening and next steps",
                        "body_text": "The worker should fail invalid payloads and the next milestone is the real pipeline.",
                        "evidence_segment_ids": ["evidence_ref_1", "evidence_ref_2"]
                    },
                    "classification_candidates": [
                        {
                            "classification_type": "intent",
                            "classification_value": "fix_failure_handling",
                            "title": "Fix failure handling",
                            "body_text": "The artifact is focused on hardening worker failure behavior.",
                            "evidence_segment_ids": ["evidence_ref_1"]
                        }
                    ],
                    "memory_candidates": [
                        {
                            "title": "Task 12 uses hosted inference",
                            "body_text": "The first real pipeline uses a hosted inference provider.",
                            "evidence_segment_ids": ["evidence_ref_2"],
                            "durability_label": "high",
                            "retrieval_value_label": "high",
                            "consequentiality_label": "medium",
                            "temporal_scope": "enduring"
                        }
                    ],
                    "entity_candidates": [],
                    "relationship_candidates": [],
                    "retrieval_candidates": [],
                    "importance_score": 7
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
    }

    #[test]
    fn openai_processor_drops_invalid_objects_without_rejecting_artifact() {
        let client = Arc::new(FixedInferenceClient {
            candidate_response: serde_json::json!({
                "summary_draft": {
                    "title": "Logo symbol preferences",
                    "body_text": "The artifact prefers the original hash-in-book symbol.",
                    "evidence_segment_ids": ["evidence_ref_1"]
                },
                "classification_candidates": [],
                "memory_candidates": [
                    {
                        "title": "Preferred HashBooks Logo Symbol",
                        "body_text": "The original hash-in-book symbol should remain the preferred logo mark.",
                        "evidence_segment_ids": ["evidence_ref_1"],
                        "durability_label": "high",
                        "retrieval_value_label": "high",
                        "consequentiality_label": "medium",
                        "temporal_scope": "enduring"
                    },
                    {
                        "title": "Prefer original logo symbol",
                        "body_text": "The original hash-in-book symbol should remain the preferred logo mark.",
                        "evidence_segment_ids": ["evidence_ref_1"],
                        "durability_label": "high",
                        "retrieval_value_label": "high",
                        "consequentiality_label": "medium",
                        "temporal_scope": "enduring"
                    }
                ],
                "entity_candidates": [],
                "relationship_candidates": [],
                "retrieval_candidates": [],
                "importance_score": 6
            })
            .to_string(),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-5-mini", "gpt-5.4");
        let processor = factory
            .build(EnrichmentTier::Standard)
            .expect("processor should build");

        let output = processor
            .process(&sample_input())
            .expect("processor should salvage valid objects");
        assert_eq!(
            output.summary.body_text,
            "The artifact prefers the original hash-in-book symbol."
        );
        assert_eq!(output.memories.len(), 1);
        assert_eq!(output.memories[0].memory_scope_value, "artifact-1");
    }

    #[test]
    fn conversation_prompt_lists_exact_allowed_evidence_refs() {
        let prompt = build_conversation_user_prompt(&sample_input()).expect("prompt should build");

        assert!(prompt.contains("allowed evidence_ref values for this artifact"));
        assert!(prompt.contains("evidence_ref_1"));
        assert!(prompt.contains("evidence_ref_2"));
        assert!(!prompt.contains("for example s1, s2"));
    }

    #[test]
    fn conversation_prompt_requires_discrete_personal_history_memories() {
        let prompt = build_conversation_user_prompt(&sample_input()).expect("prompt should build");

        assert!(prompt.contains("summary is not a substitute for memories"));
        assert!(prompt.contains("do not mirror the summary into memories"));
        assert!(prompt.contains("classifications are broad topical labels only"));
        assert!(prompt.contains("choose the closest memory_type from the schema"));
        assert!(prompt.contains("biographical and health history belongs in durable memories"));
    }

    #[test]
    fn document_prompt_mentions_meeting_notes_and_entities() {
        let prompt =
            build_document_user_prompt(&sample_document_input()).expect("prompt should build");

        assert!(prompt.contains("artifact_class: document"));
        assert!(prompt.contains("meeting notes, call notes"));
        assert!(prompt.contains("attendees, speakers, owners"));
        assert!(prompt.contains("entities: include explicit named people"));
    }

    #[test]
    fn canonicalize_memory_type_maps_health_history_to_personal_fact() {
        let memory_type = canonicalize_memory_type(
            "",
            "TBI and broken back history",
            "History includes a brain hemorrhage after a car accident, a second concussion, lung nodules, smoking history, and elite cycling background.",
        );

        assert_eq!(memory_type, "personal_fact");
    }

    fn sample_reconciliation_input() -> ReconciliationProcessorInput {
        ReconciliationProcessorInput {
            artifact_id: "artifact-1".to_string(),
            source_type: SourceType::ChatGptExport,
            summary: SummaryOutput {
                title: Some("Architecture direction".to_string()),
                body_text: "The user wants the worker to avoid silent fallback.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            },
            memories: vec![MemoryOutput {
                candidate_key: "memory_1".to_string(),
                title: Some("Avoid silent fallback".to_string()),
                body_text: "The worker should not silently fall back.".to_string(),
                memory_type: "project_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: "artifact-1".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            }],
            relationships: Vec::new(),
            retrieval_results_json: serde_json::json!({
                "objects": [
                    {
                        "object_id": "obj-1",
                        "title": "Silent fallback rule",
                        "body_text": "The worker should not silently fall back."
                    }
                ]
            })
            .to_string(),
        }
    }

    #[test]
    fn reconciliation_normalizes_ungrounded_existing_decision_to_create_new() {
        let input = sample_reconciliation_input();
        let output = ModelReconciliationOutput {
            decisions: vec![ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::StrengthenExisting,
                target_kind: "memory".to_string(),
                target_key: "memory_1".to_string(),
                matched_object_id: None,
                rationale: "This looks like the same memory.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            }],
        };

        let decisions = output
            .into_validated_outputs(&input)
            .expect("normalization should preserve the candidate");

        assert_eq!(decisions.len(), 1);
        assert_eq!(
            decisions[0].decision_kind,
            ReconciliationDecisionKind::CreateNew
        );
        assert_eq!(decisions[0].matched_object_id, None);
    }

    #[test]
    fn reconciliation_requires_id_for_existing_object_decisions_after_normalization() {
        let input = sample_reconciliation_input();
        let output = ModelReconciliationOutput {
            decisions: vec![ModelReconciliationDecision {
                decision_kind: ReconciliationDecisionKind::StrengthenExisting,
                target_kind: "memory".to_string(),
                target_key: "memory_1".to_string(),
                matched_object_id: Some("obj-1".to_string()),
                rationale: "This clearly matches the prior memory.".to_string(),
                evidence_segment_ids: vec!["seg-1".to_string()],
            }],
        };

        let decisions = output
            .into_validated_outputs(&input)
            .expect("grounded existing-object decision should remain valid");

        assert_eq!(decisions.len(), 1);
        assert_eq!(
            decisions[0].decision_kind,
            ReconciliationDecisionKind::StrengthenExisting
        );
        assert_eq!(decisions[0].matched_object_id.as_deref(), Some("obj-1"));
    }
}
