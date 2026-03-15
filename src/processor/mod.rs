use std::collections::HashSet;
use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::config::{OpenAiConfig, OpenAiReasoningEffort};
use crate::storage::types::{
    EnrichmentTier, LoadedParticipant, LoadedSegment, ReconciliationDecisionKind, RetrievalIntent,
    ScopeType, SourceType,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub reported_cost_micros: Option<u64>,
}

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
    standard_model: String,
    quality_model: String,
}

impl OpenAiProcessorFactory {
    pub fn new(config: OpenAiConfig) -> Result<Self, String> {
        let client = OpenAiClient::new(&config).map_err(|err| err.to_string())?;
        let quality_model = config
            .quality_model
            .clone()
            .unwrap_or_else(|| config.standard_model.clone());

        Ok(Self {
            client: Arc::new(client),
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
            standard_model: standard_model.into(),
            quality_model: quality_model.into(),
        }
    }
}

impl ArtifactProcessorFactory for OpenAiProcessorFactory {
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

fn segment_alias(index: usize) -> String {
    format!("s{}", index + 1)
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
                    "required": ["decision_kind", "target_kind", "target_key", "matched_object_id", "rationale", "evidence_segment_ids"],
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
                        "matched_object_id": { "type": ["string", "null"] },
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
