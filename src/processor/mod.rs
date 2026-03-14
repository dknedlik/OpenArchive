use std::collections::HashSet;
use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::config::{OpenAiConfig, OpenAiReasoningEffort};
use crate::storage::types::{EnrichmentTier, LoadedParticipant, LoadedSegment, ScopeType, SourceType};

mod anthropic;
mod gemini;
mod grok;
pub use anthropic::AnthropicProcessorFactory;
pub use gemini::{GeminiBatchClient, GeminiBatchEnrichmentRequest, GeminiBatchJob, GeminiProcessorFactory};
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
    pub importance_score: u8,
    pub escalate_to_frontier: bool,
    pub escalation_reason: Option<String>,
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
                memory_type: "artifact_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: input.artifact_id.clone(),
                evidence_segment_ids,
            }],
            importance_score: 1,
            escalate_to_frontier: false,
            escalation_reason: None,
        })
    }
}

pub trait ArtifactProcessorFactory: Send + Sync {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError>;
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
}

struct HostedArtifactProcessor {
    client: Arc<dyn InferenceClient>,
    model: String,
    pipeline_name: &'static str,
    provider_name: &'static str,
    strategy: Arc<dyn EnrichmentStrategy>,
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
        let inference_result = self
            .client
            .complete_json(&self.model, self.strategy.system_prompt(), user_prompt)?;
        let parsed: ModelArtifactOutput =
            serde_json::from_str(&inference_result.output_text).map_err(|source| {
                ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&inference_result.output_text),
                }
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
        if !should_shape_conversation_input(input) {
            let prompt = build_conversation_user_prompt(input)?;
            return processor.run_prompt(input, &prompt);
        }

        let windows = build_conversation_windows(input);
        let mut chunk_outputs = Vec::with_capacity(windows.len());
        for (index, window) in windows.iter().enumerate() {
            let chunk_prompt =
                build_conversation_chunk_prompt(input, window, index + 1, windows.len())?;
            let chunk_input = ArtifactProcessorInput {
                artifact_id: input.artifact_id.clone(),
                import_id: input.import_id.clone(),
                source_type: input.source_type,
                title: input.title.clone(),
                participants: input.participants.clone(),
                segments: window.segments.clone(),
            };
            chunk_outputs.push(processor.run_prompt(&chunk_input, &chunk_prompt)?);
        }

        let synthesis_prompt = build_conversation_synthesis_prompt(input, &chunk_outputs)?;
        processor.run_prompt(input, &synthesis_prompt)
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
}

trait InferenceClient: Send + Sync {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
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
        let auth_value =
            HeaderValue::from_str(&bearer).map_err(|err| ProcessorError::Message {
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
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_via_responses(model, system_prompt, user_prompt)
    }
}

impl OpenAiClient {
    fn complete_via_responses(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
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
                format: structured_output_schema_wrapper(),
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

        let request_body =
            serde_json::to_vec(&body).map_err(|source| ProcessorError::SerializePrompt { source })?;

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
            serde_json::from_str(&response_text).map_err(|source| ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(&response_text),
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
                        OpenRouterResponsesOutputContent::OutputText { text } if !text.trim().is_empty() => {
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
        let reported_cost_micros = usage
            .cost
            .map(|cost| (cost * 1_000_000.0).round() as u64);

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
            validate_text_field(&format!("classifications[{index}].title"), &classification.title)?;
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
                detail: format!("model returned {} memories; expected at most 5", self.memories.len()),
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
                detail:
                    "escalate_to_frontier cannot be true when importance_score is below 8"
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
            importance_score: self.importance_score,
            escalate_to_frontier: self.escalate_to_frontier,
            escalation_reason: normalize_optional_text(self.escalation_reason),
        }
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

fn build_conversation_user_prompt(input: &ArtifactProcessorInput) -> Result<String, ProcessorError> {
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

    let segments_json =
        serde_json::to_string_pretty(&prompt_segments).map_err(|source| ProcessorError::SerializePrompt { source })?;

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

#[derive(Debug, Clone)]
struct ConversationWindow {
    segments: Vec<LoadedSegment>,
}

fn should_shape_conversation_input(input: &ArtifactProcessorInput) -> bool {
    let char_count: usize = input
        .segments
        .iter()
        .map(|segment| segment.text_content.len())
        .sum();
    input.segments.len() >= 60 || char_count > 100_000
}

fn build_conversation_windows(input: &ArtifactProcessorInput) -> Vec<ConversationWindow> {
    const WINDOW_SIZE: usize = 36;
    const OVERLAP: usize = 6;

    if input.segments.len() <= WINDOW_SIZE {
        return vec![ConversationWindow {
            segments: input.segments.clone(),
        }];
    }

    let mut windows = Vec::new();
    let mut start = 0usize;
    while start < input.segments.len() {
        let end = usize::min(start + WINDOW_SIZE, input.segments.len());
        windows.push(ConversationWindow {
            segments: input.segments[start..end].to_vec(),
        });
        if end == input.segments.len() {
            break;
        }
        start = end.saturating_sub(OVERLAP);
    }

    windows
}

fn build_conversation_chunk_prompt(
    input: &ArtifactProcessorInput,
    window: &ConversationWindow,
    chunk_index: usize,
    chunk_count: usize,
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        participant_role: &'a str,
        text: &'a str,
    }

    let prompt_segments: Vec<_> = window
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
        "Input artifact chunk:\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         chunk_index: {chunk_index}\n\
         chunk_count: {chunk_count}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         \n\
         Output guidance:\n\
         - focus on durable information from this chunk only\n\
         - summarize the important local content without trying to cover the entire artifact\n\
         - classifications: prefer 0 or 1\n\
         - memories: capture distinct durable facts present in this chunk\n\
         - evidence_segment_ids must use only the evidence_ref values shown in this chunk\n\
         - cite the minimum evidence needed; prefer 1-2 refs per item\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        chunk_index = chunk_index,
        chunk_count = chunk_count,
        segments_json = segments_json,
    ))
}

fn build_conversation_synthesis_prompt(
    input: &ArtifactProcessorInput,
    chunk_outputs: &[ArtifactProcessorOutput],
) -> Result<String, ProcessorError> {
    #[derive(Serialize)]
    struct ChunkSummary<'a> {
        chunk_index: usize,
        summary_title: Option<&'a str>,
        summary_body_text: &'a str,
        summary_evidence_segment_ids: &'a [String],
        classifications: Vec<ChunkClassification<'a>>,
        memories: Vec<ChunkMemory<'a>>,
        importance_score: u8,
    }

    #[derive(Serialize)]
    struct ChunkClassification<'a> {
        classification_type: &'a str,
        classification_value: &'a str,
        title: Option<&'a str>,
        body_text: Option<&'a str>,
        evidence_segment_ids: &'a [String],
    }

    #[derive(Serialize)]
    struct ChunkMemory<'a> {
        memory_type: &'a str,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }

    let chunk_summaries: Vec<_> = chunk_outputs
        .iter()
        .enumerate()
        .map(|(index, output)| ChunkSummary {
            chunk_index: index + 1,
            summary_title: output.summary.title.as_deref(),
            summary_body_text: output.summary.body_text.as_str(),
            summary_evidence_segment_ids: &output.summary.evidence_segment_ids,
            classifications: output
                .classifications
                .iter()
                .map(|classification| ChunkClassification {
                    classification_type: classification.classification_type.as_str(),
                    classification_value: classification.classification_value.as_str(),
                    title: classification.title.as_deref(),
                    body_text: classification.body_text.as_deref(),
                    evidence_segment_ids: &classification.evidence_segment_ids,
                })
                .collect(),
            memories: output
                .memories
                .iter()
                .map(|memory| ChunkMemory {
                    memory_type: memory.memory_type.as_str(),
                    title: memory.title.as_deref(),
                    body_text: memory.body_text.as_str(),
                    evidence_segment_ids: &memory.evidence_segment_ids,
                })
                .collect(),
            importance_score: output.importance_score,
        })
        .collect();

    let chunk_json = serde_json::to_string_pretty(&chunk_summaries)
        .map_err(|source| ProcessorError::SerializePrompt { source })?;

    Ok(format!(
        "Synthesize a final artifact enrichment from these chunk findings.\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         chunk_findings:\n\
         {chunk_json}\n\
         \n\
         Output guidance:\n\
         - produce the final artifact-level summary, classifications, and memories\n\
         - merge duplicate or overlapping chunk findings into broader artifact-level outputs\n\
         - preserve important distinct subtopics when they materially add retrieval value\n\
         - evidence_segment_ids must use only the real segment ids shown in the chunk findings\n\
         - prefer 3-5 distinct memories on rich artifacts when well supported\n\
         - classifications: prefer 0 or 1\n\
         - memory_scope_value must be {artifact_id}\n\
         ",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        title = input.title.as_deref().unwrap_or(""),
        chunk_json = chunk_json,
    ))
}

fn preview(value: &str) -> String {
    value.chars().take(240).collect()
}

fn build_segment_alias_map(input: &ArtifactProcessorInput) -> std::collections::HashMap<String, String> {
    input.segments
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

const OPENAI_PROMPT_VERSION: &str = "openai-strict-v1";
const GEMINI_PROMPT_VERSION: &str = "gemini-strict-v5";

const ARTIFACT_EXTRACTION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict extraction engine. Read one artifact and return ONLY valid JSON.\n\nReturn exactly these sections:\n- summary\n- classifications\n- memories\n- importance_score\n- escalate_to_frontier\n- escalation_reason\n\nRules:\n1. Output valid JSON only. No markdown or extra text.\n2. Every summary, classification, and memory must cite real evidence_segment_ids from the artifact.\n3. Do not invent facts, intentions, preferences, or commitments.\n4. Keep the output compact and machine-readable.\n5. Prefer fewer, stronger memories over weak ones.\n6. If the artifact contains durable engineering decisions, constraints, or explicit next steps, emit at least one memory.\n7. Classifications are optional. Prefer 0 or 1 classifications. Emit a second classification only if it adds a clearly distinct retrieval hook.\n8. Do not emit generic or redundant classifications.\n9. classification_type must be \"topic\" or \"intent\".\n10. topic must be a specific retrieval-useful subject phrase, not a generic label.\n11. intent must be a short concrete goal phrase in snake_case.\n12. Memories must use only these memory_type values: preference, project_fact, identity_fact, ongoing_task, reference.\n13. memory_scope is always \"artifact\" and memory_scope_value is the artifact_id.\n14. Summary must be 2-4 compact sentences focused on lasting engineering substance.\n15. importance_score measures long-term retrieval value, operational risk, or durable decision significance.\n16. Low-signal artifacts should usually have low importance and no classifications.\n17. escalate_to_frontier may be true only when importance_score >= 8 and the artifact would materially benefit from a higher-quality pass.\n18. escalation_reason must be empty when escalate_to_frontier is false and one short sentence when true.";

const GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict extraction engine for Gemini. Read one artifact and return ONLY valid JSON.\n\nReturn exactly these sections:\n- summary\n- classifications\n- memories\n- importance_score\n- escalate_to_frontier\n- escalation_reason\n\nRules:\n1. Output valid JSON only. No markdown or extra text.\n2. Every summary, classification, and memory must cite real evidence_segment_ids from the artifact.\n3. Do not invent facts, intentions, preferences, identities, or commitments.\n4. Keep the output compact, but do not overcompress rich artifacts.\n5. For dense artifacts, preserve the main topic plus the most important subtopics, decisions, or tensions instead of collapsing them into one generic idea.\n6. Prefer fewer, stronger memories over weak ones, but do not omit durable memories from rich artifacts.\n7. When several distinct durable facts exist, prefer memories that cover different aspects of the artifact instead of repeating one theme.\n8. Use memory_type precisely: preference for stable likes/dislikes or values stated by the user, identity_fact for self-description, ongoing_task for explicit unfinished work, project_fact for durable facts or decisions, reference for reusable external resources.\n9. Only use preference or identity_fact when the artifact directly supports them with first-person statements or clear stable values. Do not force those types when support is weak.\n10. Do not split one idea into several narrow project_fact memories. Prefer 3-5 distinct high-value memories over many small ones.\n11. Classifications are optional. Prefer 0 or 1 classifications. Emit a second classification only if it adds a clearly distinct retrieval hook.\n12. Do not emit generic or redundant classifications.\n13. classification_type must be \"topic\" or \"intent\".\n14. topic must be a specific retrieval-useful subject phrase, not a generic label.\n15. intent must be a short concrete goal phrase in snake_case.\n16. Classification body_text must be brief and should not restate the full summary.\n17. memory_scope is always \"artifact\" and memory_scope_value is the artifact_id.\n18. Summary must be 2-4 compact sentences, but should still cover the artifact's lasting substance.\n19. importance_score measures long-term retrieval value, operational risk, or durable decision significance.\n20. Routine advice, consumer guidance, troubleshooting, and general Q&A should usually stay in the 3-6 range.\n21. Use 7 or higher only for artifacts with durable life decisions, major technical direction, significant operational risk, or unusually reusable knowledge.\n22. Low-signal artifacts should usually have low importance and no classifications.\n23. escalate_to_frontier may be true only when importance_score >= 8 and the artifact would materially benefit from a higher-quality pass.\n24. escalation_reason must be empty when escalate_to_frontier is false and one short sentence when true.";

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
                "importance_score": 8,
                "escalate_to_frontier": true,
                "escalation_reason": "High-value implementation direction with durable engineering impact."
            })
            .to_string(),
        });
        let factory =
            OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory.build(EnrichmentTier::Standard).expect("processor should build");

        let output = processor.process(&sample_input()).expect("processor should succeed");

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
                "importance_score": 5,
                "escalate_to_frontier": false,
                "escalation_reason": ""
            })
            .to_string(),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory.build(EnrichmentTier::Standard).expect("processor should build");

        let err = processor.process(&sample_input()).expect_err("processor should fail");
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
                    "importance_score": 7,
                    "escalate_to_frontier": false,
                    "escalation_reason": ""
                })
                .to_string(),
            ]),
        });
        let factory = OpenAiProcessorFactory::with_client(client, "gpt-4.1-mini", "gpt-5.4");
        let processor = factory.build(EnrichmentTier::Standard).expect("processor should build");

        let output = processor
            .process(&sample_input())
            .expect("processor should repair and succeed");

        assert_eq!(output.classifications.len(), 1);
        assert_eq!(output.memories.len(), 1);
        assert_eq!(output.importance_score, 7);
        assert!(!output.escalate_to_frontier);
    }
}
