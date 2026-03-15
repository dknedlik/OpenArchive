use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::config::AnthropicConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    ArtifactProcessor, ArtifactProcessorFactory, ConversationEnrichmentStrategy,
    HostedArtifactProcessor, InferenceClient, InferenceResult, InferenceUsage, ProcessorError,
    structured_output_schema,
};

pub struct AnthropicProcessorFactory {
    client: Arc<dyn InferenceClient>,
    standard_model: String,
    quality_model: String,
}

impl AnthropicProcessorFactory {
    pub fn new(config: AnthropicConfig) -> Result<Self, String> {
        let client = AnthropicClient::new(&config).map_err(|err| err.to_string())?;
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
}

impl ArtifactProcessorFactory for AnthropicProcessorFactory {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(HostedArtifactProcessor {
            client: Arc::clone(&self.client),
            model,
            pipeline_name: "anthropic_enrichment",
            provider_name: "anthropic",
            strategy: ConversationEnrichmentStrategy::openai_default(),
        }))
    }
}

struct AnthropicClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
}

impl AnthropicClient {
    fn new(config: &AnthropicConfig) -> Result<Self, ProcessorError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header = HeaderName::from_static("x-api-key");
        let api_key_value =
            HeaderValue::from_str(&config.api_key).map_err(|err| ProcessorError::Message {
                message: format!("invalid Anthropic API key header: {err}"),
            })?;
        default_headers.insert(api_key_header, api_key_value);
        default_headers.insert(
            HeaderName::from_static("anthropic-version"),
            HeaderValue::from_static("2023-06-01"),
        );

        let client = Client::builder()
            .default_headers(default_headers)
            .build()
            .map_err(|source| ProcessorError::BuildHttpClient { source })?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            max_output_tokens: config.max_output_tokens,
        })
    }
}

impl InferenceClient for AnthropicClient {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        let endpoint = format!("{}/messages", self.base_url);
        let body = AnthropicMessagesRequest {
            model,
            max_tokens: self.max_output_tokens,
            system: system_prompt,
            messages: vec![AnthropicMessageInput {
                role: "user",
                content: user_prompt,
            }],
            tools: vec![AnthropicToolDefinition {
                name: "record_enrichment",
                description: "Return the OpenArchive enrichment result as structured JSON.",
                input_schema: structured_output_schema(),
            }],
            tool_choice: AnthropicToolChoice {
                choice_type: "tool",
                name: "record_enrichment",
                disable_parallel_tool_use: true,
            },
        };
        let request_body = serde_json::to_vec(&body)
            .map_err(|source| ProcessorError::SerializePrompt { source })?;

        let response = self
            .client
            .post(endpoint)
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
                body_preview: super::preview(&response_text),
            });
        }

        let parsed: AnthropicMessagesResponse = serde_json::from_str(&response_text).map_err(
            |source| ProcessorError::ParseInferenceResponse {
                source,
                body_preview: super::preview(&response_text),
            },
        )?;

        let tool_input = parsed
            .content
            .iter()
            .find_map(|block| match block {
                AnthropicContentBlock::ToolUse { name, input, .. } if name == "record_enrichment" => {
                    Some(input.clone())
                }
                _ => None,
            })
            .ok_or_else(|| ProcessorError::Message {
                message: format!(
                    "Anthropic messages returned no enrichment tool result{}",
                    parsed
                        .stop_reason
                        .as_deref()
                        .map(|reason| format!(" (stop_reason={reason})"))
                        .unwrap_or_default()
                ),
            })?;

        let output_text = serde_json::to_string(&tool_input)
            .map_err(|source| ProcessorError::SerializePrompt { source })?;

        Ok(InferenceResult {
            output_text,
            usage: parsed.usage.map(InferenceUsage::from_anthropic_usage),
        })
    }
}

#[derive(Debug, Serialize)]
struct AnthropicMessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: Vec<AnthropicMessageInput<'a>>,
    tools: Vec<AnthropicToolDefinition>,
    tool_choice: AnthropicToolChoice<'a>,
}

#[derive(Debug, Serialize)]
struct AnthropicMessageInput<'a> {
    role: &'static str,
    content: &'a str,
}

#[derive(Debug, Serialize)]
struct AnthropicToolDefinition {
    name: &'static str,
    description: &'static str,
    input_schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct AnthropicToolChoice<'a> {
    #[serde(rename = "type")]
    choice_type: &'static str,
    name: &'a str,
    disable_parallel_tool_use: bool,
}

#[derive(Debug, Deserialize)]
struct AnthropicMessagesResponse {
    #[serde(default)]
    content: Vec<AnthropicContentBlock>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "tool_use")]
    ToolUse {
        #[serde(rename = "id")]
        _id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

impl InferenceUsage {
    fn from_anthropic_usage(usage: AnthropicUsage) -> Self {
        Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: None,
            total_tokens: usage
                .input_tokens
                .zip(usage.output_tokens)
                .map(|(input, output)| input + output),
            reported_cost_micros: None,
        }
    }
}
