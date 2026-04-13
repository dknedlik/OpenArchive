use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};

use crate::config::AnthropicConfig;
use crate::storage::types::EnrichmentTier;

use super::*;
mod types;
use types::{
    AnthropicContentBlock, AnthropicMessageInput, AnthropicMessagesRequest, AnthropicToolChoice,
    AnthropicToolDefinition,
};

pub struct AnthropicProcessorFactory {
    client: Arc<dyn InferenceClient>,
    max_output_tokens: u32,
    heavy_model: String,
    fast_model: String,
}

impl AnthropicProcessorFactory {
    pub fn new(config: AnthropicConfig) -> Result<Self, String> {
        let client = Arc::new(AnthropicClient::new(&config).map_err(|err| err.to_string())?);
        Ok(Self {
            client,
            max_output_tokens: config.max_output_tokens,
            heavy_model: config.heavy_model,
            fast_model: config.fast_model,
        })
    }
}

impl ArtifactProcessorFactory for AnthropicProcessorFactory {
    fn build(&self, _tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(ExtractionPipelineProcessor {
            client,
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.max_output_tokens),
            stage2_max_output_tokens: Some(self.max_output_tokens),
            repair_max_output_tokens: Some(self.max_output_tokens),
            pipeline_name: "anthropic_extraction_pipeline",
            provider_name: "anthropic",
        }))
    }

    fn build_reconciliation_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        Ok(Box::new(HostedReconciliationProcessor {
            client: Arc::clone(&self.client),
            model: self.fast_model.clone(),
            system_prompt: RECONCILIATION_SYSTEM_PROMPT,
        }))
    }

    fn build_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ArtifactBatchProcessor>>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        let processor: Box<dyn ArtifactProcessor> = Box::new(ExtractionPipelineProcessor {
            client,
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.max_output_tokens),
            stage2_max_output_tokens: Some(self.max_output_tokens),
            repair_max_output_tokens: Some(self.max_output_tokens),
            pipeline_name: "anthropic_extraction_pipeline",
            provider_name: "anthropic",
        });
        Ok(Some(Box::new(SequentialArtifactBatchProcessor::new(
            processor, 16, 2_000_000,
        ))))
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
            .timeout(std::time::Duration::from_secs(180))
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
    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_text_with_max_output_tokens_override(
            model,
            system_prompt,
            user_prompt,
            self.max_output_tokens,
        )
    }

    fn complete_text_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        let endpoint = format!("{}/messages", self.base_url);
        let body = serde_json::json!({
            "model": model,
            "max_tokens": max_output_tokens,
            "system": system_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": user_prompt
                }
            ]
        });
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
                body_preview: preview(&response_text),
                retry_after_seconds: None,
            });
        }

        let parsed: types::AnthropicMessagesResponse = serde_json::from_str(&response_text)
            .map_err(|source| ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(&response_text),
            })?;

        let output_text = parsed
            .content
            .iter()
            .filter_map(|block| match block {
                types::AnthropicContentBlock::Text { text } if !text.trim().is_empty() => {
                    Some(text.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        if output_text.trim().is_empty() {
            return Err(ProcessorError::Message {
                message: format!(
                    "Anthropic messages returned no text content{}",
                    parsed
                        .stop_reason
                        .as_deref()
                        .map(|reason| format!(" (stop_reason={reason})"))
                        .unwrap_or_default()
                ),
            });
        }

        Ok(InferenceResult {
            output_text,
            usage: parsed.usage.map(InferenceUsage::from_anthropic_usage),
        })
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_json_with_max_output_tokens_override(
            model,
            system_prompt,
            user_prompt,
            schema,
            self.max_output_tokens,
        )
    }

    fn complete_json_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        let endpoint = format!("{}/messages", self.base_url);
        let body = AnthropicMessagesRequest {
            model,
            max_tokens: max_output_tokens,
            system: system_prompt,
            messages: vec![AnthropicMessageInput {
                role: "user",
                content: user_prompt,
            }],
            tools: vec![AnthropicToolDefinition {
                name: "record_enrichment",
                description: "Return the OpenArchive enrichment result as structured JSON.",
                input_schema: AnthropicClient::tool_input_schema(schema),
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
                body_preview: preview(&response_text),
                retry_after_seconds: None,
            });
        }

        let parsed: types::AnthropicMessagesResponse = serde_json::from_str(&response_text)
            .map_err(|source| ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(&response_text),
            })?;

        let tool_input = parsed
            .content
            .iter()
            .find_map(|block| match block {
                AnthropicContentBlock::ToolUse { name, input, .. }
                    if name == "record_enrichment" =>
                {
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

impl AnthropicClient {
    fn tool_input_schema(schema: &serde_json::Value) -> serde_json::Value {
        schema
            .get("schema")
            .filter(|_| {
                schema.get("type").and_then(serde_json::Value::as_str) == Some("json_schema")
            })
            .cloned()
            .unwrap_or_else(|| schema.clone())
    }
}
