use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};

use crate::config::AnthropicConfig;
use crate::storage::types::EnrichmentTier;

use super::*;

mod batch;
mod types;

use batch::{
    AnthropicBatchRequestOwned, AnthropicExtractionSubmitter, AnthropicMessageBatch,
    AnthropicReconciliationSubmitter,
};
use types::{
    AnthropicContentBlock, AnthropicMessageInput, AnthropicMessagesRequest, AnthropicToolChoice,
    AnthropicToolDefinition,
};

pub struct AnthropicProcessorFactory {
    client: Arc<dyn InferenceClient>,
    batch_client: Option<Arc<AnthropicClient>>,
    standard_model: String,
    quality_model: String,
    reconcile_standard_model: String,
    reconcile_quality_model: String,
}

impl AnthropicProcessorFactory {
    pub fn new(config: AnthropicConfig) -> Result<Self, String> {
        let client = Arc::new(AnthropicClient::new(&config).map_err(|err| err.to_string())?);
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
            standard_model: config.standard_model,
            quality_model,
            reconcile_standard_model: config.reconcile_standard_model,
            reconcile_quality_model,
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
            strategy: ConversationEnrichmentStrategy::anthropic_default(),
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
        Ok(Some(Box::new(AnthropicExtractionSubmitter {
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
        Ok(Some(Box::new(AnthropicReconciliationSubmitter {
            client: Arc::clone(client),
            model,
        })))
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
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
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

    fn create_message_batch(
        &self,
        requests: &[AnthropicBatchRequestOwned],
    ) -> Result<AnthropicMessageBatch, ProcessorError> {
        let body = serde_json::to_vec(&types::AnthropicBatchCreateRequest { requests })
            .map_err(|source| ProcessorError::SerializePrompt { source })?;
        let response = self
            .client
            .post(format!("{}/messages/batches", self.base_url))
            .body(body)
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;
        parse_json_response(response)
    }

    fn get_message_batch(&self, batch_id: &str) -> Result<AnthropicMessageBatch, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/messages/batches/{}", self.base_url, batch_id))
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;
        parse_json_response(response)
    }

    fn read_results_text(&self, results_url: &str) -> Result<String, ProcessorError> {
        let url = if results_url.starts_with("http://") || results_url.starts_with("https://") {
            results_url.to_string()
        } else {
            format!("{}{}", self.base_url, results_url)
        };
        let response = self
            .client
            .get(url)
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
