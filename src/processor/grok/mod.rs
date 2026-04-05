use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

use crate::config::GrokConfig;
use crate::storage::types::EnrichmentTier;

use super::*;

pub struct GrokProcessorFactory {
    client: Arc<GrokClient>,
    stage1_max_output_tokens: u32,
    stage2_max_output_tokens: u32,
    repair_max_output_tokens: u32,
    heavy_model: String,
    fast_model: String,
}

impl GrokProcessorFactory {
    pub fn new(config: GrokConfig) -> Result<Self, String> {
        let client = Arc::new(GrokClient::new(&config).map_err(|err| err.to_string())?);
        Ok(Self {
            client,
            stage1_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            stage2_max_output_tokens: config.max_output_tokens,
            repair_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            heavy_model: config.heavy_model,
            fast_model: config.fast_model,
        })
    }
}

impl ArtifactProcessorFactory for GrokProcessorFactory {
    fn build(&self, _tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(ExtractionPipelineProcessor {
            client,
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.repair_max_output_tokens),
            pipeline_name: "grok_extraction_pipeline",
            provider_name: "grok",
        }))
    }

    fn build_reconciliation_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(HostedReconciliationProcessor {
            client,
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
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.repair_max_output_tokens),
            pipeline_name: "grok_extraction_pipeline",
            provider_name: "grok",
        });
        Ok(Some(Box::new(SequentialArtifactBatchProcessor::new(
            processor, 16, 2_000_000,
        ))))
    }
}

struct GrokClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
}

impl GrokClient {
    fn new(config: &GrokConfig) -> Result<Self, ProcessorError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let bearer = format!("Bearer {}", config.api_key);
        let auth_value = HeaderValue::from_str(&bearer).map_err(|err| ProcessorError::Message {
            message: format!("invalid Grok API key header: {err}"),
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
        })
    }
}

impl InferenceClient for GrokClient {
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

    fn complete_text_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_json_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            &serde_json::json!({ "type": "text" }),
            max_output_tokens,
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
        self.complete_json_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            max_output_tokens,
        )
    }
}

impl GrokClient {
    fn complete_json_with_max_output_tokens(
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
            reasoning: None,
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
                retry_after_seconds: None,
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
                message: "Grok responses returned empty content".to_string(),
            });
        }

        Ok(InferenceResult {
            output_text: content,
            usage,
        })
    }
}
