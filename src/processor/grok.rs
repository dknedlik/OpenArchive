use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

use crate::config::GrokConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    ArtifactProcessor, ArtifactProcessorFactory, ConversationEnrichmentStrategy,
    HostedArtifactProcessor, HostedReconciliationProcessor, InferenceClient, InferenceResult,
    InferenceUsage, OpenRouterResponsesContentItem, OpenRouterResponsesInputItem,
    OpenRouterResponsesReasoningConfig, OpenRouterResponsesRequest, OpenRouterResponsesResponse,
    OpenRouterResponsesTextConfig, ProcessorError, ReconciliationProcessor,
    RECONCILIATION_SYSTEM_PROMPT,
};

pub struct GrokProcessorFactory {
    client: Arc<dyn InferenceClient>,
    standard_model: String,
    quality_model: String,
}

impl GrokProcessorFactory {
    pub fn new(config: GrokConfig) -> Result<Self, String> {
        let client = GrokClient::new(&config).map_err(|err| err.to_string())?;
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

impl ArtifactProcessorFactory for GrokProcessorFactory {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(HostedArtifactProcessor {
            client: Arc::clone(&self.client),
            model,
            pipeline_name: "grok_enrichment",
            provider_name: "grok",
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
    fn complete_json(
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
            reasoning: OpenRouterResponsesReasoningConfig { effort: None },
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
                body_preview: super::preview(&response_text),
            });
        }

        let parsed: OpenRouterResponsesResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: super::preview(&response_text),
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
