use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::config::GeminiConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    structured_output_schema, ArtifactProcessor, ArtifactProcessorFactory,
    ARTIFACT_EXTRACTION_SYSTEM_PROMPT, GEMINI_PROMPT_VERSION, HostedArtifactProcessor,
    InferenceClient, InferenceResult, InferenceUsage, ProcessorError,
};

pub struct GeminiProcessorFactory {
    client: Arc<dyn InferenceClient>,
    standard_model: String,
    quality_model: String,
}

impl GeminiProcessorFactory {
    pub fn new(config: GeminiConfig) -> Result<Self, String> {
        let client = GeminiClient::new(&config).map_err(|err| err.to_string())?;
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

impl ArtifactProcessorFactory for GeminiProcessorFactory {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(HostedArtifactProcessor {
            client: Arc::clone(&self.client),
            model,
            pipeline_name: "gemini_enrichment",
            provider_name: "gemini",
            prompt_version: GEMINI_PROMPT_VERSION,
            system_prompt: ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
        }))
    }
}

struct GeminiClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
}

impl GeminiClient {
    fn new(config: &GeminiConfig) -> Result<Self, ProcessorError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header = HeaderName::from_static("x-goog-api-key");
        let api_key_value =
            HeaderValue::from_str(&config.api_key).map_err(|err| ProcessorError::Message {
                message: format!("invalid Gemini API key header: {err}"),
            })?;
        default_headers.insert(api_key_header, api_key_value);

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

    fn complete_via_generate_content(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        let endpoint = format!(
            "{}/{}:generateContent",
            self.base_url,
            normalize_model_resource(model)
        );

        let body = GeminiGenerateContentRequest {
            system_instruction: GeminiRequestContent {
                role: None,
                parts: vec![GeminiTextPart {
                    text: system_prompt.to_string(),
                }],
            },
            contents: vec![GeminiRequestContent {
                role: Some("user".to_string()),
                parts: vec![GeminiTextPart {
                    text: user_prompt.to_string(),
                }],
            }],
            generation_config: GeminiGenerationConfig {
                response_mime_type: "application/json".to_string(),
                response_json_schema: structured_output_schema(),
                max_output_tokens: self.max_output_tokens,
            },
        };
        let request_body =
            serde_json::to_vec(&body).map_err(|source| ProcessorError::SerializePrompt { source })?;

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

        let parsed: GeminiGenerateContentResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: super::preview(&response_text),
                }
            })?;

        let output_text = parsed.flatten_text().ok_or_else(|| ProcessorError::Message {
            message: format!(
                "Gemini generateContent returned empty content{}",
                parsed
                    .primary_finish_reason()
                    .map(|reason| format!(" (finish_reason={reason})"))
                    .unwrap_or_default()
            ),
        })?;

        Ok(InferenceResult {
            output_text,
            usage: parsed
                .usage_metadata
                .clone()
                .and_then(InferenceUsage::from_gemini_usage),
        })
    }
}

impl InferenceClient for GeminiClient {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_via_generate_content(model, system_prompt, user_prompt)
    }
}

#[derive(Debug, Clone)]
pub struct GeminiBatchEnrichmentRequest {
    pub system_prompt: String,
    pub user_prompt: String,
    pub max_output_tokens: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiBatchJob {
    pub name: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default, rename = "displayName")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, rename = "createTime")]
    pub create_time: Option<String>,
    #[serde(default, rename = "updateTime")]
    pub update_time: Option<String>,
}

pub struct GeminiBatchClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
}

impl GeminiBatchClient {
    pub fn new(config: &GeminiConfig) -> Result<Self, ProcessorError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header = HeaderName::from_static("x-goog-api-key");
        let api_key_value =
            HeaderValue::from_str(&config.api_key).map_err(|err| ProcessorError::Message {
                message: format!("invalid Gemini API key header: {err}"),
            })?;
        default_headers.insert(api_key_header, api_key_value);

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

    pub fn submit_inline_batch(
        &self,
        model: &str,
        display_name: Option<&str>,
        requests: &[GeminiBatchEnrichmentRequest],
    ) -> Result<GeminiBatchJob, ProcessorError> {
        let endpoint = format!("{}/batches", self.base_url);
        let body = GeminiBatchCreateRequest {
            model: normalize_model_resource(model),
            config: GeminiBatchConfig {
                display_name: display_name.map(|value| value.to_string()),
            },
            src: GeminiBatchSource {
                inlined_requests: GeminiInlinedRequests {
                    requests: requests
                        .iter()
                        .map(|request| GeminiBatchRequestEntry {
                            request: GeminiGenerateContentRequest {
                                system_instruction: GeminiRequestContent {
                                    role: None,
                                    parts: vec![GeminiTextPart {
                                        text: request.system_prompt.clone(),
                                    }],
                                },
                                contents: vec![GeminiRequestContent {
                                    role: Some("user".to_string()),
                                    parts: vec![GeminiTextPart {
                                        text: request.user_prompt.clone(),
                                    }],
                                }],
                                generation_config: GeminiGenerationConfig {
                                    response_mime_type: "application/json".to_string(),
                                    response_json_schema: structured_output_schema(),
                                    max_output_tokens: request
                                        .max_output_tokens
                                        .unwrap_or(self.max_output_tokens),
                                },
                            },
                        })
                        .collect(),
                },
            },
        };
        let request_body =
            serde_json::to_vec(&body).map_err(|source| ProcessorError::SerializePrompt { source })?;

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

        serde_json::from_str(&response_text).map_err(|source| ProcessorError::ParseInferenceResponse {
            source,
            body_preview: super::preview(&response_text),
        })
    }

    pub fn get_batch(&self, name: &str) -> Result<GeminiBatchJob, ProcessorError> {
        let resource = name.trim_start_matches('/');
        let endpoint = if resource.starts_with("batches/") {
            format!("{}/{}", self.base_url, resource)
        } else {
            format!("{}/batches/{}", self.base_url, resource)
        };

        let response = self
            .client
            .get(endpoint)
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

        serde_json::from_str(&response_text).map_err(|source| ProcessorError::ParseInferenceResponse {
            source,
            body_preview: super::preview(&response_text),
        })
    }
}

impl InferenceUsage {
    fn from_gemini_usage(usage: GeminiUsageMetadata) -> Option<Self> {
        if usage.prompt_token_count.is_none()
            && usage.candidates_token_count.is_none()
            && usage.total_token_count.is_none()
            && usage.thoughts_token_count.is_none()
        {
            return None;
        }

        Some(Self {
            input_tokens: usage.prompt_token_count,
            output_tokens: usage.candidates_token_count,
            reasoning_tokens: usage.thoughts_token_count,
            total_tokens: usage.total_token_count,
            reported_cost_micros: None,
        })
    }
}

fn normalize_model_resource(model: &str) -> String {
    if model.starts_with("models/") {
        model.to_string()
    } else {
        format!("models/{model}")
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentRequest {
    system_instruction: GeminiRequestContent,
    contents: Vec<GeminiRequestContent>,
    generation_config: GeminiGenerationConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiRequestContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    parts: Vec<GeminiTextPart>,
}

#[derive(Debug, Serialize)]
struct GeminiTextPart {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerationConfig {
    response_mime_type: String,
    response_json_schema: serde_json::Value,
    max_output_tokens: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    usage_metadata: Option<GeminiUsageMetadata>,
}

impl GeminiGenerateContentResponse {
    fn flatten_text(&self) -> Option<String> {
        for candidate in &self.candidates {
            if let Some(content) = &candidate.content {
                let text: String = content
                    .parts
                    .iter()
                    .filter_map(|part| part.text.as_deref())
                    .collect();
                if !text.trim().is_empty() {
                    return Some(text);
                }
            }
        }

        None
    }

    fn primary_finish_reason(&self) -> Option<&str> {
        self.candidates
            .first()
            .and_then(|candidate| candidate.finish_reason.as_deref())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiResponseContent>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponseContent {
    #[serde(default)]
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponsePart {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiUsageMetadata {
    #[serde(default)]
    prompt_token_count: Option<u64>,
    #[serde(default)]
    candidates_token_count: Option<u64>,
    #[serde(default)]
    total_token_count: Option<u64>,
    #[serde(default)]
    thoughts_token_count: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchCreateRequest {
    model: String,
    config: GeminiBatchConfig,
    src: GeminiBatchSource,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchSource {
    inlined_requests: GeminiInlinedRequests,
}

#[derive(Debug, Serialize)]
struct GeminiInlinedRequests {
    requests: Vec<GeminiBatchRequestEntry>,
}

#[derive(Debug, Serialize)]
struct GeminiBatchRequestEntry {
    request: GeminiGenerateContentRequest,
}
