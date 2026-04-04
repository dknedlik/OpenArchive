use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use log::info;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, RETRY_AFTER};

use crate::config::GeminiConfig;
use crate::storage::types::EnrichmentTier;

use super::*;

mod batch;
mod types;

use batch::{
    GeminiExtractionSubmitter, GeminiReconciliationBatchProcessor, GeminiReconciliationSubmitter,
};
use types::{
    GeminiBatchCreateRequest, GeminiBatchDefinition, GeminiBatchInputConfig, GeminiBatchOperation,
    GeminiBatchRequestEntry, GeminiBatchRequestMetadata, GeminiGenerateContentRequest,
    GeminiGenerateContentResponse, GeminiGenerationConfig, GeminiInlineBatchRequests,
    GeminiRequestContent, GeminiTextPart,
};

pub use types::{GeminiBatchEnrichmentRequest, GeminiBatchJob};

pub struct GeminiProcessorFactory {
    client: Arc<GeminiClient>,
    batch_client: Option<Arc<GeminiBatchClient>>,
    stage1_max_output_tokens: u32,
    stage2_max_output_tokens: u32,
    extract_repair_max_output_tokens: u32,
    heavy_model: String,
    fast_model: String,
}

impl GeminiProcessorFactory {
    pub fn new(config: GeminiConfig) -> Result<Self, String> {
        let client = GeminiClient::new(&config).map_err(|err| err.to_string())?;

        Ok(Self {
            client: Arc::new(client),
            batch_client: if config.batch_enabled {
                Some(Arc::new(
                    GeminiBatchClient::new(&config).map_err(|err| err.to_string())?,
                ))
            } else {
                None
            },
            stage1_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            stage2_max_output_tokens: config.max_output_tokens,
            extract_repair_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            heavy_model: config.heavy_model,
            fast_model: config.fast_model,
        })
    }
}

impl ArtifactProcessorFactory for GeminiProcessorFactory {
    fn build(&self, _tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(ExtractionPipelineProcessor {
            client,
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.extract_repair_max_output_tokens),
            pipeline_name: "gemini_extraction_pipeline",
            provider_name: "gemini",
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
            repair_max_output_tokens: Some(self.extract_repair_max_output_tokens),
            pipeline_name: "gemini_extraction_pipeline",
            provider_name: "gemini",
        });
        Ok(Some(Box::new(SequentialArtifactBatchProcessor::new(
            processor, 16, 2_000_000,
        ))))
    }

    fn build_reconciliation_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchProcessor>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        Ok(Some(Box::new(GeminiReconciliationBatchProcessor {
            client: Arc::clone(batch_client),
            model: self.fast_model.clone(),
        })))
    }

    fn build_extraction_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ExtractionBatchSubmitter>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        Ok(Some(Box::new(GeminiExtractionSubmitter {
            client: Arc::clone(batch_client),
            candidate_model: self.heavy_model.clone(),
        })))
    }

    fn build_reconciliation_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchSubmitter>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        Ok(Some(Box::new(GeminiReconciliationSubmitter {
            client: Arc::clone(batch_client),
            model: self.fast_model.clone(),
        })))
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
            .timeout(Duration::from_secs(180))
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
        schema: &serde_json::Value,
        max_output_tokens: u32,
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
                response_mime_type: Some("application/json".to_string()),
                response_schema: Some(normalize_gemini_response_schema(schema)),
                max_output_tokens,
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
        let headers = response.headers().clone();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(processor_http_status(
                status.as_u16(),
                &response_text,
                &headers,
            ));
        }

        let parsed: GeminiGenerateContentResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(&response_text),
                }
            })?;

        let output_text = parsed
            .flatten_text()
            .ok_or_else(|| ProcessorError::Message {
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

    fn complete_json_with_max_output_tokens(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_via_generate_content(
            model,
            system_prompt,
            user_prompt,
            schema,
            max_output_tokens,
        )
    }

    fn complete_text_with_max_output_tokens(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
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
                response_mime_type: None,
                response_schema: None,
                max_output_tokens,
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
        let headers = response.headers().clone();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(processor_http_status(
                status.as_u16(),
                &response_text,
                &headers,
            ));
        }

        let parsed: GeminiGenerateContentResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(&response_text),
                }
            })?;

        let output_text = parsed
            .flatten_text()
            .ok_or_else(|| ProcessorError::Message {
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

fn normalize_gemini_response_schema(schema: &serde_json::Value) -> serde_json::Value {
    let unwrapped = schema.get("schema").unwrap_or(schema);
    sanitize_gemini_schema_value(unwrapped)
}

fn sanitize_gemini_schema_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut sanitized = serde_json::Map::new();
            for (key, child) in map {
                if matches!(
                    key.as_str(),
                    "additionalProperties"
                        | "strict"
                        | "name"
                        | "schema"
                        | "minLength"
                        | "maxLength"
                        | "minItems"
                        | "maxItems"
                        | "minimum"
                        | "maximum"
                        | "pattern"
                        | "default"
                        | "description"
                ) {
                    continue;
                }
                if key == "type" {
                    let normalized = match child {
                        serde_json::Value::String(value) => {
                            serde_json::Value::String(value.to_ascii_uppercase())
                        }
                        serde_json::Value::Array(values) => {
                            let mut non_null = values
                                .iter()
                                .filter_map(|value| value.as_str())
                                .filter(|value| *value != "null");
                            let first = non_null.next();
                            let second = non_null.next();
                            match (first, second) {
                                (Some(first), None)
                                    if values
                                        .iter()
                                        .any(|value| value.as_str() == Some("null")) =>
                                {
                                    sanitized.insert(
                                        "nullable".to_string(),
                                        serde_json::Value::Bool(true),
                                    );
                                    serde_json::Value::String(first.to_ascii_uppercase())
                                }
                                _ => sanitize_gemini_schema_value(child),
                            }
                        }
                        _ => sanitize_gemini_schema_value(child),
                    };
                    sanitized.insert(key.clone(), normalized);
                    continue;
                }
                sanitized.insert(key.clone(), sanitize_gemini_schema_value(child));
            }
            serde_json::Value::Object(sanitized)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(sanitize_gemini_schema_value).collect())
        }
        _ => value.clone(),
    }
}

impl InferenceClient for GeminiClient {
    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_text_with_max_output_tokens(
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
        self.complete_json_with_max_output_tokens(
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
        self.complete_text_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
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

pub struct GeminiBatchClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
    max_batch_jobs: usize,
    batch_max_bytes: usize,
    poll_interval: Duration,
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
            .timeout(Duration::from_secs(180))
            .build()
            .map_err(|source| ProcessorError::BuildHttpClient { source })?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            max_output_tokens: config.max_output_tokens,
            max_batch_jobs: config.batch_max_jobs,
            batch_max_bytes: config.batch_max_bytes,
            poll_interval: config.batch_poll_interval,
        })
    }

    pub fn submit_inline_batch(
        &self,
        model: &str,
        display_name: Option<&str>,
        requests: &[GeminiBatchEnrichmentRequest],
    ) -> Result<GeminiBatchJob, ProcessorError> {
        let endpoint = format!(
            "{}/{}:batchGenerateContent",
            self.base_url,
            normalize_model_resource(model)
        );
        let body = GeminiBatchCreateRequest {
            batch: GeminiBatchDefinition {
                display_name: display_name.map(|value| value.to_string()),
                input_config: GeminiBatchInputConfig {
                    requests: GeminiInlineBatchRequests {
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
                                        response_mime_type: Some("application/json".to_string()),
                                        response_schema: Some(normalize_gemini_response_schema(
                                            &request.response_json_schema,
                                        )),
                                        max_output_tokens: request
                                            .max_output_tokens
                                            .unwrap_or(self.max_output_tokens),
                                    },
                                },
                                metadata: GeminiBatchRequestMetadata {
                                    key: request.key.clone(),
                                },
                            })
                            .collect(),
                    },
                },
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
        let headers = response.headers().clone();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(processor_http_status(
                status.as_u16(),
                &response_text,
                &headers,
            ));
        }

        let operation: GeminiBatchOperation =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(&response_text),
                }
            })?;
        let job = operation.into_batch_job();
        info!(
            "submitted gemini batch name={} display_name={:?} model={:?} requests={}",
            job.name,
            job.display_name,
            job.model,
            requests.len()
        );
        Ok(job)
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
        let headers = response.headers().clone();
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(processor_http_status(
                status.as_u16(),
                &response_text,
                &headers,
            ));
        }

        let operation: GeminiBatchOperation =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(&response_text),
                }
            })?;
        Ok(operation.into_batch_job())
    }

    pub fn wait_for_batch(&self, name: &str) -> Result<GeminiBatchJob, ProcessorError> {
        let mut poll_count: u64 = 0;
        loop {
            let job = match self.get_batch(name) {
                Ok(job) => job,
                Err(err) if err.is_retryable() => {
                    if should_log_poll(poll_count) {
                        log::warn!(
                            "gemini batch name={} poll={} transient poll error={}",
                            name,
                            poll_count,
                            err
                        );
                    }
                    poll_count += 1;
                    thread::sleep(self.poll_interval);
                    continue;
                }
                Err(err) => return Err(err),
            };
            let state = job.state.as_deref().unwrap_or_default();
            if should_log_poll(poll_count) {
                info!(
                    "gemini batch name={} poll={} state={}",
                    name, poll_count, state
                );
            }
            if matches!(
                state,
                "JOB_STATE_SUCCEEDED" | "SUCCEEDED" | "BATCH_STATE_SUCCEEDED"
            ) {
                return Ok(job);
            }
            if matches!(
                state,
                "JOB_STATE_FAILED"
                    | "FAILED"
                    | "JOB_STATE_CANCELLED"
                    | "CANCELLED"
                    | "BATCH_STATE_FAILED"
                    | "BATCH_STATE_CANCELLED"
            ) {
                return Err(ProcessorError::Message {
                    message: format!("Gemini batch {name} finished in terminal state {state}"),
                });
            }
            poll_count += 1;
            thread::sleep(self.poll_interval);
        }
    }
}

fn should_log_poll(poll_count: u64) -> bool {
    poll_count == 0 || poll_count.checked_rem(12) == Some(0)
}

fn normalize_model_resource(model: &str) -> String {
    if model.starts_with("models/") {
        model.to_string()
    } else {
        format!("models/{model}")
    }
}

fn processor_http_status(status: u16, response_text: &str, headers: &HeaderMap) -> ProcessorError {
    ProcessorError::InferenceHttpStatus {
        status,
        body_preview: preview(response_text),
        retry_after_seconds: parse_retry_after_seconds(headers)
            .or_else(|| parse_google_retry_delay_seconds(response_text)),
    }
}

fn parse_retry_after_seconds(headers: &HeaderMap) -> Option<i64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<i64>() {
        return Some(seconds.max(1));
    }

    httpdate::parse_http_date(value)
        .ok()
        .and_then(|when| when.duration_since(SystemTime::now()).ok())
        .and_then(|delay| i64::try_from(delay.as_secs()).ok())
        .map(|seconds| seconds.max(1))
}

fn parse_google_retry_delay_seconds(response_text: &str) -> Option<i64> {
    let parsed: serde_json::Value = serde_json::from_str(response_text).ok()?;
    let details = parsed.pointer("/error/details")?.as_array()?;
    for detail in details {
        if detail.get("@type").and_then(serde_json::Value::as_str)
            != Some("type.googleapis.com/google.rpc.RetryInfo")
        {
            continue;
        }

        if let Some(delay) = detail.get("retryDelay").and_then(serde_json::Value::as_str) {
            return parse_google_duration_seconds(delay);
        }
    }
    None
}

fn parse_google_duration_seconds(value: &str) -> Option<i64> {
    let trimmed = value.trim().strip_suffix('s')?;
    let seconds = trimmed.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Some(seconds.ceil() as i64)
}
