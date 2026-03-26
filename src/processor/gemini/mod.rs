use std::sync::Arc;
use std::thread;
use std::time::Duration;

use log::info;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};

use crate::config::GeminiConfig;
use crate::storage::types::EnrichmentTier;

use super::*;

mod batch;
mod types;

use batch::{
    GeminiBatchProcessor, GeminiExtractionSubmitter, GeminiReconciliationBatchProcessor,
    GeminiReconciliationSubmitter,
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
    extract_max_output_tokens: u32,
    extract_repair_max_output_tokens: u32,
    standard_model: String,
    quality_model: String,
    reconcile_standard_model: String,
    reconcile_quality_model: String,
}

impl GeminiProcessorFactory {
    pub fn new(config: GeminiConfig) -> Result<Self, String> {
        let client = GeminiClient::new(&config).map_err(|err| err.to_string())?;
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
            client: Arc::new(client),
            batch_client: if config.batch_enabled {
                Some(Arc::new(
                    GeminiBatchClient::new(&config).map_err(|err| err.to_string())?,
                ))
            } else {
                None
            },
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
}

impl ArtifactProcessorFactory for GeminiProcessorFactory {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(GeminiArtifactProcessor {
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

    fn build_batch_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ArtifactBatchProcessor>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(GeminiBatchProcessor {
            client: Arc::clone(batch_client),
            candidate_model: model,
        })))
    }

    fn build_reconciliation_batch_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchProcessor>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.reconcile_standard_model.clone(),
            EnrichmentTier::Quality => self.reconcile_quality_model.clone(),
        };
        Ok(Some(Box::new(GeminiReconciliationBatchProcessor {
            client: Arc::clone(batch_client),
            model,
        })))
    }

    fn build_extraction_submitter(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ExtractionBatchSubmitter>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(GeminiExtractionSubmitter {
            client: Arc::clone(batch_client),
            candidate_model: model,
        })))
    }

    fn build_reconciliation_submitter(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchSubmitter>>, ProcessorError> {
        let Some(batch_client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.reconcile_standard_model.clone(),
            EnrichmentTier::Quality => self.reconcile_quality_model.clone(),
        };
        Ok(Some(Box::new(GeminiReconciliationSubmitter {
            client: Arc::clone(batch_client),
            model,
        })))
    }
}

struct GeminiArtifactProcessor {
    client: Arc<GeminiClient>,
    candidate_model: String,
    max_output_tokens: u32,
    repair_max_output_tokens: u32,
}

impl ArtifactProcessor for GeminiArtifactProcessor {
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

impl GeminiArtifactProcessor {
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
            "gemini_enrichment",
            "gemini",
            GEMINI_PROMPT_VERSION,
        ))
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
                response_mime_type: "application/json".to_string(),
                response_schema: normalize_gemini_response_schema(schema),
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
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(ProcessorError::InferenceHttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
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
                            if first.is_some()
                                && second.is_none()
                                && values.iter().any(|value| value.as_str() == Some("null"))
                            {
                                sanitized
                                    .insert("nullable".to_string(), serde_json::Value::Bool(true));
                                serde_json::Value::String(
                                    first.expect("checked is_some").to_ascii_uppercase(),
                                )
                            } else {
                                sanitize_gemini_schema_value(child)
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
                                        response_mime_type: "application/json".to_string(),
                                        response_schema: normalize_gemini_response_schema(
                                            &request.response_json_schema,
                                        ),
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
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(ProcessorError::InferenceHttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
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
        let response_text = response
            .text()
            .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;

        if !status.is_success() {
            return Err(ProcessorError::InferenceHttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
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
                    if poll_count == 0 || poll_count % 12 == 0 {
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
            if poll_count == 0 || poll_count % 12 == 0 {
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

fn normalize_model_resource(model: &str) -> String {
    if model.starts_with("models/") {
        model.to_string()
    } else {
        format!("models/{model}")
    }
}
