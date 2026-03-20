use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use log::info;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::GeminiConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    build_conversation_user_prompt, build_reconciliation_prompt, reconciliation_output_schema,
    should_shape_conversation_input, structured_output_schema_with_allowed_refs,
    structured_output_schema_wrapper, ArtifactBatchProcessor, ArtifactProcessor,
    ArtifactProcessorFactory, ArtifactProcessorInput, ArtifactProcessorOutput, BatchHandle,
    BatchPollResult, ExtractionBatchSubmitter, HostedReconciliationProcessor, InferenceClient,
    InferenceResult, InferenceUsage, ProcessorError, ReconciliationBatchProcessor,
    ReconciliationBatchSubmitter, ReconciliationProcessor, ReconciliationProcessorInput,
    GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT, RECONCILIATION_SYSTEM_PROMPT,
};

pub struct GeminiProcessorFactory {
    client: Arc<GeminiClient>,
    batch_client: Option<Arc<GeminiBatchClient>>,
    extract_max_output_tokens: u32,
    extract_repair_max_output_tokens: u32,
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
            model,
            max_output_tokens: self.extract_max_output_tokens,
            repair_max_output_tokens: self.extract_repair_max_output_tokens,
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
            model,
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
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
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
            model,
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
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(GeminiReconciliationSubmitter {
            client: Arc::clone(batch_client),
            model,
        })))
    }
}

struct GeminiBatchProcessor {
    client: Arc<GeminiBatchClient>,
    model: String,
}

struct GeminiArtifactProcessor {
    client: Arc<GeminiClient>,
    model: String,
    max_output_tokens: u32,
    repair_max_output_tokens: u32,
}

struct GeminiReconciliationBatchProcessor {
    client: Arc<GeminiBatchClient>,
    model: String,
}

impl ArtifactProcessor for GeminiArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        super::validate_input(input)?;
        let prompt = build_conversation_user_prompt(input)?;
        match self.process_once(input, &prompt, self.max_output_tokens) {
            Ok(output) => Ok(output),
            Err(error) if super::should_retry_with_repair(&error) => {
                let repair_prompt = super::build_repair_prompt(&prompt, &error);
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
        let inference_result = self.client.complete_json_with_max_output_tokens(
            &self.model,
            GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
            user_prompt,
            &structured_output_schema_wrapper(input),
            max_output_tokens,
        )?;
        let parsed: super::ModelArtifactOutput =
            serde_json::from_str(&inference_result.output_text).map_err(|source| {
                ProcessorError::ParseModelJson {
                    source,
                    body_preview: super::preview(&inference_result.output_text),
                }
            })?;
        let parsed = parsed.resolve_evidence_aliases(input);
        let parsed = parsed
            .validate_and_salvage(input)
            .map_err(|err| super::attach_output_preview(err, &inference_result.output_text))?;
        Ok(parsed.into_processor_output(
            self.model.clone(),
            inference_result.usage,
            "gemini_enrichment",
            "gemini",
            super::GEMINI_PROMPT_VERSION,
        ))
    }
}

impl ArtifactBatchProcessor for GeminiBatchProcessor {
    fn max_batch_jobs(&self) -> usize {
        self.client.max_batch_jobs
    }

    fn max_batch_bytes(&self) -> usize {
        self.client.batch_max_bytes
    }

    fn can_process(&self, input: &ArtifactProcessorInput) -> bool {
        !should_shape_conversation_input(input)
    }

    fn estimate_size_bytes(&self, input: &ArtifactProcessorInput) -> Result<usize, ProcessorError> {
        let user_prompt = build_conversation_user_prompt(input)?;
        Ok(GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT.len() + user_prompt.len())
    }

    fn process_batch(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
        if inputs.is_empty() {
            return Vec::new();
        }

        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            match build_conversation_user_prompt(input) {
                Ok(user_prompt) => requests.push(GeminiBatchEnrichmentRequest {
                    key: input.artifact_id.clone(),
                    system_prompt: GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT.to_string(),
                    user_prompt,
                    response_json_schema: structured_output_schema_with_allowed_refs(
                        &input
                            .segments
                            .iter()
                            .enumerate()
                            .map(|(index, _)| format!("evidence_ref_{}", index + 1))
                            .collect::<Vec<_>>(),
                    ),
                    max_output_tokens: None,
                }),
                Err(err) => {
                    return inputs
                        .iter()
                        .map(|_| {
                            Err(ProcessorError::Message {
                                message: err.to_string(),
                            })
                        })
                        .collect()
                }
            }
        }

        let job = match self.client.submit_inline_batch(
            &self.model,
            Some("openarchive-enrichment"),
            &requests,
        ) {
            Ok(job) => job,
            Err(err) => {
                let message = err.to_string();
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        };

        let completed = match self.client.wait_for_batch(&job.name) {
            Ok(job) => job,
            Err(err) => {
                let message = err.to_string();
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        };

        match completed.inline_results() {
            Ok(results) => results
                .into_iter()
                .map(|result| {
                    let parsed: super::ModelArtifactOutput =
                        serde_json::from_str(&result.output_text).map_err(|source| {
                            ProcessorError::ParseModelJson {
                                source,
                                body_preview: super::preview(&result.output_text),
                            }
                        })?;
                    let input = inputs
                        .iter()
                        .find(|input| input.artifact_id == result.key)
                        .ok_or_else(|| ProcessorError::Message {
                            message: format!("Gemini batch returned unknown key {}", result.key),
                        })?;
                    let parsed = parsed.resolve_evidence_aliases(input);
                    let parsed = parsed
                        .validate_and_salvage(input)
                        .map_err(|err| super::attach_output_preview(err, &result.output_text))?;
                    Ok(parsed.into_processor_output(
                        self.model.clone(),
                        result.usage,
                        "gemini_enrichment",
                        "gemini",
                        super::GEMINI_PROMPT_VERSION,
                    ))
                })
                .collect(),
            Err(err) => {
                let message = err.to_string();
                inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect()
            }
        }
    }
}

impl ReconciliationBatchProcessor for GeminiReconciliationBatchProcessor {
    fn max_batch_jobs(&self) -> usize {
        self.client.max_batch_jobs
    }

    fn max_batch_bytes(&self) -> usize {
        self.client.batch_max_bytes
    }

    fn estimate_size_bytes(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<usize, ProcessorError> {
        let user_prompt = build_reconciliation_prompt(input)?;
        Ok(RECONCILIATION_SYSTEM_PROMPT.len() + user_prompt.len())
    }

    fn process_batch(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<super::ReconciliationDecisionOutput>, ProcessorError>> {
        if inputs.is_empty() {
            return Vec::new();
        }

        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            match build_reconciliation_prompt(input) {
                Ok(user_prompt) => requests.push(GeminiBatchEnrichmentRequest {
                    key: input.artifact_id.clone(),
                    system_prompt: RECONCILIATION_SYSTEM_PROMPT.to_string(),
                    user_prompt,
                    response_json_schema: reconciliation_output_schema(),
                    max_output_tokens: None,
                }),
                Err(err) => {
                    return inputs
                        .iter()
                        .map(|_| {
                            Err(ProcessorError::Message {
                                message: err.to_string(),
                            })
                        })
                        .collect();
                }
            }
        }

        let job = match self.client.submit_inline_batch(
            &self.model,
            Some("openarchive-reconciliation"),
            &requests,
        ) {
            Ok(job) => job,
            Err(err) => {
                let message = err.to_string();
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        };

        let completed = match self.client.wait_for_batch(&job.name) {
            Ok(job) => job,
            Err(err) => {
                let message = err.to_string();
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        };

        match completed.inline_results() {
            Ok(results) => results
                .into_iter()
                .map(|result| {
                    let parsed: super::ModelReconciliationOutput =
                        serde_json::from_str(&result.output_text).map_err(|source| {
                            ProcessorError::ParseModelJson {
                                source,
                                body_preview: super::preview(&result.output_text),
                            }
                        })?;
                    let input = inputs
                        .iter()
                        .find(|input| input.artifact_id == result.key)
                        .ok_or_else(|| ProcessorError::Message {
                            message: format!("Gemini batch returned unknown key {}", result.key),
                        })?;
                    parsed.validate_against(input)?;
                    Ok(parsed.into_outputs())
                })
                .collect(),
            Err(err) => {
                let message = err.to_string();
                inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Non-blocking batch submitter structs
// ---------------------------------------------------------------------------

struct GeminiExtractionSubmitter {
    client: Arc<GeminiBatchClient>,
    model: String,
}

struct GeminiReconciliationSubmitter {
    client: Arc<GeminiBatchClient>,
    model: String,
}

// ---------------------------------------------------------------------------
// ExtractionBatchSubmitter
// ---------------------------------------------------------------------------

impl ExtractionBatchSubmitter for GeminiExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        self.client.max_batch_jobs
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt = build_conversation_user_prompt(input)?;
            requests.push(GeminiBatchEnrichmentRequest {
                key: super::artifact_processor_batch_custom_id(input),
                system_prompt: GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT.to_string(),
                user_prompt,
                response_json_schema: structured_output_schema_with_allowed_refs(
                    &input
                        .segments
                        .iter()
                        .enumerate()
                        .map(|(index, _)| format!("evidence_ref_{}", index + 1))
                        .collect::<Vec<_>>(),
                ),
                max_output_tokens: None,
            });
        }

        let job = self.client.submit_inline_batch(
            &self.model,
            Some("openarchive-enrichment"),
            &requests,
        )?;

        Ok(BatchHandle {
            batch_id: job.name,
            provider: "gemini".to_string(),
            submitted_at: Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        let job = self.client.get_batch(&handle.batch_id)?;
        let state = job.state.as_deref().unwrap_or_default();

        if matches!(
            state,
            "JOB_STATE_SUCCEEDED" | "SUCCEEDED" | "BATCH_STATE_SUCCEEDED"
        ) {
            return Ok(BatchPollResult::Succeeded(Box::new(job)));
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
            return Ok(BatchPollResult::Failed(format!(
                "Gemini batch {} finished in terminal state {state}",
                handle.batch_id
            )));
        }
        Ok(BatchPollResult::Pending)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
        let job = match completed.downcast::<GeminiBatchJob>() {
            Ok(job) => *job,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast extraction batch result to GeminiBatchJob"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };

        let results = match job.inline_results() {
            Ok(results) => results,
            Err(err) => {
                let message = err.to_string();
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        };

        let mut by_key = HashMap::new();
        for result in results {
            by_key.insert(result.key.clone(), result);
        }

        inputs
            .iter()
            .map(|input| {
                let key = super::artifact_processor_batch_custom_id(input);
                let result = by_key.remove(&key).ok_or_else(|| ProcessorError::Message {
                    message: format!("Gemini batch returned no result for key {}", key),
                })?;
                let parsed: super::ModelArtifactOutput = serde_json::from_str(&result.output_text)
                    .map_err(|source| ProcessorError::ParseModelJson {
                        source,
                        body_preview: super::preview(&result.output_text),
                    })?;
                let parsed = parsed.resolve_evidence_aliases(input);
                let parsed = parsed
                    .validate_and_salvage(input)
                    .map_err(|err| super::attach_output_preview(err, &result.output_text))?;
                Ok(parsed.into_processor_output(
                    self.model.clone(),
                    result.usage,
                    "gemini_enrichment",
                    "gemini",
                    super::GEMINI_PROMPT_VERSION,
                ))
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// ReconciliationBatchSubmitter
// ---------------------------------------------------------------------------

impl ReconciliationBatchSubmitter for GeminiReconciliationSubmitter {
    fn max_batch_size(&self) -> usize {
        self.client.max_batch_jobs
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt = build_reconciliation_prompt(input)?;
            requests.push(GeminiBatchEnrichmentRequest {
                key: input.artifact_id.clone(),
                system_prompt: RECONCILIATION_SYSTEM_PROMPT.to_string(),
                user_prompt,
                response_json_schema: reconciliation_output_schema(),
                max_output_tokens: None,
            });
        }

        let job = self.client.submit_inline_batch(
            &self.model,
            Some("openarchive-reconciliation"),
            &requests,
        )?;

        Ok(BatchHandle {
            batch_id: job.name,
            provider: "gemini".to_string(),
            submitted_at: Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        let job = self.client.get_batch(&handle.batch_id)?;
        let state = job.state.as_deref().unwrap_or_default();

        if matches!(
            state,
            "JOB_STATE_SUCCEEDED" | "SUCCEEDED" | "BATCH_STATE_SUCCEEDED"
        ) {
            return Ok(BatchPollResult::Succeeded(Box::new(job)));
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
            return Ok(BatchPollResult::Failed(format!(
                "Gemini batch {} finished in terminal state {state}",
                handle.batch_id
            )));
        }
        Ok(BatchPollResult::Pending)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<super::ReconciliationDecisionOutput>, ProcessorError>> {
        let job = match completed.downcast::<GeminiBatchJob>() {
            Ok(job) => *job,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message:
                                "failed to downcast reconciliation batch result to GeminiBatchJob"
                                    .to_string(),
                        })
                    })
                    .collect();
            }
        };

        let results = match job.inline_results() {
            Ok(results) => results,
            Err(err) => {
                let message = err.to_string();
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        };

        results
            .into_iter()
            .map(|result| {
                let parsed: super::ModelReconciliationOutput =
                    serde_json::from_str(&result.output_text).map_err(|source| {
                        ProcessorError::ParseModelJson {
                            source,
                            body_preview: super::preview(&result.output_text),
                        }
                    })?;
                let input = inputs
                    .iter()
                    .find(|input| input.artifact_id == result.key)
                    .ok_or_else(|| ProcessorError::Message {
                        message: format!("Gemini batch returned unknown key {}", result.key),
                    })?;
                parsed.validate_against(input)?;
                Ok(parsed.into_outputs())
            })
            .collect()
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

#[derive(Debug, Clone)]
pub struct GeminiBatchEnrichmentRequest {
    pub key: String,
    pub system_prompt: String,
    pub user_prompt: String,
    pub response_json_schema: serde_json::Value,
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
    #[serde(default)]
    pub dest: Option<GeminiBatchDestination>,
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
                body_preview: super::preview(&response_text),
            });
        }

        let operation: GeminiBatchOperation =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: super::preview(&response_text),
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
                body_preview: super::preview(&response_text),
            });
        }

        let operation: GeminiBatchOperation =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: super::preview(&response_text),
                }
            })?;
        Ok(operation.into_batch_job())
    }

    pub fn wait_for_batch(&self, name: &str) -> Result<GeminiBatchJob, ProcessorError> {
        let mut poll_count: u64 = 0;
        loop {
            let job = self.get_batch(name)?;
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
    response_schema: serde_json::Value,
    max_output_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiResponseContent>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiResponseContent {
    #[serde(default)]
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Clone, Deserialize)]
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
    batch: GeminiBatchDefinition,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    input_config: GeminiBatchInputConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchInputConfig {
    requests: GeminiInlineBatchRequests,
}

#[derive(Debug, Serialize)]
struct GeminiInlineBatchRequests {
    requests: Vec<GeminiBatchRequestEntry>,
}

#[derive(Debug, Serialize)]
struct GeminiBatchRequestEntry {
    request: GeminiGenerateContentRequest,
    metadata: GeminiBatchRequestMetadata,
}

#[derive(Debug, Serialize)]
struct GeminiBatchRequestMetadata {
    key: String,
}

#[derive(Debug, Clone)]
pub struct GeminiBatchResult {
    pub key: String,
    pub output_text: String,
    pub usage: Option<InferenceUsage>,
}

impl GeminiBatchJob {
    pub fn inline_results(&self) -> Result<Vec<GeminiBatchResult>, ProcessorError> {
        let response_group = self
            .dest
            .as_ref()
            .and_then(|dest| dest.inlined_responses.as_ref())
            .ok_or_else(|| ProcessorError::Message {
                message: "Gemini batch completed without inline responses".to_string(),
            })?;
        let responses = if !response_group.responses.is_empty() {
            &response_group.responses
        } else {
            &response_group.inline_entries
        };

        responses
            .iter()
            .map(|entry| {
                let key = entry
                    .key
                    .clone()
                    .or_else(|| {
                        entry
                            .metadata
                            .as_ref()
                            .and_then(|metadata| metadata.key.clone())
                    })
                    .ok_or_else(|| ProcessorError::Message {
                        message: "Gemini batch response missing request key".to_string(),
                    })?;
                let response = entry
                    .response
                    .as_ref()
                    .ok_or_else(|| ProcessorError::Message {
                        message: format!("Gemini batch response {key} missing output"),
                    })?;
                let output_text =
                    response
                        .flatten_text()
                        .ok_or_else(|| ProcessorError::Message {
                            message: format!("Gemini batch response {key} returned empty content"),
                        })?;
                Ok(GeminiBatchResult {
                    key,
                    output_text,
                    usage: response
                        .usage_metadata
                        .clone()
                        .and_then(InferenceUsage::from_gemini_usage),
                })
            })
            .collect()
    }

    pub fn request_count(&self) -> usize {
        self.dest
            .as_ref()
            .and_then(|dest| dest.inlined_responses.as_ref())
            .map(|responses| {
                if !responses.responses.is_empty() {
                    responses.responses.len()
                } else {
                    responses.inline_entries.len()
                }
            })
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiBatchDestination {
    #[serde(default, rename = "inlinedResponses")]
    pub inlined_responses: Option<GeminiInlinedResponses>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiInlinedResponses {
    #[serde(default)]
    pub responses: Vec<GeminiInlinedResponseEntry>,
    #[serde(default, rename = "inlinedResponses")]
    pub inline_entries: Vec<GeminiInlinedResponseEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiInlinedResponseEntry {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub metadata: Option<GeminiInlinedResponseMetadata>,
    #[serde(default)]
    response: Option<GeminiGenerateContentResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiInlinedResponseMetadata {
    #[serde(default)]
    pub key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchOperation {
    name: String,
    #[serde(default)]
    metadata: Option<GeminiBatchOperationMetadata>,
    #[serde(default)]
    response: Option<GeminiBatchOperationResponse>,
}

impl GeminiBatchOperation {
    fn into_batch_job(self) -> GeminiBatchJob {
        GeminiBatchJob {
            name: self.name,
            state: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.state.clone()),
            display_name: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.display_name.clone()),
            model: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.model.clone()),
            create_time: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.create_time.clone()),
            update_time: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.update_time.clone()),
            dest: self.response.map(|response| GeminiBatchDestination {
                inlined_responses: response
                    .dest
                    .and_then(|dest| dest.inlined_responses)
                    .or(response.inlined_responses),
            }),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchOperationMetadata {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    create_time: Option<String>,
    #[serde(default)]
    update_time: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiBatchOperationResponse {
    #[serde(default)]
    dest: Option<GeminiBatchDestination>,
    #[serde(default, rename = "inlinedResponses")]
    inlined_responses: Option<GeminiInlinedResponses>,
}
