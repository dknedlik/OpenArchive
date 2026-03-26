use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::GrokConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    allowed_artifact_evidence_refs, build_two_phase_candidate_user_prompt,
    candidate_output_schema_with_allowed_refs, candidate_output_schema_wrapper,
    candidate_system_prompt, parse_candidate_output, ArtifactProcessor, ArtifactProcessorFactory,
    ArtifactProcessorInput, ArtifactProcessorOutput, BatchHandle, BatchPollResult,
    ExtractionBatchSubmitter, HostedReconciliationProcessor, InferenceClient, InferenceResult,
    InferenceUsage, OpenRouterResponsesContentItem, OpenRouterResponsesInputItem,
    OpenRouterResponsesRequest, OpenRouterResponsesResponse, OpenRouterResponsesTextConfig,
    ProcessorError, ReconciliationBatchSubmitter, ReconciliationProcessor,
    RECONCILIATION_SYSTEM_PROMPT, TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS,
};

pub struct GrokProcessorFactory {
    client: Arc<GrokClient>,
    batch_client: Option<Arc<GrokClient>>,
    extract_max_output_tokens: u32,
    extract_repair_max_output_tokens: u32,
    standard_model: String,
    quality_model: String,
    reconcile_standard_model: String,
    reconcile_quality_model: String,
}

impl GrokProcessorFactory {
    pub fn new(config: GrokConfig) -> Result<Self, String> {
        let client = Arc::new(GrokClient::new(&config).map_err(|err| err.to_string())?);
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

impl ArtifactProcessorFactory for GrokProcessorFactory {
    fn build(&self, tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };

        Ok(Box::new(GrokArtifactProcessor {
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
        Ok(Some(Box::new(GrokExtractionSubmitter {
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
        Ok(Some(Box::new(GrokReconciliationSubmitter {
            client: Arc::clone(client),
            model,
        })))
    }
}

struct GrokClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
}

struct GrokArtifactProcessor {
    client: Arc<GrokClient>,
    candidate_model: String,
    max_output_tokens: u32,
    repair_max_output_tokens: u32,
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
        self.complete_json_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            self.max_output_tokens,
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

    fn create_batch(&self, name: &str) -> Result<GrokBatch, ProcessorError> {
        let body = serde_json::json!({ "name": name });
        let response = self
            .client
            .post(format!("{}/batches", self.base_url))
            .body(
                serde_json::to_vec(&body)
                    .map_err(|source| ProcessorError::SerializePrompt { source })?,
            )
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;
        parse_grok_json_response(response)
    }

    fn add_batch_requests(
        &self,
        batch_id: &str,
        requests: &[GrokBatchRequestEnvelope],
    ) -> Result<(), ProcessorError> {
        let body = serde_json::json!({ "batch_requests": requests });
        let response = self
            .client
            .post(format!("{}/batches/{}/requests", self.base_url, batch_id))
            .body(
                serde_json::to_vec(&body)
                    .map_err(|source| ProcessorError::SerializePrompt { source })?,
            )
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;
        let _: serde_json::Value = parse_grok_json_response(response)?;
        Ok(())
    }

    fn get_batch(&self, batch_id: &str) -> Result<GrokBatch, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/batches/{}", self.base_url, batch_id))
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;
        parse_grok_json_response(response)
    }

    #[allow(dead_code)]
    fn wait_for_batch(&self, batch_id: &str) -> Result<GrokBatch, ProcessorError> {
        let mut poll_count: u64 = 0;
        loop {
            let batch = match self.get_batch(batch_id) {
                Ok(batch) => batch,
                Err(err) if err.is_retryable() => {
                    if poll_count == 0 || poll_count % 12 == 0 {
                        log::warn!(
                            "grok batch id={} poll={} transient poll error={}",
                            batch_id,
                            poll_count,
                            err
                        );
                    }
                    poll_count += 1;
                    std::thread::sleep(std::time::Duration::from_secs(5));
                    continue;
                }
                Err(err) => return Err(err),
            };
            if batch.state.num_pending == 0 {
                return Ok(batch);
            }
            poll_count += 1;
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    }

    fn list_results(&self, batch_id: &str) -> Result<Vec<GrokBatchResultItem>, ProcessorError> {
        let mut all = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .get(format!("{}/batches/{}/results", self.base_url, batch_id));
            if let Some(token) = &page_token {
                request = request.query(&[("page_token", token)]);
            }
            let page: GrokBatchResultsPage = parse_grok_json_response(
                request
                    .send()
                    .map_err(|source| ProcessorError::SendInferenceRequest { source })?,
            )?;
            all.extend(page.results);
            match page.next_page_token {
                Some(token) if !token.is_empty() => page_token = Some(token),
                _ => break,
            }
        }
        Ok(all)
    }

    fn build_chat_completion_body(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> serde_json::Value {
        serde_json::json!({
            "model": model,
            "max_output_tokens": max_output_tokens,
            "messages": [
                { "role": "system", "content": system_prompt },
                { "role": "user", "content": user_prompt }
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "openarchive_result",
                    "schema": schema.get("schema").cloned().unwrap_or_else(|| schema.clone()),
                    "strict": true
                }
            }
        })
    }
}

impl ArtifactProcessor for GrokArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        super::validate_input(input)?;
        let prompt = build_two_phase_candidate_user_prompt(input)?;
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

impl GrokArtifactProcessor {
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
            .map_err(|err| super::attach_output_preview(err, &candidate_result.output_text))?;
        Ok(candidate.into_processor_output(
            input,
            self.candidate_model.clone(),
            candidate_result.usage,
            "grok_enrichment",
            "grok",
            super::GROK_PROMPT_VERSION,
        ))
    }
}

struct GrokExtractionSubmitter {
    client: Arc<GrokClient>,
    candidate_model: String,
}

struct GrokReconciliationSubmitter {
    client: Arc<GrokClient>,
    model: String,
}

impl ExtractionBatchSubmitter for GrokExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[super::ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let batch = self.client.create_batch("openarchive-enrichment")?;
        let requests = build_grok_requests(inputs, &self.candidate_model, |input| {
            Ok(self.client.build_chat_completion_body(
                &self.candidate_model,
                candidate_system_prompt(input),
                &build_two_phase_candidate_user_prompt(input)?,
                &candidate_output_schema_with_allowed_refs(&allowed_artifact_evidence_refs(input)),
                self.client
                    .max_output_tokens
                    .max(TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS),
            ))
        })?;
        self.client.add_batch_requests(&batch.id, &requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "grok".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        let batch = self.client.get_batch(&handle.batch_id)?;
        if batch.state.num_pending == 0 {
            return Ok(BatchPollResult::Succeeded(Box::new(batch)));
        }
        Ok(BatchPollResult::Pending)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::ArtifactProcessorInput],
    ) -> Vec<Result<super::ArtifactProcessorOutput, ProcessorError>> {
        let batch = match completed.downcast::<GrokBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast Grok extraction batch result".to_string(),
                        })
                    })
                    .collect();
            }
        };
        let candidate_results =
            parse_grok_results(&self.client, &batch.id, inputs, |result, input| {
                let candidate = parse_candidate_output(&result.output_text, input)
                    .map_err(|err| super::attach_output_preview(err, &result.output_text))?;
                Ok((candidate, result.usage))
            });
        if candidate_results.iter().any(Result::is_err) {
            return candidate_results
                .into_iter()
                .map(|result| {
                    result
                        .map(|_| unreachable!("candidate error already checked"))
                        .map_err(|err| ProcessorError::Message {
                            message: err.to_string(),
                        })
                })
                .collect();
        }

        candidate_results
            .into_iter()
            .zip(inputs.iter())
            .map(|(result, input)| match result {
                Ok((candidate, usage)) => Ok(candidate.into_processor_output(
                    input,
                    self.candidate_model.clone(),
                    usage,
                    "grok_enrichment",
                    "grok",
                    super::GROK_PROMPT_VERSION,
                )),
                Err(err) => Err(ProcessorError::Message {
                    message: err.to_string(),
                }),
            })
            .collect()
    }
}

impl ReconciliationBatchSubmitter for GrokReconciliationSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[super::ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let batch = self.client.create_batch("openarchive-reconciliation")?;
        let requests = build_grok_requests(inputs, &self.model, |input| {
            Ok(self.client.build_chat_completion_body(
                &self.model,
                RECONCILIATION_SYSTEM_PROMPT,
                &super::build_reconciliation_prompt(input)?,
                &super::reconciliation_output_schema(),
                self.client.max_output_tokens,
            ))
        })?;
        self.client.add_batch_requests(&batch.id, &requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "grok".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        GrokExtractionSubmitter {
            client: Arc::clone(&self.client),
            candidate_model: self.model.clone(),
        }
        .poll_batch(handle)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<super::ReconciliationDecisionOutput>, ProcessorError>> {
        let batch = match completed.downcast::<GrokBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast Grok reconciliation batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        parse_grok_results(&self.client, &batch.id, inputs, |result, input| {
            let parsed: super::ModelReconciliationOutput =
                serde_json::from_str(&result.output_text).map_err(|source| {
                    ProcessorError::ParseModelJson {
                        source,
                        body_preview: super::preview(&result.output_text),
                    }
                })?;
            parsed.validate_against(input)?;
            Ok(parsed.into_outputs())
        })
    }
}

fn build_grok_requests<I, F>(
    inputs: &[I],
    _model: &str,
    mut build: F,
) -> Result<Vec<GrokBatchRequestEnvelope>, ProcessorError>
where
    I: GrokBatchInput,
    F: FnMut(&I) -> Result<serde_json::Value, ProcessorError>,
{
    let mut requests = Vec::with_capacity(inputs.len());
    for input in inputs {
        requests.push(GrokBatchRequestEnvelope {
            batch_request_id: input.batch_custom_id(),
            batch_request: GrokBatchRequestBody {
                chat_get_completion: build(input)?,
            },
        });
    }
    Ok(requests)
}

fn parse_grok_results<I, O, F>(
    client: &GrokClient,
    batch_id: &str,
    inputs: &[I],
    mut parse: F,
) -> Vec<Result<O, ProcessorError>>
where
    I: GrokBatchInput,
    F: FnMut(GrokBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let mut by_id = std::collections::HashMap::new();
    let items = match client.list_results(batch_id) {
        Ok(items) => items,
        Err(err) => {
            return inputs
                .iter()
                .map(|_| Err(grok_message_error(&err)))
                .collect()
        }
    };
    for item in items {
        let parsed = if let Some(error) = item.error_message() {
            if is_retryable_grok_batch_error(&error) {
                Err(ProcessorError::InferenceHttpStatus {
                    status: 503,
                    body_preview: format!(
                        "Grok batch item {} retryable error: {}",
                        item.batch_request_id, error
                    ),
                })
            } else {
                Err(ProcessorError::Message {
                    message: format!(
                        "Grok batch item {} failed: {}",
                        item.batch_request_id, error
                    ),
                })
            }
        } else {
            item.chat_completion_response()
                .ok_or_else(|| ProcessorError::InferenceHttpStatus {
                    status: 503,
                    body_preview: format!(
                        "Grok batch item {} missing response payload: {}",
                        item.batch_request_id,
                        super::preview(&item.debug_snapshot())
                    ),
                })
                .and_then(|response_value| {
                    Ok(GrokBatchParsedResult {
                        output_text: flatten_grok_response_text(response_value)?,
                        usage: extract_grok_usage(response_value),
                    })
                })
        };
        by_id.insert(item.batch_request_id, parsed);
    }
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let custom_id = input.batch_custom_id();
        match by_id.remove(&custom_id) {
            Some(Ok(item)) => outputs.push(parse(item, input)),
            Some(Err(err)) => outputs.push(Err(err)),
            None => outputs.push(Err(ProcessorError::Message {
                message: format!("Grok batch missing result for {}", custom_id),
            })),
        }
    }
    outputs
}

fn flatten_grok_response_text(value: &serde_json::Value) -> Result<String, ProcessorError> {
    if let Some(content) = value.get("content").and_then(|v| v.as_str()) {
        return Ok(content.to_string());
    }
    if let Some(output_text) = value.get("output_text").and_then(|v| v.as_str()) {
        return Ok(output_text.to_string());
    }
    if let Some(text) = value
        .get("choices")
        .and_then(|v| v.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
    {
        if let Some(content) = text.as_str() {
            return Ok(content.to_string());
        }
    }
    Err(ProcessorError::Message {
        message: "Grok batch response contained no parseable text content".to_string(),
    })
}

fn extract_grok_usage(value: &serde_json::Value) -> Option<InferenceUsage> {
    value
        .get("usage")
        .cloned()
        .and_then(|usage| serde_json::from_value::<super::OpenRouterUsage>(usage).ok())
        .and_then(InferenceUsage::from_openrouter_usage)
}

fn is_retryable_grok_batch_error(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("temporarily unavailable")
        || lowered.contains("at capacity")
        || lowered.contains("try again later")
        || lowered.contains("service unavailable")
}

trait GrokBatchInput {
    fn batch_custom_id(&self) -> String;
}

impl GrokBatchInput for super::ArtifactProcessorInput {
    fn batch_custom_id(&self) -> String {
        super::artifact_processor_batch_custom_id(self)
    }
}

impl GrokBatchInput for super::ReconciliationProcessorInput {
    fn batch_custom_id(&self) -> String {
        self.artifact_id.clone()
    }
}

struct GrokBatchParsedResult {
    output_text: String,
    usage: Option<InferenceUsage>,
}

fn grok_message_error(err: &ProcessorError) -> ProcessorError {
    ProcessorError::Message {
        message: err.to_string(),
    }
}

fn parse_grok_json_response<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> Result<T, ProcessorError> {
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

#[derive(Debug, Deserialize)]
struct GrokBatch {
    #[serde(alias = "batch_id")]
    id: String,
    state: GrokBatchState,
}

#[derive(Debug, Deserialize)]
struct GrokBatchState {
    #[serde(default)]
    num_pending: usize,
}

#[derive(Debug, Serialize)]
struct GrokBatchRequestEnvelope {
    batch_request_id: String,
    batch_request: GrokBatchRequestBody,
}

#[derive(Debug, Serialize)]
struct GrokBatchRequestBody {
    chat_get_completion: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GrokBatchResultsPage {
    #[serde(default)]
    results: Vec<GrokBatchResultItem>,
    #[serde(default, alias = "pagination_token")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct GrokBatchResultItem {
    batch_request_id: String,
    #[serde(default)]
    response: Option<serde_json::Value>,
    #[serde(default)]
    batch_result: Option<GrokBatchResultEnvelope>,
    #[serde(default)]
    error_message: Option<String>,
}

impl GrokBatchResultItem {
    fn error_message(&self) -> Option<String> {
        self.error_message
            .clone()
            .or_else(|| {
                self.response
                    .as_ref()
                    .and_then(extract_grok_batch_error_message)
            })
            .or_else(|| {
                self.batch_result
                    .as_ref()
                    .and_then(GrokBatchResultEnvelope::error_message)
            })
    }

    fn chat_completion_response(&self) -> Option<&serde_json::Value> {
        self.response.as_ref().or_else(|| {
            self.batch_result
                .as_ref()
                .and_then(|result| result.response.as_ref())
                .and_then(|response| response.get("chat_get_completion"))
        })
    }

    fn debug_snapshot(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| self.batch_request_id.clone())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct GrokBatchResultEnvelope {
    #[serde(default)]
    response: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

impl GrokBatchResultEnvelope {
    fn error_message(&self) -> Option<String> {
        self.error
            .as_ref()
            .and_then(extract_grok_batch_error_message)
            .or_else(|| {
                self.response
                    .as_ref()
                    .and_then(extract_grok_batch_error_message)
            })
    }
}

fn extract_grok_batch_error_message(value: &serde_json::Value) -> Option<String> {
    value
        .get("error")
        .and_then(|error| {
            error.as_str().map(|s| s.to_string()).or_else(|| {
                error
                    .get("message")
                    .and_then(|message| message.as_str())
                    .map(|s| s.to_string())
            })
        })
        .or_else(|| {
            value
                .get("message")
                .and_then(|message| message.as_str())
                .map(|s| s.to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grok_batch_result_item_reads_nested_chat_completion_response() {
        let item: GrokBatchResultItem = serde_json::from_value(serde_json::json!({
            "batch_request_id": "artifact-1",
            "batch_result": {
                "response": {
                    "chat_get_completion": {
                        "choices": [
                            {
                                "message": {
                                    "content": "{\"summary\":{}}"
                                }
                            }
                        ]
                    }
                }
            }
        }))
        .expect("item should deserialize");

        assert!(item.chat_completion_response().is_some());
        assert_eq!(item.error_message(), None);
    }

    #[test]
    fn grok_batch_result_item_extracts_retryable_error_from_response_payload() {
        let item: GrokBatchResultItem = serde_json::from_value(serde_json::json!({
            "batch_request_id": "artifact-1",
            "batch_result": {
                "response": {
                    "error": "Service temporarily unavailable. The model is at capacity and currently cannot serve this request. Please try again later."
                }
            }
        }))
        .expect("item should deserialize");

        let error = item.error_message().expect("error should be extracted");
        assert!(is_retryable_grok_batch_error(&error));
    }
}
