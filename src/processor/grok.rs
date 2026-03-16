use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::GrokConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    ArtifactProcessor, ArtifactProcessorFactory, BatchHandle, BatchPollResult,
    ConversationEnrichmentStrategy, ExtractionBatchSubmitter, HostedArtifactProcessor,
    HostedPreprocessProcessor, HostedReconciliationProcessor, InferenceClient, InferenceResult,
    InferenceUsage, OpenRouterResponsesContentItem, OpenRouterResponsesInputItem,
    OpenRouterResponsesReasoningConfig, OpenRouterResponsesRequest, OpenRouterResponsesResponse,
    OpenRouterResponsesTextConfig, PREPROCESS_PHASE_ONE_SYSTEM_PROMPT,
    PREPROCESS_PHASE_TWO_SYSTEM_PROMPT, PREPROCESS_PROMPT_VERSION, PreprocessBatchSubmitter,
    PreprocessPhaseOneSpanResolved, PreprocessProcessor, ProcessorError,
    ReconciliationBatchSubmitter, ReconciliationProcessor, ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
    OPENAI_PROMPT_VERSION, RECONCILIATION_SYSTEM_PROMPT,
};

pub struct GrokProcessorFactory {
    client: Arc<dyn InferenceClient>,
    batch_client: Option<Arc<GrokClient>>,
    standard_model: String,
    quality_model: String,
}

impl GrokProcessorFactory {
    pub fn new(config: GrokConfig) -> Result<Self, String> {
        let client = Arc::new(GrokClient::new(&config).map_err(|err| err.to_string())?);
        let quality_model = config
            .quality_model
            .clone()
            .unwrap_or_else(|| config.standard_model.clone());

        Ok(Self {
            client: client.clone(),
            batch_client: Some(client),
            standard_model: config.standard_model,
            quality_model,
        })
    }
}

impl ArtifactProcessorFactory for GrokProcessorFactory {
    fn build_preprocess_processor(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Box<dyn PreprocessProcessor>, ProcessorError> {
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Box::new(HostedPreprocessProcessor {
            client: Arc::clone(&self.client),
            model,
            pipeline_name: "grok_preprocess",
            provider_name: "grok",
            prompt_version: PREPROCESS_PROMPT_VERSION,
        }))
    }

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
            model,
        })))
    }

    fn build_preprocess_submitter(
        &self,
        tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn PreprocessBatchSubmitter>>, ProcessorError> {
        let Some(client) = &self.batch_client else {
            return Ok(None);
        };
        let model = match tier {
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
        };
        Ok(Some(Box::new(GrokPreprocessSubmitter {
            client: Arc::clone(client),
            model,
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
            EnrichmentTier::Standard => self.standard_model.clone(),
            EnrichmentTier::Quality => self.quality_model.clone(),
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

impl GrokClient {
    fn create_batch(
        &self,
        name: &str,
    ) -> Result<GrokBatch, ProcessorError> {
        let body = serde_json::json!({ "name": name });
        let response = self
            .client
            .post(format!("{}/batches", self.base_url))
            .body(serde_json::to_vec(&body).map_err(|source| ProcessorError::SerializePrompt { source })?)
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
            .body(serde_json::to_vec(&body).map_err(|source| ProcessorError::SerializePrompt { source })?)
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
                request.send().map_err(|source| ProcessorError::SendInferenceRequest { source })?,
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
    ) -> serde_json::Value {
        serde_json::json!({
            "model": model,
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

struct GrokExtractionSubmitter {
    client: Arc<GrokClient>,
    model: String,
}

struct GrokPreprocessSubmitter {
    client: Arc<GrokClient>,
    model: String,
}

struct GrokReconciliationSubmitter {
    client: Arc<GrokClient>,
    model: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct GrokPhaseOneData {
    resolved: std::collections::HashMap<String, Vec<PreprocessPhaseOneSpanResolved>>,
    usage: std::collections::HashMap<String, Option<InferenceUsage>>,
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
        let requests = build_grok_requests(inputs, &self.model, |input| {
            Ok(self.client.build_chat_completion_body(
                &self.model,
                ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
                &super::build_conversation_user_prompt(input)?,
                &super::structured_output_schema(),
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
                return inputs.iter().map(|_| Err(ProcessorError::Message {
                    message: "failed to downcast Grok extraction batch result".to_string(),
                })).collect();
            }
        };
        match parse_grok_results(&self.client, &batch.id, inputs, |result, input| {
            let parsed: super::ModelArtifactOutput =
                serde_json::from_str(&result.output_text).map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: super::preview(&result.output_text),
                })?;
            let parsed = parsed.resolve_evidence_aliases(input);
            parsed.validate_against(input)?;
            Ok(parsed.into_processor_output(
                self.model.clone(),
                result.usage,
                "grok_enrichment",
                "grok",
                OPENAI_PROMPT_VERSION,
            ))
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(grok_message_error(&err))).collect(),
        }
    }
}

impl PreprocessBatchSubmitter for GrokPreprocessSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn submit_phase_one(
        &self,
        inputs: &[super::PreprocessProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let batch = self.client.create_batch("openarchive-preprocess-phase-one")?;
        let requests = build_grok_requests(inputs, &self.model, |input| {
            Ok(self.client.build_chat_completion_body(
                &self.model,
                PREPROCESS_PHASE_ONE_SYSTEM_PROMPT,
                &super::build_preprocess_phase_one_user_prompt(input)?,
                &super::preprocess_phase_one_schema(),
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
            model: self.model.clone(),
        }.poll_batch(handle)
    }

    fn parse_phase_one(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::PreprocessProcessorInput],
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let batch = completed.downcast::<GrokBatch>().map_err(|_| ProcessorError::Message {
            message: "failed to downcast Grok preprocess phase-one batch result".to_string(),
        })?;
        let items = parse_grok_results(&self.client, &batch.id, inputs, |result, input| {
            let parsed: super::ModelPreprocessPhaseOneOutput =
                serde_json::from_str(&result.output_text).map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: super::preview(&result.output_text),
                })?;
            Ok((parsed.resolve_and_validate(input)?, result.usage))
        })?;
        let mut resolved = std::collections::HashMap::new();
        let mut usage = std::collections::HashMap::new();
        for (input, item) in inputs.iter().zip(items.into_iter()) {
            let (spans, item_usage) = item?;
            resolved.insert(input.artifact_id.clone(), spans);
            usage.insert(input.artifact_id.clone(), item_usage);
        }
        Ok(Box::new(GrokPhaseOneData { resolved, usage }))
    }

    fn submit_phase_two(
        &self,
        inputs: &[super::PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Result<BatchHandle, ProcessorError> {
        let data = phase_one_data.downcast_ref::<GrokPhaseOneData>().ok_or_else(|| ProcessorError::Message {
            message: "failed to downcast Grok preprocess phase-one data".to_string(),
        })?;
        let batch = self.client.create_batch("openarchive-preprocess-phase-two")?;
        let requests = build_grok_requests(inputs, &self.model, |input| {
            let spans = data.resolved.get(&input.artifact_id).ok_or_else(|| ProcessorError::Message {
                message: format!("missing phase-one spans for {}", input.artifact_id),
            })?;
            Ok(self.client.build_chat_completion_body(
                &self.model,
                PREPROCESS_PHASE_TWO_SYSTEM_PROMPT,
                &super::build_preprocess_phase_two_user_prompt(input, spans)?,
                &super::preprocess_output_schema(),
            ))
        })?;
        self.client.add_batch_requests(&batch.id, &requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "grok".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn serialize_phase_one_data(
        &self,
        phase_one_data: &dyn std::any::Any,
    ) -> Result<String, ProcessorError> {
        let data = phase_one_data.downcast_ref::<GrokPhaseOneData>().ok_or_else(|| ProcessorError::Message {
            message: "failed to downcast Grok preprocess phase-one data".to_string(),
        })?;
        serde_json::to_string(data).map_err(|source| ProcessorError::Message {
            message: format!("failed to serialize Grok preprocess phase-one data: {source}"),
        })
    }

    fn deserialize_phase_one_data(
        &self,
        serialized: &str,
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let data: GrokPhaseOneData =
            serde_json::from_str(serialized).map_err(|source| ProcessorError::ParseModelJson {
                source,
                body_preview: super::preview(serialized),
            })?;
        Ok(Box::new(data))
    }

    fn parse_phase_two(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Vec<Result<super::PreprocessProcessorOutput, ProcessorError>> {
        let batch = match completed.downcast::<GrokBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs.iter().map(|_| Err(ProcessorError::Message {
                    message: "failed to downcast Grok preprocess phase-two batch result".to_string(),
                })).collect();
            }
        };
        let Some(data) = phase_one_data.downcast_ref::<GrokPhaseOneData>() else {
            return inputs.iter().map(|_| Err(ProcessorError::Message {
                message: "failed to downcast Grok preprocess phase-one data".to_string(),
            })).collect();
        };
        match parse_grok_results(&self.client, &batch.id, inputs, |result, input| {
            let parsed: super::ModelPreprocessOutput =
                serde_json::from_str(&result.output_text).map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: super::preview(&result.output_text),
                })?;
            let spans = data.resolved.get(&input.artifact_id).ok_or_else(|| ProcessorError::Message {
                message: format!("missing phase-one spans for {}", input.artifact_id),
            })?;
            let parsed = parsed.resolve_segment_aliases(input, spans);
            parsed.validate_against(input)?;
            Ok(parsed.into_processor_output(
                input,
                self.model.clone(),
                super::combine_inference_usage(
                    data.usage.get(&input.artifact_id).cloned().unwrap_or(None),
                    result.usage,
                ),
                "grok_preprocess",
                "grok",
                PREPROCESS_PROMPT_VERSION,
            ))
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(grok_message_error(&err))).collect(),
        }
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
            model: self.model.clone(),
        }.poll_batch(handle)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<super::ReconciliationDecisionOutput>, ProcessorError>> {
        let batch = match completed.downcast::<GrokBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs.iter().map(|_| Err(ProcessorError::Message {
                    message: "failed to downcast Grok reconciliation batch result".to_string(),
                })).collect();
            }
        };
        match parse_grok_results(&self.client, &batch.id, inputs, |result, input| {
            let parsed: super::ModelReconciliationOutput =
                serde_json::from_str(&result.output_text).map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: super::preview(&result.output_text),
                })?;
            parsed.validate_against(input)?;
            Ok(parsed.into_outputs())
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(grok_message_error(&err))).collect(),
        }
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
            batch_request_id: input.batch_custom_id().to_string(),
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
) -> Result<Vec<Result<O, ProcessorError>>, ProcessorError>
where
    I: GrokBatchInput,
    F: FnMut(GrokBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let mut by_id = std::collections::HashMap::new();
    for item in client.list_results(batch_id)? {
        let parsed = if let Some(error) = item.error_message {
            return Err(ProcessorError::Message {
                message: format!("Grok batch item {} failed: {}", item.batch_request_id, error),
            });
        } else {
            let response_value = item.response.ok_or_else(|| ProcessorError::Message {
                message: format!("Grok batch item {} missing response", item.batch_request_id),
            })?;
            GrokBatchParsedResult {
                output_text: flatten_grok_response_text(&response_value)?,
                usage: extract_grok_usage(&response_value),
            }
        };
        by_id.insert(item.batch_request_id, parsed);
    }
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let item = by_id.remove(input.batch_custom_id()).ok_or_else(|| ProcessorError::Message {
            message: format!("Grok batch missing result for {}", input.batch_custom_id()),
        })?;
        outputs.push(parse(item, input));
    }
    Ok(outputs)
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

trait GrokBatchInput {
    fn batch_custom_id(&self) -> &str;
}

impl GrokBatchInput for super::ArtifactProcessorInput {
    fn batch_custom_id(&self) -> &str { &self.artifact_id }
}

impl GrokBatchInput for super::PreprocessProcessorInput {
    fn batch_custom_id(&self) -> &str { &self.artifact_id }
}

impl GrokBatchInput for super::ReconciliationProcessorInput {
    fn batch_custom_id(&self) -> &str { &self.artifact_id }
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
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GrokBatchResultItem {
    batch_request_id: String,
    #[serde(default)]
    response: Option<serde_json::Value>,
    #[serde(default)]
    error_message: Option<String>,
}
