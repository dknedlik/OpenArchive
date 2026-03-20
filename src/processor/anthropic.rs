use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::AnthropicConfig;
use crate::storage::types::EnrichmentTier;

use super::{
    ArtifactProcessor, ArtifactProcessorFactory, BatchHandle, BatchPollResult,
    ConversationEnrichmentStrategy, ExtractionBatchSubmitter, HostedArtifactProcessor,
    HostedPreprocessProcessor, HostedReconciliationProcessor, InferenceClient, InferenceResult,
    InferenceUsage, PreprocessBatchSubmitter, PreprocessPhaseOneSpanResolved, PreprocessProcessor,
    ProcessorError, ReconciliationBatchSubmitter, ReconciliationProcessor,
    ANTHROPIC_ARTIFACT_EXTRACTION_SYSTEM_PROMPT, ANTHROPIC_PROMPT_VERSION,
    PREPROCESS_PHASE_ONE_SYSTEM_PROMPT, PREPROCESS_PHASE_TWO_SYSTEM_PROMPT,
    PREPROCESS_PROMPT_VERSION, RECONCILIATION_SYSTEM_PROMPT,
    structured_output_schema_with_allowed_refs,
};

pub struct AnthropicProcessorFactory {
    client: Arc<dyn InferenceClient>,
    batch_client: Option<Arc<AnthropicClient>>,
    standard_model: String,
    quality_model: String,
}

impl AnthropicProcessorFactory {
    pub fn new(config: AnthropicConfig) -> Result<Self, String> {
        let client = Arc::new(AnthropicClient::new(&config).map_err(|err| err.to_string())?);
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

impl ArtifactProcessorFactory for AnthropicProcessorFactory {
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
            pipeline_name: "anthropic_preprocess",
            provider_name: "anthropic",
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
        Ok(Some(Box::new(AnthropicExtractionSubmitter {
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
        Ok(Some(Box::new(AnthropicPreprocessSubmitter {
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
                body_preview: super::preview(&response_text),
            });
        }

        let parsed: AnthropicMessagesResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: super::preview(&response_text),
                }
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
        let body = serde_json::to_vec(&AnthropicBatchCreateRequest { requests })
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
                body_preview: super::preview(&response_text),
            });
        }
        Ok(response_text)
    }
}

struct AnthropicExtractionSubmitter {
    client: Arc<AnthropicClient>,
    model: String,
}

struct AnthropicPreprocessSubmitter {
    client: Arc<AnthropicClient>,
    model: String,
}

struct AnthropicReconciliationSubmitter {
    client: Arc<AnthropicClient>,
    model: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AnthropicPhaseOneData {
    resolved: std::collections::HashMap<String, Vec<PreprocessPhaseOneSpanResolved>>,
    usage: std::collections::HashMap<String, Option<InferenceUsage>>,
}

impl ExtractionBatchSubmitter for AnthropicExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        100_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[super::ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let requests = build_anthropic_batch_requests(inputs, &self.model, |input| {
            Ok((
                ANTHROPIC_ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
                super::build_conversation_user_prompt(input)?,
                structured_output_schema_with_allowed_refs(
                    &input
                        .segments
                        .iter()
                        .enumerate()
                        .map(|(index, _)| format!("evidence_ref_{}", index + 1))
                        .collect::<Vec<_>>(),
                ),
            ))
        })?;
        let batch = self.client.create_message_batch(&requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "anthropic".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        let batch = self.client.get_message_batch(&handle.batch_id)?;
        match batch.processing_status.as_deref().unwrap_or_default() {
            "ended" => Ok(BatchPollResult::Succeeded(Box::new(batch))),
            "canceling" => Ok(BatchPollResult::Pending),
            _ => Ok(BatchPollResult::Pending),
        }
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::ArtifactProcessorInput],
    ) -> Vec<Result<super::ArtifactProcessorOutput, ProcessorError>> {
        let batch = match completed.downcast::<AnthropicMessageBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast Anthropic extraction batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        match parse_anthropic_batch_results(&self.client, &batch, inputs, |result, input| {
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
                "anthropic_enrichment",
                "anthropic",
                ANTHROPIC_PROMPT_VERSION,
            ))
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(message_error(&err))).collect(),
        }
    }
}

impl PreprocessBatchSubmitter for AnthropicPreprocessSubmitter {
    fn max_batch_size(&self) -> usize {
        100_000
    }

    fn submit_phase_one(
        &self,
        inputs: &[super::PreprocessProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let requests = build_anthropic_batch_requests(inputs, &self.model, |input| {
            Ok((
                PREPROCESS_PHASE_ONE_SYSTEM_PROMPT,
                super::build_preprocess_phase_one_user_prompt(input)?,
                super::preprocess_phase_one_schema(),
            ))
        })?;
        let batch = self.client.create_message_batch(&requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "anthropic".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        AnthropicExtractionSubmitter {
            client: Arc::clone(&self.client),
            model: self.model.clone(),
        }
        .poll_batch(handle)
    }

    fn parse_phase_one(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::PreprocessProcessorInput],
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let batch =
            completed
                .downcast::<AnthropicMessageBatch>()
                .map_err(|_| ProcessorError::Message {
                    message: "failed to downcast Anthropic preprocess phase-one batch result"
                        .to_string(),
                })?;
        let items =
            parse_anthropic_batch_results(&self.client, &batch, inputs, |result, input| {
                let parsed: super::ModelPreprocessPhaseOneOutput =
                    serde_json::from_str(&result.output_text).map_err(|source| {
                        ProcessorError::ParseModelJson {
                            source,
                            body_preview: super::preview(&result.output_text),
                        }
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
        Ok(Box::new(AnthropicPhaseOneData { resolved, usage }))
    }

    fn submit_phase_two(
        &self,
        inputs: &[super::PreprocessProcessorInput],
        phase_one_data: &dyn std::any::Any,
    ) -> Result<BatchHandle, ProcessorError> {
        let data = phase_one_data
            .downcast_ref::<AnthropicPhaseOneData>()
            .ok_or_else(|| ProcessorError::Message {
                message: "failed to downcast Anthropic preprocess phase-one data".to_string(),
            })?;
        let requests = build_anthropic_batch_requests(inputs, &self.model, |input| {
            let spans =
                data.resolved
                    .get(&input.artifact_id)
                    .ok_or_else(|| ProcessorError::Message {
                        message: format!("missing phase-one spans for {}", input.artifact_id),
                    })?;
            Ok((
                PREPROCESS_PHASE_TWO_SYSTEM_PROMPT,
                super::build_preprocess_phase_two_user_prompt(input, spans)?,
                super::preprocess_output_schema(),
            ))
        })?;
        let batch = self.client.create_message_batch(&requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "anthropic".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn serialize_phase_one_data(
        &self,
        phase_one_data: &dyn std::any::Any,
    ) -> Result<String, ProcessorError> {
        let data = phase_one_data
            .downcast_ref::<AnthropicPhaseOneData>()
            .ok_or_else(|| ProcessorError::Message {
                message: "failed to downcast Anthropic preprocess phase-one data".to_string(),
            })?;
        serde_json::to_string(data).map_err(|source| ProcessorError::Message {
            message: format!("failed to serialize Anthropic preprocess phase-one data: {source}"),
        })
    }

    fn deserialize_phase_one_data(
        &self,
        serialized: &str,
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let data: AnthropicPhaseOneData =
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
        let batch = match completed.downcast::<AnthropicMessageBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message:
                                "failed to downcast Anthropic preprocess phase-two batch result"
                                    .to_string(),
                        })
                    })
                    .collect();
            }
        };
        let Some(data) = phase_one_data.downcast_ref::<AnthropicPhaseOneData>() else {
            return inputs
                .iter()
                .map(|_| {
                    Err(ProcessorError::Message {
                        message: "failed to downcast Anthropic preprocess phase-one data"
                            .to_string(),
                    })
                })
                .collect();
        };
        match parse_anthropic_batch_results(&self.client, &batch, inputs, |result, input| {
            let parsed: super::ModelPreprocessOutput = serde_json::from_str(&result.output_text)
                .map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: super::preview(&result.output_text),
                })?;
            let spans =
                data.resolved
                    .get(&input.artifact_id)
                    .ok_or_else(|| ProcessorError::Message {
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
                "anthropic_preprocess",
                "anthropic",
                PREPROCESS_PROMPT_VERSION,
            ))
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(message_error(&err))).collect(),
        }
    }

    fn repair_phase_one_batch(
        &self,
        inputs: &[super::PreprocessProcessorInput],
        _error: &ProcessorError,
    ) -> Result<Box<dyn std::any::Any>, ProcessorError> {
        let mut resolved = std::collections::HashMap::new();
        let mut usage = std::collections::HashMap::new();
        for input in inputs {
            let (spans, item_usage) = super::run_preprocess_phase_one_with_repair(
                self.client.as_ref(),
                &self.model,
                input,
            )?;
            resolved.insert(input.artifact_id.clone(), spans);
            usage.insert(input.artifact_id.clone(), item_usage);
        }
        Ok(Box::new(AnthropicPhaseOneData { resolved, usage }))
    }

    fn repair_phase_two(
        &self,
        input: &super::PreprocessProcessorInput,
        phase_one_data: &dyn std::any::Any,
        _error: &ProcessorError,
    ) -> Result<super::PreprocessProcessorOutput, ProcessorError> {
        let data = phase_one_data
            .downcast_ref::<AnthropicPhaseOneData>()
            .ok_or_else(|| ProcessorError::Message {
                message: "failed to downcast Anthropic preprocess phase-one data".to_string(),
            })?;
        let spans =
            data.resolved
                .get(&input.artifact_id)
                .ok_or_else(|| ProcessorError::Message {
                    message: format!("missing phase-one spans for {}", input.artifact_id),
                })?;
        let mut output = super::run_preprocess_phase_two_with_repair(
            self.client.as_ref(),
            &self.model,
            input,
            spans,
            "anthropic_preprocess",
            "anthropic",
        )?;
        output.usage = super::combine_inference_usage(
            data.usage.get(&input.artifact_id).cloned().unwrap_or(None),
            output.usage,
        );
        Ok(output)
    }
}

impl ReconciliationBatchSubmitter for AnthropicReconciliationSubmitter {
    fn max_batch_size(&self) -> usize {
        100_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[super::ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let requests = build_anthropic_batch_requests(inputs, &self.model, |input| {
            Ok((
                RECONCILIATION_SYSTEM_PROMPT,
                super::build_reconciliation_prompt(input)?,
                super::reconciliation_output_schema(),
            ))
        })?;
        let batch = self.client.create_message_batch(&requests)?;
        Ok(BatchHandle {
            batch_id: batch.id,
            provider: "anthropic".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        AnthropicExtractionSubmitter {
            client: Arc::clone(&self.client),
            model: self.model.clone(),
        }
        .poll_batch(handle)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[super::ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<super::ReconciliationDecisionOutput>, ProcessorError>> {
        let batch = match completed.downcast::<AnthropicMessageBatch>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast Anthropic reconciliation batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        match parse_anthropic_batch_results(&self.client, &batch, inputs, |result, input| {
            let parsed: super::ModelReconciliationOutput =
                serde_json::from_str(&result.output_text).map_err(|source| {
                    ProcessorError::ParseModelJson {
                        source,
                        body_preview: super::preview(&result.output_text),
                    }
                })?;
            parsed.validate_against(input)?;
            Ok(parsed.into_outputs())
        }) {
            Ok(outputs) => outputs,
            Err(err) => inputs.iter().map(|_| Err(message_error(&err))).collect(),
        }
    }
}

fn build_anthropic_batch_requests<I, F>(
    inputs: &[I],
    model: &str,
    mut build: F,
) -> Result<Vec<AnthropicBatchRequestOwned>, ProcessorError>
where
    I: AnthropicBatchInput,
    F: FnMut(&I) -> Result<(&'static str, String, serde_json::Value), ProcessorError>,
{
    let mut requests = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (system_prompt, user_prompt, schema) = build(input)?;
        requests.push(AnthropicBatchRequestOwned {
            custom_id: input.batch_custom_id(),
            params: AnthropicMessagesRequestOwned {
                model: model.to_string(),
                max_tokens: 4096,
                system: system_prompt.to_string(),
                messages: vec![AnthropicOwnedMessageInput {
                    role: "user".to_string(),
                    content: user_prompt,
                }],
                tools: vec![AnthropicToolDefinition {
                    name: "record_enrichment",
                    description: "Return the OpenArchive enrichment result as structured JSON.",
                    input_schema: schema,
                }],
                tool_choice: AnthropicOwnedToolChoice {
                    choice_type: "tool".to_string(),
                    name: "record_enrichment".to_string(),
                    disable_parallel_tool_use: true,
                },
            },
        });
    }
    Ok(requests)
}

fn parse_anthropic_batch_results<I, O, F>(
    client: &AnthropicClient,
    batch: &AnthropicMessageBatch,
    inputs: &[I],
    mut parse: F,
) -> Result<Vec<Result<O, ProcessorError>>, ProcessorError>
where
    I: AnthropicBatchInput,
    F: FnMut(AnthropicBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let results_url = batch
        .results_url
        .as_deref()
        .ok_or_else(|| ProcessorError::Message {
            message: format!("Anthropic batch {} missing results_url", batch.id),
        })?;
    let text = client.read_results_text(results_url)?;
    let mut by_id = std::collections::HashMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let item: AnthropicBatchResultLine = serde_json::from_str(line).map_err(|source| {
            ProcessorError::ParseInferenceResponse {
                source,
                body_preview: super::preview(line),
            }
        })?;
        let parsed = match item.result {
            AnthropicBatchResultBody::Succeeded { message } => {
                let tool_input = message
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
                            "Anthropic batch item {} returned no tool result",
                            item.custom_id
                        ),
                    })?;
                AnthropicBatchParsedResult {
                    output_text: serde_json::to_string(&tool_input)
                        .map_err(|source| ProcessorError::SerializePrompt { source })?,
                    usage: message.usage.map(InferenceUsage::from_anthropic_usage),
                }
            }
            AnthropicBatchResultBody::Errored { error } => {
                return Err(ProcessorError::Message {
                    message: format!(
                        "Anthropic batch item {} failed: {}",
                        item.custom_id, error.message
                    ),
                });
            }
            AnthropicBatchResultBody::Canceled => {
                return Err(ProcessorError::Message {
                    message: format!("Anthropic batch item {} was canceled", item.custom_id),
                });
            }
            AnthropicBatchResultBody::Expired => {
                return Err(ProcessorError::Message {
                    message: format!("Anthropic batch item {} expired", item.custom_id),
                });
            }
        };
        by_id.insert(item.custom_id, parsed);
    }
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let custom_id = input.batch_custom_id();
        let item = by_id.remove(&custom_id).ok_or_else(|| ProcessorError::Message {
            message: format!("Anthropic batch missing result for {}", custom_id),
        })?;
        outputs.push(parse(item, input));
    }
    Ok(outputs)
}

trait AnthropicBatchInput {
    fn batch_custom_id(&self) -> String;
}

impl AnthropicBatchInput for super::ArtifactProcessorInput {
    fn batch_custom_id(&self) -> String {
        super::artifact_processor_batch_custom_id(self)
    }
}

impl AnthropicBatchInput for super::PreprocessProcessorInput {
    fn batch_custom_id(&self) -> String {
        self.artifact_id.clone()
    }
}

impl AnthropicBatchInput for super::ReconciliationProcessorInput {
    fn batch_custom_id(&self) -> String {
        self.artifact_id.clone()
    }
}

struct AnthropicBatchParsedResult {
    output_text: String,
    usage: Option<InferenceUsage>,
}

fn message_error(err: &ProcessorError) -> ProcessorError {
    ProcessorError::Message {
        message: err.to_string(),
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
            body_preview: super::preview(&response_text),
        });
    }
    serde_json::from_str(&response_text).map_err(|source| ProcessorError::ParseInferenceResponse {
        source,
        body_preview: super::preview(&response_text),
    })
}

#[derive(Debug, Serialize)]
struct AnthropicMessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: Vec<AnthropicMessageInput<'a>>,
    tools: Vec<AnthropicToolDefinition>,
    tool_choice: AnthropicToolChoice<'a>,
}

#[derive(Debug, Serialize)]
struct AnthropicMessagesRequestOwned {
    model: String,
    max_tokens: u32,
    system: String,
    messages: Vec<AnthropicOwnedMessageInput>,
    tools: Vec<AnthropicToolDefinition>,
    tool_choice: AnthropicOwnedToolChoice,
}

#[derive(Debug, Serialize)]
struct AnthropicMessageInput<'a> {
    role: &'static str,
    content: &'a str,
}

#[derive(Debug, Serialize)]
struct AnthropicOwnedMessageInput {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct AnthropicToolDefinition {
    name: &'static str,
    description: &'static str,
    input_schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct AnthropicToolChoice<'a> {
    #[serde(rename = "type")]
    choice_type: &'static str,
    name: &'a str,
    disable_parallel_tool_use: bool,
}

#[derive(Debug, Serialize)]
struct AnthropicOwnedToolChoice {
    #[serde(rename = "type")]
    choice_type: String,
    name: String,
    disable_parallel_tool_use: bool,
}

#[derive(Debug, Serialize)]
struct AnthropicBatchCreateRequest<'a> {
    requests: &'a [AnthropicBatchRequestOwned],
}

#[derive(Debug, Serialize)]
struct AnthropicBatchRequestOwned {
    custom_id: String,
    params: AnthropicMessagesRequestOwned,
}

#[derive(Debug, Deserialize)]
struct AnthropicMessageBatch {
    id: String,
    #[serde(default)]
    processing_status: Option<String>,
    #[serde(default)]
    results_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnthropicMessagesResponse {
    #[serde(default)]
    content: Vec<AnthropicContentBlock>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "tool_use")]
    ToolUse {
        #[serde(rename = "id")]
        _id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AnthropicBatchResultLine {
    custom_id: String,
    result: AnthropicBatchResultBody,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum AnthropicBatchResultBody {
    #[serde(rename = "succeeded")]
    Succeeded { message: AnthropicMessagesResponse },
    #[serde(rename = "errored")]
    Errored { error: AnthropicResultError },
    #[serde(rename = "canceled")]
    Canceled,
    #[serde(rename = "expired")]
    Expired,
}

#[derive(Debug, Deserialize)]
struct AnthropicResultError {
    #[serde(default)]
    message: String,
}

impl InferenceUsage {
    fn from_anthropic_usage(usage: AnthropicUsage) -> Self {
        Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: None,
            total_tokens: usage
                .input_tokens
                .zip(usage.output_tokens)
                .map(|(input, output)| input + output),
            reported_cost_micros: None,
        }
    }
}
