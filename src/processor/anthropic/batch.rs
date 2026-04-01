use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::*;

pub(super) struct AnthropicExtractionSubmitter {
    pub(super) client: Arc<AnthropicClient>,
    pub(super) candidate_model: String,
}

pub(super) struct AnthropicReconciliationSubmitter {
    pub(super) client: Arc<AnthropicClient>,
    pub(super) model: String,
}

impl ExtractionBatchSubmitter for AnthropicExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        100_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let requests = build_anthropic_batch_requests(inputs, &self.candidate_model, |input| {
            Ok((
                candidate_system_prompt(input),
                build_two_phase_candidate_user_prompt(input)?,
                candidate_output_schema_with_allowed_refs(
                    input,
                    &allowed_artifact_evidence_refs(input),
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
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
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
        let candidate_results =
            parse_anthropic_batch_results(&self.client, &batch, inputs, |result, input| {
                let candidate = parse_candidate_output(&result.output_text, input)
                    .map_err(|err| attach_output_preview(err, &result.output_text))?;
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
                    "anthropic_enrichment",
                    "anthropic",
                    ANTHROPIC_PROMPT_VERSION,
                )),
                Err(err) => Err(ProcessorError::Message {
                    message: err.to_string(),
                }),
            })
            .collect()
    }
}

impl ReconciliationBatchSubmitter for AnthropicReconciliationSubmitter {
    fn max_batch_size(&self) -> usize {
        100_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let requests = build_anthropic_batch_requests(inputs, &self.model, |input| {
            Ok((
                RECONCILIATION_SYSTEM_PROMPT,
                build_reconciliation_prompt(input)?,
                reconciliation_output_schema(),
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
            candidate_model: self.model.clone(),
        }
        .poll_batch(handle)
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>> {
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
        parse_anthropic_batch_results(&self.client, &batch, inputs, |result, input| {
            let parsed: ModelReconciliationOutput = serde_json::from_str(&result.output_text)
                .map_err(|source| ProcessorError::ParseModelJson {
                    source,
                    body_preview: preview(&result.output_text),
                })?;
            parsed.validate_against(input)?;
            Ok(parsed.into_outputs())
        })
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
            params: types::AnthropicMessagesRequestOwned {
                model: model.to_string(),
                max_tokens: 4096,
                system: system_prompt.to_string(),
                messages: vec![types::AnthropicOwnedMessageInput {
                    role: "user".to_string(),
                    content: user_prompt,
                }],
                tools: vec![types::AnthropicToolDefinition {
                    name: "record_enrichment",
                    description: "Return the OpenArchive enrichment result as structured JSON.",
                    input_schema: schema,
                }],
                tool_choice: types::AnthropicOwnedToolChoice {
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
) -> Vec<Result<O, ProcessorError>>
where
    I: AnthropicBatchInput,
    F: FnMut(AnthropicBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let results_url = batch
        .results_url
        .as_deref()
        .ok_or_else(|| ProcessorError::Message {
            message: format!("Anthropic batch {} missing results_url", batch.id),
        });
    let Some(results_url) = results_url.ok() else {
        let err = ProcessorError::Message {
            message: format!("Anthropic batch {} missing results_url", batch.id),
        };
        return inputs.iter().map(|_| Err(message_error(&err))).collect();
    };
    let text = match client.read_results_text(results_url) {
        Ok(text) => text,
        Err(err) => return inputs.iter().map(|_| Err(message_error(&err))).collect(),
    };
    let mut by_id = std::collections::HashMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let item: types::AnthropicBatchResultLine = match serde_json::from_str(line) {
            Ok(item) => item,
            Err(source) => {
                let err = ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(line),
                };
                return inputs.iter().map(|_| Err(message_error(&err))).collect();
            }
        };
        let parsed = match item.result {
            types::AnthropicBatchResultBody::Succeeded { message } => message
                .content
                .iter()
                .find_map(|block| match block {
                    types::AnthropicContentBlock::ToolUse { name, input, .. }
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
                })
                .and_then(|tool_input| {
                    serde_json::to_string(&tool_input)
                        .map(|output_text| AnthropicBatchParsedResult {
                            output_text,
                            usage: message.usage.map(InferenceUsage::from_anthropic_usage),
                        })
                        .map_err(|source| ProcessorError::SerializePrompt { source })
                }),
            types::AnthropicBatchResultBody::Errored { error } => Err(ProcessorError::Message {
                message: format!(
                    "Anthropic batch item {} failed: {}",
                    item.custom_id, error.message
                ),
            }),
            types::AnthropicBatchResultBody::Canceled => Err(ProcessorError::Message {
                message: format!("Anthropic batch item {} was canceled", item.custom_id),
            }),
            types::AnthropicBatchResultBody::Expired => Err(ProcessorError::Message {
                message: format!("Anthropic batch item {} expired", item.custom_id),
            }),
        };
        by_id.insert(item.custom_id, parsed);
    }
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let custom_id = input.batch_custom_id();
        match by_id.remove(&custom_id) {
            Some(Ok(item)) => outputs.push(parse(item, input)),
            Some(Err(err)) => outputs.push(Err(err)),
            None => outputs.push(Err(ProcessorError::Message {
                message: format!("Anthropic batch missing result for {}", custom_id),
            })),
        }
    }
    outputs
}

trait AnthropicBatchInput {
    fn batch_custom_id(&self) -> String;
}

impl AnthropicBatchInput for ArtifactProcessorInput {
    fn batch_custom_id(&self) -> String {
        artifact_processor_batch_custom_id(self)
    }
}

impl AnthropicBatchInput for ReconciliationProcessorInput {
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

#[derive(Debug, Serialize)]
pub(super) struct AnthropicBatchRequestOwned {
    custom_id: String,
    params: types::AnthropicMessagesRequestOwned,
}

#[derive(Debug, Deserialize)]
pub(super) struct AnthropicMessageBatch {
    pub(super) id: String,
    #[serde(default)]
    pub(super) processing_status: Option<String>,
    #[serde(default)]
    pub(super) results_url: Option<String>,
}
