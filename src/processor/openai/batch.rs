use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::*;

pub(super) struct OpenAiArtifactProcessor {
    pub(super) client: Arc<OpenAiClient>,
    pub(super) candidate_model: String,
    pub(super) max_output_tokens: u32,
    pub(super) repair_max_output_tokens: u32,
}

impl ArtifactProcessor for OpenAiArtifactProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        validate_input(input)?;
        let prompt =
            build_two_phase_candidate_user_prompt_with_flavor(input, PromptFlavor::OpenAi)?;
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

impl OpenAiArtifactProcessor {
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
            "openai_enrichment",
            "openai",
            OPENAI_PROMPT_VERSION,
        ))
    }
}

pub(super) struct OpenAiExtractionSubmitter {
    pub(super) client: Arc<OpenAiClient>,
    pub(super) candidate_model: String,
}

pub(super) struct OpenAiReconciliationSubmitter {
    pub(super) client: Arc<OpenAiClient>,
    pub(super) model: String,
}

impl ExtractionBatchSubmitter for OpenAiExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt =
                build_two_phase_candidate_user_prompt_with_flavor(input, PromptFlavor::OpenAi)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.batch_custom_id(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.candidate_model,
                    candidate_system_prompt(input),
                    &user_prompt,
                    &candidate_output_schema_wrapper(input),
                ),
            });
        }
        let job = self
            .client
            .submit_responses_batch(Some("openarchive-enrichment"), &requests)?;
        Ok(BatchHandle {
            batch_id: job.id,
            provider: "openai".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        let batch = self.client.get_batch(&handle.batch_id)?;
        match batch.status.as_deref().unwrap_or_default() {
            "completed" => {
                if batch.output_file_id.is_some() {
                    Ok(BatchPollResult::Succeeded(Box::new(batch)))
                } else {
                    Ok(BatchPollResult::Failed(format!(
                        "OpenAI batch {} completed without output_file_id (error_file_id={})",
                        handle.batch_id,
                        batch.error_file_id.as_deref().unwrap_or("none")
                    )))
                }
            }
            "failed" | "expired" | "cancelled" => Ok(BatchPollResult::Failed(format!(
                "OpenAI batch {} finished in terminal state {}",
                handle.batch_id,
                batch.status.unwrap_or_else(|| "unknown".to_string())
            ))),
            _ => Ok(BatchPollResult::Pending),
        }
    }

    fn parse_results(
        &self,
        completed: Box<dyn std::any::Any>,
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
        let batch = match completed.downcast::<OpenAiBatchJob>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast OpenAI extraction batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        let candidate_results = parse_openai_output_file(
            &self.client,
            batch.output_file_id.as_deref(),
            inputs,
            |result, input| {
                let candidate = parse_candidate_output(&result.output_text, input)
                    .map_err(|err| attach_output_preview(err, &result.output_text))?;
                Ok((candidate, result.usage))
            },
        );
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
                    "openai_enrichment",
                    "openai",
                    OPENAI_PROMPT_VERSION,
                )),
                Err(err) => Err(ProcessorError::Message {
                    message: err.to_string(),
                }),
            })
            .collect()
    }
}

impl ReconciliationBatchSubmitter for OpenAiReconciliationSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let mut requests = Vec::with_capacity(inputs.len());
        for input in inputs {
            let user_prompt = build_reconciliation_prompt(input)?;
            requests.push(OpenAiBatchRequest {
                custom_id: input.artifact_id.clone(),
                method: "POST".to_string(),
                url: "/v1/responses".to_string(),
                body: self.client.build_responses_request(
                    &self.model,
                    RECONCILIATION_SYSTEM_PROMPT,
                    &user_prompt,
                    &reconciliation_output_schema(),
                ),
            });
        }
        let job = self
            .client
            .submit_responses_batch(Some("openarchive-reconciliation"), &requests)?;
        Ok(BatchHandle {
            batch_id: job.id,
            provider: "openai".to_string(),
            submitted_at: std::time::Instant::now(),
        })
    }

    fn poll_batch(&self, handle: &BatchHandle) -> Result<BatchPollResult, ProcessorError> {
        OpenAiExtractionSubmitter {
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
        let batch = match completed.downcast::<OpenAiBatchJob>() {
            Ok(batch) => *batch,
            Err(_) => {
                return inputs
                    .iter()
                    .map(|_| {
                        Err(ProcessorError::Message {
                            message: "failed to downcast OpenAI reconciliation batch result"
                                .to_string(),
                        })
                    })
                    .collect();
            }
        };
        parse_openai_output_file(
            &self.client,
            batch.output_file_id.as_deref(),
            inputs,
            |result, input| {
                let parsed: ModelReconciliationOutput = serde_json::from_str(&result.output_text)
                    .map_err(|source| {
                    ProcessorError::ParseModelJson {
                        source,
                        body_preview: preview(&result.output_text),
                    }
                })?;
                parsed.into_validated_outputs(input)
            },
        )
    }
}

trait OpenAiBatchInput {
    fn batch_custom_id(&self) -> String;
}

impl OpenAiBatchInput for ArtifactProcessorInput {
    fn batch_custom_id(&self) -> String {
        artifact_processor_batch_custom_id(self)
    }
}

impl OpenAiBatchInput for ReconciliationProcessorInput {
    fn batch_custom_id(&self) -> String {
        self.artifact_id.clone()
    }
}

struct OpenAiBatchParsedResult {
    output_text: String,
    usage: Option<InferenceUsage>,
}

#[derive(Debug, Serialize)]
pub(super) struct OpenAiBatchRequest {
    custom_id: String,
    method: String,
    url: String,
    body: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct OpenAiBatchJob {
    pub(super) id: String,
    #[serde(default)]
    pub(super) status: Option<String>,
    #[serde(default)]
    pub(super) output_file_id: Option<String>,
    #[serde(default)]
    pub(super) error_file_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct OpenAiFileObject {
    pub(super) id: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchResultLine {
    custom_id: String,
    #[serde(default)]
    response: Option<OpenAiBatchResponseEnvelope>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAiBatchResponseEnvelope {
    status_code: u16,
    body: OpenRouterResponsesResponse,
}

fn parse_openai_output_file<I, O, F>(
    client: &OpenAiClient,
    output_file_id: Option<&str>,
    inputs: &[I],
    mut parse: F,
) -> Vec<Result<O, ProcessorError>>
where
    I: OpenAiBatchInput,
    F: FnMut(OpenAiBatchParsedResult, &I) -> Result<O, ProcessorError>,
{
    let Some(output_file_id) = output_file_id else {
        let err = ProcessorError::Message {
            message: "OpenAI batch missing output_file_id".to_string(),
        };
        return inputs
            .iter()
            .map(|_| Err(message_processor_error(&err)))
            .collect();
    };
    let content = match client.read_file_text(output_file_id) {
        Ok(content) => content,
        Err(err) => {
            return inputs
                .iter()
                .map(|_| Err(message_processor_error(&err)))
                .collect();
        }
    };
    let mut parsed_by_id = HashMap::new();
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let item: OpenAiBatchResultLine = match serde_json::from_str(line) {
            Ok(item) => item,
            Err(source) => {
                let err = ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(line),
                };
                return inputs
                    .iter()
                    .map(|_| Err(message_processor_error(&err)))
                    .collect();
            }
        };
        let custom_id = item.custom_id.clone();
        let item_result = match (item.response, item.error) {
            (Some(response), _) if response.status_code / 100 == 2 => {
                let usage = response
                    .body
                    .usage
                    .clone()
                    .and_then(InferenceUsage::from_openrouter_usage);
                Ok(OpenAiBatchParsedResult {
                    output_text: response.body.flatten_text(),
                    usage,
                })
            }
            (Some(response), _) => Err(ProcessorError::Message {
                message: format!(
                    "OpenAI batch item {} failed with status {}",
                    custom_id, response.status_code
                ),
            }),
            (_, Some(error)) => Err(ProcessorError::Message {
                message: format!("OpenAI batch item {} failed: {}", custom_id, error),
            }),
            _ => Err(ProcessorError::Message {
                message: format!("OpenAI batch item {} returned no response", custom_id),
            }),
        };
        parsed_by_id.insert(custom_id, item_result);
    }

    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let custom_id = input.batch_custom_id();
        match parsed_by_id.remove(&custom_id) {
            Some(Ok(result)) => outputs.push(parse(result, input)),
            Some(Err(err)) => outputs.push(Err(err)),
            None => outputs.push(Err(ProcessorError::Message {
                message: format!("OpenAI batch missing result for {}", custom_id),
            })),
        }
    }
    outputs
}

fn message_processor_error(err: &ProcessorError) -> ProcessorError {
    ProcessorError::Message {
        message: err.to_string(),
    }
}
