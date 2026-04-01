use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::*;

pub(super) struct GrokExtractionSubmitter {
    pub(super) client: Arc<GrokClient>,
    pub(super) candidate_model: String,
}

pub(super) struct GrokReconciliationSubmitter {
    pub(super) client: Arc<GrokClient>,
    pub(super) model: String,
}

impl ExtractionBatchSubmitter for GrokExtractionSubmitter {
    fn max_batch_size(&self) -> usize {
        10_000
    }

    fn prepare_and_submit(
        &self,
        inputs: &[ArtifactProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let batch = self.client.create_batch("openarchive-enrichment")?;
        let requests = build_grok_requests(inputs, |input| {
            Ok(self.client.build_chat_completion_body(
                &self.candidate_model,
                candidate_system_prompt(input),
                &build_two_phase_candidate_user_prompt(input)?,
                &candidate_output_schema_with_allowed_refs(
                    input,
                    &allowed_artifact_evidence_refs(input),
                ),
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
        inputs: &[ArtifactProcessorInput],
    ) -> Vec<Result<ArtifactProcessorOutput, ProcessorError>> {
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
                    "grok_enrichment",
                    "grok",
                    GROK_PROMPT_VERSION,
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
        inputs: &[ReconciliationProcessorInput],
    ) -> Result<BatchHandle, ProcessorError> {
        let batch = self.client.create_batch("openarchive-reconciliation")?;
        let requests = build_grok_requests(inputs, |input| {
            Ok(self.client.build_chat_completion_body(
                &self.model,
                RECONCILIATION_SYSTEM_PROMPT,
                &build_reconciliation_prompt(input)?,
                &reconciliation_output_schema(),
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
        inputs: &[ReconciliationProcessorInput],
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>> {
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

fn build_grok_requests<I, F>(
    inputs: &[I],
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
                    retry_after_seconds: None,
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
                        preview(&item.debug_snapshot())
                    ),
                    retry_after_seconds: None,
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
        .and_then(|usage| serde_json::from_value::<OpenRouterUsage>(usage).ok())
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

impl GrokBatchInput for ArtifactProcessorInput {
    fn batch_custom_id(&self) -> String {
        artifact_processor_batch_custom_id(self)
    }
}

impl GrokBatchInput for ReconciliationProcessorInput {
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

pub(super) fn parse_grok_json_response<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> Result<T, ProcessorError> {
    let status = response.status();
    let response_text = response
        .text()
        .map_err(|source| ProcessorError::ReadInferenceResponse { source })?;
    if !status.is_success() {
        return Err(ProcessorError::InferenceHttpStatus {
            status: status.as_u16(),
            body_preview: preview(&response_text),
            retry_after_seconds: None,
        });
    }
    serde_json::from_str(&response_text).map_err(|source| ProcessorError::ParseInferenceResponse {
        source,
        body_preview: preview(&response_text),
    })
}

#[derive(Debug, Deserialize)]
pub(super) struct GrokBatch {
    #[serde(alias = "batch_id")]
    pub(super) id: String,
    pub(super) state: GrokBatchState,
}

#[derive(Debug, Deserialize)]
pub(super) struct GrokBatchState {
    #[serde(default)]
    pub(super) num_pending: usize,
}

#[derive(Debug, Serialize)]
pub(super) struct GrokBatchRequestEnvelope {
    batch_request_id: String,
    batch_request: GrokBatchRequestBody,
}

#[derive(Debug, Serialize)]
struct GrokBatchRequestBody {
    chat_get_completion: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct GrokBatchResultsPage {
    #[serde(default)]
    pub(super) results: Vec<GrokBatchResultItem>,
    #[serde(default, alias = "pagination_token")]
    pub(super) next_page_token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct GrokBatchResultItem {
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
