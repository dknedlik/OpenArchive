use std::sync::Arc;

use super::*;

pub(super) struct GeminiReconciliationBatchProcessor {
    pub(super) client: Arc<GeminiBatchClient>,
    pub(super) model: String,
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
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>> {
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
                    let parsed: ModelReconciliationOutput =
                        serde_json::from_str(&result.output_text).map_err(|source| {
                            ProcessorError::ParseModelJson {
                                source,
                                body_preview: preview(&result.output_text),
                            }
                        })?;
                    let input = inputs
                        .iter()
                        .find(|input| input.artifact_id == result.key)
                        .ok_or_else(|| ProcessorError::Message {
                            message: format!("Gemini batch returned unknown key {}", result.key),
                        })?;
                    parsed.into_validated_outputs(input)
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
