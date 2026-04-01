use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::*;

pub(super) struct GeminiBatchProcessor {
    pub(super) client: Arc<GeminiBatchClient>,
    pub(super) candidate_model: String,
}

pub(super) struct GeminiReconciliationBatchProcessor {
    pub(super) client: Arc<GeminiBatchClient>,
    pub(super) model: String,
}

pub(super) struct GeminiExtractionSubmitter {
    pub(super) client: Arc<GeminiBatchClient>,
    pub(super) candidate_model: String,
}

pub(super) struct GeminiReconciliationSubmitter {
    pub(super) client: Arc<GeminiBatchClient>,
    pub(super) model: String,
}

impl ArtifactBatchProcessor for GeminiBatchProcessor {
    fn max_batch_jobs(&self) -> usize {
        self.client.max_batch_jobs
    }

    fn max_batch_bytes(&self) -> usize {
        self.client.batch_max_bytes
    }

    fn can_process(&self, input: &ArtifactProcessorInput) -> bool {
        !should_shape_artifact_input(input)
    }

    fn estimate_size_bytes(&self, input: &ArtifactProcessorInput) -> Result<usize, ProcessorError> {
        let user_prompt =
            build_two_phase_candidate_user_prompt_with_flavor(input, PromptFlavor::Gemini)?;
        Ok(candidate_system_prompt(input).len() + user_prompt.len())
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
            match build_two_phase_candidate_user_prompt_with_flavor(input, PromptFlavor::Gemini) {
                Ok(user_prompt) => requests.push(GeminiBatchEnrichmentRequest {
                    key: input.artifact_id.clone(),
                    system_prompt: candidate_system_prompt(input).to_string(),
                    user_prompt,
                    response_json_schema: candidate_output_schema_with_allowed_refs(
                        input,
                        &allowed_artifact_evidence_refs(input),
                    ),
                    max_output_tokens: Some(TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS),
                }),
                Err(err) => {
                    return inputs
                        .iter()
                        .map(|_| {
                            Err(ProcessorError::Message {
                                message: err.to_string(),
                            })
                        })
                        .collect::<Vec<_>>();
                }
            }
        }

        let job = match self.client.submit_inline_batch(
            &self.candidate_model,
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
                    let input = inputs
                        .iter()
                        .find(|input| input.artifact_id == result.key)
                        .ok_or_else(|| ProcessorError::Message {
                            message: format!("Gemini batch returned unknown key {}", result.key),
                        })?;
                    let candidate = parse_candidate_output(&result.output_text, input)
                        .map_err(|err| attach_output_preview(err, &result.output_text))?;
                    Ok(candidate.into_processor_output(
                        input,
                        self.candidate_model.clone(),
                        result.usage,
                        "gemini_enrichment",
                        "gemini",
                        GEMINI_PROMPT_VERSION,
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
            let user_prompt =
                build_two_phase_candidate_user_prompt_with_flavor(input, PromptFlavor::Gemini)?;
            requests.push(GeminiBatchEnrichmentRequest {
                key: artifact_processor_batch_custom_id(input),
                system_prompt: candidate_system_prompt(input).to_string(),
                user_prompt,
                response_json_schema: candidate_output_schema_with_allowed_refs(
                    input,
                    &allowed_artifact_evidence_refs(input),
                ),
                max_output_tokens: Some(TWO_PHASE_CANDIDATE_MAX_OUTPUT_TOKENS),
            });
        }

        let job = self.client.submit_inline_batch(
            &self.candidate_model,
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
                let key = artifact_processor_batch_custom_id(input);
                let result = by_key.remove(&key).ok_or_else(|| ProcessorError::Message {
                    message: format!("Gemini batch returned no result for key {}", key),
                })?;
                let candidate = parse_candidate_output(&result.output_text, input)
                    .map_err(|err| attach_output_preview(err, &result.output_text))?;
                Ok(candidate.into_processor_output(
                    input,
                    self.candidate_model.clone(),
                    result.usage,
                    "gemini_enrichment",
                    "gemini",
                    GEMINI_PROMPT_VERSION,
                ))
            })
            .collect()
    }
}

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
    ) -> Vec<Result<Vec<ReconciliationDecisionOutput>, ProcessorError>> {
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
                let parsed: ModelReconciliationOutput = serde_json::from_str(&result.output_text)
                    .map_err(|source| {
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
                parsed.validate_against(input)?;
                Ok(parsed.into_outputs())
            })
            .collect()
    }
}
