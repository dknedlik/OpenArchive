use std::sync::Arc;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

use crate::config::GrokConfig;
use crate::storage::types::EnrichmentTier;

use super::*;

mod batch;

use batch::{GrokExtractionSubmitter, GrokReconciliationSubmitter};

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
                body_preview: preview(&response_text),
                retry_after_seconds: None,
            });
        }

        let parsed: OpenRouterResponsesResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                ProcessorError::ParseInferenceResponse {
                    source,
                    body_preview: preview(&response_text),
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

    fn create_batch(&self, name: &str) -> Result<batch::GrokBatch, ProcessorError> {
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
        batch::parse_grok_json_response(response)
    }

    fn add_batch_requests(
        &self,
        batch_id: &str,
        requests: &[batch::GrokBatchRequestEnvelope],
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
        let _: serde_json::Value = batch::parse_grok_json_response(response)?;
        Ok(())
    }

    fn get_batch(&self, batch_id: &str) -> Result<batch::GrokBatch, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/batches/{}", self.base_url, batch_id))
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;
        batch::parse_grok_json_response(response)
    }
    fn list_results(
        &self,
        batch_id: &str,
    ) -> Result<Vec<batch::GrokBatchResultItem>, ProcessorError> {
        let mut all = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .get(format!("{}/batches/{}/results", self.base_url, batch_id));
            if let Some(token) = &page_token {
                request = request.query(&[("page_token", token)]);
            }
            let page: batch::GrokBatchResultsPage = batch::parse_grok_json_response(
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
            .map_err(|err| attach_output_preview(err, &candidate_result.output_text))?;
        Ok(candidate.into_processor_output(
            input,
            self.candidate_model.clone(),
            candidate_result.usage,
            "grok_enrichment",
            "grok",
            GROK_PROMPT_VERSION,
        ))
    }
}
