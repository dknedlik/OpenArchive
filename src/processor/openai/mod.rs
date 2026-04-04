use std::sync::Arc;
use std::time::Duration;

use reqwest::blocking::{multipart, Client};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

use crate::config::{OpenAiConfig, OpenAiReasoningEffort};
use crate::storage::types::EnrichmentTier;

use super::*;

mod batch;
mod types;

use batch::{
    OpenAiBatchJob, OpenAiBatchRequest, OpenAiExtractionSubmitter, OpenAiFileObject,
    OpenAiReconciliationSubmitter,
};
use types::OpenRouterResponsesReasoningConfig;

pub(crate) use types::{
    OpenRouterResponsesContentItem, OpenRouterResponsesInputItem, OpenRouterResponsesRequest,
    OpenRouterResponsesResponse, OpenRouterResponsesTextConfig, OpenRouterUsage,
};

pub struct OpenAiProcessorFactory {
    client: Arc<OpenAiClient>,
    batch_client: Option<Arc<OpenAiClient>>,
    stage1_max_output_tokens: u32,
    stage2_max_output_tokens: u32,
    repair_max_output_tokens: u32,
    heavy_model: String,
    fast_model: String,
}

impl OpenAiProcessorFactory {
    pub fn new(config: OpenAiConfig) -> Result<Self, String> {
        let client = Arc::new(OpenAiClient::new(&config).map_err(|err| err.to_string())?);
        Ok(Self {
            client: client.clone(),
            batch_client: Some(client),
            stage1_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            stage2_max_output_tokens: config.max_output_tokens,
            repair_max_output_tokens: config
                .repair_max_output_tokens
                .max(config.max_output_tokens),
            heavy_model: config.heavy_model,
            fast_model: config.fast_model,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_client(
        client: Arc<dyn InferenceClient>,
        heavy_model: impl Into<String>,
        fast_model: impl Into<String>,
    ) -> Self {
        let heavy_model = heavy_model.into();
        let fast_model = fast_model.into();
        Self {
            client: Arc::new(OpenAiClient::for_tests(client)),
            batch_client: None,
            stage1_max_output_tokens: 4000,
            stage2_max_output_tokens: 4000,
            repair_max_output_tokens: 4000,
            heavy_model,
            fast_model,
        }
    }
}

impl ArtifactProcessorFactory for OpenAiProcessorFactory {
    fn build(&self, _tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        Ok(Box::new(ExtractionPipelineProcessor {
            client: self.client.clone(),
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.repair_max_output_tokens),
            pipeline_name: "openai_extraction_pipeline",
            provider_name: "openai",
        }))
    }

    fn build_reconciliation_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(HostedReconciliationProcessor {
            client,
            model: self.fast_model.clone(),
            system_prompt: RECONCILIATION_SYSTEM_PROMPT,
        }))
    }

    fn build_batch_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ArtifactBatchProcessor>>, ProcessorError> {
        let processor: Box<dyn ArtifactProcessor> = Box::new(ExtractionPipelineProcessor {
            client: self.client.clone(),
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.repair_max_output_tokens),
            pipeline_name: "openai_extraction_pipeline",
            provider_name: "openai",
        });
        Ok(Some(Box::new(SequentialArtifactBatchProcessor::new(
            processor, 16, 2_000_000,
        ))))
    }

    fn build_extraction_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ExtractionBatchSubmitter>>, ProcessorError> {
        let Some(client) = &self.batch_client else {
            return Ok(None);
        };
        Ok(Some(Box::new(OpenAiExtractionSubmitter {
            client: Arc::clone(client),
            candidate_model: self.heavy_model.clone(),
        })))
    }

    fn build_reconciliation_submitter(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Option<Box<dyn ReconciliationBatchSubmitter>>, ProcessorError> {
        let Some(client) = &self.batch_client else {
            return Ok(None);
        };
        Ok(Some(Box::new(OpenAiReconciliationSubmitter {
            client: Arc::clone(client),
            model: self.fast_model.clone(),
        })))
    }
}

struct OpenAiClient {
    delegate: Option<Arc<dyn InferenceClient>>,
    client: Client,
    base_url: String,
    max_output_tokens: u32,
    reasoning_effort_override: OpenAiReasoningEffort,
}

impl OpenAiClient {
    fn new(config: &OpenAiConfig) -> Result<Self, ProcessorError> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let bearer = format!("Bearer {}", config.api_key);
        let auth_value = HeaderValue::from_str(&bearer).map_err(|err| ProcessorError::Message {
            message: format!("invalid OpenAI API key header: {err}"),
        })?;
        default_headers.insert(AUTHORIZATION, auth_value);

        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(180))
            .build()
            .map_err(|source| ProcessorError::BuildHttpClient { source })?;

        Ok(Self {
            delegate: None,
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            max_output_tokens: config.max_output_tokens,
            reasoning_effort_override: config.reasoning_effort_override,
        })
    }

    #[cfg(test)]
    fn for_tests(delegate: Arc<dyn InferenceClient>) -> Self {
        Self {
            delegate: Some(delegate),
            client: Client::builder().build().expect("test http client"),
            base_url: "https://api.openai.com/v1".to_string(),
            max_output_tokens: 4000,
            reasoning_effort_override: OpenAiReasoningEffort::Auto,
        }
    }
}

impl InferenceClient for OpenAiClient {
    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        if let Some(delegate) = &self.delegate {
            return delegate.complete_text(model, system_prompt, user_prompt);
        }
        self.complete_via_responses(
            model,
            system_prompt,
            user_prompt,
            &serde_json::json!({ "type": "text" }),
        )
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        if let Some(delegate) = &self.delegate {
            return delegate.complete_json(model, system_prompt, user_prompt, schema);
        }
        self.complete_via_responses(model, system_prompt, user_prompt, schema)
    }

    fn complete_text_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        if let Some(delegate) = &self.delegate {
            return delegate.complete_text_with_max_output_tokens_override(
                model,
                system_prompt,
                user_prompt,
                max_output_tokens,
            );
        }
        self.complete_via_responses_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            &serde_json::json!({ "type": "text" }),
            max_output_tokens,
        )
    }

    fn complete_json_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        if let Some(delegate) = &self.delegate {
            return delegate.complete_json_with_max_output_tokens_override(
                model,
                system_prompt,
                user_prompt,
                schema,
                max_output_tokens,
            );
        }
        self.complete_via_responses_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            max_output_tokens,
        )
    }
}

impl OpenAiClient {
    fn responses_text_format(schema: &serde_json::Value) -> serde_json::Value {
        let format_type = schema
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match format_type {
            "json_schema" => {
                let mut wrapped = schema.clone();
                if let Some(inner) = wrapped.get_mut("schema") {
                    Self::normalize_schema_for_openai(inner);
                }
                wrapped
            }
            "json_object" | "text" => schema.clone(),
            _ => {
                let mut normalized = schema.clone();
                Self::normalize_schema_for_openai(&mut normalized);
                serde_json::json!({
                    "type": "json_schema",
                    "name": "openarchive_structured_output",
                    "strict": true,
                    "schema": normalized,
                })
            }
        }
    }

    fn normalize_schema_for_openai(schema: &mut serde_json::Value) {
        match schema {
            serde_json::Value::Object(map) => {
                let required_names: std::collections::HashSet<String> = map
                    .get("required")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                if let Some(properties) = map
                    .get_mut("properties")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    let property_names: Vec<String> = properties.keys().cloned().collect();
                    for property_name in &property_names {
                        if let Some(property_schema) = properties.get_mut(property_name) {
                            Self::normalize_schema_for_openai(property_schema);
                            if !required_names.contains(property_name) {
                                Self::make_schema_nullable(property_schema);
                            }
                        }
                    }
                    map.insert(
                        "required".to_string(),
                        serde_json::Value::Array(
                            property_names
                                .into_iter()
                                .map(serde_json::Value::String)
                                .collect(),
                        ),
                    );
                }

                if let Some(items) = map.get_mut("items") {
                    Self::normalize_schema_for_openai(items);
                }

                if let Some(schema) = map.get_mut("schema") {
                    Self::normalize_schema_for_openai(schema);
                }

                if let Some(any_of) = map
                    .get_mut("anyOf")
                    .and_then(serde_json::Value::as_array_mut)
                {
                    for variant in any_of {
                        Self::normalize_schema_for_openai(variant);
                    }
                }

                if let Some(one_of) = map
                    .get_mut("oneOf")
                    .and_then(serde_json::Value::as_array_mut)
                {
                    for variant in one_of {
                        Self::normalize_schema_for_openai(variant);
                    }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    Self::normalize_schema_for_openai(item);
                }
            }
            _ => {}
        }
    }

    fn make_schema_nullable(schema: &mut serde_json::Value) {
        let Some(map) = schema.as_object_mut() else {
            return;
        };
        match map.get_mut("type") {
            Some(serde_json::Value::String(existing)) => {
                if existing != "null" {
                    let original = existing.clone();
                    *schema = serde_json::json!({
                        "anyOf": [
                            { "type": original },
                            { "type": "null" }
                        ]
                    });
                }
            }
            Some(serde_json::Value::Array(items)) => {
                let has_null = items.iter().any(|item| item.as_str() == Some("null"));
                if !has_null {
                    items.push(serde_json::Value::String("null".to_string()));
                }
            }
            _ => {
                if !map.contains_key("anyOf") {
                    let original = schema.clone();
                    *schema = serde_json::json!({
                        "anyOf": [
                            original,
                            { "type": "null" }
                        ]
                    });
                }
            }
        }
    }

    fn complete_via_responses(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_via_responses_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            self.max_output_tokens,
        )
    }

    fn complete_via_responses_with_max_output_tokens(
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
            reasoning: self.reasoning_effort_for_model(model).map(|effort| {
                OpenRouterResponsesReasoningConfig {
                    effort: Some(effort.as_str()),
                }
            }),
            text: OpenRouterResponsesTextConfig {
                format: Self::responses_text_format(schema),
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
                message: "OpenAI responses returned empty content".to_string(),
            });
        }

        Ok(InferenceResult {
            output_text: content,
            usage,
        })
    }

    fn build_responses_request(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "model": model,
            "max_output_tokens": self.max_output_tokens,
            "text": {
                "format": Self::responses_text_format(schema)
            },
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": system_prompt
                        }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": user_prompt
                        }
                    ]
                }
            ]
        });
        if let Some(effort) = self.reasoning_effort_for_model(model) {
            body["reasoning"] = serde_json::json!({
                "effort": effort.as_str()
            });
        }
        body
    }

    fn submit_responses_batch(
        &self,
        display_name: Option<&str>,
        requests: &[OpenAiBatchRequest],
    ) -> Result<OpenAiBatchJob, ProcessorError> {
        let mut jsonl = String::new();
        for request in requests {
            let line = serde_json::to_string(request)
                .map_err(|source| ProcessorError::SerializePrompt { source })?;
            jsonl.push_str(&line);
            jsonl.push('\n');
        }

        let file_id = self.upload_batch_file(jsonl)?;
        let body = serde_json::json!({
            "input_file_id": file_id,
            "endpoint": "/v1/responses",
            "completion_window": "24h",
            "metadata": display_name.map(|name| serde_json::json!({ "name": name })),
        });
        let request_body = serde_json::to_vec(&body)
            .map_err(|source| ProcessorError::SerializePrompt { source })?;

        let response = self
            .client
            .post(format!("{}/batches", self.base_url))
            .body(request_body)
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        Self::parse_json_response(response)
    }

    fn upload_batch_file(&self, content: String) -> Result<String, ProcessorError> {
        let part = multipart::Part::text(content)
            .file_name("openarchive-batch.jsonl")
            .mime_str("application/jsonl")
            .map_err(|err| ProcessorError::Message {
                message: format!("invalid OpenAI batch upload mime type: {err}"),
            })?;
        let form = multipart::Form::new()
            .text("purpose", "batch")
            .part("file", part);

        let response = self
            .client
            .post(format!("{}/files", self.base_url))
            .multipart(form)
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        let file: OpenAiFileObject = Self::parse_json_response(response)?;
        Ok(file.id)
    }

    fn get_batch(&self, batch_id: &str) -> Result<OpenAiBatchJob, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/batches/{}", self.base_url, batch_id))
            .send()
            .map_err(|source| ProcessorError::SendInferenceRequest { source })?;

        Self::parse_json_response(response)
    }

    fn read_file_text(&self, file_id: &str) -> Result<String, ProcessorError> {
        let response = self
            .client
            .get(format!("{}/files/{}/content", self.base_url, file_id))
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
        Ok(response_text)
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
                body_preview: preview(&response_text),
                retry_after_seconds: None,
            });
        }
        serde_json::from_str(&response_text).map_err(|source| {
            ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(&response_text),
            }
        })
    }

    fn reasoning_effort_for_model(&self, model: &str) -> Option<OpenAiReasoningEffort> {
        if model.to_ascii_lowercase().starts_with("gpt-4.1") {
            return None;
        }
        if self.reasoning_effort_override != OpenAiReasoningEffort::Auto {
            return Some(self.reasoning_effort_override);
        }

        let model = model.to_ascii_lowercase();
        if model.contains("gpt-5.4-mini") {
            return Some(OpenAiReasoningEffort::Medium);
        }
        if model.contains("gpt-5.4-nano") {
            return Some(OpenAiReasoningEffort::None);
        }
        if model.contains("gpt-5.4") {
            return Some(OpenAiReasoningEffort::Low);
        }
        if model.contains("gpt-5-mini") {
            return Some(OpenAiReasoningEffort::Medium);
        }
        if model.contains("gpt-5-nano") {
            return Some(OpenAiReasoningEffort::Minimal);
        }
        if model.contains("gpt-5") {
            return Some(OpenAiReasoningEffort::Low);
        }

        None
    }
}
