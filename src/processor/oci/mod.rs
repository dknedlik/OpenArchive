use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use rand::random;

use crate::config::OciConfig;
use crate::storage::types::EnrichmentTier;

use super::*;

pub struct OciProcessorFactory {
    client: Arc<OciClient>,
    stage1_max_output_tokens: u32,
    stage2_max_output_tokens: u32,
    repair_max_output_tokens: u32,
    heavy_model: String,
    fast_model: String,
}

impl OciProcessorFactory {
    pub fn new(config: OciConfig) -> Result<Self, String> {
        let client = Arc::new(OciClient::new(config));
        Ok(Self {
            stage1_max_output_tokens: client
                .config
                .repair_max_output_tokens
                .max(client.config.max_output_tokens),
            stage2_max_output_tokens: client.config.max_output_tokens,
            repair_max_output_tokens: client
                .config
                .repair_max_output_tokens
                .max(client.config.max_output_tokens),
            heavy_model: client.config.heavy_model.clone(),
            fast_model: client.config.fast_model.clone(),
            client,
        })
    }
}

impl ArtifactProcessorFactory for OciProcessorFactory {
    fn build(&self, _tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        let client: Arc<dyn InferenceClient> = self.client.clone();
        Ok(Box::new(ExtractionPipelineProcessor {
            client,
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.repair_max_output_tokens),
            pipeline_name: "oci_extraction_pipeline",
            provider_name: "oci",
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
        let client: Arc<dyn InferenceClient> = self.client.clone();
        let processor: Box<dyn ArtifactProcessor> = Box::new(ExtractionPipelineProcessor {
            client,
            extract_model: self.heavy_model.clone(),
            format_model: self.fast_model.clone(),
            stage1_max_output_tokens: Some(self.stage1_max_output_tokens),
            stage2_max_output_tokens: Some(self.stage2_max_output_tokens),
            repair_max_output_tokens: Some(self.repair_max_output_tokens),
            pipeline_name: "oci_extraction_pipeline",
            provider_name: "oci",
        });
        Ok(Some(Box::new(SequentialArtifactBatchProcessor::new(
            processor, 8, 2_000_000,
        ))))
    }
}

struct OciClient {
    config: OciConfig,
}

impl OciClient {
    fn new(config: OciConfig) -> Self {
        Self { config }
    }

    fn complete_json_with_max_output_tokens(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        let payload = match request_format_for_model(model) {
            OciRequestFormat::Cohere => serde_json::json!({
                "compartmentId": self.config.compartment_id,
                "servingMode": {
                    "servingType": "ON_DEMAND",
                    "modelId": model
                },
                "chatRequest": {
                    "apiFormat": "COHERE",
                    "preambleOverride": system_prompt,
                    "message": user_prompt,
                    "maxTokens": effective_max_tokens_for_model(model, max_output_tokens),
                    "temperature": 0,
                    "responseFormat": oci_response_format_for_model(model, schema),
                }
            }),
            OciRequestFormat::Generic => serde_json::json!({
                "compartmentId": self.config.compartment_id,
                "servingMode": {
                    "servingType": "ON_DEMAND",
                    "modelId": model
                },
                "chatRequest": {
                    "apiFormat": "GENERIC",
                    "messages": [
                        {
                            "role": "USER",
                            "content": [{
                                "type": "TEXT",
                                "text": format!("{system_prompt}\n\n{user_prompt}")
                            }]
                        }
                    ],
                    "maxTokens": effective_max_tokens_for_model(model, max_output_tokens),
                    "temperature": 0,
                    "responseFormat": oci_response_format_for_model(model, schema),
                }
            }),
        };

        let response = self.invoke_cli(&payload)?;
        parse_oci_chat_response(model, &response)
    }

    fn invoke_cli(&self, payload: &serde_json::Value) -> Result<serde_json::Value, ProcessorError> {
        let temp_path = self.write_temp_payload(payload)?;
        let mut command = Command::new(&self.config.cli_path);
        if let Some(profile) = &self.config.profile {
            command.arg("--profile").arg(profile);
        }
        let output = command
            .args([
                "generative-ai-inference",
                "chat-result",
                "chat",
                "--region",
                &self.config.region,
                "--from-json",
            ])
            .arg(format!("file://{}", temp_path.display()))
            .args(["--output", "json"])
            .output()
            .map_err(|source| ProcessorError::Message {
                message: format!(
                    "failed to execute OCI CLI {}: {source}",
                    self.config.cli_path
                ),
            })?;

        let _ = fs::remove_file(&temp_path);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ProcessorError::Message {
                message: format!(
                    "OCI CLI inference failed (exit {:?}): {}",
                    output.status.code(),
                    stderr.trim()
                ),
            });
        }

        serde_json::from_slice(&output.stdout).map_err(|source| {
            ProcessorError::ParseInferenceResponse {
                source,
                body_preview: preview(&String::from_utf8_lossy(&output.stdout)),
            }
        })
    }

    fn write_temp_payload(&self, payload: &serde_json::Value) -> Result<PathBuf, ProcessorError> {
        let path = std::env::temp_dir().join(format!(
            "openarchive-oci-{}-{}.json",
            std::process::id(),
            random::<u64>()
        ));
        let bytes = serde_json::to_vec(payload)
            .map_err(|source| ProcessorError::SerializePrompt { source })?;
        fs::write(&path, bytes).map_err(|source| ProcessorError::Message {
            message: format!(
                "failed to write OCI CLI payload {}: {source}",
                path.display()
            ),
        })?;
        Ok(path)
    }
}

fn effective_max_tokens_for_model(model: &str, requested: u32) -> u32 {
    if model.starts_with("meta.llama-") {
        requested.min(4096)
    } else {
        requested
    }
}

fn oci_response_format_for_model(model: &str, schema: &serde_json::Value) -> serde_json::Value {
    match request_format_for_model(model) {
        OciRequestFormat::Cohere => cohere_response_format(schema),
        OciRequestFormat::Generic => generic_response_format(schema),
    }
}

fn generic_response_format(schema: &serde_json::Value) -> serde_json::Value {
    let wrapped_type = schema
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match wrapped_type {
        "text" => serde_json::json!({ "type": "TEXT" }),
        "json_object" => serde_json::json!({ "type": "JSON_OBJECT" }),
        "json_schema" => {
            let name = schema
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("openarchive_structured_output");
            let is_strict = schema
                .get("strict")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let inner = sanitize_oci_response_schema(schema.get("schema").unwrap_or(schema));
            serde_json::json!({
                "type": "JSON_SCHEMA",
                "jsonSchema": {
                    "name": name,
                    "isStrict": is_strict,
                    "schema": inner
                }
            })
        }
        _ => serde_json::json!({
            "type": "JSON_SCHEMA",
            "jsonSchema": {
                "name": "openarchive_structured_output",
                "isStrict": true,
                "schema": sanitize_oci_response_schema(schema)
            }
        }),
    }
}

fn cohere_response_format(schema: &serde_json::Value) -> serde_json::Value {
    let wrapped_type = schema
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match wrapped_type {
        "text" => serde_json::json!({ "type": "TEXT" }),
        _ => serde_json::json!({
            "type": "JSON_OBJECT",
            "schema": sanitize_oci_response_schema(schema.get("schema").unwrap_or(schema))
        }),
    }
}

fn sanitize_oci_response_schema(schema: &serde_json::Value) -> serde_json::Value {
    match schema {
        serde_json::Value::Object(map) => {
            let mut sanitized = serde_json::Map::new();
            for (key, value) in map {
                if matches!(
                    key.as_str(),
                    "strict"
                        | "name"
                        | "schema"
                        | "minLength"
                        | "maxLength"
                        | "minItems"
                        | "maxItems"
                        | "minimum"
                        | "maximum"
                        | "pattern"
                        | "default"
                        | "description"
                ) {
                    continue;
                }
                sanitized.insert(key.clone(), sanitize_oci_response_schema(value));
            }
            serde_json::Value::Object(sanitized)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(sanitize_oci_response_schema).collect())
        }
        _ => schema.clone(),
    }
}

impl InferenceClient for OciClient {
    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_text_with_max_output_tokens_override(
            model,
            system_prompt,
            user_prompt,
            self.config.max_output_tokens,
        )
    }

    fn complete_text_with_max_output_tokens_override(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<InferenceResult, ProcessorError> {
        let payload = match request_format_for_model(model) {
            OciRequestFormat::Cohere => serde_json::json!({
                "compartmentId": self.config.compartment_id,
                "servingMode": {
                    "servingType": "ON_DEMAND",
                    "modelId": model
                },
                "chatRequest": {
                    "apiFormat": "COHERE",
                    "preambleOverride": system_prompt,
                    "message": user_prompt,
                    "maxTokens": effective_max_tokens_for_model(model, max_output_tokens),
                    "temperature": 0
                }
            }),
            OciRequestFormat::Generic => serde_json::json!({
                "compartmentId": self.config.compartment_id,
                "servingMode": {
                    "servingType": "ON_DEMAND",
                    "modelId": model
                },
                "chatRequest": {
                    "apiFormat": "GENERIC",
                    "messages": [
                        {
                            "role": "USER",
                            "content": [{
                                "type": "TEXT",
                                "text": format!("{system_prompt}\n\n{user_prompt}")
                            }]
                        }
                    ],
                    "maxTokens": effective_max_tokens_for_model(model, max_output_tokens),
                    "temperature": 0
                }
            }),
        };

        let response = self.invoke_cli(&payload)?;
        parse_oci_chat_response(model, &response)
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<InferenceResult, ProcessorError> {
        self.complete_json_with_max_output_tokens_override(
            model,
            system_prompt,
            user_prompt,
            schema,
            self.config.max_output_tokens,
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
        self.complete_json_with_max_output_tokens(
            model,
            system_prompt,
            user_prompt,
            schema,
            max_output_tokens,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OciRequestFormat {
    Generic,
    Cohere,
}

fn request_format_for_model(model: &str) -> OciRequestFormat {
    if model.starts_with("cohere.") {
        OciRequestFormat::Cohere
    } else {
        OciRequestFormat::Generic
    }
}

fn parse_oci_chat_response(
    model: &str,
    value: &serde_json::Value,
) -> Result<InferenceResult, ProcessorError> {
    let response = value
        .get("data")
        .and_then(|value| value.get("chat-response"))
        .ok_or_else(|| ProcessorError::InvalidModelOutput {
            detail: "OCI response missing data.chat-response".to_string(),
        })?;

    let raw_output = match request_format_for_model(model) {
        OciRequestFormat::Cohere => response
            .get("text")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        OciRequestFormat::Generic => extract_generic_output_text(response),
    }
    .ok_or_else(|| ProcessorError::InvalidModelOutput {
        detail: "OCI response did not contain model text output".to_string(),
    })?;

    let usage = response
        .get("usage")
        .map(parse_usage_from_value)
        .transpose()?;

    Ok(InferenceResult {
        output_text: normalize_model_output_text(&raw_output),
        usage,
    })
}

fn extract_generic_output_text(response: &serde_json::Value) -> Option<String> {
    let choice = response.get("choices")?.as_array()?.first()?;
    let message = choice.get("message")?;
    if let Some(content) = message.get("content").and_then(serde_json::Value::as_array) {
        let text = content
            .iter()
            .filter_map(|item| item.get("text").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        if !text.trim().is_empty() {
            return Some(text);
        }
    }
    message
        .get("reasoning-content")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn parse_usage_from_value(value: &serde_json::Value) -> Result<InferenceUsage, ProcessorError> {
    let prompt_tokens = value
        .get("prompt-tokens")
        .and_then(serde_json::Value::as_u64);
    let output_tokens = value
        .get("completion-tokens")
        .and_then(serde_json::Value::as_u64);
    let total_tokens = value
        .get("total-tokens")
        .and_then(serde_json::Value::as_u64);
    let reasoning_tokens = value
        .get("completion-tokens-details")
        .and_then(|value| value.get("reasoning-tokens"))
        .and_then(serde_json::Value::as_u64);

    Ok(InferenceUsage {
        input_tokens: prompt_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
        reported_cost_micros: None,
    })
}

fn normalize_model_output_text(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(fenced) = extract_fenced_json_block(trimmed) {
        return fenced;
    }
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return trimmed.to_string();
    }
    if let Some(extracted) = extract_braced_json(trimmed) {
        return extracted;
    }
    trimmed.to_string()
}

#[cfg(test)]
fn apply_oci_output_contract_overlay(prompt: &str) -> String {
    format!(
        "{prompt}\n\
         OCI output contract:\n\
         - return ONLY valid JSON with the exact required top-level fields and exact field names from the schema\n\
         - include every required top-level field even when a section is empty; use [] for empty candidate arrays\n\
         - importance_score MUST be an integer from 1 to 10 inclusive\n\
         - importance_score may never be 0, null, omitted, or a string\n\
         - do not wrap the JSON in markdown fences or add explanatory text before or after it\n"
    )
}

fn extract_fenced_json_block(text: &str) -> Option<String> {
    let fence_start = text.find("```")?;
    let after_fence = &text[fence_start + 3..];
    let content_start = after_fence.find('\n').map(|idx| idx + 1).unwrap_or(0);
    let remaining = &after_fence[content_start..];
    let fence_end = remaining.find("```")?;
    Some(remaining[..fence_end].trim().to_string())
}

fn extract_braced_json(text: &str) -> Option<String> {
    let object = text
        .find('{')
        .and_then(|start| text.rfind('}').map(|end| (start, end)))
        .filter(|(start, end)| end >= start)
        .map(|(start, end)| text[start..=end].trim().to_string());
    if object.is_some() {
        return object;
    }

    text.find('[')
        .and_then(|start| text.rfind(']').map(|end| (start, end)))
        .filter(|(start, end)| end >= start)
        .map(|(start, end)| text[start..=end].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cohere_models_use_cohere_request_format() {
        assert_eq!(
            request_format_for_model("cohere.command-a-03-2025"),
            OciRequestFormat::Cohere
        );
    }

    #[test]
    fn non_cohere_models_use_generic_request_format() {
        assert_eq!(
            request_format_for_model("meta.llama-3.3-70b-instruct"),
            OciRequestFormat::Generic
        );
        assert_eq!(
            request_format_for_model("google.gemini-2.5-flash"),
            OciRequestFormat::Generic
        );
        assert_eq!(
            request_format_for_model("xai.grok-4-1-fast-reasoning"),
            OciRequestFormat::Generic
        );
    }

    #[test]
    fn normalizer_strips_fenced_json() {
        let raw = "Here is the JSON you asked for:\n```json\n{\"a\":1}\n```";
        assert_eq!(normalize_model_output_text(raw), "{\"a\":1}");
    }

    #[test]
    fn normalizer_extracts_braced_json_from_preface() {
        let raw = "Answer:\n{\"summary\":\"ok\"}";
        assert_eq!(normalize_model_output_text(raw), "{\"summary\":\"ok\"}");
    }

    #[test]
    fn generic_response_format_uses_json_schema_wrapper() {
        let schema = serde_json::json!({
            "type": "json_schema",
            "name": "candidate_output",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "summary": { "type": "string" }
                }
            }
        });
        assert_eq!(
            generic_response_format(&schema),
            serde_json::json!({
                "type": "JSON_SCHEMA",
                "jsonSchema": {
                    "name": "candidate_output",
                    "isStrict": true,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "summary": { "type": "string" }
                        }
                    }
                }
            })
        );
    }

    #[test]
    fn cohere_response_format_unwraps_schema() {
        let schema = serde_json::json!({
            "type": "json_schema",
            "name": "candidate_output",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "summary": { "type": "string" }
                }
            }
        });
        assert_eq!(
            cohere_response_format(&schema),
            serde_json::json!({
                "type": "JSON_OBJECT",
                "schema": {
                    "type": "object",
                    "properties": {
                        "summary": { "type": "string" }
                    }
                }
            })
        );
    }

    #[test]
    fn oci_schema_sanitizer_removes_unsupported_constraints() {
        let schema = serde_json::json!({
            "type": "object",
            "description": "candidate output",
            "properties": {
                "summary": {
                    "type": "string",
                    "minLength": 1
                },
                "items": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": {
                        "type": "string",
                        "maxLength": 10
                    }
                }
            }
        });
        assert_eq!(
            sanitize_oci_response_schema(&schema),
            serde_json::json!({
                "type": "object",
                "properties": {
                    "summary": {
                        "type": "string"
                    },
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            })
        );
    }

    #[test]
    fn oci_overlay_forces_importance_score_constraints() {
        let prompt = apply_oci_output_contract_overlay("base prompt");
        assert!(prompt.contains("importance_score MUST be an integer from 1 to 10 inclusive"));
        assert!(prompt.contains("importance_score may never be 0"));
        assert!(prompt.contains("include every required top-level field"));
    }

    #[test]
    fn llama_models_are_capped_to_4096() {
        assert_eq!(
            effective_max_tokens_for_model("meta.llama-3.3-70b-instruct", 8000),
            4096
        );
    }

    #[test]
    fn gemini_models_preserve_higher_requested_budgets() {
        assert_eq!(
            effective_max_tokens_for_model("google.gemini-2.5-flash", 8000),
            8000
        );
    }
}
