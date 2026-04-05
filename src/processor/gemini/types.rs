use serde::{Deserialize, Serialize};

use super::InferenceUsage;

#[derive(Debug, Clone)]
pub struct GeminiBatchEnrichmentRequest {
    pub key: String,
    pub system_prompt: String,
    pub user_prompt: String,
    pub response_json_schema: serde_json::Value,
    pub max_output_tokens: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiBatchJob {
    pub name: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default, rename = "displayName")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, rename = "createTime")]
    pub create_time: Option<String>,
    #[serde(default, rename = "updateTime")]
    pub update_time: Option<String>,
    #[serde(default)]
    pub dest: Option<GeminiBatchDestination>,
}

impl InferenceUsage {
    pub(super) fn from_gemini_usage(usage: GeminiUsageMetadata) -> Option<Self> {
        if usage.prompt_token_count.is_none()
            && usage.candidates_token_count.is_none()
            && usage.total_token_count.is_none()
            && usage.thoughts_token_count.is_none()
        {
            return None;
        }

        Some(Self {
            input_tokens: usage.prompt_token_count,
            output_tokens: usage.candidates_token_count,
            reasoning_tokens: usage.thoughts_token_count,
            total_tokens: usage.total_token_count,
            reported_cost_micros: None,
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiGenerateContentRequest {
    pub(super) system_instruction: GeminiRequestContent,
    pub(super) contents: Vec<GeminiRequestContent>,
    pub(super) generation_config: GeminiGenerationConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiRequestContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) role: Option<String>,
    pub(super) parts: Vec<GeminiTextPart>,
}

#[derive(Debug, Serialize)]
pub(super) struct GeminiTextPart {
    pub(super) text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) response_mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) response_schema: Option<serde_json::Value>,
    pub(super) max_output_tokens: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    pub(super) usage_metadata: Option<GeminiUsageMetadata>,
}

impl GeminiGenerateContentResponse {
    pub(super) fn flatten_text(&self) -> Option<String> {
        for candidate in &self.candidates {
            if let Some(content) = &candidate.content {
                let text: String = content
                    .parts
                    .iter()
                    .filter_map(|part| part.text.as_deref())
                    .collect();
                if !text.trim().is_empty() {
                    return Some(text);
                }
            }
        }

        None
    }

    pub(super) fn primary_finish_reason(&self) -> Option<&str> {
        self.candidates
            .first()
            .and_then(|candidate| candidate.finish_reason.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiResponseContent>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiResponseContent {
    #[serde(default)]
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiResponsePart {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiUsageMetadata {
    #[serde(default)]
    prompt_token_count: Option<u64>,
    #[serde(default)]
    candidates_token_count: Option<u64>,
    #[serde(default)]
    total_token_count: Option<u64>,
    #[serde(default)]
    thoughts_token_count: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiBatchCreateRequest {
    pub(super) batch: GeminiBatchDefinition,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiBatchDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) display_name: Option<String>,
    pub(super) input_config: GeminiBatchInputConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiBatchInputConfig {
    pub(super) requests: GeminiInlineBatchRequests,
}

#[derive(Debug, Serialize)]
pub(super) struct GeminiInlineBatchRequests {
    pub(super) requests: Vec<GeminiBatchRequestEntry>,
}

#[derive(Debug, Serialize)]
pub(super) struct GeminiBatchRequestEntry {
    pub(super) request: GeminiGenerateContentRequest,
    pub(super) metadata: GeminiBatchRequestMetadata,
}

#[derive(Debug, Serialize)]
pub(super) struct GeminiBatchRequestMetadata {
    pub(super) key: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GeminiBatchResult {
    pub(super) key: String,
    pub(super) output_text: String,
}

impl GeminiBatchJob {
    pub(crate) fn inline_results(&self) -> Result<Vec<GeminiBatchResult>, super::ProcessorError> {
        let response_group = self
            .dest
            .as_ref()
            .and_then(|dest| dest.inlined_responses.as_ref())
            .ok_or_else(|| super::ProcessorError::Message {
                message: "Gemini batch completed without inline responses".to_string(),
            })?;
        let responses = if !response_group.responses.is_empty() {
            &response_group.responses
        } else {
            &response_group.inline_entries
        };

        responses
            .iter()
            .map(|entry| {
                let key = entry
                    .key
                    .clone()
                    .or_else(|| {
                        entry
                            .metadata
                            .as_ref()
                            .and_then(|metadata| metadata.key.clone())
                    })
                    .ok_or_else(|| super::ProcessorError::Message {
                        message: "Gemini batch response missing request key".to_string(),
                    })?;
                let response =
                    entry
                        .response
                        .as_ref()
                        .ok_or_else(|| super::ProcessorError::Message {
                            message: format!("Gemini batch response {key} missing output"),
                        })?;
                let output_text =
                    response
                        .flatten_text()
                        .ok_or_else(|| super::ProcessorError::Message {
                            message: format!("Gemini batch response {key} returned empty content"),
                        })?;
                Ok(GeminiBatchResult { key, output_text })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiBatchDestination {
    #[serde(default, rename = "inlinedResponses")]
    pub inlined_responses: Option<GeminiInlinedResponses>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiInlinedResponses {
    #[serde(default)]
    pub responses: Vec<GeminiInlinedResponseEntry>,
    #[serde(default, rename = "inlinedResponses")]
    pub inline_entries: Vec<GeminiInlinedResponseEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiInlinedResponseEntry {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub metadata: Option<GeminiInlinedResponseMetadata>,
    #[serde(default)]
    response: Option<GeminiGenerateContentResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeminiInlinedResponseMetadata {
    #[serde(default)]
    pub key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiBatchOperation {
    pub(super) name: String,
    #[serde(default)]
    pub(super) metadata: Option<GeminiBatchOperationMetadata>,
    #[serde(default)]
    pub(super) response: Option<GeminiBatchOperationResponse>,
}

impl GeminiBatchOperation {
    pub(super) fn into_batch_job(self) -> GeminiBatchJob {
        GeminiBatchJob {
            name: self.name,
            state: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.state.clone()),
            display_name: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.display_name.clone()),
            model: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.model.clone()),
            create_time: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.create_time.clone()),
            update_time: self
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.update_time.clone()),
            dest: self.response.map(|response| GeminiBatchDestination {
                inlined_responses: response
                    .dest
                    .and_then(|dest| dest.inlined_responses)
                    .or(response.inlined_responses),
            }),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiBatchOperationMetadata {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    create_time: Option<String>,
    #[serde(default)]
    update_time: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GeminiBatchOperationResponse {
    #[serde(default)]
    dest: Option<GeminiBatchDestination>,
    #[serde(default, rename = "inlinedResponses")]
    inlined_responses: Option<GeminiInlinedResponses>,
}
