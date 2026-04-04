use serde::{Deserialize, Serialize};

use super::InferenceUsage;

#[derive(Debug, Serialize)]
pub(super) struct AnthropicMessagesRequest<'a> {
    pub(super) model: &'a str,
    pub(super) max_tokens: u32,
    pub(super) system: &'a str,
    pub(super) messages: Vec<AnthropicMessageInput<'a>>,
    pub(super) tools: Vec<AnthropicToolDefinition>,
    pub(super) tool_choice: AnthropicToolChoice<'a>,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicMessagesRequestOwned {
    pub(super) model: String,
    pub(super) max_tokens: u32,
    pub(super) system: String,
    pub(super) messages: Vec<AnthropicOwnedMessageInput>,
    pub(super) tools: Vec<AnthropicToolDefinition>,
    pub(super) tool_choice: AnthropicOwnedToolChoice,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicMessageInput<'a> {
    pub(super) role: &'static str,
    pub(super) content: &'a str,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicOwnedMessageInput {
    pub(super) role: String,
    pub(super) content: String,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicToolDefinition {
    pub(super) name: &'static str,
    pub(super) description: &'static str,
    pub(super) input_schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicToolChoice<'a> {
    #[serde(rename = "type")]
    pub(super) choice_type: &'static str,
    pub(super) name: &'a str,
    pub(super) disable_parallel_tool_use: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicOwnedToolChoice {
    #[serde(rename = "type")]
    pub(super) choice_type: String,
    pub(super) name: String,
    pub(super) disable_parallel_tool_use: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct AnthropicBatchCreateRequest<'a> {
    pub(super) requests: &'a [super::AnthropicBatchRequestOwned],
}

#[derive(Debug, Deserialize)]
pub(super) struct AnthropicMessagesResponse {
    #[serde(default)]
    pub(super) content: Vec<AnthropicContentBlock>,
    #[serde(default)]
    pub(super) stop_reason: Option<String>,
    #[serde(default)]
    pub(super) usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub(super) enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
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
pub(super) struct AnthropicUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct AnthropicBatchResultLine {
    pub(super) custom_id: String,
    pub(super) result: AnthropicBatchResultBody,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub(super) enum AnthropicBatchResultBody {
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
pub(super) struct AnthropicResultError {
    #[serde(default)]
    pub(super) message: String,
}

impl InferenceUsage {
    pub(super) fn from_anthropic_usage(usage: AnthropicUsage) -> Self {
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
