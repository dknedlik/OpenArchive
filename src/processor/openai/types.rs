use serde::{Deserialize, Serialize};

use super::InferenceUsage;

#[derive(Debug, Serialize)]
pub(crate) struct OpenRouterResponsesRequest<'a> {
    pub(crate) model: &'a str,
    pub(crate) max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reasoning: Option<OpenRouterResponsesReasoningConfig<'a>>,
    pub(crate) text: OpenRouterResponsesTextConfig,
    pub(crate) input: Vec<OpenRouterResponsesInputItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenRouterResponsesTextConfig {
    pub(crate) format: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenRouterResponsesReasoningConfig<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) effort: Option<&'a str>,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenRouterResponsesInputItem {
    pub(crate) role: &'static str,
    pub(crate) content: Vec<OpenRouterResponsesContentItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct OpenRouterResponsesContentItem {
    #[serde(rename = "type")]
    pub(crate) item_type: &'static str,
    pub(crate) text: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OpenRouterResponsesResponse {
    #[serde(default)]
    output_text: String,
    #[serde(default)]
    output: Vec<OpenRouterResponsesOutputItem>,
    #[serde(default)]
    pub(crate) usage: Option<OpenRouterUsage>,
}

impl OpenRouterResponsesResponse {
    pub(crate) fn flatten_text(self) -> String {
        if !self.output_text.trim().is_empty() {
            return self.output_text;
        }

        let mut message_texts = Vec::new();
        let mut reasoning_texts = Vec::new();

        for item in self.output {
            match item {
                OpenRouterResponsesOutputItem::Message { content } => {
                    message_texts.extend(content.into_iter().filter_map(|content| match content {
                        OpenRouterResponsesOutputContent::OutputText { text }
                            if !text.trim().is_empty() =>
                        {
                            Some(text)
                        }
                        _ => None,
                    }));
                }
                OpenRouterResponsesOutputItem::Reasoning { summary } => {
                    reasoning_texts.extend(summary.into_iter().filter_map(|part| match part {
                        OpenRouterResponsesReasoningSummary::SummaryText { text }
                            if !text.trim().is_empty() =>
                        {
                            Some(text)
                        }
                        _ => None,
                    }));
                }
                OpenRouterResponsesOutputItem::Other => {}
            }
        }

        if !message_texts.is_empty() {
            return message_texts.join("");
        }

        reasoning_texts.join("")
    }
}

impl InferenceUsage {
    pub(crate) fn from_openrouter_usage(usage: OpenRouterUsage) -> Option<Self> {
        let reported_cost_micros = usage.cost.map(|cost| (cost * 1_000_000.0).round() as u64);

        if usage.input_tokens.is_none()
            && usage.output_tokens.is_none()
            && usage.total_tokens.is_none()
            && usage
                .output_tokens_details
                .as_ref()
                .and_then(|details| details.reasoning_tokens)
                .is_none()
            && reported_cost_micros.is_none()
        {
            return None;
        }

        Some(Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage
                .output_tokens_details
                .and_then(|details| details.reasoning_tokens),
            total_tokens: usage.total_tokens,
            reported_cost_micros,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenRouterResponsesOutputItem {
    #[serde(rename = "message")]
    Message {
        #[serde(default)]
        content: Vec<OpenRouterResponsesOutputContent>,
    },
    #[serde(rename = "reasoning")]
    Reasoning {
        #[serde(default)]
        summary: Vec<OpenRouterResponsesReasoningSummary>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenRouterResponsesOutputContent {
    #[serde(rename = "output_text")]
    OutputText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenRouterResponsesReasoningSummary {
    #[serde(rename = "summary_text")]
    SummaryText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct OpenRouterUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
    #[serde(default)]
    cost: Option<f64>,
    #[serde(default)]
    output_tokens_details: Option<OpenRouterOutputTokensDetails>,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenRouterOutputTokensDetails {
    #[serde(default)]
    reasoning_tokens: Option<u64>,
}
