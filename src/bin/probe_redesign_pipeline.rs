#![deny(warnings)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::{GeminiConfig, GrokConfig, OpenAiConfig, OpenAiReasoningEffort};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::json;

const DEFAULT_FIXTURE_CASES: &[&str] = &["architecture_direction", "durable_health_history"];
const DEFAULT_DOC_PATHS: &[&str] = &["docs/architecture.md", "docs/engineering-rules.md"];
const STAGE1_MAX_OUTPUT_TOKENS: u32 = 8000;
const STAGE2_MAX_OUTPUT_TOKENS: u32 = 4000;

#[derive(Debug, Parser)]
#[command(name = "probe_redesign_pipeline")]
#[command(
    about = "Probe the redesign extraction pipeline with stage-1 reasoning text and stage-2 schema formatting"
)]
struct Args {
    #[arg(long = "fixture-case")]
    fixture_cases: Vec<String>,

    #[arg(long = "doc-path")]
    doc_paths: Vec<PathBuf>,

    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,

    #[arg(long = "reuse-stage1-run-dir")]
    reuse_stage1_run_dir: Option<PathBuf>,

    #[arg(
        long = "gemini-extract-model",
        default_value = "gemini-3.1-pro-preview"
    )]
    gemini_extract_model: String,

    #[arg(
        long = "gemini-format-model",
        default_value = "gemini-3.1-flash-lite-preview"
    )]
    gemini_format_model: String,

    #[arg(long = "openai-extract-model", default_value = "gpt-5.4")]
    openai_extract_model: String,

    #[arg(long = "openai-extract-reasoning", default_value = "medium")]
    openai_extract_reasoning: String,

    #[arg(long = "openai-format-model", default_value = "gpt-4.1-mini")]
    openai_format_model: String,

    #[arg(long = "grok-extract-model", default_value = "grok-4-1-fast-reasoning")]
    grok_extract_model: String,

    #[arg(
        long = "grok-format-model",
        default_value = "grok-4-1-fast-non-reasoning"
    )]
    grok_format_model: String,

    #[arg(long = "gemini-only", default_value_t = false)]
    gemini_only: bool,

    #[arg(long = "openai-only", default_value_t = false)]
    openai_only: bool,

    #[arg(long = "grok-only", default_value_t = false)]
    grok_only: bool,

    #[arg(long = "skip-stage2", default_value_t = false)]
    skip_stage2: bool,
}

#[derive(Debug, Clone)]
struct ProbeArtifact {
    artifact_id: String,
    source_type: String,
    title: String,
    artifact_text: String,
}

#[derive(Debug, Clone)]
struct RunSpec {
    pipeline_name: String,
    output_dir_name: String,
    stage1_model: String,
    stage2_model: String,
    stage1_kind: Stage1Kind,
}

#[derive(Debug, Clone, Copy)]
enum Stage1Kind {
    Gemini,
    OpenAi { reasoning: OpenAiReasoningEffort },
    Grok,
}

#[derive(Debug, Clone)]
struct CompletionResult {
    output_text: String,
    elapsed: Duration,
    usage: UsageSummary,
}

#[derive(Debug, Clone)]
struct SavedStage1 {
    system_prompt: String,
    user_prompt: String,
    notes: String,
}

#[derive(Debug, Clone)]
struct ArtifactRunRecord {
    artifact_id: String,
    status: String,
    detail: String,
}

struct RunOutputFiles<'a> {
    stage1_system: &'a str,
    stage1_user: &'a str,
    stage1_result: &'a CompletionResult,
    stage2_system: &'a str,
    stage2_user: &'a str,
    formatted_json: Option<&'a serde_json::Value>,
    stage2_raw_json: &'a str,
    error: Option<&'a str>,
}

#[derive(Debug, Clone, Default)]
struct UsageSummary {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FixtureArtifact {
    artifact_id: String,
    source_type: String,
    title: Option<String>,
    segments: Vec<FixtureSegment>,
}

#[derive(Debug, Deserialize)]
struct FixtureSegment {
    participant_role: String,
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiContent>,
}

#[derive(Debug, Deserialize)]
struct GeminiContent {
    #[serde(default)]
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Deserialize)]
struct GeminiPart {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiUsageMetadata {
    #[serde(default)]
    prompt_token_count: Option<u64>,
    #[serde(default)]
    candidates_token_count: Option<u64>,
    #[serde(default)]
    total_token_count: Option<u64>,
    #[serde(default)]
    thoughts_token_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponse {
    #[serde(default)]
    output_text: String,
    #[serde(default)]
    output: Vec<OpenAiOutputItem>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenAiOutputItem {
    #[serde(rename = "message")]
    Message {
        #[serde(default)]
        content: Vec<OpenAiContentItem>,
    },
    #[serde(rename = "reasoning")]
    Reasoning {
        #[serde(default)]
        summary: Vec<OpenAiReasoningSummary>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenAiContentItem {
    #[serde(rename = "output_text")]
    OutputText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum OpenAiReasoningSummary {
    #[serde(rename = "summary_text")]
    SummaryText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct OpenAiUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
    #[serde(default)]
    output_tokens_details: Option<OpenAiOutputTokenDetails>,
}

#[derive(Debug, Deserialize)]
struct OpenAiOutputTokenDetails {
    #[serde(default)]
    reasoning_tokens: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ResponsesRequest<'a> {
    model: &'a str,
    max_output_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ResponsesReasoningConfig<'a>>,
    text: ResponsesTextConfig,
    input: Vec<ResponsesInputItem>,
}

#[derive(Debug, Serialize)]
struct ResponsesTextConfig {
    format: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ResponsesReasoningConfig<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct ResponsesInputItem {
    role: &'static str,
    content: Vec<ResponsesContentItem>,
}

#[derive(Debug, Serialize)]
struct ResponsesContentItem {
    #[serde(rename = "type")]
    item_type: &'static str,
    text: String,
}

#[derive(Debug, Deserialize)]
struct ResponsesResponse {
    #[serde(default)]
    output_text: String,
    #[serde(default)]
    output: Vec<ResponsesOutputItem>,
    #[serde(default)]
    usage: Option<ResponsesUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ResponsesOutputItem {
    #[serde(rename = "message")]
    Message {
        #[serde(default)]
        content: Vec<ResponsesOutputContent>,
    },
    #[serde(rename = "reasoning")]
    Reasoning {
        #[serde(default)]
        summary: Vec<ResponsesReasoningSummary>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ResponsesOutputContent {
    #[serde(rename = "output_text")]
    OutputText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ResponsesReasoningSummary {
    #[serde(rename = "summary_text")]
    SummaryText { text: String },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct ResponsesUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
    #[serde(default)]
    output_tokens_details: Option<ResponsesOutputTokenDetails>,
}

#[derive(Debug, Deserialize)]
struct ResponsesOutputTokenDetails {
    #[serde(default)]
    reasoning_tokens: Option<u64>,
}

impl ResponsesResponse {
    fn flatten_text(&self) -> Option<String> {
        if !self.output_text.trim().is_empty() {
            return Some(self.output_text.clone());
        }

        let mut message_texts = Vec::new();
        let mut reasoning_texts = Vec::new();

        for item in &self.output {
            match item {
                ResponsesOutputItem::Message { content } => {
                    message_texts.extend(content.iter().filter_map(|content| match content {
                        ResponsesOutputContent::OutputText { text } if !text.trim().is_empty() => {
                            Some(text.clone())
                        }
                        _ => None,
                    }));
                }
                ResponsesOutputItem::Reasoning { summary } => {
                    reasoning_texts.extend(summary.iter().filter_map(|part| match part {
                        ResponsesReasoningSummary::SummaryText { text }
                            if !text.trim().is_empty() =>
                        {
                            Some(text.clone())
                        }
                        _ => None,
                    }));
                }
                ResponsesOutputItem::Other => {}
            }
        }

        if !message_texts.is_empty() {
            return Some(message_texts.join(""));
        }
        if !reasoning_texts.is_empty() {
            return Some(reasoning_texts.join(""));
        }
        None
    }
}

struct GeminiProbeClient {
    client: Client,
    base_url: String,
}

impl GeminiProbeClient {
    fn new(config: &GeminiConfig) -> Result<Self> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header = HeaderName::from_static("x-goog-api-key");
        default_headers.insert(
            api_key_header,
            HeaderValue::from_str(&config.api_key).context("invalid Gemini API key header")?,
        );

        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(180))
            .build()
            .context("failed to build Gemini HTTP client")?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
        })
    }

    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<CompletionResult> {
        let endpoint = format!("{}/models/{}:generateContent", self.base_url, model);
        let body = json!({
            "systemInstruction": {
                "parts": [{ "text": system_prompt }]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{ "text": user_prompt }]
                }
            ],
            "generationConfig": {
                "maxOutputTokens": max_output_tokens
            }
        });

        self.send_request(&endpoint, &body)
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<CompletionResult> {
        let endpoint = format!("{}/models/{}:generateContent", self.base_url, model);
        let body = json!({
            "systemInstruction": {
                "parts": [{ "text": system_prompt }]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{ "text": user_prompt }]
                }
            ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": normalize_gemini_schema(schema),
                "maxOutputTokens": max_output_tokens
            }
        });

        self.send_request(&endpoint, &body)
    }

    fn send_request(&self, endpoint: &str, body: &serde_json::Value) -> Result<CompletionResult> {
        let started = Instant::now();
        let response = self
            .client
            .post(endpoint)
            .body(serde_json::to_vec(body).context("failed to serialize Gemini request body")?)
            .send()
            .context("failed to send Gemini request")?;

        let status = response.status();
        let response_text = response
            .text()
            .context("failed to read Gemini response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "Gemini request failed with status {}: {}",
                status.as_u16(),
                preview(&response_text)
            ));
        }

        let parsed: GeminiResponse = serde_json::from_str(&response_text).with_context(|| {
            format!(
                "failed to parse Gemini response: {}",
                preview(&response_text)
            )
        })?;
        let output_text = flatten_gemini_text(&parsed)
            .ok_or_else(|| anyhow!("Gemini response returned empty content"))?;

        Ok(CompletionResult {
            output_text,
            elapsed: started.elapsed(),
            usage: parsed
                .usage_metadata
                .map(|usage| UsageSummary {
                    input_tokens: usage.prompt_token_count,
                    output_tokens: usage.candidates_token_count,
                    reasoning_tokens: usage.thoughts_token_count,
                    total_tokens: usage.total_token_count,
                })
                .unwrap_or_default(),
        })
    }
}

struct OpenAiProbeClient {
    client: Client,
    base_url: String,
}

struct GrokProbeClient {
    client: Client,
    base_url: String,
}

impl OpenAiProbeClient {
    fn new(config: &OpenAiConfig) -> Result<Self> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let bearer = format!("Bearer {}", config.api_key);
        default_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&bearer).context("invalid OpenAI bearer header")?,
        );

        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(180))
            .build()
            .context("failed to build OpenAI HTTP client")?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
        })
    }

    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        reasoning: Option<OpenAiReasoningEffort>,
        max_output_tokens: u32,
    ) -> Result<CompletionResult> {
        let body = self.build_request(
            model,
            system_prompt,
            user_prompt,
            reasoning,
            json!({ "type": "text" }),
            max_output_tokens,
        );
        self.send_request(&body)
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<CompletionResult> {
        let body = self.build_request(
            model,
            system_prompt,
            user_prompt,
            None,
            json!({
                "type": "json_schema",
                "name": "openarchive_redesign_probe",
                "strict": true,
                "schema": schema
            }),
            max_output_tokens,
        );
        self.send_request(&body)
    }

    fn build_request(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        reasoning: Option<OpenAiReasoningEffort>,
        format: serde_json::Value,
        max_output_tokens: u32,
    ) -> serde_json::Value {
        let mut body = json!({
            "model": model,
            "max_output_tokens": max_output_tokens,
            "text": {
                "format": format
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

        if let Some(reasoning_effort) = reasoning {
            body["reasoning"] = json!({
                "effort": reasoning_effort.as_str()
            });
        }

        body
    }

    fn send_request(&self, body: &serde_json::Value) -> Result<CompletionResult> {
        let started = Instant::now();
        let response = self
            .client
            .post(format!("{}/responses", self.base_url))
            .body(serde_json::to_vec(body).context("failed to serialize OpenAI request body")?)
            .send()
            .context("failed to send OpenAI request")?;

        let status = response.status();
        let response_text = response
            .text()
            .context("failed to read OpenAI response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "OpenAI request failed with status {}: {}",
                status.as_u16(),
                preview(&response_text)
            ));
        }

        let parsed: OpenAiResponse = serde_json::from_str(&response_text).with_context(|| {
            format!(
                "failed to parse OpenAI response: {}",
                preview(&response_text)
            )
        })?;
        let output_text = flatten_openai_text(&parsed)
            .ok_or_else(|| anyhow!("OpenAI response returned empty content"))?;

        Ok(CompletionResult {
            output_text,
            elapsed: started.elapsed(),
            usage: parsed
                .usage
                .map(|usage| UsageSummary {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    reasoning_tokens: usage
                        .output_tokens_details
                        .and_then(|details| details.reasoning_tokens),
                    total_tokens: usage.total_tokens,
                })
                .unwrap_or_default(),
        })
    }
}

impl GrokProbeClient {
    fn new(config: &GrokConfig) -> Result<Self> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let bearer = format!("Bearer {}", config.api_key);
        default_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&bearer).context("invalid Grok bearer header")?,
        );

        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(180))
            .build()
            .context("failed to build Grok HTTP client")?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
        })
    }

    fn complete_text(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_output_tokens: u32,
    ) -> Result<CompletionResult> {
        let body = self.build_request(
            model,
            system_prompt,
            user_prompt,
            json!({ "type": "text" }),
            max_output_tokens,
        );
        self.send_request(&body)
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<CompletionResult> {
        let body = self.build_request(
            model,
            system_prompt,
            user_prompt,
            json!({
                "type": "json_schema",
                "name": "openarchive_redesign_probe",
                "strict": true,
                "schema": schema
            }),
            max_output_tokens,
        );
        self.send_request(&body)
    }

    fn build_request(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        format: serde_json::Value,
        max_output_tokens: u32,
    ) -> serde_json::Value {
        serde_json::to_value(ResponsesRequest {
            model,
            max_output_tokens,
            reasoning: None,
            text: ResponsesTextConfig { format },
            input: vec![
                ResponsesInputItem {
                    role: "system",
                    content: vec![ResponsesContentItem {
                        item_type: "input_text",
                        text: system_prompt.to_string(),
                    }],
                },
                ResponsesInputItem {
                    role: "user",
                    content: vec![ResponsesContentItem {
                        item_type: "input_text",
                        text: user_prompt.to_string(),
                    }],
                },
            ],
        })
        .expect("responses request should serialize")
    }

    fn send_request(&self, body: &serde_json::Value) -> Result<CompletionResult> {
        let started = Instant::now();
        let response = self
            .client
            .post(format!("{}/responses", self.base_url))
            .body(serde_json::to_vec(body).context("failed to serialize Grok request body")?)
            .send()
            .context("failed to send Grok request")?;

        let status = response.status();
        let response_text = response
            .text()
            .context("failed to read Grok response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "Grok request failed with status {}: {}",
                status.as_u16(),
                preview(&response_text)
            ));
        }

        let parsed: ResponsesResponse =
            serde_json::from_str(&response_text).with_context(|| {
                format!("failed to parse Grok response: {}", preview(&response_text))
            })?;
        let output_text = parsed
            .flatten_text()
            .ok_or_else(|| anyhow!("Grok response returned empty content"))?;

        Ok(CompletionResult {
            output_text,
            elapsed: started.elapsed(),
            usage: parsed
                .usage
                .map(|usage| UsageSummary {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                    reasoning_tokens: usage
                        .output_tokens_details
                        .and_then(|details| details.reasoning_tokens),
                    total_tokens: usage.total_tokens,
                })
                .unwrap_or_default(),
        })
    }
}

fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let args = Args::parse();
    let cases = load_cases(&args)?;
    if cases.is_empty() {
        return Err(anyhow!("no probe artifacts selected"));
    }

    let gemini = if args.openai_only || args.grok_only {
        None
    } else {
        Some(GeminiProbeClient::new(
            &GeminiConfig::from_env().context("failed to load Gemini config from env")?,
        )?)
    };
    let openai = if args.gemini_only {
        None
    } else {
        Some(OpenAiProbeClient::new(
            &OpenAiConfig::from_env().context("failed to load OpenAI config from env")?,
        )?)
    };
    let grok = if args.gemini_only || args.openai_only {
        None
    } else {
        GrokConfig::from_env()
            .ok()
            .map(|config| GrokProbeClient::new(&config))
            .transpose()?
    };
    let openai_reasoning = parse_openai_reasoning(&args.openai_extract_reasoning)?;

    let mut run_specs = Vec::new();
    if !args.openai_only && !args.grok_only {
        run_specs.push(RunSpec {
            pipeline_name: "gemini_pro_to_flash_lite".to_string(),
            output_dir_name: format!(
                "extract-{}__format-{}",
                slugify(&args.gemini_extract_model),
                slugify(&args.gemini_format_model)
            ),
            stage1_model: args.gemini_extract_model.clone(),
            stage2_model: args.gemini_format_model.clone(),
            stage1_kind: Stage1Kind::Gemini,
        });
    }
    if !args.gemini_only && !args.grok_only {
        run_specs.push(RunSpec {
            pipeline_name: format!(
                "gpt54_{}_to_41mini",
                args.openai_extract_reasoning.to_ascii_lowercase()
            ),
            output_dir_name: format!(
                "extract-{}-{}__format-{}",
                slugify(&args.openai_extract_model),
                slugify(&args.openai_extract_reasoning),
                slugify(&args.openai_format_model)
            ),
            stage1_model: args.openai_extract_model.clone(),
            stage2_model: args.openai_format_model.clone(),
            stage1_kind: Stage1Kind::OpenAi {
                reasoning: openai_reasoning,
            },
        });
    }
    if !args.gemini_only && !args.openai_only {
        run_specs.push(RunSpec {
            pipeline_name: "grok_fast_reasoning_to_fast_non_reasoning".to_string(),
            output_dir_name: format!(
                "extract-{}__format-{}",
                slugify(&args.grok_extract_model),
                slugify(&args.grok_format_model)
            ),
            stage1_model: args.grok_extract_model.clone(),
            stage2_model: args.grok_format_model.clone(),
            stage1_kind: Stage1Kind::Grok,
        });
    }

    let out_dir = args
        .out_dir
        .unwrap_or_else(|| default_output_dir("redesign_probe"));
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("failed to create output dir {}", out_dir.display()))?;

    println!("Redesign pipeline probe");
    println!("Output dir: {}", out_dir.display());
    println!("Artifacts: {}", cases.len());
    println!();

    for run_spec in &run_specs {
        println!(
            "Pipeline: {} | stage1 {} | stage2 {}",
            run_spec.pipeline_name, run_spec.stage1_model, run_spec.stage2_model
        );
        let mut records = Vec::new();
        for artifact in &cases {
            let reused_stage1 = match &args.reuse_stage1_run_dir {
                Some(run_dir) => Some(load_saved_stage1(run_dir, artifact)?),
                None => None,
            };
            let stage1_system = reused_stage1
                .as_ref()
                .map(|saved| saved.system_prompt.clone())
                .unwrap_or_else(|| stage1_system_prompt().to_string());
            let stage1_user = reused_stage1
                .as_ref()
                .map(|saved| saved.user_prompt.clone())
                .unwrap_or_else(|| stage1_user_prompt(artifact));
            let stage1_result = match reused_stage1 {
                Some(saved) => CompletionResult {
                    output_text: saved.notes,
                    elapsed: Duration::ZERO,
                    usage: UsageSummary::default(),
                },
                None => match run_spec.stage1_kind {
                    Stage1Kind::Gemini => gemini
                        .as_ref()
                        .expect("gemini client should exist when gemini run spec is present")
                        .complete_text(
                            &run_spec.stage1_model,
                            &stage1_system,
                            &stage1_user,
                            STAGE1_MAX_OUTPUT_TOKENS,
                        )?,
                    Stage1Kind::OpenAi { reasoning } => openai
                        .as_ref()
                        .expect("openai client should exist when openai run spec is present")
                        .complete_text(
                            &run_spec.stage1_model,
                            &stage1_system,
                            &stage1_user,
                            maybe_reasoning(reasoning),
                            STAGE1_MAX_OUTPUT_TOKENS,
                        )?,
                    Stage1Kind::Grok => grok
                        .as_ref()
                        .expect("grok client should exist when grok run spec is present")
                        .complete_text(
                            &run_spec.stage1_model,
                            &stage1_system,
                            &stage1_user,
                            STAGE1_MAX_OUTPUT_TOKENS,
                        )?,
                },
            };

            if args.skip_stage2 {
                write_run_outputs(
                    &out_dir,
                    run_spec,
                    artifact,
                    RunOutputFiles {
                        stage1_system: &stage1_system,
                        stage1_user: &stage1_user,
                        stage1_result: &stage1_result,
                        stage2_system: "",
                        stage2_user: "",
                        formatted_json: None,
                        stage2_raw_json: "",
                        error: None,
                    },
                )?;

                println!(
                    "  {} | s1 {}{} | stage2 skipped",
                    artifact.artifact_id,
                    format_duration(stage1_result.elapsed),
                    format_usage(&stage1_result.usage),
                );
                records.push(ArtifactRunRecord {
                    artifact_id: artifact.artifact_id.clone(),
                    status: "stage1_only".to_string(),
                    detail: "stage2 skipped".to_string(),
                });
                continue;
            }

            let stage2_system = stage2_system_prompt();
            let stage2_user = stage2_user_prompt(artifact, &stage1_result.output_text);
            let stage2_result = match run_spec.stage1_kind {
                Stage1Kind::Gemini => gemini
                    .as_ref()
                    .expect("gemini client should exist when gemini run spec is present")
                    .complete_json(
                        &run_spec.stage2_model,
                        stage2_system,
                        &stage2_user,
                        &stage2_schema(),
                        STAGE2_MAX_OUTPUT_TOKENS,
                    ),
                Stage1Kind::OpenAi { .. } => openai
                    .as_ref()
                    .expect("openai client should exist when openai run spec is present")
                    .complete_json(
                        &run_spec.stage2_model,
                        stage2_system,
                        &stage2_user,
                        &stage2_schema(),
                        STAGE2_MAX_OUTPUT_TOKENS,
                    ),
                Stage1Kind::Grok => grok
                    .as_ref()
                    .expect("grok client should exist when grok run spec is present")
                    .complete_json(
                        &run_spec.stage2_model,
                        stage2_system,
                        &stage2_user,
                        &stage2_schema(),
                        STAGE2_MAX_OUTPUT_TOKENS,
                    ),
            };

            match stage2_result {
                Ok(stage2_result) => {
                    let parse_result: Result<serde_json::Value> =
                        serde_json::from_str(&stage2_result.output_text).with_context(|| {
                            format!(
                                "failed to parse stage2 JSON for pipeline {} artifact {}",
                                run_spec.pipeline_name, artifact.artifact_id
                            )
                        });
                    match parse_result {
                        Ok(formatted_json) => {
                            write_run_outputs(
                                &out_dir,
                                run_spec,
                                artifact,
                                RunOutputFiles {
                                    stage1_system: &stage1_system,
                                    stage1_user: &stage1_user,
                                    stage1_result: &stage1_result,
                                    stage2_system,
                                    stage2_user: &stage2_user,
                                    formatted_json: Some(&formatted_json),
                                    stage2_raw_json: &stage2_result.output_text,
                                    error: None,
                                },
                            )?;

                            println!(
                                "  {} | s1 {}{} | s2 {}{} | memories {} | entities {} | relationships {}",
                                artifact.artifact_id,
                                format_duration(stage1_result.elapsed),
                                format_usage(&stage1_result.usage),
                                format_duration(stage2_result.elapsed),
                                format_usage(&stage2_result.usage),
                                array_len(&formatted_json, "memories"),
                                array_len(&formatted_json, "entities"),
                                array_len(&formatted_json, "relationships"),
                            );
                            records.push(ArtifactRunRecord {
                                artifact_id: artifact.artifact_id.clone(),
                                status: "ok".to_string(),
                                detail: format!(
                                    "memories={} entities={} relationships={}",
                                    array_len(&formatted_json, "memories"),
                                    array_len(&formatted_json, "entities"),
                                    array_len(&formatted_json, "relationships")
                                ),
                            });
                        }
                        Err(err) => {
                            write_run_outputs(
                                &out_dir,
                                run_spec,
                                artifact,
                                RunOutputFiles {
                                    stage1_system: &stage1_system,
                                    stage1_user: &stage1_user,
                                    stage1_result: &stage1_result,
                                    stage2_system,
                                    stage2_user: &stage2_user,
                                    formatted_json: None,
                                    stage2_raw_json: &stage2_result.output_text,
                                    error: Some(&err.to_string()),
                                },
                            )?;

                            println!(
                                "  {} | s1 {}{} | s2 {}{} | error parsing JSON",
                                artifact.artifact_id,
                                format_duration(stage1_result.elapsed),
                                format_usage(&stage1_result.usage),
                                format_duration(stage2_result.elapsed),
                                format_usage(&stage2_result.usage),
                            );
                            records.push(ArtifactRunRecord {
                                artifact_id: artifact.artifact_id.clone(),
                                status: "stage2_parse_error".to_string(),
                                detail: err.to_string(),
                            });
                        }
                    }
                }
                Err(err) => {
                    write_run_outputs(
                        &out_dir,
                        run_spec,
                        artifact,
                        RunOutputFiles {
                            stage1_system: &stage1_system,
                            stage1_user: &stage1_user,
                            stage1_result: &stage1_result,
                            stage2_system,
                            stage2_user: &stage2_user,
                            formatted_json: None,
                            stage2_raw_json: "",
                            error: Some(&err.to_string()),
                        },
                    )?;

                    println!(
                        "  {} | s1 {}{} | s2 error",
                        artifact.artifact_id,
                        format_duration(stage1_result.elapsed),
                        format_usage(&stage1_result.usage),
                    );
                    records.push(ArtifactRunRecord {
                        artifact_id: artifact.artifact_id.clone(),
                        status: "stage2_request_error".to_string(),
                        detail: err.to_string(),
                    });
                }
            }
        }
        write_manifest(&out_dir, run_spec, &records)?;
        println!();
    }

    Ok(())
}

fn load_cases(args: &Args) -> Result<Vec<ProbeArtifact>> {
    let fixture_cases = if args.fixture_cases.is_empty() {
        DEFAULT_FIXTURE_CASES
            .iter()
            .map(|value| (*value).to_string())
            .collect()
    } else {
        args.fixture_cases.clone()
    };

    let doc_paths = if args.doc_paths.is_empty() {
        DEFAULT_DOC_PATHS.iter().map(PathBuf::from).collect()
    } else {
        args.doc_paths.clone()
    };

    let mut cases = Vec::new();
    for fixture_case in fixture_cases {
        cases.push(load_fixture_case(&fixture_case)?);
    }
    for doc_path in doc_paths {
        cases.push(load_doc_case(&doc_path)?);
    }

    Ok(cases)
}

fn load_fixture_case(case_name: &str) -> Result<ProbeArtifact> {
    let path = Path::new("tests")
        .join("fixtures")
        .join("enrichment")
        .join(format!("{case_name}.fixture.json"));
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    let fixture: FixtureArtifact = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse fixture {}", path.display()))?;

    let mut artifact_text = String::new();
    for (index, segment) in fixture.segments.iter().enumerate() {
        if index > 0 {
            artifact_text.push_str("\n\n");
        }
        artifact_text.push_str(&format!("{}: {}", segment.participant_role, segment.text));
    }

    Ok(ProbeArtifact {
        artifact_id: fixture.artifact_id,
        source_type: fixture.source_type,
        title: fixture.title.unwrap_or_else(|| case_name.to_string()),
        artifact_text,
    })
}

fn load_doc_case(path: &Path) -> Result<ProbeArtifact> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read document {}", path.display()))?;
    let title = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("document")
        .to_string();
    let artifact_id = format!("doc-{}", slugify(&path.display().to_string()));
    let source_type = match path.extension().and_then(|ext| ext.to_str()) {
        Some("md") => "markdown_file",
        _ => "text_file",
    }
    .to_string();

    Ok(ProbeArtifact {
        artifact_id,
        source_type,
        title,
        artifact_text: raw,
    })
}

fn load_saved_stage1(run_dir: &Path, artifact: &ProbeArtifact) -> Result<SavedStage1> {
    let case_stem = slugify(&artifact.artifact_id);
    let system_path = run_dir.join(format!("{case_stem}.stage1.system.txt"));
    let user_path = run_dir.join(format!("{case_stem}.stage1.user.txt"));
    let notes_path = run_dir.join(format!("{case_stem}.stage1.txt"));

    let system_prompt = fs::read_to_string(&system_path)
        .with_context(|| format!("failed to read {}", system_path.display()))?;
    let user_prompt = fs::read_to_string(&user_path)
        .with_context(|| format!("failed to read {}", user_path.display()))?;
    let notes = fs::read_to_string(&notes_path)
        .with_context(|| format!("failed to read {}", notes_path.display()))?;

    Ok(SavedStage1 {
        system_prompt,
        user_prompt,
        notes,
    })
}

fn stage1_system_prompt() -> &'static str {
    "You are OpenArchive's extraction engine. Read the artifact and identify the highest-value things that should be retrievable later.

Write extraction notes in plain text, not JSON.

Use these top-level headings exactly:
Summary
Classifications
Memories
Entities
Relationships

General rules:
- Be accurate and conservative. Do not add unsupported claims.
- Do not use segment references or citations.
- Prefer fewer, higher-value extractions over exhaustive enumeration.
- Do not repeat yourself across memories, entities, or relationships.
- Keep memories non-overlapping. If two candidate memories describe the same underlying fact, decision, policy, history, or theme, merge them into one stronger memory.
- Split memories only when the facts are independently durable and would be useful to retrieve separately later.
- For reference documents, specifications, engineering rules, and similar non-personal documents, keep only the most durable facts and the most important named systems or technologies.
- For reference documents, do not convert each section, heading, implementation detail, or list item into a separate memory. Prefer principle-level, system-level, or policy-level memories.
- Under Classifications, Memories, Entities, and Relationships, use bullet lists only.

Formatting rules by section:
- Summary: one short paragraph.
- Classifications: one classification label per bullet. Do not combine multiple labels in one bullet.
- Memories: one memory per bullet in this exact format:
  - title: ... | body: ... | memory_type_hint: personal_fact|preference|project_fact|ongoing_state|reference
  Before emitting a memory, ask whether it stands alone or is just a supporting detail of another memory. Prefer the smallest set of memories that preserves the artifact's important durable information.
- Entities: include only named, durable, independently retrievable things such as people, organizations, named software systems, named technologies, named projects, or named medical conditions. One entity per bullet in this exact format:
  - name: ... | type: ... | note: ...
- Relationships: include only explicit or strongly supported relationships between retained named entities. Do not invent relationship chains. One relationship per bullet in this exact format:
  - subject: ... | relationship_type: ... | object: ... | note: ...
  Do not create multiple relationship bullets that restate the same underlying connection.

Never treat the following as entities unless they are clearly proper names of real retained things:
- file paths
- directory names
- commands
- role labels like User or Assistant
- abstract concepts like lineage, supersession, queue, architecture, system, project
- section names, list headings, or pipeline stages"
}

fn stage1_user_prompt(artifact: &ProbeArtifact) -> String {
    format!(
        "Artifact metadata:
- artifact_id: {}
- source_type: {}
- title: {}

Artifact content:
{}
",
        artifact.artifact_id, artifact.source_type, artifact.title, artifact.artifact_text
    )
}

fn stage2_system_prompt() -> &'static str {
    "You are a schema formatter. Read the extraction notes and transcribe them into the required JSON schema exactly.

This is a transcription task, not a reasoning task.

Rules:
- Return valid JSON only.
- Do not add new facts, entities, relationships, or classifications.
- Do not split one bullet into multiple output objects.
- Do not infer entities from memory text.
- Do not infer relationships unless there is an explicit relationship bullet.
- One classification bullet becomes one classification object.
- One memory bullet becomes one memory object.
- One entity bullet becomes one entity object.
- One relationship bullet becomes one relationship object.
- If a section has no valid bullets, return an empty array for that section.
- Preserve the meaning of each bullet as written."
}

fn stage2_user_prompt(artifact: &ProbeArtifact, stage1_notes: &str) -> String {
    format!(
        "Artifact metadata:
- artifact_id: {}
- source_type: {}
- title: {}

Formatting rules:
- Set every memory_scope to \"artifact\".
- Set every memory_scope_value to the artifact_id.
- Copy memory_type_hint directly into memory_type.
- Use only explicit entity bullets to create entities.
- Use only explicit relationship bullets to create relationships.
- Do not create entities, relationships, or classifications from the summary paragraph.
- Keep classification_type short and consistent.

Extraction notes:
{}",
        artifact.artifact_id, artifact.source_type, artifact.title, stage1_notes
    )
}

fn stage2_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary",
            "classifications",
            "memories",
            "entities",
            "relationships"
        ],
        "properties": {
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "required": ["title", "body_text"],
                "properties": {
                    "title": { "type": "string" },
                    "body_text": { "type": "string" }
                }
            },
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["label", "classification_type"],
                    "properties": {
                        "label": { "type": "string" },
                        "classification_type": { "type": "string" }
                    }
                }
            },
            "memories": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "title",
                        "body_text",
                        "memory_type",
                        "memory_scope",
                        "memory_scope_value"
                    ],
                    "properties": {
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "memory_type": {
                            "type": "string",
                            "enum": [
                                "personal_fact",
                                "preference",
                                "project_fact",
                                "ongoing_state",
                                "reference"
                            ]
                        },
                        "memory_scope": {
                            "type": "string",
                            "enum": ["artifact"]
                        },
                        "memory_scope_value": { "type": "string" }
                    }
                }
            },
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["display_name", "entity_type"],
                    "properties": {
                        "display_name": { "type": "string" },
                        "entity_type": { "type": "string" }
                    }
                }
            },
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "relationship_type",
                        "subject_name",
                        "object_name",
                        "title",
                        "body_text"
                    ],
                    "properties": {
                        "relationship_type": { "type": "string" },
                        "subject_name": { "type": "string" },
                        "object_name": { "type": "string" },
                        "title": { "type": "string" },
                        "body_text": { "type": "string" }
                    }
                }
            }
        }
    })
}

fn write_run_outputs(
    out_dir: &Path,
    run_spec: &RunSpec,
    artifact: &ProbeArtifact,
    outputs: RunOutputFiles<'_>,
) -> Result<()> {
    let run_dir = out_dir.join(&run_spec.output_dir_name);
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create pipeline dir {}", run_dir.display()))?;

    let case_stem = slugify(&artifact.artifact_id);
    let stage1_system_path = run_dir.join(format!("{case_stem}.stage1.system.txt"));
    let stage1_user_path = run_dir.join(format!("{case_stem}.stage1.user.txt"));
    let stage1_path = run_dir.join(format!("{case_stem}.stage1.txt"));
    let stage2_system_path = run_dir.join(format!("{case_stem}.stage2.system.txt"));
    let stage2_user_path = run_dir.join(format!("{case_stem}.stage2.user.txt"));
    let stage2_path = run_dir.join(format!("{case_stem}.stage2.json"));
    let summary_path = run_dir.join(format!("{case_stem}.summary.txt"));
    let error_path = run_dir.join(format!("{case_stem}.error.txt"));

    fs::write(&stage1_system_path, outputs.stage1_system)
        .with_context(|| format!("failed to write {}", stage1_system_path.display()))?;
    fs::write(&stage1_user_path, outputs.stage1_user)
        .with_context(|| format!("failed to write {}", stage1_user_path.display()))?;
    fs::write(&stage1_path, &outputs.stage1_result.output_text)
        .with_context(|| format!("failed to write {}", stage1_path.display()))?;
    fs::write(&stage2_system_path, outputs.stage2_system)
        .with_context(|| format!("failed to write {}", stage2_system_path.display()))?;
    fs::write(&stage2_user_path, outputs.stage2_user)
        .with_context(|| format!("failed to write {}", stage2_user_path.display()))?;
    fs::write(&stage2_path, outputs.stage2_raw_json)
        .with_context(|| format!("failed to write {}", stage2_path.display()))?;
    if let Some(formatted_json) = outputs.formatted_json {
        fs::write(
            &summary_path,
            format!(
                "artifact_id: {}\ntitle: {}\nmemories: {}\nentities: {}\nrelationships: {}\nsummary: {}\n",
                artifact.artifact_id,
                artifact.title,
                array_len(formatted_json, "memories"),
                array_len(formatted_json, "entities"),
                array_len(formatted_json, "relationships"),
                formatted_json
                    .get("summary")
                    .and_then(|value| value.get("body_text"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
            ),
        )
        .with_context(|| format!("failed to write {}", summary_path.display()))?;
    }
    if let Some(error) = outputs.error {
        fs::write(&error_path, error)
            .with_context(|| format!("failed to write {}", error_path.display()))?;
    }

    Ok(())
}

fn write_manifest(out_dir: &Path, run_spec: &RunSpec, records: &[ArtifactRunRecord]) -> Result<()> {
    let run_dir = out_dir.join(&run_spec.output_dir_name);
    let manifest_path = run_dir.join("manifest.txt");
    let mut contents = format!(
        "pipeline_name: {}\nstage1_model: {}\nstage2_model: {}\n\n",
        run_spec.pipeline_name, run_spec.stage1_model, run_spec.stage2_model
    );
    for record in records {
        contents.push_str(&format!(
            "{} | {} | {}\n",
            record.artifact_id, record.status, record.detail
        ));
    }
    fs::write(&manifest_path, contents)
        .with_context(|| format!("failed to write {}", manifest_path.display()))
}

fn flatten_gemini_text(response: &GeminiResponse) -> Option<String> {
    for candidate in &response.candidates {
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

fn flatten_openai_text(response: &OpenAiResponse) -> Option<String> {
    if !response.output_text.trim().is_empty() {
        return Some(response.output_text.clone());
    }

    let mut message_texts = Vec::new();
    let mut reasoning_texts = Vec::new();
    for item in &response.output {
        match item {
            OpenAiOutputItem::Message { content } => {
                for content_item in content {
                    if let OpenAiContentItem::OutputText { text } = content_item {
                        if !text.trim().is_empty() {
                            message_texts.push(text.clone());
                        }
                    }
                }
            }
            OpenAiOutputItem::Reasoning { summary } => {
                for summary_item in summary {
                    if let OpenAiReasoningSummary::SummaryText { text } = summary_item {
                        if !text.trim().is_empty() {
                            reasoning_texts.push(text.clone());
                        }
                    }
                }
            }
            OpenAiOutputItem::Other => {}
        }
    }

    if !message_texts.is_empty() {
        return Some(message_texts.join(""));
    }
    if !reasoning_texts.is_empty() {
        return Some(reasoning_texts.join(""));
    }

    None
}

fn normalize_gemini_schema(schema: &serde_json::Value) -> serde_json::Value {
    match schema {
        serde_json::Value::Object(map) => {
            let mut sanitized = serde_json::Map::new();
            for (key, child) in map {
                if matches!(
                    key.as_str(),
                    "$schema"
                        | "additionalProperties"
                        | "strict"
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
                if key == "type" {
                    let normalized = match child {
                        serde_json::Value::String(value) => {
                            serde_json::Value::String(value.to_ascii_uppercase())
                        }
                        _ => normalize_gemini_schema(child),
                    };
                    sanitized.insert(key.clone(), normalized);
                } else {
                    sanitized.insert(key.clone(), normalize_gemini_schema(child));
                }
            }
            serde_json::Value::Object(sanitized)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(normalize_gemini_schema).collect())
        }
        _ => schema.clone(),
    }
}

fn parse_openai_reasoning(value: &str) -> Result<OpenAiReasoningEffort> {
    match value {
        "none" => Ok(OpenAiReasoningEffort::None),
        "minimal" => Ok(OpenAiReasoningEffort::Minimal),
        "low" => Ok(OpenAiReasoningEffort::Low),
        "medium" => Ok(OpenAiReasoningEffort::Medium),
        "high" => Ok(OpenAiReasoningEffort::High),
        other => Err(anyhow!(
            "unsupported OpenAI reasoning effort {other}; expected none|minimal|low|medium|high"
        )),
    }
}

fn maybe_reasoning(effort: OpenAiReasoningEffort) -> Option<OpenAiReasoningEffort> {
    match effort {
        OpenAiReasoningEffort::None => None,
        other => Some(other),
    }
}

fn array_len(value: &serde_json::Value, key: &str) -> usize {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

fn format_duration(duration: Duration) -> String {
    if duration.as_millis() < 1000 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

fn format_usage(usage: &UsageSummary) -> String {
    let mut parts = Vec::new();
    if let Some(input_tokens) = usage.input_tokens {
        parts.push(format!("in {}", input_tokens));
    }
    if let Some(output_tokens) = usage.output_tokens {
        parts.push(format!("out {}", output_tokens));
    }
    if let Some(reasoning_tokens) = usage.reasoning_tokens {
        parts.push(format!("reasoning {}", reasoning_tokens));
    }
    if let Some(total_tokens) = usage.total_tokens {
        parts.push(format!("total {}", total_tokens));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" [{}]", parts.join(", "))
    }
}

fn default_output_dir(prefix: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs();
    Path::new("tmp").join(format!("{prefix}_{ts}"))
}

fn preview(text: &str) -> String {
    let trimmed = text.trim();
    let mut chars = trimmed.chars();
    let preview: String = chars.by_ref().take(300).collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn slugify(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    let mut last_was_sep = false;
    for ch in value.chars() {
        let normalized = ch.to_ascii_lowercase();
        if normalized.is_ascii_alphanumeric() {
            slug.push(normalized);
            last_was_sep = false;
        } else if !last_was_sep {
            slug.push('-');
            last_was_sep = true;
        }
    }
    let trimmed = slug.trim_matches('-');
    if trimmed.is_empty() {
        "artifact".to_string()
    } else {
        trimmed.to_string()
    }
}
