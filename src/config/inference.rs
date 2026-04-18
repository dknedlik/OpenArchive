use crate::error::{ConfigError, ConfigResult};
use std::env;
use std::time::Duration;

use super::env::{
    optional_duration_env_ms, optional_trimmed_env, positive_usize_env, required_env,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferenceConfig {
    Stub,
    OpenAi(OpenAiConfig),
    Gemini(GeminiConfig),
    Anthropic(AnthropicConfig),
    Grok(GrokConfig),
    Oci(OciConfig),
}

impl InferenceConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_MODEL_PROVIDER").unwrap_or_else(|_| "stub".to_string());
        Self::from_named_provider("OA_MODEL_PROVIDER", provider)
    }

    fn from_named_provider(key: &'static str, provider: String) -> ConfigResult<Self> {
        match provider.as_str() {
            "stub" => Ok(Self::Stub),
            "openai" => Ok(Self::OpenAi(OpenAiConfig::from_env()?)),
            "gemini" => Ok(Self::Gemini(GeminiConfig::from_env()?)),
            "anthropic" => Ok(Self::Anthropic(AnthropicConfig::from_env()?)),
            "grok" => Ok(Self::Grok(GrokConfig::from_env()?)),
            "oci" => Ok(Self::Oci(OciConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key,
                value: provider,
                expected: "stub, openai, gemini, anthropic, grok, oci",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
    pub batch_enabled: bool,
    pub batch_max_jobs: usize,
    pub batch_max_bytes: usize,
    pub batch_poll_interval: Duration,
}

impl GeminiConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let api_key = required_env("OA_GEMINI_API_KEY")?;
        Self::from_api_key_and_models(api_key, heavy_model, fast_model)
    }

    pub fn from_api_key_and_models(
        api_key: String,
        heavy_model: String,
        fast_model: String,
    ) -> ConfigResult<Self> {
        Ok(Self {
            api_key,
            base_url: env::var("OA_GEMINI_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".to_string()),
            max_output_tokens: env::var("OA_GEMINI_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            repair_max_output_tokens: env::var("OA_GEMINI_REPAIR_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(8000),
            heavy_model,
            fast_model,
            batch_enabled: env::var("OA_GEMINI_BATCH_ENABLED")
                .ok()
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            batch_max_jobs: positive_usize_env("OA_GEMINI_BATCH_MAX_JOBS")?.unwrap_or(16),
            batch_max_bytes: positive_usize_env("OA_GEMINI_BATCH_MAX_BYTES")?.unwrap_or(1_500_000),
            batch_poll_interval: optional_duration_env_ms("OA_GEMINI_BATCH_POLL_INTERVAL_MS")?
                .unwrap_or(Duration::from_secs(5)),
        })
    }

    pub fn from_api_key_and_model(api_key: String, model: String) -> ConfigResult<Self> {
        Self::from_api_key_and_models(api_key, model.clone(), model)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub reasoning_effort_override: OpenAiReasoningEffort,
    pub heavy_model: String,
    pub fast_model: String,
}

impl OpenAiConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let api_key = required_env("OA_OPENAI_API_KEY")?;
        Self::from_api_key_and_models(api_key, heavy_model, fast_model)
    }

    pub fn from_api_key_and_models(
        api_key: String,
        heavy_model: String,
        fast_model: String,
    ) -> ConfigResult<Self> {
        Ok(Self {
            api_key,
            base_url: env::var("OA_OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            max_output_tokens: env::var("OA_OPENAI_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            repair_max_output_tokens: env::var("OA_OPENAI_REPAIR_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(8000),
            reasoning_effort_override: OpenAiReasoningEffort::from_env()?,
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiReasoningEffort {
    Auto,
    None,
    Minimal,
    Low,
    Medium,
    High,
}

impl OpenAiReasoningEffort {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_OPENAI_REASONING_EFFORT").unwrap_or_else(|_| "auto".to_string());
        match value.as_str() {
            "auto" => Ok(Self::Auto),
            "none" => Ok(Self::None),
            "minimal" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OPENAI_REASONING_EFFORT",
                value,
                expected: "auto, none, minimal, low, medium, high",
            }),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnthropicConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
}

impl AnthropicConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let api_key = required_env("OA_ANTHROPIC_API_KEY")?;
        Self::from_api_key_and_models(api_key, heavy_model, fast_model)
    }

    pub fn from_api_key_and_models(
        api_key: String,
        heavy_model: String,
        fast_model: String,
    ) -> ConfigResult<Self> {
        Ok(Self {
            api_key,
            base_url: env::var("OA_ANTHROPIC_BASE_URL")
                .unwrap_or_else(|_| "https://api.anthropic.com/v1".to_string()),
            max_output_tokens: env::var("OA_ANTHROPIC_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
}

impl GrokConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        Ok(Self {
            api_key: required_env("OA_GROK_API_KEY")?,
            base_url: env::var("OA_GROK_BASE_URL")
                .unwrap_or_else(|_| "https://api.x.ai/v1".to_string()),
            max_output_tokens: env::var("OA_GROK_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            repair_max_output_tokens: env::var("OA_GROK_REPAIR_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(8000),
            heavy_model,
            fast_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciConfig {
    pub region: String,
    pub compartment_id: String,
    pub cli_path: String,
    pub profile: Option<String>,
    pub max_output_tokens: u32,
    pub repair_max_output_tokens: u32,
    pub heavy_model: String,
    pub fast_model: String,
}

impl OciConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let heavy_model = required_env("OA_HEAVY_MODEL")?;
        let fast_model = required_env("OA_FAST_MODEL")?;
        let max_output_tokens = env::var("OA_OCI_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(4000);
        let repair_max_output_tokens = env::var("OA_OCI_REPAIR_MAX_OUTPUT_TOKENS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(8000);
        Ok(Self {
            region: required_env("OA_OCI_REGION")?,
            compartment_id: required_env("OA_OCI_COMPARTMENT_ID")?,
            cli_path: env::var("OA_OCI_CLI_PATH").unwrap_or_else(|_| "oci".to_string()),
            profile: optional_trimmed_env("OA_OCI_PROFILE"),
            max_output_tokens,
            repair_max_output_tokens,
            heavy_model,
            fast_model,
        })
    }
}
