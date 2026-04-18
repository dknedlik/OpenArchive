use crate::error::{ConfigError, ConfigResult};

use super::env::{optional_trimmed_env, positive_usize_env, required_env};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingConfig {
    Disabled,
    Stub(StubEmbeddingConfig),
    Gemini(GeminiEmbeddingConfig),
    OpenAi(OpenAiEmbeddingConfig),
    OpenAiCompatible(OpenAiCompatibleEmbeddingConfig),
}

impl EmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider =
            std::env::var("OA_EMBEDDING_PROVIDER").unwrap_or_else(|_| "disabled".to_string());
        match provider.as_str() {
            "disabled" => Ok(Self::Disabled),
            "stub" => Ok(Self::Stub(StubEmbeddingConfig::from_env()?)),
            "gemini" => Ok(Self::Gemini(GeminiEmbeddingConfig::from_env()?)),
            "openai" => Ok(Self::OpenAi(OpenAiEmbeddingConfig::from_env()?)),
            "openai-compatible" => Ok(Self::OpenAiCompatible(
                OpenAiCompatibleEmbeddingConfig::from_env()?,
            )),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_EMBEDDING_PROVIDER",
                value: provider,
                expected: "disabled, stub, gemini, openai, openai-compatible",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StubEmbeddingConfig {
    pub model: String,
    pub dimensions: usize,
}

impl StubEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            model: std::env::var("OA_STUB_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "stub-embedding-small".to_string()),
            dimensions: positive_usize_env("OA_STUB_EMBEDDING_DIMENSIONS")?.unwrap_or(8),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiEmbeddingConfig {
    pub api_key: String,
    pub base_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
}

impl GeminiEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_GEMINI_API_KEY")?,
            base_url: std::env::var("OA_GEMINI_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".to_string()),
            embedding_model: std::env::var("OA_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "gemini-embedding-001".to_string()),
            embedding_dimensions: positive_usize_env("OA_EMBEDDING_DIMENSIONS")?.unwrap_or(3072),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiEmbeddingConfig {
    pub api_key: String,
    pub base_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
}

impl OpenAiEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_OPENAI_API_KEY")?,
            base_url: std::env::var("OA_OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            embedding_model: std::env::var("OA_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".to_string()),
            embedding_dimensions: positive_usize_env("OA_EMBEDDING_DIMENSIONS")?.unwrap_or(1536),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiCompatibleEmbeddingConfig {
    pub api_key: Option<String>,
    pub base_url: String,
    pub embedding_model: String,
    pub embedding_dimensions: usize,
}

impl OpenAiCompatibleEmbeddingConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: optional_trimmed_env("OA_OPENAI_COMPATIBLE_API_KEY"),
            base_url: required_env("OA_OPENAI_COMPATIBLE_BASE_URL")?,
            embedding_model: required_env("OA_EMBEDDING_MODEL")?,
            embedding_dimensions: positive_usize_env("OA_EMBEDDING_DIMENSIONS")?.unwrap_or(768),
        })
    }
}
