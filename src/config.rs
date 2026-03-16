use crate::error::{ConfigError, ConfigResult};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

/// Top-level runtime configuration. Provider-specific settings stay nested so
/// the composition root can switch implementations without leaking details into
/// the rest of the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub http: HttpConfig,
    pub relational_store: RelationalStoreConfig,
    pub object_store: ObjectStoreConfig,
    pub inference: InferenceConfig,
    pub inference_mode: InferenceExecutionMode,
}

impl AppConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            http: HttpConfig::from_env()?,
            relational_store: RelationalStoreConfig::from_env()?,
            object_store: ObjectStoreConfig::from_env()?,
            inference: InferenceConfig::from_env()?,
            inference_mode: InferenceExecutionMode::from_env()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceExecutionMode {
    Direct,
    Batch,
}

impl InferenceExecutionMode {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_INFERENCE_MODE").unwrap_or_else(|_| "batch".to_string());
        match value.as_str() {
            "direct" => Ok(Self::Direct),
            "batch" => Ok(Self::Batch),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_INFERENCE_MODE",
                value,
                expected: "direct, batch",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalStoreConfig {
    Postgres(PostgresConfig),
    Oracle(OracleConfig),
}

impl RelationalStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_RELATIONAL_STORE").unwrap_or_else(|_| "postgres".to_string());
        match provider.as_str() {
            "postgres" => Ok(Self::Postgres(PostgresConfig::from_env()?)),
            "oracle" => Ok(Self::Oracle(OracleConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                value: provider,
                expected: "postgres, oracle",
            }),
        }
    }

    pub fn oracle_config(&self) -> Option<&OracleConfig> {
        match self {
            Self::Postgres(_) => None,
            Self::Oracle(config) => Some(config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostgresConfig {
    pub connection_string: String,
}

impl PostgresConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let connection_string = env::var("OA_POSTGRES_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .map_err(|_| ConfigError::MissingEnv {
                key: "OA_POSTGRES_URL",
            })?;

        Ok(Self { connection_string })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    LocalFs(LocalFsObjectStoreConfig),
    S3Compatible(S3CompatibleObjectStoreConfig),
}

impl ObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_OBJECT_STORE").unwrap_or_else(|_| "local_fs".to_string());
        match provider.as_str() {
            "local_fs" => Ok(Self::LocalFs(LocalFsObjectStoreConfig::from_env()?)),
            "s3" => Ok(Self::S3Compatible(
                S3CompatibleObjectStoreConfig::from_env()?
            )),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OBJECT_STORE",
                value: provider,
                expected: "local_fs, s3",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFsObjectStoreConfig {
    pub root: PathBuf,
}

impl LocalFsObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let root = if let Ok(value) = env::var("OA_OBJECT_STORE_ROOT") {
            PathBuf::from(value)
        } else {
            let home = env::var("HOME")
                .map_err(|_| ConfigError::MissingEnvWithDependency { key: "HOME" })?;
            PathBuf::from(home).join(".open_archive").join("objects")
        };

        Ok(Self { root })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleObjectStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub key_prefix: Option<String>,
    pub url_style: S3UrlStyle,
}

impl S3CompatibleObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            endpoint: required_env("OA_S3_ENDPOINT")?,
            region: required_env("OA_S3_REGION")?,
            bucket: required_env("OA_S3_BUCKET")?,
            access_key_id: required_env("OA_S3_ACCESS_KEY_ID")?,
            secret_access_key: required_env("OA_S3_SECRET_ACCESS_KEY")?,
            key_prefix: optional_trimmed_env("OA_S3_KEY_PREFIX"),
            url_style: S3UrlStyle::from_env()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3UrlStyle {
    Path,
    VirtualHost,
}

impl S3UrlStyle {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_S3_URL_STYLE").unwrap_or_else(|_| "path".to_string());
        match value.as_str() {
            "path" => Ok(Self::Path),
            "virtual_host" => Ok(Self::VirtualHost),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_S3_URL_STYLE",
                value,
                expected: "path, virtual_host",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferenceConfig {
    Stub,
    OpenAi(OpenAiConfig),
    Gemini(GeminiConfig),
    Anthropic(AnthropicConfig),
    Grok(GrokConfig),
}

impl InferenceConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_INFERENCE_PROVIDER").unwrap_or_else(|_| "stub".to_string());
        match provider.as_str() {
            "stub" => Ok(Self::Stub),
            "openai" => Ok(Self::OpenAi(OpenAiConfig::from_env()?)),
            "gemini" => Ok(Self::Gemini(GeminiConfig::from_env()?)),
            "anthropic" => Ok(Self::Anthropic(AnthropicConfig::from_env()?)),
            "grok" => Ok(Self::Grok(GrokConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_INFERENCE_PROVIDER",
                value: provider,
                expected: "stub, openai, gemini, anthropic, grok",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeminiConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub standard_model: String,
    pub quality_model: Option<String>,
    pub batch_enabled: bool,
    pub batch_max_jobs: usize,
    pub batch_max_bytes: usize,
    pub batch_poll_interval: Duration,
}

impl GeminiConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_GEMINI_API_KEY")?,
            base_url: env::var("OA_GEMINI_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1beta".to_string()),
            max_output_tokens: env::var("OA_GEMINI_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            standard_model: required_env("OA_GEMINI_STANDARD_MODEL")?,
            quality_model: optional_trimmed_env("OA_GEMINI_QUALITY_MODEL"),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub reasoning_effort_override: OpenAiReasoningEffort,
    pub standard_model: String,
    pub quality_model: Option<String>,
}

impl OpenAiConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_OPENAI_API_KEY")?,
            base_url: env::var("OA_OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            max_output_tokens: env::var("OA_OPENAI_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            reasoning_effort_override: OpenAiReasoningEffort::from_env()?,
            standard_model: required_env("OA_OPENAI_STANDARD_MODEL")?,
            quality_model: optional_trimmed_env("OA_OPENAI_QUALITY_MODEL"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiReasoningEffort {
    Auto,
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
            "minimal" => Ok(Self::Minimal),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OPENAI_REASONING_EFFORT",
                value,
                expected: "auto, minimal, low, medium, high",
            }),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
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
    pub standard_model: String,
    pub quality_model: Option<String>,
}

impl AnthropicConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_ANTHROPIC_API_KEY")?,
            base_url: env::var("OA_ANTHROPIC_BASE_URL")
                .unwrap_or_else(|_| "https://api.anthropic.com/v1".to_string()),
            max_output_tokens: env::var("OA_ANTHROPIC_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            standard_model: required_env("OA_ANTHROPIC_STANDARD_MODEL")?,
            quality_model: optional_trimmed_env("OA_ANTHROPIC_QUALITY_MODEL"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrokConfig {
    pub api_key: String,
    pub base_url: String,
    pub max_output_tokens: u32,
    pub standard_model: String,
    pub quality_model: Option<String>,
}

impl GrokConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            api_key: required_env("OA_GROK_API_KEY")?,
            base_url: env::var("OA_GROK_BASE_URL")
                .unwrap_or_else(|_| "https://api.x.ai/v1".to_string()),
            max_output_tokens: env::var("OA_GROK_MAX_OUTPUT_TOKENS")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(4000),
            standard_model: required_env("OA_GROK_STANDARD_MODEL")?,
            quality_model: optional_trimmed_env("OA_GROK_QUALITY_MODEL"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OracleConfig {
    pub connect_string: String,
    pub username: String,
    pub password: String,
    pub call_timeout: Option<Duration>,
    pub pool: ConnectionPoolConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionPoolConfig {
    pub min_connections: u32,
    pub max_connections: u32,
    pub connection_increment: u32,
    pub stmt_cache_size: u32,
    pub get_timeout: Duration,
    pub ping_interval: Duration,
}

impl OracleConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let connect_string =
            env::var("OA_ORACLE_CONNECT_STRING").map_err(|_| ConfigError::MissingEnv {
                key: "OA_ORACLE_CONNECT_STRING",
            })?;
        let username = env::var("OA_ORACLE_USERNAME").map_err(|_| ConfigError::MissingEnv {
            key: "OA_ORACLE_USERNAME",
        })?;
        let password = env::var("OA_ORACLE_PASSWORD").map_err(|_| ConfigError::MissingEnv {
            key: "OA_ORACLE_PASSWORD",
        })?;

        Ok(Self {
            connect_string,
            username,
            password,
            call_timeout: optional_duration_env_ms("OA_ORACLE_CALL_TIMEOUT_MS")?,
            pool: ConnectionPoolConfig::from_env()?,
        })
    }
}

impl ConnectionPoolConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            min_connections: positive_u32_env("OA_DB_POOL_MIN")?.unwrap_or(1),
            max_connections: positive_u32_env("OA_DB_POOL_MAX")?.unwrap_or(8),
            connection_increment: positive_u32_env("OA_DB_POOL_INCREMENT")?.unwrap_or(1),
            stmt_cache_size: positive_u32_env("OA_DB_POOL_STMT_CACHE_SIZE")?.unwrap_or(50),
            get_timeout: optional_duration_env_ms("OA_DB_POOL_GET_TIMEOUT_MS")?
                .unwrap_or_else(|| Duration::from_secs(30)),
            ping_interval: optional_duration_env_ms("OA_DB_POOL_PING_INTERVAL_MS")?
                .unwrap_or_else(|| Duration::from_secs(60)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    pub bind_addr: String,
    pub request_worker_count: usize,
    pub enrichment_worker_count: usize,
    pub enrichment_poll_interval_ms: u64,
}

impl HttpConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let bind_addr = env::var("OA_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let request_worker_count = positive_usize_env("OA_HTTP_REQUEST_WORKERS")?.unwrap_or(4);
        let enrichment_worker_count = optional_usize_env("OA_ENRICHMENT_WORKERS")?.unwrap_or(1);
        let enrichment_poll_interval_ms = env::var("OA_ENRICHMENT_POLL_INTERVAL_MS")
            .map(|s| {
                s.parse::<u64>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key: "OA_ENRICHMENT_POLL_INTERVAL_MS",
                        value: s,
                    })
            })
            .unwrap_or(Ok(500))?;

        Ok(Self {
            bind_addr,
            request_worker_count,
            enrichment_worker_count,
            enrichment_poll_interval_ms,
        })
    }
}

fn positive_u32_env(key: &'static str) -> ConfigResult<Option<u32>> {
    match env::var(key) {
        Ok(raw) => {
            let value = raw
                .parse::<u32>()
                .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                    key,
                    value: raw.clone(),
                })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

fn required_env(key: &'static str) -> ConfigResult<String> {
    env::var(key).map_err(|_| ConfigError::MissingEnv { key })
}

fn optional_trimmed_env(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_duration_env_ms(key: &'static str) -> ConfigResult<Option<Duration>> {
    match env::var(key) {
        Ok(raw) => {
            let value = raw
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                    key,
                    value: raw.clone(),
                })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(Duration::from_millis(value)))
        }
        Err(_) => Ok(None),
    }
}

fn positive_usize_env(key: &'static str) -> ConfigResult<Option<usize>> {
    match env::var(key) {
        Ok(raw) => {
            let value =
                raw.parse::<usize>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key,
                        value: raw.clone(),
                    })?;
            if value == 0 {
                return Err(ConfigError::InvalidPositiveIntegerEnv { key, value: raw });
            }
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

fn optional_usize_env(key: &'static str) -> ConfigResult<Option<usize>> {
    match env::var(key) {
        Ok(raw) => {
            let value =
                raw.parse::<usize>()
                    .map_err(|_| ConfigError::InvalidPositiveIntegerEnv {
                        key,
                        value: raw.clone(),
                    })?;
            Ok(Some(value))
        }
        Err(_) => Ok(None),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageConfig {
    pub batch_size: usize,
    pub max_concurrent_batches: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentPipelineConfig {
    pub poll_interval: Duration,
    pub preprocess_workers: usize,
    pub preprocess: StageConfig,
    pub extract_workers: usize,
    pub extract: StageConfig,
    pub reconcile_workers: usize,
    pub reconcile: StageConfig,
    pub retrieve_context_workers: usize,
    pub rate_limit_requests_per_minute: u32,
}

impl EnrichmentPipelineConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let shared_worker_default = optional_usize_env("OA_ENRICHMENT_WORKERS")?.unwrap_or(1);
        Ok(Self {
            poll_interval: optional_duration_env_ms("OA_ENRICHMENT_POLL_INTERVAL_MS")?
                .unwrap_or(Duration::from_millis(2000)),
            preprocess_workers: positive_usize_env("OA_PREPROCESS_WORKERS")?
                .unwrap_or(shared_worker_default),
            preprocess: StageConfig {
                batch_size: positive_usize_env("OA_PREPROCESS_BATCH_SIZE")?.unwrap_or(3),
                max_concurrent_batches: positive_usize_env("OA_PREPROCESS_MAX_CONCURRENT")?
                    .unwrap_or(2),
            },
            extract_workers: positive_usize_env("OA_EXTRACT_WORKERS")?
                .unwrap_or(shared_worker_default),
            extract: StageConfig {
                batch_size: positive_usize_env("OA_EXTRACT_BATCH_SIZE")?.unwrap_or(5),
                max_concurrent_batches: positive_usize_env("OA_EXTRACT_MAX_CONCURRENT")?
                    .unwrap_or(3),
            },
            reconcile_workers: positive_usize_env("OA_RECONCILE_WORKERS")?
                .unwrap_or(shared_worker_default),
            reconcile: StageConfig {
                batch_size: positive_usize_env("OA_RECONCILE_BATCH_SIZE")?.unwrap_or(5),
                max_concurrent_batches: positive_usize_env("OA_RECONCILE_MAX_CONCURRENT")?
                    .unwrap_or(2),
            },
            retrieve_context_workers: positive_usize_env("OA_RETRIEVE_CONTEXT_WORKERS")?
                .unwrap_or(2),
            rate_limit_requests_per_minute: positive_u32_env("OA_RATE_LIMIT_RPM")?.unwrap_or(60),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn clear_test_env() {
        for key in [
            "OA_RELATIONAL_STORE",
            "OA_OBJECT_STORE",
            "OA_S3_ENDPOINT",
            "OA_S3_REGION",
            "OA_S3_BUCKET",
            "OA_S3_ACCESS_KEY_ID",
            "OA_S3_SECRET_ACCESS_KEY",
            "OA_S3_KEY_PREFIX",
            "OA_S3_URL_STYLE",
            "OA_INFERENCE_PROVIDER",
            "OA_INFERENCE_MODE",
            "OA_OPENAI_API_KEY",
            "OA_OPENAI_BASE_URL",
            "OA_OPENAI_MAX_OUTPUT_TOKENS",
            "OA_OPENAI_REASONING_EFFORT",
            "OA_OPENAI_STANDARD_MODEL",
            "OA_OPENAI_QUALITY_MODEL",
            "OA_GEMINI_API_KEY",
            "OA_GEMINI_BASE_URL",
            "OA_GEMINI_MAX_OUTPUT_TOKENS",
            "OA_GEMINI_STANDARD_MODEL",
            "OA_GEMINI_QUALITY_MODEL",
            "OA_ANTHROPIC_API_KEY",
            "OA_ANTHROPIC_BASE_URL",
            "OA_ANTHROPIC_MAX_OUTPUT_TOKENS",
            "OA_ANTHROPIC_STANDARD_MODEL",
            "OA_ANTHROPIC_QUALITY_MODEL",
            "OA_GROK_API_KEY",
            "OA_GROK_BASE_URL",
            "OA_GROK_MAX_OUTPUT_TOKENS",
            "OA_GROK_STANDARD_MODEL",
            "OA_GROK_QUALITY_MODEL",
            "OA_ORACLE_CONNECT_STRING",
            "OA_ORACLE_USERNAME",
            "OA_ORACLE_PASSWORD",
            "OA_ORACLE_CALL_TIMEOUT_MS",
            "OA_DB_POOL_MIN",
            "OA_DB_POOL_MAX",
            "OA_DB_POOL_INCREMENT",
            "OA_DB_POOL_STMT_CACHE_SIZE",
            "OA_DB_POOL_GET_TIMEOUT_MS",
            "OA_DB_POOL_PING_INTERVAL_MS",
            "OA_HTTP_BIND",
            "OA_HTTP_REQUEST_WORKERS",
            "OA_ENRICHMENT_WORKERS",
            "OA_ENRICHMENT_POLL_INTERVAL_MS",
            "OA_POSTGRES_URL",
            "DATABASE_URL",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn app_config_defaults_to_postgres_and_stubbed_future_providers() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );

        let config = AppConfig::from_env().expect("app config should load");

        assert!(matches!(
            config.relational_store,
            RelationalStoreConfig::Postgres(_)
        ));
        assert!(matches!(config.object_store, ObjectStoreConfig::LocalFs(_)));
        assert_eq!(config.inference, InferenceConfig::Stub);
        assert_eq!(config.inference_mode, InferenceExecutionMode::Batch);
    }

    #[test]
    fn postgres_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "postgres");
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );

        let config = AppConfig::from_env().expect("postgres provider should load");
        assert!(matches!(
            config.relational_store,
            RelationalStoreConfig::Postgres(_)
        ));
    }

    #[test]
    fn oracle_provider_loads_typed_pool_defaults() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "oracle");
        std::env::set_var("OA_ORACLE_CONNECT_STRING", "localhost:1521/FREEPDB1");
        std::env::set_var("OA_ORACLE_USERNAME", "oracle_user");
        std::env::set_var("OA_ORACLE_PASSWORD", "oracle_pass");

        let config = AppConfig::from_env().expect("oracle provider should load");
        let RelationalStoreConfig::Oracle(config) = config.relational_store else {
            panic!("expected oracle config");
        };

        assert_eq!(config.connect_string, "localhost:1521/FREEPDB1");
        assert_eq!(config.call_timeout, None);
        assert_eq!(config.pool.min_connections, 1);
        assert_eq!(config.pool.max_connections, 8);
        assert_eq!(config.pool.connection_increment, 1);
        assert_eq!(config.pool.stmt_cache_size, 50);
        assert_eq!(config.pool.get_timeout, Duration::from_secs(30));
        assert_eq!(config.pool.ping_interval, Duration::from_secs(60));
    }

    #[test]
    fn invalid_relational_store_provider_is_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "sqlite");
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );

        let error = AppConfig::from_env().expect_err("provider should be rejected");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                ..
            }
        ));
    }

    #[test]
    fn s3_object_store_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_OBJECT_STORE", "s3");
        std::env::set_var("OA_S3_ENDPOINT", "http://localhost:9000");
        std::env::set_var("OA_S3_REGION", "us-east-1");
        std::env::set_var("OA_S3_BUCKET", "openarchive");
        std::env::set_var("OA_S3_ACCESS_KEY_ID", "openarchive");
        std::env::set_var("OA_S3_SECRET_ACCESS_KEY", "openarchive-secret");
        std::env::set_var("OA_S3_KEY_PREFIX", "objects");

        let config = AppConfig::from_env().expect("s3 object-store provider should load");
        let ObjectStoreConfig::S3Compatible(config) = config.object_store else {
            panic!("expected s3 object-store config");
        };

        assert_eq!(config.endpoint, "http://localhost:9000");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.bucket, "openarchive");
        assert_eq!(config.key_prefix.as_deref(), Some("objects"));
        assert_eq!(config.url_style, S3UrlStyle::Path);
    }

    #[test]
    fn openai_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_INFERENCE_PROVIDER", "openai");
        std::env::set_var("OA_OPENAI_API_KEY", "test-key");
        std::env::set_var("OA_OPENAI_REASONING_EFFORT", "low");
        std::env::set_var("OA_OPENAI_STANDARD_MODEL", "gpt-4.1-mini");

        let config = AppConfig::from_env().expect("openai inference provider should load");
        let InferenceConfig::OpenAi(config) = config.inference else {
            panic!("expected openai config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.openai.com/v1");
        assert_eq!(config.reasoning_effort_override, OpenAiReasoningEffort::Low);
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.standard_model, "gpt-4.1-mini");
        assert_eq!(config.quality_model, None);
    }

    #[test]
    fn inference_execution_mode_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_INFERENCE_MODE", "direct");

        let config = AppConfig::from_env().expect("inference execution mode should load");
        assert_eq!(config.inference_mode, InferenceExecutionMode::Direct);
    }

    #[test]
    fn gemini_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_INFERENCE_PROVIDER", "gemini");
        std::env::set_var("OA_GEMINI_API_KEY", "test-key");
        std::env::set_var("OA_GEMINI_STANDARD_MODEL", "gemini-2.5-flash-lite");

        let config = AppConfig::from_env().expect("gemini inference provider should load");
        let InferenceConfig::Gemini(config) = config.inference else {
            panic!("expected gemini config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(
            config.base_url,
            "https://generativelanguage.googleapis.com/v1beta"
        );
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.standard_model, "gemini-2.5-flash-lite");
        assert_eq!(config.quality_model, None);
    }

    #[test]
    fn anthropic_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_INFERENCE_PROVIDER", "anthropic");
        std::env::set_var("OA_ANTHROPIC_API_KEY", "test-key");
        std::env::set_var("OA_ANTHROPIC_STANDARD_MODEL", "claude-sonnet-4-20250514");

        let config = AppConfig::from_env().expect("anthropic inference provider should load");
        let InferenceConfig::Anthropic(config) = config.inference else {
            panic!("expected anthropic config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.anthropic.com/v1");
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.standard_model, "claude-sonnet-4-20250514");
        assert_eq!(config.quality_model, None);
    }

    #[test]
    fn grok_inference_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var(
            "OA_POSTGRES_URL",
            "postgres://test:test@localhost/open_archive",
        );
        std::env::set_var("OA_INFERENCE_PROVIDER", "grok");
        std::env::set_var("OA_GROK_API_KEY", "test-key");
        std::env::set_var("OA_GROK_STANDARD_MODEL", "grok-4-fast-reasoning");

        let config = AppConfig::from_env().expect("grok inference provider should load");
        let InferenceConfig::Grok(config) = config.inference else {
            panic!("expected grok config");
        };

        assert_eq!(config.api_key, "test-key");
        assert_eq!(config.base_url, "https://api.x.ai/v1");
        assert_eq!(config.max_output_tokens, 4000);
        assert_eq!(config.standard_model, "grok-4-fast-reasoning");
        assert_eq!(config.quality_model, None);
    }
}
