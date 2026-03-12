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
}

impl AppConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            http: HttpConfig::from_env()?,
            relational_store: RelationalStoreConfig::from_env()?,
            object_store: ObjectStoreConfig::from_env()?,
            inference: InferenceConfig::from_env()?,
        })
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
            .map_err(|_| ConfigError::MissingEnv { key: "OA_POSTGRES_URL" })?;

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
            "s3" => Ok(Self::S3Compatible(S3CompatibleObjectStoreConfig::from_env()?)),
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
    // Placeholder until local or hosted inference providers are wired in.
    Stub,
}

impl InferenceConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_INFERENCE_PROVIDER").unwrap_or_else(|_| "stub".to_string());
        match provider.as_str() {
            "stub" => Ok(Self::Stub),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_INFERENCE_PROVIDER",
                value: provider,
                expected: "stub",
            }),
        }
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
        let connect_string = env::var("OA_ORACLE_CONNECT_STRING")
            .map_err(|_| ConfigError::MissingEnv { key: "OA_ORACLE_CONNECT_STRING" })?;
        let username = env::var("OA_ORACLE_USERNAME")
            .map_err(|_| ConfigError::MissingEnv { key: "OA_ORACLE_USERNAME" })?;
        let password = env::var("OA_ORACLE_PASSWORD")
            .map_err(|_| ConfigError::MissingEnv { key: "OA_ORACLE_PASSWORD" })?;

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
            let value =
                raw.parse::<u32>()
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
            let value =
                raw.parse::<u64>()
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
        std::env::set_var("OA_POSTGRES_URL", "postgres://test:test@localhost/open_archive");

        let config = AppConfig::from_env().expect("app config should load");

        assert!(matches!(
            config.relational_store,
            RelationalStoreConfig::Postgres(_)
        ));
        assert!(matches!(
            config.object_store,
            ObjectStoreConfig::LocalFs(_)
        ));
        assert_eq!(config.inference, InferenceConfig::Stub);
    }

    #[test]
    fn postgres_provider_loads_when_configured() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "postgres");
        std::env::set_var("OA_POSTGRES_URL", "postgres://test:test@localhost/open_archive");

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
        std::env::set_var("OA_POSTGRES_URL", "postgres://test:test@localhost/open_archive");

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
        std::env::set_var("OA_POSTGRES_URL", "postgres://test:test@localhost/open_archive");
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
}
