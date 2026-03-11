use crate::error::{ConfigError, ConfigResult};
use std::env;
use std::path::PathBuf;

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
    Oracle(DbConfig),
}

impl RelationalStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_RELATIONAL_STORE").unwrap_or_else(|_| "oracle".to_string());
        match provider.as_str() {
            "oracle" => Ok(Self::Oracle(DbConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                value: provider,
                expected: "oracle",
            }),
        }
    }

    pub fn oracle_config(&self) -> Option<&DbConfig> {
        match self {
            Self::Oracle(config) => Some(config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    // Placeholder until the first real object-store provider lands.
    Disabled,
}

impl ObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = env::var("OA_OBJECT_STORE").unwrap_or_else(|_| "disabled".to_string());
        match provider.as_str() {
            "disabled" => Ok(Self::Disabled),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OBJECT_STORE",
                value: provider,
                expected: "disabled",
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
pub struct DbConfig {
    pub wallet_dir: PathBuf,
    pub tns_alias: String,
    pub username: String,
    pub password: String,
}

impl DbConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let wallet_dir = if let Ok(value) = env::var("WALLET_DIR") {
            PathBuf::from(value)
        } else {
            let home = env::var("HOME")
                .map_err(|_| ConfigError::MissingEnvWithDependency { key: "HOME" })?;
            PathBuf::from(home).join(".clean-engine").join("wallet")
        };

        let tns_alias = env::var("TNS_ALIAS").unwrap_or_else(|_| "cleanengine_medium".to_string());
        let username =
            env::var("DB_USERNAME").map_err(|_| ConfigError::MissingEnv { key: "DB_USERNAME" })?;
        let password = resolve_password()?;

        Ok(Self {
            wallet_dir,
            tns_alias,
            username,
            password,
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

fn resolve_password() -> ConfigResult<String> {
    for key in [
        "DB_PASSWORD",
        "DB_DEV_PASSWORD",
        "DB_PROD_PASSWORD",
        "DB_ADMIN_PASSWORD",
    ] {
        if let Ok(value) = env::var(key) {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }
    }

    Err(ConfigError::MissingPassword)
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
            "OA_INFERENCE_PROVIDER",
            "WALLET_DIR",
            "TNS_ALIAS",
            "DB_USERNAME",
            "DB_PASSWORD",
            "DB_DEV_PASSWORD",
            "DB_PROD_PASSWORD",
            "DB_ADMIN_PASSWORD",
            "OA_HTTP_BIND",
            "OA_HTTP_REQUEST_WORKERS",
            "OA_ENRICHMENT_WORKERS",
            "OA_ENRICHMENT_POLL_INTERVAL_MS",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn app_config_defaults_to_oracle_and_stubbed_future_providers() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("HOME", "/tmp/open-archive-test-home");
        std::env::set_var("DB_USERNAME", "test-user");
        std::env::set_var("DB_PASSWORD", "test-password");

        let config = AppConfig::from_env().expect("app config should load");

        assert!(matches!(
            config.relational_store,
            RelationalStoreConfig::Oracle(_)
        ));
        assert_eq!(config.object_store, ObjectStoreConfig::Disabled);
        assert_eq!(config.inference, InferenceConfig::Stub);
    }

    #[test]
    fn invalid_relational_store_provider_is_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("OA_RELATIONAL_STORE", "postgres");
        std::env::set_var("HOME", "/tmp/open-archive-test-home");
        std::env::set_var("DB_USERNAME", "test-user");
        std::env::set_var("DB_PASSWORD", "test-password");

        let error = AppConfig::from_env().expect_err("provider should be rejected");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                ..
            }
        ));
    }
}
