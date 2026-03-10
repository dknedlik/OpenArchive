use crate::error::{ConfigError, ConfigResult};
use std::env;
use std::path::PathBuf;

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
