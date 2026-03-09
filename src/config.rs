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
            let home = env::var("HOME").map_err(|_| ConfigError::MissingEnvWithDependency {
                key: "HOME",
            })?;
            PathBuf::from(home).join(".clean-engine").join("wallet")
        };

        let tns_alias = env::var("TNS_ALIAS").unwrap_or_else(|_| "cleanengine_medium".to_string());
        let username = env::var("DB_USERNAME")
            .map_err(|_| ConfigError::MissingEnv { key: "DB_USERNAME" })?;
        let password = resolve_password()?;

        Ok(Self {
            wallet_dir,
            tns_alias,
            username,
            password,
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
