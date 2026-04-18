use crate::error::{ConfigError, ConfigResult};
use std::path::PathBuf;
use std::time::Duration;

use super::env::{
    default_local_archive_root, optional_duration_env_ms, optional_path_env, positive_u32_env,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalStoreConfig {
    Postgres(PostgresConfig),
    Sqlite(SqliteConfig),
    Oracle(OracleConfig),
}

impl RelationalStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider =
            std::env::var("OA_RELATIONAL_STORE").unwrap_or_else(|_| "sqlite".to_string());
        match provider.as_str() {
            "postgres" => Ok(Self::Postgres(PostgresConfig::from_env()?)),
            "sqlite" => Ok(Self::Sqlite(SqliteConfig::from_env()?)),
            "oracle" => Ok(Self::Oracle(OracleConfig::from_env()?)),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_RELATIONAL_STORE",
                value: provider,
                expected: "postgres, sqlite, oracle",
            }),
        }
    }

    pub fn oracle_config(&self) -> Option<&OracleConfig> {
        match self {
            Self::Postgres(_) => None,
            Self::Sqlite(_) => None,
            Self::Oracle(config) => Some(config),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqliteConfig {
    pub path: PathBuf,
    pub busy_timeout: Duration,
}

impl SqliteConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            path: optional_path_env("OA_SQLITE_PATH")?
                .unwrap_or(default_local_archive_root()?.join("open_archive.db")),
            busy_timeout: optional_duration_env_ms("OA_SQLITE_BUSY_TIMEOUT_MS")?
                .unwrap_or_else(|| Duration::from_secs(30)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PostgresConfig {
    pub connection_string: String,
}

impl PostgresConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let connection_string = std::env::var("OA_POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .map_err(|_| ConfigError::MissingEnv {
                key: "OA_POSTGRES_URL",
            })?;

        Ok(Self { connection_string })
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
            std::env::var("OA_ORACLE_CONNECT_STRING").map_err(|_| ConfigError::MissingEnv {
                key: "OA_ORACLE_CONNECT_STRING",
            })?;
        let username =
            std::env::var("OA_ORACLE_USERNAME").map_err(|_| ConfigError::MissingEnv {
                key: "OA_ORACLE_USERNAME",
            })?;
        let password =
            std::env::var("OA_ORACLE_PASSWORD").map_err(|_| ConfigError::MissingEnv {
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
