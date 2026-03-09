use crate::config::DbConfig;
use anyhow::{Context, Result};
use oracle::pool::{GetMode, Pool, PoolBuilder};
use oracle::Connection;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

static POOLS: OnceLock<Mutex<HashMap<DbConfig, Pool>>> = OnceLock::new();

pub fn connect(config: &DbConfig) -> Result<Connection> {
    ensure_tns_admin(config);

    let pool = pool_for(config)?;
    let conn = pool
        .get()
        .with_context(|| format!("failed to acquire Oracle connection for alias {}", config.tns_alias))?;

    if let Some(timeout) = oracle_call_timeout()? {
        conn.set_call_timeout(Some(timeout))
            .context("failed to set Oracle call timeout")?;
    }

    Ok(conn)
}

fn pool_for(config: &DbConfig) -> Result<Pool> {
    let pools = POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = match pools.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if let Some(pool) = guard.get(config) {
        return Ok(pool.clone());
    }

    let mut builder = PoolBuilder::new(&config.username, &config.password, &config.tns_alias);
    builder.min_connections(pool_min_connections()?);
    builder.max_connections(pool_max_connections()?);
    builder.connection_increment(pool_connection_increment()?);
    builder.stmt_cache_size(pool_stmt_cache_size()?);
    builder.get_mode(GetMode::TimedWait(pool_get_timeout()?));
    builder
        .ping_interval(Some(pool_ping_interval()?))
        .context("failed to configure Oracle pool ping interval")?;

    let pool = builder
        .build()
        .with_context(|| format!("failed to create Oracle pool for alias {}", config.tns_alias))?;
    guard.insert(config.clone(), pool.clone());
    Ok(pool)
}

fn ensure_tns_admin(config: &DbConfig) {
    if std::env::var("TNS_ADMIN").is_err() {
        std::env::set_var("TNS_ADMIN", &config.wallet_dir);
    }
}

fn oracle_call_timeout() -> Result<Option<Duration>> {
    duration_env_ms("OA_ORACLE_CALL_TIMEOUT_MS")
}

fn pool_min_connections() -> Result<u32> {
    u32_env("OA_DB_POOL_MIN").map(|value| value.unwrap_or(1))
}

fn pool_max_connections() -> Result<u32> {
    u32_env("OA_DB_POOL_MAX").map(|value| value.unwrap_or(8))
}

fn pool_connection_increment() -> Result<u32> {
    u32_env("OA_DB_POOL_INCREMENT").map(|value| value.unwrap_or(1))
}

fn pool_stmt_cache_size() -> Result<u32> {
    u32_env("OA_DB_POOL_STMT_CACHE_SIZE").map(|value| value.unwrap_or(50))
}

fn pool_get_timeout() -> Result<Duration> {
    duration_env_ms("OA_DB_POOL_GET_TIMEOUT_MS").map(|value| {
        value.unwrap_or_else(|| Duration::from_secs(30))
    })
}

fn pool_ping_interval() -> Result<Duration> {
    duration_env_ms("OA_DB_POOL_PING_INTERVAL_MS").map(|value| {
        value.unwrap_or_else(|| Duration::from_secs(60))
    })
}

fn u32_env(key: &str) -> Result<Option<u32>> {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse::<u32>()
            .with_context(|| format!("invalid {key} value {raw:?}; expected integer"))
            .map(Some),
        Err(_) => Ok(None),
    }
}

fn duration_env_ms(key: &str) -> Result<Option<Duration>> {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .with_context(|| format!("invalid {key} value {raw:?}; expected integer milliseconds"))
            .map(Duration::from_millis)
            .map(Some),
        Err(_) => Ok(None),
    }
}
