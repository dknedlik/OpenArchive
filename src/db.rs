use crate::config::OracleConfig;
use crate::error::{DbError, DbResult};
use oracle::pool::{GetMode, Pool, PoolBuilder};
use oracle::Connection;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static POOLS: OnceLock<Mutex<HashMap<OracleConfig, Pool>>> = OnceLock::new();

pub fn connect(config: &OracleConfig) -> DbResult<Connection> {
    let pool = pool_for(config)?;
    let conn = pool.get().map_err(|source| DbError::AcquireConnection {
        connect_string: config.connect_string.clone(),
        source,
    })?;

    if let Some(timeout) = config.call_timeout {
        conn.set_call_timeout(Some(timeout))
            .map_err(|source| DbError::SetCallTimeout { source })?;
    }

    Ok(conn)
}

fn pool_for(config: &OracleConfig) -> DbResult<Pool> {
    let pools = POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = match pools.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if let Some(pool) = guard.get(config) {
        return Ok(pool.clone());
    }

    let mut builder = PoolBuilder::new(&config.username, &config.password, &config.connect_string);
    builder.min_connections(config.pool.min_connections);
    builder.max_connections(config.pool.max_connections);
    builder.connection_increment(config.pool.connection_increment);
    builder.stmt_cache_size(config.pool.stmt_cache_size);
    builder.get_mode(GetMode::TimedWait(config.pool.get_timeout));
    builder
        .ping_interval(Some(config.pool.ping_interval))
        .map_err(|source| DbError::ConfigurePoolPingInterval { source })?;

    let pool = builder.build().map_err(|source| DbError::CreatePool {
        connect_string: config.connect_string.clone(),
        source,
    })?;
    guard.insert(config.clone(), pool.clone());
    Ok(pool)
}
