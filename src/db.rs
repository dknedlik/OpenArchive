use crate::config::DbConfig;
use anyhow::{Context, Result};
use oracle::Connection;
use std::env;

pub fn connect(config: &DbConfig) -> Result<Connection> {
    env::set_var(
        "TNS_ADMIN",
        env::var("TNS_ADMIN").unwrap_or_else(|_| config.wallet_dir.to_string_lossy().into_owned()),
    );

    Connection::connect(&config.username, &config.password, &config.tns_alias)
        .with_context(|| format!("failed to connect to Oracle alias {}", config.tns_alias))
}
