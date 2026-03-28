#![deny(warnings)]

use anyhow::{Context, Result};
use open_archive::config::OracleConfig;
use open_archive::db;

fn main() -> Result<()> {
    println!("probe:start");
    let config = OracleConfig::from_env()?;
    println!("probe:config");
    let conn = db::connect(&config)?;
    println!("probe:connected");
    let (connected, service_name): (i32, String) = conn
        .query_row_as(
            "select 1 as connected, sys_context('USERENV', 'SERVICE_NAME') as service_name from dual",
            &[],
        )
        .context("connected, but test query failed")?;
    println!("connected={connected}");
    println!("service_name={service_name}");
    println!("connect_string={}", config.connect_string);
    Ok(())
}
