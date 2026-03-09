mod config;
mod db;
mod migrations;
mod storage;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::DbConfig;

#[derive(Parser)]
#[command(name = "open_archive")]
#[command(about = "OpenArchive bootstrap CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    AdbCheck,
    Migrate,
    MigrateCheck,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::AdbCheck => adb_check(),
        Command::Migrate => {
            let config = DbConfig::from_env()?;
            migrations::migrate(&config)
        }
        Command::MigrateCheck => {
            let config = DbConfig::from_env()?;
            migrations::check(&config)
        }
    }
}

fn adb_check() -> Result<()> {
    let config = DbConfig::from_env()?;
    let conn = db::connect(&config)?;
    let row = conn
        .query_row_as::<(i32, String)>(
            "select 1 as connected, sys_context('USERENV', 'SERVICE_NAME') as service_name from dual",
            &[],
        )
        .context("connected, but test query failed")?;

    println!("connected={}", row.0);
    println!("service_name={}", row.1);
    println!("username={}", config.username);
    println!("tns_alias={}", config.tns_alias);
    Ok(())
}
