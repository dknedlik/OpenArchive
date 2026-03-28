#![deny(warnings)]

use anyhow::Context;
use clap::Parser;
use open_archive::config::{AppConfig, RelationalStoreConfig};
use open_archive::postgres_db;
use open_archive::storage::postgres::job::{
    recompute_all_artifact_enrichment_statuses, recompute_artifact_enrichment_status,
};

#[derive(Debug, Parser)]
struct Args {
    /// Recompute one artifact id instead of the whole archive.
    #[arg(long)]
    artifact_id: Option<String>,
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let config = AppConfig::from_env().context("failed to load application configuration")?;
    let pg_config = match &config.relational_store {
        RelationalStoreConfig::Postgres(pg_config) => pg_config,
        _ => {
            anyhow::bail!("recompute_enrichment_status only supports OA_RELATIONAL_STORE=postgres")
        }
    };

    let mut client = postgres_db::connect(pg_config)
        .context("failed to connect to configured Postgres database")?;

    if let Some(artifact_id) = args.artifact_id {
        let status = recompute_artifact_enrichment_status(&mut client, &artifact_id)
            .with_context(|| format!("failed to recompute artifact {artifact_id}"))?;
        println!("{artifact_id}\t{}", status.as_str());
    } else {
        for (artifact_id, status) in recompute_all_artifact_enrichment_statuses(&mut client)
            .context("failed to recompute artifact enrichment statuses")?
        {
            println!("{artifact_id}\t{}", status.as_str());
        }
    }

    Ok(())
}
