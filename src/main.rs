use anyhow::Context;
use clap::{command, Parser, Subcommand};
use open_archive::config::{DbConfig, HttpConfig};
use open_archive::enrichment_worker::start_enrichment_workers;
use open_archive::storage::{OracleEnrichmentJobStore, OracleImportWriteStore};
use open_archive::{db, http, migrations};

use std::sync::{atomic::Ordering, Arc};
use std::thread;

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
    Serve,
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    match cli.command {
        Command::AdbCheck => adb_check(),
        Command::Migrate => {
            let config = DbConfig::from_env().context("failed to load database configuration")?;
            migrations::migrate(&config).context("failed to apply database migrations")
        }
        Command::MigrateCheck => {
            let config = DbConfig::from_env().context("failed to load database configuration")?;
            migrations::check(&config).context("database migration check failed")
        }
        Command::Serve => serve(),
    }
}

fn adb_check() -> Result<(), anyhow::Error> {
    let config = DbConfig::from_env().context("failed to load database configuration")?;
    let conn = db::connect(&config).context("failed to connect to Oracle")?;
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

fn serve() -> Result<(), anyhow::Error> {
    env_logger::init();

    let db_config = DbConfig::from_env().context("failed to load database configuration")?;
    let http_config = HttpConfig::from_env().context("failed to load HTTP configuration")?;
    let bind_addr = http_config.bind_addr.clone();
    let request_worker_count = http_config.request_worker_count;
    let enrichment_worker_count = http_config.enrichment_worker_count;
    let store = Arc::new(OracleImportWriteStore::new(db_config.clone()));
    let server: Arc<tiny_http::Server> = Arc::new(
        tiny_http::Server::http(&bind_addr)
            .map_err(|err| anyhow::anyhow!("failed to start HTTP server: {err}"))?,
    );

    println!("listening={bind_addr}");
    println!("request_workers={request_worker_count}");
    println!("enrichment_workers={enrichment_worker_count}");

    // Start enrichment workers if enabled
    let (enrichment_workers, enrichment_shutdown) = if enrichment_worker_count > 0 {
        log::info!("Starting {} enrichment workers", enrichment_worker_count);
        let (workers, shutdown) = start_enrichment_workers(
            &http_config,
            Arc::new(OracleEnrichmentJobStore::new(db_config)),
        )?;
        (workers, Some(shutdown))
    } else {
        log::info!("Enrichment workers disabled (OA_ENRICHMENT_WORKERS=0)");
        (Vec::new(), None)
    };

    let mut workers = Vec::with_capacity(request_worker_count);
    for worker_index in 0..request_worker_count {
        let server = Arc::clone(&server);
        let store = Arc::clone(&store);
        workers.push(
            thread::Builder::new()
                .name(format!("http-worker-{worker_index}"))
                .spawn(move || loop {
                    let mut request = match server.recv() {
                        Ok(request) => request,
                        Err(err) => {
                            eprintln!("http_worker_error={err}");
                            break;
                        }
                    };

                    let response = http::build_response(&mut request, store.as_ref());
                    if let Err(err) = request.respond(response) {
                        eprintln!("http_respond_error={err}");
                    }
                })?,
        );
    }

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("HTTP worker thread panicked"))?;
    }

    // Signal enrichment workers to shut down gracefully
    if let Some(shutdown) = &enrichment_shutdown {
        log::info!("Signaling enrichment workers to shut down");
        shutdown.store(true, Ordering::SeqCst);
    }

    for worker in enrichment_workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("Enrichment worker thread panicked"))?;
    }

    Ok(())
}
