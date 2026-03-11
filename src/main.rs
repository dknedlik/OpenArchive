use anyhow::Context;
use clap::{command, Parser, Subcommand};
use open_archive::config::{DbConfig, HttpConfig};
use open_archive::enrichment_worker::start_enrichment_workers;
use open_archive::shutdown::ShutdownToken;
use open_archive::storage::{
    ArtifactReadStore, ImportWriteStore, OracleEnrichmentJobStore, OracleImportWriteStore,
};
use open_archive::{db, http, migrations};

use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

    let shutdown = ShutdownToken::new();

    {
        let shutdown = shutdown.clone();
        ctrlc::set_handler(move || {
            log::info!("Received interrupt signal, signaling shutdown...");
            shutdown.signal();
        })?;
    }

    let store = Arc::new(OracleImportWriteStore::new(db_config.clone()));
    let server: Arc<tiny_http::Server> = Arc::new(
        tiny_http::Server::http(&bind_addr)
            .map_err(|err| anyhow::anyhow!("failed to start HTTP server: {err}"))?,
    );

    println!("listening={bind_addr}");
    println!("request_workers={request_worker_count}");
    println!("enrichment_workers={enrichment_worker_count}");

    let enrichment_workers = if enrichment_worker_count > 0 {
        log::info!("Starting {} enrichment workers", enrichment_worker_count);
        start_enrichment_workers(
            &http_config,
            Arc::new(OracleEnrichmentJobStore::new(db_config)),
            shutdown.clone(),
        )?
    } else {
        log::info!("Enrichment workers disabled (OA_ENRICHMENT_WORKERS=0)");
        Vec::new()
    };

    let mut workers = Vec::with_capacity(request_worker_count);
    for worker_index in 0..request_worker_count {
        let server = Arc::clone(&server);
        let store = Arc::clone(&store);
        let shutdown = shutdown.clone();
        workers.push(
            thread::Builder::new()
                .name(format!("http-worker-{worker_index}"))
                .spawn(move || {
                    run_http_worker_loop(server.as_ref(), store.as_ref(), shutdown);
                })?,
        );
    }

    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("HTTP worker thread panicked"))?;
    }

    for worker in enrichment_workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("Enrichment worker thread panicked"))?;
    }

    log::info!("Shutdown complete");
    Ok(())
}

pub trait RequestReceiver {
    fn recv_timeout(&self, timeout: Duration)
        -> Result<Option<tiny_http::Request>, std::io::Error>;
}

impl RequestReceiver for tiny_http::Server {
    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<tiny_http::Request>, std::io::Error> {
        self.recv_timeout(timeout)
    }
}

fn run_http_worker_loop<S>(receiver: &impl RequestReceiver, store: &S, shutdown: ShutdownToken)
where
    S: ImportWriteStore + ArtifactReadStore,
{
    loop {
        if shutdown.is_shutdown() {
            break;
        }

        match receiver.recv_timeout(Duration::from_millis(500)) {
            Ok(Some(mut request)) => {
                let response = http::build_response(&mut request, store);
                if let Err(err) = request.respond(response) {
                    eprintln!("http_respond_error={err}");
                }
            }
            Ok(None) => continue,
            Err(err) => {
                eprintln!("http_worker_error={err}");
                shutdown.signal();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use open_archive::storage::{ArtifactListItem, ImportStatus, ImportWriteResult, WriteImportSet};

    struct MockReceiver {
        items: Mutex<VecDeque<Result<Option<tiny_http::Request>, std::io::Error>>>,
    }

    impl RequestReceiver for MockReceiver {
        fn recv_timeout(
            &self,
            _timeout: Duration,
        ) -> Result<Option<tiny_http::Request>, std::io::Error> {
            self.items.lock().unwrap().pop_front().unwrap_or(Ok(None))
        }
    }

    #[derive(Clone)]
    struct MockStore {
        artifacts: Vec<ArtifactListItem>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                artifacts: Vec::new(),
            }
        }
    }

    impl ImportWriteStore for MockStore {
        fn write_import(
            &self,
            _import_set: WriteImportSet,
        ) -> open_archive::error::StorageResult<ImportWriteResult> {
            Ok(ImportWriteResult {
                import_id: "test".to_string(),
                import_status: ImportStatus::Completed,
                artifacts: Vec::new(),
                failed_artifact_ids: Vec::new(),
                segments_written: 0,
            })
        }
    }

    impl ArtifactReadStore for MockStore {
        fn list_artifacts(&self) -> open_archive::error::StorageResult<Vec<ArtifactListItem>> {
            Ok(self.artifacts.clone())
        }
    }

    #[test]
    fn test_worker_exits_on_shutdown() {
        let shutdown = ShutdownToken::new();
        shutdown.signal();
        let receiver = MockReceiver {
            items: Mutex::new(VecDeque::new()),
        };
        let store = MockStore::new();
        run_http_worker_loop(&receiver, &store, shutdown);
    }

    #[test]
    fn test_worker_exits_on_error() {
        let shutdown = ShutdownToken::new();
        let receiver = MockReceiver {
            items: Mutex::new(
                vec![Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "test error",
                ))]
                .into(),
            ),
        };
        let store = MockStore::new();
        run_http_worker_loop(&receiver, &store, shutdown.clone());
        assert!(shutdown.is_shutdown());
    }

    #[test]
    fn test_worker_continues_on_timeout() {
        let shutdown = ShutdownToken::new();
        let receiver = MockReceiver {
            items: Mutex::new(vec![Ok(None), Ok(None)].into()),
        };
        let store = MockStore::new();
        let shutdown_clone = shutdown.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            shutdown_clone.signal();
        });
        run_http_worker_loop(&receiver, &store, shutdown);
    }

    #[test]
    fn test_worker_processes_request_and_continues() {
        let shutdown = ShutdownToken::new();
        let receiver = MockReceiver {
            items: Mutex::new(
                vec![Ok(Some(
                    tiny_http::TestRequest::new().with_path("/artifacts").into(),
                ))]
                .into(),
            ),
        };
        let store = MockStore::new();
        let shutdown_clone = shutdown.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            shutdown_clone.signal();
        });
        run_http_worker_loop(&receiver, &store, shutdown);
    }

    #[test]
    fn test_real_server_shutdown() {
        let addr = "127.0.0.1:0";
        let server = Arc::new(tiny_http::Server::http(addr).unwrap());
        let shutdown = ShutdownToken::new();
        let store = MockStore::new();
        let server_clone = server.clone();
        let shutdown_clone = shutdown.clone();
        let store_clone = store.clone();
        let worker = thread::spawn(move || {
            run_http_worker_loop(server_clone.as_ref(), &store_clone, shutdown_clone);
        });
        thread::sleep(Duration::from_millis(100));
        shutdown.signal();
        let start = Instant::now();
        worker.join().unwrap();
        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(600),
            "Worker took too long to shutdown: {:?}",
            duration
        );
    }
}
