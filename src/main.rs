use anyhow::Context;
use clap::{command, Parser, Subcommand};
use open_archive::app::ArchiveApplication;
use open_archive::bootstrap::{build_service_bundle, require_oracle_db_config};
use open_archive::config::EnrichmentPipelineConfig;
use open_archive::config::{AppConfig, InferenceExecutionMode};
use open_archive::enrichment_worker::start_enrichment_pipeline;
use open_archive::shutdown::ShutdownToken;
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
    OracleCheck,
    Migrate,
    MigrateCheck,
    Serve,
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    match cli.command {
        Command::OracleCheck => oracle_check(),
        Command::Migrate => {
            let config =
                AppConfig::from_env().context("failed to load application configuration")?;
            migrations::migrate(&config).context("failed to apply database migrations")
        }
        Command::MigrateCheck => {
            let config =
                AppConfig::from_env().context("failed to load application configuration")?;
            migrations::check(&config).context("database migration check failed")
        }
        Command::Serve => serve(),
    }
}

fn oracle_check() -> Result<(), anyhow::Error> {
    let config = AppConfig::from_env().context("failed to load application configuration")?;
    let config = require_oracle_db_config(&config)
        .context("failed to resolve Oracle database configuration")?;
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
    println!("connect_string={}", config.connect_string);
    Ok(())
}

fn serve() -> Result<(), anyhow::Error> {
    env_logger::init();

    let app_config = AppConfig::from_env().context("failed to load application configuration")?;
    migrations::migrate(&app_config).context("failed to apply database migrations before serve")?;
    let http_config = app_config.http.clone();
    let services = build_service_bundle(&app_config)
        .context("failed to construct configured service providers")?;
    let bind_addr = http_config.bind_addr.clone();
    let request_worker_count = http_config.request_worker_count;
    let enrichment_worker_count = http_config.enrichment_worker_count;
    let inference_mode = app_config.inference_mode;

    let shutdown = ShutdownToken::new();

    {
        let shutdown = shutdown.clone();
        ctrlc::set_handler(move || {
            log::info!("Received interrupt signal, signaling shutdown...");
            shutdown.signal();
        })?;
    }

    let app = Arc::clone(&services.app);
    let server: Arc<tiny_http::Server> = Arc::new(
        tiny_http::Server::http(&bind_addr)
            .map_err(|err| anyhow::anyhow!("failed to start HTTP server: {err}"))?,
    );

    println!("listening={bind_addr}");
    println!("request_workers={request_worker_count}");
    println!("enrichment_workers={enrichment_worker_count}");
    println!(
        "inference_mode={}",
        match inference_mode {
            InferenceExecutionMode::Direct => "direct",
            InferenceExecutionMode::Batch => "batch",
        }
    );

    let enrichment_workers = if enrichment_worker_count > 0 {
        match inference_mode {
            InferenceExecutionMode::Batch => {
                let pipeline_config = EnrichmentPipelineConfig::from_env()
                    .context("failed to load enrichment pipeline configuration")?;
                log::info!("Starting enrichment pipeline in batch mode");
                start_enrichment_pipeline(
                    &pipeline_config,
                    inference_mode,
                    Arc::clone(&services.enrichment_store),
                    Arc::clone(&services.read_store),
                    Arc::clone(&services.app.retrieval),
                    Arc::clone(&services.state_store),
                    Arc::clone(&services.derived_store),
                    services.embedding_store.clone(),
                    shutdown.clone(),
                    services.embedding_provider.clone(),
                    Arc::clone(&services.processor_factory),
                )?
            }
            InferenceExecutionMode::Direct => {
                let pipeline_config = EnrichmentPipelineConfig::from_env()
                    .context("failed to load enrichment pipeline configuration")?;
                log::info!("Starting enrichment pipeline in direct mode");
                start_enrichment_pipeline(
                    &pipeline_config,
                    inference_mode,
                    Arc::clone(&services.enrichment_store),
                    Arc::clone(&services.read_store),
                    Arc::clone(&services.app.retrieval),
                    Arc::clone(&services.state_store),
                    Arc::clone(&services.derived_store),
                    services.embedding_store.clone(),
                    shutdown.clone(),
                    services.embedding_provider.clone(),
                    Arc::clone(&services.processor_factory),
                )?
            }
        }
    } else {
        log::info!("Enrichment workers disabled (OA_ENRICHMENT_WORKERS=0)");
        Vec::new()
    };
    let mut workers = Vec::with_capacity(request_worker_count);
    for worker_index in 0..request_worker_count {
        let server = Arc::clone(&server);
        let app = Arc::clone(&app);
        let shutdown = shutdown.clone();
        workers.push(
            thread::Builder::new()
                .name(format!("http-worker-{worker_index}"))
                .spawn(move || {
                    run_http_worker_loop(server.as_ref(), app.as_ref(), shutdown);
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

fn run_http_worker_loop(
    receiver: &impl RequestReceiver,
    app: &ArchiveApplication,
    shutdown: ShutdownToken,
) {
    loop {
        if shutdown.is_shutdown() {
            break;
        }

        match receiver.recv_timeout(Duration::from_millis(500)) {
            Ok(Some(mut request)) => {
                let response = http::build_response(&mut request, app);
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

    use open_archive::app::ArchiveApplication;
    use open_archive::object_store::{NewObject, ObjectStore, PutObjectResult, StoredObject};
    use open_archive::storage::{
        ArchiveRetrievalStore, ArtifactListItem, ArtifactReadStore, ImportStatus,
        ImportWriteResult, ImportWriteStore, RetrievalIntent, RetrievedContextItem, WriteImportSet,
    };
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

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

        fn list_artifacts_filtered(
            &self,
            _filters: &open_archive::storage::ArtifactListFilters,
            _limit: usize,
            _offset: usize,
        ) -> open_archive::error::StorageResult<Vec<ArtifactListItem>> {
            Ok(self.artifacts.clone())
        }

        fn get_timeline(
            &self,
            _filters: &open_archive::storage::TimelineFilters,
            _limit: usize,
            _offset: usize,
        ) -> open_archive::error::StorageResult<Vec<open_archive::storage::TimelineEntry>> {
            Ok(Vec::new())
        }

        fn load_artifact_for_enrichment(
            &self,
            _artifact_id: &str,
        ) -> open_archive::error::StorageResult<
            Option<open_archive::storage::LoadedArtifactForEnrichment>,
        > {
            Ok(None)
        }
    }

    impl ObjectStore for MockStore {
        fn put_object(
            &self,
            object: NewObject,
        ) -> open_archive::error::ObjectStoreResult<PutObjectResult> {
            Ok(PutObjectResult {
                stored_object: StoredObject {
                    object_id: object.object_id,
                    provider: "mock".to_string(),
                    storage_key: "mock-key".to_string(),
                    mime_type: object.mime_type,
                    size_bytes: object.bytes.len() as i64,
                    sha256: object.sha256,
                },
                was_created: true,
            })
        }

        fn get_object_bytes(
            &self,
            object: &StoredObject,
        ) -> open_archive::error::ObjectStoreResult<Vec<u8>> {
            Ok(object.storage_key.as_bytes().to_vec())
        }

        fn delete_object(
            &self,
            _object: &StoredObject,
        ) -> open_archive::error::ObjectStoreResult<()> {
            Ok(())
        }
    }

    impl ArchiveRetrievalStore for MockStore {
        fn retrieve_for_intents(
            &self,
            _artifact_id: &str,
            _intents: &[RetrievalIntent],
            _limit_per_intent: usize,
        ) -> open_archive::error::StorageResult<Vec<RetrievedContextItem>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_worker_exits_on_shutdown() {
        let shutdown = ShutdownToken::new();
        shutdown.signal();
        let receiver = MockReceiver {
            items: Mutex::new(VecDeque::new()),
        };
        let app = mock_app(MockStore::new());
        run_http_worker_loop(&receiver, app.as_ref(), shutdown);
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
        let app = mock_app(MockStore::new());
        run_http_worker_loop(&receiver, app.as_ref(), shutdown.clone());
        assert!(shutdown.is_shutdown());
    }

    #[test]
    fn test_worker_continues_on_timeout() {
        let shutdown = ShutdownToken::new();
        let receiver = MockReceiver {
            items: Mutex::new(vec![Ok(None), Ok(None)].into()),
        };
        let app = mock_app(MockStore::new());
        let shutdown_clone = shutdown.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            shutdown_clone.signal();
        });
        run_http_worker_loop(&receiver, app.as_ref(), shutdown);
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
        let app = mock_app(MockStore::new());
        let shutdown_clone = shutdown.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            shutdown_clone.signal();
        });
        run_http_worker_loop(&receiver, app.as_ref(), shutdown);
    }

    #[test]
    fn test_real_server_shutdown() {
        let addr = "127.0.0.1:0";
        let server = Arc::new(tiny_http::Server::http(addr).unwrap());
        let shutdown = ShutdownToken::new();
        let app = mock_app(MockStore::new());
        let server_clone = server.clone();
        let shutdown_clone = shutdown.clone();
        let app_clone = app.clone();
        let worker = thread::spawn(move || {
            run_http_worker_loop(server_clone.as_ref(), app_clone.as_ref(), shutdown_clone);
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

    fn mock_app(store: MockStore) -> Arc<ArchiveApplication> {
        let store = Arc::new(store);
        let import_store: Arc<dyn ImportWriteStore + Send + Sync> = store.clone();
        let read_store: Arc<dyn ArtifactReadStore + Send + Sync> = store.clone();
        let retrieval_store: Arc<dyn ArchiveRetrievalStore + Send + Sync> = store.clone();
        let object_store: Arc<dyn ObjectStore + Send + Sync> = store;
        Arc::new(ArchiveApplication::new(
            import_store,
            read_store,
            retrieval_store,
            None,
            None,
            None,
            None,
            None,
            None,
            object_store,
            None,
        ))
    }
}
