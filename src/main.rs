#![deny(warnings)]

mod cli;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use open_archive::app::ArchiveApplication;
use open_archive::bootstrap::{build_service_bundle, require_oracle_db_config};
use open_archive::config::EnrichmentPipelineConfig;
use open_archive::config::{AppConfig, InferenceExecutionMode};
use open_archive::enrichment_worker::start_enrichment_pipeline;
use open_archive::migrations;
use open_archive::shutdown::ShutdownToken;

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
    /// Install Qdrant vector store sidecar
    InstallQdrant,
    /// Interactive first-run setup
    Init(cli::init::InitArgs),
    /// Import content into the archive
    Import(cli::import::ImportArgs),
    /// List artifacts in the archive
    Artifacts(cli::query::ArtifactsArgs),
    /// Load full detail for one artifact
    Artifact(cli::query::ArtifactArgs),
    /// Search the archive
    Search(cli::query::SearchArgs),
    /// View artifacts ordered chronologically
    Timeline(cli::query::TimelineArgs),
    /// Review queue management
    Review(cli::query::ReviewArgs),
    /// Check Oracle database connectivity
    OracleCheck,
    /// Apply database migrations
    Migrate,
    /// Check database migration status
    MigrateCheck,
    /// Show archive status summary
    Status,
    /// Check system health
    Doctor,
    /// Start HTTP server
    Serve,
    /// Drain job queue and exit when empty
    Enrich(EnrichArgs),
}

#[derive(Args)]
struct EnrichArgs {
    /// Parallel enrichment workers, default 1
    #[arg(long, value_name = "N")]
    workers: Option<usize>,

    /// Max jobs to process then exit, default unlimited
    #[arg(long, value_name = "N")]
    limit: Option<usize>,
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    match cli.command {
        Command::InstallQdrant => install_qdrant(),
        Command::Init(args) => cli::init::init_command(args),
        Command::Import(args) => cli::import::import_command(args),
        Command::Artifacts(args) => cli::query::artifacts_command(args),
        Command::Artifact(args) => cli::query::artifact_command(args),
        Command::Search(args) => cli::query::search_command(args),
        Command::Timeline(args) => cli::query::timeline_command(args),
        Command::Review(args) => cli::query::review_command(args),
        Command::OracleCheck => oracle_check(),
        Command::Migrate => {
            let config = AppConfig::load().context("failed to load application configuration")?;
            migrations::migrate(&config).context("failed to apply database migrations")
        }
        Command::MigrateCheck => {
            let config = AppConfig::load().context("failed to load application configuration")?;
            migrations::check(&config).context("database migration check failed")
        }
        Command::Status => cli::status::status_command(),
        Command::Doctor => cli::doctor::doctor_command(),
        Command::Serve => serve(),
        Command::Enrich(args) => enrich_command(args),
    }
}

fn oracle_check() -> Result<(), anyhow::Error> {
    let config = AppConfig::load().context("failed to load application configuration")?;
    let config = require_oracle_db_config(&config)
        .context("failed to resolve Oracle database configuration")?;
    let conn = open_archive::db::connect(config).context("failed to connect to Oracle")?;
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

fn install_qdrant() -> Result<(), anyhow::Error> {
    let config = AppConfig::load().context("failed to load application configuration")?;
    let path = open_archive::qdrant_sidecar::install_managed_qdrant(&config)
        .context("failed to install managed Qdrant")?;
    println!("qdrant_binary={}", path.display());
    Ok(())
}

fn serve() -> Result<(), anyhow::Error> {
    env_logger::init();

    let mut app_config = AppConfig::load().context("failed to load application configuration")?;
    app_config
        .validate_enrichment_ready()
        .context("enrichment not configured")?;
    let _managed_qdrant = open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut app_config)
        .context("failed to prepare managed Qdrant")?;
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
                    open_archive::enrichment_worker::EnrichmentPipelineResources {
                        job_store: Arc::clone(&services.enrichment_store),
                        read_store: Arc::clone(&services.read_store),
                        state_store: Arc::clone(&services.state_store),
                        derived_store: Arc::clone(&services.derived_store),
                        cross_artifact_store: services.cross_artifact_store.clone(),
                        embedding_store: services.embedding_store.clone(),
                        embedding_provider: services.embedding_provider.clone(),
                    },
                    shutdown.clone(),
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
                    open_archive::enrichment_worker::EnrichmentPipelineResources {
                        job_store: Arc::clone(&services.enrichment_store),
                        read_store: Arc::clone(&services.read_store),
                        state_store: Arc::clone(&services.state_store),
                        derived_store: Arc::clone(&services.derived_store),
                        cross_artifact_store: services.cross_artifact_store.clone(),
                        embedding_store: services.embedding_store.clone(),
                        embedding_provider: services.embedding_provider.clone(),
                    },
                    shutdown.clone(),
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

fn enrich_command(args: EnrichArgs) -> Result<(), anyhow::Error> {
    use open_archive::storage::JobStatus;

    env_logger::init();

    let mut app_config = AppConfig::load().context("failed to load application configuration")?;
    app_config
        .validate_enrichment_ready()
        .context("enrichment not configured")?;
    let _managed_qdrant = open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut app_config)
        .context("failed to prepare managed Qdrant")?;
    migrations::migrate(&app_config)
        .context("failed to apply database migrations before enrich")?;

    // Load services
    let services = build_service_bundle(&app_config)
        .context("failed to construct configured service providers")?;

    // Get initial status to count jobs
    let initial_status = services
        .operator_store
        .load_archive_status()
        .context("failed to load archive status")?;

    // Calculate initial counts by status
    let count_by_status = |status: JobStatus| -> usize {
        initial_status
            .jobs_by_status
            .iter()
            .find(|count| count.job_status == status)
            .map(|count| count.count)
            .unwrap_or(0)
    };

    let initial_pending = count_by_status(JobStatus::Pending);
    let initial_running = count_by_status(JobStatus::Running);
    let initial_retryable = count_by_status(JobStatus::Retryable);
    let initial_completed = count_by_status(JobStatus::Completed);
    let initial_failed = count_by_status(JobStatus::Failed);
    let initial_partial = count_by_status(JobStatus::Partial);

    // Total work to process: pending + running + retryable
    let total_jobs = initial_pending + initial_running + initial_retryable;

    if total_jobs == 0 {
        println!("no pending jobs");
        return Ok(());
    }

    let workers = args.workers.unwrap_or(1).max(1);
    let limit = args.limit;

    println!("enrichment_workers={}", workers);
    println!("pending_jobs={}", total_jobs);

    // Build pipeline config with --workers flag overriding env
    let mut pipeline_config = EnrichmentPipelineConfig::from_env()
        .context("failed to load enrichment pipeline configuration")?;

    // Override worker counts with --workers flag
    pipeline_config.direct.extract_workers = workers;
    pipeline_config.direct.reconcile_workers = workers;
    pipeline_config.direct.embedding_workers = workers;
    pipeline_config.batch.extract_workers = workers;
    pipeline_config.batch.reconcile_workers = workers;
    pipeline_config.batch.embedding_workers = workers;

    let shutdown = ShutdownToken::new();
    let inference_mode = app_config.inference_mode;

    // Set up Ctrl+C handler
    {
        let shutdown = shutdown.clone();
        ctrlc::set_handler(move || {
            log::info!("Received interrupt signal, signaling shutdown...");
            shutdown.signal();
        })?;
    }

    // Start enrichment pipeline
    let enrichment_workers = start_enrichment_pipeline(
        &pipeline_config,
        inference_mode,
        open_archive::enrichment_worker::EnrichmentPipelineResources {
            job_store: Arc::clone(&services.enrichment_store),
            read_store: Arc::clone(&services.read_store),
            state_store: Arc::clone(&services.state_store),
            derived_store: Arc::clone(&services.derived_store),
            cross_artifact_store: services.cross_artifact_store.clone(),
            embedding_store: services.embedding_store.clone(),
            embedding_provider: services.embedding_provider.clone(),
        },
        shutdown.clone(),
        Arc::clone(&services.processor_factory),
    )
    .context("failed to start enrichment pipeline")?;

    // Monitor loop: poll for progress and detect queue drain
    let mut processed_count = 0usize;
    let mut last_total_active = total_jobs;
    let poll_interval = Duration::from_millis(500);

    loop {
        if shutdown.is_shutdown() {
            break;
        }

        thread::sleep(poll_interval);

        // Check current status
        let status = match services.operator_store.load_archive_status() {
            Ok(s) => s,
            Err(err) => {
                log::warn!("Failed to load archive status: {}", err);
                continue;
            }
        };

        // Calculate current active jobs (pending + running + retryable)
        let count_by_status = |job_status: JobStatus| -> usize {
            status
                .jobs_by_status
                .iter()
                .find(|count| count.job_status == job_status)
                .map(|count| count.count)
                .unwrap_or(0)
        };

        let current_pending = count_by_status(JobStatus::Pending);
        let current_running = count_by_status(JobStatus::Running);
        let current_retryable = count_by_status(JobStatus::Retryable);
        let current_total_active = current_pending + current_running + current_retryable;

        // Detect progress (jobs moved out of active states)
        if current_total_active < last_total_active {
            let completed_this_cycle = last_total_active - current_total_active;
            for _ in 0..completed_this_cycle {
                processed_count += 1;
                if processed_count <= total_jobs {
                    println!("completed job {} of {}", processed_count, total_jobs);
                }
            }
        }

        last_total_active = current_total_active;

        // Check if limit reached
        if let Some(limit_val) = limit {
            if processed_count >= limit_val {
                println!("limit reached: processed {} jobs", processed_count);
                shutdown.signal();
                break;
            }
        }

        // Check if queue drained
        if current_total_active == 0 {
            shutdown.signal();
            break;
        }
    }

    // Wait for all workers to complete
    for worker in enrichment_workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("Enrichment worker thread panicked"))?;
    }

    // Get final status to calculate accurate summary
    let final_status = services
        .operator_store
        .load_archive_status()
        .context("failed to load final archive status")?;

    let count_by_status = |job_status: JobStatus| -> usize {
        final_status
            .jobs_by_status
            .iter()
            .find(|count| count.job_status == job_status)
            .map(|count| count.count)
            .unwrap_or(0)
    };

    let final_completed = count_by_status(JobStatus::Completed);
    let final_failed = count_by_status(JobStatus::Failed);
    let final_partial = count_by_status(JobStatus::Partial);

    // Calculate jobs that finished during this session
    // Completed = new completed + new partial
    // Failed = new failed
    let newly_completed = final_completed.saturating_sub(initial_completed);
    let newly_failed = final_failed.saturating_sub(initial_failed);
    let newly_partial = final_partial.saturating_sub(initial_partial);

    let enriched_count = newly_completed + newly_partial;
    let failed_count = newly_failed;

    // Print final summary
    println!("enriched={} failed={}", enriched_count, failed_count);

    // Exit with appropriate code
    if failed_count > 0 {
        return Err(anyhow::anyhow!("{} job(s) failed", failed_count));
    }

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
                let response = open_archive::http::build_response(&mut request, app);
                if let Err(err) = request.respond(response) {
                    log::error!("http_respond_error={err}");
                }
            }
            Ok(None) => continue,
            Err(err) => {
                log::error!("http_worker_error={err}");
                shutdown.signal();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use open_archive::app::{ArchiveApplication, ArchiveApplicationDeps};
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
            items: Mutex::new(vec![Err(std::io::Error::other("test error"))].into()),
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
        Arc::new(ArchiveApplication::new(ArchiveApplicationDeps {
            import_store,
            read_store,
            retrieval_store,
            search_read_store: None,
            artifact_detail_store: None,
            context_pack_store: None,
            cross_artifact_store: None,
            object_search_store: None,
            review_store: None,
            object_search_embedding_provider: None,
            object_store,
            writeback_store: None,
        }))
    }
}
