#![deny(warnings)]

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use open_archive::app::ArchiveApplication;
use open_archive::bootstrap::{build_service_bundle, require_oracle_db_config, ServiceBundle};
use open_archive::config::EnrichmentPipelineConfig;
use open_archive::config::{
    AppConfig, InferenceExecutionMode, ObjectStoreConfig, RelationalStoreConfig, VectorStoreConfig,
};
use open_archive::enrichment_worker::start_enrichment_pipeline;
use open_archive::error::StorageError;
use open_archive::import_service::ImportResponse;
use open_archive::qdrant_sidecar::ManagedQdrantHandle;
use open_archive::shutdown::ShutdownToken;
use open_archive::{db, http, migrations, parser};

use std::fs;
use std::path::{Path, PathBuf};
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
    InstallQdrant,
    Import(ImportArgs),
    OracleCheck,
    Migrate,
    MigrateCheck,
    Status,
    Doctor,
    Serve,
}

#[derive(Args)]
struct ImportArgs {
    #[command(subcommand)]
    source: ImportSourceCommand,
}

#[derive(Subcommand)]
enum ImportSourceCommand {
    Auto { path: PathBuf },
    Chatgpt { path: PathBuf },
    Claude { path: PathBuf },
    Grok { path: PathBuf },
    Gemini { path: PathBuf },
    Markdown { path: PathBuf },
    Text { path: PathBuf },
    Obsidian { path: PathBuf },
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    match cli.command {
        Command::InstallQdrant => install_qdrant(),
        Command::Import(args) => import_command(args),
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
        Command::Status => status_command(),
        Command::Doctor => doctor_command(),
        Command::Serve => serve(),
    }
}

fn oracle_check() -> Result<(), anyhow::Error> {
    let config = AppConfig::from_env().context("failed to load application configuration")?;
    let config = require_oracle_db_config(&config)
        .context("failed to resolve Oracle database configuration")?;
    let conn = db::connect(config).context("failed to connect to Oracle")?;
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
    let config = AppConfig::from_env().context("failed to load application configuration")?;
    let path = open_archive::qdrant_sidecar::install_managed_qdrant(&config)
        .context("failed to install managed Qdrant")?;
    println!("qdrant_binary={}", path.display());
    Ok(())
}

struct LocalRuntime {
    config: AppConfig,
    services: ServiceBundle,
    _managed_qdrant: Option<ManagedQdrantHandle>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImportMode {
    Auto,
    Chatgpt,
    Claude,
    Grok,
    Gemini,
    Markdown,
    Text,
    Obsidian,
}

impl ImportMode {
    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Chatgpt => "chatgpt",
            Self::Claude => "claude",
            Self::Grok => "grok",
            Self::Gemini => "gemini",
            Self::Markdown => "markdown",
            Self::Text => "text",
            Self::Obsidian => "obsidian",
        }
    }
}

struct PlannedImport {
    import_path: PathBuf,
    mode: ImportMode,
    payload_bytes: Option<Vec<u8>>,
}

impl PlannedImport {
    fn with_bytes(import_path: PathBuf, mode: ImportMode, bytes: Vec<u8>) -> Self {
        Self {
            import_path,
            mode,
            payload_bytes: Some(bytes),
        }
    }

    fn directory(import_path: PathBuf, mode: ImportMode) -> Self {
        Self {
            import_path,
            mode,
            payload_bytes: None,
        }
    }
}

fn import_command(args: ImportArgs) -> Result<(), anyhow::Error> {
    let (mode, path) = match args.source {
        ImportSourceCommand::Auto { path } => (ImportMode::Auto, path),
        ImportSourceCommand::Chatgpt { path } => (ImportMode::Chatgpt, path),
        ImportSourceCommand::Claude { path } => (ImportMode::Claude, path),
        ImportSourceCommand::Grok { path } => (ImportMode::Grok, path),
        ImportSourceCommand::Gemini { path } => (ImportMode::Gemini, path),
        ImportSourceCommand::Markdown { path } => (ImportMode::Markdown, path),
        ImportSourceCommand::Text { path } => (ImportMode::Text, path),
        ImportSourceCommand::Obsidian { path } => (ImportMode::Obsidian, path),
    };
    let planned = plan_imports(mode, &path)?;
    let runtime = load_local_runtime(true)?;

    let mut import_count = 0usize;
    let mut artifact_count = 0usize;
    for item in planned {
        let response = execute_import(runtime.services.app.as_ref(), &item)?;
        import_count += 1;
        artifact_count += response.artifacts.len();
        println!(
            "imported path={} mode={} import_id={} status={} artifacts={}",
            item.import_path.display(),
            item.mode.label(),
            response.import_id,
            response.import_status,
            response.artifacts.len(),
        );
    }

    println!("imports={import_count}");
    println!("artifacts={artifact_count}");
    Ok(())
}

fn status_command() -> Result<(), anyhow::Error> {
    let runtime = load_local_runtime(true)?;
    let snapshot = runtime
        .services
        .operator_store
        .load_archive_status()
        .context("failed to load archive status")?;
    let recent_artifacts = match runtime.services.app.artifacts.list_artifacts_filtered(
        &open_archive::storage::ArtifactListFilters::default(),
        5,
        0,
    ) {
        Ok(items) => items,
        Err(StorageError::UnsupportedOperation { .. }) => runtime
            .services
            .app
            .artifacts
            .list_artifacts()
            .context("failed to load recent artifacts")?
            .into_iter()
            .take(5)
            .collect(),
        Err(err) => return Err(anyhow::Error::new(err).context("failed to load recent artifacts")),
    };

    println!(
        "relational_store={}",
        relational_store_label(&runtime.config)
    );
    println!("vector_store={}", vector_store_label(&runtime.config));
    println!("object_store={}", object_store_label(&runtime.config));
    println!("artifacts={}", snapshot.artifact_count);
    println!(
        "artifact_sources={}",
        format_source_counts(&snapshot.artifacts_by_source)
    );
    println!(
        "artifact_enrichment={}",
        format_enrichment_counts(&snapshot.artifacts_by_enrichment_status)
    );
    println!("jobs={}", format_job_counts(&snapshot.jobs_by_status));
    if recent_artifacts.is_empty() {
        println!("recent_artifacts=none");
    } else {
        println!("recent_artifacts={}", recent_artifacts.len());
        for artifact in recent_artifacts {
            println!(
                "  {} {} {} {}",
                artifact.artifact_id,
                artifact.source_type,
                artifact.enrichment_status.as_str(),
                artifact
                    .title
                    .or(artifact.note_path)
                    .unwrap_or_else(|| "(untitled)".to_string())
            );
        }
    }
    Ok(())
}

fn doctor_command() -> Result<(), anyhow::Error> {
    let config = AppConfig::from_env().context("failed to load application configuration")?;
    let mut failures = 0usize;
    print_doctor_result("config", Ok("loaded".to_string()), &mut failures);

    let db_status = match &config.relational_store {
        RelationalStoreConfig::Sqlite(sqlite) => open_archive::sqlite_db::connect(sqlite)
            .map(|_| format!("sqlite {}", sqlite.path.display()))
            .map_err(anyhow::Error::new),
        RelationalStoreConfig::Postgres(pg) => open_archive::postgres_db::connect(pg)
            .map(|_| "postgres reachable".to_string())
            .map_err(anyhow::Error::new),
        RelationalStoreConfig::Oracle(oracle) => db::connect(oracle)
            .map(|_| "oracle reachable".to_string())
            .map_err(anyhow::Error::new),
    };
    print_doctor_result("database", db_status, &mut failures);

    print_doctor_result(
        "migrations",
        migrations::check(&config)
            .map(|_| "up to date".to_string())
            .map_err(anyhow::Error::new),
        &mut failures,
    );

    let object_store_status = match &config.object_store {
        ObjectStoreConfig::LocalFs(local_fs) => {
            if local_fs.root.is_dir() {
                Ok(format!("local_fs {}", local_fs.root.display()))
            } else {
                Err(anyhow::anyhow!(
                    "object store root {} does not exist",
                    local_fs.root.display()
                ))
            }
        }
        ObjectStoreConfig::S3Compatible(s3) => Ok(format!("s3 {} {}", s3.endpoint, s3.bucket)),
    };
    print_doctor_result("object_store", object_store_status, &mut failures);

    let mut runtime_config = config.clone();
    let qdrant_status = match &runtime_config.vector_store {
        VectorStoreConfig::Disabled => Ok("disabled".to_string()),
        VectorStoreConfig::PostgresPgVector => Ok("postgres pgvector".to_string()),
        VectorStoreConfig::Qdrant(_) => {
            open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut runtime_config)
                .map(|_| match &runtime_config.vector_store {
                    VectorStoreConfig::Qdrant(qdrant) => format!("healthy {}", qdrant.url),
                    _ => "healthy".to_string(),
                })
                .map_err(anyhow::Error::new)
        }
    };
    print_doctor_result("vector_store", qdrant_status, &mut failures);

    print_doctor_result(
        "provider_wiring",
        build_service_bundle(&runtime_config)
            .map(|_| "constructed".to_string())
            .map_err(anyhow::Error::new),
        &mut failures,
    );

    if failures == 0 {
        println!("doctor=ok");
        Ok(())
    } else {
        Err(anyhow::anyhow!("doctor found {failures} issue(s)"))
    }
}

fn print_doctor_result(name: &str, result: Result<String, anyhow::Error>, failures: &mut usize) {
    match result {
        Ok(detail) => println!("[ok] {name}: {detail}"),
        Err(err) => {
            *failures += 1;
            println!("[fail] {name}: {err}");
        }
    }
}

fn load_local_runtime(apply_migrations: bool) -> Result<LocalRuntime, anyhow::Error> {
    let mut config = AppConfig::from_env().context("failed to load application configuration")?;
    let managed_qdrant = open_archive::qdrant_sidecar::ensure_managed_qdrant(&mut config)
        .context("failed to prepare managed Qdrant")?;
    if apply_migrations {
        migrations::migrate(&config).context("failed to apply database migrations")?;
    }
    let services = build_service_bundle(&config)
        .context("failed to construct configured service providers")?;
    Ok(LocalRuntime {
        config,
        services,
        _managed_qdrant: managed_qdrant,
    })
}

fn execute_import(
    app: &ArchiveApplication,
    item: &PlannedImport,
) -> Result<ImportResponse, anyhow::Error> {
    // Handle Obsidian directory imports specially
    if item.mode == ImportMode::Obsidian && item.payload_bytes.is_none() {
        return app
            .imports
            .import_obsidian_vault_directory(&item.import_path)
            .map_err(anyhow::Error::new);
    }

    // All other imports require payload bytes
    let bytes = item
        .payload_bytes
        .as_ref()
        .expect("payload_bytes required for non-directory imports");

    match item.mode {
        ImportMode::Auto => unreachable!("planned imports must resolve auto before execution"),
        ImportMode::Chatgpt => app
            .imports
            .import_chatgpt_payload(bytes)
            .map_err(anyhow::Error::new),
        ImportMode::Claude => app
            .imports
            .import_claude_payload(bytes)
            .map_err(anyhow::Error::new),
        ImportMode::Grok => app
            .imports
            .import_grok_payload(bytes)
            .map_err(anyhow::Error::new),
        ImportMode::Gemini => app
            .imports
            .import_gemini_payload(bytes)
            .map_err(anyhow::Error::new),
        ImportMode::Markdown => app
            .imports
            .import_markdown_payload(bytes)
            .map_err(anyhow::Error::new),
        ImportMode::Text => app
            .imports
            .import_text_payload(bytes)
            .map_err(anyhow::Error::new),
        ImportMode::Obsidian => app
            .imports
            .import_obsidian_vault_payload(bytes)
            .map_err(anyhow::Error::new),
    }
}

fn plan_imports(mode: ImportMode, path: &Path) -> Result<Vec<PlannedImport>, anyhow::Error> {
    match mode {
        ImportMode::Auto => plan_auto_imports(path),
        ImportMode::Chatgpt | ImportMode::Claude | ImportMode::Grok | ImportMode::Gemini => {
            plan_conversation_import(mode, path)
        }
        ImportMode::Markdown => plan_document_imports(mode, path, &["md", "markdown"]),
        ImportMode::Text => plan_document_imports(mode, path, &["txt", "text"]),
        ImportMode::Obsidian => plan_obsidian_import(path),
    }
}

fn plan_auto_imports(path: &Path) -> Result<Vec<PlannedImport>, anyhow::Error> {
    if path.is_dir() {
        if looks_like_obsidian_vault(path) {
            return plan_obsidian_import(path);
        }

        let mut planned = plan_document_imports(ImportMode::Markdown, path, &["md", "markdown"])?;
        planned.extend(plan_document_imports(
            ImportMode::Text,
            path,
            &["txt", "text"],
        )?);
        if planned.is_empty() {
            return Err(anyhow::anyhow!(
                "auto import could not detect supported content under {}",
                path.display()
            ));
        }
        planned.sort_by(|left, right| left.import_path.cmp(&right.import_path));
        return Ok(planned);
    }

    if path.is_file() {
        if is_extension(path, &["md", "markdown"]) {
            return plan_document_imports(ImportMode::Markdown, path, &["md", "markdown"]);
        }
        if is_extension(path, &["txt", "text"]) {
            return plan_document_imports(ImportMode::Text, path, &["txt", "text"]);
        }

        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        if is_extension(path, &["zip"]) {
            if parser::obsidian::parse_vault_zip(&bytes).is_ok() {
                return Ok(vec![PlannedImport::with_bytes(
                    path.to_path_buf(),
                    ImportMode::Obsidian,
                    bytes,
                )]);
            }
            // Check if zip contains conversations.json (ChatGPT export)
            if open_archive::looks_like_chatgpt_zip(&bytes) {
                return Ok(vec![PlannedImport::with_bytes(
                    path.to_path_buf(),
                    ImportMode::Chatgpt,
                    bytes,
                )]);
            }
            return Err(anyhow::anyhow!(
                "auto import does not recognize zip payload {}",
                path.display()
            ));
        }

        let detected = detect_conversation_mode(path, &bytes).ok_or_else(|| {
            anyhow::anyhow!(
                "auto import could not detect supported format for {}",
                path.display()
            )
        })?;
        return Ok(vec![PlannedImport::with_bytes(
            path.to_path_buf(),
            detected,
            bytes,
        )]);
    }

    Err(anyhow::anyhow!(
        "import path {} does not exist",
        path.display()
    ))
}

fn plan_conversation_import(
    mode: ImportMode,
    path: &Path,
) -> Result<Vec<PlannedImport>, anyhow::Error> {
    if path.is_dir() {
        let default_name = match mode {
            ImportMode::Chatgpt => "conversations.json",
            _ => {
                return Err(anyhow::anyhow!(
                    "{} import requires a file path",
                    mode.label()
                ))
            }
        };
        let nested = path.join(default_name);
        if nested.is_file() {
            return plan_conversation_import(mode, &nested);
        }
        return Err(anyhow::anyhow!(
            "{} import requires a file path",
            mode.label()
        ));
    }

    let payload_bytes =
        fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;

    Ok(vec![PlannedImport::with_bytes(
        path.to_path_buf(),
        mode,
        payload_bytes,
    )])
}

fn plan_document_imports(
    mode: ImportMode,
    path: &Path,
    extensions: &[&str],
) -> Result<Vec<PlannedImport>, anyhow::Error> {
    if path.is_dir() {
        let files = collect_files_with_extensions(path, extensions)?;
        return Ok(files
            .into_iter()
            .map(|file_path| {
                PlannedImport::with_bytes(
                    file_path.clone(),
                    mode,
                    fs::read(&file_path)
                        .unwrap_or_else(|_| unreachable!("collected file should remain readable")),
                )
            })
            .collect());
    }

    if !path.is_file() {
        return Err(anyhow::anyhow!(
            "import path {} does not exist",
            path.display()
        ));
    }

    Ok(vec![PlannedImport::with_bytes(
        path.to_path_buf(),
        mode,
        fs::read(path).with_context(|| format!("failed to read {}", path.display()))?,
    )])
}

fn plan_obsidian_import(path: &Path) -> Result<Vec<PlannedImport>, anyhow::Error> {
    if path.is_dir() {
        // Use directory-based import with manifest storage (fidelity-preserving)
        return Ok(vec![PlannedImport::directory(
            path.to_path_buf(),
            ImportMode::Obsidian,
        )]);
    }

    if path.is_file() {
        // For zip files, use the existing byte-based import
        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        return Ok(vec![PlannedImport::with_bytes(
            path.to_path_buf(),
            ImportMode::Obsidian,
            bytes,
        )]);
    }

    Err(anyhow::anyhow!(
        "import path {} does not exist",
        path.display()
    ))
}

fn detect_conversation_mode(path: &Path, payload_bytes: &[u8]) -> Option<ImportMode> {
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case("conversations.json"))
        && parser::chatgpt::parse_conversations(payload_bytes).is_ok()
    {
        return Some(ImportMode::Chatgpt);
    }
    for mode in [
        ImportMode::Chatgpt,
        ImportMode::Claude,
        ImportMode::Grok,
        ImportMode::Gemini,
    ] {
        let parsed = match mode {
            ImportMode::Chatgpt => parser::chatgpt::parse_conversations(payload_bytes).is_ok(),
            ImportMode::Claude => parser::claude::parse_conversations(payload_bytes).is_ok(),
            ImportMode::Grok => parser::grok::parse_conversations(payload_bytes).is_ok(),
            ImportMode::Gemini => parser::gemini::parse_conversations(payload_bytes).is_ok(),
            _ => false,
        };
        if parsed {
            return Some(mode);
        }
    }
    None
}

fn looks_like_obsidian_vault(path: &Path) -> bool {
    path.join(".obsidian").is_dir()
}

fn collect_files_with_extensions(
    root: &Path,
    extensions: &[&str],
) -> Result<Vec<PathBuf>, anyhow::Error> {
    let mut files = Vec::new();
    collect_files_recursive(root, extensions, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files_recursive(
    root: &Path,
    extensions: &[&str],
    files: &mut Vec<PathBuf>,
) -> Result<(), anyhow::Error> {
    let mut entries = fs::read_dir(root)
        .with_context(|| format!("failed to read directory {}", root.display()))?
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("failed to read directory {}", root.display()))?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == ".git" || name == ".obsidian")
            {
                continue;
            }
            collect_files_recursive(&path, extensions, files)?;
            continue;
        }
        if is_extension(&path, extensions) {
            files.push(path);
        }
    }
    Ok(())
}

fn is_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extensions
                .iter()
                .any(|expected| extension.eq_ignore_ascii_case(expected))
        })
}

fn relational_store_label(config: &AppConfig) -> &'static str {
    match config.relational_store {
        RelationalStoreConfig::Sqlite(_) => "sqlite",
        RelationalStoreConfig::Postgres(_) => "postgres",
        RelationalStoreConfig::Oracle(_) => "oracle",
    }
}

fn vector_store_label(config: &AppConfig) -> String {
    match &config.vector_store {
        VectorStoreConfig::Disabled => "disabled".to_string(),
        VectorStoreConfig::PostgresPgVector => "postgres_pgvector".to_string(),
        VectorStoreConfig::Qdrant(qdrant) => format!("qdrant {}", qdrant.url),
    }
}

fn object_store_label(config: &AppConfig) -> String {
    match &config.object_store {
        ObjectStoreConfig::LocalFs(local_fs) => format!("local_fs {}", local_fs.root.display()),
        ObjectStoreConfig::S3Compatible(s3) => format!("s3 {} {}", s3.endpoint, s3.bucket),
    }
}

fn format_source_counts(counts: &[open_archive::storage::ArtifactSourceCount]) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|count| format!("{}={}", count.source_type.as_str(), count.count))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_enrichment_counts(counts: &[open_archive::storage::ArtifactEnrichmentCount]) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|count| format!("{}={}", count.enrichment_status.as_str(), count.count))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_job_counts(counts: &[open_archive::storage::EnrichmentJobCount]) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .iter()
        .map(|count| format!("{}={}", count.job_status.as_str(), count.count))
        .collect::<Vec<_>>()
        .join(",")
}

fn serve() -> Result<(), anyhow::Error> {
    env_logger::init();

    let mut app_config =
        AppConfig::from_env().context("failed to load application configuration")?;
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
