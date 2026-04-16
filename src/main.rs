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
    Artifacts(ArtifactsArgs),
    /// Load full detail for one artifact
    Artifact(ArtifactArgs),
    /// Search the archive
    Search(SearchArgs),
    /// View artifacts ordered chronologically
    Timeline(TimelineArgs),
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

#[derive(Args)]
struct ArtifactsArgs {
    /// Filter by source type (chatgpt_export, claude_export, grok_export, gemini_takeout, text_file, markdown_file, obsidian_vault)
    #[arg(long, value_name = "TYPE")]
    source: Option<String>,

    /// Filter by enrichment status (pending, running, completed, partial, failed)
    #[arg(long, value_name = "STATUS")]
    status: Option<String>,

    /// Maximum number of results (default: 20, max: 100)
    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    /// Pagination offset (default: 0)
    #[arg(long, value_name = "N")]
    offset: Option<usize>,
}

#[derive(Args)]
struct ArtifactArgs {
    /// Artifact ID to load
    id: String,

    /// Include segment content in output
    #[arg(long)]
    segments: bool,

    /// Segment pagination offset (requires --segments)
    #[arg(long, value_name = "N")]
    segment_offset: Option<usize>,

    /// Maximum segments to return (requires --segments, default: 50, max: 50)
    #[arg(long, value_name = "N")]
    segment_limit: Option<usize>,
}

#[derive(Args)]
struct SearchArgs {
    /// Search query text
    query: String,

    /// Maximum number of results (default: 20)
    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    /// Filter by source type (chatgpt_export, claude_export, grok_export, gemini_takeout, text_file, markdown_file, obsidian_vault)
    #[arg(long, value_name = "TYPE")]
    source: Option<String>,

    /// Filter by object type (summary, memory, entity, relationship, classification)
    #[arg(long, value_name = "TYPE")]
    object_type: Option<String>,
}

#[derive(Args)]
struct TimelineArgs {
    /// Filter by keyword in title
    #[arg(long, value_name = "TEXT")]
    keyword: Option<String>,

    /// Filter by source type (chatgpt_export, claude_export, grok_export, gemini_takeout, text_file, markdown_file, obsidian_vault)
    #[arg(long, value_name = "TYPE")]
    source: Option<String>,

    /// Filter by imported note tag (exact normalized match)
    #[arg(long, value_name = "TAG")]
    tag: Option<String>,

    /// Filter by imported note path prefix
    #[arg(long, value_name = "PREFIX")]
    path_prefix: Option<String>,

    /// Maximum number of results (default: 50, max: 100)
    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    /// Pagination offset (default: 0)
    #[arg(long, value_name = "N")]
    offset: Option<usize>,
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
        Command::Artifacts(args) => artifacts_command(args),
        Command::Artifact(args) => artifact_command(args),
        Command::Search(args) => search_command(args),
        Command::Timeline(args) => timeline_command(args),
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

/// Result of attempting to import a single item.
#[derive(Debug)]
struct ImportItemResult {
    path: PathBuf,
    outcome: Result<ImportResponse, anyhow::Error>,
}

/// Summarize import results and produce an error if any failed.
fn summarize_import_results(
    results: Vec<ImportItemResult>,
) -> (String, usize, Option<anyhow::Error>) {
    let success_count = results.iter().filter(|r| r.outcome.is_ok()).count();
    let failure_count = results.len() - success_count;
    let artifact_count: usize = results
        .iter()
        .filter_map(|r| r.outcome.as_ref().ok())
        .map(|resp| resp.artifacts.len())
        .sum();

    let summary = format!(
        "summary imports={} failed={} artifacts={}",
        success_count, failure_count, artifact_count
    );

    let error = if failure_count > 0 {
        let failures: Vec<_> = results
            .iter()
            .filter(|r| r.outcome.is_err())
            .map(|r| {
                let err = r.outcome.as_ref().unwrap_err();
                format!("{}: {:#}", r.path.display(), err)
            })
            .collect();
        Some(anyhow::anyhow!(
            "{} import(s) failed:\n  {}",
            failure_count,
            failures.join("\n  ")
        ))
    } else {
        None
    };

    (summary, artifact_count, error)
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

    // Guard against empty plan — no importable files found
    if planned.is_empty() {
        return Err(anyhow::anyhow!(
            "no importable files found at {}",
            path.display()
        ));
    }

    let runtime = load_local_runtime(true)?;

    // Collect per-file results instead of failing fast on first error
    let mut results: Vec<ImportItemResult> = Vec::with_capacity(planned.len());
    for item in planned {
        let outcome = execute_import(runtime.services.app.as_ref(), &item);
        // Report each result as it completes
        match &outcome {
            Ok(response) => println!(
                "imported path={} mode={} import_id={} status={} artifacts={}",
                item.import_path.display(),
                item.mode.label(),
                response.import_id,
                response.import_status,
                response.artifacts.len(),
            ),
            Err(err) => eprintln!(
                "failed path={} mode={} error={:#}",
                item.import_path.display(),
                item.mode.label(),
                err
            ),
        }
        results.push(ImportItemResult {
            path: item.import_path,
            outcome,
        });
    }

    // Compute summary and check for failures
    let (summary, _artifact_count, error) = summarize_import_results(results);
    println!("{}", summary);

    if let Some(err) = error {
        return Err(err);
    }

    Ok(())
}

fn artifacts_command(args: ArtifactsArgs) -> Result<(), anyhow::Error> {
    let runtime = load_local_runtime(true)?;

    let source_type = match args.source {
        Some(value) => Some(
            open_archive::storage::SourceType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid source_type: '{value}'"))?,
        ),
        None => None,
    };
    let enrichment_status = match args.status {
        Some(value) => Some(
            open_archive::storage::EnrichmentStatus::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid enrichment_status: '{value}'"))?,
        ),
        None => None,
    };

    let filters = open_archive::storage::ArtifactListFilters {
        source_type,
        enrichment_status,
        ..Default::default()
    };

    let limit = args.limit.unwrap_or(20).clamp(1, 100);
    let offset = args.offset.unwrap_or(0);

    let artifacts = match runtime
        .services
        .app
        .artifacts
        .list_artifacts_filtered(&filters, limit, offset)
    {
        Ok(items) => items,
        Err(open_archive::StorageError::UnsupportedOperation { .. }) => {
            if source_type.is_some() || enrichment_status.is_some() {
                return Err(anyhow::anyhow!(
                    "filtered listing is not supported by the current storage provider"
                ));
            }
            runtime
                .services
                .app
                .artifacts
                .list_artifacts()
                .context("failed to list artifacts")?
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>()
        }
        Err(err) => return Err(anyhow::Error::new(err).context("failed to list artifacts")),
    };

    if artifacts.is_empty() {
        println!("no artifacts found");
        return Ok(());
    }

    for artifact in artifacts {
        let display_title = artifact
            .title
            .or(artifact.note_path)
            .unwrap_or_else(|| "(untitled)".to_string());
        println!(
            "{} {} {} {}",
            artifact.artifact_id,
            artifact.source_type,
            artifact.enrichment_status.as_str(),
            display_title
        );
    }

    Ok(())
}

fn search_command(args: SearchArgs) -> Result<(), anyhow::Error> {
    let runtime = load_local_runtime(true)?;

    let service = runtime
        .services
        .app
        .require_search()
        .map_err(|err| anyhow::Error::new(err).context("search is unavailable"))?;

    let source_type = match args.source {
        Some(value) => Some(
            open_archive::storage::SourceType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid source_type: '{value}'"))?,
        ),
        None => None,
    };

    let object_type = match args.object_type {
        Some(value) => Some(
            open_archive::storage::DerivedObjectType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid object_type: '{value}'"))?,
        ),
        None => None,
    };

    let limit = args.limit.unwrap_or(20).clamp(1, 100);

    let filters = open_archive::storage::SearchFilters {
        object_type,
        source_type,
        tag: None,
        alias: None,
        path_prefix: None,
    };

    let response = service
        .search(open_archive::app::search::ArchiveSearchRequest {
            query_text: args.query,
            limit,
            filters,
        })
        .map_err(|err| anyhow::Error::new(err).context("search failed"))?;

    if response.hits.is_empty() {
        println!("no results");
        return Ok(());
    }

    const SNIPPET_TRUNCATE_LEN: usize = 120;
    for hit in response.hits {
        let match_kind_str = format_match_kind(&hit.match_kind);
        let snippet = if hit.snippet.chars().count() > SNIPPET_TRUNCATE_LEN {
            let cutoff = hit
                .snippet
                .char_indices()
                .nth(SNIPPET_TRUNCATE_LEN)
                .map(|(idx, _)| idx)
                .unwrap_or_else(|| hit.snippet.len());
            format!("{}…", &hit.snippet[..cutoff])
        } else {
            hit.snippet
        };
        // Score formatted to 2 decimal places as per requirements
        println!("{:.2} {} {}", hit.score, match_kind_str, hit.artifact_id);
        println!("  {}", snippet);
    }

    Ok(())
}

fn format_match_kind(kind: &open_archive::app::search::SearchMatchKind) -> String {
    use open_archive::app::search::SearchMatchKind;
    match kind {
        SearchMatchKind::ArtifactTitle => "title".to_string(),
        SearchMatchKind::ImportedNoteTag { .. } => "tag".to_string(),
        SearchMatchKind::ImportedNoteAlias { .. } => "alias".to_string(),
        SearchMatchKind::ImportedNotePath => "path".to_string(),
        SearchMatchKind::ImportedExternalLink { .. } => "link".to_string(),
        SearchMatchKind::DerivedObject { derived_type, .. } => derived_type.as_str().to_string(),
        SearchMatchKind::SegmentExcerpt { .. } => "segment".to_string(),
    }
}

fn timeline_command(args: TimelineArgs) -> Result<(), anyhow::Error> {
    let runtime = load_local_runtime(true)?;

    let source_type = match args.source {
        Some(value) => Some(
            open_archive::storage::SourceType::parse(&value)
                .ok_or_else(|| anyhow::anyhow!("invalid source_type: '{value}'"))?,
        ),
        None => None,
    };

    let limit = args.limit.unwrap_or(50).clamp(1, 100);
    let offset = args.offset.unwrap_or(0);

    let filters = open_archive::storage::TimelineFilters {
        keyword: args.keyword,
        source_type,
        tag: args.tag.map(|t| t.to_lowercase()),
        path_prefix: args.path_prefix.map(|p| p.to_lowercase()),
    };

    let entries = runtime
        .services
        .app
        .artifacts
        .get_timeline(&filters, limit, offset)
        .map_err(|err| anyhow::Error::new(err).context("failed to load timeline"))?;

    if entries.is_empty() {
        println!("no entries");
        return Ok(());
    }

    const SUMMARY_TRUNCATE_LEN: usize = 120;
    for entry in entries {
        // Format captured_at as date-only (extract YYYY-MM-DD from ISO 8601)
        let captured_at_date = entry
            .captured_at
            .split('T')
            .next()
            .unwrap_or(&entry.captured_at);
        let display_title = entry
            .title
            .or(entry.note_path)
            .unwrap_or_else(|| "(untitled)".to_string());
        println!(
            "{} {} {} {}",
            captured_at_date, entry.source_type, entry.artifact_id, display_title
        );

        // Summary snippet on second line only when present
        if let Some(summary) = entry.summary_snippet {
            let truncated = if summary.chars().count() > SUMMARY_TRUNCATE_LEN {
                let cutoff = summary
                    .char_indices()
                    .nth(SUMMARY_TRUNCATE_LEN)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| summary.len());
                format!("{}…", &summary[..cutoff])
            } else {
                summary
            };
            println!("  {}", truncated);
        }
    }

    Ok(())
}

fn artifact_command(args: ArtifactArgs) -> Result<(), anyhow::Error> {
    let runtime = load_local_runtime(true)?;

    let service = runtime
        .services
        .app
        .require_artifact_detail()
        .map_err(|err| anyhow::Error::new(err).context("artifact detail is unavailable"))?;

    // Warn if offset/limit provided without --segments
    if (args.segment_offset.is_some() || args.segment_limit.is_some()) && !args.segments {
        eprintln!(
            "warning: --segment-offset and --segment-limit have no effect without --segments"
        );
    }

    let include_segments = args.segments;
    let segment_offset = args.segment_offset.unwrap_or(0);
    let segment_limit = args
        .segment_limit
        .unwrap_or(open_archive::app::artifact_detail::DEFAULT_SEGMENT_LIMIT)
        .clamp(1, open_archive::app::artifact_detail::DEFAULT_SEGMENT_LIMIT);

    let artifact_id = args.id;
    let response = service
        .get(open_archive::app::artifact_detail::ArtifactDetailRequest {
            artifact_id: artifact_id.clone(),
            include_segments,
            segment_offset,
            segment_limit,
        })
        .map_err(|err| anyhow::Error::new(err).context("failed to load artifact detail"))?;

    let Some(detail) = response else {
        return Err(anyhow::anyhow!("artifact not found: {}", artifact_id));
    };

    // Header line: id source status title
    let display_title = detail
        .title
        .or_else(|| detail.note_path.clone())
        .unwrap_or_else(|| "(untitled)".to_string());
    println!(
        "{} {} {} {}",
        detail.artifact_id,
        detail.source_type.as_str(),
        detail.enrichment_status.as_str(),
        display_title
    );

    // Derived objects: one line each — object_id type title
    for obj in &detail.derived_objects {
        let obj_title = obj
            .title
            .clone()
            .unwrap_or_else(|| "(untitled)".to_string());
        println!(
            "{} {} {}",
            obj.derived_object_id,
            obj.derived_object_type.as_str(),
            obj_title
        );
    }

    // Segments (if --segments): one line each — [seq] role: text_content (truncated)
    if include_segments {
        const SEGMENT_TRUNCATE_LEN: usize = 120;
        for seg in &detail.segments {
            let role_str = seg
                .participant_role
                .as_ref()
                .map(|r| r.as_str())
                .unwrap_or("unknown");
            let content = &seg.text_content;
            let truncated = if content.chars().count() > SEGMENT_TRUNCATE_LEN {
                let cutoff = content
                    .char_indices()
                    .nth(SEGMENT_TRUNCATE_LEN)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| content.len());
                format!("{}…", &content[..cutoff])
            } else {
                content.clone()
            };
            println!("[{}] {}: {}", seg.sequence_no, role_str, truncated);
        }
    }

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
            // Check for ChatGPT export first (specific: requires conversations.json)
            // before Obsidian (generic: any zip with markdown files), so that
            // ChatGPT exports containing README.md/notes are routed correctly.
            if open_archive::looks_like_chatgpt_zip(&bytes) {
                return Ok(vec![PlannedImport::with_bytes(
                    path.to_path_buf(),
                    ImportMode::Chatgpt,
                    bytes,
                )]);
            }
            if parser::obsidian::parse_vault_zip(&bytes).is_ok() {
                return Ok(vec![PlannedImport::with_bytes(
                    path.to_path_buf(),
                    ImportMode::Obsidian,
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

    // Tests for summarize_import_results

    fn make_result(path: &str, outcome: Result<ImportResponse, anyhow::Error>) -> ImportItemResult {
        ImportItemResult {
            path: PathBuf::from(path),
            outcome,
        }
    }

    fn make_success_response(artifacts: usize) -> ImportResponse {
        ImportResponse {
            import_id: "imp_test".to_string(),
            import_status: "completed".to_string(),
            artifacts: (0..artifacts)
                .map(|i| open_archive::import_service::ImportArtifactStatus {
                    artifact_id: format!("art_{}", i),
                    enrichment_status: "pending".to_string(),
                    ingest_result: "created".to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn summarize_all_success() {
        let results = vec![
            make_result("/a.md", Ok(make_success_response(2))),
            make_result("/b.md", Ok(make_success_response(3))),
        ];
        let (summary, artifact_count, error) = summarize_import_results(results);
        assert_eq!(summary, "summary imports=2 failed=0 artifacts=5");
        assert_eq!(artifact_count, 5);
        assert!(error.is_none());
    }

    #[test]
    fn summarize_all_failure() {
        let results = vec![
            make_result("/a.md", Err(anyhow::anyhow!("parse error"))),
            make_result("/b.md", Err(anyhow::anyhow!("io error"))),
        ];
        let (summary, artifact_count, error) = summarize_import_results(results);
        assert_eq!(summary, "summary imports=0 failed=2 artifacts=0");
        assert_eq!(artifact_count, 0);
        assert!(error.is_some());
        let err_str = format!("{:#}", error.unwrap());
        assert!(err_str.contains("2 import(s) failed"));
        assert!(err_str.contains("/a.md: parse error"));
        assert!(err_str.contains("/b.md: io error"));
    }

    #[test]
    fn summarize_mixed_success_and_failure() {
        let results = vec![
            make_result("/a.md", Ok(make_success_response(1))),
            make_result("/b.md", Err(anyhow::anyhow!("invalid syntax"))),
            make_result("/c.md", Ok(make_success_response(2))),
        ];
        let (summary, artifact_count, error) = summarize_import_results(results);
        assert_eq!(summary, "summary imports=2 failed=1 artifacts=3");
        assert_eq!(artifact_count, 3);
        assert!(error.is_some());
        let err_str = format!("{:#}", error.unwrap());
        assert!(err_str.contains("1 import(s) failed"));
        assert!(err_str.contains("/b.md: invalid syntax"));
        // Ensure successful paths are NOT in the error
        assert!(!err_str.contains("/a.md"));
        assert!(!err_str.contains("/c.md"));
    }

    #[test]
    fn summarize_empty_results() {
        let results: Vec<ImportItemResult> = vec![];
        let (summary, artifact_count, error) = summarize_import_results(results);
        assert_eq!(summary, "summary imports=0 failed=0 artifacts=0");
        assert_eq!(artifact_count, 0);
        assert!(error.is_none());
    }

    #[test]
    fn summarize_preserves_error_chain() {
        let inner_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let outer_err = anyhow::Error::new(inner_err).context("while importing document");
        let results = vec![make_result("/missing.md", Err(outer_err))];
        let (_summary, _artifact_count, error) = summarize_import_results(results);
        assert!(error.is_some());
        let err_str = format!("{:#}", error.unwrap());
        // Full chain should include both context and source
        assert!(err_str.contains("while importing document"));
        assert!(err_str.contains("file not found"));
    }
}
