use anyhow::Context;
use clap::{Args, Subcommand};
use open_archive::app::ArchiveApplication;
use open_archive::import_service::ImportResponse;
use open_archive::parser;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Args)]
pub struct ImportArgs {
    #[command(subcommand)]
    pub source: ImportSourceCommand,
}

#[derive(Subcommand)]
pub enum ImportSourceCommand {
    Auto { path: PathBuf },
    Chatgpt { path: PathBuf },
    Claude { path: PathBuf },
    Grok { path: PathBuf },
    Gemini { path: PathBuf },
    Markdown { path: PathBuf },
    Text { path: PathBuf },
    Obsidian { path: PathBuf },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImportMode {
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
    pub(crate) fn label(self) -> &'static str {
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

pub(crate) struct PlannedImport {
    pub import_path: PathBuf,
    pub mode: ImportMode,
    pub payload_bytes: Option<Vec<u8>>,
}

impl PlannedImport {
    pub(crate) fn with_bytes(import_path: PathBuf, mode: ImportMode, bytes: Vec<u8>) -> Self {
        Self {
            import_path,
            mode,
            payload_bytes: Some(bytes),
        }
    }

    pub(crate) fn directory(import_path: PathBuf, mode: ImportMode) -> Self {
        Self {
            import_path,
            mode,
            payload_bytes: None,
        }
    }
}

/// Result of attempting to import a single item.
#[derive(Debug)]
pub(crate) struct ImportItemResult {
    pub path: PathBuf,
    pub outcome: Result<ImportResponse, anyhow::Error>,
}

/// Summarize import results and produce an error if any failed.
pub(crate) fn summarize_import_results(
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

pub(crate) fn import_command(args: ImportArgs) -> anyhow::Result<()> {
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

    let runtime = crate::cli::load_local_runtime(true)?;

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

#[cfg(test)]
mod tests {
    use super::*;

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
