#![deny(warnings)]

use std::error::Error as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::{
    AnthropicConfig, GeminiConfig, GrokConfig, OpenAiConfig, PostgresConfig,
};
use open_archive::parser::document::{parse_document, DocumentFormat};
use open_archive::parser::obsidian::parse_frontmatter;
use open_archive::processor::{
    AnthropicProcessorFactory, ArtifactProcessorFactory, ArtifactProcessorInput,
    GeminiProcessorFactory, GrokProcessorFactory, OpenAiProcessorFactory,
};
use open_archive::storage::types::EnrichmentTier;
use open_archive::storage::types::{
    ArtifactClass, ImportedNoteAliasRecord, ImportedNoteMetadata, ImportedNotePropertyRecord,
    ImportedNoteTagRecord, LoadedSegment, SourceType,
};
use open_archive::storage::{ArtifactReadStore, PostgresArtifactReadStore};
use open_archive::VisibilityStatus;

#[derive(Debug, Parser)]
#[command(name = "probe_output_budget")]
#[command(
    about = "Replay imported artifacts or raw markdown/text files against real extraction budgets"
)]
struct Args {
    /// Artifact ids to probe.
    #[arg()]
    artifact_ids: Vec<String>,

    /// Files to probe directly through the production parser path.
    #[arg(long = "path")]
    paths: Vec<PathBuf>,

    /// Directories to scan for markdown/text files.
    #[arg(long = "dir")]
    dirs: Vec<PathBuf>,

    /// Maximum number of files to probe when using --dir.
    #[arg(long, default_value_t = 25)]
    limit: usize,

    /// Inference provider to use for the probe.
    #[arg(long = "provider", default_value = "openai")]
    provider: String,

    /// Output token budgets to compare.
    #[arg(long = "budget", required = true)]
    budgets: Vec<u32>,

    /// Override the model for the probe. Defaults to the selected provider's standard model.
    #[arg(long = "model")]
    model: Option<String>,

    /// Print the returned summary, classifications, and memories for inspection.
    #[arg(long = "show-output", default_value_t = false)]
    show_output: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let inputs = if !args.paths.is_empty() || !args.dirs.is_empty() {
        load_inputs_from_paths(&args)?
    } else {
        if args.artifact_ids.is_empty() {
            return Err(anyhow!(
                "provide at least one artifact id or use --path/--dir"
            ));
        }
        let postgres =
            PostgresConfig::from_env().context("failed to load Postgres config from env")?;
        let read_store = PostgresArtifactReadStore::new(postgres);
        let mut inputs = Vec::with_capacity(args.artifact_ids.len());
        for artifact_id in &args.artifact_ids {
            let loaded = read_store
                .load_artifact_for_enrichment(artifact_id)
                .with_context(|| format!("failed to load artifact {}", artifact_id))?
                .ok_or_else(|| anyhow!("artifact {} not found", artifact_id))?;
            inputs.push(ArtifactProcessorInput {
                artifact_id: loaded.artifact.artifact_id.clone(),
                import_id: loaded.artifact.import_id.clone(),
                artifact_class: loaded.artifact.artifact_class,
                source_type: loaded.artifact.source_type,
                title: loaded.artifact.title.clone(),
                imported_note_metadata: Some(loaded.imported_note_metadata.clone()),
                participants: loaded.participants,
                segments: loaded.segments,
            });
        }
        inputs
    };
    let provider = ProbeProvider::from_str(&args.provider)?;
    let model = match provider {
        ProbeProvider::OpenRouter => {
            let config = OpenAiConfig::from_env().context(
                "failed to load OpenAI config from env; set OA_OPENAI_API_KEY and related vars",
            )?;
            args_model_openai(&config, &args)?
        }
        ProbeProvider::Gemini => {
            let config = GeminiConfig::from_env().context(
                "failed to load Gemini config from env; set OA_GEMINI_API_KEY and related vars",
            )?;
            args_model_gemini(&config, &args)?
        }
        ProbeProvider::Anthropic => {
            let config = AnthropicConfig::from_env().context(
                "failed to load Anthropic config from env; set OA_ANTHROPIC_API_KEY and related vars",
            )?;
            args_model_anthropic(&config, &args)?
        }
        ProbeProvider::Grok => {
            let config = GrokConfig::from_env().context(
                "failed to load Grok config from env; set OA_GROK_API_KEY and related vars",
            )?;
            args_model_grok(&config, &args)?
        }
    };

    println!("Output budget probe");
    println!("Provider: {}", provider.as_str());
    println!("Model: {}", model);
    println!("Budgets: {:?}", args.budgets);
    println!();

    for input in &inputs {
        println!(
            "Artifact: {} | title: {} | segments: {}",
            input.artifact_id,
            input.title.as_deref().unwrap_or(""),
            input.segments.len()
        );

        for budget in &args.budgets {
            let factory: Box<dyn ArtifactProcessorFactory> = match provider {
                ProbeProvider::OpenRouter => {
                    let mut config = OpenAiConfig::from_env()
                        .context("failed to reload OpenAI config from env")?;
                    config.standard_model = model.clone();
                    config.quality_model = Some(model.clone());
                    config.max_output_tokens = *budget;
                    Box::new(
                        OpenAiProcessorFactory::new(config)
                            .map_err(|err| anyhow!("failed to build OpenAI factory: {err}"))?,
                    )
                }
                ProbeProvider::Gemini => {
                    let mut config = GeminiConfig::from_env()
                        .context("failed to reload Gemini config from env")?;
                    config.standard_model = model.clone();
                    config.quality_model = Some(model.clone());
                    config.max_output_tokens = *budget;
                    Box::new(
                        GeminiProcessorFactory::new(config)
                            .map_err(|err| anyhow!("failed to build Gemini factory: {err}"))?,
                    )
                }
                ProbeProvider::Anthropic => {
                    let mut config = AnthropicConfig::from_env()
                        .context("failed to reload Anthropic config from env")?;
                    config.standard_model = model.clone();
                    config.quality_model = Some(model.clone());
                    config.max_output_tokens = *budget;
                    Box::new(
                        AnthropicProcessorFactory::new(config)
                            .map_err(|err| anyhow!("failed to build Anthropic factory: {err}"))?,
                    )
                }
                ProbeProvider::Grok => {
                    let mut config =
                        GrokConfig::from_env().context("failed to reload Grok config from env")?;
                    config.standard_model = model.clone();
                    config.quality_model = Some(model.clone());
                    config.max_output_tokens = *budget;
                    Box::new(
                        GrokProcessorFactory::new(config)
                            .map_err(|err| anyhow!("failed to build Grok factory: {err}"))?,
                    )
                }
            };
            let processor = factory
                .build(EnrichmentTier::Standard)
                .map_err(|err| anyhow!("failed to build processor: {err}"))?;

            let started = Instant::now();
            match processor.process(input) {
                Ok(output) => {
                    println!(
                        "  budget {:>4}: ok ({:.2}s) | memories {} | classifications {} | importance {}{}",
                        budget,
                        started.elapsed().as_secs_f64(),
                        output.memories.len(),
                        output.classifications.len(),
                        output.importance_score,
                        output
                            .usage
                            .as_ref()
                            .map(|usage| format!(" | {}", format_usage(usage)))
                            .unwrap_or_default()
                    );
                    if args.show_output {
                        print_output_details(&output);
                    }
                }
                Err(err) => {
                    println!(
                        "  budget {:>4}: err ({:.2}s) | {}",
                        budget,
                        started.elapsed().as_secs_f64(),
                        err
                    );
                    let mut source = err.source();
                    let mut depth = 0;
                    while let Some(cause) = source {
                        depth += 1;
                        println!("    caused by[{depth}]: {}", cause);
                        source = cause.source();
                    }
                }
            }
        }
        println!();
    }

    Ok(())
}

fn load_inputs_from_paths(args: &Args) -> Result<Vec<ArtifactProcessorInput>> {
    let mut files = args.paths.clone();
    for dir in &args.dirs {
        collect_supported_files(dir, &mut files)
            .with_context(|| format!("failed to scan {}", dir.display()))?;
    }
    files.sort();
    files.dedup();
    if files.len() > args.limit {
        files.truncate(args.limit);
    }

    let mut inputs = Vec::new();
    for path in files {
        inputs.push(build_input_from_file(path)?);
    }
    Ok(inputs)
}

fn collect_supported_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if supported_document_format(path).is_some() {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        if child.is_dir() {
            collect_supported_files(&child, files)?;
        } else if supported_document_format(&child).is_some() {
            files.push(child);
        }
    }
    Ok(())
}

fn build_input_from_file(path: PathBuf) -> Result<ArtifactProcessorInput> {
    let bytes = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let format = supported_document_format(&path)
        .ok_or_else(|| anyhow!("unsupported file format: {}", path.display()))?;
    let raw_text = std::str::from_utf8(&bytes)
        .with_context(|| format!("{} is not valid UTF-8", path.display()))?;
    let note_path = path.display().to_string();
    let (title_hint, body_text, metadata) = if format == DocumentFormat::Markdown {
        let parsed_frontmatter = parse_frontmatter(&note_path, raw_text)
            .with_context(|| format!("failed to parse frontmatter for {}", path.display()))?;
        (
            parsed_frontmatter.title,
            parsed_frontmatter.body,
            ImportedNoteMetadata {
                note_path: Some(note_path.clone()),
                properties: parsed_frontmatter
                    .properties
                    .into_iter()
                    .map(|property| ImportedNotePropertyRecord {
                        property_key: property.property_key,
                        value_kind: property.value_kind,
                        value_text: property.value_text,
                        value_json: property.value_json,
                    })
                    .collect(),
                tags: parsed_frontmatter
                    .tags
                    .into_iter()
                    .map(|tag| ImportedNoteTagRecord {
                        raw_tag: tag.raw_tag,
                        normalized_tag: tag.normalized_tag,
                        tag_path: tag.tag_path,
                        source_kind: tag.source_kind,
                    })
                    .collect(),
                aliases: parsed_frontmatter
                    .aliases
                    .into_iter()
                    .map(|alias| ImportedNoteAliasRecord {
                        alias_text: alias.alias_text,
                        normalized_alias: alias.normalized_alias,
                    })
                    .collect(),
                outbound_links: Vec::new(),
            },
        )
    } else {
        (
            None,
            raw_text.to_string(),
            ImportedNoteMetadata {
                note_path: Some(note_path.clone()),
                ..ImportedNoteMetadata::default()
            },
        )
    };

    let parsed = parse_document(body_text.as_bytes(), format)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let title = title_hint
        .or_else(|| preferred_markdown_title(&path, parsed.title))
        .or_else(|| {
            path.file_stem()
                .and_then(|value| value.to_str())
                .map(str::to_string)
        });

    Ok(ArtifactProcessorInput {
        artifact_id: path.display().to_string(),
        import_id: "filesystem-probe".to_string(),
        artifact_class: ArtifactClass::Document,
        source_type: match format {
            DocumentFormat::Markdown => SourceType::MarkdownFile,
            DocumentFormat::PlainText => SourceType::TextFile,
        },
        title,
        imported_note_metadata: Some(metadata),
        participants: Vec::new(),
        segments: vec![LoadedSegment {
            segment_id: format!("segment:{}", path.display()),
            participant_id: None,
            participant_role: None,
            sequence_no: 0,
            text_content: body_text,
            created_at_source: None,
            visibility_status: VisibilityStatus::Visible,
        }],
    })
}

fn preferred_markdown_title(path: &Path, parsed_title: Option<String>) -> Option<String> {
    let stem = path.file_stem().and_then(|value| value.to_str())?;
    if stem.eq_ignore_ascii_case("readme") || stem.eq_ignore_ascii_case("index") {
        return parsed_title.or_else(|| Some(stem.to_string()));
    }
    Some(stem.to_string())
}

fn supported_document_format(path: &Path) -> Option<DocumentFormat> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("md") | Some("markdown") => Some(DocumentFormat::Markdown),
        Some("txt") => Some(DocumentFormat::PlainText),
        _ => None,
    }
}

fn print_output_details(output: &open_archive::processor::ArtifactProcessorOutput) {
    println!(
        "    summary: {}",
        output.summary.body_text.replace('\n', " ")
    );
    if output.classifications.is_empty() {
        println!("    classifications: none");
    } else {
        for classification in &output.classifications {
            println!(
                "    classification: {}={} | {}",
                classification.classification_type,
                classification.classification_value,
                classification
                    .body_text
                    .as_deref()
                    .unwrap_or("")
                    .replace('\n', " ")
            );
        }
    }
    if output.memories.is_empty() {
        println!("    memories: none");
    } else {
        for memory in &output.memories {
            println!(
                "    memory: {} | {}",
                memory.memory_type,
                memory.title.as_deref().unwrap_or("")
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProbeProvider {
    OpenRouter,
    Gemini,
    Anthropic,
    Grok,
}

impl ProbeProvider {
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "openai" => Ok(Self::OpenRouter),
            "gemini" => Ok(Self::Gemini),
            "anthropic" => Ok(Self::Anthropic),
            "grok" => Ok(Self::Grok),
            _ => Err(anyhow!(
                "unsupported provider {value}; expected openai, gemini, anthropic, or grok"
            )),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::OpenRouter => "openai",
            Self::Gemini => "gemini",
            Self::Anthropic => "anthropic",
            Self::Grok => "grok",
        }
    }
}

fn args_model_openai(config: &OpenAiConfig, args: &Args) -> Result<String> {
    Ok(args
        .model
        .clone()
        .unwrap_or_else(|| config.standard_model.clone()))
}

fn args_model_gemini(config: &GeminiConfig, args: &Args) -> Result<String> {
    Ok(args
        .model
        .clone()
        .unwrap_or_else(|| config.standard_model.clone()))
}

fn args_model_anthropic(config: &AnthropicConfig, args: &Args) -> Result<String> {
    Ok(args
        .model
        .clone()
        .unwrap_or_else(|| config.standard_model.clone()))
}

fn args_model_grok(config: &GrokConfig, args: &Args) -> Result<String> {
    Ok(args
        .model
        .clone()
        .unwrap_or_else(|| config.standard_model.clone()))
}

fn format_usage(usage: &open_archive::processor::InferenceUsage) -> String {
    let mut parts = Vec::new();
    if let Some(input_tokens) = usage.input_tokens {
        parts.push(format!("in {}", input_tokens));
    }
    if let Some(output_tokens) = usage.output_tokens {
        parts.push(format!("out {}", output_tokens));
    }
    if let Some(reasoning_tokens) = usage.reasoning_tokens {
        parts.push(format!("reasoning {}", reasoning_tokens));
    }
    if let Some(total_tokens) = usage.total_tokens {
        parts.push(format!("total {}", total_tokens));
    }
    if let Some(cost_micros) = usage.reported_cost_micros {
        parts.push(format!("cost ${:.6}", cost_micros as f64 / 1_000_000.0));
    }
    parts.join(", ")
}
