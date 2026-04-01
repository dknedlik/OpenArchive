#![deny(warnings)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use open_archive::config::PostgresConfig;
use open_archive::parser::document::{parse_document, DocumentFormat};
use open_archive::parser::obsidian::parse_frontmatter;
use open_archive::processor::{classify_artifact_debug, ArtifactArchetype, ArtifactProcessorInput};
use open_archive::storage::types::{
    ArtifactClass, ArtifactListFilters, ImportedNoteAliasRecord, ImportedNoteMetadata,
    ImportedNotePropertyRecord, ImportedNoteTagRecord, LoadedSegment, SourceType,
};
use open_archive::storage::{ArtifactReadStore, PostgresArtifactReadStore};
use open_archive::VisibilityStatus;
use postgres::NoTls;

#[derive(Debug, Parser)]
#[command(name = "probe_artifact_classifier")]
#[command(about = "Classify imported artifacts with the deterministic archetype router")]
struct Args {
    #[arg(long = "artifact-id")]
    artifact_ids: Vec<String>,

    #[arg(long = "import-id")]
    import_ids: Vec<String>,

    #[arg(long = "source-type", value_enum)]
    source_type: Option<ProbeSourceType>,

    #[arg(long, default_value_t = 25)]
    limit: usize,

    #[arg(long = "path")]
    paths: Vec<PathBuf>,

    #[arg(long = "dir")]
    dirs: Vec<PathBuf>,

    #[arg(long, default_value_t = false)]
    show_reasons: bool,

    #[arg(long, default_value_t = false)]
    show_scores: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProbeSourceType {
    Chatgpt,
    Claude,
    Grok,
    Gemini,
    Obsidian,
    Markdown,
    Text,
}

impl ProbeSourceType {
    fn to_source_type(self) -> SourceType {
        match self {
            Self::Chatgpt => SourceType::ChatGptExport,
            Self::Claude => SourceType::ClaudeExport,
            Self::Grok => SourceType::GrokExport,
            Self::Gemini => SourceType::GeminiTakeout,
            Self::Obsidian => SourceType::ObsidianVault,
            Self::Markdown => SourceType::MarkdownFile,
            Self::Text => SourceType::TextFile,
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    println!("Artifact classifier probe");
    if !args.paths.is_empty() || !args.dirs.is_empty() {
        let inputs = load_inputs_from_paths(&args)?;
        return run_probe(inputs, &args);
    }

    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let read_store = PostgresArtifactReadStore::new(postgres.clone());

    let artifact_ids = if args.artifact_ids.is_empty() {
        select_artifact_ids(&postgres, &read_store, &args)?
    } else {
        args.artifact_ids.clone()
    };
    if artifact_ids.is_empty() {
        return Err(anyhow!("no artifacts selected"));
    }

    let mut inputs = Vec::with_capacity(artifact_ids.len());
    for artifact_id in artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(&artifact_id)
            .with_context(|| format!("failed to load artifact {}", artifact_id))?
            .ok_or_else(|| anyhow!("artifact {} not found", artifact_id))?;
        inputs.push(ArtifactProcessorInput {
            artifact_id: loaded.artifact.artifact_id.clone(),
            import_id: loaded.artifact.import_id.clone(),
            artifact_class: loaded.artifact.artifact_class,
            source_type: loaded.artifact.source_type,
            title: loaded.artifact.title.clone(),
            imported_note_metadata: Some(loaded.imported_note_metadata),
            participants: loaded.participants,
            segments: loaded.segments,
        });
    }

    run_probe(inputs, &args)
}

fn run_probe(inputs: Vec<ArtifactProcessorInput>, args: &Args) -> Result<()> {
    if inputs.is_empty() {
        return Err(anyhow!("no artifacts selected"));
    }

    println!("Artifacts: {}", inputs.len());
    println!();

    let mut archetype_counts = BTreeMap::<String, usize>::new();
    let mut facet_counts = BTreeMap::<String, usize>::new();
    let mut confidence_sum = 0.0f32;

    for input in inputs {
        let debug = classify_artifact_debug(&input);
        let profile = &debug.profile;
        *archetype_counts
            .entry(profile.primary_archetype.as_str().to_string())
            .or_default() += 1;
        for facet in &profile.facets {
            *facet_counts.entry(facet.as_str().to_string()).or_default() += 1;
        }
        confidence_sum += profile.confidence;

        println!(
            "Artifact: {} | {} | {}",
            input.artifact_id,
            input.source_type.as_str(),
            input.title.as_deref().unwrap_or("(untitled)")
        );
        println!(
            "  archetype: {} ({:.2})",
            profile.primary_archetype.as_str(),
            profile.confidence
        );
        println!(
            "  facets: {}",
            if profile.facets.is_empty() {
                "(none)".to_string()
            } else {
                profile
                    .facets
                    .iter()
                    .map(|facet| facet.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
        if args.show_reasons && !profile.reasons.is_empty() {
            println!("  reasons: {}", profile.reasons.join("; "));
        }
        if args.show_scores {
            let scores = debug
                .archetype_scores
                .iter()
                .filter(|score| score.score > 0 || score.archetype == ArtifactArchetype::Unknown)
                .map(|score| format!("{}={}", score.archetype.as_str(), score.score))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  scores: {}", scores);
        }
        println!();
    }

    println!("Summary");
    println!(
        "Average confidence: {:.2}",
        confidence_sum / archetype_counts.values().sum::<usize>() as f32
    );
    println!("Archetypes:");
    for (archetype, count) in archetype_counts {
        println!("  {}: {}", archetype, count);
    }
    println!("Facets:");
    for (facet, count) in facet_counts {
        println!("  {}: {}", facet, count);
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
    let mut skipped = 0usize;
    for path in files {
        match build_input_from_file(path.clone()) {
            Ok(input) => inputs.push(input),
            Err(err) => {
                skipped += 1;
                eprintln!("Skipping {}: {err:#}", path.display());
            }
        }
    }
    if skipped > 0 {
        eprintln!("Skipped {} file(s) during probe setup", skipped);
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

fn select_artifact_ids(
    postgres: &PostgresConfig,
    read_store: &PostgresArtifactReadStore,
    args: &Args,
) -> Result<Vec<String>> {
    if args.import_ids.is_empty() {
        let source_type = args.source_type.map(ProbeSourceType::to_source_type);
        let filters = ArtifactListFilters {
            source_type,
            enrichment_status: None,
            tag: None,
            alias: None,
            path_prefix: None,
            captured_after: None,
            captured_before: None,
        };
        let artifacts = read_store
            .list_artifacts_filtered(&filters, args.limit, 0)
            .context("failed to list artifacts")?;
        return Ok(artifacts
            .into_iter()
            .map(|artifact| artifact.artifact_id)
            .collect());
    }

    let mut client = postgres::Client::connect(&postgres.connection_string, NoTls)
        .context("failed to connect to Postgres")?;
    let mut artifact_ids = Vec::new();
    let source_type = args
        .source_type
        .map(|value| value.to_source_type().as_str().to_string());
    for import_id in &args.import_ids {
        let rows = client
            .query(
                "select artifact_id
                 from oa_artifact
                 where import_id = $1
                   and ($2::text is null or source_type = $2)
                 order by captured_at desc
                 limit $3",
                &[import_id, &source_type, &(args.limit as i64)],
            )
            .with_context(|| format!("failed to list artifacts for import {}", import_id))?;
        artifact_ids.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
    }
    artifact_ids.truncate(args.limit);
    Ok(artifact_ids)
}
