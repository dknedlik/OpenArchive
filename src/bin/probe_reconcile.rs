#![deny(warnings)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::AppConfig;
use open_archive::enrichment_worker::{
    build_reconciliation_input_from_processor_output, inspect_reconciliation_work,
    ReconciliationCandidateDisposition, ReconciliationPreparationReport,
};
use open_archive::processor::{
    ArtifactProcessorInput, ArtifactProcessorOutput, ReconciliationDecisionOutput,
};
use open_archive::storage::types::EnrichmentTier;
use open_archive::storage::{ArtifactReadStore, LoadedArtifactForEnrichment};

#[derive(Debug, Parser)]
#[command(name = "probe_reconcile")]
#[command(
    about = "Replay real artifacts through extraction and reconcile preparation to inspect exact matches, embedding bands, and ambiguity-only model fallbacks"
)]
struct Args {
    /// Artifact ids to probe.
    #[arg()]
    artifact_ids: Vec<String>,

    /// Probe the most recently captured N artifacts when explicit ids are not provided.
    #[arg(long = "recent")]
    recent: Option<usize>,

    /// Write one JSON report per artifact into this directory.
    #[arg(long = "out-dir")]
    out_dir: Option<PathBuf>,

    /// Run the reconciliation model for ambiguous cases after deterministic preparation.
    #[arg(long = "run-ambiguity-model", default_value_t = false)]
    run_ambiguity_model: bool,

    /// Print one line per candidate showing the reconcile disposition.
    #[arg(long = "show-traces", default_value_t = false)]
    show_traces: bool,
}

#[derive(Debug, serde::Serialize)]
struct ArtifactProbeReport {
    artifact_id: String,
    title: Option<String>,
    source_type: String,
    extraction_output: ArtifactProcessorOutput,
    preparation: ReconciliationPreparationReport,
    ambiguity_model_outputs: Option<Vec<ReconciliationDecisionOutput>>,
    final_outputs: Vec<ReconciliationDecisionOutput>,
}

fn main() -> Result<()> {
    load_dotenv();

    let args = Args::parse();
    if args.artifact_ids.is_empty() && args.recent.is_none() {
        return Err(anyhow!(
            "provide at least one artifact id or use --recent to sample recent artifacts"
        ));
    }

    if let Some(out_dir) = args.out_dir.as_ref() {
        fs::create_dir_all(out_dir)
            .with_context(|| format!("failed to create output directory {}", out_dir.display()))?;
    }

    let config = AppConfig::from_env().context("failed to load application configuration")?;
    let services = build_service_bundle(&config)
        .context("failed to construct configured service providers")?;
    let cross_artifact_store = services.cross_artifact_store.as_deref().ok_or_else(|| {
        anyhow!("configured provider does not expose a cross-artifact read store")
    })?;
    let embedding_provider = services
        .embedding_provider
        .as_deref()
        .ok_or_else(|| anyhow!("configured provider does not expose an embedding provider"))?;
    let extraction_processor = services
        .processor_factory
        .build(EnrichmentTier::Default)
        .map_err(|err| anyhow!("failed to build extraction processor: {err}"))?;
    let reconciliation_processor = if args.run_ambiguity_model {
        Some(
            services
                .processor_factory
                .build_reconciliation_processor(EnrichmentTier::Default)
                .map_err(|err| anyhow!("failed to build reconciliation processor: {err}"))?,
        )
    } else {
        None
    };

    let artifact_ids = resolve_artifact_ids(services.read_store.as_ref(), &args)?;
    println!("Reconcile probe");
    println!("Artifacts: {}", artifact_ids.len());
    println!(
        "Ambiguity model: {}",
        if args.run_ambiguity_model {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!();

    for artifact_id in artifact_ids {
        let loaded = services
            .read_store
            .load_artifact_for_enrichment(&artifact_id)
            .with_context(|| format!("failed to load artifact {artifact_id}"))?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        let input = build_processor_input(&loaded);
        let extraction_output = extraction_processor
            .process(&input)
            .map_err(|err| anyhow!("artifact {} extraction failed: {err}", artifact_id))?;
        let reconciliation_input = build_reconciliation_input_from_processor_output(
            &input.artifact_id,
            input.source_type,
            &extraction_output,
        )
        .with_context(|| format!("failed to build reconciliation input for {}", artifact_id))?;
        let preparation = inspect_reconciliation_work(
            &reconciliation_input,
            Some(cross_artifact_store),
            Some(embedding_provider),
        )
        .map_err(|err| {
            anyhow!(
                "failed to prepare reconciliation work for {}: {}",
                artifact_id,
                err
            )
        })?;

        let ambiguity_model_outputs: Option<Vec<ReconciliationDecisionOutput>> = match (
            reconciliation_processor.as_ref(),
            preparation.ambiguous_input.as_ref(),
        ) {
            (Some(processor), Some(ambiguous_input)) => {
                Some(processor.reconcile(ambiguous_input).map_err(|err| {
                    anyhow!("artifact {} ambiguity model failed: {err}", artifact_id)
                })?)
            }
            _ => None,
        };

        let mut final_outputs = preparation.deterministic_outputs.clone();
        if let Some(outputs) = ambiguity_model_outputs.as_ref() {
            final_outputs.extend(outputs.iter().cloned());
        }

        print_artifact_summary(
            &loaded,
            &extraction_output,
            &preparation,
            ambiguity_model_outputs.as_deref(),
            args.show_traces,
        );

        if let Some(out_dir) = args.out_dir.as_ref() {
            let report = ArtifactProbeReport {
                artifact_id: loaded.artifact.artifact_id.clone(),
                title: loaded.artifact.title.clone(),
                source_type: loaded.artifact.source_type.as_str().to_string(),
                extraction_output,
                preparation,
                ambiguity_model_outputs,
                final_outputs,
            };
            write_report(out_dir, &report)?;
        }
    }

    Ok(())
}

fn load_dotenv() {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(project_dir) = exe
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
        {
            let env_path = project_dir.join(".env");
            if env_path.exists() {
                let _ = dotenvy::from_path(&env_path);
            }
        }
    }
    let _ = dotenvy::dotenv();
}

fn resolve_artifact_ids(read_store: &dyn ArtifactReadStore, args: &Args) -> Result<Vec<String>> {
    let mut artifact_ids = Vec::new();
    let mut seen = BTreeSet::new();

    for artifact_id in &args.artifact_ids {
        if seen.insert(artifact_id.clone()) {
            artifact_ids.push(artifact_id.clone());
        }
    }

    if let Some(recent) = args.recent {
        for item in read_store
            .list_artifacts()
            .context("failed to list artifacts for --recent")?
            .into_iter()
            .take(recent)
        {
            if seen.insert(item.artifact_id.clone()) {
                artifact_ids.push(item.artifact_id);
            }
        }
    }

    Ok(artifact_ids)
}

fn build_processor_input(loaded: &LoadedArtifactForEnrichment) -> ArtifactProcessorInput {
    ArtifactProcessorInput {
        artifact_id: loaded.artifact.artifact_id.clone(),
        import_id: loaded.artifact.import_id.clone(),
        artifact_class: loaded.artifact.artifact_class,
        source_type: loaded.artifact.source_type,
        title: loaded.artifact.title.clone(),
        imported_note_metadata: Some(loaded.imported_note_metadata.clone()),
        participants: loaded.participants.clone(),
        segments: loaded.segments.clone(),
    }
}

fn print_artifact_summary(
    loaded: &LoadedArtifactForEnrichment,
    extraction_output: &ArtifactProcessorOutput,
    preparation: &ReconciliationPreparationReport,
    ambiguity_model_outputs: Option<&[ReconciliationDecisionOutput]>,
    show_traces: bool,
) {
    println!(
        "Artifact: {} | {} | {}",
        loaded.artifact.artifact_id,
        loaded.artifact.source_type.as_str(),
        loaded.artifact.title.as_deref().unwrap_or("")
    );
    println!(
        "  extraction: memories {} | entities {} | relationships {}",
        extraction_output.memories.len(),
        extraction_output.entities.len(),
        extraction_output.relationships.len()
    );
    println!(
        "  preparation: {}",
        format_counts(disposition_counts(preparation))
    );
    println!(
        "  deterministic decisions: {}",
        format_counts(decision_counts(&preparation.deterministic_outputs))
    );
    if let Some(outputs) = ambiguity_model_outputs {
        println!(
            "  ambiguity decisions: {}",
            format_counts(decision_counts(outputs))
        );
    }
    if show_traces {
        for trace in &preparation.candidate_traces {
            let top_similarity = trace
                .embedding_matches
                .first()
                .map(|matched| format!("{:.3}", matched.similarity_score))
                .unwrap_or_else(|| "-".to_string());
            let matched_object_id = trace
                .deterministic_output
                .as_ref()
                .and_then(|output| output.matched_object_id.as_deref())
                .unwrap_or("-");
            println!(
                "    {} {} -> {} | exact {} | embed {} | top {} | match {}",
                trace.target_kind,
                trace.target_key,
                disposition_label(trace.disposition),
                trace.exact_matches.len(),
                trace.embedding_matches.len(),
                top_similarity,
                matched_object_id
            );
        }
    }
    println!();
}

fn disposition_counts(preparation: &ReconciliationPreparationReport) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for trace in &preparation.candidate_traces {
        *counts
            .entry(disposition_label(trace.disposition).to_string())
            .or_insert(0) += 1;
    }
    counts
}

fn decision_counts(outputs: &[ReconciliationDecisionOutput]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for output in outputs {
        *counts
            .entry(format!("{:?}", output.decision_kind).to_lowercase())
            .or_insert(0) += 1;
    }
    counts
}

fn disposition_label(disposition: ReconciliationCandidateDisposition) -> &'static str {
    match disposition {
        ReconciliationCandidateDisposition::ExactCandidateKeyMatch => "exact_key",
        ReconciliationCandidateDisposition::FallbackWithoutEmbeddingProvider => {
            "fallback_no_embedding_provider"
        }
        ReconciliationCandidateDisposition::FallbackWithoutCrossArtifactStore => {
            "fallback_no_cross_store"
        }
        ReconciliationCandidateDisposition::NoEmbeddingMatches => "no_embedding_match",
        ReconciliationCandidateDisposition::DeterministicEmbeddingMatch => {
            "deterministic_embedding"
        }
        ReconciliationCandidateDisposition::LowSimilarityCreateNew => "low_similarity_create_new",
        ReconciliationCandidateDisposition::AmbiguousEmbeddingMatch => "ambiguous_embedding",
    }
}

fn format_counts(counts: BTreeMap<String, usize>) -> String {
    if counts.is_empty() {
        return "none".to_string();
    }
    counts
        .into_iter()
        .map(|(key, count)| format!("{key}={count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn write_report(out_dir: &Path, report: &ArtifactProbeReport) -> Result<()> {
    let path = out_dir.join(format!("{}.json", report.artifact_id));
    let json = serde_json::to_string_pretty(report)
        .with_context(|| format!("failed to serialize report for {}", report.artifact_id))?;
    fs::write(&path, json).with_context(|| format!("failed to write report {}", path.display()))?;
    Ok(())
}
