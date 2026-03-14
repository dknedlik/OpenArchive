use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use open_archive::config::{OpenRouterConfig, PostgresConfig};
use open_archive::processor::{
    ArtifactProcessorFactory, ArtifactProcessorInput, OpenRouterProcessorFactory,
};
use open_archive::storage::{ArtifactReadStore, PostgresImportWriteStore};
use open_archive::storage::types::EnrichmentTier;

#[derive(Debug, Parser)]
#[command(name = "probe_output_budget")]
#[command(about = "Replay real imported artifacts against multiple OpenRouter output budgets")]
struct Args {
    /// Artifact ids to probe.
    #[arg(required = true)]
    artifact_ids: Vec<String>,

    /// Output token budgets to compare.
    #[arg(long = "budget", required = true)]
    budgets: Vec<u32>,

    /// Override the model for the probe. Defaults to OA_OPENROUTER_STANDARD_MODEL.
    #[arg(long = "model")]
    model: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let base_openrouter = OpenRouterConfig::from_env()
        .context("failed to load OpenRouter config from env; set OA_OPENROUTER_API_KEY and related vars")?;
    let read_store = PostgresImportWriteStore::new(postgres);
    let model = args_model(&base_openrouter, &args)?;

    println!("Output budget probe");
    println!("Model: {}", model);
    println!("Budgets: {:?}", args.budgets);
    println!();

    for artifact_id in &args.artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(artifact_id)
            .with_context(|| format!("failed to load artifact {}", artifact_id))?
            .ok_or_else(|| anyhow!("artifact {} not found", artifact_id))?;
        let input = ArtifactProcessorInput {
            artifact_id: loaded.artifact.artifact_id.clone(),
            import_id: loaded.artifact.import_id.clone(),
            source_type: loaded.artifact.source_type,
            title: loaded.artifact.title.clone(),
            participants: loaded.participants,
            segments: loaded.segments,
        };

        println!(
            "Artifact: {} | title: {} | segments: {}",
            artifact_id,
            input.title.as_deref().unwrap_or(""),
            input.segments.len()
        );

        for budget in &args.budgets {
            let mut config = base_openrouter.clone();
            config.standard_model = model.clone();
            config.quality_model = Some(model.clone());
            config.max_output_tokens = *budget;

            let factory = OpenRouterProcessorFactory::new(config)
                .map_err(|err| anyhow!("failed to build OpenRouter factory: {err}"))?;
            let processor = factory
                .build(EnrichmentTier::Standard)
                .map_err(|err| anyhow!("failed to build processor: {err}"))?;

            let started = Instant::now();
            match processor.process(&input) {
                Ok(output) => {
                    println!(
                        "  budget {:>4}: ok ({:.2}s) | memories {} | classifications {} | importance {} | escalate {}{}",
                        budget,
                        started.elapsed().as_secs_f64(),
                        output.memories.len(),
                        output.classifications.len(),
                        output.importance_score,
                        output.escalate_to_frontier,
                        output
                            .usage
                            .as_ref()
                            .map(|usage| format!(" | {}", format_usage(usage)))
                            .unwrap_or_default()
                    );
                }
                Err(err) => {
                    println!(
                        "  budget {:>4}: err ({:.2}s) | {}",
                        budget,
                        started.elapsed().as_secs_f64(),
                        err
                    );
                }
            }
        }
        println!();
    }

    Ok(())
}

fn args_model(config: &OpenRouterConfig, args: &Args) -> Result<String> {
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
