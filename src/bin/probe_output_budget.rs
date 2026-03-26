use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::{
    AnthropicConfig, GeminiConfig, GrokConfig, OpenAiConfig, PostgresConfig,
};
use open_archive::processor::{
    AnthropicProcessorFactory, ArtifactProcessorFactory, ArtifactProcessorInput,
    GeminiProcessorFactory, GrokProcessorFactory, OpenAiProcessorFactory,
};
use open_archive::storage::types::EnrichmentTier;
use open_archive::storage::{ArtifactReadStore, PostgresArtifactReadStore};

#[derive(Debug, Parser)]
#[command(name = "probe_output_budget")]
#[command(about = "Replay real imported artifacts against multiple OpenRouter output budgets")]
struct Args {
    /// Artifact ids to probe.
    #[arg(required = true)]
    artifact_ids: Vec<String>,

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
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let read_store = PostgresArtifactReadStore::new(postgres);
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

    for artifact_id in &args.artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(artifact_id)
            .with_context(|| format!("failed to load artifact {}", artifact_id))?
            .ok_or_else(|| anyhow!("artifact {} not found", artifact_id))?;
        let input = ArtifactProcessorInput {
            artifact_id: loaded.artifact.artifact_id.clone(),
            import_id: loaded.artifact.import_id.clone(),
            artifact_class: loaded.artifact.artifact_class,
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
            match processor.process(&input) {
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
                }
            }
        }
        println!();
    }

    Ok(())
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
