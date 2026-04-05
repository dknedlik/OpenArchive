#![deny(warnings)]

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use postgres::NoTls;

use open_archive::config::{
    AnthropicConfig, GeminiConfig, GrokConfig, OciConfig, OpenAiConfig, PostgresConfig,
};
use open_archive::processor::{
    memory_candidate_key_from_fields, AnthropicProcessorFactory, ArtifactProcessorFactory,
    EntityOutput, GeminiProcessorFactory, GrokProcessorFactory, MemoryOutput, OciProcessorFactory,
    OpenAiProcessorFactory, ReconciliationProcessorInput, RelationshipOutput, SummaryOutput,
};
use open_archive::storage::enrichment_state_store::EnrichmentStateStore;
use open_archive::storage::types::{
    ArtifactReconcilePayload, ReconciliationDecisionKind, SourceType,
};
use open_archive::storage::{
    ArtifactReadStore, PostgresArtifactReadStore, PostgresDerivedMetadataStore,
};

#[derive(Debug, Parser)]
#[command(name = "probe_reconcile_provider")]
#[command(about = "Replay real reconciliation jobs against a selected inference provider")]
struct Args {
    #[arg(required = true)]
    job_ids: Vec<String>,

    #[arg(long = "provider", value_enum, default_value_t = ProbeProvider::OpenAi)]
    provider: ProbeProvider,

    #[arg(long = "model")]
    model: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProbeProvider {
    #[value(name = "openai", alias = "open-ai")]
    OpenAi,
    Gemini,
    Anthropic,
    Grok,
    Oci,
}

impl ProbeProvider {
    fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Gemini => "gemini",
            Self::Anthropic => "anthropic",
            Self::Grok => "grok",
            Self::Oci => "oci",
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let read_store = PostgresArtifactReadStore::new(postgres.clone());
    let derived_store = PostgresDerivedMetadataStore::new(postgres.clone());
    let mut client = postgres::Client::connect(&postgres.connection_string, NoTls)
        .context("failed to connect to Postgres")?;

    let probe = build_probe(&args)?;
    let processor = probe
        .factory
        .build_reconciliation_processor(open_archive::storage::EnrichmentTier::Default)
        .map_err(|err| anyhow!("failed to build reconciliation processor: {err}"))?;

    println!("Reconcile provider probe");
    println!("Provider: {}", args.provider.as_str());
    println!("Model: {}", probe.model);
    println!();

    for job_id in &args.job_ids {
        let row = client
            .query_opt(
                "select payload_json::text, artifact_id, last_error_message
                 from oa_enrichment_job
                 where job_id = $1",
                &[job_id],
            )
            .with_context(|| format!("failed to load job {job_id}"))?
            .ok_or_else(|| anyhow!("job {job_id} not found"))?;

        let payload_json: String = row.get(0);
        let artifact_id: String = row.get(1);
        let last_error_message: Option<String> = row.get(2);
        let payload = ArtifactReconcilePayload::from_json(&payload_json)
            .context("invalid reconcile payload_json")?;

        let loaded = read_store
            .load_artifact_for_enrichment(&artifact_id)
            .with_context(|| format!("failed to load artifact {artifact_id}"))?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        let extraction_result = derived_store
            .load_extraction_result(&payload.extraction_result_id)
            .with_context(|| {
                format!(
                    "failed to load extraction result {}",
                    payload.extraction_result_id
                )
            })?
            .ok_or_else(|| {
                anyhow!(
                    "extraction result {} not found",
                    payload.extraction_result_id
                )
            })?;
        let input = build_reconciliation_input(
            &loaded.artifact.artifact_id,
            loaded.artifact.source_type,
            &extraction_result,
        )?;

        println!("Job: {job_id}");
        println!(
            "Artifact: {} | title: {}",
            loaded.artifact.artifact_id,
            loaded.artifact.title.as_deref().unwrap_or("")
        );
        println!(
            "Candidates: {} memories, {} entities, {} relationships",
            input.memories.len(),
            input.entities.len(),
            input.relationships.len()
        );
        if let Some(message) = &last_error_message {
            println!("Last error: {message}");
        }

        match processor.reconcile(&input) {
            Ok(decisions) => {
                println!("Decisions: {}", decisions.len());
                for decision in decisions {
                    println!(
                        "  - {} | {} | {}{}",
                        decision.target_kind.as_str(),
                        decision.target_key,
                        reconciliation_decision_kind_label(decision.decision_kind),
                        decision
                            .matched_object_id
                            .as_ref()
                            .map(|id| format!(" | matched {}", id))
                            .unwrap_or_default()
                    );
                }
            }
            Err(err) => {
                println!("Error: {err}");
            }
        }
        println!();
    }

    Ok(())
}

struct ProbeFactory {
    model: String,
    factory: Box<dyn ArtifactProcessorFactory>,
}

fn build_probe(args: &Args) -> Result<ProbeFactory> {
    match args.provider {
        ProbeProvider::OpenAi => {
            let mut config = OpenAiConfig::from_env().context(
                "failed to load OpenAI config from env; set OA_OPENAI_API_KEY and related vars",
            )?;
            let model = args
                .model
                .clone()
                .unwrap_or_else(|| config.fast_model.clone());
            config.fast_model = model.clone();
            let factory = OpenAiProcessorFactory::new(config)
                .map_err(|err| anyhow!("failed to build OpenAI factory: {err}"))?;
            Ok(ProbeFactory {
                model,
                factory: Box::new(factory),
            })
        }
        ProbeProvider::Gemini => {
            let mut config = GeminiConfig::from_env().context(
                "failed to load Gemini config from env; set OA_GEMINI_API_KEY and related vars",
            )?;
            let model = args
                .model
                .clone()
                .unwrap_or_else(|| config.fast_model.clone());
            config.fast_model = model.clone();
            let factory = GeminiProcessorFactory::new(config)
                .map_err(|err| anyhow!("failed to build Gemini factory: {err}"))?;
            Ok(ProbeFactory {
                model,
                factory: Box::new(factory),
            })
        }
        ProbeProvider::Anthropic => {
            let mut config = AnthropicConfig::from_env().context(
                "failed to load Anthropic config from env; set OA_ANTHROPIC_API_KEY and related vars",
            )?;
            let model = args
                .model
                .clone()
                .unwrap_or_else(|| config.fast_model.clone());
            config.fast_model = model.clone();
            let factory = AnthropicProcessorFactory::new(config)
                .map_err(|err| anyhow!("failed to build Anthropic factory: {err}"))?;
            Ok(ProbeFactory {
                model,
                factory: Box::new(factory),
            })
        }
        ProbeProvider::Grok => {
            let mut config = GrokConfig::from_env().context(
                "failed to load Grok config from env; set OA_GROK_API_KEY and related vars",
            )?;
            let model = args
                .model
                .clone()
                .unwrap_or_else(|| config.fast_model.clone());
            config.fast_model = model.clone();
            let factory = GrokProcessorFactory::new(config)
                .map_err(|err| anyhow!("failed to build Grok factory: {err}"))?;
            Ok(ProbeFactory {
                model,
                factory: Box::new(factory),
            })
        }
        ProbeProvider::Oci => {
            let mut config = OciConfig::from_env().context(
                "failed to load OCI config from env; set OA_OCI_REGION, OA_OCI_COMPARTMENT_ID, and related vars",
            )?;
            let model = args
                .model
                .clone()
                .unwrap_or_else(|| config.fast_model.clone());
            config.fast_model = model.clone();
            let factory = OciProcessorFactory::new(config)
                .map_err(|err| anyhow!("failed to build OCI factory: {err}"))?;
            Ok(ProbeFactory {
                model,
                factory: Box::new(factory),
            })
        }
    }
}

fn build_reconciliation_input(
    artifact_id: &str,
    source_type: SourceType,
    extraction_result: &open_archive::storage::types::ArtifactExtractionResult,
) -> Result<ReconciliationProcessorInput> {
    Ok(ReconciliationProcessorInput {
        artifact_id: artifact_id.to_string(),
        source_type,
        summary: SummaryOutput {
            title: extraction_result.summary_title.clone(),
            body_text: extraction_result.summary_body_text.clone(),
            evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
        },
        memories: extraction_result
            .memories
            .iter()
            .map(|memory| MemoryOutput {
                candidate_key: if memory.candidate_key.is_empty() {
                    memory_candidate_key_from_fields(
                        &memory.memory_type,
                        memory.memory_scope,
                        &memory.memory_scope_value,
                        memory.title.as_deref(),
                        &memory.body_text,
                    )
                } else {
                    memory.candidate_key.clone()
                },
                title: memory.title.clone(),
                body_text: memory.body_text.clone(),
                memory_type: memory.memory_type.clone(),
                memory_scope: memory.memory_scope,
                memory_scope_value: memory.memory_scope_value.clone(),
                evidence_segment_ids: memory.evidence_segment_ids.clone(),
            })
            .collect(),
        entities: extraction_result
            .entities
            .iter()
            .map(|entity| EntityOutput {
                entity_key: entity.entity_key.clone(),
                display_name: entity.display_name.clone(),
                entity_type: entity.entity_type.clone(),
                evidence_segment_ids: entity.evidence_segment_ids.clone(),
            })
            .collect(),
        relationships: extraction_result
            .relationships
            .iter()
            .map(|relationship| RelationshipOutput {
                relationship_type: relationship.relationship_type.clone(),
                subject_key: relationship.subject_key.clone(),
                object_key: relationship.object_key.clone(),
                title: relationship.title.clone(),
                body_text: relationship.body_text.clone(),
                confidence_label: relationship.confidence_label.clone(),
                evidence_segment_ids: relationship.evidence_segment_ids.clone(),
            })
            .collect(),
        retrieval_results_json: "[]".to_string(),
    })
}

fn reconciliation_decision_kind_label(kind: ReconciliationDecisionKind) -> &'static str {
    match kind {
        ReconciliationDecisionKind::CreateNew => "create_new",
        ReconciliationDecisionKind::AttachToExisting => "attach_to_existing",
        ReconciliationDecisionKind::StrengthenExisting => "strengthen_existing",
        ReconciliationDecisionKind::SupersedeExisting => "supersede_existing",
        ReconciliationDecisionKind::ContradictsExisting => "contradicts_existing",
        ReconciliationDecisionKind::InsufficientEvidence => "insufficient_evidence",
    }
}
