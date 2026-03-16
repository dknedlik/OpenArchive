use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};

use open_archive::config::{GrokConfig, PostgresConfig};
use open_archive::processor::{
    ArtifactProcessorFactory, ArtifactProcessorInput, BatchPollResult, GrokProcessorFactory,
    MemoryOutput, PreprocessProcessorInput, RelationshipOutput, ReconciliationProcessorInput,
    SummaryOutput,
};
use open_archive::storage::enrichment_state_store::EnrichmentStateStore;
use open_archive::storage::types::{
    ArtifactReconcilePayload, EnrichmentTier,
};
use open_archive::storage::{
    ArtifactReadStore, PostgresDerivedMetadataStore, PostgresImportWriteStore,
};
use postgres::NoTls;

#[derive(Debug, Parser)]
#[command(name = "probe_grok_batch")]
#[command(about = "Exercise Grok batch submit/poll/parse for preprocess, extract, or reconcile")]
struct Args {
    #[command(subcommand)]
    command: Command,

    #[arg(long = "model")]
    model: Option<String>,

    #[arg(long, default_value_t = 2)]
    poll_seconds: u64,

    #[arg(long, default_value_t = 120)]
    timeout_seconds: u64,
}

#[derive(Debug, Subcommand)]
enum Command {
    Preprocess {
        artifact_ids: Vec<String>,
    },
    Extract {
        artifact_ids: Vec<String>,
    },
    Reconcile {
        job_ids: Vec<String>,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let mut grok = GrokConfig::from_env().context("failed to load Grok config from env")?;
    if let Some(model) = &args.model {
        grok.standard_model = model.clone();
        grok.quality_model = Some(model.clone());
    }
    let model = grok.standard_model.clone();
    let factory =
        GrokProcessorFactory::new(grok).map_err(|err| anyhow!("failed to build Grok factory: {err}"))?;
    let read_store = PostgresImportWriteStore::new(postgres.clone());
    let derived_store = PostgresDerivedMetadataStore::new(postgres.clone());

    println!("Grok batch probe");
    println!("Model: {model}");
    println!("Poll: {}s", args.poll_seconds);
    println!("Timeout: {}s", args.timeout_seconds);
    println!();

    match args.command {
        Command::Preprocess { artifact_ids } => {
            run_preprocess(
                &factory,
                &read_store,
                &artifact_ids,
                Duration::from_secs(args.poll_seconds),
                Duration::from_secs(args.timeout_seconds),
            )?;
        }
        Command::Extract { artifact_ids } => {
            run_extract(
                &factory,
                &read_store,
                &artifact_ids,
                Duration::from_secs(args.poll_seconds),
                Duration::from_secs(args.timeout_seconds),
            )?;
        }
        Command::Reconcile { job_ids } => {
            run_reconcile(
                &factory,
                &postgres,
                &read_store,
                &derived_store,
                &job_ids,
                Duration::from_secs(args.poll_seconds),
                Duration::from_secs(args.timeout_seconds),
            )?;
        }
    }

    Ok(())
}

fn run_preprocess(
    factory: &GrokProcessorFactory,
    read_store: &PostgresImportWriteStore,
    artifact_ids: &[String],
    poll_interval: Duration,
    timeout: Duration,
) -> Result<()> {
    let submitter = factory
        .build_preprocess_submitter(EnrichmentTier::Standard)?
        .ok_or_else(|| anyhow!("Grok preprocess submitter unavailable"))?;
    let mut inputs = Vec::new();
    for artifact_id in artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(artifact_id)?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        inputs.push(PreprocessProcessorInput {
            artifact_id: loaded.artifact.artifact_id,
            import_id: loaded.artifact.import_id,
            source_type: loaded.artifact.source_type,
            title: loaded.artifact.title,
            participants: loaded.participants,
            segments: loaded.segments,
        });
    }

    println!("Stage: preprocess");
    println!("Artifacts: {}", inputs.len());
    let phase_one = submitter.submit_phase_one(&inputs)?;
    println!("Phase 1 batch: {}", phase_one.batch_id);
    let phase_one_done = wait_for_batch(
        "preprocess phase 1",
        &*submitter,
        &phase_one,
        poll_interval,
        timeout,
    )?;
    let phase_one_data = submitter.parse_phase_one(phase_one_done, &inputs)?;
    println!("Phase 1 parsed");

    let phase_two = submitter.submit_phase_two(&inputs, &*phase_one_data)?;
    println!("Phase 2 batch: {}", phase_two.batch_id);
    let phase_two_done = wait_for_batch(
        "preprocess phase 2",
        &*submitter,
        &phase_two,
        poll_interval,
        timeout,
    )?;
    let results = submitter.parse_phase_two(phase_two_done, &inputs, &*phase_one_data);
    for (input, result) in inputs.iter().zip(results.iter()) {
        match result {
            Ok(output) => {
                println!(
                    "  ok {} | threads={} escalate={}",
                    input.artifact_id,
                    output.topic_threads.len(),
                    output.escalate_to_quality
                );
            }
            Err(err) => {
                println!("  err {} | {}", input.artifact_id, err);
            }
        }
    }
    Ok(())
}

fn run_extract(
    factory: &GrokProcessorFactory,
    read_store: &PostgresImportWriteStore,
    artifact_ids: &[String],
    poll_interval: Duration,
    timeout: Duration,
) -> Result<()> {
    let submitter = factory
        .build_extraction_submitter(EnrichmentTier::Standard)?
        .ok_or_else(|| anyhow!("Grok extraction submitter unavailable"))?;
    let mut inputs = Vec::new();
    for artifact_id in artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(artifact_id)?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        inputs.push(ArtifactProcessorInput {
            artifact_id: loaded.artifact.artifact_id,
            import_id: loaded.artifact.import_id,
            source_type: loaded.artifact.source_type,
            title: loaded.artifact.title,
            participants: loaded.participants,
            segments: loaded.segments,
        });
    }

    println!("Stage: extract");
    println!("Artifacts: {}", inputs.len());
    let handle = submitter.prepare_and_submit(&inputs)?;
    println!("Batch: {}", handle.batch_id);
    let completed = wait_for_batch("extract", &*submitter, &handle, poll_interval, timeout)?;
    let results = submitter.parse_results(completed, &inputs);
    for (input, result) in inputs.iter().zip(results.iter()) {
        match result {
            Ok(output) => {
                println!(
                    "  ok {} | classifications={} memories={} relationships={} intents={} importance={}",
                    input.artifact_id,
                    output.classifications.len(),
                    output.memories.len(),
                    output.relationships.len(),
                    output.retrieval_intents.len(),
                    output.importance_score
                );
            }
            Err(err) => {
                println!("  err {} | {}", input.artifact_id, err);
            }
        }
    }
    Ok(())
}

fn run_reconcile(
    factory: &GrokProcessorFactory,
    postgres: &PostgresConfig,
    read_store: &PostgresImportWriteStore,
    derived_store: &PostgresDerivedMetadataStore,
    job_ids: &[String],
    poll_interval: Duration,
    timeout: Duration,
) -> Result<()> {
    let submitter = factory
        .build_reconciliation_submitter(EnrichmentTier::Standard)?
        .ok_or_else(|| anyhow!("Grok reconciliation submitter unavailable"))?;
    let mut client = postgres::Client::connect(&postgres.connection_string, NoTls)
        .context("failed to connect to Postgres")?;
    let mut inputs = Vec::new();

    for job_id in job_ids {
        let row = client
            .query_opt(
                "select payload_json::text, artifact_id from oa_enrichment_job where job_id = $1",
                &[job_id],
            )?
            .ok_or_else(|| anyhow!("job {job_id} not found"))?;
        let payload_json: String = row.get(0);
        let artifact_id: String = row.get(1);
        let payload = ArtifactReconcilePayload::from_json(&payload_json)?;
        let loaded = read_store
            .load_artifact_for_enrichment(&artifact_id)?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        let extraction_result = derived_store
            .load_extraction_result(&payload.extraction_result_id)?
            .ok_or_else(|| anyhow!("extraction result {} not found", payload.extraction_result_id))?;
        let retrieval_result_set = derived_store
            .load_retrieval_result_set(&payload.retrieval_result_set_id)?
            .ok_or_else(|| anyhow!("retrieval result set {} not found", payload.retrieval_result_set_id))?;

        inputs.push(ReconciliationProcessorInput {
            artifact_id,
            source_type: loaded.artifact.source_type,
            summary: SummaryOutput {
                title: extraction_result.summary_title.clone(),
                body_text: extraction_result.summary_body_text.clone(),
                evidence_segment_ids: extraction_result.summary_evidence_segment_ids.clone(),
            },
            memories: extraction_result
                .memories
                .iter()
                .map(|memory| MemoryOutput {
                    title: memory.title.clone(),
                    body_text: memory.body_text.clone(),
                    memory_type: memory.memory_type.clone(),
                    memory_scope: memory.memory_scope,
                    memory_scope_value: memory.memory_scope_value.clone(),
                    evidence_segment_ids: memory.evidence_segment_ids.clone(),
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
            retrieval_results_json: serde_json::to_string_pretty(&retrieval_result_set)?,
        });
    }

    println!("Stage: reconcile");
    println!("Jobs: {}", inputs.len());
    for input in &inputs {
        let candidate_count = input.memories.len() + input.relationships.len();
        println!("  {} | candidates={}", input.artifact_id, candidate_count);
    }
    let handle = submitter.prepare_and_submit(&inputs)?;
    println!("Batch: {}", handle.batch_id);
    let completed = wait_for_batch("reconcile", &*submitter, &handle, poll_interval, timeout)?;
    let results = submitter.parse_results(completed, &inputs);
    for (input, result) in inputs.iter().zip(results.iter()) {
        match result {
            Ok(decisions) => {
                println!(
                    "  ok {} | decisions={}",
                    input.artifact_id,
                    decisions.len()
                );
                for decision in decisions {
                    println!(
                        "    - {} | {} | {}",
                        serde_json::to_string(&decision.decision_kind)
                            .unwrap_or_else(|_| "\"?\"".to_string()),
                        decision.target_kind,
                        decision.target_key
                    );
                }
            }
            Err(err) => {
                println!("  err {} | {}", input.artifact_id, err);
            }
        }
    }
    Ok(())
}

fn wait_for_batch<T: BatchProbe + ?Sized>(
    label: &str,
    submitter: &T,
    handle: &open_archive::processor::BatchHandle,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<Box<dyn std::any::Any>> {
    let started = Instant::now();
    loop {
        if started.elapsed() > timeout {
            return Err(anyhow!("{label} batch {} timed out", handle.batch_id));
        }
        match submitter.poll(handle)? {
            BatchPollResult::Pending => {
                println!("  polling {} ... pending", handle.batch_id);
                thread::sleep(poll_interval);
            }
            BatchPollResult::Succeeded(data) => return Ok(data),
            BatchPollResult::Failed(message) => {
                return Err(anyhow!("{label} batch {} failed: {}", handle.batch_id, message))
            }
        }
    }
}

trait BatchProbe {
    fn poll(
        &self,
        handle: &open_archive::processor::BatchHandle,
    ) -> Result<BatchPollResult, open_archive::processor::ProcessorError>;
}

impl BatchProbe for dyn open_archive::processor::ExtractionBatchSubmitter {
    fn poll(
        &self,
        handle: &open_archive::processor::BatchHandle,
    ) -> Result<BatchPollResult, open_archive::processor::ProcessorError> {
        self.poll_batch(handle)
    }
}

impl BatchProbe for dyn open_archive::processor::PreprocessBatchSubmitter {
    fn poll(
        &self,
        handle: &open_archive::processor::BatchHandle,
    ) -> Result<BatchPollResult, open_archive::processor::ProcessorError> {
        self.poll_batch(handle)
    }
}

impl BatchProbe for dyn open_archive::processor::ReconciliationBatchSubmitter {
    fn poll(
        &self,
        handle: &open_archive::processor::BatchHandle,
    ) -> Result<BatchPollResult, open_archive::processor::ProcessorError> {
        self.poll_batch(handle)
    }
}
