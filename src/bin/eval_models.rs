#![deny(warnings)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::OpenAiConfig;
use open_archive::processor::{
    ArtifactProcessorFactory, ArtifactProcessorInput, OpenAiProcessorFactory,
};
use open_archive::storage::types::{
    EnrichmentTier, LoadedParticipant, LoadedSegment, ScopeType, SourceType,
};
use open_archive::{ParticipantRole, VisibilityStatus};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "eval_models")]
#[command(about = "Run OpenArchive enrichment eval fixtures against one or more OpenAI models")]
struct Args {
    /// OpenAI model ids to compare.
    #[arg(required = true)]
    models: Vec<String>,

    /// Fixture case names to run. Defaults to all fixtures in tests/fixtures/enrichment.
    #[arg(long = "case")]
    cases: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EvalFixture {
    artifact_id: String,
    source_type: String,
    title: Option<String>,
    segments: Vec<EvalSegment>,
    expectations: EvalExpectations,
}

#[derive(Debug, Deserialize)]
struct EvalSegment {
    segment_id: String,
    participant_role: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct EvalExpectations {
    memory_count: EvalRange,
    classification_count: EvalRange,
    importance_score: EvalRange,
    required_phrases: Vec<String>,
    forbidden_classification_values: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EvalRange {
    min: usize,
    max: usize,
}

#[derive(Debug)]
struct CaseResult {
    case_name: String,
    score: u8,
    passed: bool,
    notes: Vec<String>,
    output_summary: String,
    elapsed: Duration,
    usage_summary: Option<String>,
    reported_cost_micros: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ModelPricing {
    input_per_million: f32,
    output_per_million: f32,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = OpenAiConfig::from_env()
        .context("failed to load OpenAI config from env; set OA_OPENAI_API_KEY and related vars")?;
    let case_names = if args.cases.is_empty() {
        discover_case_names()?
    } else {
        args.cases.clone()
    };

    if case_names.is_empty() {
        return Err(anyhow!("no eval fixture cases found"));
    }

    println!("OpenArchive model eval");
    println!("Cases: {}", case_names.join(", "));
    println!();

    let mut overall: Vec<(String, f32, usize, usize, Duration)> = Vec::new();

    for model in args.models {
        let results = run_model(&config, &model, &case_names)
            .with_context(|| format!("model {model} failed during eval"))?;

        let total_score: usize = results.iter().map(|result| result.score as usize).sum();
        let average = total_score as f32 / results.len() as f32;
        let passed = results.iter().filter(|result| result.passed).count();
        let total_elapsed: Duration = results.iter().map(|result| result.elapsed).sum();
        let average_elapsed =
            Duration::from_secs_f64(total_elapsed.as_secs_f64() / results.len() as f64);
        let total_cost_micros: u64 = results
            .iter()
            .filter_map(|result| result.reported_cost_micros)
            .sum();
        let has_cost = results
            .iter()
            .any(|result| result.reported_cost_micros.is_some());

        println!("Model: {model}");
        if let Some(pricing) = pricing_for_model(&model) {
            println!(
                "Pricing: ${:.4}/M input, ${:.4}/M output",
                pricing.input_per_million, pricing.output_per_million
            );
        } else {
            println!("Pricing: unknown");
        }

        for result in &results {
            println!(
                "  {}: {:>3}/100 {} ({})",
                result.case_name,
                result.score,
                if result.passed { "pass" } else { "warn" },
                format_duration(result.elapsed),
            );
            println!("    {}", result.output_summary);
            if let Some(usage_summary) = &result.usage_summary {
                println!("    usage: {}", usage_summary);
            }
            if !result.notes.is_empty() {
                println!("    notes: {}", result.notes.join("; "));
            }
        }

        if has_cost {
            println!(
                "  overall: {:.1}/100 ({} of {} cases passed, total {}, avg {}, cost {})",
                average,
                passed,
                results.len(),
                format_duration(total_elapsed),
                format_duration(average_elapsed),
                format_cost_micros(total_cost_micros),
            );
        } else {
            println!(
                "  overall: {:.1}/100 ({} of {} cases passed, total {}, avg {})",
                average,
                passed,
                results.len(),
                format_duration(total_elapsed),
                format_duration(average_elapsed),
            );
        }
        println!();

        overall.push((model, average, passed, results.len(), total_elapsed));
    }

    overall.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    println!("Ranking:");
    for (index, (model, average, passed, total, elapsed)) in overall.iter().enumerate() {
        println!(
            "  {}. {} - {:.1}/100 ({} of {} passed, {})",
            index + 1,
            model,
            average,
            passed,
            total,
            format_duration(*elapsed),
        );
    }

    Ok(())
}

fn run_model(
    base_config: &OpenAiConfig,
    model: &str,
    case_names: &[String],
) -> Result<Vec<CaseResult>> {
    let factory = OpenAiProcessorFactory::new(OpenAiConfig {
        api_key: base_config.api_key.clone(),
        base_url: base_config.base_url.clone(),
        max_output_tokens: base_config.max_output_tokens,
        repair_max_output_tokens: base_config.repair_max_output_tokens,
        reasoning_effort_override: base_config.reasoning_effort_override,
        standard_model: model.to_string(),
        quality_model: Some(model.to_string()),
        reconcile_standard_model: model.to_string(),
        reconcile_quality_model: Some(model.to_string()),
    })
    .map_err(|err| anyhow!("failed to build OpenAI factory: {err}"))?;

    let processor = factory
        .build(EnrichmentTier::Standard)
        .map_err(|err| anyhow!("failed to build processor for {model}: {err}"))?;

    let mut results = Vec::new();
    for case_name in case_names {
        let fixture = load_fixture(case_name)?;
        let input = build_processor_input(&fixture)?;
        let started = Instant::now();
        let output = processor
            .process(&input)
            .map_err(|err| anyhow!("case {} returned processor error: {err}", case_name))?;
        let elapsed = started.elapsed();
        results.push(score_case(case_name, &fixture, &output, elapsed));
    }

    Ok(results)
}

fn build_processor_input(fixture: &EvalFixture) -> Result<ArtifactProcessorInput> {
    let source_type = SourceType::parse(&fixture.source_type)
        .ok_or_else(|| anyhow!("unsupported source_type {}", fixture.source_type))?;

    let mut participants_by_role: BTreeMap<String, String> = BTreeMap::new();
    let mut participants = Vec::new();

    for segment in &fixture.segments {
        let role = ParticipantRole::parse(&segment.participant_role)
            .ok_or_else(|| anyhow!("unsupported participant_role {}", segment.participant_role))?;
        if !participants_by_role.contains_key(&segment.participant_role) {
            let participant_id = format!("participant-{}", participants_by_role.len() + 1);
            participants_by_role.insert(segment.participant_role.clone(), participant_id.clone());
            participants.push(LoadedParticipant {
                participant_id,
                participant_role: role,
                display_name: None,
                external_id: None,
            });
        }
    }

    let segments = fixture
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| {
            let role = ParticipantRole::parse(&segment.participant_role).ok_or_else(|| {
                anyhow!("unsupported participant_role {}", segment.participant_role)
            })?;
            Ok(LoadedSegment {
                segment_id: segment.segment_id.clone(),
                participant_id: participants_by_role.get(&segment.participant_role).cloned(),
                participant_role: Some(role),
                sequence_no: index as i32 + 1,
                text_content: segment.text.clone(),
                created_at_source: None,
                visibility_status: VisibilityStatus::Visible,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArtifactProcessorInput {
        artifact_id: fixture.artifact_id.clone(),
        import_id: format!("eval-import-{}", fixture.artifact_id),
        artifact_class: open_archive::storage::ArtifactClass::Conversation,
        source_type,
        title: fixture.title.clone(),
        imported_note_metadata: None,
        participants,
        segments,
    })
}

fn score_case(
    case_name: &str,
    fixture: &EvalFixture,
    output: &open_archive::processor::ArtifactProcessorOutput,
    elapsed: Duration,
) -> CaseResult {
    let mut score: i32 = 100;
    let mut notes = Vec::new();
    let valid_segment_ids: HashSet<&str> = fixture
        .segments
        .iter()
        .map(|segment| segment.segment_id.as_str())
        .collect();

    let mut missing_phrase_count = 0;
    let searchable_text = build_searchable_text(fixture, output);

    if output.classifications.len() < fixture.expectations.classification_count.min
        || output.classifications.len() > fixture.expectations.classification_count.max
    {
        score -= 10;
        notes.push(format!(
            "classification count {} outside {}..={}",
            output.classifications.len(),
            fixture.expectations.classification_count.min,
            fixture.expectations.classification_count.max
        ));
    }

    if output.memories.len() < fixture.expectations.memory_count.min
        || output.memories.len() > fixture.expectations.memory_count.max
    {
        score -= 15;
        notes.push(format!(
            "memory count {} outside {}..={}",
            output.memories.len(),
            fixture.expectations.memory_count.min,
            fixture.expectations.memory_count.max
        ));
    }

    let importance = output.importance_score as usize;
    if importance < fixture.expectations.importance_score.min
        || importance > fixture.expectations.importance_score.max
    {
        score -= 10;
        notes.push(format!(
            "importance {} outside {}..={}",
            importance,
            fixture.expectations.importance_score.min,
            fixture.expectations.importance_score.max
        ));
    }

    if !validate_evidence(&output.summary.evidence_segment_ids, &valid_segment_ids) {
        score -= 20;
        notes.push("summary has invalid evidence ids".to_string());
    }

    for classification in &output.classifications {
        if !matches!(
            classification.classification_type.as_str(),
            "topic" | "intent"
        ) {
            score -= 10;
            notes.push(format!(
                "invalid classification type {}",
                classification.classification_type
            ));
        }

        if fixture
            .expectations
            .forbidden_classification_values
            .iter()
            .any(|value| value.eq_ignore_ascii_case(&classification.classification_value))
        {
            score -= 10;
            notes.push(format!(
                "generic classification value {}",
                classification.classification_value
            ));
        }

        if !validate_evidence(&classification.evidence_segment_ids, &valid_segment_ids) {
            score -= 10;
            notes.push(format!(
                "classification {} has invalid evidence ids",
                classification.classification_value
            ));
        }
    }

    let mut seen_memory_titles = HashSet::new();
    for memory in &output.memories {
        if !matches!(
            memory.memory_type.as_str(),
            "personal_fact" | "preference" | "project_fact" | "ongoing_state" | "reference"
        ) {
            score -= 10;
            notes.push(format!("invalid memory type {}", memory.memory_type));
        }

        if memory.memory_scope != ScopeType::Artifact {
            score -= 10;
            notes.push(format!("unexpected memory scope {:?}", memory.memory_scope));
        }

        if memory.memory_scope_value != fixture.artifact_id {
            score -= 10;
            notes.push(format!(
                "memory scope value {} != {}",
                memory.memory_scope_value, fixture.artifact_id
            ));
        }

        if !seen_memory_titles.insert(
            memory
                .title
                .clone()
                .unwrap_or_default()
                .to_ascii_lowercase(),
        ) {
            score -= 5;
            notes.push("duplicate memory title".to_string());
        }

        if !validate_evidence(&memory.evidence_segment_ids, &valid_segment_ids) {
            score -= 10;
            notes.push(format!(
                "memory {:?} has invalid evidence ids",
                memory.title
            ));
        }
    }

    for phrase in &fixture.expectations.required_phrases {
        if !searchable_text.contains(&phrase.to_ascii_lowercase()) {
            missing_phrase_count += 1;
        }
    }
    if missing_phrase_count > 0 {
        let penalty = ((missing_phrase_count * 30)
            / fixture.expectations.required_phrases.len().max(1)) as i32;
        score -= penalty;
        notes.push(format!(
            "missing {} required theme(s)",
            missing_phrase_count
        ));
    }

    score = score.clamp(0, 100);
    let output_summary = summarize_output(output);
    let usage_summary = output.usage.as_ref().map(format_usage_summary);

    CaseResult {
        case_name: case_name.to_string(),
        score: score as u8,
        passed: score >= 80,
        notes,
        output_summary,
        elapsed,
        usage_summary,
        reported_cost_micros: output
            .usage
            .as_ref()
            .and_then(|usage| usage.reported_cost_micros),
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_millis() < 1000 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

fn validate_evidence(evidence_segment_ids: &[String], valid_segment_ids: &HashSet<&str>) -> bool {
    !evidence_segment_ids.is_empty()
        && evidence_segment_ids
            .iter()
            .all(|segment_id| valid_segment_ids.contains(segment_id.as_str()))
}

fn summarize_output(output: &open_archive::processor::ArtifactProcessorOutput) -> String {
    format!(
        "{} memory(ies), {} classification(s), importance {}",
        output.memories.len(),
        output.classifications.len(),
        output.importance_score
    )
}

fn format_usage_summary(usage: &open_archive::processor::InferenceUsage) -> String {
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
        parts.push(format!("cost {}", format_cost_micros(cost_micros)));
    }
    parts.join(", ")
}

fn format_cost_micros(cost_micros: u64) -> String {
    format!("${:.6}", cost_micros as f64 / 1_000_000.0)
}

fn build_searchable_text(
    fixture: &EvalFixture,
    output: &open_archive::processor::ArtifactProcessorOutput,
) -> String {
    let mut parts = Vec::new();
    if let Some(title) = &fixture.title {
        parts.push(title.to_ascii_lowercase());
    }
    if let Some(title) = &output.summary.title {
        parts.push(title.to_ascii_lowercase());
    }
    parts.push(output.summary.body_text.to_ascii_lowercase());
    for classification in &output.classifications {
        if let Some(title) = &classification.title {
            parts.push(title.to_ascii_lowercase());
        }
        if let Some(body_text) = &classification.body_text {
            parts.push(body_text.to_ascii_lowercase());
        }
        parts.push(classification.classification_value.to_ascii_lowercase());
    }
    for memory in &output.memories {
        if let Some(title) = &memory.title {
            parts.push(title.to_ascii_lowercase());
        }
        parts.push(memory.body_text.to_ascii_lowercase());
    }
    parts.join("\n")
}

fn pricing_for_model(model: &str) -> Option<ModelPricing> {
    let table: HashMap<&'static str, ModelPricing> = HashMap::from([
        (
            "qwen/qwen-2.5-72b-instruct",
            ModelPricing {
                input_per_million: 0.12,
                output_per_million: 0.39,
            },
        ),
        (
            "cohere/command-r7b-12-2024",
            ModelPricing {
                input_per_million: 0.0375,
                output_per_million: 0.15,
            },
        ),
        (
            "openai/gpt-4.1-mini",
            ModelPricing {
                input_per_million: 0.40,
                output_per_million: 1.60,
            },
        ),
        (
            "google/gemini-2.5-flash-lite",
            ModelPricing {
                input_per_million: 0.10,
                output_per_million: 0.40,
            },
        ),
        (
            "openai/gpt-5.4",
            ModelPricing {
                input_per_million: 2.50,
                output_per_million: 15.00,
            },
        ),
    ]);
    table.get(model).copied()
}

fn discover_case_names() -> Result<Vec<String>> {
    let mut case_names = Vec::new();
    for entry in fs::read_dir(fixture_dir()).context("failed to read fixture directory")? {
        let entry = entry?;
        let path = entry.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .ends_with(".fixture.json")
        {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap()
            .trim_end_matches(".fixture.json")
            .to_string();
        case_names.push(name);
    }
    case_names.sort();
    Ok(case_names)
}

fn load_fixture(case_name: &str) -> Result<EvalFixture> {
    let path = fixture_dir().join(format!("{case_name}.fixture.json"));
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read fixture {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse fixture {}", path.display()))
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("enrichment")
}
