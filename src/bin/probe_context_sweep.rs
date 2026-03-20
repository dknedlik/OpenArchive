use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use open_archive::config::{
    AnthropicConfig, EnrichmentPipelineConfig, GeminiConfig, GrokConfig, OpenAiConfig,
    PostgresConfig,
};
use open_archive::processor::{
    AnthropicProcessorFactory, ArtifactProcessorFactory, ArtifactProcessorInput,
    ArtifactProcessorOutput, ClassificationOutput, EntityOutput, GeminiProcessorFactory,
    GrokProcessorFactory, InferenceUsage, MemoryOutput, OpenAiProcessorFactory,
    PreprocessProcessorInput, RelationshipOutput,
};
use open_archive::storage::types::{
    ConversationWindowRef, EnrichmentTier, RetrievalIntent, TopicThreadRef,
};
use open_archive::storage::{ArtifactReadStore, PostgresArtifactReadStore};

#[derive(Debug, Parser)]
#[command(name = "probe_context_sweep")]
#[command(
    about = "Compare provider behavior across full-artifact, fixed-window, and topic-thread extraction modes"
)]
struct Args {
    #[arg(required = true)]
    artifact_ids: Vec<String>,

    #[arg(long = "provider", value_enum)]
    providers: Vec<Provider>,

    #[arg(long = "mode", value_enum)]
    modes: Vec<ProbeMode>,

    #[arg(long = "preprocess-provider", value_enum)]
    preprocess_provider: Option<Provider>,

    #[arg(long = "tier", value_enum, default_value_t = ProbeTier::Standard)]
    tier: ProbeTier,

    #[arg(long = "budget")]
    budgets: Vec<u32>,

    #[arg(long = "window-segments", default_value_t = 20)]
    window_segments: usize,

    #[arg(long = "window-overlap", default_value_t = 4)]
    window_overlap: usize,

    #[arg(long = "max-chars")]
    max_chars: Option<usize>,

    #[arg(long = "show-output", default_value_t = false)]
    show_output: bool,

    #[arg(long = "show-chunks", default_value_t = false)]
    show_chunks: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Provider {
    Openai,
    Gemini,
    Anthropic,
    Grok,
}

impl Provider {
    fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Gemini => "gemini",
            Self::Anthropic => "anthropic",
            Self::Grok => "grok",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProbeMode {
    Full,
    Windowed,
    TopicThreads,
}

impl ProbeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Windowed => "windowed",
            Self::TopicThreads => "topic_threads",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProbeTier {
    Standard,
    Quality,
}

impl ProbeTier {
    fn as_enrichment_tier(self) -> EnrichmentTier {
        match self {
            Self::Standard => EnrichmentTier::Standard,
            Self::Quality => EnrichmentTier::Quality,
        }
    }
}

#[derive(Debug, Clone)]
struct ChunkSummary {
    label: String,
    segment_count: usize,
    char_count: usize,
    first_sequence_no: i32,
    last_sequence_no: i32,
}

#[derive(Debug, Clone)]
struct PreMergeMetrics {
    memories: usize,
    classifications: usize,
    entities: usize,
    relationships: usize,
    retrieval_intents: usize,
    unique_evidence_segments: usize,
}

#[derive(Debug, Clone)]
struct PreprocessMetrics {
    provider: Provider,
    usage: Option<InferenceUsage>,
    topic_thread_count: usize,
    coverage_window_count: usize,
}

#[derive(Debug, Clone)]
struct ModeRun {
    output: ArtifactProcessorOutput,
    chunk_summaries: Vec<ChunkSummary>,
    pre_merge: PreMergeMetrics,
    extract_usage: Option<InferenceUsage>,
    preprocess: Option<PreprocessMetrics>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let pipeline_config = EnrichmentPipelineConfig::from_env()
        .context("failed to load enrichment pipeline config from env")?;
    let effective_max_chars = args
        .max_chars
        .unwrap_or(pipeline_config.chunking.max_chars_per_chunk);
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let read_store = PostgresArtifactReadStore::new(postgres);

    let providers = if args.providers.is_empty() {
        vec![
            Provider::Openai,
            Provider::Gemini,
            Provider::Anthropic,
            Provider::Grok,
        ]
    } else {
        args.providers.clone()
    };
    let modes = if args.modes.is_empty() {
        vec![
            ProbeMode::Full,
            ProbeMode::Windowed,
            ProbeMode::TopicThreads,
        ]
    } else {
        args.modes.clone()
    };

    println!("Context sweep probe");
    println!(
        "Providers: {}",
        providers
            .iter()
            .map(|provider| provider.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "Modes: {}",
        modes
            .iter()
            .map(|mode| mode.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!("Tier: {:?}", args.tier);
    println!(
        "Chunk limits: max_segments={} overlap={}{}",
        args.window_segments,
        args.window_overlap,
        format!(" max_chars={effective_max_chars}")
    );
    println!();

    for artifact_id in &args.artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(artifact_id)
            .with_context(|| format!("failed to load artifact {artifact_id}"))?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        let extract_input = ArtifactProcessorInput {
            artifact_id: loaded.artifact.artifact_id.clone(),
            import_id: loaded.artifact.import_id.clone(),
            source_type: loaded.artifact.source_type,
            title: loaded.artifact.title.clone(),
            participants: loaded.participants.clone(),
            segments: loaded.segments.clone(),
        };
        let preprocess_input = PreprocessProcessorInput {
            artifact_id: loaded.artifact.artifact_id.clone(),
            import_id: loaded.artifact.import_id.clone(),
            source_type: loaded.artifact.source_type,
            title: loaded.artifact.title.clone(),
            participants: loaded.participants,
            segments: loaded.segments,
        };
        let artifact_char_count = total_chars(&extract_input);

        println!(
            "Artifact: {} | title: {} | segments: {} | chars: {}",
            artifact_id,
            extract_input.title.as_deref().unwrap_or(""),
            extract_input.segments.len(),
            artifact_char_count
        );

        for provider in &providers {
            let budgets = budgets_for_provider(*provider, &args)?;
            for budget in budgets {
                for mode in &modes {
                    let started = Instant::now();
                    match run_mode(
                        &args,
                        *provider,
                        budget,
                        *mode,
                        &extract_input,
                        &preprocess_input,
                    ) {
                        Ok(run) => {
                            print_run_summary(
                                *provider,
                                budget,
                                *mode,
                                &run,
                                extract_input.segments.len(),
                                started.elapsed().as_secs_f64(),
                            );
                            if args.show_chunks {
                                print_chunks(&run.chunk_summaries);
                            }
                            if args.show_output {
                                print_output_details(&run.output);
                            }
                        }
                        Err(err) => {
                            println!(
                                "  provider={} budget={} mode={} | err ({:.2}s) | {:#}",
                                provider.as_str(),
                                budget,
                                mode.as_str(),
                                started.elapsed().as_secs_f64(),
                                err
                            );
                        }
                    }
                }
            }
        }
        println!();
    }

    Ok(())
}

fn budgets_for_provider(provider: Provider, args: &Args) -> Result<Vec<u32>> {
    if !args.budgets.is_empty() {
        return Ok(args.budgets.clone());
    }

    let budget = match provider {
        Provider::Openai => {
            OpenAiConfig::from_env()
                .context("failed to load OpenAI config from env")?
                .max_output_tokens
        }
        Provider::Gemini => {
            GeminiConfig::from_env()
                .context("failed to load Gemini config from env")?
                .max_output_tokens
        }
        Provider::Anthropic => {
            AnthropicConfig::from_env()
                .context("failed to load Anthropic config from env")?
                .max_output_tokens
        }
        Provider::Grok => {
            GrokConfig::from_env()
                .context("failed to load Grok config from env")?
                .max_output_tokens
        }
    };
    Ok(vec![budget])
}

fn run_mode(
    args: &Args,
    provider: Provider,
    budget: u32,
    mode: ProbeMode,
    extract_input: &ArtifactProcessorInput,
    preprocess_input: &PreprocessProcessorInput,
) -> Result<ModeRun> {
    let pipeline_config = EnrichmentPipelineConfig::from_env()
        .context("failed to load enrichment pipeline config from env")?;
    let effective_max_chars = Some(
        args.max_chars
            .unwrap_or(pipeline_config.chunking.max_chars_per_chunk),
    );
    let tier = args.tier.as_enrichment_tier();
    let extract_factory = build_factory(provider, budget)?;
    let processor = extract_factory
        .build(tier)
        .map_err(|err| anyhow!("failed to build extract processor: {err}"))?;

    match mode {
        ProbeMode::Full => {
            let output = processor.process(extract_input)?;
            let usage = output.usage.clone();
            Ok(ModeRun {
                pre_merge: pre_merge_metrics(std::slice::from_ref(&output)),
                chunk_summaries: vec![summarize_chunk("full", extract_input)],
                extract_usage: usage,
                preprocess: None,
                output,
            })
        }
        ProbeMode::Windowed => {
            let chunks = split_input(
                extract_input,
                "window",
                args.window_segments,
                args.window_overlap,
                effective_max_chars,
            );
            run_chunked(chunks, processor.as_ref(), None)
        }
        ProbeMode::TopicThreads => {
            let preprocess_provider = args.preprocess_provider.unwrap_or(provider);
            let preprocess_factory = build_factory(preprocess_provider, budget)?;
            let preprocess_processor = preprocess_factory
                .build_preprocess_processor(tier)
                .map_err(|err| anyhow!("failed to build preprocess processor: {err}"))?;
            let preprocess_output = preprocess_processor.segment(preprocess_input)?;
            let coverage_windows = build_preprocess_coverage_windows(
                &preprocess_input.artifact_id,
                &preprocess_input.segments,
                &preprocess_output.topic_threads,
                args.window_segments,
                args.window_overlap,
            );
            let base_chunks = build_topic_thread_inputs(
                extract_input,
                &preprocess_output.topic_threads,
                &coverage_windows,
            );
            let chunks = refine_chunks(
                base_chunks,
                args.window_segments,
                args.window_overlap,
                effective_max_chars,
            );
            run_chunked(
                chunks,
                processor.as_ref(),
                Some(PreprocessMetrics {
                    provider: preprocess_provider,
                    usage: preprocess_output.usage.clone(),
                    topic_thread_count: preprocess_output.topic_threads.len(),
                    coverage_window_count: coverage_windows.len(),
                }),
            )
        }
    }
}

fn run_chunked(
    chunks: Vec<ArtifactProcessorInput>,
    processor: &dyn open_archive::processor::ArtifactProcessor,
    preprocess: Option<PreprocessMetrics>,
) -> Result<ModeRun> {
    let mut outputs = Vec::new();
    let chunk_summaries = chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| summarize_chunk(&format!("chunk-{}", index + 1), chunk))
        .collect::<Vec<_>>();

    for chunk in &chunks {
        let output = processor.process(chunk).with_context(|| {
            format!(
                "chunk extraction failed for sequences {}-{}",
                chunk
                    .segments
                    .first()
                    .map(|segment| segment.sequence_no)
                    .unwrap_or(0),
                chunk
                    .segments
                    .last()
                    .map(|segment| segment.sequence_no)
                    .unwrap_or(0)
            )
        })?;
        outputs.push(output);
    }

    let merged = merge_chunk_outputs(&chunks[0], &outputs);
    let usage = combine_usages(outputs.iter().filter_map(|output| output.usage.as_ref()));
    let pre_merge = pre_merge_metrics(&outputs);
    Ok(ModeRun {
        output: merged,
        chunk_summaries,
        pre_merge,
        extract_usage: usage,
        preprocess,
    })
}

fn build_factory(provider: Provider, budget: u32) -> Result<Box<dyn ArtifactProcessorFactory>> {
    match provider {
        Provider::Openai => {
            let mut config =
                OpenAiConfig::from_env().context("failed to load OpenAI config from env")?;
            config.max_output_tokens = budget;
            Ok(Box::new(OpenAiProcessorFactory::new(config).map_err(
                |err| anyhow!("failed to build OpenAI factory: {err}"),
            )?))
        }
        Provider::Gemini => {
            let mut config =
                GeminiConfig::from_env().context("failed to load Gemini config from env")?;
            config.max_output_tokens = budget;
            Ok(Box::new(GeminiProcessorFactory::new(config).map_err(
                |err| anyhow!("failed to build Gemini factory: {err}"),
            )?))
        }
        Provider::Anthropic => {
            let mut config =
                AnthropicConfig::from_env().context("failed to load Anthropic config from env")?;
            config.max_output_tokens = budget;
            Ok(Box::new(AnthropicProcessorFactory::new(config).map_err(
                |err| anyhow!("failed to build Anthropic factory: {err}"),
            )?))
        }
        Provider::Grok => {
            let mut config =
                GrokConfig::from_env().context("failed to load Grok config from env")?;
            config.max_output_tokens = budget;
            Ok(Box::new(GrokProcessorFactory::new(config).map_err(
                |err| anyhow!("failed to build Grok factory: {err}"),
            )?))
        }
    }
}

fn split_input(
    input: &ArtifactProcessorInput,
    label_prefix: &str,
    max_segments: usize,
    overlap: usize,
    max_chars: Option<usize>,
) -> Vec<ArtifactProcessorInput> {
    if input.segments.is_empty() {
        return Vec::new();
    }

    let effective_max_segments = max_segments.max(1);
    let effective_overlap = overlap.min(effective_max_segments.saturating_sub(1));
    let mut chunks = Vec::new();
    let mut start = 0usize;

    while start < input.segments.len() {
        let mut end = start;
        let mut char_count = 0usize;
        while end < input.segments.len() && (end - start) < effective_max_segments {
            let next_chars = input.segments[end].text_content.len();
            let would_exceed_chars = max_chars
                .map(|limit| end > start && char_count + next_chars > limit)
                .unwrap_or(false);
            if would_exceed_chars {
                break;
            }
            char_count += next_chars;
            end += 1;
        }
        if end == start {
            end += 1;
        }

        let first_sequence_no = input.segments[start].sequence_no;
        let last_sequence_no = input.segments[end - 1].sequence_no;
        chunks.push(ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: Some(match &input.title {
                Some(title) => {
                    format!("{title} [{label_prefix} {first_sequence_no}-{last_sequence_no}]")
                }
                None => format!("{label_prefix} {first_sequence_no}-{last_sequence_no}"),
            }),
            participants: input.participants.clone(),
            segments: input.segments[start..end].to_vec(),
        });

        if end == input.segments.len() {
            break;
        }

        let next_start = end.saturating_sub(effective_overlap);
        start = if next_start <= start {
            start + 1
        } else {
            next_start
        };
    }

    chunks
}

fn refine_chunks(
    chunks: Vec<ArtifactProcessorInput>,
    max_segments: usize,
    overlap: usize,
    max_chars: Option<usize>,
) -> Vec<ArtifactProcessorInput> {
    let mut refined = Vec::new();
    for chunk in chunks {
        if chunk.segments.len() <= max_segments
            && max_chars
                .map(|limit| total_chars(&chunk) <= limit)
                .unwrap_or(true)
        {
            refined.push(chunk);
        } else {
            refined.extend(split_input(
                &chunk,
                "subchunk",
                max_segments,
                overlap,
                max_chars,
            ));
        }
    }
    refined
}

fn build_preprocess_coverage_windows(
    artifact_id: &str,
    segments: &[open_archive::storage::LoadedSegment],
    topic_threads: &[TopicThreadRef],
    window_segments: usize,
    window_overlap: usize,
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }

    let covered: HashSet<i32> = topic_threads
        .iter()
        .flat_map(|thread| thread.spans.iter())
        .flat_map(|span| span.start_sequence_no..=span.end_sequence_no)
        .collect();
    let uncovered: Vec<_> = segments
        .iter()
        .filter(|segment| !covered.contains(&segment.sequence_no))
        .cloned()
        .collect();

    build_contiguous_windows(
        artifact_id,
        &uncovered,
        window_segments.max(1),
        window_overlap,
    )
}

fn build_contiguous_windows(
    artifact_id: &str,
    segments: &[open_archive::storage::LoadedSegment],
    window_segments: usize,
    window_overlap: usize,
) -> Vec<ConversationWindowRef> {
    if segments.is_empty() {
        return Vec::new();
    }
    if segments.len() <= window_segments {
        return vec![ConversationWindowRef {
            window_id: format!("{artifact_id}:window:0"),
            label: "coverage fallback".to_string(),
            start_sequence_no: segments
                .first()
                .map(|segment| segment.sequence_no)
                .unwrap_or(0),
            end_sequence_no: segments
                .last()
                .map(|segment| segment.sequence_no)
                .unwrap_or(0),
        }];
    }

    let step = window_segments
        .saturating_sub(window_overlap.min(window_segments.saturating_sub(1)))
        .max(1);
    let mut windows = Vec::new();
    let mut start = 0usize;

    while start < segments.len() {
        let end = (start + window_segments).min(segments.len());
        let first = &segments[start];
        let last = &segments[end - 1];
        windows.push(ConversationWindowRef {
            window_id: format!("{artifact_id}:window:{}", windows.len()),
            label: format!("messages {}-{}", first.sequence_no, last.sequence_no),
            start_sequence_no: first.sequence_no,
            end_sequence_no: last.sequence_no,
        });
        if end == segments.len() {
            break;
        }
        start += step;
    }

    windows
}

fn build_topic_thread_inputs(
    input: &ArtifactProcessorInput,
    topic_threads: &[TopicThreadRef],
    coverage_windows: &[ConversationWindowRef],
) -> Vec<ArtifactProcessorInput> {
    let mut inputs = Vec::new();
    let mut covered = HashSet::new();

    for thread in topic_threads {
        let segments: Vec<_> = input
            .segments
            .iter()
            .filter(|segment| {
                thread.spans.iter().any(|span| {
                    segment.sequence_no >= span.start_sequence_no
                        && segment.sequence_no <= span.end_sequence_no
                })
            })
            .cloned()
            .collect();
        if segments.is_empty() {
            continue;
        }
        for segment in &segments {
            covered.insert(segment.sequence_no);
        }
        inputs.push(ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: Some(match &input.title {
                Some(title) => format!("{title} [{}]", thread.label),
                None => thread.label.clone(),
            }),
            participants: input.participants.clone(),
            segments,
        });
    }

    for window in coverage_windows {
        let segments: Vec<_> = input
            .segments
            .iter()
            .filter(|segment| {
                !covered.contains(&segment.sequence_no)
                    && segment.sequence_no >= window.start_sequence_no
                    && segment.sequence_no <= window.end_sequence_no
            })
            .cloned()
            .collect();
        if segments.is_empty() {
            continue;
        }
        inputs.push(ArtifactProcessorInput {
            artifact_id: input.artifact_id.clone(),
            import_id: input.import_id.clone(),
            source_type: input.source_type,
            title: input.title.clone(),
            participants: input.participants.clone(),
            segments,
        });
    }

    if inputs.is_empty() {
        vec![input.clone()]
    } else {
        inputs
    }
}

fn summarize_chunk(label: &str, input: &ArtifactProcessorInput) -> ChunkSummary {
    ChunkSummary {
        label: label.to_string(),
        segment_count: input.segments.len(),
        char_count: total_chars(input),
        first_sequence_no: input
            .segments
            .first()
            .map(|segment| segment.sequence_no)
            .unwrap_or(0),
        last_sequence_no: input
            .segments
            .last()
            .map(|segment| segment.sequence_no)
            .unwrap_or(0),
    }
}

fn total_chars(input: &ArtifactProcessorInput) -> usize {
    input
        .segments
        .iter()
        .map(|segment| segment.text_content.len())
        .sum()
}

fn pre_merge_metrics(outputs: &[ArtifactProcessorOutput]) -> PreMergeMetrics {
    let unique_evidence_segments = outputs
        .iter()
        .flat_map(all_evidence_segment_ids)
        .collect::<BTreeSet<_>>()
        .len();
    PreMergeMetrics {
        memories: outputs.iter().map(|output| output.memories.len()).sum(),
        classifications: outputs
            .iter()
            .map(|output| output.classifications.len())
            .sum(),
        entities: outputs.iter().map(|output| output.entities.len()).sum(),
        relationships: outputs
            .iter()
            .map(|output| output.relationships.len())
            .sum(),
        retrieval_intents: outputs
            .iter()
            .map(|output| output.retrieval_intents.len())
            .sum(),
        unique_evidence_segments,
    }
}

fn all_evidence_segment_ids(output: &ArtifactProcessorOutput) -> Vec<String> {
    let mut ids = output.summary.evidence_segment_ids.clone();
    for classification in &output.classifications {
        extend_unique(&mut ids, &classification.evidence_segment_ids);
    }
    for memory in &output.memories {
        extend_unique(&mut ids, &memory.evidence_segment_ids);
    }
    for entity in &output.entities {
        extend_unique(&mut ids, &entity.evidence_segment_ids);
    }
    for relationship in &output.relationships {
        extend_unique(&mut ids, &relationship.evidence_segment_ids);
    }
    for intent in &output.retrieval_intents {
        extend_unique(&mut ids, &intent.evidence_segment_ids);
    }
    ids
}

fn print_run_summary(
    provider: Provider,
    budget: u32,
    mode: ProbeMode,
    run: &ModeRun,
    total_segments: usize,
    elapsed_secs: f64,
) {
    let post_evidence = all_evidence_segment_ids(&run.output)
        .into_iter()
        .collect::<BTreeSet<_>>()
        .len();
    let max_chunk_segments = run
        .chunk_summaries
        .iter()
        .map(|chunk| chunk.segment_count)
        .max()
        .unwrap_or(0);
    let max_chunk_chars = run
        .chunk_summaries
        .iter()
        .map(|chunk| chunk.char_count)
        .max()
        .unwrap_or(0);

    println!(
        "  provider={} budget={} mode={} | ok ({:.2}s) | chunks {} max_chunk_segments {} max_chunk_chars {} | memories {} classifications {} entities {} relationships {} | evidence {}/{} ({:.0}%)",
        provider.as_str(),
        budget,
        mode.as_str(),
        elapsed_secs,
        run.chunk_summaries.len(),
        max_chunk_segments,
        max_chunk_chars,
        run.output.memories.len(),
        run.output.classifications.len(),
        run.output.entities.len(),
        run.output.relationships.len(),
        post_evidence,
        total_segments,
        percent(post_evidence, total_segments),
    );
    println!(
        "    pre-merge: memories {} classifications {} entities {} relationships {} intents {} evidence {}",
        run.pre_merge.memories,
        run.pre_merge.classifications,
        run.pre_merge.entities,
        run.pre_merge.relationships,
        run.pre_merge.retrieval_intents,
        run.pre_merge.unique_evidence_segments
    );
    if let Some(preprocess) = &run.preprocess {
        println!(
            "    preprocess: provider={} threads {} coverage_windows {}{}",
            preprocess.provider.as_str(),
            preprocess.topic_thread_count,
            preprocess.coverage_window_count,
            preprocess
                .usage
                .as_ref()
                .map(|usage| format!(" | {}", format_usage(usage)))
                .unwrap_or_default()
        );
    }
    if let Some(usage) = &run.extract_usage {
        println!("    extract usage: {}", format_usage(usage));
    }
}

fn print_chunks(chunks: &[ChunkSummary]) {
    for chunk in chunks {
        println!(
            "    {}: seq {}-{} | segments {} | chars {}",
            chunk.label,
            chunk.first_sequence_no,
            chunk.last_sequence_no,
            chunk.segment_count,
            chunk.char_count
        );
    }
}

fn print_output_details(output: &ArtifactProcessorOutput) {
    println!(
        "    summary: {}",
        output.summary.body_text.replace('\n', " ")
    );
    if output.memories.is_empty() {
        println!("    memories: none");
    } else {
        let memory_titles = output
            .memories
            .iter()
            .map(|memory| {
                memory
                    .title
                    .clone()
                    .unwrap_or_else(|| memory.candidate_key.clone())
            })
            .collect::<Vec<_>>()
            .join(" | ");
        println!("    memories: {}", memory_titles);
    }
}

fn format_usage(usage: &InferenceUsage) -> String {
    let mut fields = Vec::new();
    if let Some(value) = usage.input_tokens {
        fields.push(format!("in={value}"));
    }
    if let Some(value) = usage.output_tokens {
        fields.push(format!("out={value}"));
    }
    if let Some(value) = usage.reasoning_tokens {
        fields.push(format!("reason={value}"));
    }
    if let Some(value) = usage.total_tokens {
        fields.push(format!("total={value}"));
    }
    if let Some(value) = usage.reported_cost_micros {
        fields.push(format!("cost_micros={value}"));
    }
    if fields.is_empty() {
        "usage unavailable".to_string()
    } else {
        fields.join(" ")
    }
}

fn percent(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}

fn combine_usages<'a>(usages: impl Iterator<Item = &'a InferenceUsage>) -> Option<InferenceUsage> {
    let mut total = InferenceUsage {
        input_tokens: Some(0),
        output_tokens: Some(0),
        reasoning_tokens: Some(0),
        total_tokens: Some(0),
        reported_cost_micros: Some(0),
    };
    let mut saw_any = false;

    for usage in usages {
        saw_any = true;
        add_optional(&mut total.input_tokens, usage.input_tokens);
        add_optional(&mut total.output_tokens, usage.output_tokens);
        add_optional(&mut total.reasoning_tokens, usage.reasoning_tokens);
        add_optional(&mut total.total_tokens, usage.total_tokens);
        add_optional(&mut total.reported_cost_micros, usage.reported_cost_micros);
    }

    saw_any.then_some(total)
}

fn add_optional(target: &mut Option<u64>, incoming: Option<u64>) {
    match (*target, incoming) {
        (Some(existing), Some(value)) => *target = Some(existing + value),
        (_, None) => *target = None,
        (None, Some(_)) => {}
    }
}

fn merge_chunk_outputs(
    input: &ArtifactProcessorInput,
    outputs: &[ArtifactProcessorOutput],
) -> ArtifactProcessorOutput {
    let first = outputs
        .first()
        .cloned()
        .expect("merge_chunk_outputs requires at least one chunk output");
    if outputs.len() == 1 {
        return first;
    }

    let mut summary_bodies = Vec::new();
    let mut summary_titles = Vec::new();
    let mut summary_evidence = Vec::new();
    let mut classifications = BTreeMap::<(String, String), ClassificationOutput>::new();
    let mut memories = BTreeMap::<String, MemoryOutput>::new();
    let mut entities = BTreeMap::<String, EntityOutput>::new();
    let mut relationships = BTreeMap::<String, RelationshipOutput>::new();
    let mut retrieval_intents = BTreeMap::<(String, String), RetrievalIntent>::new();
    let mut importance_score = 1u8;
    let mut escalate_to_frontier = false;
    let mut escalation_reasons = Vec::new();

    for output in outputs {
        if let Some(title) = &output.summary.title {
            if !summary_titles.contains(title) {
                summary_titles.push(title.clone());
            }
        }
        if !summary_bodies.contains(&output.summary.body_text) {
            summary_bodies.push(output.summary.body_text.clone());
        }
        extend_unique(&mut summary_evidence, &output.summary.evidence_segment_ids);
        importance_score = importance_score.max(output.importance_score);
        escalate_to_frontier |= output.escalate_to_frontier;
        if let Some(reason) = &output.escalation_reason {
            if !escalation_reasons.contains(reason) {
                escalation_reasons.push(reason.clone());
            }
        }

        for classification in &output.classifications {
            let key = (
                classification.classification_type.clone(),
                classification.classification_value.clone(),
            );
            if let Some(existing) = classifications.get_mut(&key) {
                merge_classification_output(existing, classification);
            } else {
                classifications.insert(key, classification.clone());
            }
        }
        for memory in &output.memories {
            let key = memory_target_key_from_output(memory);
            if let Some(existing) = memories.get_mut(&key) {
                merge_memory_output(existing, memory);
            } else {
                memories.insert(key, memory.clone());
            }
        }
        for entity in &output.entities {
            if let Some(existing) = entities.get_mut(&entity.entity_key) {
                merge_entity_output(existing, entity);
            } else {
                entities.insert(entity.entity_key.clone(), entity.clone());
            }
        }
        for relationship in &output.relationships {
            let key = relationship_target_key_from_output(relationship);
            if let Some(existing) = relationships.get_mut(&key) {
                merge_relationship_output(existing, relationship);
            } else {
                relationships.insert(key, relationship.clone());
            }
        }
        for intent in &output.retrieval_intents {
            let key = (intent.intent_type.clone(), intent.query_text.clone());
            if let Some(existing) = retrieval_intents.get_mut(&key) {
                merge_retrieval_intent(existing, intent);
            } else {
                retrieval_intents.insert(key, intent.clone());
            }
        }
    }

    ArtifactProcessorOutput {
        pipeline_name: first.pipeline_name,
        pipeline_version: first.pipeline_version,
        provider_name: first.provider_name,
        model_name: first.model_name,
        prompt_version: first.prompt_version,
        usage: None,
        summary: open_archive::processor::SummaryOutput {
            title: summary_titles
                .first()
                .cloned()
                .or_else(|| input.title.clone()),
            body_text: summary_bodies.join(" "),
            evidence_segment_ids: summary_evidence,
        },
        classifications: classifications.into_values().collect(),
        memories: memories.into_values().collect(),
        entities: entities.into_values().collect(),
        relationships: relationships.into_values().collect(),
        retrieval_intents: retrieval_intents.into_values().collect(),
        importance_score,
        escalate_to_frontier,
        escalation_reason: if escalate_to_frontier && !escalation_reasons.is_empty() {
            Some(escalation_reasons.join(" "))
        } else {
            None
        },
    }
}

fn memory_target_key_from_output(memory: &MemoryOutput) -> String {
    let title_part = memory.title.as_deref().unwrap_or("");
    let body_prefix: String = memory.body_text.chars().take(48).collect();
    format!(
        "{}:{}:{}:{}:{}",
        memory.memory_type,
        memory.memory_scope.as_str(),
        memory.memory_scope_value,
        title_part,
        body_prefix
    )
}

fn relationship_target_key_from_output(relationship: &RelationshipOutput) -> String {
    format!(
        "{}:{}:{}",
        relationship.relationship_type, relationship.subject_key, relationship.object_key
    )
}

fn extend_unique(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn merge_classification_output(target: &mut ClassificationOutput, incoming: &ClassificationOutput) {
    if incoming.title.as_ref().map(|v| v.len()).unwrap_or(0)
        > target.title.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        target.title = incoming.title.clone();
    }
    if incoming.body_text.as_ref().map(|v| v.len()).unwrap_or(0)
        > target.body_text.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        target.body_text = incoming.body_text.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_memory_output(target: &mut MemoryOutput, incoming: &MemoryOutput) {
    if incoming.title.as_ref().map(|v| v.len()).unwrap_or(0)
        > target.title.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        target.title = incoming.title.clone();
    }
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_entity_output(target: &mut EntityOutput, incoming: &EntityOutput) {
    if incoming.display_name.len() > target.display_name.len() {
        target.display_name = incoming.display_name.clone();
    }
    if incoming.entity_type.len() > target.entity_type.len() {
        target.entity_type = incoming.entity_type.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_relationship_output(target: &mut RelationshipOutput, incoming: &RelationshipOutput) {
    if incoming.title.as_ref().map(|v| v.len()).unwrap_or(0)
        > target.title.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        target.title = incoming.title.clone();
    }
    if incoming.body_text.len() > target.body_text.len() {
        target.body_text = incoming.body_text.clone();
    }
    if confidence_rank(&incoming.confidence_label) > confidence_rank(&target.confidence_label) {
        target.confidence_label = incoming.confidence_label.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn merge_retrieval_intent(target: &mut RetrievalIntent, incoming: &RetrievalIntent) {
    if incoming.question.len() > target.question.len() {
        target.question = incoming.question.clone();
    }
    extend_unique(
        &mut target.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
}

fn confidence_rank(value: &str) -> u8 {
    match value {
        "high" => 3,
        "medium" => 2,
        _ => 1,
    }
}
