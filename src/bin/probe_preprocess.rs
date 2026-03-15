use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use open_archive::config::{GeminiConfig, PostgresConfig};
use open_archive::processor::{ArtifactProcessorFactory, GeminiProcessorFactory, InferenceUsage, PreprocessProcessorInput};
use open_archive::storage::types::{EnrichmentTier, LoadedSegment, SegmentSpanRef, TopicThreadRef};
use open_archive::storage::{ArtifactReadStore, PostgresImportWriteStore};

#[derive(Debug, Parser)]
#[command(name = "probe_preprocess")]
#[command(about = "Replay real imported artifacts against preprocess segmentation models")]
struct Args {
    #[arg(required = true)]
    artifact_ids: Vec<String>,

    #[arg(long = "model")]
    model: Option<String>,

    #[arg(long = "tier", value_enum, default_value_t = ProbeTier::Standard)]
    tier: ProbeTier,

    #[arg(long = "mode", value_enum, default_value_t = ProbeMode::OnePass)]
    mode: ProbeMode,

    #[arg(long = "profile", value_enum, default_value_t = PromptProfile::Baseline)]
    profile: PromptProfile,

    #[arg(long = "show-segments", default_value_t = false)]
    show_segments: bool,

    #[arg(long = "segment-chars", default_value_t = 120)]
    segment_chars: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProbeTier {
    Standard,
    Quality,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProbeMode {
    OnePass,
    TwoPhase,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PromptProfile {
    Baseline,
    StrictIds,
}

impl PromptProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::StrictIds => "strict-ids",
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let gemini = GeminiConfig::from_env().context("failed to load Gemini config from env")?;
    let read_store = PostgresImportWriteStore::new(postgres);

    println!("Preprocess probe");
    println!("Provider: gemini");
    println!("Tier: {}", args.tier.as_str());
    println!("Mode: {}", args.mode.as_str());
    println!("Profile: {}", args.profile.as_str());
    if let Some(model) = &args.model {
        println!("Model override: {model}");
    }
    println!();

    for artifact_id in &args.artifact_ids {
        let loaded = read_store
            .load_artifact_for_enrichment(artifact_id)
            .with_context(|| format!("failed to load artifact {artifact_id}"))?
            .ok_or_else(|| anyhow!("artifact {artifact_id} not found"))?;
        let input = PreprocessProcessorInput {
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

        let started = Instant::now();
        let result = match args.mode {
            ProbeMode::OnePass => run_one_pass(&gemini, &args, &input),
            ProbeMode::TwoPhase => run_two_phase(&gemini, &args, &input),
        };

        match result {
            Ok(output) => {
                println!(
                    "  ok ({:.2}s) | threads {} | spans {} | escalate {}{}",
                    started.elapsed().as_secs_f64(),
                    output.topic_threads.len(),
                    output
                        .phase_one_spans
                        .as_ref()
                        .map(|spans| spans.len())
                        .unwrap_or(0),
                    output.escalate_to_quality,
                    output
                        .usage
                        .as_ref()
                        .map(|usage| format!(" | {}", format_usage(usage)))
                        .unwrap_or_default()
                );
                print_topic_threads(&output.topic_threads);
                if let Some(spans) = &output.phase_one_spans {
                    print_phase_one_spans(spans);
                }
                if args.show_segments {
                    print_segment_assignments(&input, &output.topic_threads, args.segment_chars);
                }
            }
            Err(err) => {
                println!("  err ({:.2}s) | {}", started.elapsed().as_secs_f64(), err);
            }
        }
        println!();
    }

    Ok(())
}

fn run_one_pass(
    gemini: &GeminiConfig,
    args: &Args,
    input: &PreprocessProcessorInput,
) -> Result<ProbeOutput> {
    let mut config = gemini.clone();
    if let Some(model) = &args.model {
        config.standard_model = model.clone();
        config.quality_model = Some(model.clone());
    }
    let factory = GeminiProcessorFactory::new(config)
        .map_err(|err| anyhow!("failed to build Gemini factory: {err}"))?;
    let tier = match args.tier {
        ProbeTier::Standard => EnrichmentTier::Standard,
        ProbeTier::Quality => EnrichmentTier::Quality,
    };
    let processor = factory
        .build_preprocess_processor(tier)
        .map_err(|err| anyhow!("failed to build preprocess processor: {err}"))?;
    let output = processor.segment(input)?;
    Ok(ProbeOutput {
        topic_threads: output.topic_threads,
        escalate_to_quality: output.escalate_to_quality,
        usage: output.usage,
        phase_one_spans: None,
    })
}

fn run_two_phase(
    gemini: &GeminiConfig,
    args: &Args,
    input: &PreprocessProcessorInput,
) -> Result<ProbeOutput> {
    let model = selected_model(gemini, args);
    let client = GeminiProbeClient::new(gemini)?;

    let phase_one_started = Instant::now();
    let phase_one = client.complete_json(
        &model,
        phase_one_system_prompt(args.profile),
        &build_phase_one_user_prompt(input, args.profile)?,
        &phase_one_schema(),
    )?;
    let phase_one_elapsed = phase_one_started.elapsed().as_secs_f64();
    let parsed_phase_one: PhaseOneOutput = serde_json::from_str(&phase_one.output_text)
        .map_err(|source| anyhow!("failed to parse phase-one JSON: {source}: {}", preview(&phase_one.output_text)))?;
    let normalized_phase_one = normalize_phase_one(parsed_phase_one, input)?;

    let phase_two_started = Instant::now();
    let phase_two = client.complete_json(
        &model,
        phase_two_system_prompt(args.profile),
        &build_phase_two_user_prompt(input, &normalized_phase_one, args.profile)?,
        &phase_two_schema(),
    )?;
    let phase_two_elapsed = phase_two_started.elapsed().as_secs_f64();
    let parsed_phase_two: PhaseTwoOutput = serde_json::from_str(&phase_two.output_text)
        .map_err(|source| anyhow!("failed to parse phase-two JSON: {source}: {}", preview(&phase_two.output_text)))?;
    let topic_threads = normalize_phase_two(parsed_phase_two, &normalized_phase_one)?;

    let usage = combine_usage(phase_one.usage, phase_two.usage);
    println!(
        "    phase1 {:.2}s | phase2 {:.2}s",
        phase_one_elapsed, phase_two_elapsed
    );

    Ok(ProbeOutput {
        topic_threads,
        escalate_to_quality: false,
        usage,
        phase_one_spans: Some(normalized_phase_one),
    })
}

fn selected_model(gemini: &GeminiConfig, args: &Args) -> String {
    args.model.clone().unwrap_or_else(|| match args.tier {
        ProbeTier::Standard => gemini.standard_model.clone(),
        ProbeTier::Quality => gemini
            .quality_model
            .clone()
            .unwrap_or_else(|| gemini.standard_model.clone()),
    })
}

#[derive(Debug, Clone)]
struct ProbeOutput {
    topic_threads: Vec<TopicThreadRef>,
    escalate_to_quality: bool,
    usage: Option<InferenceUsage>,
    phase_one_spans: Option<Vec<PhaseOneSpanNormalized>>,
}

#[derive(Debug, Clone)]
struct PhaseOneSpanNormalized {
    span_id: String,
    label: String,
    summary: String,
    focus_key: String,
    confidence_label: String,
    start_evidence_ref: String,
    end_evidence_ref: String,
    start_sequence_no: i32,
    end_sequence_no: i32,
    start_excerpt: String,
    middle_excerpt: String,
    end_excerpt: String,
}

#[derive(Debug, Deserialize)]
struct PhaseOneOutput {
    topic_spans: Vec<PhaseOneSpan>,
}

#[derive(Debug, Deserialize)]
struct PhaseOneSpan {
    span_id: String,
    label: String,
    summary: String,
    focus_key: String,
    confidence_label: String,
    start_evidence_ref: String,
    end_evidence_ref: String,
}

#[derive(Debug, Deserialize)]
struct PhaseTwoOutput {
    topic_threads: Vec<PhaseTwoThread>,
}

#[derive(Debug, Deserialize)]
struct PhaseTwoThread {
    thread_key: String,
    label: String,
    summary: String,
    confidence_label: String,
    spans: Vec<PhaseTwoSpan>,
}

#[derive(Debug, Deserialize)]
struct PhaseTwoSpan {
    start_evidence_ref: String,
    end_evidence_ref: String,
}

fn normalize_phase_one(
    output: PhaseOneOutput,
    input: &PreprocessProcessorInput,
) -> Result<Vec<PhaseOneSpanNormalized>> {
    if output.topic_spans.is_empty() {
        return Err(anyhow!("phase one returned no topic_spans"));
    }
    let alias_map: HashMap<String, i32> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| (segment_alias(index), segment.sequence_no))
        .collect();
    let mut normalized = Vec::with_capacity(output.topic_spans.len());
    for span in output.topic_spans {
        let start = alias_map
            .get(&span.start_evidence_ref)
            .copied()
            .ok_or_else(|| anyhow!("unknown start ref {}", span.start_evidence_ref))?;
        let end = alias_map
            .get(&span.end_evidence_ref)
            .copied()
            .ok_or_else(|| anyhow!("unknown end ref {}", span.end_evidence_ref))?;
        if start > end {
            return Err(anyhow!("phase one span {} start > end", span.span_id));
        }
        normalized.push(PhaseOneSpanNormalized {
            span_id: span.span_id,
            label: span.label,
            summary: span.summary,
            focus_key: span.focus_key,
            confidence_label: span.confidence_label,
            start_evidence_ref: span.start_evidence_ref,
            end_evidence_ref: span.end_evidence_ref,
            start_sequence_no: start,
            end_sequence_no: end,
            start_excerpt: excerpt_for_sequence(input, start),
            middle_excerpt: excerpt_for_sequence(input, start + ((end - start) / 2)),
            end_excerpt: excerpt_for_sequence(input, end),
        });
    }
    normalized.sort_by_key(|span| span.start_sequence_no);
    let normalized = fill_phase_one_gaps(normalized, input);
    ensure_phase_one_covers_artifact(&normalized, &input.segments)?;
    Ok(merge_adjacent_equal_spans(normalized))
}

fn ensure_phase_one_covers_artifact(
    spans: &[PhaseOneSpanNormalized],
    segments: &[LoadedSegment],
) -> Result<()> {
    let mut covered = HashSet::new();
    for span in spans {
        for sequence_no in span.start_sequence_no..=span.end_sequence_no {
            covered.insert(sequence_no);
        }
    }
    for segment in segments {
        if !covered.contains(&segment.sequence_no) {
            return Err(anyhow!(
                "phase one did not cover sequence_no {}",
                segment.sequence_no
            ));
        }
    }
    Ok(())
}

fn fill_phase_one_gaps(
    mut spans: Vec<PhaseOneSpanNormalized>,
    input: &PreprocessProcessorInput,
) -> Vec<PhaseOneSpanNormalized> {
    let covered: HashSet<i32> = spans
        .iter()
        .flat_map(|span| span.start_sequence_no..=span.end_sequence_no)
        .collect();
    for segment in &input.segments {
        if covered.contains(&segment.sequence_no) {
            continue;
        }
        let excerpt = truncate(&segment.text_content.replace('\n', " "), 160);
        spans.push(PhaseOneSpanNormalized {
            span_id: format!("gap-{}", segment.sequence_no),
            label: "Gap recovery".to_string(),
            summary: "Recovered uncovered tail or transition segment.".to_string(),
            focus_key: "gap recovery".to_string(),
            confidence_label: "low".to_string(),
            start_evidence_ref: segment_alias(index_for_sequence(input, segment.sequence_no).unwrap_or(0)),
            end_evidence_ref: segment_alias(index_for_sequence(input, segment.sequence_no).unwrap_or(0)),
            start_sequence_no: segment.sequence_no,
            end_sequence_no: segment.sequence_no,
            start_excerpt: excerpt.clone(),
            middle_excerpt: excerpt.clone(),
            end_excerpt: excerpt,
        });
    }
    spans.sort_by_key(|span| span.start_sequence_no);
    spans
}

fn merge_adjacent_equal_spans(spans: Vec<PhaseOneSpanNormalized>) -> Vec<PhaseOneSpanNormalized> {
    let mut merged: Vec<PhaseOneSpanNormalized> = Vec::new();
    for span in spans {
        if let Some(last) = merged.last_mut() {
            if last.label == span.label && last.end_sequence_no + 1 >= span.start_sequence_no {
                last.end_sequence_no = last.end_sequence_no.max(span.end_sequence_no);
                last.end_evidence_ref = span.end_evidence_ref.clone();
                if span.summary.len() > last.summary.len() {
                    last.summary = span.summary;
                }
                if confidence_rank(&span.confidence_label) > confidence_rank(&last.confidence_label) {
                    last.confidence_label = span.confidence_label;
                }
                continue;
            }
        }
        merged.push(span);
    }
    merged
}

fn normalize_phase_two(
    output: PhaseTwoOutput,
    spans: &[PhaseOneSpanNormalized],
) -> Result<Vec<TopicThreadRef>> {
    if output.topic_threads.is_empty() {
        return Err(anyhow!("phase two returned no topic_threads"));
    }
    let valid_refs: HashMap<&str, i32> = spans
        .iter()
        .flat_map(|span| {
            [
                (span.start_evidence_ref.as_str(), span.start_sequence_no),
                (span.end_evidence_ref.as_str(), span.end_sequence_no),
            ]
        })
        .collect();
    let mut topic_threads = Vec::with_capacity(output.topic_threads.len());
    for thread in output.topic_threads {
        if thread.spans.is_empty() {
            return Err(anyhow!("phase two thread {} has no spans", thread.thread_key));
        }
        let mut ordered = Vec::new();
        for span in &thread.spans {
            let start = valid_refs
                .get(span.start_evidence_ref.as_str())
                .copied()
                .ok_or_else(|| anyhow!("phase two referenced unknown start ref {}", span.start_evidence_ref))?;
            let end = valid_refs
                .get(span.end_evidence_ref.as_str())
                .copied()
                .ok_or_else(|| anyhow!("phase two referenced unknown end ref {}", span.end_evidence_ref))?;
            if start > end {
                return Err(anyhow!(
                    "phase two thread {} has start > end for {} -> {}",
                    thread.thread_key,
                    span.start_evidence_ref,
                    span.end_evidence_ref
                ));
            }
            ordered.push(SegmentSpanRef {
                start_sequence_no: start,
                end_sequence_no: end,
            });
        }
        ordered.sort_by_key(|span| span.start_sequence_no);
        let spans = ordered
            .into_iter()
            .fold(Vec::<SegmentSpanRef>::new(), |mut acc, span| {
                if let Some(last) = acc.last_mut() {
                    if last.end_sequence_no + 1 >= span.start_sequence_no {
                        last.end_sequence_no = last.end_sequence_no.max(span.end_sequence_no);
                        return acc;
                    }
                }
                acc.push(SegmentSpanRef {
                    start_sequence_no: span.start_sequence_no,
                    end_sequence_no: span.end_sequence_no,
                });
                acc
            });
        topic_threads.push(TopicThreadRef {
            thread_id: thread.thread_key,
            label: thread.label,
            summary: thread.summary,
            confidence_label: thread.confidence_label,
            spans,
        });
    }
    Ok(topic_threads)
}

fn build_phase_one_user_prompt(
    input: &PreprocessProcessorInput,
    profile: PromptProfile,
) -> Result<String> {
    #[derive(Serialize)]
    struct PromptSegment<'a> {
        evidence_ref: String,
        sequence_no: i32,
        participant_role: &'a str,
        text: &'a str,
    }

    let prompt_segments: Vec<_> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: segment_alias(index),
            sequence_no: segment.sequence_no,
            participant_role: segment
                .participant_role
                .map(|role| role.as_str())
                .unwrap_or("unknown"),
            text: segment.text_content.as_str(),
        })
        .collect();

    let strict_tail = match profile {
        PromptProfile::Baseline => String::new(),
        PromptProfile::StrictIds => "\nID contract:\n- start_evidence_ref and end_evidence_ref must be copied exactly from the evidence_ref values shown in segments.\n- Never invent, rename, transform, expand, or combine refs.\n- Never output segment ids, artifact ids, sequence numbers, span ids, or strings like segment-*, seg-*, span-*, s1_s2.\n".to_string(),
    };
    Ok(format!(
        "Create contiguous local topic spans for this conversation.\nartifact_id: {}\ntitle: {}\nsegments:\n{}\n{}",
        input.artifact_id,
        input.title.as_deref().unwrap_or(""),
        serde_json::to_string_pretty(&prompt_segments)?,
        strict_tail,
    ))
}

fn build_phase_two_user_prompt(
    input: &PreprocessProcessorInput,
    spans: &[PhaseOneSpanNormalized],
    profile: PromptProfile,
) -> Result<String> {
    #[derive(Serialize)]
    struct PromptSpan<'a> {
        span_id: &'a str,
        label: &'a str,
        summary: &'a str,
        focus_key: &'a str,
        confidence_label: &'a str,
        start_evidence_ref: &'a str,
        end_evidence_ref: &'a str,
        start_sequence_no: i32,
        end_sequence_no: i32,
        start_excerpt: &'a str,
        middle_excerpt: &'a str,
        end_excerpt: &'a str,
    }

    let prompt_spans: Vec<_> = spans
        .iter()
        .map(|span| PromptSpan {
            span_id: span.span_id.as_str(),
            label: span.label.as_str(),
            summary: span.summary.as_str(),
            focus_key: span.focus_key.as_str(),
            confidence_label: span.confidence_label.as_str(),
            start_evidence_ref: span.start_evidence_ref.as_str(),
            end_evidence_ref: span.end_evidence_ref.as_str(),
            start_sequence_no: span.start_sequence_no,
            end_sequence_no: span.end_sequence_no,
            start_excerpt: span.start_excerpt.as_str(),
            middle_excerpt: span.middle_excerpt.as_str(),
            end_excerpt: span.end_excerpt.as_str(),
        })
        .collect();

    let strict_tail = match profile {
        PromptProfile::Baseline => String::new(),
        PromptProfile::StrictIds => {
            let mut allowed_refs = spans
                .iter()
                .flat_map(|span| [span.start_evidence_ref.clone(), span.end_evidence_ref.clone()])
                .collect::<Vec<_>>();
            allowed_refs.sort();
            allowed_refs.dedup();
            format!(
                "ID contract:\n\
- In topic_threads.spans, start_evidence_ref and end_evidence_ref must be copied exactly from the start_evidence_ref/end_evidence_ref values shown in the spans list.\n\
- Allowed refs for this artifact: {:?}\n\
- Never output span_id in those fields.\n\
- Never invent, rename, transform, expand, or combine refs.\n\
- Never output segment ids, artifact ids, sequence numbers, or strings like segment-*, seg-*, span-*, s1_s2.\n\n",
                allowed_refs
            )
        }
    };

    Ok(format!(
        "Merge local topic spans into durable topic threads for this conversation.\n\
artifact_id: {}\n\
title: {}\n\
\n\
Merge criteria:\n\
- Merge spans only when they clearly continue the same specific question, decision, troubleshooting thread, workflow, or entity-specific investigation.\n\
- Use focus_key as a strong identity hint. Same broad domain with different focus_key values should usually stay separate.\n\
- Do NOT merge spans merely because they are in the same broad domain, product area, or subject family.\n\
- Distinguish separate subthreads inside the same domain.\n\
- Prefer keeping two threads separate when unsure.\n\
\n\
{}\n\
spans:\n{}\n",
        input.artifact_id,
        input.title.as_deref().unwrap_or(""),
        strict_tail,
        serde_json::to_string_pretty(&prompt_spans)?,
    ))
}

fn phase_one_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "topic_spans": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "span_id": { "type": "string" },
                        "label": { "type": "string" },
                        "summary": { "type": "string" },
                        "focus_key": { "type": "string" },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "start_evidence_ref": { "type": "string" },
                        "end_evidence_ref": { "type": "string" }
                    },
                    "required": ["span_id", "label", "summary", "focus_key", "confidence_label", "start_evidence_ref", "end_evidence_ref"]
                }
            }
        },
        "required": ["topic_spans"]
    })
}

fn phase_two_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "topic_threads": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "thread_key": { "type": "string" },
                        "label": { "type": "string" },
                        "summary": { "type": "string" },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "spans": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "start_evidence_ref": { "type": "string" },
                                    "end_evidence_ref": { "type": "string" }
                                },
                                "required": ["start_evidence_ref", "end_evidence_ref"]
                            }
                        }
                    },
                    "required": ["thread_key", "label", "summary", "confidence_label", "spans"]
                }
            }
        },
        "required": ["topic_threads"]
    })
}

fn phase_one_system_prompt(profile: PromptProfile) -> &'static str {
    match profile {
        PromptProfile::Baseline => "You are OpenArchive's local segmentation engine. Return ONLY valid JSON.\n\nRules:\n1. Partition the conversation into contiguous topic_spans.\n2. Every segment must be covered exactly once.\n3. Keep spans local and contiguous. Do not merge non-contiguous returns in this phase.\n4. Split when the active question, decision, troubleshooting target, work item, or named subject changes in a meaningful way.\n5. Do not create tiny spans for brief clarifications unless the thread actually changes.\n6. Labels must be short and specific.\n7. Summaries must be one short sentence.\n8. focus_key must be a short phrase naming the concrete thread target, such as the specific question, workflow, entity, or decision under discussion.\n9. Output valid JSON only.",
        PromptProfile::StrictIds => "You are OpenArchive's local segmentation engine. Return ONLY valid JSON.\n\nRules:\n1. Partition the conversation into contiguous topic_spans.\n2. Every segment must be covered exactly once.\n3. Keep spans local and contiguous. Do not merge non-contiguous returns in this phase.\n4. Split when the active question, decision, troubleshooting target, work item, or named subject changes in a meaningful way.\n5. Do not create tiny spans for brief clarifications unless the thread actually changes.\n6. Labels must be short and specific.\n7. Summaries must be one short sentence.\n8. focus_key must be a short phrase naming the concrete thread target, such as the specific question, workflow, entity, or decision under discussion.\n9. start_evidence_ref and end_evidence_ref must be copied exactly from the provided evidence_ref strings.\n10. Never invent, transform, expand, or combine ids.\n11. Never output values like segment-*, seg-*, span-*, artifact ids, or sequence numbers in evidence ref fields.\n12. Output valid JSON only.",
    }
}

fn phase_two_system_prompt(profile: PromptProfile) -> &'static str {
    match profile {
        PromptProfile::Baseline => "You are OpenArchive's thread consolidation engine. Return ONLY valid JSON.\n\nRules:\n1. Merge local spans into durable topic_threads.\n2. Revisited topics should reuse one thread with multiple spans.\n3. Do not merge spans on broad domain overlap alone.\n4. Only merge when the later span clearly resumes the same specific thread of work, question, workflow, or investigation.\n5. Treat focus_key as a strong identity signal; different focus_key values usually imply different threads unless the excerpts show a clear return to the same exact thread.\n6. Prefer separate threads when unsure.\n7. Summaries must be one short sentence.\n8. Output valid JSON only.",
        PromptProfile::StrictIds => "You are OpenArchive's thread consolidation engine. Return ONLY valid JSON.\n\nRules:\n1. Merge local spans into durable topic_threads.\n2. Revisited topics should reuse one thread with multiple spans.\n3. Do not merge spans on broad domain overlap alone.\n4. Only merge when the later span clearly resumes the same specific thread of work, question, workflow, or investigation.\n5. Treat focus_key as a strong identity signal; different focus_key values usually imply different threads unless the excerpts show a clear return to the same exact thread.\n6. Prefer separate threads when unsure.\n7. In topic_threads.spans, start_evidence_ref and end_evidence_ref must be copied exactly from the provided spans.\n8. Never output span_id in evidence ref fields.\n9. Never invent, transform, expand, or combine ids.\n10. Never output values like segment-*, seg-*, span-*, artifact ids, or sequence numbers in evidence ref fields.\n11. Summaries must be one short sentence.\n12. Output valid JSON only.",
    }
}

fn print_topic_threads(topic_threads: &[TopicThreadRef]) {
    if topic_threads.is_empty() {
        println!("  topic_threads: none");
        return;
    }

    for thread in topic_threads {
        let spans = thread
            .spans
            .iter()
            .map(|span| format!("{}-{}", span.start_sequence_no, span.end_sequence_no))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "  - {} [{}] {} | spans: {}",
            thread.label, thread.confidence_label, thread.summary, spans
        );
    }
}

fn print_phase_one_spans(spans: &[PhaseOneSpanNormalized]) {
    println!("    phase-one spans:");
    for span in spans {
        println!(
            "    - {} [{}] {} | focus={} | {}-{}",
            span.label,
            span.confidence_label,
            span.summary,
            span.focus_key,
            span.start_sequence_no,
            span.end_sequence_no
        );
    }
}

fn print_segment_assignments(
    input: &PreprocessProcessorInput,
    topic_threads: &[TopicThreadRef],
    segment_chars: usize,
) {
    let mut assignments: BTreeMap<i32, Vec<&str>> = BTreeMap::new();
    for thread in topic_threads {
        for span in &thread.spans {
            for sequence_no in span.start_sequence_no..=span.end_sequence_no {
                assignments.entry(sequence_no).or_default().push(thread.label.as_str());
            }
        }
    }

    for segment in &input.segments {
        let labels = assignments
            .get(&segment.sequence_no)
            .cloned()
            .unwrap_or_default()
            .join(", ");
        let text = truncate(&segment.text_content.replace('\n', " "), segment_chars);
        println!(
            "    {:>3} | {:<30} | {}",
            segment.sequence_no,
            if labels.is_empty() { "(unassigned)" } else { labels.as_str() },
            text
        );
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut truncated = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        truncated.push_str("...");
    }
    truncated
}

fn excerpt_for_sequence(input: &PreprocessProcessorInput, sequence_no: i32) -> String {
    input
        .segments
        .iter()
        .find(|segment| segment.sequence_no == sequence_no)
        .map(|segment| truncate(&segment.text_content.replace('\n', " "), 160))
        .unwrap_or_default()
}

fn segment_alias(index: usize) -> String {
    format!("s{}", index + 1)
}

fn index_for_sequence(input: &PreprocessProcessorInput, sequence_no: i32) -> Option<usize> {
    input
        .segments
        .iter()
        .position(|segment| segment.sequence_no == sequence_no)
}

fn confidence_rank(value: &str) -> u8 {
    match value {
        "high" => 3,
        "medium" => 2,
        _ => 1,
    }
}

fn combine_usage(
    first: Option<InferenceUsage>,
    second: Option<InferenceUsage>,
) -> Option<InferenceUsage> {
    match (first, second) {
        (None, None) => None,
        (first, second) => Some(InferenceUsage {
            input_tokens: sum_option(first.as_ref().and_then(|u| u.input_tokens), second.as_ref().and_then(|u| u.input_tokens)),
            output_tokens: sum_option(first.as_ref().and_then(|u| u.output_tokens), second.as_ref().and_then(|u| u.output_tokens)),
            reasoning_tokens: sum_option(first.as_ref().and_then(|u| u.reasoning_tokens), second.as_ref().and_then(|u| u.reasoning_tokens)),
            total_tokens: sum_option(first.as_ref().and_then(|u| u.total_tokens), second.as_ref().and_then(|u| u.total_tokens)),
            reported_cost_micros: sum_option(
                first.as_ref().and_then(|u| u.reported_cost_micros),
                second.as_ref().and_then(|u| u.reported_cost_micros),
            ),
        }),
    }
}

fn sum_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, None) => None,
        (left, right) => Some(left.unwrap_or(0) + right.unwrap_or(0)),
    }
}

fn format_usage(usage: &InferenceUsage) -> String {
    let mut parts = Vec::new();
    if let Some(input_tokens) = usage.input_tokens {
        parts.push(format!("input_tokens={input_tokens}"));
    }
    if let Some(output_tokens) = usage.output_tokens {
        parts.push(format!("output_tokens={output_tokens}"));
    }
    if let Some(reasoning_tokens) = usage.reasoning_tokens {
        parts.push(format!("reasoning_tokens={reasoning_tokens}"));
    }
    if let Some(total_tokens) = usage.total_tokens {
        parts.push(format!("total_tokens={total_tokens}"));
    }
    parts.join(", ")
}

fn preview(value: &str) -> String {
    const MAX: usize = 240;
    let mut preview = value.chars().take(MAX).collect::<String>();
    if value.chars().count() > MAX {
        preview.push_str("...");
    }
    preview
}

struct GeminiProbeClient {
    client: Client,
    base_url: String,
    max_output_tokens: u32,
}

impl GeminiProbeClient {
    fn new(config: &GeminiConfig) -> Result<Self> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header = HeaderName::from_static("x-goog-api-key");
        let api_key_value = HeaderValue::from_str(&config.api_key)
            .map_err(|err| anyhow!("invalid Gemini API key header: {err}"))?;
        default_headers.insert(api_key_header, api_key_value);

        let client = Client::builder()
            .default_headers(default_headers)
            .build()
            .context("failed to build Gemini HTTP client")?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            max_output_tokens: config.max_output_tokens,
        })
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<GeminiProbeResponse> {
        let endpoint = format!(
            "{}/models/{}:generateContent",
            self.base_url,
            model.trim_start_matches("models/")
        );
        let body = serde_json::json!({
            "systemInstruction": {
                "parts": [{ "text": system_prompt }]
            },
            "contents": [{
                "role": "user",
                "parts": [{ "text": user_prompt }]
            }],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": normalize_gemini_schema(schema),
                "maxOutputTokens": self.max_output_tokens
            }
        });
        let response = self
            .client
            .post(endpoint)
            .body(serde_json::to_vec(&body).context("failed to serialize Gemini request body")?)
            .send()
            .context("failed to send inference request")?;
        let status = response.status();
        let response_text = response.text().context("failed to read inference response")?;
        if !status.is_success() {
            return Err(anyhow!(
                "inference returned HTTP status {}: {}",
                status.as_u16(),
                preview(&response_text)
            ));
        }
        let parsed: GeminiGenerateContentResponse = serde_json::from_str(&response_text)
            .with_context(|| format!("failed to parse Gemini response: {}", preview(&response_text)))?;
        let output_text = parsed
            .flatten_text()
            .ok_or_else(|| anyhow!("Gemini returned empty content"))?;
        Ok(GeminiProbeResponse {
            output_text,
            usage: parsed
                .usage_metadata
                .as_ref()
                .map(InferenceUsage::from),
        })
    }
}

fn normalize_gemini_schema(schema: &serde_json::Value) -> serde_json::Value {
    match schema {
        serde_json::Value::Object(map) => {
            let mut sanitized = serde_json::Map::new();
            for (key, child) in map {
                if matches!(
                    key.as_str(),
                    "additionalProperties"
                        | "strict"
                        | "name"
                        | "schema"
                        | "minLength"
                        | "maxLength"
                        | "minItems"
                        | "maxItems"
                        | "minimum"
                        | "maximum"
                        | "pattern"
                        | "default"
                        | "title"
                        | "description"
                ) {
                    continue;
                }
                if key == "type" {
                    sanitized.insert(
                        key.clone(),
                        serde_json::Value::String(
                            child
                                .as_str()
                                .unwrap_or("object")
                                .to_ascii_uppercase(),
                        ),
                    );
                } else {
                    sanitized.insert(key.clone(), normalize_gemini_schema(child));
                }
            }
            serde_json::Value::Object(sanitized)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(normalize_gemini_schema).collect())
        }
        _ => schema.clone(),
    }
}

#[derive(Debug)]
struct GeminiProbeResponse {
    output_text: String,
    usage: Option<InferenceUsage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    usage_metadata: Option<GeminiUsageMetadata>,
}

impl GeminiGenerateContentResponse {
    fn flatten_text(&self) -> Option<String> {
        self.candidates
            .iter()
            .filter_map(|candidate| candidate.content.as_ref())
            .flat_map(|content| content.parts.iter())
            .filter_map(|part| part.text.as_deref())
            .find(|text| !text.trim().is_empty())
            .map(str::to_string)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiContent>,
}

#[derive(Debug, Deserialize)]
struct GeminiContent {
    #[serde(default)]
    parts: Vec<GeminiTextPart>,
}

#[derive(Debug, Deserialize)]
struct GeminiTextPart {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiUsageMetadata {
    #[serde(default)]
    prompt_token_count: Option<u64>,
    #[serde(default)]
    candidates_token_count: Option<u64>,
    #[serde(default)]
    thoughts_token_count: Option<u64>,
    #[serde(default)]
    total_token_count: Option<u64>,
}

impl From<&GeminiUsageMetadata> for InferenceUsage {
    fn from(value: &GeminiUsageMetadata) -> Self {
        Self {
            input_tokens: value.prompt_token_count,
            output_tokens: value.candidates_token_count,
            reasoning_tokens: value.thoughts_token_count,
            total_tokens: value.total_token_count,
            reported_cost_micros: None,
        }
    }
}

impl ProbeTier {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Quality => "quality",
        }
    }
}

impl ProbeMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::OnePass => "one-pass",
            Self::TwoPhase => "two-phase",
        }
    }
}
