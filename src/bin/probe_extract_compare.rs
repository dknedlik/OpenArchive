use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use clap::Parser;

use open_archive::config::{GeminiConfig, PostgresConfig};
use open_archive::processor::{
    ArtifactProcessorInput, ArtifactProcessorOutput, ClassificationOutput, EntityOutput,
    MemoryOutput, PreprocessProcessorInput, RelationshipOutput, SummaryOutput,
};
use open_archive::storage::types::{RetrievalIntent, SegmentSpanRef, TopicThreadRef};
use open_archive::storage::{ArtifactReadStore, PostgresImportWriteStore};

#[derive(Debug, Parser)]
#[command(name = "probe_extract_compare")]
#[command(about = "Compare enrichment extraction with and without preprocess segmentation")]
struct Args {
    #[arg(required = true)]
    artifact_ids: Vec<String>,

    #[arg(long = "model")]
    model: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let gemini = GeminiConfig::from_env().context("failed to load Gemini config from env")?;
    let read_store = PostgresImportWriteStore::new(postgres);

    println!("Extraction compare");
    println!("Provider: gemini");
    if let Some(model) = &args.model {
        println!("Model override: {model}");
    }
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

        println!(
            "Artifact: {} | title: {} | segments: {}",
            artifact_id,
            extract_input.title.as_deref().unwrap_or(""),
            extract_input.segments.len()
        );

        let baseline =
            run_extract(&gemini, &args, &extract_input).context("baseline extraction failed")?;
        let preprocess_output = run_two_phase_preprocess(&gemini, &args, &preprocess_input)
            .context("two-phase preprocess failed")?;
        let chunk_inputs =
            build_topic_thread_inputs(&extract_input, &preprocess_output.topic_threads);
        let mut chunk_outputs = Vec::new();
        for chunk_input in &chunk_inputs {
            let output = run_extract(&gemini, &args, chunk_input).with_context(|| {
                format!(
                    "segmented extraction failed for chunk {:?}",
                    chunk_input.title
                )
            })?;
            chunk_outputs.push(output);
        }
        let segmented = merge_chunk_outputs(&extract_input, &chunk_outputs);

        println!("  Baseline:");
        print_extract_summary(&baseline);
        println!("  With Preprocess:");
        print_extract_summary(&segmented);
        println!();
    }

    Ok(())
}

fn run_extract(
    gemini: &GeminiConfig,
    args: &Args,
    input: &ArtifactProcessorInput,
) -> Result<ArtifactProcessorOutput> {
    let model = args
        .model
        .clone()
        .unwrap_or_else(|| gemini.standard_model.clone());
    let client = probe_preprocess_client(gemini)?;
    let body = client.complete_json(
        &model,
        GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT,
        &build_extract_user_prompt(input)?,
        &extract_schema(),
    )?;
    let parsed: ExtractModelOutput = serde_json::from_str(&body).map_err(|source| {
        anyhow!(
            "failed to parse extraction JSON: {source}: {}",
            preview(&body)
        )
    })?;
    Ok(parsed.into_output())
}

fn print_extract_summary(output: &ArtifactProcessorOutput) {
    println!(
        "    summary: {}",
        truncate(&output.summary.body_text.replace('\n', " "), 220)
    );
    println!(
        "    classifications={} memories={} entities={} relationships={} retrieval_intents={} importance={}",
        output.classifications.len(),
        output.memories.len(),
        output.entities.len(),
        output.relationships.len(),
        output.retrieval_intents.len(),
        output.importance_score
    );
    if !output.classifications.is_empty() {
        let values = output
            .classifications
            .iter()
            .map(|item| format!("{}={}", item.classification_type, item.classification_value))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("    class: {}", truncate(&values, 220));
    }
    if !output.memories.is_empty() {
        let values = output
            .memories
            .iter()
            .map(|item| {
                item.title
                    .clone()
                    .unwrap_or_else(|| item.memory_type.clone())
            })
            .collect::<Vec<_>>()
            .join(" | ");
        println!("    memories: {}", truncate(&values, 220));
    }
    if !output.retrieval_intents.is_empty() {
        let values = output
            .retrieval_intents
            .iter()
            .map(|item| format!("{}:{}", item.intent_type, item.query_text))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("    intents: {}", truncate(&values, 280));
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    let mut truncated = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        truncated.push_str("...");
    }
    truncated
}

#[derive(Debug, Clone)]
struct PreprocessResult {
    topic_threads: Vec<TopicThreadRef>,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractModelOutput {
    summary: ExtractSummary,
    #[serde(default)]
    classifications: Vec<ExtractClassification>,
    #[serde(default)]
    memories: Vec<ExtractMemory>,
    #[serde(default)]
    entities: Vec<ExtractEntity>,
    #[serde(default)]
    relationships: Vec<ExtractRelationship>,
    #[serde(default)]
    retrieval_intents: Vec<ExtractRetrievalIntent>,
    #[serde(default = "default_importance_score")]
    importance_score: u8,
    #[serde(default)]
    escalate_to_frontier: bool,
    #[serde(default)]
    escalation_reason: String,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractSummary {
    #[serde(default)]
    title: Option<String>,
    body_text: String,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractClassification {
    classification_type: String,
    classification_value: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    body_text: Option<String>,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractMemory {
    memory_type: String,
    memory_scope: open_archive::storage::types::ScopeType,
    memory_scope_value: String,
    #[serde(default)]
    title: Option<String>,
    body_text: String,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractEntity {
    entity_key: String,
    display_name: String,
    entity_type: String,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractRelationship {
    relationship_type: String,
    subject_key: String,
    object_key: String,
    #[serde(default)]
    title: Option<String>,
    body_text: String,
    confidence_label: String,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ExtractRetrievalIntent {
    question: String,
    query_text: String,
    intent_type: String,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

impl ExtractModelOutput {
    fn into_output(self) -> ArtifactProcessorOutput {
        ArtifactProcessorOutput {
            pipeline_name: "gemini_enrichment".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some("gemini".to_string()),
            model_name: None,
            prompt_version: Some("gemini-strict-v5".to_string()),
            usage: None,
            summary: SummaryOutput {
                title: self.summary.title,
                body_text: self.summary.body_text,
                evidence_segment_ids: self.summary.evidence_segment_ids,
            },
            classifications: self
                .classifications
                .into_iter()
                .map(|item| ClassificationOutput {
                    title: item.title,
                    body_text: item.body_text,
                    classification_type: item.classification_type,
                    classification_value: item.classification_value,
                    evidence_segment_ids: item.evidence_segment_ids,
                })
                .collect(),
            memories: self
                .memories
                .into_iter()
                .map(|item| MemoryOutput {
                    title: item.title,
                    body_text: item.body_text,
                    memory_type: item.memory_type,
                    memory_scope: item.memory_scope,
                    memory_scope_value: item.memory_scope_value,
                    evidence_segment_ids: item.evidence_segment_ids,
                })
                .collect(),
            entities: self
                .entities
                .into_iter()
                .map(|item| EntityOutput {
                    entity_key: item.entity_key,
                    display_name: item.display_name,
                    entity_type: item.entity_type,
                    evidence_segment_ids: item.evidence_segment_ids,
                })
                .collect(),
            relationships: self
                .relationships
                .into_iter()
                .map(|item| RelationshipOutput {
                    relationship_type: item.relationship_type,
                    subject_key: item.subject_key,
                    object_key: item.object_key,
                    title: item.title,
                    body_text: item.body_text,
                    confidence_label: item.confidence_label,
                    evidence_segment_ids: item.evidence_segment_ids,
                })
                .collect(),
            retrieval_intents: self
                .retrieval_intents
                .into_iter()
                .map(|item| RetrievalIntent {
                    intent_id: format!("probe:{}", item.query_text),
                    question: item.question,
                    query_text: item.query_text,
                    intent_type: item.intent_type,
                    evidence_segment_ids: item.evidence_segment_ids,
                })
                .collect(),
            importance_score: self.importance_score,
            escalate_to_frontier: self.escalate_to_frontier,
            escalation_reason: if self.escalate_to_frontier
                && !self.escalation_reason.trim().is_empty()
            {
                Some(self.escalation_reason)
            } else {
                None
            },
        }
    }
}

fn default_importance_score() -> u8 {
    1
}

#[derive(Debug, Clone)]
struct PhaseOneSpanNormalized {
    span_id: String,
    label: String,
    summary: String,
    focus_key: String,
    confidence_label: String,
    start_sequence_no: i32,
    end_sequence_no: i32,
    start_excerpt: String,
    middle_excerpt: String,
    end_excerpt: String,
}

#[derive(Debug, serde::Deserialize)]
struct PhaseOneOutput {
    topic_spans: Vec<PhaseOneSpan>,
}

#[derive(Debug, serde::Deserialize)]
struct PhaseOneSpan {
    span_id: String,
    label: String,
    summary: String,
    focus_key: String,
    confidence_label: String,
    start_evidence_ref: String,
    end_evidence_ref: String,
}

#[derive(Debug, serde::Deserialize)]
struct PhaseTwoOutput {
    topic_threads: Vec<PhaseTwoThread>,
}

#[derive(Debug, serde::Deserialize)]
struct PhaseTwoThread {
    thread_id: String,
    label: String,
    summary: String,
    confidence_label: String,
    span_ids: Vec<String>,
}

fn run_two_phase_preprocess(
    gemini: &GeminiConfig,
    args: &Args,
    input: &PreprocessProcessorInput,
) -> Result<PreprocessResult> {
    let model = args
        .model
        .clone()
        .unwrap_or_else(|| gemini.standard_model.clone());
    let client = probe_preprocess_client(gemini)?;

    let phase_one = client.complete_json(
        &model,
        TWO_PHASE_SEGMENT_SYSTEM_PROMPT,
        &build_phase_one_user_prompt(input)?,
        &phase_one_schema(),
    )?;
    let parsed_phase_one: PhaseOneOutput = serde_json::from_str(&phase_one).map_err(|source| {
        anyhow!(
            "failed to parse phase-one JSON: {source}: {}",
            preview(&phase_one)
        )
    })?;
    let normalized_phase_one = normalize_phase_one(parsed_phase_one, input)?;

    let phase_two = client.complete_json(
        &model,
        TWO_PHASE_CONSOLIDATE_SYSTEM_PROMPT,
        &build_phase_two_user_prompt(input, &normalized_phase_one)?,
        &phase_two_schema(),
    )?;
    let parsed_phase_two: PhaseTwoOutput = serde_json::from_str(&phase_two).map_err(|source| {
        anyhow!(
            "failed to parse phase-two JSON: {source}: {}",
            preview(&phase_two)
        )
    })?;
    let topic_threads = normalize_phase_two(parsed_phase_two, &normalized_phase_one)?;

    Ok(PreprocessResult { topic_threads })
}

fn build_topic_thread_inputs(
    input: &ArtifactProcessorInput,
    topic_threads: &[TopicThreadRef],
) -> Vec<ArtifactProcessorInput> {
    let mut inputs = Vec::new();
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
    if inputs.is_empty() {
        inputs.push(input.clone());
    }
    inputs
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
        summary: SummaryOutput {
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

fn extend_unique(target: &mut Vec<String>, incoming: &[String]) {
    for value in incoming {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

fn merge_classification_output(
    existing: &mut ClassificationOutput,
    incoming: &ClassificationOutput,
) {
    extend_unique(
        &mut existing.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
    if existing.body_text.as_ref().map(|v| v.len()).unwrap_or(0)
        < incoming.body_text.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        existing.body_text = incoming.body_text.clone();
    }
    if existing.title.as_ref().map(|v| v.len()).unwrap_or(0)
        < incoming.title.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        existing.title = incoming.title.clone();
    }
}

fn merge_memory_output(existing: &mut MemoryOutput, incoming: &MemoryOutput) {
    extend_unique(
        &mut existing.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
    if incoming.body_text.len() > existing.body_text.len() {
        existing.body_text = incoming.body_text.clone();
    }
    if existing.title.as_ref().map(|v| v.len()).unwrap_or(0)
        < incoming.title.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        existing.title = incoming.title.clone();
    }
}

fn merge_entity_output(existing: &mut EntityOutput, incoming: &EntityOutput) {
    extend_unique(
        &mut existing.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
    if incoming.display_name.len() > existing.display_name.len() {
        existing.display_name = incoming.display_name.clone();
    }
}

fn merge_relationship_output(existing: &mut RelationshipOutput, incoming: &RelationshipOutput) {
    extend_unique(
        &mut existing.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
    if incoming.body_text.len() > existing.body_text.len() {
        existing.body_text = incoming.body_text.clone();
    }
    if confidence_rank(&incoming.confidence_label) > confidence_rank(&existing.confidence_label) {
        existing.confidence_label = incoming.confidence_label.clone();
    }
    if existing.title.as_ref().map(|v| v.len()).unwrap_or(0)
        < incoming.title.as_ref().map(|v| v.len()).unwrap_or(0)
    {
        existing.title = incoming.title.clone();
    }
}

fn merge_retrieval_intent(existing: &mut RetrievalIntent, incoming: &RetrievalIntent) {
    extend_unique(
        &mut existing.evidence_segment_ids,
        &incoming.evidence_segment_ids,
    );
    if incoming.question.len() > existing.question.len() {
        existing.question = incoming.question.clone();
    }
}

fn memory_target_key_from_output(memory: &MemoryOutput) -> String {
    format!(
        "{}:{}:{}",
        memory.memory_type,
        memory.memory_scope.as_str(),
        memory.memory_scope_value
    )
}

fn relationship_target_key_from_output(relationship: &RelationshipOutput) -> String {
    format!(
        "{}:{}:{}",
        relationship.relationship_type, relationship.subject_key, relationship.object_key
    )
}

fn confidence_rank(value: &str) -> u8 {
    match value {
        "high" => 3,
        "medium" => 2,
        _ => 1,
    }
}

fn normalize_phase_one(
    output: PhaseOneOutput,
    input: &PreprocessProcessorInput,
) -> Result<Vec<PhaseOneSpanNormalized>> {
    let alias_map: HashMap<String, i32> = input
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| (segment_alias(index), segment.sequence_no))
        .collect();
    let mut normalized = Vec::new();
    for span in output.topic_spans {
        let start = alias_map
            .get(&span.start_evidence_ref)
            .copied()
            .ok_or_else(|| anyhow!("unknown start ref {}", span.start_evidence_ref))?;
        let end = alias_map
            .get(&span.end_evidence_ref)
            .copied()
            .ok_or_else(|| anyhow!("unknown end ref {}", span.end_evidence_ref))?;
        normalized.push(PhaseOneSpanNormalized {
            span_id: span.span_id,
            label: span.label,
            summary: span.summary,
            focus_key: span.focus_key,
            confidence_label: span.confidence_label,
            start_sequence_no: start,
            end_sequence_no: end,
            start_excerpt: excerpt_for_sequence(input, start),
            middle_excerpt: excerpt_for_sequence(input, start + ((end - start) / 2)),
            end_excerpt: excerpt_for_sequence(input, end),
        });
    }
    normalized.sort_by_key(|span| span.start_sequence_no);
    Ok(fill_phase_one_gaps(normalized, input))
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

fn normalize_phase_two(
    output: PhaseTwoOutput,
    spans: &[PhaseOneSpanNormalized],
) -> Result<Vec<TopicThreadRef>> {
    let span_map: HashMap<&str, &PhaseOneSpanNormalized> = spans
        .iter()
        .map(|span| (span.span_id.as_str(), span))
        .collect();
    let mut topic_threads = Vec::new();
    for thread in output.topic_threads {
        let mut ordered = Vec::new();
        for span_id in &thread.span_ids {
            let span = span_map
                .get(span_id.as_str())
                .copied()
                .ok_or_else(|| anyhow!("phase two referenced unknown span_id {}", span_id))?;
            ordered.push(span);
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
            thread_id: thread.thread_id,
            label: thread.label,
            summary: thread.summary,
            confidence_label: thread.confidence_label,
            spans,
        });
    }
    Ok(topic_threads)
}

fn build_phase_one_user_prompt(input: &PreprocessProcessorInput) -> Result<String> {
    #[derive(serde::Serialize)]
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
    Ok(format!(
        "Create contiguous local topic spans for this conversation.\nartifact_id: {}\ntitle: {}\nsegments:\n{}\n",
        input.artifact_id,
        input.title.as_deref().unwrap_or(""),
        serde_json::to_string_pretty(&prompt_segments)?,
    ))
}

fn build_extract_user_prompt(input: &ArtifactProcessorInput) -> Result<String> {
    #[derive(serde::Serialize)]
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
    Ok(format!(
        "Extract durable knowledge from this artifact.\nartifact_id: {}\ntitle: {}\nsegments:\n{}\n",
        input.artifact_id,
        input.title.as_deref().unwrap_or(""),
        serde_json::to_string_pretty(&prompt_segments)?,
    ))
}

fn build_phase_two_user_prompt(
    input: &PreprocessProcessorInput,
    spans: &[PhaseOneSpanNormalized],
) -> Result<String> {
    #[derive(serde::Serialize)]
    struct PromptSpan<'a> {
        span_id: &'a str,
        label: &'a str,
        summary: &'a str,
        focus_key: &'a str,
        confidence_label: &'a str,
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
            start_sequence_no: span.start_sequence_no,
            end_sequence_no: span.end_sequence_no,
            start_excerpt: span.start_excerpt.as_str(),
            middle_excerpt: span.middle_excerpt.as_str(),
            end_excerpt: span.end_excerpt.as_str(),
        })
        .collect();
    Ok(format!(
        "Merge local topic spans into durable topic threads for this conversation.\nartifact_id: {}\ntitle: {}\n\nMerge criteria:\n- Merge spans only when they clearly continue the same specific question, decision, troubleshooting thread, workflow, or entity-specific investigation.\n- Use focus_key as a strong identity hint. Same broad domain with different focus_key values should usually stay separate.\n- Do NOT merge spans merely because they are in the same broad domain, product area, or subject family.\n- Distinguish separate subthreads inside the same domain.\n- Prefer keeping two threads separate when unsure.\n- Every span_id must appear exactly once.\n\nspans:\n{}\n",
        input.artifact_id,
        input.title.as_deref().unwrap_or(""),
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
                        "thread_id": { "type": "string" },
                        "label": { "type": "string" },
                        "summary": { "type": "string" },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "span_ids": { "type": "array", "items": { "type": "string" } }
                    },
                    "required": ["thread_id", "label", "summary", "confidence_label", "span_ids"]
                }
            }
        },
        "required": ["topic_threads"]
    })
}

const TWO_PHASE_SEGMENT_SYSTEM_PROMPT: &str = "You are OpenArchive's local segmentation engine. Return ONLY valid JSON.\n\nRules:\n1. Partition the conversation into contiguous topic_spans.\n2. Every segment must be covered exactly once.\n3. Keep spans local and contiguous. Do not merge non-contiguous returns in this phase.\n4. Split when the active question, decision, troubleshooting target, work item, or named subject changes in a meaningful way.\n5. Do not create tiny spans for brief clarifications unless the thread actually changes.\n6. Labels must be short and specific.\n7. Summaries must be one short sentence.\n8. focus_key must be a short phrase naming the concrete thread target, such as the specific question, workflow, entity, or decision under discussion.\n9. Output valid JSON only.";

const TWO_PHASE_CONSOLIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's thread consolidation engine. Return ONLY valid JSON.\n\nRules:\n1. Merge local spans into durable topic_threads.\n2. Revisited topics should reuse one thread with multiple span_ids.\n3. Every span_id must appear exactly once.\n4. Do not merge spans on broad domain overlap alone.\n5. Only merge when the later span clearly resumes the same specific thread of work, question, workflow, or investigation.\n6. Treat focus_key as a strong identity signal; different focus_key values usually imply different threads unless the excerpts show a clear return to the same exact thread.\n7. Prefer separate threads when unsure.\n8. Summaries must be one short sentence.\n9. Output valid JSON only.";

const GEMINI_ARTIFACT_EXTRACTION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict extraction engine for Gemini. Read one artifact and return ONLY valid JSON.\n\nReturn exactly these sections:\n- summary\n- classifications\n- memories\n- entities\n- relationships\n- retrieval_intents\n- importance_score\n- escalate_to_frontier\n- escalation_reason\n\nRules:\n1. Output valid JSON only. No markdown or extra text.\n2. Every emitted item must cite real evidence_segment_ids from the artifact.\n3. Do not invent facts, intentions, preferences, identities, entities, or commitments.\n4. Keep the output compact, but do not overcompress rich artifacts.\n5. Use retrieval_intents to ask for archive lookups when prior-state matching, contradiction checks, or duplicate detection matter.\n6. Do not guess prior continuity; emit retrieval_intents instead.\n7. Emit relationships only when the artifact explicitly supports the link.\n8. relationship confidence_label must be low, medium, or high.\n9. memory_scope is always artifact and memory_scope_value is the artifact_id.\n10. Low-signal artifacts should usually have low importance and no classifications.\n11. escalate_to_frontier may be true only when importance_score >= 8 and the artifact would materially benefit from a higher-quality pass.\n12. escalation_reason must be empty when escalate_to_frontier is false and one short sentence when true.";

fn extract_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "summary": {
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "body_text": { "type": "string" },
                    "evidence_segment_ids": { "type": "array", "items": { "type": "string" } }
                }
            },
            "classifications": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "classification_type": { "type": "string", "enum": ["topic", "intent"] },
                        "classification_value": { "type": "string" },
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "evidence_segment_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "memories": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "memory_type": { "type": "string", "enum": ["preference", "project_fact", "identity_fact", "ongoing_task", "reference"] },
                        "memory_scope": { "type": "string", "enum": ["artifact"] },
                        "memory_scope_value": { "type": "string" },
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "evidence_segment_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "entities": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "entity_key": { "type": "string" },
                        "display_name": { "type": "string" },
                        "entity_type": { "type": "string" },
                        "evidence_segment_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "relationships": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "relationship_type": { "type": "string" },
                        "subject_key": { "type": "string" },
                        "object_key": { "type": "string" },
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
                        "evidence_segment_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "retrieval_intents": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "question": { "type": "string" },
                        "query_text": { "type": "string" },
                        "intent_type": { "type": "string", "enum": ["topic_lookup", "memory_match", "entity_lookup", "relationship_lookup", "contradiction_check"] },
                        "evidence_segment_ids": { "type": "array", "items": { "type": "string" } }
                    }
                }
            },
            "importance_score": { "type": "integer" },
            "escalate_to_frontier": { "type": "boolean" },
            "escalation_reason": { "type": "string" }
        }
    })
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

fn preview(value: &str) -> String {
    truncate(value, 240)
}

struct ProbeClient {
    client: reqwest::blocking::Client,
    base_url: String,
    max_output_tokens: u32,
}

fn probe_preprocess_client(config: &GeminiConfig) -> Result<ProbeClient> {
    let mut default_headers = reqwest::header::HeaderMap::new();
    default_headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    let api_key_header = reqwest::header::HeaderName::from_static("x-goog-api-key");
    let api_key_value = reqwest::header::HeaderValue::from_str(&config.api_key)
        .map_err(|err| anyhow!("invalid Gemini API key header: {err}"))?;
    default_headers.insert(api_key_header, api_key_value);
    let client = reqwest::blocking::Client::builder()
        .default_headers(default_headers)
        .build()
        .context("failed to build Gemini probe client")?;
    Ok(ProbeClient {
        client,
        base_url: config.base_url.trim_end_matches('/').to_string(),
        max_output_tokens: config.max_output_tokens,
    })
}

impl ProbeClient {
    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
    ) -> Result<String> {
        let endpoint = format!(
            "{}/models/{}:generateContent",
            self.base_url,
            model.trim_start_matches("models/")
        );
        let body = serde_json::json!({
            "systemInstruction": { "parts": [{ "text": system_prompt }] },
            "contents": [{ "role": "user", "parts": [{ "text": user_prompt }] }],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": normalize_gemini_schema(schema),
                "maxOutputTokens": self.max_output_tokens
            }
        });
        let response = self
            .client
            .post(endpoint)
            .body(serde_json::to_vec(&body)?)
            .send()
            .context("failed to send inference request")?;
        let status = response.status();
        let response_text = response
            .text()
            .context("failed to read inference response")?;
        if !status.is_success() {
            return Err(anyhow!(
                "inference returned HTTP status {}: {}",
                status.as_u16(),
                preview(&response_text)
            ));
        }
        let parsed: serde_json::Value =
            serde_json::from_str(&response_text).with_context(|| {
                format!(
                    "failed to parse inference response: {}",
                    preview(&response_text)
                )
            })?;
        let text = parsed["candidates"][0]["content"]["parts"][0]["text"]
            .as_str()
            .ok_or_else(|| anyhow!("Gemini returned no text"))?;
        Ok(text.to_string())
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
                            child.as_str().unwrap_or("object").to_ascii_uppercase(),
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
