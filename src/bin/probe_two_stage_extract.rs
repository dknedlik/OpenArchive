use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use std::fs;
use std::path::PathBuf;

use open_archive::config::{GeminiConfig, PostgresConfig};
use open_archive::storage::{ArtifactReadStore, PostgresArtifactReadStore};

#[derive(Debug, Parser)]
#[command(name = "probe_two_stage_extract")]
#[command(
    about = "Replay one artifact through Gemini candidate extraction then Gemini finalization"
)]
struct Args {
    #[arg(required = true)]
    artifact_id: String,

    #[arg(long = "candidate-model", default_value = "gemini-3-flash-preview")]
    candidate_model: String,

    #[arg(
        long = "finalizer-model",
        default_value = "gemini-3.1-flash-lite-preview"
    )]
    finalizer_model: String,

    #[arg(long = "candidate-max-output", default_value_t = 7000)]
    candidate_max_output: u32,

    #[arg(long = "finalizer-max-output", default_value_t = 4000)]
    finalizer_max_output: u32,

    #[arg(long = "print-candidates", default_value_t = false)]
    print_candidates: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let gemini = GeminiConfig::from_env().context("failed to load Gemini config from env")?;
    let read_store = PostgresArtifactReadStore::new(postgres);
    let client = GeminiProbeClient::new(&gemini)?;

    let loaded = read_store
        .load_artifact_for_enrichment(&args.artifact_id)
        .with_context(|| format!("failed to load artifact {}", args.artifact_id))?
        .ok_or_else(|| anyhow!("artifact {} not found", args.artifact_id))?;

    let prompt = build_artifact_prompt(
        &loaded.artifact.artifact_id,
        loaded.artifact.source_type.as_str(),
        loaded.artifact.title.as_deref().unwrap_or(""),
        &loaded.segments,
    )?;

    println!("Two-stage extract probe");
    println!("Artifact: {}", loaded.artifact.artifact_id);
    println!("Title: {}", loaded.artifact.title.as_deref().unwrap_or(""));
    println!("Segments: {}", loaded.segments.len());
    println!("Candidate model: {}", args.candidate_model);
    println!("Finalizer model: {}", args.finalizer_model);
    println!();

    let candidate_output = client.complete_json(
        &args.candidate_model,
        CANDIDATE_SYSTEM_PROMPT,
        &prompt,
        &candidate_schema(),
        args.candidate_max_output,
    )?;
    let candidate_value: serde_json::Value = serde_json::from_str(&candidate_output.output_text)
        .context("failed to parse candidate output JSON")?;
    println!(
        "Candidate stage: {:.2}s | memories {} | classifications {} | entities {} | relationships {}{}",
        candidate_output.elapsed_secs,
        array_len(&candidate_value, "memory_candidates"),
        array_len(&candidate_value, "classification_candidates"),
        array_len(&candidate_value, "entity_candidates"),
        array_len(&candidate_value, "relationship_candidates"),
        format_usage(candidate_output.usage.as_ref()),
    );
    if args.print_candidates {
        println!();
        println!("Raw candidate JSON:");
        println!("{}", candidate_output.output_text);
        println!();
    }

    let finalizer_prompt = build_finalizer_prompt(&loaded.artifact.artifact_id, &candidate_value)?;
    let final_output = client.complete_json(
        &args.finalizer_model,
        FINALIZER_SYSTEM_PROMPT,
        &finalizer_prompt,
        &final_schema(),
        args.finalizer_max_output,
    )?;
    let final_value: serde_json::Value = match serde_json::from_str(&final_output.output_text) {
        Ok(value) => value,
        Err(error) => {
            let path = write_probe_dump(
                "probe_two_stage_extract_finalizer_error.json",
                &final_output.output_text,
            )
            .context("failed to write finalizer error dump")?;
            return Err(error).with_context(|| {
                format!(
                    "failed to parse final JSON; raw finalizer output written to {}",
                    path.display()
                )
            });
        }
    };
    println!(
        "Finalizer stage: {:.2}s | memories {} | classifications {} | entities {} | relationships {}{}",
        final_output.elapsed_secs,
        array_len(&final_value, "memories"),
        array_len(&final_value, "classifications"),
        array_len(&final_value, "entities"),
        array_len(&final_value, "relationships"),
        format_usage(final_output.usage.as_ref()),
    );
    println!();

    println!("Final summary:");
    println!(
        "  {}",
        final_value
            .get("summary")
            .and_then(|value| value.get("body_text"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .replace('\n', " ")
    );
    println!("Final memories:");
    if let Some(memories) = final_value
        .get("memories")
        .and_then(serde_json::Value::as_array)
    {
        for memory in memories {
            println!(
                "  - {} | {}",
                memory
                    .get("memory_type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(""),
                memory
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
            );
        }
    }
    println!();

    println!("Raw final JSON:");
    println!("{}", final_output.output_text);

    Ok(())
}

fn write_probe_dump(filename: &str, contents: &str) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(filename);
    fs::write(&path, contents)
        .with_context(|| format!("failed to write probe dump to {}", path.display()))?;
    Ok(path)
}

fn array_len(value: &serde_json::Value, key: &str) -> usize {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len)
}

#[derive(Serialize)]
struct PromptSegment<'a> {
    evidence_ref: String,
    participant_role: &'a str,
    text: &'a str,
}

fn build_artifact_prompt(
    artifact_id: &str,
    source_type: &str,
    title: &str,
    segments: &[open_archive::storage::types::LoadedSegment],
) -> Result<String> {
    let prompt_segments: Vec<_> = segments
        .iter()
        .enumerate()
        .map(|(index, segment)| PromptSegment {
            evidence_ref: format!("evidence_ref_{}", index + 1),
            participant_role: segment
                .participant_role
                .map(|role| role.as_str())
                .unwrap_or("unknown"),
            text: segment.text_content.as_str(),
        })
        .collect();
    let segments_json =
        serde_json::to_string_pretty(&prompt_segments).context("failed to serialize segments")?;

    Ok(format!(
        "Input artifact:\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         title: {title}\n\
         \n\
         segments:\n\
         {segments_json}\n\
         \n\
         Output guidance:\n\
         - the artifact text is divided into segments; each segment has an evidence_ref identifier\n\
         - every emitted item must include evidence_segment_ids using only evidence_ref values shown in segments\n\
         - preserve evidence_ref values exactly\n\
         - do not invent unsupported claims; every emitted item must be directly supported or strongly supported by artifact content\n\
         - be exhaustive on durable, retrieval-worthy content\n\
         - avoid obvious duplicates and broad rollups when a more specific candidate works\n",
    ))
}

fn build_finalizer_prompt(
    artifact_id: &str,
    candidate_value: &serde_json::Value,
) -> Result<String> {
    let candidate_json = serde_json::to_string_pretty(candidate_value)
        .context("failed to serialize candidate bundle")?;
    Ok(format!(
        "Finalize this extraction candidate bundle into the target schema.\n\
         artifact_id: {artifact_id}\n\
         \n\
         Candidate bundle:\n\
         {candidate_json}\n\
         \n\
         Rules:\n\
         - Use only facts and evidence refs present in the candidate bundle.\n\
         - Do not invent new memories, entities, relationships, classifications, or retrieval intents.\n\
         - Merge overlapping candidates and drop non-durable or redundant items.\n\
         - Prefer specific, retrieval-worthy memories over broad umbrella memories.\n\
         - Choose memory_type only from: personal_fact, preference, project_fact, ongoing_state, reference.\n\
         - Preserve evidence refs exactly as provided.\n\
         - Fix mistyped or weak candidates where possible.\n\
         - Output valid JSON only.\n"
    ))
}

fn candidate_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary_draft",
            "classification_candidates",
            "memory_candidates",
            "entity_candidates",
            "relationship_candidates",
            "retrieval_candidates",
            "importance_score"
        ],
        "properties": {
            "summary_draft": summary_schema(),
            "classification_candidates": {
                "type": "array",
                "items": classification_schema()
            },
            "memory_candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "title",
                        "body_text",
                        "evidence_segment_ids",
                        "durability_label",
                        "retrieval_value_label",
                        "consequentiality_label",
                        "temporal_scope"
                    ],
                    "properties": {
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "durability_label": {
                            "type": "string",
                            "enum": ["low", "medium", "high"]
                        },
                        "retrieval_value_label": {
                            "type": "string",
                            "enum": ["low", "medium", "high"]
                        },
                        "consequentiality_label": {
                            "type": "string",
                            "enum": ["low", "medium", "high"]
                        },
                        "temporal_scope": {
                            "type": "string",
                            "enum": ["ephemeral", "time_bound", "ongoing", "enduring"]
                        },
                        "evidence_segment_ids": evidence_array_schema()
                    }
                }
            },
            "entity_candidates": {
                "type": "array",
                "items": entity_schema()
            },
            "relationship_candidates": {
                "type": "array",
                "items": relationship_schema()
            },
            "retrieval_candidates": {
                "type": "array",
                "items": retrieval_intent_schema()
            },
            "importance_score": { "type": "integer" }
        }
    })
}

fn final_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "summary",
            "classifications",
            "memories",
            "entities",
            "relationships",
            "retrieval_intents",
            "importance_score"
        ],
        "properties": {
            "summary": summary_schema(),
            "classifications": {
                "type": "array",
                "items": classification_schema()
            },
            "memories": {
                "type": "array",
                "maxItems": 15,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": [
                        "memory_type",
                        "memory_scope",
                        "memory_scope_value",
                        "title",
                        "body_text",
                        "evidence_segment_ids"
                    ],
                    "properties": {
                        "memory_type": {
                            "type": "string",
                            "enum": [
                                "personal_fact",
                                "preference",
                                "project_fact",
                                "ongoing_state",
                                "reference"
                            ]
                        },
                        "memory_scope": { "type": "string", "enum": ["artifact"] },
                        "memory_scope_value": { "type": "string" },
                        "title": { "type": "string" },
                        "body_text": { "type": "string" },
                        "evidence_segment_ids": evidence_array_schema()
                    }
                }
            },
            "entities": {
                "type": "array",
                "items": entity_schema()
            },
            "relationships": {
                "type": "array",
                "items": relationship_schema()
            },
            "retrieval_intents": {
                "type": "array",
                "items": retrieval_intent_schema()
            },
            "importance_score": { "type": "integer" }
        }
    })
}

fn summary_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["title", "body_text", "evidence_segment_ids"],
        "properties": {
            "title": { "type": "string" },
            "body_text": { "type": "string" },
            "evidence_segment_ids": evidence_array_schema()
        }
    })
}

fn classification_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["classification_type", "classification_value", "title", "body_text", "evidence_segment_ids"],
        "properties": {
            "classification_type": { "type": "string", "enum": ["topic", "intent"] },
            "classification_value": { "type": "string" },
            "title": { "type": "string" },
            "body_text": { "type": "string" },
            "evidence_segment_ids": evidence_array_schema()
        }
    })
}

fn entity_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["entity_key", "display_name", "entity_type", "evidence_segment_ids"],
        "properties": {
            "entity_key": { "type": "string" },
            "display_name": { "type": "string" },
            "entity_type": { "type": "string" },
            "evidence_segment_ids": evidence_array_schema()
        }
    })
}

fn relationship_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["relationship_type", "subject_key", "object_key", "title", "body_text", "confidence_label", "evidence_segment_ids"],
        "properties": {
            "relationship_type": { "type": "string" },
            "subject_key": { "type": "string" },
            "object_key": { "type": "string" },
            "title": { "type": "string" },
            "body_text": { "type": "string" },
            "confidence_label": { "type": "string", "enum": ["low", "medium", "high"] },
            "evidence_segment_ids": evidence_array_schema()
        }
    })
}

fn retrieval_intent_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["question", "query_text", "intent_type", "evidence_segment_ids"],
        "properties": {
            "question": { "type": "string" },
            "query_text": { "type": "string" },
            "intent_type": {
                "type": "string",
                "enum": ["topic_lookup", "memory_match", "entity_lookup", "relationship_lookup", "contradiction_check"]
            },
            "evidence_segment_ids": evidence_array_schema()
        }
    })
}

fn evidence_array_schema() -> serde_json::Value {
    json!({
        "type": "array",
        "items": { "type": "string" }
    })
}

const CANDIDATE_SYSTEM_PROMPT: &str = "You are OpenArchive's candidate extraction engine. Read one artifact and return ONLY valid JSON.\n\nReturn these sections:\n- summary_draft\n- classification_candidates\n- memory_candidates\n- entity_candidates\n- relationship_candidates\n- retrieval_candidates\n- importance_score\n\nGeneral rules:\n1. Output valid JSON only.\n2. The artifact text is divided into segments. Every emitted item must include evidence_segment_ids that reference the provided segment identifiers exactly.\n3. Do not invent unsupported claims. Every emitted item must be directly supported or strongly supported by artifact content.\n4. Treat the output families as different jobs. A good summary does not replace memories, classifications, entities, relationships, or retrieval intents.\n5. Prefer recall over concision at this stage. It is acceptable to surface more candidates than the final extraction will keep.\n6. Be exhaustive, not representative.\n7. Avoid obvious duplicates, but do not suppress distinct candidates just because they are related.\n\nObject guidance:\n- summary_draft: Produce a coherent, human-readable synthesis of the artifact's central topics, actors, stakes, and outcomes. Do not turn it into a list of every fact.\n- classification_candidates: Emit useful browse or filter lenses. Do not over-prune.\n- memory_candidates: Emit every distinct durable or reusable fact, state, constraint, decision, preference, background detail, history item, or ongoing condition that could be retrieved independently later. Each candidate must be atomic and standalone. Do not collapse several concrete items into one broad rollup. When in doubt between including and omitting, include. For each memory candidate, assign: durability_label = one of high|medium|low; retrieval_value_label = one of high|medium|low; consequentiality_label = one of high|medium|low; temporal_scope = one of ephemeral|time_bound|ongoing|enduring.\n- entity_candidates: Emit salient entities that could matter as independent retrieval targets later. It is acceptable to include candidates that may later be pruned.\n- relationship_candidates: Emit explicit or strongly supported relationships between emitted entities. It is acceptable to include related candidates that may later be simplified.\n- retrieval_candidates: Emit plausible future questions that a user or system may ask later. They should help retrieval and follow-up reasoning, not just restate the summary.\n- importance_score: Reflect how useful this artifact is likely to be for future retrieval and personalization.";

const FINALIZER_SYSTEM_PROMPT: &str = "You are OpenArchive's extraction finalizer. Use only the provided candidate bundle and return ONLY valid JSON in the requested final schema.\n\nGeneral rules:\n1. Do not invent facts, evidence refs, or objects not supported by the candidate bundle.\n2. Preserve evidence refs exactly.\n3. Do not treat any count limit as a target. Keep the strongest distinct items that belong; do not pad, and do not collapse unrelated facts merely to be shorter.\n4. When several candidates are individually valid, prefer the set with higher long-term retrieval value, clearer future usefulness, and greater downstream consequence.\n5. The candidate bundle is already a first-pass extraction. Pruning is optional, not required. If the candidate count is already within the schema limit, you do not need to remove anything. Drop or merge only when candidates are genuine duplicates, clear near-duplicates, unsupported by the bundle, or clearly low-value and local to this artifact.\n\nObject guidance:\n- summary: Keep it coherent and central. Preserve the main story of the artifact without turning the summary into a dump of all candidates.\n- classifications: Keep only useful browse or filter lenses. Remove generic, weak, or redundant classifications.\n- memories: Keep distinct, durable, retrieval-worthy memories. Use the candidate labels for durability, retrieval value, consequentiality, and temporal scope as editorial signals, not rigid rules. Candidates marked high on durability, retrieval value, or consequentiality should usually be retained unless they duplicate another stronger candidate. Deprioritize transient logistics, one-off setup details, low-value inventories, and items whose future usefulness is narrowly local to this artifact. Keep separate memories when candidates would answer meaningfully different future retrieval questions, even if they are related or appear close together in the source. Merge only genuine duplicates or near-duplicates. Do not collapse multiple important facts into an umbrella memory when keeping them separate would improve future retrieval.\n- entities: Keep only salient entities that stand on their own as future retrieval targets, but be conservative about dropping supported entities when they anchor retained memories, relationships, or retrieval intents.\n- relationships: Keep semantically clean, well-supported relationships between retained entities. Prefer retaining a supported relationship over dropping it merely for brevity.\n- retrieval_intents: Keep questions that are likely future lookups or follow-ups, not paraphrases of the summary. Prefer preserving intents that map to retained memories or relationships.\n\nSchema guidance:\n- Fix bad type assignments and choose memory_type only from the allowed schema values.\n- Output valid JSON only.";

struct GeminiProbeClient {
    client: Client,
    base_url: String,
}

impl GeminiProbeClient {
    fn new(config: &GeminiConfig) -> Result<Self> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let api_key_header = HeaderName::from_static("x-goog-api-key");
        let api_key_value = HeaderValue::from_str(&config.api_key)?;
        default_headers.insert(api_key_header, api_key_value);

        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(std::time::Duration::from_secs(180))
            .build()?;
        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
        })
    }

    fn complete_json(
        &self,
        model: &str,
        system_prompt: &str,
        user_prompt: &str,
        schema: &serde_json::Value,
        max_output_tokens: u32,
    ) -> Result<ProbeResult> {
        let started = std::time::Instant::now();
        let endpoint = format!("{}/models/{}:generateContent", self.base_url, model);
        let body = json!({
            "system_instruction": {
                "parts": [{ "text": system_prompt }]
            },
            "contents": [{
                "role": "user",
                "parts": [{ "text": user_prompt }]
            }],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": normalize_gemini_schema(schema),
                "maxOutputTokens": max_output_tokens
            }
        });
        let request_body =
            serde_json::to_vec(&body).context("failed to serialize Gemini request body")?;
        let response = self.client.post(endpoint).body(request_body).send()?;
        let status = response.status();
        let response_text = response.text()?;
        if !status.is_success() {
            anyhow::bail!(
                "Gemini HTTP {}: {}",
                status.as_u16(),
                preview(&response_text)
            );
        }
        let parsed: GeminiGenerateContentResponse =
            serde_json::from_str(&response_text).context("failed to parse Gemini response JSON")?;
        let output_text = parsed
            .flatten_text()
            .ok_or_else(|| anyhow!("Gemini returned empty content"))?;
        Ok(ProbeResult {
            output_text,
            usage: parsed.usage_metadata,
            elapsed_secs: started.elapsed().as_secs_f64(),
        })
    }
}

struct ProbeResult {
    output_text: String,
    usage: Option<GeminiUsageMetadata>,
    elapsed_secs: f64,
}

fn preview(value: &str) -> String {
    value.chars().take(240).collect()
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
                        | "description"
                ) {
                    continue;
                }
                if key == "type" {
                    let normalized = match child {
                        serde_json::Value::String(value) => {
                            serde_json::Value::String(value.to_ascii_uppercase())
                        }
                        _ => normalize_gemini_schema(child),
                    };
                    sanitized.insert(key.clone(), normalized);
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

fn format_usage(usage: Option<&GeminiUsageMetadata>) -> String {
    let Some(usage) = usage else {
        return String::new();
    };
    format!(
        " | in {} out {} total {} thoughts {}",
        usage.prompt_token_count.unwrap_or_default(),
        usage.candidates_token_count.unwrap_or_default(),
        usage.total_token_count.unwrap_or_default(),
        usage.thoughts_token_count.unwrap_or_default()
    )
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default, rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
}

impl GeminiGenerateContentResponse {
    fn flatten_text(&self) -> Option<String> {
        let mut output = String::new();
        for candidate in &self.candidates {
            let Some(content) = &candidate.content else {
                continue;
            };
            for part in &content.parts {
                if let Some(text) = &part.text {
                    output.push_str(text);
                }
            }
        }
        if output.trim().is_empty() {
            None
        } else {
            Some(output)
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiResponseContent>,
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiResponseContent {
    #[serde(default)]
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiResponsePart {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GeminiUsageMetadata {
    #[serde(default, rename = "promptTokenCount")]
    prompt_token_count: Option<u64>,
    #[serde(default, rename = "candidatesTokenCount")]
    candidates_token_count: Option<u64>,
    #[serde(default, rename = "totalTokenCount")]
    total_token_count: Option<u64>,
    #[serde(default, rename = "thoughtsTokenCount")]
    thoughts_token_count: Option<u64>,
}
