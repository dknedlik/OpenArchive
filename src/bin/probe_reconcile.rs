#![deny(warnings)]

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use postgres::NoTls;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use open_archive::config::{GeminiConfig, PostgresConfig};
use open_archive::processor::{
    memory_candidate_key_from_fields, EntityOutput, InferenceUsage, MemoryOutput,
    ReconciliationProcessorInput, RelationshipOutput, SummaryOutput,
};
use open_archive::storage::enrichment_state_store::EnrichmentStateStore;
use open_archive::storage::types::{ArtifactReconcilePayload, SourceType};
use open_archive::storage::{
    ArtifactReadStore, PostgresArtifactReadStore, PostgresDerivedMetadataStore,
};

#[derive(Debug, Parser)]
#[command(name = "probe_reconcile")]
#[command(about = "Replay real reconciliation jobs against Gemini and inspect raw outputs")]
struct Args {
    #[arg(required = true)]
    job_ids: Vec<String>,

    #[arg(long = "model")]
    model: Option<String>,

    #[arg(long = "profile", value_enum, default_value_t = PromptProfile::Baseline)]
    profile: PromptProfile,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PromptProfile {
    Baseline,
    StrictKeys,
}

impl PromptProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::StrictKeys => "strict-keys",
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let postgres = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let gemini = GeminiConfig::from_env().context("failed to load Gemini config from env")?;
    let read_store = PostgresArtifactReadStore::new(postgres.clone());
    let derived_store = PostgresDerivedMetadataStore::new(postgres.clone());
    let probe = GeminiProbeClient::new(&gemini)?;
    let model = args
        .model
        .clone()
        .unwrap_or_else(|| gemini.fast_model.clone());

    println!("Reconcile probe");
    println!("Provider: gemini");
    println!("Model: {model}");
    println!("Profile: {}", args.profile.as_str());
    println!();

    let mut client = postgres::Client::connect(&postgres.connection_string, NoTls)
        .context("failed to connect to Postgres")?;

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
        if let Some(message) = &last_error_message {
            println!("Last error: {message}");
        }

        let candidate_keys = candidate_target_keys(&input);
        println!("Candidate target keys ({}):", candidate_keys.len());
        for key in &candidate_keys {
            println!("  - {key}");
        }

        let prompt = build_reconciliation_prompt(&input, args.profile)?;
        let response = probe.complete_json(
            &model,
            RECONCILIATION_SYSTEM_PROMPT,
            &prompt,
            &reconciliation_output_schema(),
        )?;

        println!();
        println!("Raw JSON:");
        println!("{}", response.output_text);

        let parsed: ModelReconciliationOutput = serde_json::from_str(&response.output_text)
            .with_context(|| {
                format!(
                    "failed to parse reconciliation JSON: {}",
                    preview(&response.output_text)
                )
            })?;

        println!();
        println!("Decision target keys:");
        for decision in &parsed.decisions {
            let status = if candidate_keys.contains(&decision.target_key) {
                "ok"
            } else {
                "invalid"
            };
            println!(
                "  - [{}] {} | {} | {}",
                status, decision.target_kind, decision.target_key, decision.rationale
            );
        }

        match validate_reconciliation_output(&parsed, &input) {
            Ok(()) => println!(
                "Validation: ok{}",
                response
                    .usage
                    .as_ref()
                    .map(|usage| format!(" | {}", format_usage(usage)))
                    .unwrap_or_default()
            ),
            Err(err) => println!(
                "Validation: err: {}{}",
                err,
                response
                    .usage
                    .as_ref()
                    .map(|usage| format!(" | {}", format_usage(usage)))
                    .unwrap_or_default()
            ),
        }
        println!();
    }

    Ok(())
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

fn candidate_target_keys(
    input: &ReconciliationProcessorInput,
) -> std::collections::BTreeSet<String> {
    input
        .memories
        .iter()
        .map(|memory| memory.candidate_key.clone())
        .chain(input.relationships.iter().map(|relationship| {
            format!(
                "{}:{}:{}",
                relationship.relationship_type, relationship.subject_key, relationship.object_key
            )
        }))
        .collect()
}

fn build_reconciliation_prompt(
    input: &ReconciliationProcessorInput,
    profile: PromptProfile,
) -> Result<String> {
    #[derive(Serialize)]
    struct CandidateMemory<'a> {
        target_key: String,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }

    #[derive(Serialize)]
    struct CandidateRelationship<'a> {
        target_key: String,
        relationship_type: &'a str,
        subject_key: &'a str,
        object_key: &'a str,
        title: Option<&'a str>,
        body_text: &'a str,
        evidence_segment_ids: &'a [String],
    }

    let memories: Vec<_> = input
        .memories
        .iter()
        .map(|memory| CandidateMemory {
            target_key: memory.candidate_key.clone(),
            title: memory.title.as_deref(),
            body_text: memory.body_text.as_str(),
            evidence_segment_ids: &memory.evidence_segment_ids,
        })
        .collect();
    let relationships: Vec<_> = input
        .relationships
        .iter()
        .map(|relationship| CandidateRelationship {
            target_key: format!(
                "{}:{}:{}",
                relationship.relationship_type, relationship.subject_key, relationship.object_key
            ),
            relationship_type: relationship.relationship_type.as_str(),
            subject_key: relationship.subject_key.as_str(),
            object_key: relationship.object_key.as_str(),
            title: relationship.title.as_deref(),
            body_text: relationship.body_text.as_str(),
            evidence_segment_ids: &relationship.evidence_segment_ids,
        })
        .collect();

    let extra_rules = match profile {
        PromptProfile::Baseline => "",
        PromptProfile::StrictKeys => {
            "\nSTRICT target_key rules:\n\
             - Copy target_key exactly from candidate_memories or candidate_relationships.\n\
             - Do not paraphrase, normalize, slugify, shorten, or synthesize a new target_key.\n\
             - If a candidate is about grilling, dietary goals, or bacon sausage, use the exact listed target_key verbatim.\n\
             - Before answering, verify every target_key string is an exact byte-for-byte match to one listed candidate target_key.\n"
        }
    };

    Ok(format!(
        "Reconcile extraction candidates against archive retrieval results.\n\
         artifact_id: {artifact_id}\n\
         source_type: {source_type}\n\
         summary: {summary}\n\
         \n\
         candidate_memories:\n\
         {memories_json}\n\
         \n\
         candidate_relationships:\n\
         {relationships_json}\n\
         \n\
         retrieval_results:\n\
         {retrieval_results_json}\n\
         \n\
         Output one decision per candidate memory or relationship.\n\
         target_key must match a candidate target_key exactly.\n\
         target_kind must be memory or relationship.\n\
         {extra_rules}",
        artifact_id = input.artifact_id,
        source_type = input.source_type.as_str(),
        summary = input.summary.body_text,
        memories_json = serde_json::to_string_pretty(&memories)
            .context("failed to serialize candidate memories")?,
        relationships_json = serde_json::to_string_pretty(&relationships)
            .context("failed to serialize candidate relationships")?,
        retrieval_results_json = input.retrieval_results_json,
        extra_rules = extra_rules,
    ))
}

fn reconciliation_output_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["decisions"],
        "properties": {
            "decisions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["decision_kind", "target_kind", "target_key", "rationale", "evidence_segment_ids"],
                    "properties": {
                        "decision_kind": {
                            "type": "string",
                            "enum": [
                                "create_new",
                                "attach_to_existing",
                                "strengthen_existing",
                                "supersede_existing",
                                "contradicts_existing",
                                "insufficient_evidence"
                            ]
                        },
                        "target_kind": { "type": "string", "enum": ["memory", "relationship"] },
                        "target_key": { "type": "string" },
                        "matched_object_id": { "type": "string" },
                        "rationale": { "type": "string" },
                        "evidence_segment_ids": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    }
                }
            }
        }
    })
}

const RECONCILIATION_SYSTEM_PROMPT: &str = "You are OpenArchive's strict reconciliation engine. Use only the provided extraction candidates, retrieval results, and source evidence. Return ONLY valid JSON.\n\nRules:\n1. Prefer no merge over a weak merge.\n2. Choose create_new when the archive evidence is insufficient.\n3. Use attach_to_existing or strengthen_existing only when the retrieved object clearly matches the candidate.\n4. Use supersede_existing only when the new artifact clearly updates or replaces prior state.\n5. Use contradicts_existing only when the artifact clearly conflicts with retrieved prior state.\n6. Never merge identities, projects, or relationships on vague topical overlap.\n7. Every decision must cite real evidence_segment_ids from the extraction candidates.\n8. Output valid JSON only.";

#[derive(Debug, Deserialize)]
struct ModelReconciliationOutput {
    decisions: Vec<ModelDecision>,
}

#[derive(Debug, Deserialize)]
struct ModelDecision {
    target_kind: String,
    target_key: String,
    rationale: String,
    #[serde(default)]
    evidence_segment_ids: Vec<String>,
}

fn validate_reconciliation_output(
    output: &ModelReconciliationOutput,
    input: &ReconciliationProcessorInput,
) -> Result<()> {
    let valid_targets = candidate_target_keys(input);
    let valid_evidence_ids: std::collections::BTreeSet<&str> = input
        .summary
        .evidence_segment_ids
        .iter()
        .chain(
            input
                .memories
                .iter()
                .flat_map(|memory| memory.evidence_segment_ids.iter()),
        )
        .chain(
            input
                .relationships
                .iter()
                .flat_map(|relationship| relationship.evidence_segment_ids.iter()),
        )
        .map(String::as_str)
        .collect();
    let mut seen_targets = std::collections::BTreeSet::new();

    for (index, decision) in output.decisions.iter().enumerate() {
        if !valid_targets.contains(&decision.target_key) {
            return Err(anyhow!(
                "decisions[{index}].target_key {:?} does not match a candidate",
                decision.target_key
            ));
        }
        if !seen_targets.insert(decision.target_key.clone()) {
            return Err(anyhow!(
                "decisions[{index}].target_key {:?} is duplicated",
                decision.target_key
            ));
        }
        for evidence_id in &decision.evidence_segment_ids {
            if !valid_evidence_ids.contains(evidence_id.as_str()) {
                return Err(anyhow!(
                    "decisions[{index}].evidence_segment_ids contains unknown segment id {:?}",
                    evidence_id
                ));
            }
        }
    }

    if seen_targets != valid_targets {
        return Err(anyhow!(
            "reconciliation output must provide exactly one decision for each candidate memory or relationship"
        ));
    }

    Ok(())
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
        default_headers.insert(
            HeaderName::from_static("x-goog-api-key"),
            HeaderValue::from_str(&config.api_key)
                .map_err(|err| anyhow!("invalid Gemini API key header: {err}"))?,
        );
        Ok(Self {
            client: Client::builder()
                .default_headers(default_headers)
                .build()
                .context("failed to build Gemini HTTP client")?,
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
        let parsed: GeminiGenerateContentResponse = serde_json::from_str(&response_text)
            .with_context(|| {
                format!(
                    "failed to parse Gemini response: {}",
                    preview(&response_text)
                )
            })?;
        let output_text = parsed
            .flatten_text()
            .ok_or_else(|| anyhow!("Gemini returned empty content"))?;
        Ok(GeminiProbeResponse {
            output_text,
            usage: parsed.usage_metadata.as_ref().map(InferenceUsage::from),
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

fn preview(value: &str) -> String {
    let mut preview = value.chars().take(240).collect::<String>();
    if value.chars().count() > 240 {
        preview.push_str("...");
    }
    preview
}

fn format_usage(usage: &InferenceUsage) -> String {
    let input_tokens = usage
        .input_tokens
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let output_tokens = usage
        .output_tokens
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let total_tokens = usage
        .total_tokens
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!("tokens in/out/total = {input_tokens}/{output_tokens}/{total_tokens}")
}
