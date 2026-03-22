use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::PostgresConfig;
use postgres::Client;
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(name = "audit_knowledge")]
#[command(about = "Score persisted enrichment quality across the real corpus")]
struct Args {
    /// Restrict the audit to one import id. Defaults to the most recent import.
    #[arg(long)]
    import_id: Option<String>,

    /// Restrict to one provider name.
    #[arg(long)]
    provider: Option<String>,

    /// Restrict to one model name.
    #[arg(long)]
    model: Option<String>,

    /// Restrict to one prompt version.
    #[arg(long)]
    prompt_version: Option<String>,

    /// Number of worst-scoring artifacts to print for each provider/model/prompt bucket.
    #[arg(long, default_value_t = 5)]
    worst: usize,

    /// Score only artifacts completed by every compared signature.
    #[arg(long, default_value_t = false)]
    shared_only: bool,
}

#[derive(Debug, Clone)]
struct AuditArtifact {
    artifact_id: String,
    title: Option<String>,
    source_char_count: usize,
}

#[derive(Debug, Clone)]
struct AuditRun {
    derivation_run_id: String,
    artifact_id: String,
    provider_name: String,
    model_name: String,
    prompt_version: String,
    importance_score: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RunSignature {
    provider_name: String,
    model_name: String,
    prompt_version: String,
}

#[derive(Debug, Clone)]
struct AuditObject {
    derived_object_type: String,
    title: Option<String>,
    body_text: Option<String>,
    object_json: Option<Value>,
    evidence_count: usize,
}

#[derive(Debug, Clone)]
struct ArtifactAuditScore {
    artifact_id: String,
    title: Option<String>,
    total: f32,
    summary_score: f32,
    evidence_score: f32,
    memory_score: f32,
    classification_score: f32,
    compression_score: f32,
    importance_score: f32,
    duplication_penalty: f32,
    notes: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct BucketAggregate {
    artifact_count: usize,
    total_score_sum: f32,
    summary_score_sum: f32,
    evidence_score_sum: f32,
    memory_score_sum: f32,
    classification_score_sum: f32,
    compression_score_sum: f32,
    importance_score_sum: f32,
    duplication_penalty_sum: f32,
    total_memories: usize,
    total_classifications: usize,
    total_summaries: usize,
    artifacts_with_memories: usize,
    artifacts_with_classifications: usize,
    issue_counts: BTreeMap<String, usize>,
    worst_artifacts: Vec<ArtifactAuditScore>,
}

#[derive(Debug, Clone)]
struct ArtifactRunAudit {
    run: AuditRun,
    objects: Vec<AuditObject>,
    score: ArtifactAuditScore,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let pg = PostgresConfig::from_env().context("failed to load Postgres config from env")?;
    let mut client = Client::connect(&pg.connection_string, postgres::NoTls)
        .context("failed to connect to Postgres")?;

    let import_id = match args.import_id.clone() {
        Some(import_id) => import_id,
        None => latest_import_id(&mut client)?,
    };

    let artifacts = load_artifacts(&mut client, &import_id)?;
    if artifacts.is_empty() {
        return Err(anyhow!("no artifacts found for import {import_id}"));
    }

    let runs = load_completed_runs(&mut client, &import_id, &args)?;
    if runs.is_empty() {
        return Err(anyhow!("no completed derivation runs matched the filter"));
    }

    let objects_by_run = load_objects(&mut client, runs.keys().map(String::as_str).collect())?;
    let duplicate_title_counts = build_duplicate_memory_title_counts(&objects_by_run);

    let mut per_signature_artifacts: BTreeMap<RunSignature, BTreeMap<String, ArtifactRunAudit>> =
        BTreeMap::new();

    for run in runs.values() {
        let Some(artifact) = artifacts.get(&run.artifact_id) else {
            continue;
        };
        let objects = objects_by_run
            .get(&run.derivation_run_id)
            .cloned()
            .unwrap_or_default();

        let score = score_artifact(run, artifact, &objects, &duplicate_title_counts);
        let signature = RunSignature {
            provider_name: run.provider_name.clone(),
            model_name: run.model_name.clone(),
            prompt_version: run.prompt_version.clone(),
        };
        per_signature_artifacts
            .entry(signature)
            .or_default()
            .insert(
                artifact.artifact_id.clone(),
                ArtifactRunAudit {
                    run: run.clone(),
                    objects,
                    score,
                },
            );
    }

    let shared_artifact_ids = if args.shared_only {
        shared_artifact_ids(&per_signature_artifacts)
    } else {
        None
    };

    let mut per_signature: BTreeMap<RunSignature, BucketAggregate> = BTreeMap::new();
    for (signature, artifact_runs) in &per_signature_artifacts {
        for (artifact_id, artifact_run) in artifact_runs {
            if shared_artifact_ids
                .as_ref()
                .is_some_and(|shared| !shared.contains(artifact_id))
            {
                continue;
            }

            accumulate_bucket(
                per_signature.entry(signature.clone()).or_default(),
                &artifact_run.run,
                &artifact_run.objects,
                artifact_run.score.clone(),
                args.worst,
            );
        }
    }

    println!("OpenArchive knowledge audit");
    println!("Import: {import_id}");
    println!("Artifacts: {}", artifacts.len());
    if let Some(shared) = &shared_artifact_ids {
        println!("Shared artifacts only: {} artifact(s)", shared.len());
    }
    println!();

    for (signature, aggregate) in &per_signature {
        print_bucket(signature, aggregate);
    }

    Ok(())
}

fn shared_artifact_ids(
    per_signature_artifacts: &BTreeMap<RunSignature, BTreeMap<String, ArtifactRunAudit>>,
) -> Option<BTreeSet<String>> {
    let mut values = per_signature_artifacts.values();
    let first = values.next()?;
    let mut shared: BTreeSet<String> = first.keys().cloned().collect();
    for artifact_runs in values {
        let artifact_ids: BTreeSet<String> = artifact_runs.keys().cloned().collect();
        shared = shared
            .intersection(&artifact_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
    }
    Some(shared)
}

fn latest_import_id(client: &mut Client) -> Result<String> {
    let row = client
        .query_opt(
            "select import_id from oa_import order by created_at desc limit 1",
            &[],
        )
        .context("failed to query latest import id")?
        .ok_or_else(|| anyhow!("no imports found"))?;
    Ok(row.get::<_, String>(0))
}

fn load_artifacts(client: &mut Client, import_id: &str) -> Result<HashMap<String, AuditArtifact>> {
    let rows = client.query(
        "select a.artifact_id,
                a.import_id,
                a.title,
                count(s.segment_id)::bigint as segment_count,
                coalesce(sum(length(coalesce(s.text_content, ''))), 0)::bigint as source_char_count
           from oa_artifact a
           left join oa_segment s on s.artifact_id = a.artifact_id
          where a.import_id = $1
          group by a.artifact_id, a.import_id, a.title",
        &[&import_id],
    )?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let artifact = AuditArtifact {
                artifact_id: row.get::<_, String>(0),
                title: row.get::<_, Option<String>>(2),
                source_char_count: row.get::<_, i64>(4) as usize,
            };
            (artifact.artifact_id.clone(), artifact)
        })
        .collect())
}

fn load_completed_runs(
    client: &mut Client,
    import_id: &str,
    args: &Args,
) -> Result<HashMap<String, AuditRun>> {
    let rows = client.query(
        "select r.derivation_run_id,
                r.artifact_id,
                coalesce(r.provider_name, ''),
                coalesce(r.model_name, ''),
                coalesce(r.prompt_version, ''),
                r.input_scope_json::text
           from oa_derivation_run r
           join oa_artifact a on a.artifact_id = r.artifact_id
          where a.import_id = $1
            and r.run_status = 'completed'",
        &[&import_id],
    )?;

    let mut runs = HashMap::new();
    for row in rows {
        let provider_name = row.get::<_, String>(2);
        let model_name = row.get::<_, String>(3);
        let prompt_version = row.get::<_, String>(4);

        if args
            .provider
            .as_ref()
            .is_some_and(|expected| expected != &provider_name)
        {
            continue;
        }
        if args
            .model
            .as_ref()
            .is_some_and(|expected| expected != &model_name)
        {
            continue;
        }
        if args
            .prompt_version
            .as_ref()
            .is_some_and(|expected| expected != &prompt_version)
        {
            continue;
        }

        let input_scope_json = row.get::<_, String>(5);
        let parsed_scope: Option<Value> = serde_json::from_str(&input_scope_json).ok();

        let run = AuditRun {
            derivation_run_id: row.get::<_, String>(0),
            artifact_id: row.get::<_, String>(1),
            provider_name,
            model_name,
            prompt_version,
            importance_score: parsed_scope
                .as_ref()
                .and_then(|json| json.get("importance_score"))
                .and_then(Value::as_u64),
        };

        runs.insert(run.derivation_run_id.clone(), run);
    }

    Ok(runs)
}

fn load_objects(
    client: &mut Client,
    run_ids: Vec<&str>,
) -> Result<HashMap<String, Vec<AuditObject>>> {
    if run_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = client.query(
        "select o.derivation_run_id,
                o.derived_object_type,
                o.title,
                o.body_text,
                o.object_json::text,
                count(e.evidence_link_id)::bigint as evidence_count
           from oa_derived_object o
           left join oa_evidence_link e on e.derived_object_id = o.derived_object_id
          where o.derivation_run_id = any($1)
            and o.object_status = 'active'
          group by o.derived_object_id
          order by o.created_at",
        &[&run_ids],
    )?;

    let mut by_run: HashMap<String, Vec<AuditObject>> = HashMap::new();
    for row in rows {
        by_run
            .entry(row.get::<_, String>(0))
            .or_default()
            .push(AuditObject {
                derived_object_type: row.get::<_, String>(1),
                title: row.get::<_, Option<String>>(2),
                body_text: row.get::<_, Option<String>>(3),
                object_json: row
                    .get::<_, Option<String>>(4)
                    .and_then(|json| serde_json::from_str(&json).ok()),
                evidence_count: row.get::<_, i64>(5) as usize,
            });
    }

    Ok(by_run)
}

fn build_duplicate_memory_title_counts(
    objects_by_run: &HashMap<String, Vec<AuditObject>>,
) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for objects in objects_by_run.values() {
        for object in objects {
            if object.derived_object_type != "memory" {
                continue;
            }
            let Some(title) = object.title.as_ref() else {
                continue;
            };
            let key = normalize_phrase(title);
            if key.is_empty() {
                continue;
            }
            *counts.entry(key).or_insert(0) += 1;
        }
    }
    counts
}

fn score_artifact(
    run: &AuditRun,
    artifact: &AuditArtifact,
    objects: &[AuditObject],
    duplicate_title_counts: &HashMap<String, usize>,
) -> ArtifactAuditScore {
    let mut notes = Vec::new();

    let summaries: Vec<_> = objects
        .iter()
        .filter(|object| object.derived_object_type == "summary")
        .collect();
    let classifications: Vec<_> = objects
        .iter()
        .filter(|object| object.derived_object_type == "classification")
        .collect();
    let memories: Vec<_> = objects
        .iter()
        .filter(|object| object.derived_object_type == "memory")
        .collect();

    let summary_score = score_summary(&summaries, artifact, &mut notes);
    let evidence_score = score_evidence(objects, &mut notes);
    let memory_score = score_memories(&memories, artifact, &mut notes);
    let classification_score = score_classifications(&classifications, artifact, &mut notes);
    let compression_score = score_compression(&summaries, artifact, &mut notes);
    let importance_score = score_importance(run, artifact, &mut notes);
    let duplication_penalty =
        score_duplicate_penalty(&memories, duplicate_title_counts, &mut notes);

    let total = (summary_score
        + evidence_score
        + memory_score
        + classification_score
        + compression_score
        + importance_score
        - duplication_penalty)
        .clamp(0.0, 100.0);

    ArtifactAuditScore {
        artifact_id: artifact.artifact_id.clone(),
        title: artifact.title.clone(),
        total,
        summary_score,
        evidence_score,
        memory_score,
        classification_score,
        compression_score,
        importance_score,
        duplication_penalty,
        notes,
    }
}

fn score_summary(
    summaries: &[&AuditObject],
    artifact: &AuditArtifact,
    notes: &mut Vec<String>,
) -> f32 {
    if summaries.is_empty() {
        notes.push("missing_summary".to_string());
        return 0.0;
    }
    if summaries.len() > 1 {
        notes.push("multiple_summaries".to_string());
    }

    let summary = summaries[0];
    let title_ok = summary
        .title
        .as_ref()
        .is_some_and(|value| !value.trim().is_empty());
    let body_len = summary
        .body_text
        .as_ref()
        .map(|body| body.trim().chars().count())
        .unwrap_or(0);

    let mut score: f32 = 10.0;
    if title_ok {
        score += 5.0;
    } else {
        notes.push("empty_summary_title".to_string());
    }
    if body_len >= 80 {
        score += 5.0;
    } else if body_len >= 30 {
        score += 2.5;
        notes.push("short_summary".to_string());
    } else {
        notes.push("tiny_summary".to_string());
    }

    if artifact.source_char_count > 0 && body_len > artifact.source_char_count {
        notes.push("summary_longer_than_source".to_string());
        score -= 4.0;
    }

    score.clamp(0.0, 20.0)
}

fn score_evidence(objects: &[AuditObject], notes: &mut Vec<String>) -> f32 {
    if objects.is_empty() {
        notes.push("no_objects".to_string());
        return 0.0;
    }

    let missing = objects
        .iter()
        .filter(|object| object.evidence_count == 0)
        .count();
    let avg = objects
        .iter()
        .map(|object| object.evidence_count as f32)
        .sum::<f32>()
        / objects.len() as f32;

    let mut score = 20.0 * ((objects.len() - missing) as f32 / objects.len() as f32);
    if avg > 5.0 {
        score -= 3.0;
        notes.push("evidence_overcited".to_string());
    }
    if avg < 1.0 {
        notes.push("evidence_sparse".to_string());
    }
    if missing > 0 {
        notes.push("objects_missing_evidence".to_string());
    }

    score.clamp(0.0, 20.0)
}

fn score_memories(
    memories: &[&AuditObject],
    artifact: &AuditArtifact,
    notes: &mut Vec<String>,
) -> f32 {
    if memories.is_empty() {
        if artifact.source_char_count >= 500 {
            notes.push("no_memories_on_rich_artifact".to_string());
            return 2.0;
        }
        return 8.0;
    }

    let mut score = 6.0;
    let count = memories.len();
    if count <= 5 {
        score += 4.0;
    } else {
        notes.push("too_many_memories".to_string());
    }

    let useful = memories
        .iter()
        .filter(|memory| {
            memory
                .body_text
                .as_ref()
                .map(|body| body.trim().chars().count() >= 40)
                .unwrap_or(false)
        })
        .count();
    score += 5.0 * (useful as f32 / count as f32);

    let memory_types: HashSet<_> = memories
        .iter()
        .filter_map(|memory| {
            memory
                .object_json
                .as_ref()
                .and_then(|json| json.get("memory_type"))
                .and_then(Value::as_str)
        })
        .collect();
    if memory_types.len() == 1 && count >= 3 {
        notes.push("memory_type_monoculture".to_string());
        score -= 2.0;
    } else if memory_types.len() >= 2 {
        score += 1.0;
    }

    score.clamp(0.0, 15.0)
}

fn score_classifications(
    classifications: &[&AuditObject],
    artifact: &AuditArtifact,
    notes: &mut Vec<String>,
) -> f32 {
    if classifications.len() > 2 {
        notes.push("too_many_classifications".to_string());
        return 0.0;
    }

    let mut score: f32 = 5.0;
    if classifications.is_empty() && artifact.source_char_count < 400 {
        score += 2.0;
    }
    if classifications.len() == 2 {
        score -= 1.0;
    }

    for classification in classifications {
        if let Some(json) = &classification.object_json {
            if let Some(value) = json.get("classification_value").and_then(Value::as_str) {
                let normalized = normalize_phrase(value);
                if matches!(
                    normalized.as_str(),
                    "planning" | "architecture" | "technical" | "discussion" | "work"
                ) {
                    notes.push("generic_classification".to_string());
                    score -= 2.0;
                }
            }
        }
    }

    score.clamp(0.0, 10.0)
}

fn score_compression(
    summaries: &[&AuditObject],
    artifact: &AuditArtifact,
    notes: &mut Vec<String>,
) -> f32 {
    let Some(summary) = summaries.first() else {
        return 0.0;
    };
    let source_chars = artifact.source_char_count.max(1) as f32;
    let summary_chars = summary
        .body_text
        .as_ref()
        .map(|body| body.trim().chars().count() as f32)
        .unwrap_or(0.0);

    if summary_chars == 0.0 {
        return 0.0;
    }

    let ratio = summary_chars / source_chars;
    let score = if (0.03..=0.35).contains(&ratio) {
        10.0
    } else if ratio < 0.03 {
        notes.push("summary_overcompressed".to_string());
        6.0
    } else if ratio <= 0.5 {
        notes.push("summary_verbose".to_string());
        7.0
    } else {
        notes.push("summary_too_verbose".to_string());
        3.0
    };

    score
}

fn score_importance(run: &AuditRun, artifact: &AuditArtifact, notes: &mut Vec<String>) -> f32 {
    let Some(score) = run.importance_score else {
        notes.push("missing_importance_score".to_string());
        return 3.0;
    };

    let mut result: f32 = 10.0;
    if artifact.source_char_count < 400 && score > 6 {
        notes.push("importance_too_high_for_small_artifact".to_string());
        result -= 3.0;
    }
    if artifact.source_char_count > 2000 && score < 3 {
        notes.push("importance_too_low_for_large_artifact".to_string());
        result -= 2.0;
    }
    result.clamp(0.0, 10.0)
}

fn score_duplicate_penalty(
    memories: &[&AuditObject],
    duplicate_title_counts: &HashMap<String, usize>,
    notes: &mut Vec<String>,
) -> f32 {
    let mut penalty: f32 = 0.0;
    let mut seen = HashSet::new();
    for memory in memories {
        let Some(title) = memory.title.as_ref() else {
            continue;
        };
        let normalized = normalize_phrase(title);
        if normalized.is_empty() {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            notes.push("duplicate_memory_title_within_artifact".to_string());
            penalty += 2.0;
            continue;
        }
        if duplicate_title_counts
            .get(&normalized)
            .copied()
            .unwrap_or(0)
            >= 4
        {
            notes.push("memory_title_repeated_across_corpus".to_string());
            penalty += 1.0;
        }
    }

    penalty.clamp(0.0, 5.0)
}

fn normalize_phrase(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn accumulate_bucket(
    bucket: &mut BucketAggregate,
    _run: &AuditRun,
    objects: &[AuditObject],
    score: ArtifactAuditScore,
    worst_limit: usize,
) {
    bucket.artifact_count += 1;
    bucket.total_score_sum += score.total;
    bucket.summary_score_sum += score.summary_score;
    bucket.evidence_score_sum += score.evidence_score;
    bucket.memory_score_sum += score.memory_score;
    bucket.classification_score_sum += score.classification_score;
    bucket.compression_score_sum += score.compression_score;
    bucket.importance_score_sum += score.importance_score;
    bucket.duplication_penalty_sum += score.duplication_penalty;

    let summary_count = objects
        .iter()
        .filter(|object| object.derived_object_type == "summary")
        .count();
    let classification_count = objects
        .iter()
        .filter(|object| object.derived_object_type == "classification")
        .count();
    let memory_count = objects
        .iter()
        .filter(|object| object.derived_object_type == "memory")
        .count();

    bucket.total_summaries += summary_count;
    bucket.total_classifications += classification_count;
    bucket.total_memories += memory_count;
    if memory_count > 0 {
        bucket.artifacts_with_memories += 1;
    }
    if classification_count > 0 {
        bucket.artifacts_with_classifications += 1;
    }
    for note in &score.notes {
        *bucket.issue_counts.entry(note.clone()).or_insert(0) += 1;
    }

    bucket.worst_artifacts.push(score);
    bucket
        .worst_artifacts
        .sort_by(|a, b| a.total.partial_cmp(&b.total).unwrap());
    bucket.worst_artifacts.truncate(worst_limit);
}

fn print_bucket(signature: &RunSignature, aggregate: &BucketAggregate) {
    let count = aggregate.artifact_count.max(1) as f32;
    println!(
        "{} | {} | {}",
        signature.provider_name, signature.model_name, signature.prompt_version
    );
    println!(
        "  system score: {:.1}/100 across {} artifacts",
        aggregate.total_score_sum / count,
        aggregate.artifact_count
    );
    println!(
        "  subscores: summary {:.1}, evidence {:.1}, memories {:.1}, classifications {:.1}, compression {:.1}, importance {:.1}, duplication penalty {:.1}",
        aggregate.summary_score_sum / count,
        aggregate.evidence_score_sum / count,
        aggregate.memory_score_sum / count,
        aggregate.classification_score_sum / count,
        aggregate.compression_score_sum / count,
        aggregate.importance_score_sum / count,
        aggregate.duplication_penalty_sum / count,
    );
    println!(
        "  output mix: summaries {}, classifications {}, memories {} | memory coverage {:.0}% | classification coverage {:.0}%",
        aggregate.total_summaries,
        aggregate.total_classifications,
        aggregate.total_memories,
        100.0 * aggregate.artifacts_with_memories as f32 / count,
        100.0 * aggregate.artifacts_with_classifications as f32 / count,
    );

    if !aggregate.issue_counts.is_empty() {
        let mut issues: Vec<_> = aggregate.issue_counts.iter().collect();
        issues.sort_by(|a, b| b.1.cmp(a.1));
        let summary = issues
            .into_iter()
            .take(6)
            .map(|(issue, count)| format!("{issue}={count}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("  top issues: {summary}");
    }

    if !aggregate.worst_artifacts.is_empty() {
        println!("  worst artifacts:");
        for artifact in &aggregate.worst_artifacts {
            println!(
                "    {:.1} | {} | {}{}",
                artifact.total,
                artifact.artifact_id,
                artifact.title.as_deref().unwrap_or("(untitled)"),
                if artifact.notes.is_empty() {
                    String::new()
                } else {
                    format!(" | {}", artifact.notes.join(", "))
                }
            );
        }
    }

    println!();
}
