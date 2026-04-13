#![deny(warnings)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "probe_reconcile_report")]
#[command(about = "Summarize one or more probe_reconcile output directories side by side")]
struct Args {
    /// Slice label and directory, in the form label=/path/to/reports.
    #[arg(long = "slice", required = true)]
    slices: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ProbeReport {
    title: Option<String>,
    preparation: ProbePreparation,
    ambiguity_model_outputs: Option<Vec<DecisionOutput>>,
}

#[derive(Debug, Deserialize)]
struct ProbePreparation {
    candidate_traces: Vec<CandidateTrace>,
    deterministic_outputs: Vec<DecisionOutput>,
}

#[derive(Debug, Deserialize)]
struct CandidateTrace {
    disposition: Option<String>,
    embedding_matches: Vec<EmbeddingMatch>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingMatch {
    similarity_score: f32,
}

#[derive(Debug, Deserialize)]
struct DecisionOutput {
    decision_kind: String,
}

#[derive(Debug, Default)]
struct SliceMetrics {
    files: usize,
    exact: usize,
    deterministic_embedding: usize,
    ambiguous: usize,
    low_similarity: usize,
    deterministic_attach: usize,
    deterministic_create: usize,
    ambiguity_attach: usize,
    ambiguity_create: usize,
    ambiguous_scores: Vec<f32>,
    deterministic_scores: Vec<f32>,
    low_scores: Vec<f32>,
    per_artifact_ambiguous: Vec<(String, usize)>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let slices = parse_slices(&args.slices)?;
    let mut metrics_by_label = BTreeMap::new();
    for (label, dirs) in slices {
        metrics_by_label.insert(label, summarize_dirs(&dirs)?);
    }

    print_summary(&metrics_by_label);
    Ok(())
}

fn parse_slices(values: &[String]) -> Result<BTreeMap<String, Vec<PathBuf>>> {
    let mut slices = BTreeMap::<String, Vec<PathBuf>>::new();
    for value in values {
        let Some((label, dirs)) = value.split_once('=') else {
            return Err(anyhow!(
                "slice must use label=/path/to/reports or label=/dir1,/dir2 form: {value}"
            ));
        };
        let label = label.trim();
        if label.is_empty() {
            return Err(anyhow!("slice label must not be empty: {value}"));
        }
        let parsed_dirs = dirs
            .split(',')
            .map(str::trim)
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        if parsed_dirs.is_empty() {
            return Err(anyhow!(
                "slice must include at least one directory: {value}"
            ));
        }
        slices
            .entry(label.to_string())
            .or_default()
            .extend(parsed_dirs);
    }
    Ok(slices)
}

fn summarize_dirs(dirs: &[PathBuf]) -> Result<SliceMetrics> {
    let mut metrics = SliceMetrics::default();
    for dir in dirs {
        summarize_dir(dir, &mut metrics)?;
    }

    if metrics.files == 0 {
        let joined = dirs
            .iter()
            .map(|dir| dir.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow!("no JSON reports found in {joined}"));
    }

    metrics
        .per_artifact_ambiguous
        .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    Ok(metrics)
}

fn summarize_dir(dir: &Path, metrics: &mut SliceMetrics) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read probe report directory {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to list probe report directory {}", dir.display()))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let report: ProbeReport = serde_json::from_str(
            &fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", path.display()))?;
        metrics.files += 1;

        let mut ambiguous_for_artifact = 0usize;
        for trace in report.preparation.candidate_traces {
            let Some(disposition) = trace.disposition.as_deref() else {
                continue;
            };
            let top_score = trace.embedding_matches.first().map(|m| m.similarity_score);
            match disposition {
                "exact_candidate_key_match" => metrics.exact += 1,
                "deterministic_embedding_match" => {
                    metrics.deterministic_embedding += 1;
                    if let Some(score) = top_score {
                        metrics.deterministic_scores.push(score);
                    }
                }
                "ambiguous_embedding_match" => {
                    metrics.ambiguous += 1;
                    ambiguous_for_artifact += 1;
                    if let Some(score) = top_score {
                        metrics.ambiguous_scores.push(score);
                    }
                }
                "low_similarity_create_new" => {
                    metrics.low_similarity += 1;
                    if let Some(score) = top_score {
                        metrics.low_scores.push(score);
                    }
                }
                _ => {}
            }
        }

        metrics.per_artifact_ambiguous.push((
            report.title.unwrap_or_else(|| "<untitled>".to_string()),
            ambiguous_for_artifact,
        ));

        for decision in report.preparation.deterministic_outputs {
            match normalize_decision_kind(&decision.decision_kind) {
                "attach_to_existing" => metrics.deterministic_attach += 1,
                "create_new" => metrics.deterministic_create += 1,
                _ => {}
            }
        }
        for decision in report.ambiguity_model_outputs.unwrap_or_default() {
            match normalize_decision_kind(&decision.decision_kind) {
                "attach_to_existing" => metrics.ambiguity_attach += 1,
                "create_new" => metrics.ambiguity_create += 1,
                _ => {}
            }
        }
    }
    Ok(())
}

fn normalize_decision_kind(kind: &str) -> &str {
    match kind {
        "strengthen_existing" => "attach_to_existing",
        other => other,
    }
}

fn print_summary(metrics_by_label: &BTreeMap<String, SliceMetrics>) {
    let labels: Vec<&str> = metrics_by_label.keys().map(String::as_str).collect();
    let first_width = 20usize;
    let other_width = labels
        .iter()
        .map(|label| label.len().max(12))
        .max()
        .unwrap_or(12)
        + 2;

    print!("{:<first_width$}", "metric", first_width = first_width);
    for label in &labels {
        print!("{:>other_width$}", label, other_width = other_width);
    }
    println!();

    let rows = [
        ("files", row_usize(metrics_by_label, |m| m.files)),
        ("exact", row_usize(metrics_by_label, |m| m.exact)),
        (
            "det_embed",
            row_usize(metrics_by_label, |m| m.deterministic_embedding),
        ),
        ("ambiguous", row_usize(metrics_by_label, |m| m.ambiguous)),
        ("low_new", row_usize(metrics_by_label, |m| m.low_similarity)),
        (
            "det_attach",
            row_usize(metrics_by_label, |m| m.deterministic_attach),
        ),
        (
            "det_create",
            row_usize(metrics_by_label, |m| m.deterministic_create),
        ),
        (
            "amb_attach",
            row_usize(metrics_by_label, |m| m.ambiguity_attach),
        ),
        (
            "amb_create",
            row_usize(metrics_by_label, |m| m.ambiguity_create),
        ),
        (
            "amb_attach_%",
            row_string(metrics_by_label, |m| {
                let total = m.ambiguity_attach + m.ambiguity_create;
                if total == 0 {
                    "-".to_string()
                } else {
                    format!("{:.1}", (m.ambiguity_attach as f64 / total as f64) * 100.0)
                }
            }),
        ),
        (
            "amb_score_med",
            row_string(metrics_by_label, |m| format_median(&m.ambiguous_scores)),
        ),
        (
            "det_score_med",
            row_string(metrics_by_label, |m| format_median(&m.deterministic_scores)),
        ),
        (
            "low_score_med",
            row_string(metrics_by_label, |m| format_median(&m.low_scores)),
        ),
    ];

    for (name, values) in rows {
        print!("{:<first_width$}", name, first_width = first_width);
        for value in values {
            print!("{:>other_width$}", value, other_width = other_width);
        }
        println!();
    }

    println!();
    println!("top ambiguous artifacts");
    for (label, metrics) in metrics_by_label {
        print!("{label}:");
        let top = metrics
            .per_artifact_ambiguous
            .iter()
            .take(3)
            .map(|(title, count)| format!(" {count}x {title}"))
            .collect::<Vec<_>>()
            .join(" |");
        println!("{top}");
    }
}

fn row_usize(
    metrics_by_label: &BTreeMap<String, SliceMetrics>,
    value: impl Fn(&SliceMetrics) -> usize,
) -> Vec<String> {
    metrics_by_label
        .values()
        .map(|metrics| value(metrics).to_string())
        .collect()
}

fn row_string(
    metrics_by_label: &BTreeMap<String, SliceMetrics>,
    value: impl Fn(&SliceMetrics) -> String,
) -> Vec<String> {
    metrics_by_label.values().map(value).collect()
}

fn format_median(values: &[f32]) -> String {
    if values.is_empty() {
        return "-".to_string();
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let mid = sorted.len() / 2;
    let median = if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    };
    format!("{median:.3}")
}
