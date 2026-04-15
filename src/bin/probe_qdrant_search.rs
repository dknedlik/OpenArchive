#![deny(warnings)]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use postgres::NoTls;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{json, Value};

const DEFAULT_COLLECTION: &str = "oa_probe_vectors";
const DEFAULT_QDRANT_URL: &str = "http://127.0.0.1:6333";
const SEARCH_TOP_K: usize = 10;
const UPSERT_BATCH_SIZE: usize = 32;

#[derive(Debug, Parser)]
#[command(name = "probe_qdrant_search")]
#[command(
    about = "Probe Qdrant filtered vector search with either synthetic data or a snapshot of current Postgres embeddings"
)]
struct Args {
    /// Qdrant base URL.
    #[arg(long = "qdrant-url")]
    qdrant_url: Option<String>,

    /// Collection name used for the probe.
    #[arg(long = "collection", default_value = DEFAULT_COLLECTION)]
    collection: String,

    /// Load real embeddings from Postgres instead of generating synthetic vectors.
    #[arg(long = "postgres-url")]
    postgres_url: Option<String>,

    /// Maximum number of Postgres embeddings to import for the probe.
    #[arg(long = "limit", default_value_t = 0)]
    limit: usize,

    /// Number of search queries to benchmark.
    #[arg(long = "query-sample", default_value_t = 25)]
    query_sample: usize,

    /// Recreate the collection before loading points.
    #[arg(long = "recreate", default_value_t = true)]
    recreate: bool,

    /// Synthetic point count when Postgres is not used.
    #[arg(long = "synthetic-points", default_value_t = 4096)]
    synthetic_points: usize,

    /// Synthetic vector dimensions when Postgres is not used.
    #[arg(long = "synthetic-dimensions", default_value_t = 256)]
    synthetic_dimensions: usize,

    /// Synthetic artifact count when Postgres is not used.
    #[arg(long = "synthetic-artifacts", default_value_t = 256)]
    synthetic_artifacts: usize,

    /// Fixed RNG seed for reproducible synthetic probes.
    #[arg(long = "seed", default_value_t = 7)]
    seed: u64,

    /// Force exact search to compare recall/latency against Qdrant's default ANN behavior.
    #[arg(long = "exact", default_value_t = false)]
    exact: bool,

    /// Optional output file for the benchmark summary JSON.
    #[arg(long = "out")]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct ProbePoint {
    point_id: u64,
    derived_object_id: String,
    artifact_id: String,
    derived_object_type: String,
    object_status: String,
    vector: Vec<f32>,
}

#[derive(Debug, serde::Serialize)]
struct ProbeSummary {
    source: String,
    collection: String,
    search_mode: String,
    point_count: usize,
    dimensions: usize,
    load_elapsed_ms: u128,
    filtered_self_hit_rate: f32,
    filtered_nonempty_rate: f32,
    cross_artifact_nonempty_rate: f32,
    filtered_latency_ms: LatencySummary,
    cross_artifact_latency_ms: LatencySummary,
}

#[derive(Debug, serde::Serialize)]
struct LatencySummary {
    p50: f64,
    p95: f64,
    max: f64,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    result: Vec<SearchHit>,
}

#[derive(Debug, Deserialize)]
struct SearchHit {
    id: Value,
    score: f32,
    payload: Option<SearchPayload>,
}

#[derive(Debug, Deserialize)]
struct SearchPayload {
    derived_object_id: Option<String>,
}

fn main() -> Result<()> {
    load_dotenv();

    let args = Args::parse();
    let qdrant_url = args
        .qdrant_url
        .unwrap_or_else(default_qdrant_url)
        .trim_end_matches('/')
        .to_string();
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build HTTP client")?;

    ensure_qdrant_ready(&client, &qdrant_url)?;

    let points = if let Some(postgres_url) = args.postgres_url.as_deref() {
        load_points_from_postgres(postgres_url, args.limit)?
    } else {
        generate_synthetic_points(
            args.synthetic_points,
            args.synthetic_dimensions,
            args.synthetic_artifacts,
            args.seed,
        )
    };
    if points.is_empty() {
        return Err(anyhow!("probe dataset is empty"));
    }

    let dimensions = points[0].vector.len();
    if points.iter().any(|point| point.vector.len() != dimensions) {
        return Err(anyhow!(
            "probe dataset contains inconsistent vector dimensions"
        ));
    }

    recreate_collection(
        &client,
        &qdrant_url,
        &args.collection,
        dimensions,
        args.recreate,
    )?;

    let load_start = Instant::now();
    upsert_points(&client, &qdrant_url, &args.collection, &points)?;
    let load_elapsed = load_start.elapsed();

    let query_points = build_query_sample(&points, args.query_sample);
    let filtered = benchmark_filtered_queries(
        &client,
        &qdrant_url,
        &args.collection,
        &query_points,
        args.exact,
    )?;
    let cross_artifact = benchmark_cross_artifact_queries(
        &client,
        &qdrant_url,
        &args.collection,
        &query_points,
        args.exact,
    )?;

    let summary = ProbeSummary {
        source: if args.postgres_url.is_some() {
            "postgres_snapshot".to_string()
        } else {
            "synthetic".to_string()
        },
        collection: args.collection.clone(),
        search_mode: if args.exact {
            "exact".to_string()
        } else {
            "default_ann".to_string()
        },
        point_count: points.len(),
        dimensions,
        load_elapsed_ms: load_elapsed.as_millis(),
        filtered_self_hit_rate: ratio(filtered.self_hits, filtered.query_count),
        filtered_nonempty_rate: ratio(filtered.nonempty, filtered.query_count),
        cross_artifact_nonempty_rate: ratio(cross_artifact.nonempty, cross_artifact.query_count),
        filtered_latency_ms: latency_summary(&filtered.latencies),
        cross_artifact_latency_ms: latency_summary(&cross_artifact.latencies),
    };

    println!("Qdrant probe");
    println!("  source: {}", summary.source);
    println!("  qdrant_url: {}", qdrant_url);
    println!("  collection: {}", summary.collection);
    println!("  search_mode: {}", summary.search_mode);
    println!("  points: {}", summary.point_count);
    println!("  dimensions: {}", summary.dimensions);
    println!("  load_elapsed_ms: {}", summary.load_elapsed_ms);
    println!(
        "  filtered_self_hit_rate: {:.3}",
        summary.filtered_self_hit_rate
    );
    println!(
        "  filtered_nonempty_rate: {:.3}",
        summary.filtered_nonempty_rate
    );
    println!(
        "  cross_artifact_nonempty_rate: {:.3}",
        summary.cross_artifact_nonempty_rate
    );
    println!(
        "  filtered_latency_ms: p50 {:.2} | p95 {:.2} | max {:.2}",
        summary.filtered_latency_ms.p50,
        summary.filtered_latency_ms.p95,
        summary.filtered_latency_ms.max
    );
    println!(
        "  cross_artifact_latency_ms: p50 {:.2} | p95 {:.2} | max {:.2}",
        summary.cross_artifact_latency_ms.p50,
        summary.cross_artifact_latency_ms.p95,
        summary.cross_artifact_latency_ms.max
    );

    if let Some(path) = args.out.as_ref() {
        let json = serde_json::to_string_pretty(&summary)
            .context("failed to serialize Qdrant probe summary")?;
        std::fs::write(path, json)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }

    Ok(())
}

fn load_dotenv() {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(project_dir) = exe
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
        {
            let env_path = project_dir.join(".env");
            if env_path.exists() {
                let _ = dotenvy::from_path(&env_path);
            }
        }
    }
    let _ = dotenvy::dotenv();
}

fn default_qdrant_url() -> String {
    std::env::var("OA_QDRANT_URL").unwrap_or_else(|_| DEFAULT_QDRANT_URL.to_string())
}

fn ensure_qdrant_ready(client: &Client, qdrant_url: &str) -> Result<()> {
    client
        .get(format!("{qdrant_url}/collections"))
        .send()
        .context("failed to contact Qdrant")?
        .error_for_status()
        .context("Qdrant health check failed")?;
    Ok(())
}

fn recreate_collection(
    client: &Client,
    qdrant_url: &str,
    collection: &str,
    dimensions: usize,
    recreate: bool,
) -> Result<()> {
    if recreate {
        let response = client
            .delete(format!("{qdrant_url}/collections/{collection}"))
            .send()
            .context("failed to delete existing probe collection")?;
        if let Err(err) = response.error_for_status_ref() {
            let status = response.status();
            if status.as_u16() != 404 {
                return Err(err).context("failed to delete probe collection");
            }
        }
    }

    client
        .put(format!("{qdrant_url}/collections/{collection}?wait=true"))
        .json(&json!({
            "vectors": {
                "size": dimensions,
                "distance": "Cosine"
            }
        }))
        .send()
        .context("failed to create probe collection")?
        .error_for_status()
        .context("Qdrant collection creation failed")?;
    Ok(())
}

fn upsert_points(
    client: &Client,
    qdrant_url: &str,
    collection: &str,
    points: &[ProbePoint],
) -> Result<()> {
    for batch in points.chunks(UPSERT_BATCH_SIZE) {
        let qdrant_points = batch
            .iter()
            .map(|point| {
                json!({
                    "id": point.point_id,
                    "vector": point.vector,
                    "payload": {
                        "derived_object_id": point.derived_object_id,
                        "artifact_id": point.artifact_id,
                        "derived_object_type": point.derived_object_type,
                        "object_status": point.object_status,
                    }
                })
            })
            .collect::<Vec<_>>();

        client
            .put(format!(
                "{qdrant_url}/collections/{collection}/points?wait=true"
            ))
            .json(&json!({ "points": qdrant_points }))
            .send()
            .context("failed to upsert probe points")?
            .error_for_status()
            .context("Qdrant point upsert failed")?;
    }
    Ok(())
}

fn load_points_from_postgres(postgres_url: &str, limit: usize) -> Result<Vec<ProbePoint>> {
    let mut client = postgres::Client::connect(postgres_url, NoTls)
        .with_context(|| format!("failed to connect to Postgres at {postgres_url}"))?;

    let sql = if limit > 0 {
        "SELECT e.derived_object_id,
                e.artifact_id,
                e.derived_object_type,
                d.object_status,
                e.embedding::text
           FROM oa_derived_object_embedding e
           JOIN oa_derived_object d ON d.derived_object_id = e.derived_object_id
          WHERE d.object_status = 'active'
            AND e.derived_object_type IN ('summary', 'memory', 'relationship', 'entity')
          ORDER BY e.derived_object_id ASC
          LIMIT $1"
    } else {
        "SELECT e.derived_object_id,
                e.artifact_id,
                e.derived_object_type,
                d.object_status,
                e.embedding::text
           FROM oa_derived_object_embedding e
           JOIN oa_derived_object d ON d.derived_object_id = e.derived_object_id
          WHERE d.object_status = 'active'
            AND e.derived_object_type IN ('summary', 'memory', 'relationship', 'entity')
          ORDER BY e.derived_object_id ASC"
    };

    let rows = if limit > 0 {
        client
            .query(sql, &[&(limit as i64)])
            .context("failed to load probe embeddings from Postgres")?
    } else {
        client
            .query(sql, &[])
            .context("failed to load probe embeddings from Postgres")?
    };

    let mut points = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        let embedding_text: String = row.get(4);
        points.push(ProbePoint {
            point_id: (index + 1) as u64,
            derived_object_id: row.get(0),
            artifact_id: row.get(1),
            derived_object_type: row.get(2),
            object_status: row.get(3),
            vector: parse_pgvector_text(&embedding_text)
                .with_context(|| format!("failed to parse embedding for point {}", index + 1))?,
        });
    }
    Ok(points)
}

fn parse_pgvector_text(text: &str) -> Result<Vec<f32>> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| anyhow!("invalid pgvector text format"))?;
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    inner
        .split(',')
        .map(|item| {
            item.parse::<f32>()
                .with_context(|| format!("invalid float component {item}"))
        })
        .collect()
}

fn generate_synthetic_points(
    point_count: usize,
    dimensions: usize,
    artifact_count: usize,
    seed: u64,
) -> Vec<ProbePoint> {
    let mut rng = StdRng::seed_from_u64(seed);
    let cluster_count = usize::max(8, usize::min(64, artifact_count));
    let centers = (0..cluster_count)
        .map(|cluster| make_center(dimensions, cluster as f32 * 0.173))
        .collect::<Vec<_>>();

    (0..point_count)
        .map(|index| {
            let cluster = index % cluster_count;
            let artifact_ix = index % artifact_count;
            let derived_object_type = match index % 4 {
                0 => "memory",
                1 => "entity",
                2 => "relationship",
                _ => "summary",
            };
            let mut vector = centers[cluster].clone();
            for value in &mut vector {
                *value += rng.gen_range(-0.015f32..0.015f32);
            }
            normalize_vector(&mut vector);
            ProbePoint {
                point_id: (index + 1) as u64,
                derived_object_id: format!("synthetic-dobj-{}", index + 1),
                artifact_id: format!("artifact-{}", artifact_ix + 1),
                derived_object_type: derived_object_type.to_string(),
                object_status: "active".to_string(),
                vector,
            }
        })
        .collect()
}

fn make_center(dimensions: usize, phase: f32) -> Vec<f32> {
    let mut center = (0..dimensions)
        .map(|slot| ((slot as f32 * 0.013) + phase).sin())
        .collect::<Vec<_>>();
    normalize_vector(&mut center);
    center
}

fn normalize_vector(vector: &mut [f32]) {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in vector {
            *value /= norm;
        }
    }
}

fn build_query_sample(points: &[ProbePoint], query_sample: usize) -> Vec<ProbePoint> {
    let sample_count = usize::min(points.len(), usize::max(1, query_sample));
    if sample_count == points.len() {
        return points.to_vec();
    }

    let stride = usize::max(1, points.len() / sample_count);
    let mut sample = Vec::with_capacity(sample_count);
    let mut index = 0usize;
    while sample.len() < sample_count && index < points.len() {
        sample.push(points[index].clone());
        index += stride;
    }
    sample
}

#[derive(Default)]
struct QueryStats {
    latencies: Vec<Duration>,
    query_count: usize,
    nonempty: usize,
    self_hits: usize,
}

fn benchmark_filtered_queries(
    client: &Client,
    qdrant_url: &str,
    collection: &str,
    queries: &[ProbePoint],
    exact: bool,
) -> Result<QueryStats> {
    let mut stats = QueryStats::default();
    for query in queries {
        let filter = json!({
            "must": [
                { "key": "object_status", "match": { "value": "active" } },
                { "key": "derived_object_type", "match": { "value": query.derived_object_type } }
            ]
        });
        let start = Instant::now();
        let hits = search_points(
            client,
            qdrant_url,
            collection,
            &query.vector,
            Some(filter),
            exact,
        )?;
        stats.latencies.push(start.elapsed());
        stats.query_count += 1;
        if !hits.is_empty() {
            stats.nonempty += 1;
            if hit_matches_query(&hits[0], query) {
                stats.self_hits += 1;
            }
        }
    }
    Ok(stats)
}

fn benchmark_cross_artifact_queries(
    client: &Client,
    qdrant_url: &str,
    collection: &str,
    queries: &[ProbePoint],
    exact: bool,
) -> Result<QueryStats> {
    let mut stats = QueryStats::default();
    for query in queries {
        let filter = json!({
            "must": [
                { "key": "object_status", "match": { "value": "active" } },
                { "key": "derived_object_type", "match": { "value": query.derived_object_type } }
            ],
            "must_not": [
                { "key": "artifact_id", "match": { "value": query.artifact_id } }
            ]
        });
        let start = Instant::now();
        let hits = search_points(
            client,
            qdrant_url,
            collection,
            &query.vector,
            Some(filter),
            exact,
        )?;
        stats.latencies.push(start.elapsed());
        stats.query_count += 1;
        if !hits.is_empty() {
            stats.nonempty += 1;
        }
    }
    Ok(stats)
}

fn search_points(
    client: &Client,
    qdrant_url: &str,
    collection: &str,
    vector: &[f32],
    filter: Option<Value>,
    exact: bool,
) -> Result<Vec<SearchHit>> {
    let mut body = json!({
        "vector": vector,
        "limit": SEARCH_TOP_K,
        "with_payload": true,
    });
    if exact {
        body["params"] = json!({ "exact": true });
    }
    if let Some(filter) = filter {
        body["filter"] = filter;
    }

    let response = client
        .post(format!(
            "{qdrant_url}/collections/{collection}/points/search"
        ))
        .json(&body)
        .send()
        .context("failed to query Qdrant search endpoint")?
        .error_for_status()
        .context("Qdrant search request failed")?;
    let parsed: SearchResponse = response
        .json()
        .context("failed to parse Qdrant search response")?;
    Ok(parsed.result)
}

fn hit_matches_query(hit: &SearchHit, query: &ProbePoint) -> bool {
    let id_matches = hit.id.as_u64() == Some(query.point_id);
    let payload_matches = hit
        .payload
        .as_ref()
        .and_then(|payload| payload.derived_object_id.as_deref())
        == Some(query.derived_object_id.as_str());
    (id_matches || payload_matches) && hit.score > 0.99
}

fn ratio(numerator: usize, denominator: usize) -> f32 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f32 / denominator as f32
    }
}

fn latency_summary(latencies: &[Duration]) -> LatencySummary {
    let mut millis = latencies
        .iter()
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .collect::<Vec<_>>();
    if millis.is_empty() {
        return LatencySummary {
            p50: 0.0,
            p95: 0.0,
            max: 0.0,
        };
    }
    millis.sort_by(f64::total_cmp);
    LatencySummary {
        p50: percentile(&millis, 0.50),
        p95: percentile(&millis, 0.95),
        max: *millis.last().unwrap_or(&0.0),
    }
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_values.len() - 1) as f64 * percentile).round() as usize;
    sorted_values[rank]
}
