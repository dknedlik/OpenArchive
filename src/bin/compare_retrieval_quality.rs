#![deny(warnings)]

use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::app::search::ObjectSearchService;
use open_archive::bootstrap::build_service_bundle;
use open_archive::config::{
    AppConfig, LocalFsObjectStoreConfig, ObjectStoreConfig, QdrantConfig, RelationalStoreConfig,
    SqliteConfig, VectorStoreConfig,
};
use open_archive::embedding::EmbeddingProvider;
use open_archive::storage::{
    DerivedObjectSearchResult, DerivedObjectSearchStore, ObjectSearchFilters,
};
use postgres::{Client, NoTls};
use std::time::Duration;

const DEFAULT_QUERIES: &[&str] = &[
    "fireplace flue drafting tips",
    "apple books audiobook cloud backup",
    "rendering bacon fat techniques and tips",
    "switching from iphone to android",
    "oracle stock plunge ai risks and backlog",
    "creatine 5g vs 10g full body benefits",
    "why fake news thrives on x",
    "home depot vs lowes dog policies",
    "moby dick ineffable depths",
    "dark mode adjustment",
];

#[derive(Debug, Parser)]
#[command(name = "compare_retrieval_quality")]
#[command(about = "Compare side-by-side retrieval quality for Postgres and SQLite/Qdrant")]
struct Args {
    #[arg(
        long = "sqlite-path",
        default_value = "tmp/sqlite-side-by-side/open_archive.db"
    )]
    sqlite_path: PathBuf,

    #[arg(
        long = "sqlite-object-root",
        default_value = "tmp/sqlite-side-by-side/objects"
    )]
    sqlite_object_root: PathBuf,

    #[arg(long = "qdrant-url", default_value = "http://127.0.0.1:6333")]
    qdrant_url: String,

    #[arg(long = "qdrant-collection", default_value = "oa_sqlite_side_by_side")]
    qdrant_collection: String,

    #[arg(long = "limit", default_value_t = 5)]
    limit: usize,

    #[arg(long = "postgres-url")]
    postgres_url: Option<String>,

    #[arg(long = "summary-query-limit", default_value_t = 15)]
    summary_query_limit: usize,

    #[arg(long = "query")]
    queries: Vec<String>,
}

struct Backend {
    object_search: ObjectSearchService,
    object_search_store: std::sync::Arc<dyn DerivedObjectSearchStore + Send + Sync>,
    embedding_provider: Option<std::sync::Arc<dyn EmbeddingProvider>>,
}

struct QueryComparison {
    query: String,
    postgres_merged: Vec<DerivedObjectSearchResult>,
    sqlite_merged: Vec<DerivedObjectSearchResult>,
    postgres_semantic: Vec<DerivedObjectSearchResult>,
    sqlite_semantic: Vec<DerivedObjectSearchResult>,
}

fn main() -> Result<()> {
    load_dotenv();
    let args = Args::parse();
    let base_config = AppConfig::from_env().context("failed to load base config from env")?;
    let postgres_url = postgres_url_from_args_or_env(&args, &base_config)?;

    let postgres_backend = build_postgres_backend(&base_config)?;
    let sqlite_backend = build_sqlite_backend(&base_config, &args)?;

    let queries = build_query_set(&postgres_url, &args)?;

    let mut comparisons = Vec::new();
    for query in queries {
        comparisons.push(compare_query(
            &postgres_backend,
            &sqlite_backend,
            &query,
            args.limit,
        )?);
    }

    print_report(&comparisons, args.limit);
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
                return;
            }
        }
    }
    let _ = dotenvy::dotenv();
}

fn postgres_url_from_args_or_env(args: &Args, base: &AppConfig) -> Result<String> {
    if let Some(url) = args.postgres_url.clone() {
        return Ok(url);
    }
    match &base.relational_store {
        RelationalStoreConfig::Postgres(pg) => Ok(pg.connection_string.clone()),
        _ => std::env::var("OA_POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .map_err(|_| anyhow!("postgres url is required for summary query sampling")),
    }
}

fn build_query_set(postgres_url: &str, args: &Args) -> Result<Vec<String>> {
    let mut queries = BTreeSet::new();
    for query in DEFAULT_QUERIES {
        queries.insert(query.to_string());
    }
    for query in &args.queries {
        queries.insert(query.trim().to_string());
    }
    for query in load_summary_queries(postgres_url, args.summary_query_limit)? {
        queries.insert(query);
    }
    Ok(queries
        .into_iter()
        .filter(|query| !query.trim().is_empty())
        .collect())
}

fn load_summary_queries(postgres_url: &str, limit: usize) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut client = Client::connect(postgres_url, NoTls)
        .with_context(|| format!("failed to connect to Postgres at {postgres_url}"))?;
    let rows = client
        .query(
            "SELECT DISTINCT title
             FROM oa_derived_object
             WHERE derived_object_type = 'summary'
               AND object_status = 'active'
               AND title IS NOT NULL
               AND title <> ''
             ORDER BY title ASC
             LIMIT $1",
            &[&(limit as i64)],
        )
        .context("failed to load summary titles from Postgres")?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

fn build_postgres_backend(base: &AppConfig) -> Result<Backend> {
    let mut config = base.clone();
    config.vector_store = VectorStoreConfig::PostgresPgVector;
    config.relational_store = match &base.relational_store {
        RelationalStoreConfig::Postgres(pg) => RelationalStoreConfig::Postgres(pg.clone()),
        _ => {
            return Err(anyhow!(
                "base env must contain a usable Postgres relational config"
            ));
        }
    };
    let bundle = build_service_bundle(&config).context("failed to build Postgres bundle")?;
    let object_search_store = bundle
        .object_search_store
        .ok_or_else(|| anyhow!("postgres bundle missing object search store"))?;
    Ok(Backend {
        object_search: ObjectSearchService::new(
            object_search_store.clone(),
            bundle.embedding_provider.clone(),
        ),
        object_search_store,
        embedding_provider: bundle.embedding_provider,
    })
}

fn build_sqlite_backend(base: &AppConfig, args: &Args) -> Result<Backend> {
    let mut config = base.clone();
    config.relational_store = RelationalStoreConfig::Sqlite(SqliteConfig {
        path: args.sqlite_path.clone(),
        busy_timeout: Duration::from_secs(30),
    });
    config.vector_store = VectorStoreConfig::Qdrant(QdrantConfig {
        url: args.qdrant_url.clone(),
        collection_name: args.qdrant_collection.clone(),
        request_timeout: Duration::from_secs(30),
        exact: true,
    });
    config.object_store = ObjectStoreConfig::LocalFs(LocalFsObjectStoreConfig {
        root: args.sqlite_object_root.clone(),
    });
    let bundle = build_service_bundle(&config).context("failed to build SQLite/Qdrant bundle")?;
    let object_search_store = bundle
        .object_search_store
        .ok_or_else(|| anyhow!("sqlite bundle missing object search store"))?;
    Ok(Backend {
        object_search: ObjectSearchService::new(
            object_search_store.clone(),
            bundle.embedding_provider.clone(),
        ),
        object_search_store,
        embedding_provider: bundle.embedding_provider,
    })
}

fn compare_query(
    postgres: &Backend,
    sqlite: &Backend,
    query: &str,
    limit: usize,
) -> Result<QueryComparison> {
    let filters = ObjectSearchFilters {
        query: Some(query.to_string()),
        ..Default::default()
    };
    let postgres_merged = postgres
        .object_search
        .search(filters.clone(), limit)
        .with_context(|| format!("postgres merged search failed for query {query:?}"))?;
    let sqlite_merged = sqlite
        .object_search
        .search(filters.clone(), limit)
        .with_context(|| format!("sqlite merged search failed for query {query:?}"))?;

    let query_embedding = postgres
        .embedding_provider
        .as_ref()
        .ok_or_else(|| anyhow!("postgres backend missing embedding provider"))?
        .embed_texts(&[query.to_string()])
        .with_context(|| format!("failed to embed query {query:?}"))?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("embedding provider returned no vector for query {query:?}"))?;

    let semantic_filters = ObjectSearchFilters::default();
    let postgres_semantic = postgres
        .object_search_store
        .search_objects_by_embedding(&semantic_filters, &query_embedding, limit)
        .with_context(|| format!("postgres semantic search failed for query {query:?}"))?;
    let sqlite_semantic = sqlite
        .object_search_store
        .search_objects_by_embedding(&semantic_filters, &query_embedding, limit)
        .with_context(|| format!("sqlite semantic search failed for query {query:?}"))?;

    Ok(QueryComparison {
        query: query.to_string(),
        postgres_merged,
        sqlite_merged,
        postgres_semantic,
        sqlite_semantic,
    })
}

fn print_report(comparisons: &[QueryComparison], limit: usize) {
    println!("Retrieval quality comparison");
    println!("  top_k: {}", limit);
    println!("  queries: {}", comparisons.len());
    println!();

    let merged_overlap = average_overlap(
        comparisons
            .iter()
            .map(|comparison| (&comparison.postgres_merged, &comparison.sqlite_merged)),
    );
    let semantic_overlap = average_overlap(
        comparisons
            .iter()
            .map(|comparison| (&comparison.postgres_semantic, &comparison.sqlite_semantic)),
    );
    let semantic_candidate_overlap = average_candidate_overlap(comparisons);
    let merged_top1_agreement = comparisons
        .iter()
        .filter(|comparison| {
            top1_equivalent(&comparison.postgres_merged, &comparison.sqlite_merged)
        })
        .count() as f32
        / comparisons.len().max(1) as f32;
    let semantic_top1_agreement = comparisons
        .iter()
        .filter(|comparison| {
            top1_equivalent(&comparison.postgres_semantic, &comparison.sqlite_semantic)
        })
        .count() as f32
        / comparisons.len().max(1) as f32;
    let semantic_summary_agreement = comparisons
        .iter()
        .filter(|comparison| {
            summary_top3_equivalent(&comparison.postgres_semantic, &comparison.sqlite_semantic)
        })
        .count() as f32
        / comparisons.len().max(1) as f32;

    println!(
        "Average concept overlap: merged {:.2} / semantic {:.2}",
        merged_overlap, semantic_overlap
    );
    println!(
        "Average semantic candidate-key overlap: {:.2}",
        semantic_candidate_overlap
    );
    println!("Merged top-1 agreement rate: {:.2}", merged_top1_agreement);
    println!(
        "Semantic top-1 agreement rate: {:.2}",
        semantic_top1_agreement
    );
    println!(
        "Semantic top-3 summary agreement rate: {:.2}",
        semantic_summary_agreement
    );
    println!();

    for comparison in comparisons {
        println!("Query: {}", comparison.query);
        println!(
            "  merged concept overlap: {:.2}",
            concept_overlap(&comparison.postgres_merged, &comparison.sqlite_merged)
        );
        println!(
            "  semantic concept overlap: {:.2}",
            concept_overlap(&comparison.postgres_semantic, &comparison.sqlite_semantic)
        );
        println!(
            "  semantic candidate overlap: {:.2}",
            candidate_overlap(&comparison.postgres_semantic, &comparison.sqlite_semantic)
        );
        println!(
            "  semantic top1 agreement: {}",
            if top1_equivalent(&comparison.postgres_semantic, &comparison.sqlite_semantic) {
                "yes"
            } else {
                "no"
            }
        );
        print_backend_results("postgres merged", &comparison.postgres_merged);
        print_backend_results("sqlite  merged", &comparison.sqlite_merged);
        print_backend_results("postgres semantic", &comparison.postgres_semantic);
        print_backend_results("sqlite  semantic", &comparison.sqlite_semantic);
        println!();
    }
}

fn average_overlap<'a, I>(pairs: I) -> f32
where
    I: Iterator<
        Item = (
            &'a Vec<DerivedObjectSearchResult>,
            &'a Vec<DerivedObjectSearchResult>,
        ),
    >,
{
    let mut total = 0.0f32;
    let mut count = 0usize;
    for (left, right) in pairs {
        total += concept_overlap(left, right);
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        total / count as f32
    }
}

fn average_candidate_overlap(comparisons: &[QueryComparison]) -> f32 {
    if comparisons.is_empty() {
        return 0.0;
    }
    comparisons
        .iter()
        .map(|comparison| {
            candidate_overlap(&comparison.postgres_semantic, &comparison.sqlite_semantic)
        })
        .sum::<f32>()
        / comparisons.len() as f32
}

fn concept_overlap(left: &[DerivedObjectSearchResult], right: &[DerivedObjectSearchResult]) -> f32 {
    let left_signatures = left
        .iter()
        .flat_map(result_signatures)
        .collect::<BTreeSet<_>>();
    let right_signatures = right
        .iter()
        .flat_map(result_signatures)
        .collect::<BTreeSet<_>>();
    if left_signatures.is_empty() && right_signatures.is_empty() {
        return 1.0;
    }
    let intersection = left_signatures.intersection(&right_signatures).count() as f32;
    let union = left_signatures.union(&right_signatures).count() as f32;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

fn candidate_overlap(
    left: &[DerivedObjectSearchResult],
    right: &[DerivedObjectSearchResult],
) -> f32 {
    let left_candidates = left
        .iter()
        .filter_map(normalized_candidate_key)
        .collect::<BTreeSet<_>>();
    let right_candidates = right
        .iter()
        .filter_map(normalized_candidate_key)
        .collect::<BTreeSet<_>>();
    if left_candidates.is_empty() && right_candidates.is_empty() {
        return 1.0;
    }
    let intersection = left_candidates.intersection(&right_candidates).count() as f32;
    let union = left_candidates.union(&right_candidates).count() as f32;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

fn top1_equivalent(
    left: &[DerivedObjectSearchResult],
    right: &[DerivedObjectSearchResult],
) -> bool {
    match (left.first(), right.first()) {
        (Some(left), Some(right)) => result_equivalent(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn summary_top3_equivalent(
    left: &[DerivedObjectSearchResult],
    right: &[DerivedObjectSearchResult],
) -> bool {
    let left_summary = left
        .iter()
        .take(3)
        .find(|result| result.derived_object_type.as_str() == "summary");
    let right_summary = right
        .iter()
        .take(3)
        .find(|result| result.derived_object_type.as_str() == "summary");
    match (left_summary, right_summary) {
        (Some(left), Some(right)) => result_equivalent(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn result_equivalent(left: &DerivedObjectSearchResult, right: &DerivedObjectSearchResult) -> bool {
    if normalized_candidate_key(left).is_some()
        && normalized_candidate_key(left) == normalized_candidate_key(right)
    {
        return true;
    }

    let left_title = normalized_title(left);
    let right_title = normalized_title(right);
    if left_title.is_empty() || right_title.is_empty() {
        return false;
    }
    left_title == right_title || token_jaccard(&left_title, &right_title) >= 0.6
}

fn result_signatures(result: &DerivedObjectSearchResult) -> Vec<String> {
    let mut signatures = Vec::new();
    if let Some(candidate_key) = normalized_candidate_key(result) {
        signatures.push(format!("ck:{candidate_key}"));
    }
    let title = normalized_title(result);
    if !title.is_empty() {
        signatures.push(format!("title:{title}"));
    }
    signatures
}

fn normalized_candidate_key(result: &DerivedObjectSearchResult) -> Option<String> {
    result
        .candidate_key
        .as_ref()
        .map(|value| normalize_text(value))
        .filter(|value| !value.is_empty())
}

fn normalized_title(result: &DerivedObjectSearchResult) -> String {
    result
        .title
        .as_deref()
        .or(result.body_text.as_deref())
        .map(normalize_text)
        .unwrap_or_default()
}

fn normalize_text(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    let mut last_was_space = true;
    for ch in text.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            ' '
        };
        if mapped == ' ' {
            if !last_was_space {
                normalized.push(' ');
            }
            last_was_space = true;
        } else {
            normalized.push(mapped);
            last_was_space = false;
        }
    }
    normalized.trim().to_string()
}

fn token_jaccard(left: &str, right: &str) -> f32 {
    let left_tokens = left.split_whitespace().collect::<BTreeSet<_>>();
    let right_tokens = right.split_whitespace().collect::<BTreeSet<_>>();
    if left_tokens.is_empty() && right_tokens.is_empty() {
        return 1.0;
    }
    let intersection = left_tokens.intersection(&right_tokens).count() as f32;
    let union = left_tokens.union(&right_tokens).count() as f32;
    if union == 0.0 {
        0.0
    } else {
        intersection / union
    }
}

fn print_backend_results(label: &str, results: &[DerivedObjectSearchResult]) {
    println!("  {}:", label);
    for (index, result) in results.iter().enumerate() {
        let title = result
            .title
            .clone()
            .or_else(|| result.body_text.clone())
            .unwrap_or_else(|| "<untitled>".to_string());
        let title = truncate(&title, 88);
        println!(
            "    {}. [{}] {} ({}){}",
            index + 1,
            result.derived_object_type.as_str(),
            title,
            result.artifact_id,
            result
                .candidate_key
                .as_ref()
                .map(|value| format!(" <{}>", value))
                .unwrap_or_default()
        );
    }
    if results.is_empty() {
        println!("    <no results>");
    }
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out = text
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    out.push('…');
    out
}
