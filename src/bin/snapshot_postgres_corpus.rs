#![deny(warnings)]

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use open_archive::config::{QdrantConfig, SqliteConfig};
use open_archive::migrations;
use open_archive::sqlite_db;
use open_archive::storage::types::DerivedObjectType;
use open_archive::storage::{DerivedObjectEmbeddingStore, NewDerivedObjectEmbedding};
use open_archive::vector::QdrantVectorStore;
use postgres::{Client, NoTls};
use reqwest::blocking::Client as HttpClient;
use rusqlite::{params_from_iter, Connection};

const TABLE_COPIES: &[TableCopy] = &[
    TableCopy::new(
        "oa_object_ref",
        &[
            "object_id",
            "object_kind",
            "storage_provider",
            "storage_key",
            "mime_type",
            "size_bytes",
            "sha256",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_import",
        &[
            "import_id",
            "source_type",
            "import_status",
            "payload_object_id",
            "source_filename",
            "source_content_hash",
            "conversation_count_detected",
            "conversation_count_imported",
            "conversation_count_failed",
            "created_at",
            "completed_at",
            "error_message",
        ],
    ),
    TableCopy::new(
        "oa_artifact",
        &[
            "artifact_id",
            "import_id",
            "artifact_class",
            "source_type",
            "artifact_status",
            "enrichment_status",
            "source_conversation_key",
            "source_conversation_hash",
            "title",
            "created_at_source",
            "captured_at",
            "started_at",
            "ended_at",
            "primary_language",
            "content_hash_version",
            "content_facets_json",
            "normalization_version",
            "error_message",
        ],
    ),
    TableCopy::new(
        "oa_conversation_participant",
        &[
            "participant_id",
            "artifact_id",
            "participant_role",
            "display_name",
            "provider_name",
            "model_name",
            "source_participant_key",
            "sequence_no",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_segment",
        &[
            "segment_id",
            "artifact_id",
            "participant_id",
            "segment_type",
            "source_segment_key",
            "parent_segment_id",
            "sequence_no",
            "created_at_source",
            "text_content",
            "text_content_hash",
            "locator_json",
            "visibility_status",
            "unsupported_content_json",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_enrichment_job",
        &[
            "job_id",
            "artifact_id",
            "job_type",
            "job_status",
            "attempt_count",
            "max_attempts",
            "priority_no",
            "claimed_by",
            "claimed_at",
            "available_at",
            "payload_json",
            "last_error_message",
            "created_at",
            "completed_at",
            "enrichment_tier",
            "spawned_by_job_id",
            "required_capabilities",
        ],
    ),
    TableCopy::new(
        "oa_derivation_run",
        &[
            "derivation_run_id",
            "artifact_id",
            "job_id",
            "run_type",
            "pipeline_name",
            "pipeline_version",
            "provider_name",
            "model_name",
            "prompt_version",
            "run_status",
            "input_scope_type",
            "input_scope_json",
            "started_at",
            "completed_at",
            "error_message",
        ],
    ),
    TableCopy::new(
        "oa_derived_object",
        &[
            "derived_object_id",
            "artifact_id",
            "derivation_run_id",
            "derived_object_type",
            "origin_kind",
            "object_status",
            "confidence_score",
            "confidence_label",
            "scope_type",
            "scope_id",
            "title",
            "body_text",
            "object_json",
            "supersedes_derived_object_id",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_archive_link",
        &[
            "archive_link_id",
            "source_object_id",
            "target_object_id",
            "link_type",
            "confidence_score",
            "rationale",
            "origin_kind",
            "contributed_by",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_artifact_extraction_result",
        &[
            "extraction_result_id",
            "artifact_id",
            "job_id",
            "pipeline_name",
            "pipeline_version",
            "result_json",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_reconciliation_decision",
        &[
            "reconciliation_decision_id",
            "artifact_id",
            "job_id",
            "extraction_result_id",
            "pipeline_name",
            "pipeline_version",
            "decision_kind",
            "target_kind",
            "target_key",
            "matched_object_id",
            "rationale",
            "decision_json",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_enrichment_batch",
        &[
            "provider_batch_id",
            "provider_name",
            "stage_name",
            "phase_name",
            "owner_worker_id",
            "batch_status",
            "context_json",
            "submitted_at",
            "completed_at",
            "last_error_message",
        ],
    ),
    TableCopy::new(
        "oa_enrichment_batch_job",
        &["provider_batch_id", "job_id", "job_order"],
    ),
    TableCopy::new(
        "oa_review_decision",
        &[
            "review_decision_id",
            "item_id",
            "item_kind",
            "artifact_id",
            "derived_object_id",
            "decision_status",
            "note_text",
            "decided_by",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_artifact_note_property",
        &[
            "artifact_note_property_id",
            "artifact_id",
            "property_key",
            "value_kind",
            "value_text",
            "value_json",
            "sequence_no",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_artifact_note_tag",
        &[
            "artifact_note_tag_id",
            "artifact_id",
            "raw_tag",
            "normalized_tag",
            "tag_path",
            "source_kind",
            "sequence_no",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_artifact_note_alias",
        &[
            "artifact_note_alias_id",
            "artifact_id",
            "alias_text",
            "normalized_alias",
            "sequence_no",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_artifact_note_link",
        &[
            "artifact_note_link_id",
            "artifact_id",
            "source_segment_id",
            "link_kind",
            "target_kind",
            "raw_target",
            "normalized_target",
            "display_text",
            "target_path",
            "target_heading",
            "target_block",
            "external_url",
            "resolved_artifact_id",
            "resolution_status",
            "locator_json",
            "sequence_no",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_artifact_link",
        &[
            "artifact_link_id",
            "source_artifact_id",
            "target_artifact_id",
            "link_type",
            "link_value",
            "created_at",
        ],
    ),
    TableCopy::new(
        "oa_context_pack_cache",
        &[
            "context_pack_id",
            "artifact_id",
            "pack_type",
            "pack_status",
            "request_hash",
            "pack_json",
            "derivation_run_id",
            "created_at",
            "expires_at",
        ],
    ),
];

#[derive(Debug, Parser)]
#[command(name = "snapshot_postgres_corpus")]
#[command(about = "Snapshot the current Postgres corpus directly into SQLite and Qdrant")]
struct Args {
    #[arg(long = "postgres-url")]
    postgres_url: String,

    #[arg(
        long = "sqlite-path",
        default_value = "tmp/sqlite-snapshot/open_archive.db"
    )]
    sqlite_path: PathBuf,

    #[arg(long = "qdrant-url", default_value = "http://127.0.0.1:6333")]
    qdrant_url: String,

    #[arg(long = "qdrant-collection", default_value = "oa_sqlite_snapshot")]
    qdrant_collection: String,

    #[arg(long = "recreate", default_value_t = true)]
    recreate: bool,
}

#[derive(Debug, Clone, Copy)]
struct TableCopy {
    table: &'static str,
    columns: &'static [&'static str],
}

impl TableCopy {
    const fn new(table: &'static str, columns: &'static [&'static str]) -> Self {
        Self { table, columns }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(parent) = args.sqlite_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if args.recreate && args.sqlite_path.exists() {
        fs::remove_file(&args.sqlite_path)
            .with_context(|| format!("failed to remove {}", args.sqlite_path.display()))?;
    }

    let sqlite_config = SqliteConfig {
        path: args.sqlite_path.clone(),
        busy_timeout: Duration::from_secs(30),
    };
    migrations::sqlite::migrate(&sqlite_config).context("failed to migrate SQLite target")?;

    let mut pg = Client::connect(&args.postgres_url, NoTls)
        .with_context(|| format!("failed to connect to Postgres at {}", args.postgres_url))?;
    let mut sqlite = sqlite_db::connect(&sqlite_config).context("failed to open SQLite target")?;
    sqlite
        .execute_batch("PRAGMA foreign_keys = ON;")
        .context("failed to configure sqlite pragmas")?;

    for table in TABLE_COPIES {
        let copied = copy_table(&mut pg, &mut sqlite, table)
            .with_context(|| format!("failed to copy {}", table.table))?;
        println!("copied {:>6} rows into {}", copied, table.table);
    }

    recreate_qdrant_collection(&args.qdrant_url, &args.qdrant_collection)
        .context("failed to recreate Qdrant collection")?;
    let qdrant_store = QdrantVectorStore::new(
        QdrantConfig {
            url: args.qdrant_url.clone(),
            collection_name: args.qdrant_collection.clone(),
            request_timeout: Duration::from_secs(30),
            exact: true,
        },
        None,
    )
    .context("failed to initialize Qdrant vector store")?;
    let embedding_count = copy_embeddings_to_qdrant(&mut pg, &qdrant_store)
        .context("failed to load Qdrant vectors")?;
    println!(
        "copied {:>6} embeddings into Qdrant collection {}",
        embedding_count, args.qdrant_collection
    );

    Ok(())
}

fn copy_table(pg: &mut Client, sqlite: &mut Connection, table: &TableCopy) -> Result<usize> {
    let select_columns = table
        .columns
        .iter()
        .map(|column| format!("{column}::text AS {column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let order_by = table.columns.join(", ");
    let query = format!(
        "SELECT {select_columns} FROM {} ORDER BY {order_by}",
        table.table
    );
    let rows = pg
        .query(&query, &[])
        .with_context(|| format!("failed to read {}", table.table))?;

    let placeholders = (1..=table.columns.len())
        .map(|ix| format!("?{ix}"))
        .collect::<Vec<_>>()
        .join(", ");
    let insert = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table.table,
        table.columns.join(", "),
        placeholders
    );

    let tx = sqlite
        .transaction()
        .with_context(|| format!("failed to begin sqlite transaction for {}", table.table))?;
    let mut stmt = tx
        .prepare(&insert)
        .with_context(|| format!("failed to prepare sqlite insert for {}", table.table))?;
    for row in &rows {
        let values = (0..table.columns.len())
            .map(|index| row.get::<_, Option<String>>(index))
            .collect::<Vec<_>>();
        stmt.execute(params_from_iter(values))
            .with_context(|| format!("failed to insert row into {}", table.table))?;
    }
    drop(stmt);
    tx.commit()
        .with_context(|| format!("failed to commit sqlite copy for {}", table.table))?;
    Ok(rows.len())
}

fn recreate_qdrant_collection(qdrant_url: &str, collection: &str) -> Result<()> {
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to build Qdrant HTTP client")?;
    let url = format!(
        "{}/collections/{}",
        qdrant_url.trim_end_matches('/'),
        collection
    );
    let response = client
        .delete(&url)
        .send()
        .context("failed to delete Qdrant collection")?;
    match response.status().as_u16() {
        200 | 202 | 404 => Ok(()),
        status => {
            let body = response.text().unwrap_or_default();
            Err(anyhow!(
                "Qdrant collection delete returned HTTP {status}: {body}"
            ))
        }
    }
}

fn copy_embeddings_to_qdrant(pg: &mut Client, qdrant_store: &QdrantVectorStore) -> Result<usize> {
    let rows = pg
        .query(
            "SELECT derived_object_id,
                    artifact_id,
                    derived_object_type,
                    embedding_provider,
                    embedding_model,
                    content_text_hash,
                    embedding::text
               FROM oa_derived_object_embedding
              ORDER BY derived_object_id ASC",
            &[],
        )
        .context("failed to read embeddings from Postgres")?;

    let mut copied = 0usize;
    let mut batch = Vec::with_capacity(64);
    for row in rows {
        batch.push(NewDerivedObjectEmbedding {
            derived_object_id: row.get(0),
            artifact_id: row.get(1),
            derived_object_type: DerivedObjectType::parse(&row.get::<_, String>(2))
                .ok_or_else(|| anyhow!("invalid derived_object_type in embedding snapshot"))?,
            provider_name: row.get(3),
            model_name: row.get(4),
            content_text_hash: row.get(5),
            embedding: parse_pgvector_text(&row.get::<_, String>(6))
                .context("failed to parse pgvector embedding text")?,
        });
        if batch.len() >= 64 {
            qdrant_store
                .upsert_embeddings(&batch)
                .context("failed to upsert embedding batch into Qdrant")?;
            copied += batch.len();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        qdrant_store
            .upsert_embeddings(&batch)
            .context("failed to upsert final embedding batch into Qdrant")?;
        copied += batch.len();
    }
    Ok(copied)
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
