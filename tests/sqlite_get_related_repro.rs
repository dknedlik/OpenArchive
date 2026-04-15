//! Probe for the reported `get_related` hang.
//!
//! Builds a realistic-ish SQLite corpus (many derived objects, a fan of
//! archive links, lots of shared candidate keys) and times
//! `get_related_objects`. If the call wedges or scales pathologically the
//! 10-second wall clock check below will fail.

use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};

use open_archive::config::SqliteConfig;
use open_archive::migrations;
use open_archive::storage::{DerivedObjectSearchStore, SqliteRetrievalReadStore};

static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempSqliteDb {
    config: SqliteConfig,
    path: PathBuf,
}

impl TempSqliteDb {
    fn create() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let counter = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "open_archive_get_related_repro_{}_{}_{}.sqlite",
            std::process::id(),
            nanos,
            counter,
        ));
        let _ = std::fs::remove_file(&path);
        let config = SqliteConfig {
            path: path.clone(),
            busy_timeout: Duration::from_secs(5),
        };
        migrations::sqlite::migrate(&config).expect("sqlite migration should succeed");
        Self { config, path }
    }

    fn connection(&self) -> Connection {
        open_archive::sqlite_db::connect(&self.config).expect("sqlite connect")
    }
}

impl Drop for TempSqliteDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(self.path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(self.path.with_extension("sqlite-shm"));
    }
}

fn seed_corpus(conn: &Connection, n_objects: usize, n_links: usize) -> String {
    // Single payload + import + artifact + run, then many derived objects.
    conn.execute(
        "INSERT INTO oa_object_ref (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256)
         VALUES ('payload-x', 'import_payload', 'test', 'test/payload', 'application/json', 0, 'sha-x')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO oa_import (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, conversation_count_detected, conversation_count_imported, conversation_count_failed)
         VALUES ('import-x', 'chatgpt_export', 'completed', 'payload-x', 'export.json', 'h-x', 1, 1, 0)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO oa_artifact (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status, source_conversation_key, source_conversation_hash, title, content_hash_version, content_facets_json, normalization_version)
         VALUES ('art-x', 'import-x', 'conversation', 'chatgpt_export', 'normalized', 'completed', 'k-x', 'h-art-x', 'title', 'v1', '[]', '1.0.0')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO oa_derivation_run (derivation_run_id, artifact_id, run_type, pipeline_name, pipeline_version, run_status, input_scope_type, input_scope_json)
         VALUES ('run-x', 'art-x', 'artifact_extraction', 't', 'v1', 'completed', 'artifact', '{}')",
        [],
    )
    .unwrap();

    // Insert n_objects derived objects. Half share a hot candidate_key so the
    // self-join has a fan-out of ~n_objects/2. candidate_key is a STORED
    // generated column sourced from object_json.$.candidate_key.
    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..n_objects {
        let key = if i % 2 == 0 {
            "hot-key".to_string()
        } else {
            format!("cold-key-{i}")
        };
        let object_json = format!("{{\"candidate_key\":\"{key}\"}}");
        tx.execute(
            "INSERT INTO oa_derived_object (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status, confidence_score, scope_type, scope_id, title, body_text, object_json)
             VALUES (?1, 'art-x', 'run-x', 'summary', 'inferred', 'active', 0.5, 'artifact', 'art-x', ?2, ?3, ?4)",
            params![format!("d-{i}"), format!("title {i}"), format!("body {i}"), object_json],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    // Source object id we will query against.
    let source = "d-0".to_string();

    // Fan a bunch of archive_links off the source object.
    let tx = conn.unchecked_transaction().unwrap();
    for i in 0..n_links {
        let target_idx = (i * 2) + 1; // odd ids so we don't double-up with hot key
        if target_idx >= n_objects {
            break;
        }
        tx.execute(
            "INSERT INTO oa_archive_link (archive_link_id, source_object_id, target_object_id, link_type, confidence_score, rationale, origin_kind)
             VALUES (?1, ?2, ?3, 'same_topic', 0.8, 'r', 'agent_contributed')",
            params![format!("al-{i}"), source, format!("d-{target_idx}")],
        )
        .unwrap();
    }
    tx.commit().unwrap();

    source
}

#[test]
fn get_related_objects_completes_under_time_budget() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    // 50k objects, 50 archive links from d-0. Half share a hot candidate_key
    // (25k matches) — this is the size where a pathological plan in the
    // `candidate_key` self-join would burn seconds.
    let source = seed_corpus(&conn, 50_000, 50);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());

    let start = Instant::now();
    let result = store.get_related_objects(&source, 20).expect("get_related");
    let elapsed = start.elapsed();

    eprintln!(
        "get_related_objects returned {} entries in {:?}",
        result.len(),
        elapsed
    );

    assert!(
        elapsed < Duration::from_secs(2),
        "get_related_objects took {elapsed:?} on a 5k-object corpus — likely the reported hang"
    );
    assert!(!result.is_empty(), "expected at least one related entry");
}

#[test]
fn get_related_objects_for_unknown_id_returns_empty_quickly() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let _ = seed_corpus(&conn, 1_000, 0);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());

    let start = Instant::now();
    let result = store
        .get_related_objects("does-not-exist", 20)
        .expect("get_related");
    let elapsed = start.elapsed();

    eprintln!("unknown id returned {} entries in {:?}", result.len(), elapsed);
    assert!(elapsed < Duration::from_secs(1));
    assert!(result.is_empty());
}

#[test]
fn get_related_objects_explain_query_plan() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let _ = seed_corpus(&conn, 5_000, 50);

    let plan = |sql: &str| -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("prepare explain");
        let rows = stmt
            .query_map(params!["d-0", 20i64], |row| {
                Ok(format!(
                    "id={} parent={} notused={} detail={}",
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?
                ))
            })
            .expect("query plan");
        rows.collect::<Result<Vec<_>, _>>().expect("plan rows")
    };

    eprintln!("=== archive_link plan ===");
    for line in plan(
        "SELECT d.derived_object_id FROM oa_archive_link al \
         JOIN oa_derived_object d ON d.derived_object_id = CASE \
            WHEN al.source_object_id = ?1 THEN al.target_object_id \
            ELSE al.source_object_id END \
         WHERE (al.source_object_id = ?1 OR al.target_object_id = ?1) \
           AND d.object_status = 'active' \
         ORDER BY d.derived_object_id ASC LIMIT ?2",
    ) {
        eprintln!("  {line}");
    }
    eprintln!("=== candidate_key plan ===");
    for line in plan(
        "SELECT d.derived_object_id FROM oa_derived_object source \
         JOIN oa_derived_object d ON d.candidate_key = source.candidate_key \
            AND d.derived_object_id <> source.derived_object_id \
         WHERE source.derived_object_id = ?1 \
           AND COALESCE(source.candidate_key, '') <> '' \
           AND d.object_status = 'active' \
         ORDER BY d.derived_object_id ASC LIMIT ?2",
    ) {
        eprintln!("  {line}");
    }
}

#[test]
fn get_related_objects_for_object_with_null_candidate_key_terminates() {
    // Reproduces the case where the source object exists but has a NULL
    // candidate_key — the second self-join branch must short-circuit instead
    // of joining every NULL row in the table.
    let db = TempSqliteDb::create();
    let conn = db.connection();
    seed_corpus(&conn, 0, 0); // base scaffolding
    // Insert a single object with an empty json (no candidate_key).
    conn.execute(
        "INSERT INTO oa_derived_object (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status, confidence_score, scope_type, scope_id, title, body_text, object_json)
         VALUES ('null-key', 'art-x', 'run-x', 'summary', 'inferred', 'active', 0.5, 'artifact', 'art-x', 't', 'b', '{}')",
        [],
    )
    .unwrap();
    // Add many neighbours that also have NULL candidate_key — partial index
    // excludes them, but a bad query plan could still scan them.
    for i in 0..2_000 {
        conn.execute(
            "INSERT INTO oa_derived_object (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status, confidence_score, scope_type, scope_id, title, body_text, object_json)
             VALUES (?1, 'art-x', 'run-x', 'summary', 'inferred', 'active', 0.5, 'artifact', 'art-x', 't', 'b', '{}')",
            params![format!("nk-{i}")],
        )
        .unwrap();
    }
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());
    let start = Instant::now();
    let result = store.get_related_objects("null-key", 20).expect("get_related");
    let elapsed = start.elapsed();
    eprintln!("null-key returned {} entries in {:?}", result.len(), elapsed);
    assert!(elapsed < Duration::from_secs(1));
    assert!(result.is_empty());
}
