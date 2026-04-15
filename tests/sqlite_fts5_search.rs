//! End-to-end coverage for the SQLite FTS5 search path.
//!
//! These tests reproduce the regressions reported during local MCP testing:
//!
//! 1. multi-token plain queries should *broaden* results, not eliminate them
//!    ("dressage horse training" should still find a row that mentions
//!    "dressage" plus "horses" plus "training").
//! 2. result ranking should differentiate matches via FTS5 `bm25()`, not
//!    return identical hard-coded scores per match kind.
//!
//! Tests run against a real on-disk SQLite database (rusqlite `bundled` ships
//! with FTS5 enabled) so they exercise the actual FTS5 virtual tables, MATCH
//! syntax, and bm25 ranking that production uses.

use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};

use open_archive::config::SqliteConfig;
use open_archive::migrations;
use open_archive::storage::{
    ArchiveSearchCandidate, ArchiveSearchReadStore, DerivedObjectSearchStore, ObjectSearchFilters,
    SearchCandidateKind, SearchFilters, SqliteRetrievalReadStore,
};

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
            "open_archive_fts5_search_{}_{}_{}.sqlite",
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

fn seed_payload(conn: &Connection, suffix: &str) -> String {
    let object_id = format!("payload-{suffix}");
    conn.execute(
        "INSERT INTO oa_object_ref (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256)
         VALUES (?1, 'import_payload', 'test', ?2, 'application/json', 0, ?3)",
        params![object_id, format!("test/{object_id}"), format!("sha-{suffix}")],
    )
    .expect("insert payload");
    object_id
}

fn seed_import(conn: &Connection, suffix: &str, payload_id: &str) -> String {
    let import_id = format!("import-{suffix}");
    conn.execute(
        "INSERT INTO oa_import (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, conversation_count_detected, conversation_count_imported, conversation_count_failed)
         VALUES (?1, 'chatgpt_export', 'completed', ?2, ?3, ?4, 1, 1, 0)",
        params![
            import_id,
            payload_id,
            format!("export-{suffix}.json"),
            format!("conv-hash-{suffix}")
        ],
    )
    .expect("insert import");
    import_id
}

fn seed_artifact(conn: &Connection, suffix: &str, import_id: &str, title: &str) -> String {
    let artifact_id = format!("artifact-{suffix}");
    conn.execute(
        "INSERT INTO oa_artifact (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status, source_conversation_key, source_conversation_hash, title, content_hash_version, content_facets_json, normalization_version)
         VALUES (?1, ?2, 'conversation', 'chatgpt_export', 'normalized', 'completed', ?3, ?4, ?5, 'v1', '[\"messages\",\"text\"]', '1.0.0')",
        params![
            artifact_id,
            import_id,
            format!("src-key-{suffix}"),
            format!("conv-hash-art-{suffix}"),
            title,
        ],
    )
    .expect("insert artifact");
    artifact_id
}

fn seed_derivation_run(conn: &Connection, suffix: &str, artifact_id: &str) -> String {
    let run_id = format!("run-{suffix}");
    conn.execute(
        "INSERT INTO oa_derivation_run (derivation_run_id, artifact_id, run_type, pipeline_name, pipeline_version, run_status, input_scope_type, input_scope_json)
         VALUES (?1, ?2, 'artifact_extraction', 'test_pipeline', 'v1', 'completed', 'artifact', '{}')",
        params![run_id, artifact_id],
    )
    .expect("insert derivation run");
    run_id
}

#[allow(clippy::too_many_arguments)]
fn seed_derived_object(
    conn: &Connection,
    suffix: &str,
    artifact_id: &str,
    run_id: &str,
    object_type: &str,
    title: Option<&str>,
    body_text: Option<&str>,
    confidence_score: Option<f64>,
) -> String {
    let object_id = format!("derived-{suffix}");
    conn.execute(
        "INSERT INTO oa_derived_object (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status, confidence_score, scope_type, scope_id, title, body_text, object_json)
         VALUES (?1, ?2, ?3, ?4, 'inferred', 'active', ?5, 'artifact', ?2, ?6, ?7, '{}')",
        params![object_id, artifact_id, run_id, object_type, confidence_score, title, body_text],
    )
    .expect("insert derived object");
    object_id
}

fn seed_segment(
    conn: &Connection,
    suffix: &str,
    artifact_id: &str,
    sequence_no: i64,
    text_content: &str,
) -> String {
    let segment_id = format!("seg-{suffix}-{sequence_no}");
    conn.execute(
        "INSERT INTO oa_segment (segment_id, artifact_id, segment_type, sequence_no, text_content, text_content_hash, visibility_status)
         VALUES (?1, ?2, 'content_block', ?3, ?4, ?5, 'visible')",
        params![
            segment_id,
            artifact_id,
            sequence_no,
            text_content,
            format!("hash-{suffix}-{sequence_no}")
        ],
    )
    .expect("insert segment");
    segment_id
}

/// Seed three artifacts with derived objects whose body text overlaps the
/// query terms to varying degrees. Returns the artifact ids in the order
/// (high overlap, medium overlap, low overlap).
fn seed_dressage_corpus(conn: &Connection) -> (String, String, String) {
    let payload_id = seed_payload(conn, "main");
    let import_id = seed_import(conn, "main", &payload_id);

    // High-overlap artifact: title and derived body both mention all 3 terms.
    let high_artifact = seed_artifact(conn, "high", &import_id, "Dressage horse training drills");
    let high_run = seed_derivation_run(conn, "high", &high_artifact);
    seed_derived_object(
        conn,
        "high",
        &high_artifact,
        &high_run,
        "summary",
        Some("Dressage horse training plan"),
        Some("Notes on dressage flatwork drills for a young horse working through training level movements."),
        Some(0.9),
    );
    seed_segment(
        conn,
        "high",
        &high_artifact,
        0,
        "We talked about dressage flatwork for the young horse and our training goals for the season.",
    );

    // Medium-overlap artifact: only some of the terms are present.
    let medium_artifact = seed_artifact(
        conn,
        "medium",
        &import_id,
        "Horse stable maintenance checklist",
    );
    let medium_run = seed_derivation_run(conn, "medium", &medium_artifact);
    seed_derived_object(
        conn,
        "medium",
        &medium_artifact,
        &medium_run,
        "summary",
        Some("Stable upkeep notes"),
        Some("Daily routines for keeping a horse stall clean. Mentions training schedule indirectly."),
        Some(0.7),
    );
    seed_segment(
        conn,
        "medium",
        &medium_artifact,
        0,
        "The horse needs fresh shavings each morning before the training session begins.",
    );

    // Low-overlap artifact: contains a single term elsewhere in body text.
    let low_artifact = seed_artifact(conn, "low", &import_id, "Pasture rotation log");
    let low_run = seed_derivation_run(conn, "low", &low_artifact);
    seed_derived_object(
        conn,
        "low",
        &low_artifact,
        &low_run,
        "summary",
        Some("Pasture rotation log entry"),
        Some("Rotated the herd to the back field. The new horse is settling in well."),
        Some(0.5),
    );

    (high_artifact, medium_artifact, low_artifact)
}

fn collect_artifact_ids(candidates: &[ArchiveSearchCandidate]) -> Vec<String> {
    candidates.iter().map(|c| c.artifact_id.clone()).collect()
}

#[test]
fn search_candidates_multi_token_query_returns_partial_matches() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let (high, medium, _low) = seed_dressage_corpus(&conn);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());

    // Single-token query — sanity check that FTS5 matching is wired up at all.
    let single = store
        .search_candidates("dressage", 20, &SearchFilters::default())
        .expect("single-token search");
    let single_artifacts = collect_artifact_ids(&single);
    assert!(
        single_artifacts.contains(&high),
        "single-token 'dressage' should hit the high-overlap artifact, got {single_artifacts:?}"
    );

    // The regression: a longer phrase used to return zero hits because LIKE
    // required the whole phrase as a contiguous substring. With FTS5 + OR
    // semantics, adding tokens *broadens* recall.
    let multi = store
        .search_candidates("dressage horse training", 20, &SearchFilters::default())
        .expect("multi-token search");
    let multi_artifacts = collect_artifact_ids(&multi);
    assert!(
        multi_artifacts.contains(&high),
        "multi-token query should still hit the high-overlap artifact, got {multi_artifacts:?}"
    );
    assert!(
        multi_artifacts.contains(&medium),
        "multi-token query should also pick up the medium-overlap artifact via OR semantics, got {multi_artifacts:?}"
    );
}

#[test]
fn search_candidates_scores_differentiate_derived_object_hits() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let (_high, _medium, _low) = seed_dressage_corpus(&conn);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());
    let candidates = store
        .search_candidates("dressage horse training", 20, &SearchFilters::default())
        .expect("search should succeed");

    // Pull the derived-object hits and confirm we got at least two so
    // ranking can actually be compared.
    let derived_scores: Vec<i32> = candidates
        .iter()
        .filter(|c| matches!(c.match_kind, SearchCandidateKind::DerivedObject { .. }))
        .map(|c| c.score_hint)
        .collect();
    assert!(
        derived_scores.len() >= 2,
        "expected at least two derived-object hits to compare, got {derived_scores:?}"
    );

    // The previous implementation returned a hard-coded `260` for every
    // derived-object hit. After bm25 propagation the better match must
    // outscore the worse one.
    let max_score = *derived_scores.iter().max().unwrap();
    let min_score = *derived_scores.iter().min().unwrap();
    assert!(
        max_score > min_score,
        "derived-object scores should differentiate via bm25, got {derived_scores:?}"
    );
    assert!(
        max_score > 260,
        "best derived-object score should exceed the hard-coded base of 260, got {max_score}"
    );
}

#[test]
fn search_candidates_punctuation_falls_back_to_plain_mode() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let _ = seed_dressage_corpus(&conn);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());

    // Stray punctuation (a half-finished phrase) used to crash FTS5 parsing.
    // The detector now requires balanced quotes before entering Operator mode,
    // so this should silently degrade to a plain bag-of-words query.
    let result =
        store.search_candidates("dressage \"horse training", 20, &SearchFilters::default());
    assert!(
        result.is_ok(),
        "unbalanced quote should not error out, got {result:?}"
    );
    let candidates = result.unwrap();
    assert!(
        !candidates.is_empty(),
        "unbalanced quote query should still return matches via plain-mode fallback"
    );
}

#[test]
fn search_objects_multi_token_query_uses_fts5_or_semantics() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let (high, medium, _low) = seed_dressage_corpus(&conn);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());

    let results = store
        .search_objects(
            &ObjectSearchFilters {
                query: Some("dressage horse training".to_string()),
                object_type: None,
                candidate_key: None,
                artifact_id: None,
            },
            20,
        )
        .expect("object search should succeed");

    let artifacts: Vec<String> = results.iter().map(|r| r.artifact_id.clone()).collect();
    assert!(
        artifacts.contains(&high),
        "multi-token object search should hit the high-overlap artifact, got {artifacts:?}"
    );
    assert!(
        artifacts.contains(&medium),
        "multi-token object search should also hit the medium-overlap artifact, got {artifacts:?}"
    );

    // bm25 should produce a positive lexical score for matches.
    for hit in &results {
        let score = hit.score.unwrap_or(0.0);
        assert!(
            score > 0.0,
            "object search hit {} should have a positive bm25-derived score, got {score}",
            hit.derived_object_id
        );
    }

    // The best match should outscore the worst.
    let best = results.first().expect("at least one hit");
    let worst = results.last().expect("at least one hit");
    if results.len() >= 2 {
        assert!(
            best.score.unwrap_or(0.0) >= worst.score.unwrap_or(0.0),
            "results should be sorted with best bm25 score first, got {results:?}"
        );
    }
}

#[test]
fn search_objects_without_query_falls_back_to_confidence_order() {
    let db = TempSqliteDb::create();
    let conn = db.connection();
    let (high, medium, low) = seed_dressage_corpus(&conn);
    drop(conn);

    let store = SqliteRetrievalReadStore::new(db.config.clone());

    let results = store
        .search_objects(
            &ObjectSearchFilters {
                query: None,
                object_type: None,
                candidate_key: None,
                artifact_id: None,
            },
            20,
        )
        .expect("query-less object search should succeed");

    // With no query we keep the legacy confidence-ordered behaviour. The seeded
    // confidence scores are 0.9 / 0.7 / 0.5, so the order should follow.
    let artifacts: Vec<String> = results.iter().map(|r| r.artifact_id.clone()).collect();
    let high_pos = artifacts.iter().position(|id| id == &high);
    let medium_pos = artifacts.iter().position(|id| id == &medium);
    let low_pos = artifacts.iter().position(|id| id == &low);
    assert!(
        high_pos.is_some() && medium_pos.is_some() && low_pos.is_some(),
        "query-less search should return all seeded objects, got {artifacts:?}"
    );
    assert!(high_pos.unwrap() < medium_pos.unwrap());
    assert!(medium_pos.unwrap() < low_pos.unwrap());

    // Without a query we don't compute bm25 — score should be `None`.
    for hit in &results {
        assert!(
            hit.score.is_none(),
            "query-less search should leave score unset, got {:?}",
            hit.score
        );
    }
}
