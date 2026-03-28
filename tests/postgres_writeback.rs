#![deny(warnings)]

#[path = "support/fixtures.rs"]
mod fixtures;

use open_archive::config::PostgresConfig;
use open_archive::migrations;
use open_archive::storage::{
    ImportWriteStore, NewAgentMemory, NewArchiveLink, PostgresImportWriteStore,
    PostgresWritebackStore, WritebackStore,
};
use postgres::NoTls;
use std::sync::OnceLock;

fn postgres_config() -> Option<PostgresConfig> {
    if std::env::var("OA_POSTGRES_INTEGRATION_TESTS").is_err() {
        return None;
    }

    let connection_string = std::env::var("OA_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("OA_POSTGRES_URL"))
        .unwrap_or_else(|_| {
            "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive_writeback_test"
                .to_string()
        });

    Some(PostgresConfig { connection_string })
}

fn admin_connection_string(connection_string: &str) -> String {
    let (base, _) = connection_string
        .rsplit_once('/')
        .expect("postgres connection string should include database name");
    format!("{base}/postgres")
}

fn recreate_test_database(config: &PostgresConfig) {
    let admin = admin_connection_string(&config.connection_string);
    let mut client =
        postgres::Client::connect(&admin, NoTls).expect("connect to postgres admin database");
    let database_name = config
        .connection_string
        .rsplit('/')
        .next()
        .expect("database name");
    client
        .execute(
            &format!("DROP DATABASE IF EXISTS {database_name} WITH (FORCE)"),
            &[],
        )
        .expect("drop integration database");
    client
        .execute(&format!("CREATE DATABASE {database_name}"), &[])
        .expect("create integration database");
}

fn harness() -> Option<PostgresHarness> {
    static CONFIG: OnceLock<Option<PostgresConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(postgres_config)
        .clone()
        .map(PostgresHarness)
}

struct PostgresHarness(PostgresConfig);

impl PostgresHarness {
    fn reset_schema(&self) {
        assert_eq!(
            std::env::var("OA_ALLOW_SCHEMA_RESET").as_deref(),
            Ok("1"),
            "refusing to reset integration schema without OA_ALLOW_SCHEMA_RESET=1"
        );
        recreate_test_database(&self.0);
        migrations::postgres::migrate(&self.0).expect("postgres schema migrate should succeed");
    }
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn postgres_writeback_store_persists_agent_memory_with_derivation_run() {
    let Some(harness) = harness() else { return };
    let _guard = fixtures::lock_live_test();
    harness.reset_schema();

    let fixture = fixtures::make_test_import_fixture(&fixtures::unique_suffix("wbmem"));
    let artifact_id = fixture.artifact_id.clone();

    PostgresImportWriteStore::new(harness.0.clone())
        .write_import(fixture.write_set)
        .expect("seed import should succeed");

    let store = PostgresWritebackStore::new(harness.0.clone());
    let derived_object_id = format!("dobj-{}", fixtures::unique_suffix("agentmem"));
    store
        .store_agent_memory(&NewAgentMemory {
            derived_object_id: derived_object_id.clone(),
            artifact_id: artifact_id.clone(),
            title: "Agent memory".to_string(),
            body_text: "Persisted by MCP writeback".to_string(),
            memory_type: "fact".to_string(),
            candidate_key: Some("agent-memory".to_string()),
            contributed_by: Some("test-agent".to_string()),
            evidence: vec![],
        })
        .expect("agent memory write should succeed");

    let derivation_run_id = format!("agentrun-{derived_object_id}");
    let mut client = open_archive::postgres_db::connect(&harness.0).expect("connect");
    let row = client
        .query_one(
            "SELECT o.artifact_id,
                    o.origin_kind,
                    o.derived_object_type,
                    o.scope_type,
                    o.scope_id,
                    o.candidate_key,
                    r.run_type,
                    r.provider_name,
                    r.input_scope_type
               FROM oa_derived_object o
               JOIN oa_derivation_run r ON r.derivation_run_id = o.derivation_run_id
              WHERE o.derived_object_id = $1
                AND r.derivation_run_id = $2",
            &[&derived_object_id, &derivation_run_id],
        )
        .expect("agent-contributed rows should exist");

    assert_eq!(row.get::<_, String>(0), artifact_id);
    assert_eq!(row.get::<_, String>(1), "agent_contributed");
    assert_eq!(row.get::<_, String>(2), "memory");
    assert_eq!(row.get::<_, String>(3), "artifact");
    assert_eq!(row.get::<_, String>(4), artifact_id);
    assert_eq!(
        row.get::<_, Option<String>>(5),
        Some("agent-memory".to_string())
    );
    assert_eq!(row.get::<_, String>(6), "agent_contributed");
    assert_eq!(
        row.get::<_, Option<String>>(7),
        Some("test-agent".to_string())
    );
    assert_eq!(row.get::<_, String>(8), "artifact");
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn postgres_writeback_link_upsert_keeps_one_canonical_row() {
    let Some(harness) = harness() else { return };
    let _guard = fixtures::lock_live_test();
    harness.reset_schema();

    let fixture_a = fixtures::make_test_import_fixture(&fixtures::unique_suffix("wblka"));
    let fixture_b = fixtures::make_test_import_fixture(&fixtures::unique_suffix("wblkb"));

    let import_store = PostgresImportWriteStore::new(harness.0.clone());
    import_store
        .write_import(fixture_a.write_set)
        .expect("seed import A should succeed");
    import_store
        .write_import(fixture_b.write_set)
        .expect("seed import B should succeed");

    let store = PostgresWritebackStore::new(harness.0.clone());
    let source_object_id = format!("dobj-{}", fixtures::unique_suffix("srcmem"));
    let target_object_id = format!("dobj-{}", fixtures::unique_suffix("tgtmem"));

    store
        .store_agent_memory(&NewAgentMemory {
            derived_object_id: source_object_id.clone(),
            artifact_id: fixture_a.artifact_id.clone(),
            title: "Source memory".to_string(),
            body_text: "First memory".to_string(),
            memory_type: "fact".to_string(),
            candidate_key: Some("shared-topic-a".to_string()),
            contributed_by: Some("writer-a".to_string()),
            evidence: vec![],
        })
        .expect("source memory write should succeed");
    store
        .store_agent_memory(&NewAgentMemory {
            derived_object_id: target_object_id.clone(),
            artifact_id: fixture_b.artifact_id.clone(),
            title: "Target memory".to_string(),
            body_text: "Second memory".to_string(),
            memory_type: "fact".to_string(),
            candidate_key: Some("shared-topic-b".to_string()),
            contributed_by: Some("writer-b".to_string()),
            evidence: vec![],
        })
        .expect("target memory write should succeed");

    let first_link_id = format!("alink-{}", fixtures::unique_suffix("first"));
    store
        .store_archive_link(&NewArchiveLink {
            archive_link_id: first_link_id.clone(),
            source_object_id: source_object_id.clone(),
            target_object_id: target_object_id.clone(),
            link_type: "same_topic".to_string(),
            confidence_score: Some(0.35),
            rationale: Some("Initial rationale".to_string()),
            contributed_by: Some("agent-a".to_string()),
        })
        .expect("initial link should succeed");

    store
        .store_archive_link(&NewArchiveLink {
            archive_link_id: format!("alink-{}", fixtures::unique_suffix("second")),
            source_object_id: source_object_id.clone(),
            target_object_id: target_object_id.clone(),
            link_type: "same_topic".to_string(),
            confidence_score: Some(0.92),
            rationale: Some("Updated rationale".to_string()),
            contributed_by: Some("agent-b".to_string()),
        })
        .expect("upserted link should succeed");

    let mut client = open_archive::postgres_db::connect(&harness.0).expect("connect");
    let row = client
        .query_one(
            "SELECT COUNT(*) OVER (),
                    archive_link_id,
                    confidence_score::double precision,
                    rationale,
                    contributed_by
               FROM oa_archive_link
              WHERE source_object_id = $1
                AND target_object_id = $2
                AND link_type = 'same_topic'",
            &[&source_object_id, &target_object_id],
        )
        .expect("canonical archive link row should exist");

    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), first_link_id);
    assert_eq!(row.get::<_, Option<f64>>(2), Some(0.92));
    assert_eq!(
        row.get::<_, Option<String>>(3),
        Some("Updated rationale".to_string())
    );
    assert_eq!(row.get::<_, Option<String>>(4), Some("agent-b".to_string()));
}
