#![deny(warnings)]

#[path = "support/fixtures.rs"]
mod fixtures;

use open_archive::config::PostgresConfig;
use open_archive::migrations;
use open_archive::storage::{
    ArchiveSearchReadStore, ArtifactContextPackReadStore, ArtifactDetailReadStore,
    ImportWriteStore, PostgresImportWriteStore, PostgresRetrievalReadStore,
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
            "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive_retrieval_test"
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
fn postgres_retrieval_read_models_load_search_detail_and_context_material() {
    let Some(harness) = harness() else { return };
    let _guard = fixtures::lock_live_test();
    harness.reset_schema();

    let fixture = fixtures::make_test_import_fixture(&fixtures::unique_suffix("retpg"));
    let artifact_id = fixture.artifact_id.clone();
    let job_id = fixture.job_id.clone();
    let segment_ids = fixture.segment_ids.clone();

    PostgresImportWriteStore::new(harness.0.clone())
        .write_import(fixture.write_set)
        .expect("seed import should succeed");
    fixtures::seed_postgres_stub_derivations(&harness.0, &artifact_id, &job_id, &segment_ids);

    let read_store = PostgresRetrievalReadStore::new(harness.0.clone());

    let search_hits = read_store
        .search_candidates(
            "summary",
            10,
            &open_archive::storage::SearchFilters::default(),
        )
        .expect("search should succeed");
    assert!(!search_hits.is_empty());
    assert!(search_hits.iter().any(|hit| hit.artifact_id == artifact_id));

    let detail = read_store
        .load_artifact_detail(&artifact_id)
        .expect("detail should load")
        .expect("detail record should exist");
    assert_eq!(detail.artifact.artifact_id, artifact_id);
    assert_eq!(detail.segments.len(), 3);
    assert!(!detail.derived_objects.is_empty());

    let context = read_store
        .load_artifact_context_pack_material(&artifact_id)
        .expect("context material should load")
        .expect("context material should exist");
    assert_eq!(context.artifact.artifact_id, artifact_id);
    assert!(!context.derived_objects.is_empty());
    assert!(!context.evidence_links.is_empty());
}
