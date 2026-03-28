#![deny(warnings)]

#[path = "support/fixtures.rs"]
mod fixtures;

use open_archive::app::retrieval::{ArchiveRetrievalService, ArchiveRetrievalServiceApi};
use open_archive::config::{HttpConfig, PostgresConfig};
use open_archive::enrichment_worker::start_enrichment_workers;
use open_archive::migrations;
use open_archive::shutdown::ShutdownToken;
use open_archive::storage::{
    ArtifactReadStore, DerivedMetadataWriteStore, EnrichmentJobLifecycleStore,
    EnrichmentStateStore, ImportWriteStore, PostgresArtifactReadStore,
    PostgresDerivedMetadataStore, PostgresEnrichmentJobStore, PostgresImportWriteStore,
    PostgresRetrievalReadStore,
};
use postgres::NoTls;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

fn postgres_config() -> Option<PostgresConfig> {
    if std::env::var("OA_POSTGRES_INTEGRATION_TESTS").is_err() {
        return None;
    }

    let connection_string = std::env::var("OA_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("OA_POSTGRES_URL"))
        .unwrap_or_else(|_| {
            "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive_worker_test".to_string()
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

    fn import_store(&self) -> Arc<dyn ImportWriteStore> {
        Arc::new(PostgresImportWriteStore::new(self.0.clone()))
    }

    fn read_store(&self) -> Arc<dyn ArtifactReadStore> {
        Arc::new(PostgresArtifactReadStore::new(self.0.clone()))
    }

    fn job_store(&self) -> Arc<dyn EnrichmentJobLifecycleStore> {
        Arc::new(PostgresEnrichmentJobStore::new(self.0.clone()))
    }

    fn derived_store(&self) -> Arc<dyn DerivedMetadataWriteStore> {
        Arc::new(PostgresDerivedMetadataStore::new(self.0.clone()))
    }

    fn retrieval_store(&self) -> Arc<dyn ArchiveRetrievalServiceApi> {
        Arc::new(ArchiveRetrievalService::new(Arc::new(
            PostgresRetrievalReadStore::new(self.0.clone()),
        )))
    }

    fn state_store(&self) -> Arc<dyn EnrichmentStateStore> {
        Arc::new(PostgresDerivedMetadataStore::new(self.0.clone()))
    }
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_stub_worker_persists_derivations_and_completes_job() {
    let Some(harness) = harness() else { return };
    let _guard = fixtures::lock_live_test();
    harness.reset_schema();

    let fixture = fixtures::make_test_import_fixture(&fixtures::unique_suffix("wrkpg"));
    let job_id = fixture.job_id.clone();
    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("seed import should succeed");

    let shutdown = ShutdownToken::new();
    let workers = start_enrichment_workers(
        &HttpConfig {
            bind_addr: "127.0.0.1:3000".to_string(),
            request_worker_count: 1,
            enrichment_worker_count: 1,
            enrichment_poll_interval_ms: 10,
        },
        harness.job_store(),
        harness.read_store(),
        harness.retrieval_store(),
        harness.state_store(),
        harness.derived_store(),
        shutdown.clone(),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(150));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    let mut client = open_archive::postgres_db::connect(&harness.0).expect("connect");
    let status: String = client
        .query_one(
            "SELECT job_status FROM oa_enrichment_job WHERE job_id = $1",
            &[&job_id],
        )
        .expect("job row should exist")
        .get(0);
    assert_eq!(status, "completed");

    let run_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM oa_derivation_run WHERE job_id = $1",
            &[&job_id],
        )
        .expect("run count")
        .get(0);
    assert_eq!(run_count, 1);

    let object_count: i64 = client
        .query_one(
            "SELECT COUNT(*) \
             FROM oa_derived_object o \
             JOIN oa_derivation_run r ON r.derivation_run_id = o.derivation_run_id \
             WHERE r.job_id = $1",
            &[&job_id],
        )
        .expect("object count")
        .get(0);
    assert!(object_count >= 3);

    let evidence_count: i64 = client
        .query_one(
            "SELECT COUNT(*) \
             FROM oa_evidence_link e \
             JOIN oa_derived_object o ON o.derived_object_id = e.derived_object_id \
             JOIN oa_derivation_run r ON r.derivation_run_id = o.derivation_run_id \
             WHERE r.job_id = $1",
            &[&job_id],
        )
        .expect("evidence count")
        .get(0);
    assert!(evidence_count >= 3);
}
