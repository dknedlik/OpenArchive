mod support;

use open_archive::config::{HttpConfig, OracleConfig};
use open_archive::enrichment_worker::start_enrichment_workers;
use open_archive::migrations;
use open_archive::shutdown::ShutdownToken;
use open_archive::storage::{
    ArchiveRetrievalStore, ArtifactReadStore, DerivedMetadataWriteStore,
    EnrichmentJobLifecycleStore, EnrichmentStateStore, ImportWriteStore,
    OracleDerivedMetadataStore, OracleEnrichmentJobStore, OracleImportWriteStore,
};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

fn oracle_config() -> Option<OracleConfig> {
    if std::env::var("OA_ORACLE_INTEGRATION_TESTS").is_err() {
        return None;
    }

    let mut config = OracleConfig::from_env().ok()?;
    if let Ok(value) = std::env::var("OA_TEST_ORACLE_USERNAME") {
        config.username = value;
    }
    if let Ok(value) = std::env::var("OA_TEST_ORACLE_PASSWORD") {
        config.password = value;
    }
    if let Ok(value) = std::env::var("OA_TEST_ORACLE_CONNECT_STRING") {
        config.connect_string = value;
    }
    Some(config)
}

fn harness() -> Option<OracleHarness> {
    static CONFIG: OnceLock<Option<OracleConfig>> = OnceLock::new();
    CONFIG.get_or_init(oracle_config).clone().map(OracleHarness)
}

struct OracleHarness(OracleConfig);

impl OracleHarness {
    fn reset_schema(&self) {
        assert_eq!(
            std::env::var("OA_ALLOW_SCHEMA_RESET").as_deref(),
            Ok("1"),
            "refusing to reset integration schema without OA_ALLOW_SCHEMA_RESET=1"
        );
        migrations::oracle::reset(&self.0)
            .and_then(|_| migrations::oracle::migrate(&self.0))
            .expect("oracle schema reset should succeed");
    }

    fn import_store(&self) -> Arc<dyn ImportWriteStore> {
        Arc::new(OracleImportWriteStore::new(self.0.clone()))
    }

    fn read_store(&self) -> Arc<dyn ArtifactReadStore> {
        Arc::new(OracleImportWriteStore::new(self.0.clone()))
    }

    fn job_store(&self) -> Arc<dyn EnrichmentJobLifecycleStore> {
        Arc::new(OracleEnrichmentJobStore::new(self.0.clone()))
    }

    fn derived_store(&self) -> Arc<dyn DerivedMetadataWriteStore> {
        Arc::new(OracleDerivedMetadataStore::new(self.0.clone()))
    }

    fn retrieval_store(&self) -> Arc<dyn ArchiveRetrievalStore> {
        Arc::new(OracleImportWriteStore::new(self.0.clone()))
    }

    fn state_store(&self) -> Arc<dyn EnrichmentStateStore> {
        Arc::new(OracleDerivedMetadataStore::new(self.0.clone()))
    }
}

#[test]
#[ignore = "requires local Oracle; set OA_ORACLE_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_stub_worker_persists_derivations_and_completes_job() {
    let Some(harness) = harness() else { return };
    let _guard = support::lock_live_test();
    harness.reset_schema();

    let fixture = support::make_test_import_fixture(&support::unique_suffix("wrkor"));
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

    let conn = open_archive::db::connect(&harness.0).expect("connect");
    let (status,): (String,) = conn
        .query_row_as(
            "SELECT job_status FROM oa_enrichment_job WHERE job_id = :1",
            &[&job_id],
        )
        .expect("job row should exist");
    assert_eq!(status, "completed");

    let (run_count,): (i64,) = conn
        .query_row_as(
            "SELECT COUNT(*) FROM oa_derivation_run WHERE job_id = :1",
            &[&job_id],
        )
        .expect("run count");
    assert_eq!(run_count, 1);

    let (object_count,): (i64,) = conn
        .query_row_as(
            "SELECT COUNT(*) \
             FROM oa_derived_object o \
             JOIN oa_derivation_run r ON r.derivation_run_id = o.derivation_run_id \
             WHERE r.job_id = :1",
            &[&job_id],
        )
        .expect("object count");
    assert!(object_count >= 3);

    let (evidence_count,): (i64,) = conn
        .query_row_as(
            "SELECT COUNT(*) \
             FROM oa_evidence_link e \
             JOIN oa_derived_object o ON o.derived_object_id = e.derived_object_id \
             JOIN oa_derivation_run r ON r.derivation_run_id = o.derivation_run_id \
             WHERE r.job_id = :1",
            &[&job_id],
        )
        .expect("evidence count");
    assert!(evidence_count >= 3);
}
