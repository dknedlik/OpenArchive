mod support;

use open_archive::config::DbConfig;
use open_archive::migrations;
use open_archive::storage::{EnrichmentJobLifecycleStore, ImportWriteStore};
use std::sync::OnceLock;
use support::{ImportRecord, JobRecord, ProviderHarness};

fn oracle_config() -> Option<DbConfig> {
    if std::env::var("OA_INTEGRATION_TESTS").is_err() {
        return None;
    }
    if std::env::var("OA_ORACLE_CALL_TIMEOUT_MS").is_err() {
        std::env::set_var("OA_ORACLE_CALL_TIMEOUT_MS", "30000");
    }

    let mut config = DbConfig::from_env().ok()?;
    if let Ok(value) = std::env::var("OA_TEST_DB_USERNAME") {
        config.username = value;
    }
    if let Ok(value) = std::env::var("OA_TEST_DB_PASSWORD") {
        config.password = value;
    }
    if let Ok(value) = std::env::var("OA_TEST_TNS_ALIAS") {
        config.tns_alias = value;
    }
    if let Ok(value) = std::env::var("OA_TEST_WALLET_DIR") {
        config.wallet_dir = value.into();
    }
    Some(config)
}

fn harness() -> Option<OracleHarness> {
    static CONFIG: OnceLock<Option<DbConfig>> = OnceLock::new();
    CONFIG.get_or_init(oracle_config).clone().map(OracleHarness)
}

struct OracleHarness(DbConfig);

impl ProviderHarness for OracleHarness {
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

    fn import_store(&self) -> Box<dyn ImportWriteStore> {
        Box::new(open_archive::storage::OracleImportWriteStore::new(self.0.clone()))
    }

    fn job_store(&self) -> Box<dyn EnrichmentJobLifecycleStore> {
        Box::new(open_archive::storage::OracleEnrichmentJobStore::new(self.0.clone()))
    }

    fn seed_existing_artifact(&self, _import_set: &open_archive::storage::WriteImportSet) {
        unreachable!("job harness does not seed duplicate artifact rows directly")
    }

    fn fetch_import_record(&self, _import_id: &str) -> ImportRecord {
        unreachable!("job harness does not verify import rows")
    }

    fn fetch_job_record(&self, job_id: &str) -> JobRecord {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        let row = conn
            .query_row_as::<(String, i32, Option<String>, Option<String>)>(
                "SELECT job_status, attempt_count, claimed_by, last_error_message \
                 FROM oa_enrichment_job WHERE job_id = :1",
                &[&job_id],
            )
            .expect("job row should exist");
        JobRecord {
            status: row.0,
            attempt_count: row.1,
            claimed_by: row.2,
            error_message: row.3,
        }
    }

    fn count_payload_objects_by_sha256(&self, _payload_sha256: &str) -> i64 {
        unreachable!("job harness does not verify payload rows")
    }

    fn count_artifacts_by_import_id(&self, _import_id: &str) -> i64 {
        unreachable!("job harness does not verify import artifacts")
    }

    fn count_artifacts_by_source_hash(&self, _source_conversation_hash: &str) -> i64 {
        unreachable!("job harness does not verify artifact hashes")
    }

    fn count_segments_by_artifact_id(&self, _artifact_id: &str) -> i64 {
        unreachable!("job harness does not verify segments")
    }

    fn count_participants_by_artifact_id(&self, _artifact_id: &str) -> i64 {
        unreachable!("job harness does not verify participants")
    }

    fn count_jobs_by_artifact_id(&self, _artifact_id: &str) -> i64 {
        unreachable!("job harness does not verify job counts")
    }
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_claim_complete_happy_path() {
    let Some(harness) = harness() else { return };
    support::contract_claim_complete_happy_path(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_claim_fail_terminal() {
    let Some(harness) = harness() else { return };
    support::contract_claim_fail_terminal(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_claim_retryable_reclaim_complete() {
    let Some(harness) = harness() else { return };
    support::contract_claim_retryable_reclaim_complete(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_retryable_exhausted_becomes_terminal() {
    let Some(harness) = harness() else { return };
    support::contract_retryable_exhausted_becomes_terminal(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_claim_returns_none_when_empty() {
    let Some(harness) = harness() else { return };
    support::contract_claim_returns_none_when_empty(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_concurrent_claim_protection() {
    let Some(harness) = harness() else { return };
    support::contract_concurrent_claim_protection(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_payload_matches_documented_schema() {
    let Some(harness) = harness() else { return };
    support::contract_payload_matches_documented_schema(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_claim_skips_future_available_at() {
    let Some(harness) = harness() else { return };
    support::contract_claim_skips_future_available_at(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_non_claiming_worker_cannot_complete_job() {
    let Some(harness) = harness() else { return };
    support::contract_non_claiming_worker_cannot_complete_job(&harness);
}
