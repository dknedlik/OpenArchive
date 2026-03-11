mod support;

use open_archive::config::PostgresConfig;
use open_archive::migrations;
use open_archive::storage::{EnrichmentJobLifecycleStore, ImportWriteStore};
use postgres::NoTls;
use std::sync::OnceLock;
use support::{ImportRecord, JobRecord, ProviderHarness};

fn postgres_config() -> Option<PostgresConfig> {
    if std::env::var("OA_PG_INTEGRATION_TESTS").is_err() {
        return None;
    }

    let connection_string = std::env::var("OA_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("OA_POSTGRES_URL"))
        .unwrap_or_else(|_| "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive_job_test".to_string());

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
        .execute(&format!("DROP DATABASE IF EXISTS {database_name} WITH (FORCE)"), &[])
        .expect("drop integration database");
    client
        .execute(&format!("CREATE DATABASE {database_name}"), &[])
        .expect("create integration database");
}

fn harness() -> Option<PostgresHarness> {
    static CONFIG: OnceLock<Option<PostgresConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            postgres_config()
        })
        .clone()
        .map(PostgresHarness)
}

struct PostgresHarness(PostgresConfig);

impl ProviderHarness for PostgresHarness {
    fn reset_schema(&self) {
        recreate_test_database(&self.0);
        migrations::postgres::migrate(&self.0).expect("postgres schema migrate should succeed");
    }

    fn import_store(&self) -> Box<dyn ImportWriteStore> {
        Box::new(open_archive::storage::PostgresImportWriteStore::new(self.0.clone()))
    }

    fn job_store(&self) -> Box<dyn EnrichmentJobLifecycleStore> {
        Box::new(open_archive::storage::PostgresEnrichmentJobStore::new(self.0.clone()))
    }

    fn seed_existing_artifact(&self, _import_set: &open_archive::storage::WriteImportSet) {
        unreachable!("job harness does not seed duplicate artifact rows directly")
    }

    fn fetch_import_record(&self, _import_id: &str) -> ImportRecord {
        unreachable!("job harness does not verify import rows")
    }

    fn fetch_job_record(&self, job_id: &str) -> JobRecord {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        let row = client
            .query_one(
                "SELECT job_status, attempt_count, claimed_by, last_error_message \
                 FROM oa_enrichment_job WHERE job_id = $1",
                &[&job_id],
            )
            .expect("job row should exist");
        JobRecord {
            status: row.get::<_, String>(0),
            attempt_count: row.get::<_, i32>(1),
            claimed_by: row.get::<_, Option<String>>(2),
            error_message: row.get::<_, Option<String>>(3),
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
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_claim_complete_happy_path() {
    let Some(harness) = harness() else { return };
    support::contract_claim_complete_happy_path(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_claim_fail_terminal() {
    let Some(harness) = harness() else { return };
    support::contract_claim_fail_terminal(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_claim_retryable_reclaim_complete() {
    let Some(harness) = harness() else { return };
    support::contract_claim_retryable_reclaim_complete(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_retryable_exhausted_becomes_terminal() {
    let Some(harness) = harness() else { return };
    support::contract_retryable_exhausted_becomes_terminal(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_claim_returns_none_when_empty() {
    let Some(harness) = harness() else { return };
    support::contract_claim_returns_none_when_empty(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_concurrent_claim_protection() {
    let Some(harness) = harness() else { return };
    support::contract_concurrent_claim_protection(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_payload_matches_documented_schema() {
    let Some(harness) = harness() else { return };
    support::contract_payload_matches_documented_schema(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_claim_skips_future_available_at() {
    let Some(harness) = harness() else { return };
    support::contract_claim_skips_future_available_at(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_PG_INTEGRATION_TESTS=1"]
fn test_non_claiming_worker_cannot_complete_job() {
    let Some(harness) = harness() else { return };
    support::contract_non_claiming_worker_cannot_complete_job(&harness);
}
