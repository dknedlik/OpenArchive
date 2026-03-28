#![deny(warnings)]

#[path = "support/contracts.rs"]
mod contracts;
#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/harness.rs"]
mod harness;

use harness::{ImportRecord, ProviderHarness};
use open_archive::config::PostgresConfig;
use open_archive::migrations;
use open_archive::storage::{ImportWriteStore, PostgresImportWriteStore};
use postgres::NoTls;
use std::sync::OnceLock;

fn postgres_config() -> Option<PostgresConfig> {
    if std::env::var("OA_POSTGRES_INTEGRATION_TESTS").is_err() {
        return None;
    }

    let connection_string = std::env::var("OA_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("OA_POSTGRES_URL"))
        .unwrap_or_else(|_| {
            "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive_import_test".to_string()
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

impl ProviderHarness for PostgresHarness {
    fn reset_schema(&self) {
        assert_eq!(
            std::env::var("OA_ALLOW_SCHEMA_RESET").as_deref(),
            Ok("1"),
            "refusing to reset integration schema without OA_ALLOW_SCHEMA_RESET=1"
        );
        recreate_test_database(&self.0);
        migrations::postgres::migrate(&self.0).expect("postgres schema migrate should succeed");
    }

    fn import_store(&self) -> Box<dyn ImportWriteStore> {
        Box::new(PostgresImportWriteStore::new(self.0.clone()))
    }

    fn job_store(&self) -> Box<dyn open_archive::storage::EnrichmentJobLifecycleStore> {
        Box::new(open_archive::storage::PostgresEnrichmentJobStore::new(
            self.0.clone(),
        ))
    }

    fn seed_existing_artifact(&self, import_set: &open_archive::storage::WriteImportSet) {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("seed connect");
        let payload = &import_set.payload_object;
        let import = &import_set.import;
        let artifact = &import_set.artifact_sets[0].artifact;

        client
            .execute(
                "INSERT INTO oa_object_ref \
                 (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &payload.object_id,
                    &"import_payload",
                    &payload.stored_object.provider,
                    &payload.stored_object.storage_key,
                    &payload.stored_object.mime_type,
                    &payload.stored_object.size_bytes,
                    &payload.stored_object.sha256,
                ],
            )
            .expect("seed payload object");

        client
            .execute(
                "INSERT INTO oa_import \
                 (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, \
                  conversation_count_detected, conversation_count_imported, conversation_count_failed, completed_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, 1, 0, NOW())",
                &[
                    &import.import_id,
                    &import.source_type.as_str(),
                    &open_archive::storage::ImportStatus::Completed.as_str(),
                    &import.payload_object_id,
                    &import.source_filename,
                    &import.source_content_hash,
                    &import.conversation_count_detected,
                ],
            )
            .expect("seed import");

        client
            .execute(
                "INSERT INTO oa_artifact \
                 (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status, \
                  source_conversation_hash, content_hash_version, content_facets_json, normalization_version) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::text::jsonb, $10)",
                &[
                    &artifact.artifact_id,
                    &artifact.import_id,
                    &artifact.artifact_class.as_str(),
                    &artifact.source_type.as_str(),
                    &artifact.artifact_status.as_str(),
                    &artifact.enrichment_status.as_str(),
                    &artifact.source_conversation_hash,
                    &artifact.content_hash_version,
                    &artifact.content_facets_json,
                    &artifact.normalization_version,
                ],
            )
            .expect("seed artifact");
    }

    fn fetch_import_record(&self, import_id: &str) -> ImportRecord {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        let row = client
            .query_one(
                "SELECT import_status, conversation_count_imported, conversation_count_failed, payload_object_id \
                 FROM oa_import WHERE import_id = $1",
                &[&import_id],
            )
            .expect("import row should exist");
        ImportRecord {
            status: row.get::<_, String>(0),
            count_imported: row.get::<_, i32>(1) as i64,
            count_failed: row.get::<_, i32>(2) as i64,
            payload_object_id: row.get::<_, String>(3),
        }
    }

    fn fetch_job_record(&self, _job_id: &str) -> harness::JobRecord {
        unreachable!("import harness does not exercise job verification")
    }

    fn count_payload_objects_by_sha256(&self, payload_sha256: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_object_ref WHERE sha256 = $1 AND object_kind = 'import_payload'",
                &[&payload_sha256],
            )
            .expect("payload count query")
            .get::<_, i64>(0)
    }

    fn count_artifacts_by_import_id(&self, import_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_artifact WHERE import_id = $1 OR artifact_id = $1",
                &[&import_id],
            )
            .expect("artifact count query")
            .get::<_, i64>(0)
    }

    fn count_artifacts_by_source_hash(&self, source_conversation_hash: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_artifact WHERE source_conversation_hash = $1",
                &[&source_conversation_hash],
            )
            .expect("artifact hash count query")
            .get::<_, i64>(0)
    }

    fn count_segments_by_artifact_id(&self, artifact_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_segment WHERE artifact_id = $1",
                &[&artifact_id],
            )
            .expect("segment count query")
            .get::<_, i64>(0)
    }

    fn count_participants_by_artifact_id(&self, artifact_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_conversation_participant WHERE artifact_id = $1",
                &[&artifact_id],
            )
            .expect("participant count query")
            .get::<_, i64>(0)
    }

    fn count_jobs_by_artifact_id(&self, artifact_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_enrichment_job WHERE artifact_id = $1",
                &[&artifact_id],
            )
            .expect("job count query")
            .get::<_, i64>(0)
    }
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_single_import_happy_path() {
    let Some(harness) = harness() else { return };
    contracts::contract_write_single_import_happy_path(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_import_duplicate_payload_is_idempotent() {
    let Some(harness) = harness() else { return };
    contracts::contract_write_import_duplicate_payload_is_idempotent(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_import_duplicate_artifact_hash_is_idempotent() {
    let Some(harness) = harness() else { return };
    contracts::contract_write_import_duplicate_artifact_hash_is_idempotent(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_import_partial_success_finalizes_completed_with_errors() {
    let Some(harness) = harness() else { return };
    contracts::contract_write_import_partial_success_finalizes_completed_with_errors(&harness);
}
