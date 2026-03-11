mod support;

use open_archive::config::DbConfig;
use open_archive::migrations;
use open_archive::storage::{ImportWriteStore, OracleImportWriteStore};
use support::{ImportRecord, ProviderHarness};
use std::sync::OnceLock;

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
        Box::new(OracleImportWriteStore::new(self.0.clone()))
    }

    fn job_store(&self) -> Box<dyn open_archive::storage::EnrichmentJobLifecycleStore> {
        Box::new(open_archive::storage::OracleEnrichmentJobStore::new(self.0.clone()))
    }

    fn seed_existing_artifact(&self, import_set: &open_archive::storage::WriteImportSet) {
        let conn = open_archive::db::connect(&self.0).expect("seed connect");
        let payload = &import_set.payload_object;
        let import = &import_set.import;
        let artifact = &import_set.artifact_sets[0].artifact;

        conn.execute(
            "INSERT INTO oa_object_ref \
             (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256) \
             VALUES (:1, :2, :3, :4, :5, :6, :7)",
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

        let source_type = import.source_type.as_str();
        let import_status = open_archive::storage::ImportStatus::Completed.as_str();
        conn.execute(
            "INSERT INTO oa_import \
             (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, \
              conversation_count_detected, conversation_count_imported, conversation_count_failed, completed_at) \
             VALUES (:1, :2, :3, :4, :5, :6, :7, 1, 0, SYSTIMESTAMP)",
            &[
                &import.import_id,
                &source_type,
                &import_status,
                &import.payload_object_id,
                &import.source_filename,
                &import.source_content_hash,
                &import.conversation_count_detected,
            ],
        )
        .expect("seed import");

        let artifact_class = artifact.artifact_class.as_str();
        let artifact_source_type = artifact.source_type.as_str();
        let artifact_status = artifact.artifact_status.as_str();
        let enrichment_status = artifact.enrichment_status.as_str();
        conn.execute(
            "INSERT INTO oa_artifact \
             (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status, \
              source_conversation_hash, content_hash_version, content_facets_json, normalization_version) \
             VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)",
            &[
                &artifact.artifact_id,
                &artifact.import_id,
                &artifact_class,
                &artifact_source_type,
                &artifact_status,
                &enrichment_status,
                &artifact.source_conversation_hash,
                &artifact.content_hash_version,
                &artifact.content_facets_json,
                &artifact.normalization_version,
            ],
        )
        .expect("seed artifact");

        conn.commit().expect("commit seed artifact");
    }

    fn insert_invalid_job_type_row(
        &self,
        _job_id: &str,
        _artifact_id: &str,
        _job_type: &str,
        _payload_json: &str,
    ) {
        unreachable!("import harness does not exercise invalid job_type insert")
    }

    fn fetch_import_record(&self, import_id: &str) -> ImportRecord {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        let row = conn
            .query_row_as::<(String, i64, i64, String)>(
                "SELECT import_status, conversation_count_imported, conversation_count_failed, payload_object_id \
                 FROM oa_import WHERE import_id = :1",
                &[&import_id],
            )
            .expect("import row should exist");
        ImportRecord {
            status: row.0,
            count_imported: row.1,
            count_failed: row.2,
            payload_object_id: row.3,
        }
    }

    fn fetch_job_record(&self, _job_id: &str) -> support::JobRecord {
        unreachable!("import harness does not exercise job verification")
    }

    fn count_payload_objects_by_sha256(&self, payload_sha256: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_object_ref WHERE sha256 = :1 AND object_kind = 'import_payload'",
            &[&payload_sha256],
        )
        .expect("payload count query")
        .0
    }

    fn count_artifacts_by_import_id(&self, import_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_artifact WHERE import_id = :1 OR artifact_id = :1",
            &[&import_id],
        )
        .expect("artifact count query")
        .0
    }

    fn count_artifacts_by_source_hash(&self, source_conversation_hash: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_artifact WHERE source_conversation_hash = :1",
            &[&source_conversation_hash],
        )
        .expect("artifact hash count query")
        .0
    }

    fn count_segments_by_artifact_id(&self, artifact_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_segment WHERE artifact_id = :1",
            &[&artifact_id],
        )
        .expect("segment count query")
        .0
    }

    fn count_participants_by_artifact_id(&self, artifact_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_conversation_participant WHERE artifact_id = :1",
            &[&artifact_id],
        )
        .expect("participant count query")
        .0
    }

    fn count_jobs_by_artifact_id(&self, artifact_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1",
            &[&artifact_id],
        )
        .expect("job count query")
        .0
    }
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_single_import_happy_path() {
    let Some(harness) = harness() else { return };
    support::contract_write_single_import_happy_path(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_import_duplicate_payload_is_idempotent() {
    let Some(harness) = harness() else { return };
    support::contract_write_import_duplicate_payload_is_idempotent(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_import_duplicate_artifact_hash_is_idempotent() {
    let Some(harness) = harness() else { return };
    support::contract_write_import_duplicate_artifact_hash_is_idempotent(&harness);
}

#[test]
#[ignore = "requires Oracle integration env; set OA_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn test_write_import_partial_success_finalizes_completed_with_errors() {
    let Some(harness) = harness() else { return };
    support::contract_write_import_partial_success_finalizes_completed_with_errors(&harness);
}
