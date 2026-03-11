mod support;

use open_archive::config::PostgresConfig;
use open_archive::migrations;
use open_archive::storage::{DerivedMetadataWriteStore, PostgresDerivedMetadataStore};
use postgres::NoTls;
use serde_json::Value;
use std::sync::OnceLock;
use support::{DerivationRunRecord, DerivedMetadataHarness, TestImportFixture};

fn postgres_config() -> Option<PostgresConfig> {
    if std::env::var("OA_POSTGRES_INTEGRATION_TESTS").is_err() {
        return None;
    }

    let connection_string = std::env::var("OA_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("OA_POSTGRES_URL"))
        .unwrap_or_else(|_| {
            "postgres://openarchive:openarchive@127.0.0.1:5432/openarchive_derived_test".to_string()
        });

    Some(PostgresConfig { connection_string })
}

fn admin_connection_string(connection_string: &str) -> String {
    let (base, _) = connection_string
        .rsplit_once('/')
        .expect("postgres connection string should include database name");
    format!("{base}/postgres")
}

fn harness() -> Option<PostgresHarness> {
    static CONFIG: OnceLock<Option<PostgresConfig>> = OnceLock::new();
    CONFIG.get_or_init(postgres_config).clone().map(PostgresHarness)
}

struct PostgresHarness(PostgresConfig);

impl DerivedMetadataHarness for PostgresHarness {
    fn reset_schema(&self) {
        assert_eq!(
            std::env::var("OA_ALLOW_SCHEMA_RESET").as_deref(),
            Ok("1"),
            "refusing to reset integration schema without OA_ALLOW_SCHEMA_RESET=1"
        );

        let admin = admin_connection_string(&self.0.connection_string);
        let mut client =
            postgres::Client::connect(&admin, NoTls).expect("connect to postgres admin database");
        let database_name = self
            .0
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

        migrations::postgres::migrate(&self.0).expect("postgres schema migrate should succeed");
    }

    fn derivation_store(&self) -> Box<dyn DerivedMetadataWriteStore> {
        Box::new(PostgresDerivedMetadataStore::new(self.0.clone()))
    }

    fn seed_artifact(&self, fixture: &TestImportFixture) {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("seed connect");
        let payload = &fixture.write_set.payload_object;
        let import = &fixture.write_set.import;
        let artifact_set = &fixture.write_set.artifact_sets[0];
        let artifact = &artifact_set.artifact;

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

        for participant in &artifact_set.participants {
            client
                .execute(
                    "INSERT INTO oa_conversation_participant \
                     (participant_id, artifact_id, participant_role, display_name, provider_name, \
                      model_name, source_participant_key, sequence_no) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        &participant.participant_id,
                        &participant.artifact_id,
                        &participant.participant_role.as_str(),
                        &participant.display_name,
                        &participant.provider_name,
                        &participant.model_name,
                        &participant.source_participant_key,
                        &participant.sequence_no,
                    ],
                )
                .expect("seed participant");
        }

        for segment in &artifact_set.segments {
            client
                .execute(
                    "INSERT INTO oa_segment \
                     (segment_id, artifact_id, participant_id, segment_type, source_segment_key, parent_segment_id, \
                      sequence_no, created_at_source, text_content, text_content_hash, locator_json, \
                      visibility_status, unsupported_content_json) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::timestamptz, $9, $10, $11::text::jsonb, \
                             $12, $13::text::jsonb)",
                    &[
                        &segment.segment_id,
                        &segment.artifact_id,
                        &segment.participant_id,
                        &segment.segment_type.as_str(),
                        &segment.source_segment_key,
                        &segment.parent_segment_id,
                        &segment.sequence_no,
                        &segment.created_at_source.as_ref().map(|ts| ts.as_str()),
                        &segment.text_content,
                        &segment.text_content_hash,
                        &segment.locator_json,
                        &segment.visibility_status.as_str(),
                        &segment.unsupported_content_json,
                    ],
                )
                .expect("seed segment");
        }

        client
            .execute(
                "INSERT INTO oa_enrichment_job \
                 (job_id, artifact_id, job_type, job_status, max_attempts, priority_no, payload_json, attempt_count) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7::text::jsonb, 0)",
                &[
                    &artifact_set.job.job_id,
                    &artifact_set.job.artifact_id,
                    &artifact_set.job.job_type.as_str(),
                    &artifact_set.job.job_status.as_str(),
                    &artifact_set.job.max_attempts,
                    &artifact_set.job.priority_no,
                    &artifact_set.job.payload_json,
                ],
            )
            .expect("seed job");
    }

    fn fetch_derivation_run_record(&self, derivation_run_id: &str) -> DerivationRunRecord {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        let row = client
            .query_one(
                "SELECT artifact_id, run_type, run_status FROM oa_derivation_run WHERE derivation_run_id = $1",
                &[&derivation_run_id],
            )
            .expect("select derivation run");
        DerivationRunRecord {
            artifact_id: row.get(0),
            run_type: row.get(1),
            run_status: row.get(2),
        }
    }

    fn count_derived_objects_for_run(&self, derivation_run_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_derived_object WHERE derivation_run_id = $1",
                &[&derivation_run_id],
            )
            .expect("derived count")
            .get(0)
    }

    fn count_evidence_links_for_objects(&self, derived_object_ids: &[String]) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_evidence_link WHERE derived_object_id = ANY($1)",
                &[&derived_object_ids],
            )
            .expect("evidence count")
            .get(0)
    }

    fn fetch_object_json(&self, derived_object_id: &str) -> Value {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        let payload: String = client
            .query_one(
                "SELECT object_json::text FROM oa_derived_object WHERE derived_object_id = $1",
                &[&derived_object_id],
            )
            .expect("object payload")
            .get(0);
        serde_json::from_str(&payload).expect("json payload")
    }

    fn count_derivation_run_by_id(&self, derivation_run_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_derivation_run WHERE derivation_run_id = $1",
                &[&derivation_run_id],
            )
            .expect("run count")
            .get(0)
    }

    fn count_derived_object_by_id(&self, derived_object_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_derived_object WHERE derived_object_id = $1",
                &[&derived_object_id],
            )
            .expect("object count")
            .get(0)
    }

    fn count_evidence_links_for_object(&self, derived_object_id: &str) -> i64 {
        let mut client = open_archive::postgres_db::connect(&self.0).expect("connect");
        client
            .query_one(
                "SELECT COUNT(*)::bigint FROM oa_evidence_link WHERE derived_object_id = $1",
                &[&derived_object_id],
            )
            .expect("evidence count")
            .get(0)
    }
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn writes_summary_classification_and_memory_with_evidence() {
    let Some(harness) = harness() else { return };
    support::contract_writes_summary_classification_and_memory_with_evidence(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn rejects_cross_artifact_evidence_links_without_writing_rows() {
    let Some(harness) = harness() else { return };
    support::contract_rejects_cross_artifact_evidence_links_without_writing_rows(&harness);
}

#[test]
#[ignore = "requires local Postgres; set OA_POSTGRES_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn rolls_back_partial_writes_when_evidence_insert_fails() {
    let Some(harness) = harness() else { return };
    support::contract_rolls_back_partial_writes_when_evidence_insert_fails(&harness);
}
