mod support;

use open_archive::config::OracleConfig;
use open_archive::migrations;
use open_archive::storage::{
    DerivedMetadataWriteStore, ImportWriteStore, OracleDerivedMetadataStore, OracleImportWriteStore,
};
use serde_json::Value;
use std::sync::OnceLock;
use support::{DerivationRunRecord, DerivedMetadataHarness, TestImportFixture};

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

impl DerivedMetadataHarness for OracleHarness {
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

    fn derivation_store(&self) -> Box<dyn DerivedMetadataWriteStore> {
        Box::new(OracleDerivedMetadataStore::new(self.0.clone()))
    }

    fn seed_artifact(&self, fixture: &TestImportFixture) {
        OracleImportWriteStore::new(self.0.clone())
            .write_import(open_archive::storage::WriteImportSet {
                payload_object: open_archive::storage::NewImportObjectRef {
                    object_id: fixture.write_set.payload_object.object_id.clone(),
                    payload_format: fixture.write_set.payload_object.payload_format,
                    mime_type: fixture.write_set.payload_object.mime_type.clone(),
                    copied_bytes: fixture.write_set.payload_object.copied_bytes.clone(),
                    size_bytes: fixture.write_set.payload_object.size_bytes,
                    sha256: fixture.write_set.payload_object.sha256.clone(),
                    stored_object: fixture.write_set.payload_object.stored_object.clone(),
                },
                import: open_archive::storage::NewImport {
                    import_id: fixture.write_set.import.import_id.clone(),
                    source_type: fixture.write_set.import.source_type,
                    import_status: fixture.write_set.import.import_status,
                    payload_object_id: fixture.write_set.import.payload_object_id.clone(),
                    source_filename: fixture.write_set.import.source_filename.clone(),
                    source_content_hash: fixture.write_set.import.source_content_hash.clone(),
                    conversation_count_detected: fixture.write_set.import.conversation_count_detected,
                },
                artifact_sets: vec![open_archive::storage::WriteArtifactSet {
                    artifact: open_archive::storage::NewArtifact {
                        artifact_id: fixture.write_set.artifact_sets[0].artifact.artifact_id.clone(),
                        import_id: fixture.write_set.artifact_sets[0].artifact.import_id.clone(),
                        artifact_class: fixture.write_set.artifact_sets[0].artifact.artifact_class,
                        source_type: fixture.write_set.artifact_sets[0].artifact.source_type,
                        artifact_status: fixture.write_set.artifact_sets[0].artifact.artifact_status,
                        enrichment_status: fixture.write_set.artifact_sets[0].artifact.enrichment_status,
                        source_conversation_key: fixture.write_set.artifact_sets[0].artifact.source_conversation_key.clone(),
                        source_conversation_hash: fixture.write_set.artifact_sets[0].artifact.source_conversation_hash.clone(),
                        title: fixture.write_set.artifact_sets[0].artifact.title.clone(),
                        created_at_source: fixture.write_set.artifact_sets[0].artifact.created_at_source.clone(),
                        started_at: fixture.write_set.artifact_sets[0].artifact.started_at.clone(),
                        ended_at: fixture.write_set.artifact_sets[0].artifact.ended_at.clone(),
                        primary_language: fixture.write_set.artifact_sets[0].artifact.primary_language.clone(),
                        content_hash_version: fixture.write_set.artifact_sets[0].artifact.content_hash_version.clone(),
                        content_facets_json: fixture.write_set.artifact_sets[0].artifact.content_facets_json.clone(),
                        normalization_version: fixture.write_set.artifact_sets[0].artifact.normalization_version.clone(),
                    },
                    participants: fixture.write_set.artifact_sets[0]
                        .participants
                        .iter()
                        .map(|p| open_archive::storage::NewParticipant {
                            participant_id: p.participant_id.clone(),
                            artifact_id: p.artifact_id.clone(),
                            participant_role: p.participant_role,
                            display_name: p.display_name.clone(),
                            provider_name: p.provider_name.clone(),
                            model_name: p.model_name.clone(),
                            source_participant_key: p.source_participant_key.clone(),
                            sequence_no: p.sequence_no,
                        })
                        .collect(),
                    segments: fixture.write_set.artifact_sets[0]
                        .segments
                        .iter()
                        .map(|s| open_archive::storage::NewSegment {
                            segment_id: s.segment_id.clone(),
                            artifact_id: s.artifact_id.clone(),
                            participant_id: s.participant_id.clone(),
                            segment_type: s.segment_type,
                            source_segment_key: s.source_segment_key.clone(),
                            parent_segment_id: s.parent_segment_id.clone(),
                            sequence_no: s.sequence_no,
                            created_at_source: s.created_at_source.clone(),
                            text_content: s.text_content.clone(),
                            text_content_hash: s.text_content_hash.clone(),
                            locator_json: s.locator_json.clone(),
                            visibility_status: s.visibility_status,
                            unsupported_content_json: s.unsupported_content_json.clone(),
                        })
                        .collect(),
                    job: open_archive::storage::NewEnrichmentJob {
                        job_id: fixture.write_set.artifact_sets[0].job.job_id.clone(),
                        artifact_id: fixture.write_set.artifact_sets[0].job.artifact_id.clone(),
                        job_type: fixture.write_set.artifact_sets[0].job.job_type,
                        enrichment_tier: fixture.write_set.artifact_sets[0].job.enrichment_tier,
                        spawned_by_job_id: fixture.write_set.artifact_sets[0]
                            .job
                            .spawned_by_job_id
                            .clone(),
                        job_status: fixture.write_set.artifact_sets[0].job.job_status,
                        max_attempts: fixture.write_set.artifact_sets[0].job.max_attempts,
                        priority_no: fixture.write_set.artifact_sets[0].job.priority_no,
                        required_capabilities: fixture.write_set.artifact_sets[0]
                            .job
                            .required_capabilities
                            .clone(),
                        payload_json: fixture.write_set.artifact_sets[0].job.payload_json.clone(),
                    },
                }],
            })
            .expect("seed import should succeed");
    }

    fn fetch_derivation_run_record(&self, derivation_run_id: &str) -> DerivationRunRecord {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        let row = conn
            .query_row_as::<(String, String, String)>(
                "SELECT artifact_id, run_type, run_status FROM oa_derivation_run WHERE derivation_run_id = :1",
                &[&derivation_run_id],
            )
            .expect("select derivation run");
        DerivationRunRecord { artifact_id: row.0, run_type: row.1, run_status: row.2 }
    }

    fn count_derived_objects_for_run(&self, derivation_run_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_derived_object WHERE derivation_run_id = :1",
            &[&derivation_run_id],
        ).expect("derived count").0
    }

    fn count_evidence_links_for_objects(&self, derived_object_ids: &[String]) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        let [a, b, c] = derived_object_ids else { panic!("expected 3 object ids") };
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_evidence_link WHERE derived_object_id IN (:1, :2, :3)",
            &[a, b, c],
        ).expect("evidence count").0
    }

    fn fetch_object_json(&self, derived_object_id: &str) -> Value {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        let payload: String = conn.query_row_as::<(String,)>(
            "SELECT object_json FROM oa_derived_object WHERE derived_object_id = :1",
            &[&derived_object_id],
        ).expect("object payload").0;
        serde_json::from_str(&payload).expect("json payload")
    }

    fn count_derivation_run_by_id(&self, derivation_run_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_derivation_run WHERE derivation_run_id = :1",
            &[&derivation_run_id],
        ).expect("run count").0
    }

    fn count_derived_object_by_id(&self, derived_object_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_derived_object WHERE derived_object_id = :1",
            &[&derived_object_id],
        ).expect("object count").0
    }

    fn count_evidence_links_for_object(&self, derived_object_id: &str) -> i64 {
        let conn = open_archive::db::connect(&self.0).expect("connect");
        conn.query_row_as::<(i64,)>(
            "SELECT COUNT(*) FROM oa_evidence_link WHERE derived_object_id = :1",
            &[&derived_object_id],
        ).expect("evidence count").0
    }
}

#[test]
#[ignore = "requires local Oracle; set OA_ORACLE_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn writes_summary_classification_and_memory_with_evidence() {
    let Some(harness) = harness() else { return };
    support::contract_writes_summary_classification_and_memory_with_evidence(&harness);
}

#[test]
#[ignore = "requires local Oracle; set OA_ORACLE_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn rejects_cross_artifact_evidence_links_without_writing_rows() {
    let Some(harness) = harness() else { return };
    support::contract_rejects_cross_artifact_evidence_links_without_writing_rows(&harness);
}

#[test]
#[ignore = "requires local Oracle; set OA_ORACLE_INTEGRATION_TESTS=1 and OA_ALLOW_SCHEMA_RESET=1"]
fn rolls_back_partial_writes_when_evidence_insert_fails() {
    let Some(harness) = harness() else { return };
    support::contract_rolls_back_partial_writes_when_evidence_insert_fails(&harness);
}
