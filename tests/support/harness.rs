use open_archive::storage::{
    DerivedMetadataWriteStore, EnrichmentJobLifecycleStore, ImportWriteStore, WriteImportSet,
};
use serde_json::Value;

use super::fixtures::TestImportFixture;

pub struct ImportRecord {
    pub status: String,
    pub count_imported: i64,
    pub count_failed: i64,
    pub payload_object_id: String,
}

pub struct JobRecord {
    pub status: String,
    pub attempt_count: i32,
    pub claimed_by: Option<String>,
    pub error_message: Option<String>,
}

pub struct DerivationRunRecord {
    pub artifact_id: String,
    pub run_type: String,
    pub run_status: String,
}

pub trait ProviderHarness {
    fn reset_schema(&self);
    fn import_store(&self) -> Box<dyn ImportWriteStore>;
    fn job_store(&self) -> Box<dyn EnrichmentJobLifecycleStore>;

    fn seed_existing_artifact(&self, import_set: &WriteImportSet);

    fn fetch_import_record(&self, import_id: &str) -> ImportRecord;
    fn fetch_job_record(&self, job_id: &str) -> JobRecord;

    fn count_payload_objects_by_sha256(&self, payload_sha256: &str) -> i64;
    fn count_artifacts_by_import_id(&self, import_id: &str) -> i64;
    fn count_artifacts_by_source_hash(&self, source_conversation_hash: &str) -> i64;
    fn count_segments_by_artifact_id(&self, artifact_id: &str) -> i64;
    fn count_participants_by_artifact_id(&self, artifact_id: &str) -> i64;
    fn count_jobs_by_artifact_id(&self, artifact_id: &str) -> i64;
}

pub trait DerivedMetadataHarness {
    fn reset_schema(&self);
    fn derivation_store(&self) -> Box<dyn DerivedMetadataWriteStore>;
    fn seed_artifact(&self, fixture: &TestImportFixture);
    fn fetch_derivation_run_record(&self, derivation_run_id: &str) -> DerivationRunRecord;
    fn count_derived_objects_for_run(&self, derivation_run_id: &str) -> i64;
    fn count_derived_objects_for_run_with_status(
        &self,
        derivation_run_id: &str,
        status: &str,
    ) -> i64;
    fn fetch_object_json(&self, derived_object_id: &str) -> Value;
    fn count_derivation_run_by_id(&self, derivation_run_id: &str) -> i64;
    fn count_derived_object_by_id(&self, derived_object_id: &str) -> i64;
    fn count_derived_objects_for_artifact_with_status(
        &self,
        artifact_id: &str,
        status: &str,
    ) -> i64;
}

fn _touch_provider_harness_api(
    harness: &dyn ProviderHarness,
    import_set: &WriteImportSet,
    import_id: &str,
    job_id: &str,
    payload_sha256: &str,
    artifact_id: &str,
) {
    if false {
        harness.reset_schema();
        let _ = harness.import_store();
        let _ = harness.job_store();
        harness.seed_existing_artifact(import_set);
        let import_record = harness.fetch_import_record(import_id);
        let _ = (
            &import_record.status,
            import_record.count_imported,
            import_record.count_failed,
            &import_record.payload_object_id,
        );
        let job_record = harness.fetch_job_record(job_id);
        let _ = (
            &job_record.status,
            job_record.attempt_count,
            &job_record.claimed_by,
            &job_record.error_message,
        );
        let _ = harness.count_payload_objects_by_sha256(payload_sha256);
        let _ = harness.count_artifacts_by_import_id(import_id);
        let _ = harness.count_artifacts_by_source_hash("source-hash");
        let _ = harness.count_segments_by_artifact_id(artifact_id);
        let _ = harness.count_participants_by_artifact_id(artifact_id);
        let _ = harness.count_jobs_by_artifact_id(artifact_id);
    }
}

fn _touch_derived_harness_api(
    harness: &dyn DerivedMetadataHarness,
    fixture: &TestImportFixture,
    derivation_run_id: &str,
    derived_object_id: &str,
) {
    if false {
        harness.reset_schema();
        let _ = harness.derivation_store();
        harness.seed_artifact(fixture);
        let run = harness.fetch_derivation_run_record(derivation_run_id);
        let _ = (&run.artifact_id, &run.run_type, &run.run_status);
        let _ = harness.count_derived_objects_for_run(derivation_run_id);
        let _ = harness.count_derived_objects_for_run_with_status(derivation_run_id, "active");
        let _ = harness.fetch_object_json(derived_object_id);
        let _ = harness.count_derivation_run_by_id(derivation_run_id);
        let _ = harness.count_derived_object_by_id(derived_object_id);
        let _ =
            harness.count_derived_objects_for_artifact_with_status(&fixture.artifact_id, "active");
    }
}

const _: fn(&dyn ProviderHarness, &WriteImportSet, &str, &str, &str, &str) =
    _touch_provider_harness_api;
const _: fn(&dyn DerivedMetadataHarness, &TestImportFixture, &str, &str) =
    _touch_derived_harness_api;
