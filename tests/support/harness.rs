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
    fn count_evidence_links_for_objects(&self, derived_object_ids: &[String]) -> i64;
    fn fetch_object_json(&self, derived_object_id: &str) -> Value;
    fn count_derivation_run_by_id(&self, derivation_run_id: &str) -> i64;
    fn count_derived_object_by_id(&self, derived_object_id: &str) -> i64;
    fn count_evidence_links_for_object(&self, derived_object_id: &str) -> i64;
    fn count_derived_objects_for_artifact_with_status(
        &self,
        artifact_id: &str,
        status: &str,
    ) -> i64;
}
