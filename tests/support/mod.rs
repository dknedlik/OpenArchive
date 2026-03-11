#![allow(dead_code)]

use open_archive::object_store::StoredObject;
use open_archive::storage::{
    ArtifactClass, ArtifactIngestResult, ArtifactStatus, ConversationEnrichmentPayload,
    EnrichmentJobLifecycleStore, EnrichmentStatus, ImportStatus, ImportWriteStore, JobStatus,
    JobType, NewArtifact, NewEnrichmentJob, NewImport, NewImportObjectRef, NewParticipant,
    NewSegment, ParticipantRole, PayloadFormat, RetryOutcome, SegmentType, SourceType,
    VisibilityStatus, WriteArtifactSet, WriteImportSet,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static LIVE_TEST_MUTEX: Mutex<()> = Mutex::new(());

pub fn lock_live_test() -> MutexGuard<'static, ()> {
    match LIVE_TEST_MUTEX.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub fn unique_suffix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let label = &label[..label.len().min(6)];
    let unique = format!("{:x}{:x}", nanos, counter);
    format!("{label}-{}", &unique[..16.min(unique.len())])
}

pub fn sha256_hex(input: &str) -> String {
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    format!("{:x}", h.finalize())
}

pub fn payload_sha256(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

pub struct TestImportFixture {
    pub write_set: WriteImportSet,
    pub artifact_id: String,
    pub job_id: String,
    pub payload_object_id: String,
    pub payload_sha256: String,
    pub segment_ids: Vec<String>,
}

pub fn make_test_import_fixture(suffix: &str) -> TestImportFixture {
    make_test_import_fixture_with_max_attempts(suffix, 3)
}

pub fn make_test_import_fixture_with_max_attempts(
    suffix: &str,
    max_attempts: i32,
) -> TestImportFixture {
    let payload_bytes = format!("test payload content {suffix}").into_bytes();
    let payload_sha = payload_sha256(&payload_bytes);
    let payload_size = payload_bytes.len() as i64;

    let payload_object_id = format!("payload-{suffix}");
    let import_id = format!("import-{suffix}");
    let artifact_id = format!("artifact-{suffix}");
    let participant_id_user = format!("part-user-{suffix}");
    let participant_id_asst = format!("part-asst-{suffix}");
    let job_id = format!("job-{suffix}");
    let segment_ids: Vec<String> = (0..3).map(|i| format!("seg-{suffix}-{i}")).collect();
    let conv_hash = sha256_hex(&format!("conv-hash-{suffix}"));

    let enrichment_payload =
        ConversationEnrichmentPayload::new_v1(&artifact_id, &import_id, SourceType::ChatGptExport);

    let write_set = WriteImportSet {
        payload_object: NewImportObjectRef {
            object_id: payload_object_id.clone(),
            payload_format: PayloadFormat::ChatGptExportJson,
            mime_type: "application/json".to_string(),
            copied_bytes: payload_bytes,
            size_bytes: payload_size,
            sha256: payload_sha.clone(),
            stored_object: StoredObject {
                object_id: payload_object_id.clone(),
                provider: "test".to_string(),
                storage_key: format!("test/{payload_object_id}"),
                mime_type: "application/json".to_string(),
                size_bytes: payload_size,
                sha256: payload_sha.clone(),
            },
        },
        import: NewImport {
            import_id: import_id.clone(),
            source_type: SourceType::ChatGptExport,
            import_status: ImportStatus::Pending,
            payload_object_id: payload_object_id.clone(),
            source_filename: Some(format!("export-{suffix}.json")),
            source_content_hash: conv_hash.clone(),
            conversation_count_detected: 1,
        },
        artifact_sets: vec![WriteArtifactSet {
            artifact: NewArtifact {
                artifact_id: artifact_id.clone(),
                import_id: import_id.clone(),
                artifact_class: ArtifactClass::Conversation,
                source_type: SourceType::ChatGptExport,
                artifact_status: ArtifactStatus::Captured,
                enrichment_status: EnrichmentStatus::Pending,
                source_conversation_key: Some(format!("src-key-{suffix}")),
                source_conversation_hash: conv_hash,
                title: Some(format!("Test Conversation {suffix}")),
                created_at_source: None,
                started_at: None,
                ended_at: None,
                primary_language: Some("en".to_string()),
                content_hash_version: "v1".to_string(),
                content_facets_json: r#"["messages","text"]"#.to_string(),
                normalization_version: "1.0.0".to_string(),
            },
            participants: vec![
                NewParticipant {
                    participant_id: participant_id_user.clone(),
                    artifact_id: artifact_id.clone(),
                    participant_role: ParticipantRole::User,
                    display_name: Some("User".to_string()),
                    provider_name: None,
                    model_name: None,
                    source_participant_key: Some(format!("user-key-{suffix}")),
                    sequence_no: 0,
                },
                NewParticipant {
                    participant_id: participant_id_asst.clone(),
                    artifact_id: artifact_id.clone(),
                    participant_role: ParticipantRole::Assistant,
                    display_name: Some("Assistant".to_string()),
                    provider_name: Some("openai".to_string()),
                    model_name: Some("gpt-4".to_string()),
                    source_participant_key: Some(format!("asst-key-{suffix}")),
                    sequence_no: 1,
                },
            ],
            segments: vec![
                NewSegment {
                    segment_id: segment_ids[0].clone(),
                    artifact_id: artifact_id.clone(),
                    participant_id: Some(participant_id_user.clone()),
                    segment_type: SegmentType::Message,
                    source_segment_key: Some(format!("seg-key-{suffix}-0")),
                    parent_segment_id: None,
                    sequence_no: 0,
                    created_at_source: None,
                    text_content: "Hello, world!".to_string(),
                    text_content_hash: sha256_hex("Hello, world!"),
                    locator_json: None,
                    visibility_status: VisibilityStatus::Visible,
                    unsupported_content_json: None,
                },
                NewSegment {
                    segment_id: segment_ids[1].clone(),
                    artifact_id: artifact_id.clone(),
                    participant_id: Some(participant_id_asst.clone()),
                    segment_type: SegmentType::Message,
                    source_segment_key: Some(format!("seg-key-{suffix}-1")),
                    parent_segment_id: None,
                    sequence_no: 1,
                    created_at_source: None,
                    text_content: "Hi there!".to_string(),
                    text_content_hash: sha256_hex("Hi there!"),
                    locator_json: None,
                    visibility_status: VisibilityStatus::Visible,
                    unsupported_content_json: None,
                },
                NewSegment {
                    segment_id: segment_ids[2].clone(),
                    artifact_id: artifact_id.clone(),
                    participant_id: Some(participant_id_user),
                    segment_type: SegmentType::Message,
                    source_segment_key: Some(format!("seg-key-{suffix}-2")),
                    parent_segment_id: None,
                    sequence_no: 2,
                    created_at_source: None,
                    text_content: "Goodbye!".to_string(),
                    text_content_hash: sha256_hex("Goodbye!"),
                    locator_json: None,
                    visibility_status: VisibilityStatus::Visible,
                    unsupported_content_json: None,
                },
            ],
            job: NewEnrichmentJob {
                job_id: job_id.clone(),
                artifact_id: artifact_id.clone(),
                job_type: JobType::ConversationEnrichment,
                job_status: JobStatus::Pending,
                max_attempts,
                priority_no: 100,
                payload_json: enrichment_payload.to_json(),
            },
        }],
    };

    TestImportFixture {
        write_set,
        artifact_id,
        job_id,
        payload_object_id,
        payload_sha256: payload_sha,
        segment_ids,
    }
}

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

pub fn contract_write_single_import_happy_path<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("happy");
    let fixture = make_test_import_fixture(&suffix);
    let import_id = fixture.write_set.import.import_id.clone();
    let artifact_id = fixture.artifact_id.clone();

    let result = harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    assert_eq!(result.import_id, import_id);
    assert_eq!(result.import_status, ImportStatus::Completed);
    assert_eq!(result.artifacts.len(), 1);
    assert_eq!(result.artifacts[0].ingest_result, ArtifactIngestResult::Created);
    assert!(result.failed_artifact_ids.is_empty());
    assert_eq!(result.segments_written, 3);

    let import_record = harness.fetch_import_record(&import_id);
    assert_eq!(import_record.status, "completed");
    assert_eq!(import_record.count_imported, 1);
    assert_eq!(import_record.count_failed, 0);
    assert_eq!(harness.count_artifacts_by_import_id(&import_id), 1);
    assert_eq!(harness.count_segments_by_artifact_id(&artifact_id), 3);
    assert_eq!(harness.count_participants_by_artifact_id(&artifact_id), 2);
    assert_eq!(harness.count_jobs_by_artifact_id(&artifact_id), 1);
}

pub fn contract_write_import_duplicate_payload_is_idempotent<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("dupload");
    let original = make_test_import_fixture(&suffix);
    harness
        .import_store()
        .write_import(original.write_set)
        .expect("first write should succeed");

    let original_artifact_id = format!("artifact-{suffix}");
    let mut duplicate = make_test_import_fixture(&suffix);
    duplicate.write_set.import.import_id = format!("import-{suffix}-2nd");
    duplicate.write_set.payload_object.object_id = format!("payload-{suffix}-2nd");
    duplicate.write_set.payload_object.stored_object.object_id =
        duplicate.write_set.payload_object.object_id.clone();
    duplicate.write_set.payload_object.stored_object.storage_key =
        format!("test/{}", duplicate.write_set.payload_object.object_id);

    let import_id = duplicate.write_set.import.import_id.clone();
    let payload_sha256 = duplicate.payload_sha256.clone();
    let duplicate_hash = duplicate.write_set.artifact_sets[0]
        .artifact
        .source_conversation_hash
        .clone();

    let result = harness
        .import_store()
        .write_import(duplicate.write_set)
        .expect("duplicate payload should be idempotent");

    assert_eq!(result.import_status, ImportStatus::Completed);
    assert_eq!(result.artifacts.len(), 1);
    assert_eq!(result.artifacts[0].ingest_result, ArtifactIngestResult::AlreadyExists);
    assert!(result.failed_artifact_ids.is_empty());

    let import_record = harness.fetch_import_record(&import_id);
    assert_eq!(import_record.status, "completed");
    assert_eq!(import_record.payload_object_id, format!("payload-{suffix}"));
    assert_eq!(harness.count_payload_objects_by_sha256(&payload_sha256), 1);
    assert_eq!(harness.count_artifacts_by_source_hash(&duplicate_hash), 1);
    assert_eq!(harness.count_segments_by_artifact_id(&original_artifact_id), 3);
    assert_eq!(harness.count_participants_by_artifact_id(&original_artifact_id), 2);
    assert_eq!(harness.count_jobs_by_artifact_id(&original_artifact_id), 1);
}

pub fn contract_write_import_duplicate_artifact_hash_is_idempotent<H: ProviderHarness>(
    harness: &H,
) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("dupart");
    let seeded = make_test_import_fixture(&suffix);
    harness.seed_existing_artifact(&seeded.write_set);

    let suffix2 = unique_suffix("dupartb");
    let mut duplicate = make_test_import_fixture(&suffix2);
    duplicate.write_set.artifact_sets[0].artifact.source_conversation_hash =
        sha256_hex(&format!("conv-hash-{suffix}"));

    let import_id = duplicate.write_set.import.import_id.clone();
    let duplicate_hash = duplicate.write_set.artifact_sets[0]
        .artifact
        .source_conversation_hash
        .clone();

    let result = harness
        .import_store()
        .write_import(duplicate.write_set)
        .expect("duplicate artifact hash should be idempotent");

    assert_eq!(result.import_status, ImportStatus::Completed);
    assert_eq!(result.artifacts.len(), 1);
    assert_eq!(result.artifacts[0].ingest_result, ArtifactIngestResult::AlreadyExists);
    assert!(result.failed_artifact_ids.is_empty());

    let import_record = harness.fetch_import_record(&import_id);
    assert_eq!(import_record.status, "completed");
    assert_eq!(import_record.count_imported, 0);
    assert_eq!(import_record.count_failed, 0);
    assert_eq!(harness.count_artifacts_by_import_id(&import_id), 0);
    assert_eq!(harness.count_artifacts_by_source_hash(&duplicate_hash), 1);
    assert_eq!(harness.count_segments_by_artifact_id(&seeded.artifact_id), 0);
    assert_eq!(harness.count_participants_by_artifact_id(&seeded.artifact_id), 0);
    assert_eq!(harness.count_jobs_by_artifact_id(&seeded.artifact_id), 0);
}

pub fn contract_write_import_partial_success_finalizes_completed_with_errors<H: ProviderHarness>(
    harness: &H,
) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("partial");
    let mut import_fixture = make_test_import_fixture(&suffix);
    let import_id = import_fixture.write_set.import.import_id.clone();
    let successful_artifact_id = import_fixture.artifact_id.clone();

    let second_suffix = unique_suffix("partialb");
    let mut second_artifact = make_test_import_fixture(&second_suffix)
        .write_set
        .artifact_sets
        .remove(0);
    second_artifact.artifact.import_id = import_id.clone();
    second_artifact.segments[1].sequence_no = second_artifact.segments[0].sequence_no;
    let failed_artifact_id = second_artifact.artifact.artifact_id.clone();

    import_fixture.write_set.import.conversation_count_detected = 2;
    import_fixture.write_set.artifact_sets.push(second_artifact);

    let result = harness
        .import_store()
        .write_import(import_fixture.write_set)
        .expect("partial failures should be recorded");

    assert_eq!(result.import_status, ImportStatus::CompletedWithErrors);
    assert_eq!(result.artifacts.len(), 1);
    assert_eq!(result.failed_artifact_ids, vec![failed_artifact_id.clone()]);
    assert_eq!(result.segments_written, 3);
    assert_eq!(result.artifacts[0].ingest_result, ArtifactIngestResult::Created);
    assert_eq!(result.artifacts[0].artifact_id, successful_artifact_id);

    let import_record = harness.fetch_import_record(&import_id);
    assert_eq!(import_record.status, "completed_with_errors");
    assert_eq!(import_record.count_imported, 1);
    assert_eq!(import_record.count_failed, 1);
    assert_eq!(harness.count_artifacts_by_import_id(&import_id), 1);
    assert_eq!(harness.count_artifacts_by_import_id(&failed_artifact_id), 0);
    assert_eq!(harness.count_participants_by_artifact_id(&failed_artifact_id), 0);
    assert_eq!(harness.count_segments_by_artifact_id(&failed_artifact_id), 0);
    assert_eq!(harness.count_jobs_by_artifact_id(&failed_artifact_id), 0);
    assert_eq!(harness.count_participants_by_artifact_id(&successful_artifact_id), 2);
    assert_eq!(harness.count_segments_by_artifact_id(&successful_artifact_id), 3);
    assert_eq!(harness.count_jobs_by_artifact_id(&successful_artifact_id), 1);
}

pub fn contract_claim_complete_happy_path<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("clmcmp");
    let fixture = make_test_import_fixture(&suffix);
    let expected_job_id = fixture.job_id.clone();
    let expected_artifact_id = fixture.artifact_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let lifecycle_store = harness.job_store();
    let claimed = lifecycle_store
        .claim_next_job("worker-happy")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");

    assert_eq!(claimed.job_id, expected_job_id);
    assert_eq!(claimed.artifact_id, expected_artifact_id);
    assert_eq!(claimed.job_type, JobType::ConversationEnrichment);
    assert_eq!(claimed.attempt_count, 1);
    assert_eq!(claimed.max_attempts, 3);
    assert!(!claimed.payload_json.is_empty());

    lifecycle_store
        .complete_job("worker-happy", &claimed.job_id)
        .expect("complete_job should succeed");

    let job = harness.fetch_job_record(&expected_job_id);
    assert_eq!(job.status, "completed");
    assert_eq!(job.attempt_count, 1);
    assert!(job.claimed_by.is_none());
    assert!(job.error_message.is_none());
}

pub fn contract_claim_fail_terminal<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("clfail");
    let fixture = make_test_import_fixture(&suffix);
    let expected_job_id = fixture.job_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let lifecycle_store = harness.job_store();
    let claimed = lifecycle_store
        .claim_next_job("worker-fail")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");

    assert_eq!(claimed.job_id, expected_job_id);

    lifecycle_store
        .fail_job("worker-fail", &claimed.job_id, "something went wrong")
        .expect("fail_job should succeed");

    let job = harness.fetch_job_record(&expected_job_id);
    assert_eq!(job.status, "failed");
    assert_eq!(job.attempt_count, 1);
    assert!(job.claimed_by.is_none());
    assert_eq!(job.error_message.as_deref(), Some("something went wrong"));
}

pub fn contract_claim_retryable_reclaim_complete<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("retry");
    let fixture = make_test_import_fixture(&suffix);
    let expected_job_id = fixture.job_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let lifecycle_store = harness.job_store();
    let claimed_1 = lifecycle_store
        .claim_next_job("worker-retry-1")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");
    assert_eq!(claimed_1.job_id, expected_job_id);
    assert_eq!(claimed_1.attempt_count, 1);

    let outcome = lifecycle_store
        .mark_job_retryable("worker-retry-1", &claimed_1.job_id, "transient error", 0)
        .expect("mark_job_retryable should succeed");
    assert_eq!(outcome, RetryOutcome::Retried);

    let claimed_2 = lifecycle_store
        .claim_next_job("worker-retry-2")
        .expect("claim_next_job should succeed")
        .expect("should re-claim the retryable job");
    assert_eq!(claimed_2.job_id, expected_job_id);
    assert_eq!(claimed_2.attempt_count, 2);

    lifecycle_store
        .complete_job("worker-retry-2", &claimed_2.job_id)
        .expect("complete_job should succeed");

    let job = harness.fetch_job_record(&expected_job_id);
    assert_eq!(job.status, "completed");
    assert_eq!(job.attempt_count, 2);
}

pub fn contract_retryable_exhausted_becomes_terminal<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("exhst");
    let fixture = make_test_import_fixture_with_max_attempts(&suffix, 2);
    let expected_job_id = fixture.job_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let lifecycle_store = harness.job_store();
    let claimed_1 = lifecycle_store
        .claim_next_job("worker-exhaust-1")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");
    assert_eq!(claimed_1.attempt_count, 1);

    let outcome_1 = lifecycle_store
        .mark_job_retryable("worker-exhaust-1", &claimed_1.job_id, "transient error 1", 0)
        .expect("mark_job_retryable should succeed");
    assert_eq!(outcome_1, RetryOutcome::Retried);

    let claimed_2 = lifecycle_store
        .claim_next_job("worker-exhaust-2")
        .expect("claim_next_job should succeed")
        .expect("should re-claim the retryable job");
    assert_eq!(claimed_2.attempt_count, 2);

    let outcome_2 = lifecycle_store
        .mark_job_retryable("worker-exhaust-2", &claimed_2.job_id, "transient error 2", 0)
        .expect("mark_job_retryable should succeed");
    assert_eq!(outcome_2, RetryOutcome::RetriesExhausted);

    let job = harness.fetch_job_record(&expected_job_id);
    assert_eq!(job.status, "failed");
    assert_eq!(job.attempt_count, 2);
    assert!(job.claimed_by.is_none());
    assert_eq!(job.error_message.as_deref(), Some("transient error 2"));
}

pub fn contract_claim_returns_none_when_empty<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let result = harness
        .job_store()
        .claim_next_job("worker-empty")
        .expect("claim_next_job should succeed");

    assert!(result.is_none());
}

pub fn contract_concurrent_claim_protection<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let fixture_a = make_test_import_fixture(&unique_suffix("conca"));
    let fixture_b = make_test_import_fixture(&unique_suffix("concb"));
    let job_id_a = fixture_a.job_id.clone();
    let job_id_b = fixture_b.job_id.clone();

    harness
        .import_store()
        .write_import(fixture_a.write_set)
        .expect("write_import A should succeed");
    harness
        .import_store()
        .write_import(fixture_b.write_set)
        .expect("write_import B should succeed");

    let store_1 = harness.job_store();
    let store_2 = harness.job_store();

    let claimed_1 = store_1
        .claim_next_job("worker-conc-1")
        .expect("claim 1 should succeed")
        .expect("store_1 should claim a job");

    let claimed_2 = store_2
        .claim_next_job("worker-conc-2")
        .expect("claim 2 should succeed")
        .expect("store_2 should claim a job");

    assert_ne!(claimed_1.job_id, claimed_2.job_id);

    let claimed_ids: std::collections::HashSet<&str> =
        [claimed_1.job_id.as_str(), claimed_2.job_id.as_str()]
            .into_iter()
            .collect();
    let expected_ids: std::collections::HashSet<&str> =
        [job_id_a.as_str(), job_id_b.as_str()].into_iter().collect();
    assert_eq!(claimed_ids, expected_ids);

    let claimed_3 = store_1
        .claim_next_job("worker-conc-3")
        .expect("claim 3 should succeed");
    assert!(claimed_3.is_none());
}

pub fn contract_payload_matches_documented_schema<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("pyload");
    let fixture = make_test_import_fixture(&suffix);
    let artifact_id = fixture.artifact_id.clone();
    let import_id = fixture.write_set.import.import_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let claimed = harness
        .job_store()
        .claim_next_job("worker-payload")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");

    let payload = ConversationEnrichmentPayload::from_json(&claimed.payload_json)
        .expect("payload_json should deserialize");
    let expected = ConversationEnrichmentPayload::new_v1(
        &artifact_id,
        &import_id,
        SourceType::ChatGptExport,
    );

    assert_eq!(payload, expected);
    assert_eq!(payload.schema_version, "1");
    assert_eq!(payload.artifact_id, artifact_id);
    assert_eq!(payload.import_id, import_id);
    assert_eq!(payload.source_type, "chatgpt_export");
}

pub fn contract_claim_skips_future_available_at<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("future");
    let fixture = make_test_import_fixture(&suffix);
    let expected_job_id = fixture.job_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let lifecycle_store = harness.job_store();
    let claimed = lifecycle_store
        .claim_next_job("worker-future")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");
    assert_eq!(claimed.job_id, expected_job_id);

    let outcome = lifecycle_store
        .mark_job_retryable("worker-future", &claimed.job_id, "will retry later", 3600)
        .expect("mark_job_retryable should succeed");
    assert_eq!(outcome, RetryOutcome::Retried);

    let result = lifecycle_store
        .claim_next_job("worker-future-2")
        .expect("claim_next_job should succeed");
    assert!(result.is_none());

    let job = harness.fetch_job_record(&expected_job_id);
    assert_eq!(job.status, "retryable");
}

pub fn contract_non_claiming_worker_cannot_complete_job<H: ProviderHarness>(harness: &H) {
    let _guard = lock_live_test();
    harness.reset_schema();

    let suffix = unique_suffix("owner");
    let fixture = make_test_import_fixture(&suffix);
    let expected_job_id = fixture.job_id.clone();

    harness
        .import_store()
        .write_import(fixture.write_set)
        .expect("write_import should succeed");

    let lifecycle_store = harness.job_store();
    let claimed = lifecycle_store
        .claim_next_job("worker-owner")
        .expect("claim_next_job should succeed")
        .expect("should claim the pending job");
    assert_eq!(claimed.job_id, expected_job_id);

    lifecycle_store
        .complete_job("worker-other", &claimed.job_id)
        .expect_err("non-claiming worker must not complete another worker's job");

    let job = harness.fetch_job_record(&expected_job_id);
    assert_eq!(job.status, "running");
    assert_eq!(job.claimed_by.as_deref(), Some("worker-owner"));
}
