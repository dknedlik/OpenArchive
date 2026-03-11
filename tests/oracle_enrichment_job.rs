use open_archive::config::DbConfig;
use open_archive::migrations;
use open_archive::storage::{
    ArtifactClass, ArtifactStatus, ConversationEnrichmentPayload, EnrichmentJobLifecycleStore,
    EnrichmentStatus, ImportWriteStore, JobStatus, JobType, NewArtifact, NewEnrichmentJob,
    NewImport, NewImportPayload, NewParticipant, NewSegment, OracleEnrichmentJobStore,
    OracleImportWriteStore, PayloadFormat, RetryOutcome, SegmentType, SourceType, VisibilityStatus,
    WriteArtifactSet, WriteImportSet,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn adb_config() -> Option<DbConfig> {
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

static HARNESS_INIT: OnceLock<Result<DbConfig, String>> = OnceLock::new();
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static LIVE_TEST_MUTEX: Mutex<()> = Mutex::new(());

fn ensure_test_harness() -> Option<DbConfig> {
    let config = adb_config()?;

    let initialized = HARNESS_INIT.get_or_init(|| {
        if std::env::var("OA_ALLOW_SCHEMA_RESET").as_deref() != Ok("1") {
            return Err(
                "refusing to reset integration schema without OA_ALLOW_SCHEMA_RESET=1".to_string(),
            );
        }

        let expected_username = std::env::var("OA_TEST_SCHEMA_USERNAME")
            .or_else(|_| std::env::var("OA_TEST_DB_USERNAME"))
            .unwrap_or_else(|_| "test_schema".to_string());
        if !config.username.eq_ignore_ascii_case(&expected_username) {
            return Err(format!(
                "refusing to reset schema for DB_USERNAME={}; expected test schema user {}",
                config.username, expected_username
            ));
        }

        migrations::reset(&config)
            .and_then(|_| migrations::migrate(&config))
            .map_err(|err| format!("{err:#}"))?;

        Ok(config.clone())
    });

    match initialized {
        Ok(cfg) => Some(cfg.clone()),
        Err(err) => panic!("integration test harness init failed: {err}"),
    }
}

fn unique_suffix(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let label = &label[..label.len().min(6)];
    let unique = format!("{:x}{:x}", nanos, counter);
    format!("{label}-{}", &unique[..16.min(unique.len())])
}

fn lock_live_test() -> MutexGuard<'static, ()> {
    match LIVE_TEST_MUTEX.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn sha256_hex(input: &str) -> String {
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    format!("{:x}", h.finalize())
}

/// Build a minimal WriteImportSet with one artifact and one enrichment job.
/// Uses `ConversationEnrichmentPayload::new_v1()` for the payload_json.
fn make_test_import_set(suffix: &str) -> WriteImportSet {
    make_test_import_set_with_max_attempts(suffix, 3)
}

/// Like `make_test_import_set` but with a configurable `max_attempts` for the job.
fn make_test_import_set_with_max_attempts(suffix: &str, max_attempts: i32) -> WriteImportSet {
    let payload_bytes = format!("test payload content {suffix}").into_bytes();
    let payload_sha256 = {
        let mut h = Sha256::new();
        h.update(&payload_bytes);
        format!("{:x}", h.finalize())
    };
    let payload_size = payload_bytes.len() as i64;

    let payload_id = format!("payload-{suffix}");
    let import_id = format!("import-{suffix}");
    let artifact_id = format!("artifact-{suffix}");
    let participant_id_user = format!("part-user-{suffix}");
    let participant_id_asst = format!("part-asst-{suffix}");
    let job_id = format!("job-{suffix}");

    let seg_ids: Vec<String> = (0..3).map(|i| format!("seg-{suffix}-{i}")).collect();

    let conv_hash = {
        let mut h = Sha256::new();
        h.update(format!("conv-hash-{suffix}").as_bytes());
        format!("{:x}", h.finalize())
    };

    let enrichment_payload =
        ConversationEnrichmentPayload::new_v1(&artifact_id, &import_id, SourceType::ChatGptExport);

    WriteImportSet {
        payload: NewImportPayload {
            payload_id: payload_id.clone(),
            payload_format: PayloadFormat::ChatGptExportJson,
            payload_mime_type: "application/json".to_string(),
            payload_bytes,
            payload_size_bytes: payload_size,
            payload_sha256,
        },
        import: NewImport {
            import_id: import_id.clone(),
            source_type: SourceType::ChatGptExport,
            import_status: open_archive::storage::ImportStatus::Pending,
            payload_id,
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
                    participant_role: open_archive::storage::ParticipantRole::User,
                    display_name: Some("User".to_string()),
                    provider_name: None,
                    model_name: None,
                    source_participant_key: Some(format!("user-key-{suffix}")),
                    sequence_no: 0,
                },
                NewParticipant {
                    participant_id: participant_id_asst.clone(),
                    artifact_id: artifact_id.clone(),
                    participant_role: open_archive::storage::ParticipantRole::Assistant,
                    display_name: Some("Assistant".to_string()),
                    provider_name: Some("openai".to_string()),
                    model_name: Some("gpt-4".to_string()),
                    source_participant_key: Some(format!("asst-key-{suffix}")),
                    sequence_no: 1,
                },
            ],
            segments: vec![
                NewSegment {
                    segment_id: seg_ids[0].clone(),
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
                    segment_id: seg_ids[1].clone(),
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
                    segment_id: seg_ids[2].clone(),
                    artifact_id: artifact_id.clone(),
                    participant_id: Some(participant_id_user.clone()),
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
                job_id,
                artifact_id: artifact_id.clone(),
                job_type: JobType::ConversationEnrichment,
                job_status: JobStatus::Pending,
                max_attempts,
                priority_no: 100,
                payload_json: enrichment_payload.to_json(),
            },
        }],
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_claim_complete_happy_path() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("clmcmp");
    let import_set = make_test_import_set(&suffix);
    let expected_job_id = import_set.artifact_sets[0].job.job_id.clone();
    let expected_artifact_id = import_set.artifact_sets[0].artifact.artifact_id.clone();

    {
        let _test_lock = lock_live_test();

        // Seed: import a job via the write path.
        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        // Claim the job.
        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());
        let claimed = lifecycle_store
            .claim_next_job("worker-happy")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        assert_eq!(claimed.job_id, expected_job_id);
        assert_eq!(claimed.artifact_id, expected_artifact_id);
        assert_eq!(claimed.job_type, JobType::ConversationEnrichment);
        assert_eq!(claimed.attempt_count, 1, "first claim should be attempt 1");
        assert_eq!(claimed.max_attempts, 3);
        assert!(!claimed.payload_json.is_empty());

        // Complete the job.
        lifecycle_store
            .complete_job("worker-happy", &claimed.job_id)
            .expect("complete_job should succeed");

        // Verify DB state after completion.
        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status, attempt_count, claimed_by, error_msg): (
            String,
            i32,
            Option<String>,
            Option<String>,
        ) = verify_conn
            .query_row_as::<(String, i32, Option<String>, Option<String>)>(
                "SELECT job_status, attempt_count, claimed_by, last_error_message \
                 FROM oa_enrichment_job WHERE job_id = :1",
                &[&expected_job_id],
            )
            .expect("job row");

        assert_eq!(status, "completed");
        assert_eq!(attempt_count, 1);
        assert!(claimed_by.is_none(), "claimed_by should be cleared");
        assert!(error_msg.is_none(), "no error on success");
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_claim_fail_terminal() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("clfail");
    let import_set = make_test_import_set(&suffix);
    let expected_job_id = import_set.artifact_sets[0].job.job_id.clone();

    {
        let _test_lock = lock_live_test();

        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());
        let claimed = lifecycle_store
            .claim_next_job("worker-fail")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        assert_eq!(claimed.job_id, expected_job_id);

        // Fail the job terminally.
        lifecycle_store
            .fail_job("worker-fail", &claimed.job_id, "something went wrong")
            .expect("fail_job should succeed");

        // Verify DB state.
        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status, attempt_count, claimed_by, error_msg): (
            String,
            i32,
            Option<String>,
            Option<String>,
        ) = verify_conn
            .query_row_as::<(String, i32, Option<String>, Option<String>)>(
                "SELECT job_status, attempt_count, claimed_by, last_error_message \
                 FROM oa_enrichment_job WHERE job_id = :1",
                &[&expected_job_id],
            )
            .expect("job row");

        assert_eq!(status, "failed");
        assert_eq!(attempt_count, 1);
        assert!(claimed_by.is_none(), "claimed_by should be cleared");
        assert_eq!(
            error_msg.as_deref(),
            Some("something went wrong"),
            "error message should be recorded"
        );
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_claim_retryable_reclaim_complete() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("retry");
    let import_set = make_test_import_set(&suffix);
    let expected_job_id = import_set.artifact_sets[0].job.job_id.clone();

    {
        let _test_lock = lock_live_test();

        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());

        // Claim attempt 1.
        let claimed_1 = lifecycle_store
            .claim_next_job("worker-retry-1")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        assert_eq!(claimed_1.job_id, expected_job_id);
        assert_eq!(claimed_1.attempt_count, 1);

        // Mark retryable with 0-second delay so it's immediately re-claimable.
        let outcome = lifecycle_store
            .mark_job_retryable("worker-retry-1", &claimed_1.job_id, "transient error", 0)
            .expect("mark_job_retryable should succeed");

        assert_eq!(outcome, RetryOutcome::Retried);

        // Re-claim: attempt 2.
        let claimed_2 = lifecycle_store
            .claim_next_job("worker-retry-2")
            .expect("claim_next_job should succeed")
            .expect("should re-claim the retryable job");

        assert_eq!(claimed_2.job_id, expected_job_id);
        assert_eq!(
            claimed_2.attempt_count, 2,
            "second claim should be attempt 2"
        );

        // Complete on the second attempt.
        lifecycle_store
            .complete_job("worker-retry-2", &claimed_2.job_id)
            .expect("complete_job should succeed");

        // Verify DB state.
        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status, attempt_count): (String, i32) = verify_conn
            .query_row_as::<(String, i32)>(
                "SELECT job_status, attempt_count \
                 FROM oa_enrichment_job WHERE job_id = :1",
                &[&expected_job_id],
            )
            .expect("job row");

        assert_eq!(status, "completed");
        assert_eq!(attempt_count, 2);
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_retryable_exhausted_becomes_terminal() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("exhst");
    // max_attempts=2 so after 2 claims, retries are exhausted.
    let import_set = make_test_import_set_with_max_attempts(&suffix, 2);
    let expected_job_id = import_set.artifact_sets[0].job.job_id.clone();

    {
        let _test_lock = lock_live_test();

        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());

        // Claim attempt 1.
        let claimed_1 = lifecycle_store
            .claim_next_job("worker-exhaust-1")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        assert_eq!(claimed_1.attempt_count, 1);

        // Mark retryable (attempt 1 < max_attempts 2 => Retried).
        let outcome_1 = lifecycle_store
            .mark_job_retryable(
                "worker-exhaust-1",
                &claimed_1.job_id,
                "transient error 1",
                0,
            )
            .expect("mark_job_retryable should succeed");

        assert_eq!(outcome_1, RetryOutcome::Retried);

        // Re-claim: attempt 2.
        let claimed_2 = lifecycle_store
            .claim_next_job("worker-exhaust-2")
            .expect("claim_next_job should succeed")
            .expect("should re-claim the retryable job");

        assert_eq!(claimed_2.attempt_count, 2);

        // Mark retryable again (attempt 2 >= max_attempts 2 => RetriesExhausted).
        let outcome_2 = lifecycle_store
            .mark_job_retryable(
                "worker-exhaust-2",
                &claimed_2.job_id,
                "transient error 2",
                0,
            )
            .expect("mark_job_retryable should succeed");

        assert_eq!(outcome_2, RetryOutcome::RetriesExhausted);

        // Verify DB state: job should be terminally failed.
        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status, attempt_count, claimed_by, error_msg): (
            String,
            i32,
            Option<String>,
            Option<String>,
        ) = verify_conn
            .query_row_as::<(String, i32, Option<String>, Option<String>)>(
                "SELECT job_status, attempt_count, claimed_by, last_error_message \
                 FROM oa_enrichment_job WHERE job_id = :1",
                &[&expected_job_id],
            )
            .expect("job row");

        assert_eq!(status, "failed");
        assert_eq!(attempt_count, 2);
        assert!(claimed_by.is_none(), "claimed_by should be cleared");
        assert_eq!(
            error_msg.as_deref(),
            Some("transient error 2"),
            "last error message should be from the final retry attempt"
        );
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_claim_returns_none_when_empty() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    {
        let _test_lock = lock_live_test();

        // No jobs seeded. The schema was reset, so the table should be empty
        // (or all existing jobs are in non-claimable states from prior tests).
        // We use a unique worker_id to avoid confusion.
        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());
        let result = lifecycle_store
            .claim_next_job("worker-empty")
            .expect("claim_next_job should succeed");

        assert!(
            result.is_none(),
            "claim should return None when no jobs are pending"
        );
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_concurrent_claim_protection() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix_a = unique_suffix("conca");
    let suffix_b = unique_suffix("concb");
    let import_set_a = make_test_import_set(&suffix_a);
    let import_set_b = make_test_import_set(&suffix_b);
    let job_id_a = import_set_a.artifact_sets[0].job.job_id.clone();
    let job_id_b = import_set_b.artifact_sets[0].job.job_id.clone();

    {
        let _test_lock = lock_live_test();

        // Seed two separate jobs via the import write path.
        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set_a)
            .expect("write_import A should succeed");
        write_store
            .write_import(import_set_b)
            .expect("write_import B should succeed");

        // Two independent lifecycle stores (each gets its own connection).
        let store_1 = OracleEnrichmentJobStore::new(config.clone());
        let store_2 = OracleEnrichmentJobStore::new(config.clone());

        // Each store claims one job sequentially. The committed status change
        // from the first claim ensures the second gets a different job.
        let claimed_1 = store_1
            .claim_next_job("worker-conc-1")
            .expect("claim 1 should succeed")
            .expect("store_1 should claim a job");

        let claimed_2 = store_2
            .claim_next_job("worker-conc-2")
            .expect("claim 2 should succeed")
            .expect("store_2 should claim a job");

        // Verify each store got a different job.
        assert_ne!(
            claimed_1.job_id, claimed_2.job_id,
            "each store must claim a different job"
        );

        // Verify both claimed jobs are among the expected job IDs.
        let claimed_ids: std::collections::HashSet<&str> =
            [claimed_1.job_id.as_str(), claimed_2.job_id.as_str()]
                .into_iter()
                .collect();
        let expected_ids: std::collections::HashSet<&str> =
            [job_id_a.as_str(), job_id_b.as_str()].into_iter().collect();
        assert_eq!(
            claimed_ids, expected_ids,
            "claimed jobs should be exactly the two seeded jobs"
        );

        // A third claim should return None (both jobs are now running).
        let claimed_3 = store_1
            .claim_next_job("worker-conc-3")
            .expect("claim 3 should succeed");
        assert!(
            claimed_3.is_none(),
            "no more pending jobs after both are claimed"
        );
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_payload_matches_documented_schema() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("pyload");
    let import_set = make_test_import_set(&suffix);
    let artifact_id = import_set.artifact_sets[0].artifact.artifact_id.clone();
    let import_id = import_set.import.import_id.clone();

    {
        let _test_lock = lock_live_test();

        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());
        let claimed = lifecycle_store
            .claim_next_job("worker-payload")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        // Deserialize the payload_json from the claimed job.
        let payload = ConversationEnrichmentPayload::from_json(&claimed.payload_json)
            .expect("payload_json should deserialize to ConversationEnrichmentPayload");

        // Verify it matches the expected documented schema.
        let expected = ConversationEnrichmentPayload::new_v1(
            &artifact_id,
            &import_id,
            SourceType::ChatGptExport,
        );

        assert_eq!(
            payload, expected,
            "round-tripped payload must match original"
        );
        assert_eq!(payload.schema_version, "1");
        assert_eq!(payload.artifact_id, artifact_id);
        assert_eq!(payload.import_id, import_id);
        assert_eq!(payload.source_type, "chatgpt_export");
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_claim_skips_future_available_at() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("future");
    let import_set = make_test_import_set(&suffix);
    let expected_job_id = import_set.artifact_sets[0].job.job_id.clone();

    {
        let _test_lock = lock_live_test();

        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());

        // Claim and then mark retryable with a large delay (3600 seconds = 1 hour).
        let claimed = lifecycle_store
            .claim_next_job("worker-future")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        assert_eq!(claimed.job_id, expected_job_id);

        let outcome = lifecycle_store
            .mark_job_retryable("worker-future", &claimed.job_id, "will retry later", 3600)
            .expect("mark_job_retryable should succeed");

        assert_eq!(outcome, RetryOutcome::Retried);

        // Attempt to claim again. The job's available_at is ~1 hour in the future,
        // so claim should return None.
        let result = lifecycle_store
            .claim_next_job("worker-future-2")
            .expect("claim_next_job should succeed");

        assert!(
            result.is_none(),
            "should not claim a job whose available_at is in the future"
        );

        // Verify the job is still in retryable state in the DB.
        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status,): (String,) = verify_conn
            .query_row_as::<(String,)>(
                "SELECT job_status FROM oa_enrichment_job WHERE job_id = :1",
                &[&expected_job_id],
            )
            .expect("job row");

        assert_eq!(status, "retryable");
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_non_claiming_worker_cannot_complete_job() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("owner");
    let import_set = make_test_import_set(&suffix);
    let expected_job_id = import_set.artifact_sets[0].job.job_id.clone();

    {
        let _test_lock = lock_live_test();

        let write_store = OracleImportWriteStore::new(config.clone());
        write_store
            .write_import(import_set)
            .expect("write_import should succeed");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());
        let claimed = lifecycle_store
            .claim_next_job("worker-owner")
            .expect("claim_next_job should succeed")
            .expect("should claim the pending job");

        assert_eq!(claimed.job_id, expected_job_id);

        let err = lifecycle_store
            .complete_job("worker-other", &claimed.job_id)
            .expect_err("non-claiming worker must not complete another worker's job");
        assert!(
            err.to_string().contains("failed to update enrichment job"),
            "unexpected error: {err}"
        );

        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status, claimed_by): (String, Option<String>) = verify_conn
            .query_row_as::<(String, Option<String>)>(
                "SELECT job_status, claimed_by FROM oa_enrichment_job WHERE job_id = :1",
                &[&expected_job_id],
            )
            .expect("job row");

        assert_eq!(status, "running");
        assert_eq!(claimed_by.as_deref(), Some("worker-owner"));
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_claim_fails_on_unknown_job_type() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let job_id = format!("job-invalid-{}", unique_suffix("inv"));
    let artifact_id = format!("artifact-invalid-{}", unique_suffix("inv"));
    let invalid_job_type = "completely_unknown_type";
    let payload_json = r#"{"test": "payload"}"#;

    {
        let _test_lock = lock_live_test();

        // Directly insert a job row with an invalid job_type to simulate data corruption
        let conn = open_archive::db::connect(&config).expect("connect");
        conn.execute(
            &format!(
                "INSERT INTO oa_enrichment_job \
                 (job_id, artifact_id, job_type, job_status, max_attempts, priority_no, payload_json, attempt_count) \
                 VALUES (:1, :2, :3, 'pending', 3, 100, :4, 0)"
            ),
            &[&job_id, &artifact_id, &invalid_job_type, &payload_json],
        )
        .expect("insert invalid job_type row");

        let lifecycle_store = OracleEnrichmentJobStore::new(config.clone());

        // Attempt to claim the job should fail with InvalidJobType error
        let err = lifecycle_store
            .claim_next_job("worker-invalid")
            .expect_err("claiming job with invalid job_type must fail");

        assert!(
            err.to_string().contains("invalid job_type"),
            "error should indicate invalid job_type: {err}"
        );
        assert!(
            err.to_string().contains(&invalid_job_type),
            "error should include the invalid job_type value: {err}"
        );
        assert!(
            err.to_string().contains(&job_id),
            "error should include the job_id: {err}"
        );

        // Verify the row is unchanged: still pending, no claimed_by, attempt_count unchanged
        let verify_conn = open_archive::db::connect(&config).expect("verify connect");
        let (status, attempt_count, claimed_by): (String, i32, Option<String>) = verify_conn
            .query_row_as::<(String, i32, Option<String>)>(
                "SELECT job_status, attempt_count, claimed_by \
                 FROM oa_enrichment_job WHERE job_id = :1",
                &[&job_id],
            )
            .expect("job row should exist");

        assert_eq!(
            status, "pending",
            "job must remain in pending state after failed claim"
        );
        assert_eq!(
            attempt_count, 0,
            "attempt_count must not be incremented after failed claim"
        );
        assert!(
            claimed_by.is_none(),
            "claimed_by must remain NULL after failed claim"
        );
    }
}
