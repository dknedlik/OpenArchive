use open_archive::config::DbConfig;
use open_archive::migrations;
use open_archive::storage::{
    ArtifactClass, ArtifactStatus, EnrichmentStatus, JobStatus, JobType, ParticipantRole,
    PayloadFormat, SegmentType, SourceType, VisibilityStatus,
};
use open_archive::storage::{
    ArtifactIngestResult, ImportStatus, ImportWriteStore, NewArtifact, NewEnrichmentJob, NewImport,
    NewImportPayload, NewParticipant, NewSegment, OracleImportWriteStore,
};
use open_archive::storage::{WriteArtifactSet, WriteImportSet};
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

/// Build a small but complete WriteImportSet.
/// `suffix` is appended to IDs so tests don't collide.
fn make_test_import_set(suffix: &str) -> WriteImportSet {
    use sha2::{Digest, Sha256};

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
            import_status: ImportStatus::Pending,
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
                max_attempts: 3,
                priority_no: 100,
                payload_json: format!(r#"{{"artifact_id":"{}"}}"#, artifact_id),
            },
        }],
    }
}

fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    format!("{:x}", h.finalize())
}

fn seed_existing_artifact(conn: &oracle::Connection, import_set: &WriteImportSet) {
    let payload = &import_set.payload;
    let import = &import_set.import;
    let artifact = &import_set.artifact_sets[0].artifact;

    let payload_format = payload.payload_format.as_str();
    conn.execute(
        "INSERT INTO oa_import_payload \
         (payload_id, payload_format, payload_mime_type, payload_bytes, payload_size_bytes, payload_sha256) \
         VALUES (:1, :2, :3, :4, :5, :6)",
        &[
            &payload.payload_id,
            &payload_format,
            &payload.payload_mime_type,
            &payload.payload_bytes,
            &payload.payload_size_bytes,
            &payload.payload_sha256,
        ],
    )
    .expect("seed payload");

    let source_type = import.source_type.as_str();
    let import_status = ImportStatus::Completed.as_str();
    conn.execute(
        "INSERT INTO oa_import \
         (import_id, source_type, import_status, payload_id, source_filename, source_content_hash, \
          conversation_count_detected, conversation_count_imported, conversation_count_failed, completed_at) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, 1, 0, SYSTIMESTAMP)",
        &[
            &import.import_id,
            &source_type,
            &import_status,
            &import.payload_id,
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

fn count_rows(conn: &oracle::Connection, sql: &str, bind: &dyn oracle::sql_type::ToSql) -> i64 {
    conn.query_row_as::<(i64,)>(sql, &[bind])
        .expect("count query")
        .0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_write_single_import_happy_path() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("happy");
    let import_set = make_test_import_set(&suffix);
    let import_id = import_set.import.import_id.clone();
    {
        let _test_lock = lock_live_test();
        let store = OracleImportWriteStore::new(config.clone());
        let result = store
            .write_import(import_set)
            .expect("write_import should succeed");

        assert_eq!(result.import_id, import_id);
        assert_eq!(result.import_status, ImportStatus::Completed);
        assert_eq!(result.artifacts.len(), 1);
        assert_eq!(
            result.artifacts[0].ingest_result,
            ArtifactIngestResult::Created
        );
        assert!(result.failed_artifact_ids.is_empty());
        assert_eq!(result.segments_written, 3);

        // Verify rows in DB
        let verify_conn = open_archive::db::connect(&config).expect("connect");

        let (status, count_imported, count_failed): (String, i64, i64) = verify_conn
            .query_row_as::<(String, i64, i64)>(
                "SELECT import_status, conversation_count_imported, conversation_count_failed \
                 FROM oa_import WHERE import_id = :1",
                &[&import_id],
            )
            .expect("import row should exist");
        assert_eq!(status, "completed");
        assert_eq!(count_imported, 1);
        assert_eq!(count_failed, 0);

        let artifact_id = &result.artifacts[0].artifact_id;
        let (art_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_artifact WHERE artifact_id = :1",
                &[artifact_id],
            )
            .expect("artifact count query");
        assert_eq!(art_count, 1);

        let (seg_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_segment WHERE artifact_id = :1",
                &[artifact_id],
            )
            .expect("segment count query");
        assert_eq!(seg_count, 3);

        let (part_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_conversation_participant WHERE artifact_id = :1",
                &[artifact_id],
            )
            .expect("participant count query");
        assert_eq!(part_count, 2);

        let (job_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1",
                &[artifact_id],
            )
            .expect("job count query");
        assert_eq!(job_count, 1);
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_write_import_rollback_on_duplicate_payload() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("dupload");
    let import_set_1 = make_test_import_set(&suffix);

    let store = OracleImportWriteStore::new(config.clone());

    {
        let _test_lock = lock_live_test();
        store
            .write_import(import_set_1)
            .expect("first write should succeed");

        let original_artifact_id = format!("artifact-{suffix}");

        // Build a second import with a different import_id but reuse the same payload sha256
        let mut import_set_2 = make_test_import_set(&suffix); // same suffix => same sha256
        import_set_2.import.import_id = format!("import-{suffix}-2nd");
        import_set_2.payload.payload_id = format!("payload-{suffix}-2nd"); // different payload_id but same sha256

        let import_id_2 = import_set_2.import.import_id.clone();
        let payload_sha256 = import_set_2.payload.payload_sha256.clone();
        let duplicate_hash = import_set_2.artifact_sets[0]
            .artifact
            .source_conversation_hash
            .clone();

        let result = store
            .write_import(import_set_2)
            .expect("write_import with duplicate payload should be idempotent");
        assert_eq!(result.import_status, ImportStatus::Completed);
        assert_eq!(result.artifacts.len(), 1);
        assert_eq!(
            result.artifacts[0].ingest_result,
            ArtifactIngestResult::AlreadyExists
        );
        assert!(result.failed_artifact_ids.is_empty());

        // A second import row should exist and point at the previously stored payload.
        let verify_conn = open_archive::db::connect(&config).expect("connect");
        let (payload_id, status): (String, String) = verify_conn
            .query_row_as::<(String, String)>(
                "SELECT payload_id, import_status FROM oa_import WHERE import_id = :1",
                &[&import_id_2],
            )
            .expect("import row should exist");
        assert_eq!(status, "completed");

        let (payload_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_import_payload WHERE payload_sha256 = :1",
                &[&payload_sha256],
            )
            .expect("payload count query");
        assert_eq!(payload_count, 1);
        assert_eq!(payload_id, format!("payload-{suffix}"));

        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_artifact WHERE source_conversation_hash = :1",
                &duplicate_hash,
            ),
            1
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_segment WHERE artifact_id = :1",
                &original_artifact_id,
            ),
            3
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_conversation_participant WHERE artifact_id = :1",
                &original_artifact_id,
            ),
            2
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1",
                &original_artifact_id,
            ),
            1
        );
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_write_import_rollback_on_duplicate_artifact_hash() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let suffix = unique_suffix("dupart");
    let import_set_1 = make_test_import_set(&suffix);

    let store = OracleImportWriteStore::new(config.clone());
    {
        let _test_lock = lock_live_test();
        let seed_conn = open_archive::db::connect(&config).expect("seed connect");
        seed_existing_artifact(&seed_conn, &import_set_1);

        // Second import with unique payload but same artifact source_conversation_hash + source_type + content_hash_version
        let suffix2 = unique_suffix("dupartb");
        let mut import_set_2 = make_test_import_set(&suffix2);
        // Override the artifact hash to match the first import's artifact hash
        import_set_2.artifact_sets[0]
            .artifact
            .source_conversation_hash = {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(format!("conv-hash-{suffix}").as_bytes());
            format!("{:x}", h.finalize())
        };
        // Keep source_type and content_hash_version the same (they are already "chatgpt_export" and "v1")

        let import_id_2 = import_set_2.import.import_id.clone();
        let duplicate_hash = import_set_2.artifact_sets[0]
            .artifact
            .source_conversation_hash
            .clone();

        let result = store
            .write_import(import_set_2)
            .expect("write_import should treat duplicate artifact hash as idempotent");
        assert_eq!(result.import_status, ImportStatus::Completed);
        assert_eq!(result.artifacts.len(), 1);
        assert_eq!(
            result.artifacts[0].ingest_result,
            ArtifactIngestResult::AlreadyExists
        );
        assert!(result.failed_artifact_ids.is_empty());

        // No oa_artifact row should exist under import_id_2, but the import row should be finalized.
        let verify_conn = open_archive::db::connect(&config).expect("connect");
        let (artifact_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_artifact WHERE import_id = :1",
                &[&import_id_2],
            )
            .expect("count query");
        assert_eq!(
            artifact_count, 0,
            "no dangling artifact rows after rollback"
        );

        let (status, count_imported, count_failed): (String, i64, i64) = verify_conn
            .query_row_as::<(String, i64, i64)>(
                "SELECT import_status, conversation_count_imported, conversation_count_failed \
                 FROM oa_import WHERE import_id = :1",
                &[&import_id_2],
            )
            .expect("import row should exist");
        assert_eq!(status, "completed");
        assert_eq!(count_imported, 0);
        assert_eq!(count_failed, 0);
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_artifact WHERE source_conversation_hash = :1",
                &duplicate_hash,
            ),
            1
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_segment WHERE artifact_id = :1",
                &import_set_1.artifact_sets[0].artifact.artifact_id,
            ),
            0,
            "seeded duplicate-hash fixture only creates the artifact row"
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_conversation_participant WHERE artifact_id = :1",
                &import_set_1.artifact_sets[0].artifact.artifact_id,
            ),
            0,
            "seeded duplicate-hash fixture only creates the artifact row"
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1",
                &import_set_1.artifact_sets[0].artifact.artifact_id,
            ),
            0,
            "seeded duplicate-hash fixture only creates the artifact row"
        );
    }
}

#[test]
#[ignore = "requires ADB; set OA_INTEGRATION_TESTS=1 and DB env vars"]
fn test_write_import_partial_success_finalizes_completed_with_errors() {
    let config = match ensure_test_harness() {
        Some(c) => c,
        None => return,
    };

    let store = OracleImportWriteStore::new(config.clone());
    let suffix = unique_suffix("partial");
    let mut import_set = make_test_import_set(&suffix);
    let import_id = import_set.import.import_id.clone();
    let successful_artifact_id = import_set.artifact_sets[0].artifact.artifact_id.clone();

    let second_suffix = unique_suffix("partialb");
    let mut second_artifact = make_test_import_set(&second_suffix).artifact_sets.remove(0);
    second_artifact.artifact.import_id = import_id.clone();
    second_artifact.segments[1].sequence_no = second_artifact.segments[0].sequence_no;
    let failed_artifact_id = second_artifact.artifact.artifact_id.clone();

    import_set.import.conversation_count_detected = 2;
    import_set.artifact_sets.push(second_artifact);

    {
        let _test_lock = lock_live_test();
        let result = store
            .write_import(import_set)
            .expect("write_import should succeed with partial failures recorded");

        assert_eq!(result.import_status, ImportStatus::CompletedWithErrors);
        assert_eq!(result.artifacts.len(), 1);
        assert_eq!(result.failed_artifact_ids, vec![failed_artifact_id.clone()]);
        assert_eq!(result.segments_written, 3);
        assert_eq!(
            result.artifacts[0].ingest_result,
            ArtifactIngestResult::Created
        );
        assert_eq!(result.artifacts[0].artifact_id, successful_artifact_id);

        let verify_conn = open_archive::db::connect(&config).expect("connect");
        let (status, count_imported, count_failed): (String, i64, i64) = verify_conn
            .query_row_as::<(String, i64, i64)>(
                "SELECT import_status, conversation_count_imported, conversation_count_failed \
                 FROM oa_import WHERE import_id = :1",
                &[&import_id],
            )
            .expect("import row should exist");
        assert_eq!(status, "completed_with_errors");
        assert_eq!(count_imported, 1);
        assert_eq!(count_failed, 1);

        let (artifact_count,): (i64,) = verify_conn
            .query_row_as::<(i64,)>(
                "SELECT COUNT(*) FROM oa_artifact WHERE import_id = :1",
                &[&import_id],
            )
            .expect("artifact count query");
        assert_eq!(artifact_count, 1);

        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_artifact WHERE artifact_id = :1",
                &failed_artifact_id,
            ),
            0
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_conversation_participant WHERE artifact_id = :1",
                &failed_artifact_id,
            ),
            0
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_segment WHERE artifact_id = :1",
                &failed_artifact_id,
            ),
            0
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1",
                &failed_artifact_id,
            ),
            0
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_conversation_participant WHERE artifact_id = :1",
                &successful_artifact_id,
            ),
            2
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_segment WHERE artifact_id = :1",
                &successful_artifact_id,
            ),
            3
        );
        assert_eq!(
            count_rows(
                &verify_conn,
                "SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = :1",
                &successful_artifact_id,
            ),
            1
        );
    }
}
