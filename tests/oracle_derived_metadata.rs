use open_archive::config::DbConfig;
use open_archive::migrations;
use open_archive::object_store::StoredObject;
use open_archive::storage::{
    ArtifactClass, ArtifactStatus, ClassificationObjectJson, ConversationEnrichmentPayload,
    DerivationRunStatus, DerivationRunType, DerivedMetadataWriteStore, DerivedObjectPayload,
    EnrichmentStatus, EvidenceRole, ImportStatus, ImportWriteStore, InputScopeType, JobStatus,
    JobType, MemoryObjectJson, NewArtifact, NewDerivationRun, NewDerivedObject, NewEnrichmentJob,
    NewEvidenceLink, NewImport, NewImportObjectRef, NewParticipant, NewSegment, ObjectStatus,
    OracleDerivedMetadataStore, OracleImportWriteStore, OriginKind, ParticipantRole, PayloadFormat,
    ScopeType, SegmentType, SourceType, SummaryObjectJson, SupportStrength, VisibilityStatus,
    WriteArtifactSet, WriteDerivationAttempt, WriteDerivedObject, WriteImportSet,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

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

        migrations::oracle::reset(&config)
            .and_then(|_| migrations::oracle::migrate(&config))
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

struct SeededArtifact {
    import_set: WriteImportSet,
    artifact_id: String,
    job_id: String,
    segment_ids: Vec<String>,
}

fn make_test_import_set(suffix: &str) -> SeededArtifact {
    let payload_bytes = format!("test payload content {suffix}").into_bytes();
    let payload_sha256 = {
        let mut h = Sha256::new();
        h.update(&payload_bytes);
        format!("{:x}", h.finalize())
    };

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

    let import_set = WriteImportSet {
        payload_object: NewImportObjectRef {
            object_id: payload_object_id.clone(),
            payload_format: PayloadFormat::ChatGptExportJson,
            mime_type: "application/json".to_string(),
            copied_bytes: payload_bytes.clone(),
            size_bytes: payload_bytes.len() as i64,
            sha256: payload_sha256,
            stored_object: StoredObject {
                object_id: payload_object_id.clone(),
                provider: "test".to_string(),
                storage_key: format!("test/{payload_object_id}"),
                mime_type: "application/json".to_string(),
                size_bytes: payload_bytes.len() as i64,
                sha256: sha256_hex(&format!("payload-{suffix}")),
            },
        },
        import: NewImport {
            import_id: import_id.clone(),
            source_type: SourceType::ChatGptExport,
            import_status: ImportStatus::Pending,
            payload_object_id,
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
                    participant_id: Some(participant_id_user),
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
                    participant_id: Some(participant_id_asst),
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
                    participant_id: Some(format!("part-user-{suffix}")),
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
                max_attempts: 3,
                priority_no: 100,
                payload_json: enrichment_payload.to_json(),
            },
        }],
    };

    SeededArtifact {
        import_set,
        artifact_id,
        job_id,
        segment_ids,
    }
}

fn seed_artifact(config: &DbConfig, seeded: &SeededArtifact) {
    let store = OracleImportWriteStore::new(config.clone());
    store
        .write_import(make_import_clone(seeded))
        .expect("seed import should succeed");
}

fn make_import_clone(seeded: &SeededArtifact) -> WriteImportSet {
    WriteImportSet {
        payload_object: NewImportObjectRef {
            object_id: seeded.import_set.payload_object.object_id.clone(),
            payload_format: seeded.import_set.payload_object.payload_format,
            mime_type: seeded.import_set.payload_object.mime_type.clone(),
            copied_bytes: seeded.import_set.payload_object.copied_bytes.clone(),
            size_bytes: seeded.import_set.payload_object.size_bytes,
            sha256: seeded.import_set.payload_object.sha256.clone(),
            stored_object: seeded.import_set.payload_object.stored_object.clone(),
        },
        import: NewImport {
            import_id: seeded.import_set.import.import_id.clone(),
            source_type: seeded.import_set.import.source_type,
            import_status: seeded.import_set.import.import_status,
            payload_object_id: seeded.import_set.import.payload_object_id.clone(),
            source_filename: seeded.import_set.import.source_filename.clone(),
            source_content_hash: seeded.import_set.import.source_content_hash.clone(),
            conversation_count_detected: seeded.import_set.import.conversation_count_detected,
        },
        artifact_sets: seeded
            .import_set
            .artifact_sets
            .iter()
            .map(|artifact_set| WriteArtifactSet {
                artifact: NewArtifact {
                    artifact_id: artifact_set.artifact.artifact_id.clone(),
                    import_id: artifact_set.artifact.import_id.clone(),
                    artifact_class: artifact_set.artifact.artifact_class,
                    source_type: artifact_set.artifact.source_type,
                    artifact_status: artifact_set.artifact.artifact_status,
                    enrichment_status: artifact_set.artifact.enrichment_status,
                    source_conversation_key: artifact_set.artifact.source_conversation_key.clone(),
                    source_conversation_hash: artifact_set
                        .artifact
                        .source_conversation_hash
                        .clone(),
                    title: artifact_set.artifact.title.clone(),
                    created_at_source: artifact_set.artifact.created_at_source.clone(),
                    started_at: artifact_set.artifact.started_at.clone(),
                    ended_at: artifact_set.artifact.ended_at.clone(),
                    primary_language: artifact_set.artifact.primary_language.clone(),
                    content_hash_version: artifact_set.artifact.content_hash_version.clone(),
                    content_facets_json: artifact_set.artifact.content_facets_json.clone(),
                    normalization_version: artifact_set.artifact.normalization_version.clone(),
                },
                participants: artifact_set
                    .participants
                    .iter()
                    .map(|participant| NewParticipant {
                        participant_id: participant.participant_id.clone(),
                        artifact_id: participant.artifact_id.clone(),
                        participant_role: participant.participant_role,
                        display_name: participant.display_name.clone(),
                        provider_name: participant.provider_name.clone(),
                        model_name: participant.model_name.clone(),
                        source_participant_key: participant.source_participant_key.clone(),
                        sequence_no: participant.sequence_no,
                    })
                    .collect(),
                segments: artifact_set
                    .segments
                    .iter()
                    .map(|segment| NewSegment {
                        segment_id: segment.segment_id.clone(),
                        artifact_id: segment.artifact_id.clone(),
                        participant_id: segment.participant_id.clone(),
                        segment_type: segment.segment_type,
                        source_segment_key: segment.source_segment_key.clone(),
                        parent_segment_id: segment.parent_segment_id.clone(),
                        sequence_no: segment.sequence_no,
                        created_at_source: segment.created_at_source.clone(),
                        text_content: segment.text_content.clone(),
                        text_content_hash: segment.text_content_hash.clone(),
                        locator_json: segment.locator_json.clone(),
                        visibility_status: segment.visibility_status,
                        unsupported_content_json: segment.unsupported_content_json.clone(),
                    })
                    .collect(),
                job: NewEnrichmentJob {
                    job_id: artifact_set.job.job_id.clone(),
                    artifact_id: artifact_set.job.artifact_id.clone(),
                    job_type: artifact_set.job.job_type,
                    job_status: artifact_set.job.job_status,
                    max_attempts: artifact_set.job.max_attempts,
                    priority_no: artifact_set.job.priority_no,
                    payload_json: artifact_set.job.payload_json.clone(),
                },
            })
            .collect(),
    }
}

fn count_rows(conn: &oracle::Connection, sql: &str, bind: &str) -> i64 {
    conn.query_row_as::<(i64,)>(sql, &[&bind])
        .expect("count query")
        .0
}

#[test]
fn writes_summary_classification_and_memory_with_evidence() {
    let config = match ensure_test_harness() {
        Some(config) => config,
        None => return,
    };
    let _guard = lock_live_test();

    let seeded = make_test_import_set(&unique_suffix("drvok"));
    seed_artifact(&config, &seeded);

    let store = OracleDerivedMetadataStore::new(config.clone());
    let run_id = format!("drv-run-{}", seeded.artifact_id);
    let summary_id = format!("summary-{}", seeded.artifact_id);
    let classification_id = format!("class-{}", seeded.artifact_id);
    let memory_id = format!("memory-{}", seeded.artifact_id);

    let result = store
        .write_derivation_attempt(WriteDerivationAttempt {
            run: NewDerivationRun {
                derivation_run_id: run_id.clone(),
                artifact_id: seeded.artifact_id.clone(),
                job_id: Some(seeded.job_id.clone()),
                run_type: DerivationRunType::SummaryExtraction,
                pipeline_name: "fixture_pipeline".to_string(),
                pipeline_version: "1.0.0".to_string(),
                provider_name: Some("fixture".to_string()),
                model_name: Some("stub-v1".to_string()),
                prompt_version: Some("p1".to_string()),
                run_status: DerivationRunStatus::Completed,
                input_scope_type: InputScopeType::Artifact,
                input_scope_json: format!(r#"{{"artifact_id":"{}"}}"#, seeded.artifact_id),
                completed_at: None,
                error_message: None,
            },
            objects: vec![
                WriteDerivedObject {
                    object: NewDerivedObject {
                        derived_object_id: summary_id.clone(),
                        artifact_id: seeded.artifact_id.clone(),
                        derivation_run_id: run_id.clone(),
                        origin_kind: OriginKind::Inferred,
                        object_status: ObjectStatus::Active,
                        confidence_score: Some(0.91),
                        confidence_label: Some("high".to_string()),
                        scope_type: ScopeType::Artifact,
                        scope_id: seeded.artifact_id.clone(),
                        supersedes_derived_object_id: None,
                        payload: DerivedObjectPayload::Summary {
                            title: Some("conversation_summary".to_string()),
                            body_text: "This conversation covered setup and parting.".to_string(),
                            object_json: Some(SummaryObjectJson {
                                summary_kind: Some("conversation_resume".to_string()),
                                summary_version: Some("1".to_string()),
                            }),
                        },
                    },
                    evidence_links: vec![
                        NewEvidenceLink {
                            evidence_link_id: format!("evidence-{}-1", summary_id),
                            derived_object_id: summary_id.clone(),
                            segment_id: seeded.segment_ids[0].clone(),
                            evidence_role: EvidenceRole::PrimarySupport,
                            evidence_rank: 1,
                            support_strength: SupportStrength::Strong,
                        },
                        NewEvidenceLink {
                            evidence_link_id: format!("evidence-{}-2", summary_id),
                            derived_object_id: summary_id.clone(),
                            segment_id: seeded.segment_ids[2].clone(),
                            evidence_role: EvidenceRole::SecondarySupport,
                            evidence_rank: 2,
                            support_strength: SupportStrength::Medium,
                        },
                    ],
                },
                WriteDerivedObject {
                    object: NewDerivedObject {
                        derived_object_id: classification_id.clone(),
                        artifact_id: seeded.artifact_id.clone(),
                        derivation_run_id: run_id.clone(),
                        origin_kind: OriginKind::Deterministic,
                        object_status: ObjectStatus::Active,
                        confidence_score: Some(0.88),
                        confidence_label: Some("medium".to_string()),
                        scope_type: ScopeType::Artifact,
                        scope_id: seeded.artifact_id.clone(),
                        supersedes_derived_object_id: None,
                        payload: DerivedObjectPayload::Classification {
                            title: Some("conversation_type".to_string()),
                            body_text: Some("Greeting and sign-off exchange".to_string()),
                            object_json: ClassificationObjectJson {
                                classification_type: "conversation_type".to_string(),
                                classification_value: "greeting".to_string(),
                            },
                        },
                    },
                    evidence_links: vec![NewEvidenceLink {
                        evidence_link_id: format!("evidence-{}-1", classification_id),
                        derived_object_id: classification_id.clone(),
                        segment_id: seeded.segment_ids[1].clone(),
                        evidence_role: EvidenceRole::PrimarySupport,
                        evidence_rank: 1,
                        support_strength: SupportStrength::Strong,
                    }],
                },
                WriteDerivedObject {
                    object: NewDerivedObject {
                        derived_object_id: memory_id.clone(),
                        artifact_id: seeded.artifact_id.clone(),
                        derivation_run_id: run_id.clone(),
                        origin_kind: OriginKind::Inferred,
                        object_status: ObjectStatus::Active,
                        confidence_score: Some(0.72),
                        confidence_label: Some("medium".to_string()),
                        scope_type: ScopeType::Segment,
                        scope_id: seeded.segment_ids[2].clone(),
                        supersedes_derived_object_id: None,
                        payload: DerivedObjectPayload::Memory {
                            title: Some("closing_pattern".to_string()),
                            body_text: "The user closes the conversation politely.".to_string(),
                            object_json: MemoryObjectJson {
                                memory_type: "preference".to_string(),
                                memory_scope: ScopeType::Segment,
                                memory_scope_value: seeded.segment_ids[2].clone(),
                            },
                        },
                    },
                    evidence_links: vec![NewEvidenceLink {
                        evidence_link_id: format!("evidence-{}-1", memory_id),
                        derived_object_id: memory_id.clone(),
                        segment_id: seeded.segment_ids[2].clone(),
                        evidence_role: EvidenceRole::PrimarySupport,
                        evidence_rank: 1,
                        support_strength: SupportStrength::Strong,
                    }],
                },
            ],
        })
        .expect("derivation write should succeed");

    assert_eq!(result.derivation_run_id, run_id);
    assert_eq!(result.derived_object_ids.len(), 3);
    assert_eq!(result.evidence_links_written, 4);

    let verify_conn = open_archive::db::connect(&config).expect("verify connect");
    let run_row = verify_conn
        .query_row_as::<(String, String, String)>(
            "SELECT artifact_id, run_type, run_status FROM oa_derivation_run WHERE derivation_run_id = :1",
            &[&run_id],
        )
        .expect("select derivation run");
    assert_eq!(run_row.0, seeded.artifact_id);
    assert_eq!(run_row.1, "summary_extraction");
    assert_eq!(run_row.2, "completed");

    let counts = verify_conn
        .query_row_as::<(i64, i64)>(
            "SELECT \
                (SELECT COUNT(*) FROM oa_derived_object WHERE derivation_run_id = :1), \
                (SELECT COUNT(*) FROM oa_evidence_link WHERE derived_object_id IN (:2, :3, :4)) \
             FROM dual",
            &[&run_id, &summary_id, &classification_id, &memory_id],
        )
        .expect("counts");
    assert_eq!(counts.0, 3);
    assert_eq!(counts.1, 4);

    let classification_json: String = verify_conn
        .query_row_as::<(String,)>(
            "SELECT object_json FROM oa_derived_object WHERE derived_object_id = :1",
            &[&classification_id],
        )
        .expect("classification payload")
        .0;
    let classification_payload: Value =
        serde_json::from_str(&classification_json).expect("classification json");
    assert_eq!(
        classification_payload["classification_type"],
        Value::String("conversation_type".to_string())
    );
    assert_eq!(
        classification_payload["classification_value"],
        Value::String("greeting".to_string())
    );
}

#[test]
fn rejects_cross_artifact_evidence_links_without_writing_rows() {
    let config = match ensure_test_harness() {
        Some(config) => config,
        None => return,
    };
    let _guard = lock_live_test();

    let seeded_a = make_test_import_set(&unique_suffix("drva"));
    let seeded_b = make_test_import_set(&unique_suffix("drvb"));
    seed_artifact(&config, &seeded_a);
    seed_artifact(&config, &seeded_b);

    let store = OracleDerivedMetadataStore::new(config.clone());
    let run_id = format!("drv-run-cross-{}", seeded_a.artifact_id);
    let summary_id = format!("summary-cross-{}", seeded_a.artifact_id);

    let error = store
        .write_derivation_attempt(WriteDerivationAttempt {
            run: NewDerivationRun {
                derivation_run_id: run_id.clone(),
                artifact_id: seeded_a.artifact_id.clone(),
                job_id: Some(seeded_a.job_id.clone()),
                run_type: DerivationRunType::SummaryExtraction,
                pipeline_name: "fixture_pipeline".to_string(),
                pipeline_version: "1.0.0".to_string(),
                provider_name: Some("fixture".to_string()),
                model_name: Some("stub-v1".to_string()),
                prompt_version: Some("p1".to_string()),
                run_status: DerivationRunStatus::Completed,
                input_scope_type: InputScopeType::Artifact,
                input_scope_json: format!(r#"{{"artifact_id":"{}"}}"#, seeded_a.artifact_id),
                completed_at: None,
                error_message: None,
            },
            objects: vec![WriteDerivedObject {
                object: NewDerivedObject {
                    derived_object_id: summary_id.clone(),
                    artifact_id: seeded_a.artifact_id.clone(),
                    derivation_run_id: run_id.clone(),
                    origin_kind: OriginKind::Inferred,
                    object_status: ObjectStatus::Active,
                    confidence_score: Some(0.9),
                    confidence_label: Some("high".to_string()),
                    scope_type: ScopeType::Artifact,
                    scope_id: seeded_a.artifact_id.clone(),
                    supersedes_derived_object_id: None,
                    payload: DerivedObjectPayload::Summary {
                        title: Some("conversation_summary".to_string()),
                        body_text: "This should be rejected.".to_string(),
                        object_json: None,
                    },
                },
                evidence_links: vec![NewEvidenceLink {
                    evidence_link_id: format!("evidence-{}-1", summary_id),
                    derived_object_id: summary_id.clone(),
                    segment_id: seeded_b.segment_ids[0].clone(),
                    evidence_role: EvidenceRole::PrimarySupport,
                    evidence_rank: 1,
                    support_strength: SupportStrength::Strong,
                }],
            }],
        })
        .expect_err("cross-artifact evidence should be rejected");

    let error_text = format!("{error}");
    assert!(error_text.contains("outside artifact"));

    let verify_conn = open_archive::db::connect(&config).expect("verify connect");
    assert_eq!(
        count_rows(
            &verify_conn,
            "SELECT COUNT(*) FROM oa_derivation_run WHERE derivation_run_id = :1",
            &run_id,
        ),
        0
    );
    assert_eq!(
        count_rows(
            &verify_conn,
            "SELECT COUNT(*) FROM oa_derived_object WHERE derived_object_id = :1",
            &summary_id,
        ),
        0
    );
}

#[test]
fn rolls_back_partial_writes_when_evidence_insert_fails() {
    let config = match ensure_test_harness() {
        Some(config) => config,
        None => return,
    };
    let _guard = lock_live_test();

    let seeded = make_test_import_set(&unique_suffix("drvrb"));
    seed_artifact(&config, &seeded);

    let store = OracleDerivedMetadataStore::new(config.clone());
    let run_id = format!("drv-run-rb-{}", seeded.artifact_id);
    let summary_id = format!("summary-rb-{}", seeded.artifact_id);

    let error = store
        .write_derivation_attempt(WriteDerivationAttempt {
            run: NewDerivationRun {
                derivation_run_id: run_id.clone(),
                artifact_id: seeded.artifact_id.clone(),
                job_id: Some(seeded.job_id.clone()),
                run_type: DerivationRunType::SummaryExtraction,
                pipeline_name: "fixture_pipeline".to_string(),
                pipeline_version: "1.0.0".to_string(),
                provider_name: Some("fixture".to_string()),
                model_name: Some("stub-v1".to_string()),
                prompt_version: Some("p1".to_string()),
                run_status: DerivationRunStatus::Completed,
                input_scope_type: InputScopeType::Artifact,
                input_scope_json: format!(r#"{{"artifact_id":"{}"}}"#, seeded.artifact_id),
                completed_at: None,
                error_message: None,
            },
            objects: vec![WriteDerivedObject {
                object: NewDerivedObject {
                    derived_object_id: summary_id.clone(),
                    artifact_id: seeded.artifact_id.clone(),
                    derivation_run_id: run_id.clone(),
                    origin_kind: OriginKind::Inferred,
                    object_status: ObjectStatus::Active,
                    confidence_score: Some(0.93),
                    confidence_label: Some("high".to_string()),
                    scope_type: ScopeType::Artifact,
                    scope_id: seeded.artifact_id.clone(),
                    supersedes_derived_object_id: None,
                    payload: DerivedObjectPayload::Summary {
                        title: Some("conversation_summary".to_string()),
                        body_text: "This insert should rollback.".to_string(),
                        object_json: None,
                    },
                },
                evidence_links: vec![
                    NewEvidenceLink {
                        evidence_link_id: format!("evidence-{}-1", summary_id),
                        derived_object_id: summary_id.clone(),
                        segment_id: seeded.segment_ids[0].clone(),
                        evidence_role: EvidenceRole::PrimarySupport,
                        evidence_rank: 1,
                        support_strength: SupportStrength::Strong,
                    },
                    NewEvidenceLink {
                        evidence_link_id: format!("evidence-{}-2", summary_id),
                        derived_object_id: summary_id.clone(),
                        segment_id: seeded.segment_ids[1].clone(),
                        evidence_role: EvidenceRole::SecondarySupport,
                        evidence_rank: 1,
                        support_strength: SupportStrength::Medium,
                    },
                ],
            }],
        })
        .expect_err("duplicate evidence rank should fail");

    let error_text = format!("{error}");
    assert!(error_text.contains("failed to insert evidence link"));

    let verify_conn = open_archive::db::connect(&config).expect("verify connect");
    assert_eq!(
        count_rows(
            &verify_conn,
            "SELECT COUNT(*) FROM oa_derivation_run WHERE derivation_run_id = :1",
            &run_id,
        ),
        0
    );
    assert_eq!(
        count_rows(
            &verify_conn,
            "SELECT COUNT(*) FROM oa_derived_object WHERE derived_object_id = :1",
            &summary_id,
        ),
        0
    );
    assert_eq!(
        count_rows(
            &verify_conn,
            "SELECT COUNT(*) FROM oa_evidence_link WHERE derived_object_id = :1",
            &summary_id,
        ),
        0
    );
}
