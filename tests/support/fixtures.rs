use open_archive::config::PostgresConfig;
use open_archive::object_store::StoredObject;
use open_archive::storage::{
    ArtifactClass, ArtifactExtractPayload, ArtifactStatus, ClassificationObjectJson,
    DerivationRunStatus, DerivationRunType, DerivedMetadataWriteStore, DerivedObjectPayload,
    EnrichmentStatus, EvidenceRole, ImportStatus, InputScopeType, JobStatus, JobType,
    MemoryObjectJson, NewArtifact, NewDerivationRun, NewDerivedObject, NewEnrichmentJob,
    NewEvidenceLink, NewImport, NewImportObjectRef, NewParticipant, NewSegment, ObjectStatus,
    OriginKind, ParticipantRole, PayloadFormat, ScopeType, SegmentType, SourceType,
    SummaryObjectJson, SupportStrength, VisibilityStatus, WriteArtifactSet, WriteDerivationAttempt,
    WriteDerivedObject, WriteImportSet,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static LIVE_TEST_MUTEX: Mutex<()> = Mutex::new(());

pub struct TestImportFixture {
    pub write_set: WriteImportSet,
    pub artifact_id: String,
    pub job_id: String,
    pub payload_object_id: String,
    pub payload_sha256: String,
    pub segment_ids: Vec<String>,
}

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

    let enrichment_payload = ArtifactExtractPayload::new_v1(
        &artifact_id,
        &import_id,
        SourceType::ChatGptExport,
        Vec::new(),
        Vec::new(),
    );

    let write_set = WriteImportSet {
        payload_object: NewImportObjectRef {
            object_id: payload_object_id.clone(),
            payload_format: PayloadFormat::ChatGptExportJson,
            mime_type: "application/json".to_string(),
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
                    segment_type: SegmentType::ContentBlock,
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
                    segment_type: SegmentType::ContentBlock,
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
                    segment_type: SegmentType::ContentBlock,
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
                job_type: JobType::ArtifactExtract,
                enrichment_tier: open_archive::storage::EnrichmentTier::Standard,
                spawned_by_job_id: None,
                job_status: JobStatus::Pending,
                max_attempts,
                priority_no: 100,
                required_capabilities: vec!["text".to_string()],
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

pub fn seed_postgres_stub_derivations(
    config: &PostgresConfig,
    artifact_id: &str,
    job_id: &str,
    segment_ids: &[String],
) {
    let store = open_archive::storage::PostgresDerivedMetadataStore::new(config.clone());
    let derivation_run_id = format!("run-{artifact_id}");
    let summary_object_id = format!("derived-summary-{artifact_id}");
    let memory_object_id = format!("derived-memory-{artifact_id}");
    let started_at = open_archive::SourceTimestamp::parse_rfc3339("2026-01-01T00:00:00Z")
        .expect("valid started_at");
    let completed_at = open_archive::SourceTimestamp::parse_rfc3339("2026-01-01T00:01:00Z")
        .expect("valid completed_at");

    store
        .write_derivation_attempt(WriteDerivationAttempt {
            run: NewDerivationRun {
                derivation_run_id: derivation_run_id.clone(),
                artifact_id: artifact_id.to_string(),
                job_id: Some(job_id.to_string()),
                run_type: DerivationRunType::ArtifactExtraction,
                pipeline_name: "test_pipeline".to_string(),
                pipeline_version: "v1".to_string(),
                provider_name: Some("test".to_string()),
                model_name: Some("stub".to_string()),
                prompt_version: Some("v1".to_string()),
                run_status: DerivationRunStatus::Completed,
                input_scope_type: InputScopeType::Artifact,
                input_scope_json: format!(r#"{{"artifact_id":"{artifact_id}"}}"#),
                started_at,
                completed_at: Some(completed_at),
                error_message: None,
            },
            objects: vec![
                WriteDerivedObject {
                    object: NewDerivedObject {
                        derived_object_id: summary_object_id.clone(),
                        artifact_id: artifact_id.to_string(),
                        derivation_run_id: derivation_run_id.clone(),
                        origin_kind: OriginKind::Explicit,
                        object_status: ObjectStatus::Active,
                        confidence_score: Some(0.9),
                        confidence_label: Some("high".to_string()),
                        scope_type: ScopeType::Artifact,
                        scope_id: artifact_id.to_string(),
                        supersedes_derived_object_id: None,
                        payload: DerivedObjectPayload::Summary {
                            title: Some("Summary".to_string()),
                            body_text: "Summary for retrieval testing".to_string(),
                            object_json: Some(SummaryObjectJson {
                                summary_kind: Some("artifact_context_pack".to_string()),
                                summary_version: Some("v1".to_string()),
                            }),
                        },
                    },
                    evidence_links: vec![NewEvidenceLink {
                        evidence_link_id: format!("evidence-summary-{artifact_id}"),
                        derived_object_id: summary_object_id,
                        segment_id: segment_ids[0].clone(),
                        evidence_role: EvidenceRole::PrimarySupport,
                        evidence_rank: 0,
                        support_strength: SupportStrength::Strong,
                    }],
                },
                WriteDerivedObject {
                    object: NewDerivedObject {
                        derived_object_id: memory_object_id.clone(),
                        artifact_id: artifact_id.to_string(),
                        derivation_run_id,
                        origin_kind: OriginKind::Explicit,
                        object_status: ObjectStatus::Active,
                        confidence_score: Some(0.8),
                        confidence_label: Some("medium".to_string()),
                        scope_type: ScopeType::Artifact,
                        scope_id: artifact_id.to_string(),
                        supersedes_derived_object_id: None,
                        payload: DerivedObjectPayload::Memory {
                            title: Some("Memory".to_string()),
                            body_text: "Memory for retrieval testing".to_string(),
                            object_json: MemoryObjectJson {
                                candidate_key: "seed-memory".to_string(),
                                memory_type: "fact".to_string(),
                                memory_scope: ScopeType::Artifact,
                                memory_scope_value: artifact_id.to_string(),
                            },
                        },
                    },
                    evidence_links: vec![NewEvidenceLink {
                        evidence_link_id: format!("evidence-memory-{artifact_id}"),
                        derived_object_id: memory_object_id,
                        segment_id: segment_ids[1].clone(),
                        evidence_role: EvidenceRole::PrimarySupport,
                        evidence_rank: 0,
                        support_strength: SupportStrength::Strong,
                    }],
                },
            ],
        })
        .expect("seed derivation attempt");
}

pub(super) struct FixtureDerivationAttemptSpec<'a> {
    pub run_id: &'a str,
    pub summary_id: &'a str,
    pub classification_id: &'a str,
    pub memory_id: &'a str,
    pub pipeline_version: &'a str,
    pub provider_name: &'a str,
    pub model_name: &'a str,
    pub prompt_version: &'a str,
}

pub(super) fn fixture_derivation_attempt(
    fixture: &TestImportFixture,
    spec: FixtureDerivationAttemptSpec<'_>,
) -> WriteDerivationAttempt {
    let FixtureDerivationAttemptSpec {
        run_id,
        summary_id,
        classification_id,
        memory_id,
        pipeline_version,
        provider_name,
        model_name,
        prompt_version,
    } = spec;
    WriteDerivationAttempt {
        run: NewDerivationRun {
            derivation_run_id: run_id.to_string(),
            artifact_id: fixture.artifact_id.clone(),
            job_id: Some(fixture.job_id.clone()),
            run_type: DerivationRunType::ArtifactReconciliation,
            pipeline_name: "fixture_pipeline".to_string(),
            pipeline_version: pipeline_version.to_string(),
            provider_name: Some(provider_name.to_string()),
            model_name: Some(model_name.to_string()),
            prompt_version: Some(prompt_version.to_string()),
            run_status: DerivationRunStatus::Completed,
            input_scope_type: InputScopeType::Artifact,
            input_scope_json: format!(r#"{{"artifact_id":"{}"}}"#, fixture.artifact_id),
            started_at: open_archive::SourceTimestamp::from(chrono::Utc::now()),
            completed_at: None,
            error_message: None,
        },
        objects: vec![
            WriteDerivedObject {
                object: NewDerivedObject {
                    derived_object_id: summary_id.to_string(),
                    artifact_id: fixture.artifact_id.clone(),
                    derivation_run_id: run_id.to_string(),
                    origin_kind: OriginKind::Inferred,
                    object_status: ObjectStatus::Active,
                    confidence_score: Some(0.91),
                    confidence_label: Some("high".to_string()),
                    scope_type: ScopeType::Artifact,
                    scope_id: fixture.artifact_id.clone(),
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
                        derived_object_id: summary_id.to_string(),
                        segment_id: fixture.segment_ids[0].clone(),
                        evidence_role: EvidenceRole::PrimarySupport,
                        evidence_rank: 1,
                        support_strength: SupportStrength::Strong,
                    },
                    NewEvidenceLink {
                        evidence_link_id: format!("evidence-{}-2", summary_id),
                        derived_object_id: summary_id.to_string(),
                        segment_id: fixture.segment_ids[2].clone(),
                        evidence_role: EvidenceRole::SecondarySupport,
                        evidence_rank: 2,
                        support_strength: SupportStrength::Medium,
                    },
                ],
            },
            WriteDerivedObject {
                object: NewDerivedObject {
                    derived_object_id: classification_id.to_string(),
                    artifact_id: fixture.artifact_id.clone(),
                    derivation_run_id: run_id.to_string(),
                    origin_kind: OriginKind::Deterministic,
                    object_status: ObjectStatus::Active,
                    confidence_score: Some(0.88),
                    confidence_label: Some("medium".to_string()),
                    scope_type: ScopeType::Artifact,
                    scope_id: fixture.artifact_id.clone(),
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
                    derived_object_id: classification_id.to_string(),
                    segment_id: fixture.segment_ids[1].clone(),
                    evidence_role: EvidenceRole::PrimarySupport,
                    evidence_rank: 1,
                    support_strength: SupportStrength::Strong,
                }],
            },
            WriteDerivedObject {
                object: NewDerivedObject {
                    derived_object_id: memory_id.to_string(),
                    artifact_id: fixture.artifact_id.clone(),
                    derivation_run_id: run_id.to_string(),
                    origin_kind: OriginKind::Inferred,
                    object_status: ObjectStatus::Active,
                    confidence_score: Some(0.72),
                    confidence_label: Some("medium".to_string()),
                    scope_type: ScopeType::Segment,
                    scope_id: fixture.segment_ids[2].clone(),
                    supersedes_derived_object_id: None,
                    payload: DerivedObjectPayload::Memory {
                        title: Some("closing_pattern".to_string()),
                        body_text: "The user closes the conversation politely.".to_string(),
                        object_json: MemoryObjectJson {
                            candidate_key: "closing-pattern".to_string(),
                            memory_type: "preference".to_string(),
                            memory_scope: ScopeType::Segment,
                            memory_scope_value: fixture.segment_ids[2].clone(),
                        },
                    },
                },
                evidence_links: vec![NewEvidenceLink {
                    evidence_link_id: format!("evidence-{}-1", memory_id),
                    derived_object_id: memory_id.to_string(),
                    segment_id: fixture.segment_ids[2].clone(),
                    evidence_role: EvidenceRole::PrimarySupport,
                    evidence_rank: 1,
                    support_strength: SupportStrength::Strong,
                }],
            },
        ],
    }
}

fn _touch_fixture_fields(fixture: &TestImportFixture) {
    if false {
        let _ = (
            &fixture.write_set,
            &fixture.artifact_id,
            &fixture.job_id,
            &fixture.payload_object_id,
            &fixture.payload_sha256,
            &fixture.segment_ids,
        );
    }
}

fn _touch_postgres_fixture_support(fixture: &TestImportFixture, config: &PostgresConfig) {
    if false {
        seed_postgres_stub_derivations(
            config,
            &fixture.artifact_id,
            &fixture.job_id,
            &fixture.segment_ids,
        );
    }
}

fn _touch_derivation_fixture_support(fixture: &TestImportFixture) {
    if false {
        let _ = fixture_derivation_attempt(
            fixture,
            FixtureDerivationAttemptSpec {
                run_id: "run",
                summary_id: "summary",
                classification_id: "classification",
                memory_id: "memory",
                pipeline_version: "v1",
                provider_name: "provider",
                model_name: "model",
                prompt_version: "prompt",
            },
        );
    }
}

const _: fn(&TestImportFixture) = _touch_fixture_fields;
const _: fn(&TestImportFixture, &PostgresConfig) = _touch_postgres_fixture_support;
const _: fn(&TestImportFixture) = _touch_derivation_fixture_support;
