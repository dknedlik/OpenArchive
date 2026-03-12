use open_archive::config::HttpConfig;
use open_archive::enrichment_worker::{
    format_worker_id, start_enrichment_workers, start_enrichment_workers_with_factory,
};
use open_archive::error::StorageResult;
use open_archive::processor::{ConversationProcessor, ConversationProcessorFactory, ProcessorError};
use open_archive::shutdown::ShutdownToken;
use open_archive::storage::types::{
    ClaimedJob, ConversationEnrichmentPayload, EnrichmentTier, LoadedArtifactForEnrichment,
    LoadedConversationForEnrichment, LoadedParticipant, LoadedSegment, RetryOutcome, SourceType,
};
use open_archive::storage::{
    ArtifactListItem, ArtifactReadStore, DerivationWriteResult, DerivedMetadataWriteStore,
    EnrichmentJobLifecycleStore, JobType, WriteDerivationAttempt,
};
use open_archive::{ParticipantRole, VisibilityStatus};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn test_format_worker_id() {
    assert_eq!(format_worker_id(123, 0), "enrichment:123:0");
    assert_eq!(format_worker_id(456, 1), "enrichment:456:1");
}

#[test]
fn test_disabled_workers_start() {
    let config = test_config(0);
    let job_store = Arc::new(EmptyQueueMockStore::new());
    let read_store = Arc::new(FixedReadStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();

    let workers = start_enrichment_workers(&config, job_store, read_store, derived_store, shutdown)
        .expect("disabled workers should start")
        .len();

    assert_eq!(workers, 0);
}

#[test]
fn test_enabled_workers_start_and_shutdown() {
    let config = test_config(2);
    let job_store = Arc::new(EmptyQueueMockStore::new());
    let read_store = Arc::new(FixedReadStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();

    let workers = start_enrichment_workers(
        &config,
        job_store,
        read_store,
        derived_store,
        shutdown.clone(),
    )
    .expect("workers should start");
    assert_eq!(workers.len(), 2);

    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }
}

#[test]
fn test_worker_persists_stub_outputs_and_completes_job() {
    let config = test_config(1);
    let job_store = Arc::new(SingleJobStore::new(valid_claimed_job()));
    let read_store = Arc::new(FixedReadStore::with_sample_conversation());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store.clone();

    let workers = start_enrichment_workers(
        &config,
        job_store_trait,
        read_store_trait,
        derived_store_trait,
        shutdown.clone(),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(75));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert!(job_store.complete_count.load(Ordering::SeqCst) > 0);
    assert_eq!(job_store.fail_count.load(Ordering::SeqCst), 0);
    let attempts = derived_store.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].objects.len() >= 3);
}

#[test]
fn test_worker_fails_job_when_factory_rejects_claimed_tier() {
    let config = test_config(1);
    let mut claimed_job = valid_claimed_job();
    claimed_job.enrichment_tier = EnrichmentTier::Quality;

    let job_store = Arc::new(SingleJobStore::new(claimed_job));
    let read_store = Arc::new(FixedReadStore::with_sample_conversation());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store;

    let workers = start_enrichment_workers_with_factory(
        &config,
        job_store_trait,
        read_store_trait,
        derived_store_trait,
        shutdown.clone(),
        Arc::new(RejectAllFactory),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(75));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert_eq!(job_store.complete_count.load(Ordering::SeqCst), 0);
    assert!(job_store.fail_count.load(Ordering::SeqCst) > 0);
}

fn test_config(enrichment_worker_count: usize) -> HttpConfig {
    HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 1,
        enrichment_worker_count,
        enrichment_poll_interval_ms: 10,
    }
}

fn valid_claimed_job() -> ClaimedJob {
    let payload = ConversationEnrichmentPayload::new_v1(
        "artifact-1",
        "import-1",
        SourceType::ChatGptExport,
    );
    ClaimedJob {
        job_id: "job-1".to_string(),
        artifact_id: "artifact-1".to_string(),
        job_type: JobType::ConversationEnrichment,
        enrichment_tier: EnrichmentTier::Standard,
        spawned_by_job_id: None,
        attempt_count: 1,
        max_attempts: 3,
        required_capabilities: vec!["text".to_string()],
        payload_json: payload.to_json(),
    }
}

struct SingleJobStore {
    claimed_job: Mutex<Option<ClaimedJob>>,
    claim_count: AtomicUsize,
    complete_count: AtomicUsize,
    fail_count: AtomicUsize,
}

impl SingleJobStore {
    fn new(claimed_job: ClaimedJob) -> Self {
        Self {
            claimed_job: Mutex::new(Some(claimed_job)),
            claim_count: AtomicUsize::new(0),
            complete_count: AtomicUsize::new(0),
            fail_count: AtomicUsize::new(0),
        }
    }
}

impl EnrichmentJobLifecycleStore for SingleJobStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.claimed_job.lock().unwrap().take())
    }

    fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
        self.complete_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn fail_job(&self, _worker_id: &str, _job_id: &str, _error_message: &str) -> StorageResult<()> {
        self.fail_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn mark_job_retryable(
        &self,
        _worker_id: &str,
        _job_id: &str,
        _error_message: &str,
        _retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        Ok(RetryOutcome::Retried)
    }
}

struct EmptyQueueMockStore {
    claim_count: AtomicUsize,
}

impl EmptyQueueMockStore {
    fn new() -> Self {
        Self {
            claim_count: AtomicUsize::new(0),
        }
    }
}

impl EnrichmentJobLifecycleStore for EmptyQueueMockStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }

    fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
        Ok(())
    }

    fn fail_job(&self, _worker_id: &str, _job_id: &str, _error_message: &str) -> StorageResult<()> {
        Ok(())
    }

    fn mark_job_retryable(
        &self,
        _worker_id: &str,
        _job_id: &str,
        _error_message: &str,
        _retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        Ok(RetryOutcome::Retried)
    }
}

#[derive(Default)]
struct FixedReadStore {
    loaded: Option<LoadedConversationForEnrichment>,
}

impl FixedReadStore {
    fn with_sample_conversation() -> Self {
        Self {
            loaded: Some(LoadedConversationForEnrichment {
                artifact: LoadedArtifactForEnrichment {
                    artifact_id: "artifact-1".to_string(),
                    import_id: "import-1".to_string(),
                    source_type: SourceType::ChatGptExport,
                    title: Some("Test conversation".to_string()),
                },
                participants: vec![
                    LoadedParticipant {
                        participant_id: "participant-user".to_string(),
                        participant_role: ParticipantRole::User,
                        display_name: Some("User".to_string()),
                        external_id: Some("user-1".to_string()),
                    },
                    LoadedParticipant {
                        participant_id: "participant-assistant".to_string(),
                        participant_role: ParticipantRole::Assistant,
                        display_name: Some("Assistant".to_string()),
                        external_id: Some("assistant-1".to_string()),
                    },
                ],
                segments: vec![
                    LoadedSegment {
                        segment_id: "segment-1".to_string(),
                        participant_id: Some("participant-user".to_string()),
                        participant_role: Some(ParticipantRole::User),
                        sequence_no: 0,
                        text_content: "hello".to_string(),
                        created_at_source: None,
                        visibility_status: VisibilityStatus::Visible,
                    },
                    LoadedSegment {
                        segment_id: "segment-2".to_string(),
                        participant_id: Some("participant-assistant".to_string()),
                        participant_role: Some(ParticipantRole::Assistant),
                        sequence_no: 1,
                        text_content: "hi".to_string(),
                        created_at_source: None,
                        visibility_status: VisibilityStatus::Visible,
                    },
                ],
            }),
        }
    }
}

impl ArtifactReadStore for FixedReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<ArtifactListItem>> {
        Ok(Vec::new())
    }

    fn load_conversation_for_enrichment(
        &self,
        _artifact_id: &str,
    ) -> StorageResult<Option<LoadedConversationForEnrichment>> {
        Ok(self.loaded.clone())
    }
}

#[derive(Default)]
struct MockDerivedStore {
    attempts: Mutex<Vec<WriteDerivationAttempt>>,
}

impl DerivedMetadataWriteStore for MockDerivedStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        let derivation_run_id = attempt.run.derivation_run_id.clone();
        let derived_object_ids = attempt
            .objects
            .iter()
            .map(|object| object.object.derived_object_id.clone())
            .collect::<Vec<_>>();
        let evidence_links_written = attempt
            .objects
            .iter()
            .map(|object| object.evidence_links.len())
            .sum();
        self.attempts.lock().unwrap().push(attempt);
        Ok(DerivationWriteResult {
            derivation_run_id,
            derived_object_ids,
            evidence_links_written,
        })
    }
}

struct RejectAllFactory;

impl ConversationProcessorFactory for RejectAllFactory {
    fn build(
        &self,
        _tier: EnrichmentTier,
    ) -> std::result::Result<Box<dyn ConversationProcessor>, ProcessorError> {
        Err(ProcessorError::Message {
            message: "tier rejected".to_string(),
        })
    }
}
