use open_archive::app::retrieval::ArchiveRetrievalServiceApi;
use open_archive::config::HttpConfig;
use open_archive::enrichment_worker::{
    format_worker_id, start_enrichment_workers, start_enrichment_workers_with_factory,
};
use open_archive::error::StorageResult;
use open_archive::processor::{ArtifactProcessor, ArtifactProcessorFactory, ProcessorError};
use open_archive::shutdown::ShutdownToken;
use open_archive::storage::types::{
    ArtifactExtractPayload, ArtifactExtractionResult, ClaimedJob, EnrichmentTier,
    LoadedArtifactForEnrichment, LoadedArtifactRecord, LoadedParticipant, LoadedSegment,
    NewEnrichmentBatch, NewEnrichmentJob, PersistedEnrichmentBatch, ReconciliationDecision,
    RetrievalResultSet, RetryOutcome, SourceType,
};
use open_archive::storage::{
    ArchiveRetrievalStore, ArtifactListItem, ArtifactReadStore, DerivationWriteResult,
    DerivedMetadataWriteStore, EnrichmentJobLifecycleStore, EnrichmentStateStore, JobType,
    RetrievalIntent, RetrievedContextItem, WriteDerivationAttempt,
};
use open_archive::{ParticipantRole, VisibilityStatus};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
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
    let retrieval_store: Arc<dyn ArchiveRetrievalServiceApi> = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();

    let workers = start_enrichment_workers(
        &config,
        job_store,
        read_store,
        retrieval_store,
        state_store,
        derived_store,
        shutdown,
    )
    .expect("disabled workers should start")
    .len();

    assert_eq!(workers, 0);
}

#[test]
fn test_enabled_workers_start_and_shutdown() {
    let config = test_config(2);
    let job_store = Arc::new(EmptyQueueMockStore::new());
    let read_store = Arc::new(FixedReadStore::default());
    let retrieval_store: Arc<dyn ArchiveRetrievalServiceApi> = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();

    let workers = start_enrichment_workers(
        &config,
        job_store,
        read_store,
        retrieval_store,
        state_store,
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
    let read_store = Arc::new(FixedReadStore::with_sample_artifact());
    let retrieval_store = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let retrieval_store_trait: Arc<dyn ArchiveRetrievalServiceApi> = retrieval_store;
    let state_store_trait: Arc<dyn EnrichmentStateStore> = state_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store.clone();

    let workers = start_enrichment_workers(
        &config,
        job_store_trait,
        read_store_trait,
        retrieval_store_trait,
        state_store_trait,
        derived_store_trait,
        shutdown.clone(),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(250));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert!(job_store.complete_count.load(Ordering::SeqCst) > 0);
    assert_eq!(job_store.fail_count.load(Ordering::SeqCst), 0);
    let attempts = derived_store.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].objects.len() >= 3);
    assert_eq!(
        attempts[0].run.run_status,
        open_archive::storage::DerivationRunStatus::Completed
    );
    assert!(attempts[0].run.completed_at.is_some());
}

#[test]
fn test_worker_fails_job_when_factory_rejects_claimed_tier() {
    let config = test_config(1);
    let mut claimed_job = valid_claimed_job();
    claimed_job.enrichment_tier = EnrichmentTier::Quality;

    let job_store = Arc::new(SingleJobStore::new(claimed_job));
    let read_store = Arc::new(FixedReadStore::with_sample_artifact());
    let retrieval_store = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let retrieval_store_trait: Arc<dyn ArchiveRetrievalServiceApi> = retrieval_store;
    let state_store_trait: Arc<dyn EnrichmentStateStore> = state_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store;

    let workers = start_enrichment_workers_with_factory(
        &config,
        job_store_trait,
        read_store_trait,
        retrieval_store_trait,
        state_store_trait,
        derived_store_trait,
        shutdown.clone(),
        Arc::new(RejectAllFactory),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(150));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert_eq!(job_store.complete_count.load(Ordering::SeqCst), 0);
    assert!(job_store.fail_count.load(Ordering::SeqCst) > 0);
}

#[test]
fn test_worker_marks_job_retryable_for_transient_inference_failures() {
    let config = test_config(1);
    let job_store = Arc::new(SingleJobStore::new(valid_claimed_job()));
    let read_store = Arc::new(FixedReadStore::with_sample_artifact());
    let retrieval_store = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let retrieval_store_trait: Arc<dyn ArchiveRetrievalServiceApi> = retrieval_store;
    let state_store_trait: Arc<dyn EnrichmentStateStore> = state_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store;

    let workers = start_enrichment_workers_with_factory(
        &config,
        job_store_trait,
        read_store_trait,
        retrieval_store_trait,
        state_store_trait,
        derived_store_trait,
        shutdown.clone(),
        Arc::new(RetryableProcessorFactory),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(150));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert_eq!(job_store.complete_count.load(Ordering::SeqCst), 0);
    assert_eq!(job_store.fail_count.load(Ordering::SeqCst), 0);
    assert!(job_store.retryable_count.load(Ordering::SeqCst) > 0);
    assert!(job_store
        .last_retryable_message
        .lock()
        .unwrap()
        .as_deref()
        .unwrap_or_default()
        .contains("Processor execution failed"));
}

#[test]
fn test_worker_fails_job_when_artifact_cannot_be_loaded() {
    let config = test_config(1);
    let job_store = Arc::new(SingleJobStore::new(valid_claimed_job()));
    let read_store = Arc::new(FixedReadStore::default());
    let retrieval_store = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let retrieval_store_trait: Arc<dyn ArchiveRetrievalServiceApi> = retrieval_store;
    let state_store_trait: Arc<dyn EnrichmentStateStore> = state_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store.clone();

    let workers = start_enrichment_workers(
        &config,
        job_store_trait,
        read_store_trait,
        retrieval_store_trait,
        state_store_trait,
        derived_store_trait,
        shutdown.clone(),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(150));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert_eq!(job_store.complete_count.load(Ordering::SeqCst), 0);
    assert!(job_store.fail_count.load(Ordering::SeqCst) > 0);
    assert!(job_store
        .last_fail_message
        .lock()
        .unwrap()
        .as_deref()
        .unwrap_or_default()
        .contains("not found for extraction"));
    assert!(derived_store.attempts.lock().unwrap().is_empty());
}

#[test]
fn test_worker_fails_job_when_payload_source_type_is_invalid() {
    let config = test_config(1);
    let mut claimed_job = valid_claimed_job();
    claimed_job.payload_json = serde_json::json!({
        "schema_version": "v1",
        "artifact_id": "artifact-1",
        "import_id": "import-1",
        "source_type": "not_a_real_source"
    })
    .to_string();

    let job_store = Arc::new(SingleJobStore::new(claimed_job));
    let read_store = Arc::new(FixedReadStore::with_sample_artifact());
    let retrieval_store = Arc::new(MockRetrievalStore);
    let state_store = Arc::new(MockStateStore::default());
    let derived_store = Arc::new(MockDerivedStore::default());
    let shutdown = ShutdownToken::new();
    let job_store_trait: Arc<dyn EnrichmentJobLifecycleStore> = job_store.clone();
    let read_store_trait: Arc<dyn ArtifactReadStore> = read_store;
    let retrieval_store_trait: Arc<dyn ArchiveRetrievalServiceApi> = retrieval_store;
    let state_store_trait: Arc<dyn EnrichmentStateStore> = state_store;
    let derived_store_trait: Arc<dyn DerivedMetadataWriteStore> = derived_store.clone();

    let workers = start_enrichment_workers(
        &config,
        job_store_trait,
        read_store_trait,
        retrieval_store_trait,
        state_store_trait,
        derived_store_trait,
        shutdown.clone(),
    )
    .expect("worker should start");

    std::thread::sleep(Duration::from_millis(150));
    shutdown.signal();
    for worker in workers {
        worker.join().expect("worker should join cleanly");
    }

    assert_eq!(job_store.complete_count.load(Ordering::SeqCst), 0);
    assert!(job_store.fail_count.load(Ordering::SeqCst) > 0);
    assert!(job_store
        .last_fail_message
        .lock()
        .unwrap()
        .as_deref()
        .unwrap_or_default()
        .contains("Invalid artifact source_type in extract payload"));
    assert!(derived_store.attempts.lock().unwrap().is_empty());
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
    let payload = ArtifactExtractPayload::new_v1(
        "artifact-1",
        "import-1",
        SourceType::ChatGptExport,
        Vec::new(),
        Vec::new(),
    );
    ClaimedJob {
        job_id: "job-1".to_string(),
        artifact_id: "artifact-1".to_string(),
        job_type: JobType::ArtifactExtract,
        enrichment_tier: EnrichmentTier::Standard,
        spawned_by_job_id: None,
        attempt_count: 1,
        max_attempts: 3,
        required_capabilities: vec!["text".to_string()],
        payload_json: payload.to_json(),
    }
}

struct SingleJobStore {
    pending_jobs: Mutex<Vec<ClaimedJob>>,
    claim_count: AtomicUsize,
    complete_count: AtomicUsize,
    fail_count: AtomicUsize,
    retryable_count: AtomicUsize,
    last_fail_message: Mutex<Option<String>>,
    last_retryable_message: Mutex<Option<String>>,
}

impl SingleJobStore {
    fn new(claimed_job: ClaimedJob) -> Self {
        Self {
            pending_jobs: Mutex::new(vec![ClaimedJob {
                attempt_count: 0,
                ..claimed_job
            }]),
            claim_count: AtomicUsize::new(0),
            complete_count: AtomicUsize::new(0),
            fail_count: AtomicUsize::new(0),
            retryable_count: AtomicUsize::new(0),
            last_fail_message: Mutex::new(None),
            last_retryable_message: Mutex::new(None),
        }
    }
}

impl EnrichmentJobLifecycleStore for SingleJobStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);
        let mut pending = self.pending_jobs.lock().unwrap();
        if pending.is_empty() {
            return Ok(None);
        }
        let mut job = pending.remove(0);
        job.attempt_count += 1;
        Ok(Some(job))
    }

    fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
        self.complete_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn fail_job(&self, _worker_id: &str, _job_id: &str, _error_message: &str) -> StorageResult<()> {
        self.fail_count.fetch_add(1, Ordering::SeqCst);
        *self.last_fail_message.lock().unwrap() = Some(_error_message.to_string());
        Ok(())
    }

    fn mark_job_retryable(
        &self,
        _worker_id: &str,
        _job_id: &str,
        _error_message: &str,
        _retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        self.retryable_count.fetch_add(1, Ordering::SeqCst);
        *self.last_retryable_message.lock().unwrap() = Some(_error_message.to_string());
        Ok(RetryOutcome::Retried)
    }

    fn reschedule_running_job(
        &self,
        _worker_id: &str,
        _job_id: &str,
        _error_message: &str,
        _retry_after_seconds: i64,
    ) -> StorageResult<()> {
        Ok(())
    }

    fn enqueue_jobs(&self, jobs: &[NewEnrichmentJob]) -> StorageResult<()> {
        let mut pending = self.pending_jobs.lock().unwrap();
        pending.extend(jobs.iter().map(|job| ClaimedJob {
            job_id: job.job_id.clone(),
            artifact_id: job.artifact_id.clone(),
            job_type: job.job_type,
            enrichment_tier: job.enrichment_tier,
            spawned_by_job_id: job.spawned_by_job_id.clone(),
            attempt_count: 0,
            max_attempts: job.max_attempts,
            required_capabilities: job.required_capabilities.clone(),
            payload_json: job.payload_json.clone(),
        }));
        Ok(())
    }

    fn claim_matching_jobs(
        &self,
        _worker_id: &str,
        _template_job: &ClaimedJob,
        _limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        Ok(Vec::new())
    }

    fn claim_jobs_by_type(
        &self,
        _worker_id: &str,
        _job_type: JobType,
        _enrichment_tier: Option<EnrichmentTier>,
        _limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        Ok(Vec::new())
    }

    fn record_batch_submission(
        &self,
        _batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn transition_batch_submission(
        &self,
        _completed_provider_batch_id: &str,
        _next_batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn complete_batch(&self, _provider_batch_id: &str) -> StorageResult<()> {
        Ok(())
    }

    fn fail_batch_record(
        &self,
        _provider_batch_id: &str,
        _error_message: &str,
    ) -> StorageResult<()> {
        Ok(())
    }

    fn load_running_batches(
        &self,
        _stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        Ok(Vec::new())
    }

    fn reconcile_stale_running_batches(&self, _stage_name: &str) -> StorageResult<usize> {
        Ok(0)
    }

    fn reconcile_stale_running_jobs(&self, _worker_id: &str) -> StorageResult<usize> {
        Ok(0)
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

    fn reschedule_running_job(
        &self,
        _worker_id: &str,
        _job_id: &str,
        _error_message: &str,
        _retry_after_seconds: i64,
    ) -> StorageResult<()> {
        Ok(())
    }

    fn enqueue_jobs(&self, _jobs: &[NewEnrichmentJob]) -> StorageResult<()> {
        Ok(())
    }

    fn claim_matching_jobs(
        &self,
        _worker_id: &str,
        _template_job: &ClaimedJob,
        _limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        Ok(Vec::new())
    }

    fn claim_jobs_by_type(
        &self,
        _worker_id: &str,
        _job_type: JobType,
        _enrichment_tier: Option<EnrichmentTier>,
        _limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        Ok(Vec::new())
    }

    fn record_batch_submission(
        &self,
        _batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn transition_batch_submission(
        &self,
        _completed_provider_batch_id: &str,
        _next_batch: &NewEnrichmentBatch,
        _jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        Ok(())
    }

    fn complete_batch(&self, _provider_batch_id: &str) -> StorageResult<()> {
        Ok(())
    }

    fn fail_batch_record(
        &self,
        _provider_batch_id: &str,
        _error_message: &str,
    ) -> StorageResult<()> {
        Ok(())
    }

    fn load_running_batches(
        &self,
        _stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        Ok(Vec::new())
    }

    fn reconcile_stale_running_batches(&self, _stage_name: &str) -> StorageResult<usize> {
        Ok(0)
    }

    fn reconcile_stale_running_jobs(&self, _worker_id: &str) -> StorageResult<usize> {
        Ok(0)
    }
}

#[derive(Default)]
struct FixedReadStore {
    loaded: Option<LoadedArtifactForEnrichment>,
}

impl FixedReadStore {
    fn with_sample_artifact() -> Self {
        Self {
            loaded: Some(LoadedArtifactForEnrichment {
                artifact: LoadedArtifactRecord {
                    artifact_id: "artifact-1".to_string(),
                    import_id: "import-1".to_string(),
                    artifact_class: open_archive::storage::ArtifactClass::Conversation,
                    source_type: SourceType::ChatGptExport,
                    title: Some("Test artifact".to_string()),
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

    fn list_artifacts_filtered(
        &self,
        _filters: &open_archive::storage::ArtifactListFilters,
        _limit: usize,
        _offset: usize,
    ) -> StorageResult<Vec<ArtifactListItem>> {
        Ok(Vec::new())
    }

    fn get_timeline(
        &self,
        _filters: &open_archive::storage::TimelineFilters,
        _limit: usize,
        _offset: usize,
    ) -> StorageResult<Vec<open_archive::storage::TimelineEntry>> {
        Ok(Vec::new())
    }

    fn load_artifact_for_enrichment(
        &self,
        _artifact_id: &str,
    ) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
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

struct MockRetrievalStore;

impl ArchiveRetrievalStore for MockRetrievalStore {
    fn retrieve_for_intents(
        &self,
        _artifact_id: &str,
        _intents: &[RetrievalIntent],
        _limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        Ok(Vec::new())
    }
}

impl ArchiveRetrievalServiceApi for MockRetrievalStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> open_archive::Result<Vec<RetrievedContextItem>> {
        ArchiveRetrievalStore::retrieve_for_intents(self, artifact_id, intents, limit_per_intent)
            .map_err(open_archive::OpenArchiveError::from)
    }
}

#[derive(Default)]
struct MockStateStore {
    extraction_results: Mutex<HashMap<String, ArtifactExtractionResult>>,
    retrieval_result_sets: Mutex<HashMap<String, RetrievalResultSet>>,
    reconciliation_decisions: Mutex<HashMap<String, Vec<ReconciliationDecision>>>,
}

impl EnrichmentStateStore for MockStateStore {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()> {
        self.extraction_results
            .lock()
            .unwrap()
            .insert(result.extraction_result_id.clone(), result.clone());
        Ok(())
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        Ok(self
            .extraction_results
            .lock()
            .unwrap()
            .get(extraction_result_id)
            .cloned())
    }

    fn save_retrieval_result_set(&self, result_set: &RetrievalResultSet) -> StorageResult<()> {
        self.retrieval_result_sets.lock().unwrap().insert(
            result_set.retrieval_result_set_id.clone(),
            result_set.clone(),
        );
        Ok(())
    }

    fn load_retrieval_result_set(
        &self,
        retrieval_result_set_id: &str,
    ) -> StorageResult<Option<RetrievalResultSet>> {
        Ok(self
            .retrieval_result_sets
            .lock()
            .unwrap()
            .get(retrieval_result_set_id)
            .cloned())
    }

    fn save_reconciliation_decisions(
        &self,
        decisions: &[ReconciliationDecision],
    ) -> StorageResult<()> {
        if let Some(first) = decisions.first() {
            self.reconciliation_decisions
                .lock()
                .unwrap()
                .insert(first.extraction_result_id.clone(), decisions.to_vec());
        }
        Ok(())
    }

    fn load_reconciliation_decisions(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Vec<ReconciliationDecision>> {
        Ok(self
            .reconciliation_decisions
            .lock()
            .unwrap()
            .get(extraction_result_id)
            .cloned()
            .unwrap_or_default())
    }
}

struct RejectAllFactory;

impl ArtifactProcessorFactory for RejectAllFactory {
    fn build(
        &self,
        _tier: EnrichmentTier,
    ) -> std::result::Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        Err(ProcessorError::Message {
            message: "tier rejected".to_string(),
        })
    }

    fn build_reconciliation_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> std::result::Result<
        Box<dyn open_archive::processor::ReconciliationProcessor>,
        ProcessorError,
    > {
        Err(ProcessorError::Message {
            message: "tier rejected".to_string(),
        })
    }
}

struct RetryableProcessorFactory;

impl ArtifactProcessorFactory for RetryableProcessorFactory {
    fn build(
        &self,
        _tier: EnrichmentTier,
    ) -> std::result::Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        Ok(Box::new(RetryableProcessor))
    }

    fn build_reconciliation_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> std::result::Result<
        Box<dyn open_archive::processor::ReconciliationProcessor>,
        ProcessorError,
    > {
        Ok(Box::new(RetryableReconciliationProcessor))
    }
}

struct RetryableProcessor;

impl ArtifactProcessor for RetryableProcessor {
    fn process(
        &self,
        _input: &open_archive::processor::ArtifactProcessorInput,
    ) -> std::result::Result<open_archive::processor::ArtifactProcessorOutput, ProcessorError> {
        Err(ProcessorError::InferenceHttpStatus {
            status: 429,
            body_preview: "rate limited".to_string(),
        })
    }
}

struct RetryableReconciliationProcessor;

impl open_archive::processor::ReconciliationProcessor for RetryableReconciliationProcessor {
    fn reconcile(
        &self,
        _input: &open_archive::processor::ReconciliationProcessorInput,
    ) -> std::result::Result<
        Vec<open_archive::processor::ReconciliationDecisionOutput>,
        ProcessorError,
    > {
        Err(ProcessorError::InferenceHttpStatus {
            status: 429,
            body_preview: "rate limited".to_string(),
        })
    }
}
