use open_archive::config::HttpConfig;
use open_archive::enrichment_worker::{
    format_worker_id, start_enrichment_workers, start_enrichment_workers_with_executor,
};
use open_archive::error::StorageResult;
use open_archive::storage::types::{
    ClaimedJob, ConversationEnrichmentPayload, JobType, RetryOutcome, SourceType,
};
use open_archive::storage::EnrichmentJobLifecycleStore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_format_worker_id() {
    let id = format_worker_id(123, 0);
    assert_eq!(id, "enrichment:123:0");

    let id2 = format_worker_id(456, 1);
    assert_eq!(id2, "enrichment:456:1");
}

#[test]
fn test_disabled_workers_start() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 4,
        enrichment_worker_count: 0,
        enrichment_poll_interval_ms: 500,
    };

    let store = Arc::new(MockStore::new());
    let result = start_enrichment_workers(&config, store);
    assert!(result.is_ok());
    let (workers, _) = result.unwrap();
    assert!(workers.is_empty());
}

#[test]
fn test_enabled_workers_start() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 4,
        enrichment_worker_count: 2,
        enrichment_poll_interval_ms: 500,
    };

    let store = Arc::new(MockStore::new());
    let result = start_enrichment_workers(&config, store);
    assert!(result.is_ok());
    let (workers, shutdown) = result.unwrap();
    assert_eq!(workers.len(), 2);

    for worker in &workers {
        let name = worker.thread().name().unwrap();
        assert!(name.starts_with("enrichment-worker-"));
    }

    // Clean shutdown
    shutdown.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("Worker should join cleanly");
    }
}

#[test]
fn test_empty_queue_handling() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 1,
        enrichment_worker_count: 1,
        enrichment_poll_interval_ms: 10,
    };

    let store = Arc::new(EmptyQueueMockStore::new());
    let result = start_enrichment_workers(&config, store.clone());
    assert!(result.is_ok());
    let (workers, shutdown) = result.unwrap();

    // Give worker time to poll at least once
    std::thread::sleep(Duration::from_millis(50));

    let claim_count = store.claim_count.load(Ordering::SeqCst);
    assert!(claim_count > 0, "Worker should poll even with empty queue");

    // Clean shutdown
    shutdown.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("Worker should join cleanly");
    }
}

#[test]
fn test_worker_calls_complete_on_success() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 1,
        enrichment_worker_count: 1,
        enrichment_poll_interval_ms: 10,
    };

    let store = Arc::new(MockStore::new());
    let result = start_enrichment_workers(&config, store.clone());
    assert!(result.is_ok());
    let (workers, shutdown) = result.unwrap();

    // Wait for at least one job to be processed
    std::thread::sleep(Duration::from_millis(50));

    let claim_count = store.claim_count.load(Ordering::SeqCst);
    let complete_count = store.complete_count.load(Ordering::SeqCst);

    assert!(claim_count > 0, "Worker should claim jobs");
    assert!(
        complete_count > 0,
        "At least some jobs should be completed (placeholder always succeeds)"
    );

    // Clean shutdown
    shutdown.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("Worker should join cleanly");
    }
}

#[test]
fn test_worker_calls_fail_on_payload_parse_error() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 1,
        enrichment_worker_count: 1,
        enrichment_poll_interval_ms: 10,
    };

    let store = Arc::new(InvalidPayloadMockStore::new());
    let result = start_enrichment_workers(&config, store.clone());
    assert!(result.is_ok());
    let (workers, shutdown) = result.unwrap();

    // Wait for worker to process the job
    std::thread::sleep(Duration::from_millis(50));

    let claim_count = store.claim_count.load(Ordering::SeqCst);
    let fail_count = store.fail_count.load(Ordering::SeqCst);

    assert!(claim_count > 0, "Worker should claim jobs");
    assert!(
        fail_count > 0,
        "Worker should call fail_job when payload parsing fails"
    );

    // Clean shutdown
    shutdown.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("Worker should join cleanly");
    }
}

#[test]
fn test_worker_logs_error_on_store_failure() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 1,
        enrichment_worker_count: 1,
        enrichment_poll_interval_ms: 10,
    };

    let store = Arc::new(FailingCompleteMockStore::new());
    let result = start_enrichment_workers(&config, store.clone());
    assert!(result.is_ok());
    let (workers, shutdown) = result.unwrap();

    // Wait for worker to process the job
    std::thread::sleep(Duration::from_millis(50));

    let claim_count = store.claim_count.load(Ordering::SeqCst);

    assert!(claim_count > 0, "Worker should claim jobs");
    // Note: We can't directly test the error logging, but the worker should
    // not crash when store operations fail

    // Clean shutdown
    shutdown.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("Worker should join cleanly");
    }
}

#[test]
fn test_invalid_poll_interval_rejected() {
    std::env::remove_var("OA_ENRICHMENT_POLL_INTERVAL_MS");
    std::env::set_var("OA_ENRICHMENT_POLL_INTERVAL_MS", "invalid");

    let result = HttpConfig::from_env();

    std::env::remove_var("OA_ENRICHMENT_POLL_INTERVAL_MS");

    assert!(result.is_err());
}

#[test]
fn test_zero_workers_allowed() {
    std::env::remove_var("OA_ENRICHMENT_WORKERS");
    std::env::set_var("OA_ENRICHMENT_WORKERS", "0");

    let result = HttpConfig::from_env();

    std::env::remove_var("OA_ENRICHMENT_WORKERS");

    assert!(
        result.is_ok(),
        "Config should accept OA_ENRICHMENT_WORKERS=0"
    );
    assert_eq!(result.unwrap().enrichment_worker_count, 0);
}

// Mock store implementations

struct MockStore {
    claim_count: AtomicUsize,
    complete_count: AtomicUsize,
    fail_count: AtomicUsize,
}

impl MockStore {
    fn new() -> Self {
        Self {
            claim_count: AtomicUsize::new(0),
            complete_count: AtomicUsize::new(0),
            fail_count: AtomicUsize::new(0),
        }
    }
}

impl EnrichmentJobLifecycleStore for MockStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);

        let job_id = format!("job-{}", self.claim_count.load(Ordering::SeqCst));
        let payload = ConversationEnrichmentPayload::new_v1(
            "test-artifact",
            "test-import",
            SourceType::ChatGptExport,
        );

        Ok(Some(ClaimedJob {
            job_id,
            artifact_id: payload.artifact_id.clone(),
            job_type: JobType::ConversationEnrichment,
            attempt_count: 1,
            max_attempts: 3,
            payload_json: payload.to_json(),
        }))
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

struct InvalidPayloadMockStore {
    claim_count: AtomicUsize,
    fail_count: AtomicUsize,
}

impl InvalidPayloadMockStore {
    fn new() -> Self {
        Self {
            claim_count: AtomicUsize::new(0),
            fail_count: AtomicUsize::new(0),
        }
    }
}

impl EnrichmentJobLifecycleStore for InvalidPayloadMockStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);

        let job_id = format!("job-{}", self.claim_count.load(Ordering::SeqCst));

        Ok(Some(ClaimedJob {
            job_id,
            artifact_id: "test-artifact".to_string(),
            job_type: JobType::ConversationEnrichment,
            attempt_count: 1,
            max_attempts: 3,
            payload_json: "invalid json {{{".to_string(),
        }))
    }

    fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
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

struct FailingCompleteMockStore {
    claim_count: AtomicUsize,
}

impl FailingCompleteMockStore {
    fn new() -> Self {
        Self {
            claim_count: AtomicUsize::new(0),
        }
    }
}

impl EnrichmentJobLifecycleStore for FailingCompleteMockStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);

        let job_id = format!("job-{}", self.claim_count.load(Ordering::SeqCst));
        let payload = ConversationEnrichmentPayload::new_v1(
            "test-artifact",
            "test-import",
            SourceType::ChatGptExport,
        );

        Ok(Some(ClaimedJob {
            job_id,
            artifact_id: payload.artifact_id.clone(),
            job_type: JobType::ConversationEnrichment,
            attempt_count: 1,
            max_attempts: 3,
            payload_json: payload.to_json(),
        }))
    }

    fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
        // Simulate a database error by returning an arbitrary storage error
        use open_archive::error::StorageError;
        Err(StorageError::ClaimJob {
            source: oracle::Error::new(
                oracle::ErrorKind::InvalidOperation,
                "Test: database connection failed",
            ),
        })
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

struct FailingExecutorMockStore {
    claim_count: AtomicUsize,
    fail_count: AtomicUsize,
}

impl FailingExecutorMockStore {
    fn new() -> Self {
        Self {
            claim_count: AtomicUsize::new(0),
            fail_count: AtomicUsize::new(0),
        }
    }
}

impl EnrichmentJobLifecycleStore for FailingExecutorMockStore {
    fn claim_next_job(&self, _worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        self.claim_count.fetch_add(1, Ordering::SeqCst);

        let job_id = format!("job-{}", self.claim_count.load(Ordering::SeqCst));
        let payload = ConversationEnrichmentPayload::new_v1(
            "test-artifact",
            "test-import",
            SourceType::ChatGptExport,
        );

        Ok(Some(ClaimedJob {
            job_id,
            artifact_id: payload.artifact_id.clone(),
            job_type: JobType::ConversationEnrichment,
            attempt_count: 1,
            max_attempts: 3,
            payload_json: payload.to_json(),
        }))
    }

    fn complete_job(&self, _worker_id: &str, _job_id: &str) -> StorageResult<()> {
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

#[test]
fn test_worker_calls_fail_on_executor_error() {
    let config = HttpConfig {
        bind_addr: "127.0.0.1:3000".to_string(),
        request_worker_count: 1,
        enrichment_worker_count: 1,
        enrichment_poll_interval_ms: 10,
    };

    let store = Arc::new(FailingExecutorMockStore::new());
    let result = start_enrichment_workers_with_executor(&config, store.clone(), |_payload| {
        Err("test executor failure".to_string())
    });
    assert!(result.is_ok());
    let (workers, shutdown) = result.unwrap();

    // Wait for worker to process the job
    std::thread::sleep(Duration::from_millis(50));

    let claim_count = store.claim_count.load(Ordering::SeqCst);
    let fail_count = store.fail_count.load(Ordering::SeqCst);

    assert!(claim_count > 0, "Worker should claim jobs");
    assert!(
        fail_count > 0,
        "Worker should call fail_job when executor fails"
    );

    // Clean shutdown
    shutdown.store(true, Ordering::SeqCst);
    for worker in workers {
        worker.join().expect("Worker should join cleanly");
    }
}
