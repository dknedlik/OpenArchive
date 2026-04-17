mod context;
mod embedding;
mod extract;
mod jobs;
mod pipeline;
mod reconcile;

#[cfg(test)]
mod tests;

use crate::config::ExtractionChunkingConfig;
use crate::error::{WorkerError, WorkerResult};
use crate::processor::StubProcessorFactory;
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArtifactReadStore, DerivedMetadataWriteStore, EnrichmentJobLifecycleStore, EnrichmentStateStore,
};
use context::{ExtractionPolicy, WorkerResources};
use jobs::process_claimed_jobs;
use log::{debug, error, info};
use rand::random;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use context::{EnrichmentPipelineResources, WorkerStartConfig};
pub use pipeline::start_enrichment_pipeline;
pub use reconcile::{
    build_reconciliation_input_from_processor_output, inspect_reconciliation_work,
    ReconciliationCandidateDisposition, ReconciliationCandidateTrace,
    ReconciliationPreparationReport,
};

/// Worker ID format: enrichment:<pid>:<worker_index>
pub fn format_worker_id(pid: u32, worker_index: usize) -> String {
    format!("enrichment:{}:{}", pid, worker_index)
}

fn enrichment_worker(
    worker_id: String,
    res: WorkerResources,
    chunking: Arc<ExtractionChunkingConfig>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!("Enrichment worker {} starting", worker_id);

    loop {
        if shutdown.is_shutdown() {
            info!("Enrichment worker {} shutting down", worker_id);
            break;
        }

        match res.job_store.claim_next_job(&worker_id) {
            Ok(Some(claimed_job)) => {
                debug!("Worker {} claimed job {}", worker_id, claimed_job.job_id);
                let ctx = res.as_context();
                let policy = ExtractionPolicy {
                    chunking: chunking.as_ref(),
                };
                if let Err(err) = process_claimed_jobs(
                    &worker_id,
                    claimed_job,
                    &ctx,
                    crate::config::InferenceExecutionMode::Direct,
                    &policy,
                ) {
                    error!(
                        "Worker {} failed to process claimed work: {}",
                        worker_id, err
                    );
                }
            }
            Ok(None) => thread::sleep(poll_interval),
            Err(err) => {
                error!("Worker {} failed to claim job: {}", worker_id, err);
                thread::sleep(poll_interval);
            }
        }
    }
}

pub(super) fn complete_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
) -> std::result::Result<(), String> {
    job_store
        .complete_job(worker_id, job_id)
        .map_err(|err| format!("failed to complete job {}: {}", job_id, err))
}

pub(super) fn new_id(prefix: &str) -> String {
    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let entropy = random::<u64>();
    format!("{prefix}-{nanos:x}-{counter:x}-{entropy:x}")
}

/// Start enrichment worker pool with shutdown capability.
pub fn start_enrichment_workers(
    config: &crate::config::HttpConfig,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    _retrieval_service: Arc<dyn crate::app::retrieval::ArchiveRetrievalServiceApi>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    start_enrichment_workers_with_factory(
        WorkerStartConfig {
            http: config,
            shutdown,
            processor_factory: Arc::new(StubProcessorFactory),
        },
        EnrichmentPipelineResources {
            job_store,
            read_store,
            state_store,
            derived_store,
            cross_artifact_store: None,
            embedding_store: None,
            embedding_provider: None,
        },
    )
}

#[doc(hidden)]
pub fn start_enrichment_workers_with_factory(
    config: WorkerStartConfig<'_>,
    resources: EnrichmentPipelineResources,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let WorkerStartConfig {
        http,
        shutdown,
        processor_factory,
    } = config;
    let EnrichmentPipelineResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store: _,
        embedding_store: _,
        embedding_provider: _,
    } = resources;
    let pid = std::process::id();
    let poll_interval = Duration::from_millis(http.enrichment_poll_interval_ms);

    let workers: Vec<_> = (0..http.enrichment_worker_count)
        .map(|worker_index| {
            let worker_id = format_worker_id(pid, worker_index);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let state_store = Arc::clone(&state_store);
            let derived_store = Arc::clone(&derived_store);
            let shutdown = shutdown.clone();
            let processor_factory = Arc::clone(&processor_factory);
            let chunking = Arc::new(ExtractionChunkingConfig {
                max_segments_per_chunk: 20,
                chunk_overlap_segments: 4,
                max_chars_per_chunk: 25_000,
            });
            thread::Builder::new()
                .name(format!("enrichment-worker-{}", worker_index))
                .spawn(move || {
                    enrichment_worker(
                        worker_id,
                        WorkerResources {
                            job_store,
                            read_store,
                            state_store,
                            derived_store,
                            cross_artifact_store: None,
                            embedding_store: None,
                            embedding_provider: None,
                            processor_factory,
                        },
                        chunking,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|source| WorkerError::SpawnThread {
                    worker_kind: format!("enrichment worker {}", worker_index),
                    source: Box::new(source),
                })
        })
        .collect::<WorkerResult<Vec<_>>>()?;

    Ok(workers)
}
