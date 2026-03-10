use crate::storage::{types, EnrichmentJobLifecycleStore};
use anyhow::Result;
use log::{debug, error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Worker ID format: enrichment:<pid>:<worker_index>
pub fn format_worker_id(pid: u32, worker_index: usize) -> String {
    format!("enrichment:{}:{}", pid, worker_index)
}

/// Placeholder enrichment executor - skeleton implementation
/// This is a no-op executor that always succeeds, suitable for skeleton runtime
pub fn placeholder_enrichment_executor(
    _payload: &types::ConversationEnrichmentPayload,
) -> Result<(), String> {
    // Skeleton implementation: validate payload shape but always succeed
    // Real enrichment logic will be implemented in subsequent milestones
    Ok(())
}

/// Enrichment worker that polls for jobs and processes them
///
/// # Arguments
/// * `worker_id` - Unique identifier for this worker
/// * `store` - Job lifecycle store for claiming and finalizing jobs
/// * `poll_interval` - Duration to sleep between poll attempts when no jobs available
/// * `shutdown` - Atomic flag that when true, causes the worker to exit after completing current work
/// * `executor` - Function to execute the job payload
fn enrichment_worker(
    worker_id: String,
    store: Arc<dyn EnrichmentJobLifecycleStore>,
    poll_interval: Duration,
    shutdown: Arc<AtomicBool>,
    executor: fn(&types::ConversationEnrichmentPayload) -> Result<(), String>,
) {
    info!("Enrichment worker {} starting", worker_id);

    loop {
        // Check shutdown flag
        if shutdown.load(Ordering::SeqCst) {
            info!("Enrichment worker {} shutting down", worker_id);
            break;
        }

        match store.claim_next_job(&worker_id) {
            Ok(Some(claimed_job)) => {
                debug!("Worker {} claimed job {}", worker_id, claimed_job.job_id);

                // Parse the payload JSON
                let payload = match types::ConversationEnrichmentPayload::from_json(
                    &claimed_job.payload_json,
                ) {
                    Ok(payload) => payload,
                    Err(e) => {
                        error!(
                            "Worker {} failed to parse payload for job {}: {}",
                            worker_id, claimed_job.job_id, e
                        );
                        if let Err(fail_err) = store.fail_job(
                            &worker_id,
                            &claimed_job.job_id,
                            "Failed to parse payload JSON",
                        ) {
                            error!(
                                "Worker {} failed to fail job {}: {}",
                                worker_id, claimed_job.job_id, fail_err
                            );
                        }
                        continue;
                    }
                };

                let result = executor(&payload);

                match result {
                    Ok(_) => {
                        if let Err(e) = store.complete_job(&worker_id, &claimed_job.job_id) {
                            error!(
                                "Worker {} failed to complete job {}: {}",
                                worker_id, claimed_job.job_id, e
                            );
                        } else {
                            debug!("Worker {} completed job {}", worker_id, claimed_job.job_id);
                        }
                    }
                    Err(error_message) => {
                        if let Err(e) =
                            store.fail_job(&worker_id, &claimed_job.job_id, &error_message)
                        {
                            error!(
                                "Worker {} failed to fail job {}: {}",
                                worker_id, claimed_job.job_id, e
                            );
                        } else {
                            debug!(
                                "Worker {} failed job {} with message: {}",
                                worker_id, claimed_job.job_id, error_message
                            );
                        }
                    }
                }
            }
            Ok(None) => {
                // No jobs available, sleep before next poll
                thread::sleep(poll_interval);
            }
            Err(e) => {
                error!("Worker {} failed to claim job: {}", worker_id, e);
                // Continue polling even on store errors
                thread::sleep(poll_interval);
            }
        }
    }
}

/// Start enrichment worker pool with shutdown capability
///
/// Returns both the worker thread handles and the shutdown flag.
/// Tests can set the shutdown flag to true to gracefully stop workers.
pub fn start_enrichment_workers(
    config: &crate::config::HttpConfig,
    store: Arc<dyn EnrichmentJobLifecycleStore>,
) -> Result<(Vec<thread::JoinHandle<()>>, Arc<AtomicBool>), anyhow::Error> {
    start_enrichment_workers_with_executor(config, store, placeholder_enrichment_executor)
}

#[doc(hidden)]
pub fn start_enrichment_workers_with_executor(
    config: &crate::config::HttpConfig,
    store: Arc<dyn EnrichmentJobLifecycleStore>,
    executor: fn(&types::ConversationEnrichmentPayload) -> Result<(), String>,
) -> Result<(Vec<thread::JoinHandle<()>>, Arc<AtomicBool>), anyhow::Error> {
    let pid = std::process::id();
    let poll_interval = Duration::from_millis(config.enrichment_poll_interval_ms);
    let shutdown = Arc::new(AtomicBool::new(false));

    let workers: Vec<_> = (0..config.enrichment_worker_count)
        .map(|worker_index| {
            let worker_id = format_worker_id(pid, worker_index);
            let store = Arc::clone(&store);
            let shutdown = Arc::clone(&shutdown);

            thread::Builder::new()
                .name(format!("enrichment-worker-{}", worker_index))
                .spawn(move || {
                    enrichment_worker(worker_id, store, poll_interval, shutdown, executor);
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn enrichment worker thread: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok((workers, shutdown))
}
