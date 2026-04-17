use super::context::{ExtractionPolicy, WorkerContext, WorkerResources};
use super::embedding::embedding_worker_loop;
use super::jobs::process_claimed_jobs;
use crate::config::{EnrichmentPipelineConfig, ExtractionChunkingConfig, InferenceExecutionMode};
use crate::embedding::EmbeddingProvider;
use crate::error::{WorkerError, WorkerResult};
use crate::processor::ArtifactProcessorFactory;
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArtifactReadStore, CrossArtifactReadStore, DerivedMetadataWriteStore,
    DerivedObjectEmbeddingStore, EnrichmentJobLifecycleStore, EnrichmentStateStore, EnrichmentTier,
    JobType,
};
use log::{debug, error, info};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub fn start_enrichment_pipeline(
    config: &EnrichmentPipelineConfig,
    execution_mode: InferenceExecutionMode,
    resources: super::EnrichmentPipelineResources,
    shutdown: ShutdownToken,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let super::EnrichmentPipelineResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store,
        embedding_store,
        embedding_provider,
    } = resources;
    if execution_mode == InferenceExecutionMode::Batch {
        validate_batch_execution_support(processor_factory.as_ref())?;
    }
    let pid = std::process::id();
    let chunking = Arc::new(config.chunking.clone());
    let poll_interval = config.poll_interval;
    let direct_resources = Arc::new(WorkerResources {
        job_store: Arc::clone(&job_store),
        read_store: Arc::clone(&read_store),
        state_store: Arc::clone(&state_store),
        derived_store: Arc::clone(&derived_store),
        cross_artifact_store: cross_artifact_store.clone(),
        embedding_store: embedding_store.clone(),
        embedding_provider: embedding_provider.clone(),
        processor_factory: Arc::clone(&processor_factory),
    });
    let mut handles = Vec::new();

    match execution_mode {
        InferenceExecutionMode::Batch => {
            let mode = &config.batch;
            handles.extend(spawn_extract_batch_workers(BatchWorkerConfig {
                pid,
                worker_count: mode.extract_workers,
                job_store: Arc::clone(&job_store),
                read_store: Arc::clone(&read_store),
                state_store: Arc::clone(&state_store),
                derived_store: Arc::clone(&derived_store),
                cross_artifact_store: cross_artifact_store.clone(),
                embedding_store: embedding_store.clone(),
                embedding_provider: embedding_provider.clone(),
                processor_factory: Arc::clone(&processor_factory),
                chunking: Some(Arc::clone(&chunking)),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
            handles.extend(spawn_reconcile_batch_workers(BatchWorkerConfig {
                pid,
                worker_count: mode.reconcile_workers,
                job_store: Arc::clone(&job_store),
                read_store: Arc::clone(&read_store),
                state_store: Arc::clone(&state_store),
                derived_store: Arc::clone(&derived_store),
                cross_artifact_store: cross_artifact_store.clone(),
                embedding_store: embedding_store.clone(),
                embedding_provider: embedding_provider.clone(),
                processor_factory: Arc::clone(&processor_factory),
                chunking: Some(Arc::clone(&chunking)),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
        }
        InferenceExecutionMode::Direct => {
            let mode = &config.direct;
            handles.extend(spawn_direct_stage_workers(DirectStageWorkerConfig {
                pid,
                stage_name: "extract",
                job_type: JobType::ArtifactExtract,
                execution_mode: InferenceExecutionMode::Direct,
                worker_count: mode.extract_workers,
                resources: Arc::clone(&direct_resources),
                chunking: Arc::clone(&chunking),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
            handles.extend(spawn_direct_stage_workers(DirectStageWorkerConfig {
                pid,
                stage_name: "reconcile",
                job_type: JobType::ArtifactReconcile,
                execution_mode: InferenceExecutionMode::Direct,
                worker_count: mode.reconcile_workers,
                resources: Arc::clone(&direct_resources),
                chunking: Arc::clone(&chunking),
                poll_interval,
                shutdown: shutdown.clone(),
            })?);
        }
    }

    let (embedding_workers, extract_workers, reconcile_workers) = match execution_mode {
        InferenceExecutionMode::Batch => (
            config.batch.embedding_workers,
            config.batch.extract_workers,
            config.batch.reconcile_workers,
        ),
        InferenceExecutionMode::Direct => (
            config.direct.embedding_workers,
            config.direct.extract_workers,
            config.direct.reconcile_workers,
        ),
    };

    if embedding_workers > 0 {
        if let (Some(embedding_store), Some(embedding_provider)) =
            (embedding_store, embedding_provider)
        {
            handles.extend(spawn_embedding_workers(
                pid,
                embedding_workers,
                Arc::clone(&job_store),
                embedding_store,
                embedding_provider,
                poll_interval,
                shutdown.clone(),
            )?);
        } else {
            info!("Embedding workers configured but embedding provider/store unavailable");
        }
    }

    info!(
        "Enrichment pipeline started: mode={:?}, extract_workers={}, reconcile_workers={}, embedding_workers={}, chunk_segments={}, chunk_overlap={}, chunk_max_chars={}",
        execution_mode,
        extract_workers,
        reconcile_workers,
        embedding_workers,
        config.chunking.max_segments_per_chunk,
        config.chunking.chunk_overlap_segments,
        config.chunking.max_chars_per_chunk
    );
    Ok(handles)
}

fn validate_batch_execution_support(
    processor_factory: &dyn ArtifactProcessorFactory,
) -> WorkerResult<()> {
    if processor_factory
        .build_batch_processor(EnrichmentTier::Default)?
        .is_none()
    {
        return Err(WorkerError::MissingBatchProcessor {
            stage: "extraction",
        });
    }
    if processor_factory
        .build_reconciliation_batch_processor(EnrichmentTier::Default)?
        .is_none()
    {
        return Err(WorkerError::MissingBatchProcessor {
            stage: "reconciliation",
        });
    }
    Ok(())
}

struct BatchWorkerConfig {
    pid: u32,
    worker_count: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    state_store: Arc<dyn EnrichmentStateStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
    embedding_store: Option<Arc<dyn DerivedObjectEmbeddingStore>>,
    embedding_provider: Option<Arc<dyn EmbeddingProvider>>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    chunking: Option<Arc<ExtractionChunkingConfig>>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
}

fn spawn_reconcile_batch_workers(
    config: BatchWorkerConfig,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let BatchWorkerConfig {
        pid,
        worker_count,
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store,
        embedding_store,
        embedding_provider,
        processor_factory,
        chunking,
        poll_interval,
        shutdown,
    } = config;
    let chunking = chunking.expect("reconcile batch workers require chunking placeholder");
    let resources = Arc::new(WorkerResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store,
        embedding_store,
        embedding_provider,
        processor_factory,
    });
    spawn_direct_stage_workers(DirectStageWorkerConfig {
        pid,
        stage_name: "reconcile",
        job_type: JobType::ArtifactReconcile,
        execution_mode: InferenceExecutionMode::Batch,
        worker_count,
        resources,
        chunking,
        poll_interval,
        shutdown,
    })
}

fn spawn_extract_batch_workers(
    config: BatchWorkerConfig,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    let BatchWorkerConfig {
        pid,
        worker_count,
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store: _,
        embedding_store: _,
        embedding_provider: _,
        processor_factory,
        chunking,
        poll_interval,
        shutdown,
    } = config;
    let chunking = chunking.expect("extract batch workers require chunking");
    let resources = Arc::new(WorkerResources {
        job_store,
        read_store,
        state_store,
        derived_store,
        cross_artifact_store: None,
        embedding_store: None,
        embedding_provider: None,
        processor_factory,
    });
    spawn_direct_stage_workers(DirectStageWorkerConfig {
        pid,
        stage_name: "extract",
        job_type: JobType::ArtifactExtract,
        execution_mode: InferenceExecutionMode::Batch,
        worker_count,
        resources,
        chunking,
        poll_interval,
        shutdown,
    })
}

struct DirectStageWorkerConfig {
    pid: u32,
    stage_name: &'static str,
    job_type: JobType,
    execution_mode: InferenceExecutionMode,
    worker_count: usize,
    resources: Arc<WorkerResources>,
    chunking: Arc<ExtractionChunkingConfig>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
}

fn spawn_direct_stage_workers(
    cfg: DirectStageWorkerConfig,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    (0..cfg.worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:{}:{}", cfg.pid, cfg.stage_name, i);
            let resources = Arc::clone(&cfg.resources);
            let chunking = Arc::clone(&cfg.chunking);
            let shutdown = cfg.shutdown.clone();
            thread::Builder::new()
                .name(format!("direct-{}-{}", cfg.stage_name, i))
                .spawn(move || {
                    let ctx = resources.as_context();
                    let policy = ExtractionPolicy {
                        chunking: chunking.as_ref(),
                    };
                    direct_stage_worker_loop(
                        worker_id,
                        cfg.job_type,
                        cfg.execution_mode,
                        &ctx,
                        &policy,
                        cfg.poll_interval,
                        shutdown,
                    );
                })
                .map_err(|source| WorkerError::SpawnThread {
                    worker_kind: format!("direct {} worker {}", cfg.stage_name, i),
                    source: Box::new(source),
                })
        })
        .collect()
}

fn direct_stage_worker_loop(
    worker_id: String,
    job_type: JobType,
    execution_mode: InferenceExecutionMode,
    ctx: &WorkerContext<'_>,
    policy: &ExtractionPolicy<'_>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!(
        "Direct stage worker {} starting for {:?}",
        worker_id, job_type
    );

    loop {
        if shutdown.is_shutdown() {
            info!("Direct stage worker {} shutting down", worker_id);
            break;
        }

        match ctx.job_store.claim_jobs_by_type(
            &worker_id,
            job_type,
            Some(EnrichmentTier::Default),
            1,
        ) {
            Ok(jobs) if !jobs.is_empty() => {
                let job = jobs.into_iter().next().expect("one claimed job");
                debug!(
                    "Direct stage worker {} claimed {} job {}",
                    worker_id,
                    job.job_type.as_str(),
                    job.job_id
                );
                if let Err(err) = process_claimed_jobs(&worker_id, job, ctx, execution_mode, policy)
                {
                    error!(
                        "Direct stage worker {} failed to process stage job: {}",
                        worker_id, err
                    );
                }
            }
            Ok(_) => thread::sleep(poll_interval),
            Err(err) => {
                error!(
                    "Direct stage worker {} failed to claim job: {}",
                    worker_id, err
                );
                thread::sleep(poll_interval);
            }
        }
    }
}

fn spawn_embedding_workers(
    pid: u32,
    worker_count: usize,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    embedding_store: Arc<dyn DerivedObjectEmbeddingStore>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) -> WorkerResult<Vec<thread::JoinHandle<()>>> {
    (0..worker_count)
        .map(|i| {
            let worker_id = format!("enrichment:{}:embed:{}", pid, i);
            let job_store = Arc::clone(&job_store);
            let embedding_store = Arc::clone(&embedding_store);
            let embedding_provider = Arc::clone(&embedding_provider);
            let shutdown = shutdown.clone();

            thread::Builder::new()
                .name(format!("embed-{}", i))
                .spawn(move || {
                    embedding_worker_loop(
                        worker_id,
                        job_store.as_ref(),
                        embedding_store.as_ref(),
                        embedding_provider.as_ref(),
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|source| WorkerError::SpawnThread {
                    worker_kind: format!("embedding worker {}", i),
                    source: Box::new(source),
                })
        })
        .collect()
}
