use crate::error::{ConfigError, ConfigResult};
use std::env;
use std::time::Duration;

use super::env::{optional_duration_env_ms, optional_usize_env, positive_usize_env};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferenceExecutionMode {
    Direct,
    Batch,
}

impl InferenceExecutionMode {
    pub fn from_env() -> ConfigResult<Self> {
        let value = env::var("OA_INFERENCE_MODE").unwrap_or_else(|_| "batch".to_string());
        match value.as_str() {
            "direct" => Ok(Self::Direct),
            "batch" => Ok(Self::Batch),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_INFERENCE_MODE",
                value,
                expected: "direct, batch",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageConfig {
    pub batch_size: usize,
    pub max_concurrent_batches: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPipelineModeConfig {
    pub extract_workers: usize,
    pub reconcile_workers: usize,
    pub embedding_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPipelineModeConfig {
    pub extract_workers: usize,
    pub extract: StageConfig,
    pub reconcile_workers: usize,
    pub reconcile: StageConfig,
    pub embedding_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionChunkingConfig {
    pub max_segments_per_chunk: usize,
    pub chunk_overlap_segments: usize,
    pub max_chars_per_chunk: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentPipelineConfig {
    pub poll_interval: Duration,
    pub direct: DirectPipelineModeConfig,
    pub batch: BatchPipelineModeConfig,
    pub chunking: ExtractionChunkingConfig,
}

impl EnrichmentPipelineConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let shared_worker_default = optional_usize_env("OA_ENRICHMENT_WORKERS")?.unwrap_or(1);
        let direct_extract_workers =
            positive_usize_env("OA_DIRECT_EXTRACT_WORKERS")?.unwrap_or(shared_worker_default);
        let direct_reconcile_workers =
            positive_usize_env("OA_DIRECT_RECONCILE_WORKERS")?.unwrap_or(shared_worker_default);
        let direct_embedding_workers =
            positive_usize_env("OA_DIRECT_EMBEDDING_WORKERS")?.unwrap_or(1);
        let batch_extract_workers =
            positive_usize_env("OA_BATCH_EXTRACT_WORKERS")?.unwrap_or(shared_worker_default);
        let batch_reconcile_workers =
            positive_usize_env("OA_BATCH_RECONCILE_WORKERS")?.unwrap_or(shared_worker_default);
        let batch_embedding_workers =
            positive_usize_env("OA_BATCH_EMBEDDING_WORKERS")?.unwrap_or(1);
        Ok(Self {
            poll_interval: optional_duration_env_ms("OA_ENRICHMENT_POLL_INTERVAL_MS")?
                .unwrap_or(Duration::from_millis(2000)),
            direct: DirectPipelineModeConfig {
                extract_workers: direct_extract_workers,
                reconcile_workers: direct_reconcile_workers,
                embedding_workers: direct_embedding_workers,
            },
            batch: BatchPipelineModeConfig {
                extract_workers: batch_extract_workers,
                extract: StageConfig {
                    batch_size: positive_usize_env("OA_BATCH_EXTRACT_BATCH_SIZE")?.unwrap_or(5),
                    max_concurrent_batches: positive_usize_env("OA_BATCH_EXTRACT_MAX_CONCURRENT")?
                        .unwrap_or(3),
                },
                reconcile_workers: batch_reconcile_workers,
                reconcile: StageConfig {
                    batch_size: positive_usize_env("OA_BATCH_RECONCILE_BATCH_SIZE")?.unwrap_or(5),
                    max_concurrent_batches: positive_usize_env(
                        "OA_BATCH_RECONCILE_MAX_CONCURRENT",
                    )?
                    .unwrap_or(2),
                },
                embedding_workers: batch_embedding_workers,
            },
            chunking: ExtractionChunkingConfig {
                max_segments_per_chunk: positive_usize_env("OA_EXTRACT_CHUNK_SEGMENTS")?
                    .unwrap_or(20),
                chunk_overlap_segments: optional_usize_env("OA_EXTRACT_CHUNK_OVERLAP")?
                    .unwrap_or(4),
                max_chars_per_chunk: positive_usize_env("OA_EXTRACT_CHUNK_MAX_CHARS")?
                    .unwrap_or(25_000),
            },
        })
    }
}
