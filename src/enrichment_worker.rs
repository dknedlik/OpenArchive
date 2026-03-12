use crate::processor::{
    ArtifactProcessorFactory, ArtifactProcessorInput, ArtifactProcessorOutput,
    StubProcessorFactory,
};
use crate::shutdown::ShutdownToken;
use crate::storage::{
    ArtifactReadStore, ClaimedJob, ClassificationObjectJson, ArtifactEnrichmentPayload,
    DerivationRunStatus, DerivationRunType, DerivedMetadataWriteStore, DerivedObjectPayload,
    EnrichmentJobLifecycleStore, EvidenceRole, InputScopeType, MemoryObjectJson, NewDerivationRun,
    NewDerivedObject, NewEvidenceLink, ObjectStatus, OriginKind, ScopeType, SummaryObjectJson,
    SupportStrength, WriteDerivationAttempt, WriteDerivedObject,
};
use crate::domain::SourceTimestamp;
use anyhow::Result;
use log::{debug, error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Worker ID format: enrichment:<pid>:<worker_index>
pub fn format_worker_id(pid: u32, worker_index: usize) -> String {
    format!("enrichment:{}:{}", pid, worker_index)
}

fn enrichment_worker(
    worker_id: String,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
    poll_interval: Duration,
    shutdown: ShutdownToken,
) {
    info!("Enrichment worker {} starting", worker_id);

    loop {
        if shutdown.is_shutdown() {
            info!("Enrichment worker {} shutting down", worker_id);
            break;
        }

        match job_store.claim_next_job(&worker_id) {
            Ok(Some(claimed_job)) => {
                debug!("Worker {} claimed job {}", worker_id, claimed_job.job_id);
                if let Err(err) = process_claimed_job(
                    &worker_id,
                    &claimed_job,
                    job_store.as_ref(),
                    read_store.as_ref(),
                    derived_store.as_ref(),
                    processor_factory.as_ref(),
                ) {
                    error!(
                        "Worker {} failed to process job {}: {}",
                        worker_id, claimed_job.job_id, err
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

fn process_claimed_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    read_store: &dyn ArtifactReadStore,
    derived_store: &dyn DerivedMetadataWriteStore,
    processor_factory: &dyn ArtifactProcessorFactory,
) -> std::result::Result<(), String> {
    let payload = ArtifactEnrichmentPayload::from_json(&claimed_job.payload_json)
        .map_err(|err| fail_job(job_store, worker_id, &claimed_job.job_id, "Failed to parse payload JSON", err))?;

    let loaded = read_store
        .load_artifact_for_enrichment(&claimed_job.artifact_id)
        .map_err(|err| fail_job(job_store, worker_id, &claimed_job.job_id, "Failed to load artifact for enrichment", err))?
        .ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!("Artifact {} not found for enrichment", claimed_job.artifact_id),
            )
        })?;

    let source_type =
        crate::storage::SourceType::from_str(&payload.source_type).ok_or_else(|| {
            fail_job_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                format!(
                    "Invalid artifact source_type in enrichment payload: {}",
                    payload.source_type
                ),
            )
        })?;

    let processor_input = ArtifactProcessorInput {
        artifact_id: loaded.artifact.artifact_id.clone(),
        import_id: payload.import_id.clone(),
        source_type,
        title: loaded.artifact.title.clone(),
        participants: loaded.participants,
        segments: loaded.segments,
    };

    let processor = processor_factory
        .build(claimed_job.enrichment_tier)
        .map_err(|err| fail_job(job_store, worker_id, &claimed_job.job_id, "Failed to build enrichment processor", err))?;

    let output = processor
        .process(&processor_input)
        .map_err(|err| fail_job(job_store, worker_id, &claimed_job.job_id, "Processor execution failed", err))?;

    let attempt = build_derivation_attempt(claimed_job, &processor_input, &output);
    derived_store
        .write_derivation_attempt(attempt)
        .map_err(|err| fail_job(job_store, worker_id, &claimed_job.job_id, "Failed to persist derivation output", err))?;

    job_store
        .complete_job(worker_id, &claimed_job.job_id)
        .map_err(|err| format!("failed to complete job {}: {}", claimed_job.job_id, err))?;
    debug!("Worker {} completed job {}", worker_id, claimed_job.job_id);
    Ok(())
}

fn fail_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    context: &str,
    err: impl std::fmt::Display,
) -> String {
    fail_job_message(job_store, worker_id, job_id, format!("{context}: {err}"))
}

fn fail_job_message(
    job_store: &dyn EnrichmentJobLifecycleStore,
    worker_id: &str,
    job_id: &str,
    message: String,
) -> String {
    if let Err(fail_err) = job_store.fail_job(worker_id, job_id, &message) {
        format!("{message}; additionally failed to mark job failed: {fail_err}")
    } else {
        message
    }
}

fn build_derivation_attempt(
    claimed_job: &ClaimedJob,
    input: &ArtifactProcessorInput,
    output: &ArtifactProcessorOutput,
) -> WriteDerivationAttempt {
    let derivation_run_id = new_id("drvrun");
    let completed_at = SourceTimestamp::from(chrono::Utc::now());
    let mut objects = Vec::with_capacity(1 + output.classifications.len() + output.memories.len());

    let summary_object_id = new_id("dobj");
    objects.push(WriteDerivedObject {
        object: NewDerivedObject {
            derived_object_id: summary_object_id.clone(),
            artifact_id: input.artifact_id.clone(),
            derivation_run_id: derivation_run_id.clone(),
            origin_kind: OriginKind::Deterministic,
            object_status: ObjectStatus::Active,
            confidence_score: None,
            confidence_label: None,
            scope_type: ScopeType::Artifact,
            scope_id: input.artifact_id.clone(),
            supersedes_derived_object_id: None,
            payload: DerivedObjectPayload::Summary {
                title: output.summary.title.clone(),
                body_text: output.summary.body_text.clone(),
                object_json: Some(SummaryObjectJson {
                    summary_kind: Some("artifact".to_string()),
                    summary_version: Some(output.pipeline_version.clone()),
                }),
            },
        },
        evidence_links: build_evidence_links(&summary_object_id, &output.summary.evidence_segment_ids),
    });

    for classification in &output.classifications {
        let derived_object_id = new_id("dobj");
        objects.push(WriteDerivedObject {
            object: NewDerivedObject {
                derived_object_id: derived_object_id.clone(),
                artifact_id: input.artifact_id.clone(),
                derivation_run_id: derivation_run_id.clone(),
                origin_kind: OriginKind::Deterministic,
                object_status: ObjectStatus::Active,
                confidence_score: None,
                confidence_label: None,
                scope_type: ScopeType::Artifact,
                scope_id: input.artifact_id.clone(),
                supersedes_derived_object_id: None,
                payload: DerivedObjectPayload::Classification {
                    title: classification.title.clone(),
                    body_text: classification.body_text.clone(),
                    object_json: ClassificationObjectJson {
                        classification_type: classification.classification_type.clone(),
                        classification_value: classification.classification_value.clone(),
                    },
                },
            },
            evidence_links: build_evidence_links(
                &derived_object_id,
                &classification.evidence_segment_ids,
            ),
        });
    }

    for memory in &output.memories {
        let derived_object_id = new_id("dobj");
        objects.push(WriteDerivedObject {
            object: NewDerivedObject {
                derived_object_id: derived_object_id.clone(),
                artifact_id: input.artifact_id.clone(),
                derivation_run_id: derivation_run_id.clone(),
                origin_kind: OriginKind::Deterministic,
                object_status: ObjectStatus::Active,
                confidence_score: None,
                confidence_label: None,
                scope_type: memory.memory_scope,
                scope_id: memory.memory_scope_value.clone(),
                supersedes_derived_object_id: None,
                payload: DerivedObjectPayload::Memory {
                    title: memory.title.clone(),
                    body_text: memory.body_text.clone(),
                    object_json: MemoryObjectJson {
                        memory_type: memory.memory_type.clone(),
                        memory_scope: memory.memory_scope,
                        memory_scope_value: memory.memory_scope_value.clone(),
                    },
                },
            },
            evidence_links: build_evidence_links(&derived_object_id, &memory.evidence_segment_ids),
        });
    }

    WriteDerivationAttempt {
        run: NewDerivationRun {
            derivation_run_id,
            artifact_id: input.artifact_id.clone(),
            job_id: Some(claimed_job.job_id.clone()),
            run_type: DerivationRunType::SummaryExtraction,
            pipeline_name: output.pipeline_name.clone(),
            pipeline_version: output.pipeline_version.clone(),
            provider_name: Some("stub".to_string()),
            model_name: Some("stub".to_string()),
            prompt_version: Some("stub-v1".to_string()),
            run_status: DerivationRunStatus::Completed,
            input_scope_type: InputScopeType::Artifact,
            input_scope_json: serde_json::json!({
                "artifact_id": input.artifact_id,
                "segment_count": input.segments.len()
            })
            .to_string(),
            completed_at: Some(completed_at),
            error_message: None,
        },
        objects,
    }
}

fn build_evidence_links(derived_object_id: &str, segment_ids: &[String]) -> Vec<NewEvidenceLink> {
    segment_ids
        .iter()
        .enumerate()
        .map(|(index, segment_id)| NewEvidenceLink {
            evidence_link_id: new_id("evidence"),
            derived_object_id: derived_object_id.to_string(),
            segment_id: segment_id.clone(),
            evidence_role: if index == 0 {
                EvidenceRole::PrimarySupport
            } else {
                EvidenceRole::SecondarySupport
            },
            evidence_rank: (index + 1) as i64,
            support_strength: SupportStrength::Strong,
        })
        .collect()
}

fn new_id(prefix: &str) -> String {
    static ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos:x}-{counter:x}")
}

/// Start enrichment worker pool with shutdown capability.
pub fn start_enrichment_workers(
    config: &crate::config::HttpConfig,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
) -> Result<Vec<thread::JoinHandle<()>>, anyhow::Error> {
    start_enrichment_workers_with_factory(
        config,
        job_store,
        read_store,
        derived_store,
        shutdown,
        Arc::new(StubProcessorFactory),
    )
}

#[doc(hidden)]
pub fn start_enrichment_workers_with_factory(
    config: &crate::config::HttpConfig,
    job_store: Arc<dyn EnrichmentJobLifecycleStore>,
    read_store: Arc<dyn ArtifactReadStore>,
    derived_store: Arc<dyn DerivedMetadataWriteStore>,
    shutdown: ShutdownToken,
    processor_factory: Arc<dyn ArtifactProcessorFactory>,
) -> Result<Vec<thread::JoinHandle<()>>, anyhow::Error> {
    let pid = std::process::id();
    let poll_interval = Duration::from_millis(config.enrichment_poll_interval_ms);

    let workers: Vec<_> = (0..config.enrichment_worker_count)
        .map(|worker_index| {
            let worker_id = format_worker_id(pid, worker_index);
            let job_store = Arc::clone(&job_store);
            let read_store = Arc::clone(&read_store);
            let derived_store = Arc::clone(&derived_store);
            let shutdown = shutdown.clone();
            let processor_factory = Arc::clone(&processor_factory);

            thread::Builder::new()
                .name(format!("enrichment-worker-{}", worker_index))
                .spawn(move || {
                    enrichment_worker(
                        worker_id,
                        job_store,
                        read_store,
                        derived_store,
                        processor_factory,
                        poll_interval,
                        shutdown,
                    );
                })
                .map_err(|e| anyhow::anyhow!("Failed to spawn enrichment worker thread: {}", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(workers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::{ArtifactProcessor, ArtifactProcessorFactory, ProcessorError};
    use crate::storage::types::{
        EnrichmentTier, LoadedArtifactForEnrichment, LoadedArtifactRecord, LoadedParticipant,
        LoadedSegment, RetryOutcome,
    };
    use crate::storage::{ArtifactListItem, ArtifactReadStore};
    use std::sync::Mutex;

    struct MockJobStore {
        failed: Mutex<Vec<(String, String, String)>>,
        completed: Mutex<Vec<(String, String)>>,
    }

    impl MockJobStore {
        fn new() -> Self {
            Self {
                failed: Mutex::new(Vec::new()),
                completed: Mutex::new(Vec::new()),
            }
        }
    }

    impl EnrichmentJobLifecycleStore for MockJobStore {
        fn claim_next_job(
            &self,
            _worker_id: &str,
        ) -> crate::error::StorageResult<Option<ClaimedJob>> {
            Ok(None)
        }

        fn complete_job(&self, worker_id: &str, job_id: &str) -> crate::error::StorageResult<()> {
            self.completed
                .lock()
                .unwrap()
                .push((worker_id.to_string(), job_id.to_string()));
            Ok(())
        }

        fn fail_job(
            &self,
            worker_id: &str,
            job_id: &str,
            error_message: &str,
        ) -> crate::error::StorageResult<()> {
            self.failed.lock().unwrap().push((
                worker_id.to_string(),
                job_id.to_string(),
                error_message.to_string(),
            ));
            Ok(())
        }

        fn mark_job_retryable(
            &self,
            _worker_id: &str,
            _job_id: &str,
            _error_message: &str,
            _retry_after_seconds: i64,
        ) -> crate::error::StorageResult<RetryOutcome> {
            Ok(RetryOutcome::Retried)
        }
    }

    struct MockReadStore {
        loaded: Option<LoadedArtifactForEnrichment>,
    }

    impl ArtifactReadStore for MockReadStore {
        fn list_artifacts(&self) -> crate::error::StorageResult<Vec<ArtifactListItem>> {
            Ok(Vec::new())
        }

        fn load_artifact_for_enrichment(
            &self,
            _artifact_id: &str,
        ) -> crate::error::StorageResult<Option<LoadedArtifactForEnrichment>> {
            Ok(self.loaded.clone())
        }
    }

    struct MockDerivedStore {
        attempts: Mutex<Vec<WriteDerivationAttempt>>,
    }

    impl MockDerivedStore {
        fn new() -> Self {
            Self {
                attempts: Mutex::new(Vec::new()),
            }
        }
    }

    impl DerivedMetadataWriteStore for MockDerivedStore {
        fn write_derivation_attempt(
            &self,
            attempt: WriteDerivationAttempt,
        ) -> crate::error::StorageResult<crate::storage::DerivationWriteResult> {
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
            Ok(crate::storage::DerivationWriteResult {
                derivation_run_id,
                derived_object_ids,
                evidence_links_written,
            })
        }
    }

    struct FailingFactory;

    impl ArtifactProcessorFactory for FailingFactory {
        fn build(
            &self,
            _tier: EnrichmentTier,
        ) -> std::result::Result<Box<dyn ArtifactProcessor>, ProcessorError> {
            Err(ProcessorError::Message {
                message: "boom".to_string(),
            })
        }
    }

    #[test]
    fn process_claimed_job_persists_derivation_attempt_and_completes_job() {
        let job_store = MockJobStore::new();
        let read_store = MockReadStore {
            loaded: Some(LoadedArtifactForEnrichment {
                artifact: LoadedArtifactRecord {
                    artifact_id: "artifact-1".to_string(),
                    import_id: "import-1".to_string(),
                    source_type: crate::storage::SourceType::ChatGptExport,
                    title: Some("Test conversation".to_string()),
                },
                participants: vec![LoadedParticipant {
                    participant_id: "participant-1".to_string(),
                    participant_role: crate::ParticipantRole::User,
                    display_name: Some("User".to_string()),
                    external_id: Some("user-1".to_string()),
                }],
                segments: vec![LoadedSegment {
                    segment_id: "segment-1".to_string(),
                    participant_id: Some("participant-1".to_string()),
                    participant_role: Some(crate::ParticipantRole::User),
                    sequence_no: 0,
                    text_content: "hello".to_string(),
                    created_at_source: None,
                    visibility_status: crate::VisibilityStatus::Visible,
                }],
            }),
        };
        let derived_store = MockDerivedStore::new();
        let claimed_job = ClaimedJob {
            job_id: "job-1".to_string(),
            artifact_id: "artifact-1".to_string(),
            job_type: crate::storage::JobType::ArtifactEnrichment,
            enrichment_tier: EnrichmentTier::Standard,
            spawned_by_job_id: None,
            attempt_count: 1,
            max_attempts: 3,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactEnrichmentPayload::new_v1(
                "artifact-1",
                "import-1",
                crate::storage::SourceType::ChatGptExport,
            )
            .to_json(),
        };

        process_claimed_job(
            "worker-1",
            &claimed_job,
            &job_store,
            &read_store,
            &derived_store,
            &StubProcessorFactory,
        )
        .expect("job should succeed");

        assert_eq!(job_store.completed.lock().unwrap().len(), 1);
        assert!(job_store.failed.lock().unwrap().is_empty());
        let attempts = derived_store.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].run.job_id.as_deref(), Some("job-1"));
        assert!(attempts[0].objects.len() >= 3);
    }

    #[test]
    fn process_claimed_job_marks_failure_when_factory_rejects_tier() {
        let job_store = MockJobStore::new();
        let read_store = MockReadStore { loaded: None };
        let derived_store = MockDerivedStore::new();
        let claimed_job = ClaimedJob {
            job_id: "job-2".to_string(),
            artifact_id: "artifact-2".to_string(),
            job_type: crate::storage::JobType::ArtifactEnrichment,
            enrichment_tier: EnrichmentTier::Quality,
            spawned_by_job_id: None,
            attempt_count: 1,
            max_attempts: 3,
            required_capabilities: vec!["text".to_string()],
            payload_json: ArtifactEnrichmentPayload::new_v1(
                "artifact-2",
                "import-2",
                crate::storage::SourceType::ChatGptExport,
            )
            .to_json(),
        };

        let result = process_claimed_job(
            "worker-2",
            &claimed_job,
            &job_store,
            &read_store,
            &derived_store,
            &FailingFactory,
        );

        assert!(result.is_err());
        assert!(job_store.completed.lock().unwrap().is_empty());
        assert_eq!(job_store.failed.lock().unwrap().len(), 1);
    }
}
