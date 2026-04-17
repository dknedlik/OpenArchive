use super::jobs::{fail_job, fail_job_message, mark_job_retryable_message};
use super::{complete_job, new_id};
use crate::embedding::EmbeddingProvider;
use crate::storage::{
    ClaimedJob, DerivedObjectEmbeddingItem, DerivedObjectEmbeddingPayload,
    DerivedObjectEmbeddingStore, EnrichmentJobLifecycleStore, EnrichmentTier, JobStatus, JobType,
    NewDerivedObjectEmbedding, NewEnrichmentJob, ObjectStatus, WriteDerivedObject,
};
use log::{error, info, warn};
use sha2::{Digest, Sha256};
use std::thread;
use std::time::Duration;

const RETRYABLE_INFERENCE_BACKOFF_SECONDS: i64 = 60;

pub(super) fn process_embedding_job(
    worker_id: &str,
    claimed_job: &ClaimedJob,
    job_store: &dyn EnrichmentJobLifecycleStore,
    embedding_store: Option<&dyn DerivedObjectEmbeddingStore>,
    embedding_provider: Option<&dyn EmbeddingProvider>,
) -> std::result::Result<(), String> {
    let payload =
        DerivedObjectEmbeddingPayload::from_json(&claimed_job.payload_json).map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to parse embedding payload JSON",
                err,
            )
        })?;

    let Some(embedding_store) = embedding_store else {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "No embedding store configured for derived object embeddings".to_string(),
        ));
    };
    let Some(embedding_provider) = embedding_provider else {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            "No embedding provider configured for derived object embeddings".to_string(),
        ));
    };

    let texts = payload
        .objects
        .iter()
        .map(DerivedObjectEmbeddingItem::text_for_embedding)
        .collect::<Vec<_>>();
    let vectors = embedding_provider.embed_texts(&texts).map_err(|err| {
        let message = format!("Embedding generation failed: {err}");
        if embedding_error_is_retryable(&err) {
            mark_job_retryable_message(
                job_store,
                worker_id,
                &claimed_job.job_id,
                message,
                RETRYABLE_INFERENCE_BACKOFF_SECONDS,
            )
        } else {
            fail_job_message(job_store, worker_id, &claimed_job.job_id, message)
        }
    })?;

    if vectors.len() != payload.objects.len() {
        return Err(fail_job_message(
            job_store,
            worker_id,
            &claimed_job.job_id,
            format!(
                "Embedding provider returned {} vectors for {} objects",
                vectors.len(),
                payload.objects.len()
            ),
        ));
    }

    let embeddings = payload
        .objects
        .iter()
        .zip(vectors.into_iter())
        .map(|(object, embedding)| {
            let derived_object_type = crate::storage::DerivedObjectType::parse(
                &object.derived_object_type,
            )
            .ok_or_else(|| {
                fail_job_message(
                    job_store,
                    worker_id,
                    &claimed_job.job_id,
                    format!(
                        "Invalid derived_object_type in embedding payload: {}",
                        object.derived_object_type
                    ),
                )
            })?;
            Ok(NewDerivedObjectEmbedding {
                derived_object_id: object.derived_object_id.clone(),
                artifact_id: payload.artifact_id.clone(),
                derived_object_type,
                provider_name: embedding_provider.provider_name().to_string(),
                model_name: embedding_provider.model_name().to_string(),
                content_text_hash: sha256_hex(&object.text_for_embedding()),
                embedding,
            })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;

    embedding_store
        .upsert_embeddings(&embeddings)
        .map_err(|err| {
            fail_job(
                job_store,
                worker_id,
                &claimed_job.job_id,
                "Failed to persist derived object embeddings",
                err,
            )
        })?;

    complete_job(job_store, worker_id, &claimed_job.job_id)?;
    Ok(())
}

pub(super) fn build_embedding_job(
    claimed_job: &ClaimedJob,
    artifact_id: &str,
    objects: &[WriteDerivedObject],
) -> Option<NewEnrichmentJob> {
    let items = objects
        .iter()
        .filter_map(|object_write| {
            let object = &object_write.object;
            if object.object_status != ObjectStatus::Active {
                return None;
            }
            let derived_object_type = object.payload.derived_object_type();
            if !derived_object_type.supports_embeddings() {
                return None;
            }
            let body_text = object
                .payload
                .body_text()
                .map(str::trim)
                .filter(|value| !value.is_empty())?
                .to_string();
            Some(DerivedObjectEmbeddingItem {
                derived_object_id: object.derived_object_id.clone(),
                derived_object_type: derived_object_type.as_str().to_string(),
                title: object.payload.title().map(ToOwned::to_owned),
                body_text,
            })
        })
        .collect::<Vec<_>>();

    if items.is_empty() {
        return None;
    }

    Some(NewEnrichmentJob {
        job_id: new_id("job"),
        artifact_id: artifact_id.to_string(),
        job_type: JobType::DerivedObjectEmbed,
        enrichment_tier: EnrichmentTier::Default,
        spawned_by_job_id: Some(claimed_job.job_id.clone()),
        job_status: JobStatus::Pending,
        max_attempts: 3,
        priority_no: 120,
        required_capabilities: vec!["text".to_string()],
        payload_json: DerivedObjectEmbeddingPayload::new_v1(artifact_id, items).to_json(),
    })
}

pub(super) fn try_enqueue_embedding_job(
    job_store: &dyn EnrichmentJobLifecycleStore,
    parent_job_id: &str,
    embedding_job: &NewEnrichmentJob,
) {
    if let Err(err) = job_store.enqueue_jobs(std::slice::from_ref(embedding_job)) {
        warn!(
            "Failed to enqueue embedding job {} spawned by {}: {}",
            embedding_job.job_id, parent_job_id, err
        );
    }
}

pub(super) fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(super) fn embedding_error_is_retryable(err: &crate::error::EmbeddingError) -> bool {
    match err {
        crate::error::EmbeddingError::SendRequest { .. }
        | crate::error::EmbeddingError::ReadResponse { .. } => true,
        crate::error::EmbeddingError::HttpStatus { status, .. } => *status >= 500,
        _ => false,
    }
}

pub(super) fn embedding_worker_loop(
    worker_id: String,
    job_store: &dyn EnrichmentJobLifecycleStore,
    embedding_store: &dyn DerivedObjectEmbeddingStore,
    embedding_provider: &dyn EmbeddingProvider,
    poll_interval: Duration,
    shutdown: crate::shutdown::ShutdownToken,
) {
    info!("Embedding worker {} starting", worker_id);

    loop {
        if shutdown.is_shutdown() {
            info!("Embedding worker {} shutting down", worker_id);
            break;
        }

        match job_store.claim_jobs_by_type(
            &worker_id,
            JobType::DerivedObjectEmbed,
            Some(EnrichmentTier::Default),
            1,
        ) {
            Ok(jobs) if !jobs.is_empty() => {
                let job = jobs.into_iter().next().expect("one claimed job");
                if let Err(err) = process_embedding_job(
                    &worker_id,
                    &job,
                    job_store,
                    Some(embedding_store),
                    Some(embedding_provider),
                ) {
                    error!(
                        "Embedding worker {} failed to process job: {}",
                        worker_id, err
                    );
                }
            }
            Ok(_) => thread::sleep(poll_interval),
            Err(err) => {
                error!(
                    "Embedding worker {} failed to claim job: {}",
                    worker_id, err
                );
                thread::sleep(poll_interval);
            }
        }
    }
}
