pub mod artifact;
pub mod derivation;
pub mod import;
pub mod job;
pub mod segment;

use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::types::{ClaimedJob, RetryOutcome};
use crate::storage::{
    ArtifactIngestResult, ArtifactReadStore, ImportWriteResult, ImportWriteStore, ImportedArtifact,
    StorageTx, WriteImportSet,
};

pub struct PostgresStorageTx {
    client: postgres::Client,
}

impl StorageTx for PostgresStorageTx {
    fn commit(&mut self) -> StorageResult<()> {
        self.client
            .batch_execute("COMMIT")
            .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: "transaction".to_string(),
                source,
            }))
    }
}

pub struct PostgresImportWriteStore {
    config: PostgresConfig,
}

impl PostgresImportWriteStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl ArtifactReadStore for PostgresImportWriteStore {
    fn list_artifacts(&self) -> StorageResult<Vec<crate::storage::types::ArtifactListItem>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::list_artifacts(&mut client)
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<crate::storage::types::LoadedArtifactForEnrichment>> {
        let mut client = postgres_db::connect(&self.config)?;
        artifact::load_artifact_for_enrichment(&mut client, artifact_id)
    }
}

impl ImportWriteStore for PostgresImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let mut client = postgres_db::connect(&self.config)?;
        client
            .batch_execute("BEGIN")
            .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            }))?;
        let mut tx = PostgresStorageTx { client };

        if let Some(existing_object_id) =
            import::find_payload_object_id_by_sha256(&mut tx.client, &import_set.payload_object.sha256)?
        {
            import_set.import.payload_object_id = existing_object_id;
        } else {
            import::insert_payload_object(&mut tx.client, &import_set.payload_object)?;
        }
        import::insert_import(&mut tx.client, &import_set.import)?;
        tx.commit()?;

        let import_id = import_set.import.import_id.clone();
        let mut artifacts: Vec<ImportedArtifact> =
            Vec::with_capacity(import_set.artifact_sets.len());
        let mut failed_artifact_ids: Vec<String> =
            Vec::with_capacity(import_set.artifact_sets.len());
        let mut segments_written: usize = 0;
        let mut count_imported: i32 = 0;
        let mut count_failed: i32 = 0;
        let mut artifact_errors: Vec<String> = Vec::new();

        for artifact_set in import_set.artifact_sets {
            let artifact_id = artifact_set.artifact.artifact_id.clone();

            if let Some((existing_artifact_id, enrichment_status)) = artifact::find_artifact_by_source_hash(
                &mut tx.client,
                artifact_set.artifact.source_type,
                &artifact_set.artifact.content_hash_version,
                &artifact_set.artifact.source_conversation_hash,
            )? {
                artifacts.push(ImportedArtifact {
                    artifact_id: existing_artifact_id,
                    enrichment_status,
                    ingest_result: ArtifactIngestResult::AlreadyExists,
                });
                continue;
            }

            tx.client
                .batch_execute("BEGIN")
                .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                    connection_string: self.config.connection_string.clone(),
                    source,
                }))?;

            let phase2_result: StorageResult<usize> = (|| {
                artifact::insert_artifact(&mut tx.client, &artifact_set.artifact)?;

                for participant in &artifact_set.participants {
                    artifact::insert_participant(&mut tx.client, participant)?;
                }

                let mut seg_count = 0usize;
                for seg in &artifact_set.segments {
                    segment::insert_segment(&mut tx.client, seg)?;
                    seg_count += 1;
                }

                job::insert_job(&mut tx.client, &artifact_set.job)?;
                Ok(seg_count)
            })();

            match phase2_result {
                Ok(seg_count) => {
                    tx.commit()?;
                    artifacts.push(ImportedArtifact {
                        artifact_id,
                        enrichment_status: artifact_set.artifact.enrichment_status,
                        ingest_result: ArtifactIngestResult::Created,
                    });
                    segments_written += seg_count;
                    count_imported += 1;
                }
                Err(e) => {
                    tx.client
                        .batch_execute("ROLLBACK")
                        .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                            connection_string: self.config.connection_string.clone(),
                            source,
                        }))?;
                    failed_artifact_ids.push(artifact_id.clone());
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {e:#}", artifact_id));
                }
            }
        }

        let import_status = if count_failed == 0 {
            crate::storage::types::ImportStatus::Completed
        } else if count_imported == 0 {
            crate::storage::types::ImportStatus::Failed
        } else {
            crate::storage::types::ImportStatus::CompletedWithErrors
        };
        let error_message = if artifact_errors.is_empty() {
            None
        } else {
            Some(artifact_errors.join(" | "))
        };

        tx.client
            .batch_execute("BEGIN")
            .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            }))?;
        import::finalize_import(
            &mut tx.client,
            &import_id,
            count_imported,
            count_failed,
            import_status,
            error_message.as_deref(),
        )?;
        tx.commit()?;

        Ok(ImportWriteResult {
            import_id,
            import_status,
            artifacts,
            failed_artifact_ids,
            segments_written,
        })
    }
}

pub struct PostgresEnrichmentJobStore {
    config: PostgresConfig,
}

pub struct PostgresDerivedMetadataStore {
    config: PostgresConfig,
}

impl PostgresDerivedMetadataStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl PostgresEnrichmentJobStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

impl EnrichmentJobLifecycleStore for PostgresEnrichmentJobStore {
    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let mut client = postgres_db::connect(&self.config)?;
        job::claim_next_job(&mut client, worker_id)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::complete_job(&mut client, worker_id, job_id)
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let mut client = postgres_db::connect(&self.config)?;
        job::fail_job(&mut client, worker_id, job_id, error_message)
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let mut client = postgres_db::connect(&self.config)?;
        job::mark_job_retryable(&mut client, worker_id, job_id, error_message, retry_after_seconds)
    }
}

impl DerivedMetadataWriteStore for PostgresDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        let mut client = postgres_db::connect(&self.config)?;
        client
            .batch_execute("BEGIN")
            .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: self.config.connection_string.clone(),
                source,
            }))?;

        let result = (|| {
            derivation::validate_derivation_attempt(
                &mut client,
                &self.config.connection_string,
                &attempt,
            )?;
            derivation::insert_derivation_run(
                &mut client,
                &self.config.connection_string,
                &attempt.run,
            )?;

            let mut derived_object_ids = Vec::with_capacity(attempt.objects.len());
            let mut evidence_links_written = 0usize;
            for object_write in &attempt.objects {
                derivation::insert_derived_object(
                    &mut client,
                    &self.config.connection_string,
                    &object_write.object,
                )?;
                derived_object_ids.push(object_write.object.derived_object_id.clone());

                for link in &object_write.evidence_links {
                    derivation::insert_evidence_link(
                        &mut client,
                        &self.config.connection_string,
                        link,
                    )?;
                    evidence_links_written += 1;
                }
            }

            Ok(DerivationWriteResult {
                derivation_run_id: attempt.run.derivation_run_id.clone(),
                derived_object_ids,
                evidence_links_written,
            })
        })();

        match result {
            Ok(result) => {
                client
                    .batch_execute("COMMIT")
                    .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                        connection_string: self.config.connection_string.clone(),
                        source,
                    }))?;
                Ok(result)
            }
            Err(error) => {
                client
                    .batch_execute("ROLLBACK")
                    .map_err(|source| crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                        connection_string: self.config.connection_string.clone(),
                        source,
                    }))?;
                Err(error)
            }
        }
    }
}
