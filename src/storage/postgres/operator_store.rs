use crate::config::PostgresConfig;
use crate::error::StorageError;
use crate::postgres_db::SharedPostgresClient;
use crate::storage::{
    ArchiveStatusSnapshot, ArtifactEnrichmentCount, ArtifactSourceCount, EnrichmentJobCount,
    EnrichmentStatus, JobStatus, OperatorStore, SourceType,
};

pub struct PostgresOperatorStore {
    client: SharedPostgresClient,
}

impl PostgresOperatorStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

impl OperatorStore for PostgresOperatorStore {
    fn load_archive_status(&self) -> crate::error::StorageResult<ArchiveStatusSnapshot> {
        self.client.with_client(|client| {
            let artifact_count: i64 = client
                .query_one("SELECT COUNT(*) FROM oa_artifact", &[])
                .map_err(|source| StorageError::InvalidStatusRead {
                    detail: format!("failed to query total artifact count: {source}"),
                })?
                .get(0);

            let artifacts_by_source = client
                .query(
                    "SELECT source_type, COUNT(*) \
                     FROM oa_artifact \
                     GROUP BY source_type \
                     ORDER BY source_type",
                    &[],
                )
                .map_err(|source| StorageError::InvalidStatusRead {
                    detail: format!("failed to query artifact source counts: {source}"),
                })?
                .into_iter()
                .map(|row| {
                    let source_type: String = row.get(0);
                    let count: i64 = row.get(1);
                    Ok(ArtifactSourceCount {
                        source_type: SourceType::parse(&source_type).ok_or_else(|| {
                            StorageError::InvalidStatusRead {
                                detail: format!(
                                    "invalid source_type in status snapshot: {source_type}"
                                ),
                            }
                        })?,
                        count: usize::try_from(count).map_err(|_| {
                            StorageError::InvalidStatusRead {
                                detail: format!(
                                    "invalid artifact source count in status snapshot: {count}"
                                ),
                            }
                        })?,
                    })
                })
                .collect::<crate::error::StorageResult<Vec<_>>>()?;

            let artifacts_by_enrichment_status = client
                .query(
                    "SELECT enrichment_status, COUNT(*) \
                     FROM oa_artifact \
                     GROUP BY enrichment_status \
                     ORDER BY enrichment_status",
                    &[],
                )
                .map_err(|source| StorageError::InvalidStatusRead {
                    detail: format!("failed to query artifact enrichment counts: {source}"),
                })?
                .into_iter()
                .map(|row| {
                    let enrichment_status: String = row.get(0);
                    let count: i64 = row.get(1);
                    Ok(ArtifactEnrichmentCount {
                        enrichment_status: EnrichmentStatus::parse(&enrichment_status).ok_or_else(
                            || StorageError::InvalidStatusRead {
                                detail: format!(
                                    "invalid enrichment_status in status snapshot: {enrichment_status}"
                                ),
                            },
                        )?,
                        count: usize::try_from(count).map_err(|_| {
                            StorageError::InvalidStatusRead {
                                detail: format!(
                                    "invalid enrichment count in status snapshot: {count}"
                                ),
                            }
                        })?,
                    })
                })
                .collect::<crate::error::StorageResult<Vec<_>>>()?;

            let jobs_by_status = client
                .query(
                    "SELECT job_status, COUNT(*) \
                     FROM oa_enrichment_job \
                     GROUP BY job_status \
                     ORDER BY job_status",
                    &[],
                )
                .map_err(|source| StorageError::InvalidStatusRead {
                    detail: format!("failed to query job status counts: {source}"),
                })?
                .into_iter()
                .map(|row| {
                    let job_status: String = row.get(0);
                    let count: i64 = row.get(1);
                    Ok(EnrichmentJobCount {
                        job_status: JobStatus::parse(&job_status).ok_or_else(|| {
                            StorageError::InvalidStatusRead {
                                detail: format!(
                                    "invalid job_status in status snapshot: {job_status}"
                                ),
                            }
                        })?,
                        count: usize::try_from(count).map_err(|_| {
                            StorageError::InvalidStatusRead {
                                detail: format!(
                                    "invalid job count in status snapshot: {count}"
                                ),
                            }
                        })?,
                    })
                })
                .collect::<crate::error::StorageResult<Vec<_>>>()?;

            Ok(ArchiveStatusSnapshot {
                artifact_count: usize::try_from(artifact_count).map_err(|_| {
                    StorageError::InvalidStatusRead {
                        detail: format!(
                            "invalid total artifact count in status snapshot: {artifact_count}"
                        ),
                    }
                })?,
                artifacts_by_source,
                artifacts_by_enrichment_status,
                jobs_by_status,
            })
        })
    }
}
