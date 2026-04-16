use crate::config::OracleConfig;
use crate::db;
use crate::error::StorageError;
use crate::storage::{
    ArchiveStatusSnapshot, ArtifactEnrichmentCount, ArtifactSourceCount, EnrichmentJobCount,
    EnrichmentStatus, JobStatus, OperatorStore, SourceType,
};

pub struct OracleOperatorStore {
    config: OracleConfig,
}

impl OracleOperatorStore {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}

impl OperatorStore for OracleOperatorStore {
    fn load_archive_status(&self) -> crate::error::StorageResult<ArchiveStatusSnapshot> {
        let conn = db::connect(&self.config)?;
        let (artifact_count,) = conn
            .query_row_as::<(i64,)>("SELECT COUNT(*) FROM oa_artifact", &[])
            .map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;

        let source_rows = conn
            .query(
                "SELECT source_type, COUNT(*) \
                 FROM oa_artifact \
                 GROUP BY source_type \
                 ORDER BY source_type",
                &[],
            )
            .map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
        let mut artifacts_by_source = Vec::new();
        for row_result in source_rows {
            let row = row_result.map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            let source_type: String = row.get(0).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            let count: i64 = row.get(1).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            artifacts_by_source.push(ArtifactSourceCount {
                source_type: SourceType::parse(&source_type).ok_or_else(|| {
                    StorageError::InvalidStatusRead {
                        detail: format!("invalid source_type in status snapshot: {source_type}"),
                    }
                })?,
                count: usize::try_from(count).map_err(|_| StorageError::InvalidStatusRead {
                    detail: format!("invalid artifact count in status snapshot: {count}"),
                })?,
            });
        }

        let enrichment_rows = conn
            .query(
                "SELECT enrichment_status, COUNT(*) \
                 FROM oa_artifact \
                 GROUP BY enrichment_status \
                 ORDER BY enrichment_status",
                &[],
            )
            .map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
        let mut artifacts_by_enrichment_status = Vec::new();
        for row_result in enrichment_rows {
            let row = row_result.map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            let enrichment_status: String =
                row.get(0).map_err(|source| StorageError::ListArtifacts {
                    source: Box::new(source),
                })?;
            let count: i64 = row.get(1).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            artifacts_by_enrichment_status.push(ArtifactEnrichmentCount {
                enrichment_status: EnrichmentStatus::parse(&enrichment_status).ok_or_else(
                    || StorageError::InvalidStatusRead {
                        detail: format!(
                            "invalid enrichment_status in status snapshot: {enrichment_status}"
                        ),
                    },
                )?,
                count: usize::try_from(count).map_err(|_| StorageError::InvalidStatusRead {
                    detail: format!("invalid enrichment count in status snapshot: {count}"),
                })?,
            });
        }

        let job_rows = conn
            .query(
                "SELECT job_status, COUNT(*) \
                 FROM oa_enrichment_job \
                 GROUP BY job_status \
                 ORDER BY job_status",
                &[],
            )
            .map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
        let mut jobs_by_status = Vec::new();
        for row_result in job_rows {
            let row = row_result.map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            let job_status: String = row.get(0).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            let count: i64 = row.get(1).map_err(|source| StorageError::ListArtifacts {
                source: Box::new(source),
            })?;
            jobs_by_status.push(EnrichmentJobCount {
                job_status: JobStatus::parse(&job_status).ok_or_else(|| {
                    StorageError::InvalidStatusRead {
                        detail: format!("invalid job_status in status snapshot: {job_status}"),
                    }
                })?,
                count: usize::try_from(count).map_err(|_| StorageError::InvalidStatusRead {
                    detail: format!("invalid job count in status snapshot: {count}"),
                })?,
            });
        }

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
    }
}
