use anyhow::{anyhow, Result};
use oracle::Connection;

use crate::storage::types::NewEnrichmentJob;

pub fn insert_job(conn: &Connection, j: &NewEnrichmentJob) -> Result<()> {
    let job_type = j.job_type.as_str();
    let job_status = j.job_status.as_str();
    conn.execute(
        "INSERT INTO oa_enrichment_job \
         (job_id, artifact_id, job_type, job_status, max_attempts, priority_no, payload_json) \
         VALUES (:1, :2, :3, :4, :5, :6, :7)",
        &[
            &j.job_id,
            &j.artifact_id,
            &job_type,
            &job_status,
            &j.max_attempts,
            &j.priority_no,
            &j.payload_json,
        ],
    )
    .map_err(|e| anyhow!(e))?;
    Ok(())
}
