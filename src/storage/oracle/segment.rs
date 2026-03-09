use anyhow::{anyhow, Result};
use oracle::Connection;

use crate::storage::types::NewSegment;

pub fn insert_segment(conn: &Connection, s: &NewSegment) -> Result<()> {
    let segment_type = s.segment_type.as_str();
    let visibility = s.visibility_status.as_str();
    let created_at_source = s.created_at_source.as_ref().map(|ts| ts.as_str());
    conn.execute(
        "INSERT INTO oa_segment \
         (segment_id, artifact_id, participant_id, segment_type, source_segment_key, \
          parent_segment_id, sequence_no, created_at_source, text_content, text_content_hash, \
          locator_json, visibility_status, unsupported_content_json) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, \
                 TO_TIMESTAMP_TZ(:8, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM'), \
                 :9, :10, :11, :12, :13)",
        &[
            &s.segment_id,
            &s.artifact_id,
            &s.participant_id,
            &segment_type,
            &s.source_segment_key,
            &s.parent_segment_id,
            &s.sequence_no,
            &created_at_source,
            &s.text_content,
            &s.text_content_hash,
            &s.locator_json,
            &visibility,
            &s.unsupported_content_json,
        ],
    )
    .map_err(|e| anyhow!(e))?;
    Ok(())
}
