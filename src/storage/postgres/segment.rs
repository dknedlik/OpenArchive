use crate::error::StorageResult;
use crate::storage::types::NewSegment;

pub fn insert_segment(client: &mut postgres::Client, s: &NewSegment) -> StorageResult<()> {
    let segment_type = s.segment_type.as_str();
    let visibility_status = s.visibility_status.as_str();
    client
        .execute(
            "INSERT INTO oa_segment \
             (segment_id, artifact_id, participant_id, segment_type, source_segment_key, \
              parent_segment_id, sequence_no, created_at_source, text_content, text_content_hash, \
              locator_json, visibility_status, unsupported_content_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::timestamptz, $9, $10, $11::text::jsonb, $12, $13::text::jsonb)",
            &[
                &s.segment_id,
                &s.artifact_id,
                &s.participant_id,
                &segment_type,
                &s.source_segment_key,
                &s.parent_segment_id,
                &s.sequence_no,
                &s.created_at_source.as_ref().map(|ts| ts.as_str()),
                &s.text_content,
                &s.text_content_hash,
                &s.locator_json,
                &visibility_status,
                &s.unsupported_content_json,
            ],
        )
        .map_err(|source| {
            crate::error::StorageError::Db(crate::error::DbError::ConnectPostgres {
                connection_string: "postgres".to_string(),
                source: Box::new(source),
            })
        })?;
    Ok(())
}
