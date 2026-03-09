use crate::error::{StorageError, StorageResult};
use oracle::Connection;

use crate::storage::types::{ImportStatus, NewImport, NewImportPayload};

pub fn insert_payload(conn: &Connection, p: &NewImportPayload) -> StorageResult<()> {
    let format = p.payload_format.as_str();
    conn.execute(
        "INSERT INTO oa_import_payload \
         (payload_id, payload_format, payload_mime_type, payload_bytes, payload_size_bytes, payload_sha256) \
         VALUES (:1, :2, :3, :4, :5, :6)",
        &[
            &p.payload_id,
            &format,
            &p.payload_mime_type,
            &p.payload_bytes,
            &p.payload_size_bytes,
            &p.payload_sha256,
        ],
    )
    .map_err(|source| StorageError::InsertPayload {
        payload_id: p.payload_id.clone(),
        source,
    })?;
    Ok(())
}

pub fn insert_import(conn: &Connection, i: &NewImport) -> StorageResult<()> {
    let source_type = i.source_type.as_str();
    let status = i.import_status.as_str();
    conn.execute(
        "INSERT INTO oa_import \
         (import_id, source_type, import_status, payload_id, source_filename, source_content_hash, conversation_count_detected) \
         VALUES (:1, :2, :3, :4, :5, :6, :7)",
        &[
            &i.import_id,
            &source_type,
            &status,
            &i.payload_id,
            &i.source_filename,
            &i.source_content_hash,
            &i.conversation_count_detected,
        ],
    )
    .map_err(|source| StorageError::InsertImport {
        import_id: i.import_id.clone(),
        source,
    })?;
    Ok(())
}

pub fn update_import_counts(
    conn: &Connection,
    import_id: &str,
    imported: i64,
    failed: i64,
) -> StorageResult<()> {
    conn.execute(
        "UPDATE oa_import SET conversation_count_imported = :1, conversation_count_failed = :2 \
         WHERE import_id = :3",
        &[&imported, &failed, &import_id],
    )
    .map_err(|source| StorageError::UpdateImportCounts {
        import_id: import_id.to_string(),
        source,
    })?;
    Ok(())
}

pub fn finalize_import(
    conn: &Connection,
    import_id: &str,
    imported: i64,
    failed: i64,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    let status_str = status.as_str();
    conn.execute(
        "UPDATE oa_import \
         SET conversation_count_imported = :1, \
             conversation_count_failed = :2, \
             import_status = :3, \
             completed_at = SYSTIMESTAMP, \
             error_message = :4 \
         WHERE import_id = :5",
        &[&imported, &failed, &status_str, &error_message, &import_id],
    )
    .map_err(|source| StorageError::FinalizeImport {
        import_id: import_id.to_string(),
        source,
    })?;
    Ok(())
}

pub fn complete_import(
    conn: &Connection,
    import_id: &str,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    let status_str = status.as_str();
    conn.execute(
        "UPDATE oa_import SET import_status = :1, completed_at = SYSTIMESTAMP, error_message = :2 \
         WHERE import_id = :3",
        &[&status_str, &error_message, &import_id],
    )
    .map_err(|source| StorageError::CompleteImport {
        import_id: import_id.to_string(),
        source,
    })?;
    Ok(())
}
