use crate::error::{StorageError, StorageResult};
use crate::storage::types::{ImportStatus, NewImport, NewImportObjectRef};

pub fn find_payload_object_id_by_sha256(
    client: &mut postgres::Client,
    payload_sha256: &str,
) -> StorageResult<Option<String>> {
    let row = client
        .query_opt(
            "SELECT object_id FROM oa_object_ref WHERE sha256 = $1 AND object_kind = 'import_payload'",
            &[&payload_sha256],
        )
        .map_err(map_pg_err)?;

    Ok(row.map(|row| row.get::<_, String>(0)))
}

pub fn insert_payload_object(
    client: &mut postgres::Client,
    p: &NewImportObjectRef,
) -> StorageResult<()> {
    client
        .execute(
            "INSERT INTO oa_object_ref \
             (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &p.object_id,
                &"import_payload",
                &p.stored_object.provider,
                &p.stored_object.storage_key,
                &p.stored_object.mime_type,
                &p.stored_object.size_bytes,
                &p.stored_object.sha256,
            ],
        )
        .map_err(|source| StorageError::InsertPayload {
            payload_id: p.object_id.clone(),
            source: Box::new(source),
        })?;
    Ok(())
}

pub fn insert_import(client: &mut postgres::Client, i: &NewImport) -> StorageResult<()> {
    let source_type = i.source_type.as_str();
    let status = i.import_status.as_str();
    client
        .execute(
            "INSERT INTO oa_import \
             (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, conversation_count_detected) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &i.import_id,
                &source_type,
                &status,
                &i.payload_object_id,
                &i.source_filename,
                &i.source_content_hash,
                &i.conversation_count_detected,
            ],
        )
        .map_err(|source| StorageError::InsertImport {
            import_id: i.import_id.clone(),
            source: Box::new(source),
        })?;
    Ok(())
}

pub fn finalize_import(
    client: &mut postgres::Client,
    import_id: &str,
    imported: i32,
    failed: i32,
    status: ImportStatus,
    error_message: Option<&str>,
) -> StorageResult<()> {
    let status_str = status.as_str();
    client
        .execute(
            "UPDATE oa_import \
             SET conversation_count_imported = $1, \
                 conversation_count_failed = $2, \
                 import_status = $3, \
                 completed_at = NOW(), \
                 error_message = $4 \
            WHERE import_id = $5",
            &[&imported, &failed, &status_str, &error_message, &import_id],
        )
        .map_err(map_pg_err)?;
    Ok(())
}

fn map_pg_err(source: postgres::Error) -> StorageError {
    StorageError::Db(map_postgres_error(source))
}

fn map_postgres_error(source: postgres::Error) -> crate::error::DbError {
    crate::error::DbError::ConnectPostgres {
        connection_string: "postgres".to_string(),
        source,
    }
}
