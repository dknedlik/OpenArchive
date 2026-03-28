use postgres::Client;

use crate::error::{DbError, StorageError, StorageResult};
use crate::storage::types::NewDerivedObjectEmbedding;

fn map_pg_err(connection_string: &str, source: postgres::Error) -> StorageError {
    StorageError::Db(DbError::ConnectPostgres {
        connection_string: connection_string.to_string(),
        source: Box::new(source),
    })
}

pub fn upsert_embeddings(
    client: &mut Client,
    connection_string: &str,
    embeddings: &[NewDerivedObjectEmbedding],
) -> StorageResult<()> {
    for embedding in embeddings {
        let vector_lit = vector_literal(&embedding.embedding);
        let sql = format!(
            "INSERT INTO oa_derived_object_embedding
             (derived_object_id, artifact_id, derived_object_type, embedding_provider,
              embedding_model, content_text_hash, embedding)
             VALUES ($1, $2, $3, $4, $5, $6, '{vector_lit}'::vector)
             ON CONFLICT (derived_object_id) DO UPDATE
             SET artifact_id = EXCLUDED.artifact_id,
                 derived_object_type = EXCLUDED.derived_object_type,
                 embedding_provider = EXCLUDED.embedding_provider,
                 embedding_model = EXCLUDED.embedding_model,
                 content_text_hash = EXCLUDED.content_text_hash,
                 embedding = EXCLUDED.embedding,
                 updated_at = NOW()"
        );
        client
            .execute(
                &sql,
                &[
                    &embedding.derived_object_id,
                    &embedding.artifact_id,
                    &embedding.derived_object_type.as_str(),
                    &embedding.provider_name,
                    &embedding.model_name,
                    &embedding.content_text_hash,
                ],
            )
            .map_err(|source| map_pg_err(connection_string, source))?;
    }
    Ok(())
}

pub(crate) fn vector_literal(values: &[f32]) -> String {
    let serialized = values
        .iter()
        .map(|value| format!("{value:.8}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{serialized}]")
}
