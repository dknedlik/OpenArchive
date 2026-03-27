use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db::SharedPostgresClient;
use crate::storage::embedding_store::DerivedObjectEmbeddingStore;

use super::embedding;

pub struct PostgresDerivedObjectEmbeddingStore {
    client: SharedPostgresClient,
}

impl PostgresDerivedObjectEmbeddingStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

impl DerivedObjectEmbeddingStore for PostgresDerivedObjectEmbeddingStore {
    fn upsert_embeddings(
        &self,
        embeddings: &[crate::storage::types::NewDerivedObjectEmbedding],
    ) -> StorageResult<()> {
        self.client.with_client(|client| {
            embedding::upsert_embeddings(client, self.client.connection_string(), embeddings)
        })
    }
}
