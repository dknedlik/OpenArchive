use crate::error::StorageResult;
use crate::storage::types::NewDerivedObjectEmbedding;

pub trait DerivedObjectEmbeddingStore: Send + Sync {
    fn upsert_embeddings(&self, embeddings: &[NewDerivedObjectEmbedding]) -> StorageResult<()>;
}
