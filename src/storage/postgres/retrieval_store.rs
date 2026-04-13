use crate::config::PostgresConfig;
use crate::error::StorageResult;
use crate::postgres_db::SharedPostgresClient;
use crate::storage::archive_retrieval_store::ArchiveRetrievalStore;
use crate::storage::retrieval_read_store::{
    ArchiveSearchCandidate, ArchiveSearchReadStore, ArtifactContextPackMaterial,
    ArtifactContextPackReadStore, ArtifactDetailReadStore, ArtifactDetailView,
    CrossArtifactReadStore, DerivedObjectSearchResult, DerivedObjectSearchStore, GraphRelatedEntry,
    ObjectSearchFilters, RelatedDerivedObject, RelatedDerivedObjectEmbeddingMatch, SearchFilters,
};
use crate::storage::review_read_store::{
    NewReviewDecision, ReviewCandidate, ReviewQueueFilters, ReviewReadStore, ReviewWriteStore,
};
use crate::storage::types::{DerivedObjectType, RetrievalIntent, RetrievedContextItem};

use super::{retrieval, review};

pub struct PostgresArchiveRetrievalStore {
    client: SharedPostgresClient,
}

impl PostgresArchiveRetrievalStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

pub struct PostgresRetrievalReadStore {
    client: SharedPostgresClient,
}

impl PostgresRetrievalReadStore {
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            client: SharedPostgresClient::new(config),
        }
    }
}

fn retrieve_for_intents_with_client(
    client: &SharedPostgresClient,
    artifact_id: &str,
    intents: &[RetrievalIntent],
    limit_per_intent: usize,
) -> StorageResult<Vec<RetrievedContextItem>> {
    client.with_client(|pg| {
        retrieval::retrieve_for_intents(
            pg,
            client.connection_string(),
            artifact_id,
            intents,
            limit_per_intent,
        )
    })
}

impl ArchiveRetrievalStore for PostgresArchiveRetrievalStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        retrieve_for_intents_with_client(&self.client, artifact_id, intents, limit_per_intent)
    }
}

impl ArchiveRetrievalStore for PostgresRetrievalReadStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        retrieve_for_intents_with_client(&self.client, artifact_id, intents, limit_per_intent)
    }
}

impl ArchiveSearchReadStore for PostgresRetrievalReadStore {
    fn search_candidates(
        &self,
        query_text: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> StorageResult<Vec<ArchiveSearchCandidate>> {
        self.client.with_client(|client| {
            retrieval::search_candidates(
                client,
                self.client.connection_string(),
                query_text,
                limit,
                filters,
            )
        })
    }
}

impl ArtifactDetailReadStore for PostgresRetrievalReadStore {
    fn load_artifact_detail(&self, artifact_id: &str) -> StorageResult<Option<ArtifactDetailView>> {
        self.client.with_client(|client| {
            retrieval::load_artifact_detail(client, self.client.connection_string(), artifact_id)
        })
    }
}

impl ArtifactContextPackReadStore for PostgresRetrievalReadStore {
    fn load_artifact_context_pack_material(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<ArtifactContextPackMaterial>> {
        self.client.with_client(|client| {
            retrieval::load_artifact_context_pack_material(
                client,
                self.client.connection_string(),
                artifact_id,
            )
        })
    }
}

impl DerivedObjectSearchStore for PostgresRetrievalReadStore {
    fn search_objects(
        &self,
        filters: &ObjectSearchFilters,
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        self.client.with_client(|client| {
            retrieval::search_objects(client, self.client.connection_string(), filters, limit)
        })
    }

    fn search_objects_by_embedding(
        &self,
        filters: &ObjectSearchFilters,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        self.client.with_client(|client| {
            retrieval::search_objects_by_embedding(
                client,
                self.client.connection_string(),
                filters,
                query_embedding,
                limit,
            )
        })
    }

    fn get_related_objects(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<GraphRelatedEntry>> {
        self.client.with_client(|client| {
            retrieval::get_related_objects(
                client,
                self.client.connection_string(),
                derived_object_id,
                limit,
            )
        })
    }
}

impl CrossArtifactReadStore for PostgresRetrievalReadStore {
    fn find_related_by_candidate_keys(
        &self,
        artifact_id: &str,
        candidate_keys: &[String],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>> {
        self.client.with_client(|client| {
            retrieval::find_related_by_candidate_keys(
                client,
                self.client.connection_string(),
                artifact_id,
                candidate_keys,
                limit,
            )
        })
    }

    fn find_related_by_embedding(
        &self,
        artifact_id: &str,
        derived_object_type: DerivedObjectType,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>> {
        self.client.with_client(|client| {
            retrieval::find_related_by_embedding(
                client,
                self.client.connection_string(),
                artifact_id,
                derived_object_type,
                query_embedding,
                limit,
            )
        })
    }
}

impl ReviewReadStore for PostgresRetrievalReadStore {
    fn list_review_candidates(
        &self,
        filters: &ReviewQueueFilters,
        limit: usize,
    ) -> StorageResult<Vec<ReviewCandidate>> {
        self.client.with_client(|client| {
            review::list_review_candidates(client, self.client.connection_string(), filters, limit)
        })
    }
}

impl ReviewWriteStore for PostgresRetrievalReadStore {
    fn record_review_decision(&self, decision: &NewReviewDecision) -> StorageResult<()> {
        self.client.with_client(|client| {
            review::record_review_decision(client, self.client.connection_string(), decision)
        })
    }

    fn retry_artifact_enrichment(&self, artifact_id: &str) -> StorageResult<String> {
        self.client.with_client(|client| {
            review::retry_artifact_enrichment(client, self.client.connection_string(), artifact_id)
        })
    }
}
