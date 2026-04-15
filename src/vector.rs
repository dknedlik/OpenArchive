use std::sync::Mutex;

use reqwest::blocking::{Client, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::config::QdrantConfig;
use crate::embedding::EmbeddingProvider;
use crate::error::{ConfigError, ConfigResult, StorageError, StorageResult};
use crate::storage::{
    DerivedObjectEmbeddingStore, DerivedObjectType, NewDerivedObjectEmbedding, ObjectSearchFilters,
    VectorSearchHit, VectorSearchStore,
};

#[derive(Debug)]
pub struct QdrantVectorStore {
    client: Client,
    config: QdrantConfig,
    ensured_dimensions: Mutex<Option<usize>>,
}

impl QdrantVectorStore {
    pub fn new(
        config: QdrantConfig,
        embedding_provider: Option<&dyn EmbeddingProvider>,
    ) -> ConfigResult<Self> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|source| ConfigError::InvalidVectorStoreConfig {
                message: format!("failed to build Qdrant HTTP client: {source}"),
            })?;
        let store = Self {
            client,
            config,
            ensured_dimensions: Mutex::new(None),
        };
        if let Some(provider) = embedding_provider {
            store.ensure_collection_configured(provider.dimensions())?;
        }
        Ok(store)
    }

    fn ensure_collection_configured(&self, dimensions: usize) -> ConfigResult<()> {
        let mut ensured = self
            .ensured_dimensions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ensured.as_ref() == Some(&dimensions) {
            return Ok(());
        }

        let operation = "inspect collection";
        let response = self
            .client
            .get(self.collection_url())
            .send()
            .map_err(|source| ConfigError::InvalidVectorStoreConfig {
                message: format!("failed to inspect Qdrant collection: {source}"),
            })?;

        match response.status().as_u16() {
            200 => {
                let body =
                    response
                        .text()
                        .map_err(|source| ConfigError::InvalidVectorStoreConfig {
                            message: format!("failed to read Qdrant collection response: {source}"),
                        })?;
                let value: Value = serde_json::from_str(&body).map_err(|source| {
                    ConfigError::InvalidVectorStoreConfig {
                        message: format!(
                            "failed to parse Qdrant collection response for {operation}: {source}"
                        ),
                    }
                })?;
                let Some(actual_dimensions) = collection_dimensions(&value) else {
                    return Err(ConfigError::InvalidVectorStoreConfig {
                        message: "Qdrant collection response did not include vector size"
                            .to_string(),
                    });
                };
                if actual_dimensions != dimensions {
                    return Err(ConfigError::InvalidVectorStoreConfig {
                        message: format!(
                            "Qdrant collection {} is sized for {} dimensions but embeddings are configured for {}",
                            self.config.collection_name, actual_dimensions, dimensions
                        ),
                    });
                }
            }
            404 => {
                self.client
                    .put(format!("{}?wait=true", self.collection_url()))
                    .json(&json!({
                        "vectors": {
                            "size": dimensions,
                            "distance": "Cosine"
                        }
                    }))
                    .send()
                    .map_err(|source| ConfigError::InvalidVectorStoreConfig {
                        message: format!("failed to create Qdrant collection: {source}"),
                    })?
                    .error_for_status()
                    .map_err(|source| ConfigError::InvalidVectorStoreConfig {
                        message: format!("Qdrant collection creation failed: {source}"),
                    })?;
            }
            status => {
                let body = response.text().unwrap_or_default();
                return Err(ConfigError::InvalidVectorStoreConfig {
                    message: format!(
                        "Qdrant collection inspection returned HTTP {status}: {}",
                        preview(&body)
                    ),
                });
            }
        }

        *ensured = Some(dimensions);
        Ok(())
    }

    fn ensure_collection_for_write(&self, dimensions: usize) -> StorageResult<()> {
        self.ensure_collection_configured(dimensions)
            .map_err(|err| StorageError::VectorStoreParseResponse {
                operation: "ensure collection",
                detail: err.to_string(),
            })
    }

    fn collection_url(&self) -> String {
        format!(
            "{}/collections/{}",
            self.config.url.trim_end_matches('/'),
            self.config.collection_name
        )
    }

    fn search_hits(
        &self,
        query_embedding: &[f32],
        limit: usize,
        filter: Option<Value>,
    ) -> StorageResult<Vec<VectorSearchHit>> {
        let mut body = json!({
            "vector": query_embedding,
            "limit": limit.max(1),
            "with_payload": true,
        });
        if let Some(filter) = filter {
            body["filter"] = filter;
        }
        if self.config.exact {
            body["params"] = json!({ "exact": true });
        }

        let response = self
            .client
            .post(format!("{}/points/search", self.collection_url()))
            .json(&body)
            .send()
            .map_err(|source| StorageError::VectorStoreRequest {
                operation: "search",
                source: Box::new(source),
            })?;
        let parsed = parse_json_response::<SearchResponse>(response, "search")?;
        Ok(parsed
            .result
            .into_iter()
            .filter_map(|hit| {
                hit.payload.and_then(|payload| {
                    payload
                        .derived_object_id
                        .map(|derived_object_id| VectorSearchHit {
                            derived_object_id,
                            score: hit.score,
                        })
                })
            })
            .collect())
    }
}

impl DerivedObjectEmbeddingStore for QdrantVectorStore {
    fn upsert_embeddings(&self, embeddings: &[NewDerivedObjectEmbedding]) -> StorageResult<()> {
        if embeddings.is_empty() {
            return Ok(());
        }

        let dimensions = embeddings[0].embedding.len();
        if embeddings
            .iter()
            .any(|embedding| embedding.embedding.len() != dimensions)
        {
            return Err(StorageError::InvalidDerivationWrite {
                detail: "embedding batch contains inconsistent vector dimensions".to_string(),
            });
        }
        self.ensure_collection_for_write(dimensions)?;

        let points = embeddings
            .iter()
            .map(|embedding| {
                json!({
                    "id": qdrant_point_id(&embedding.derived_object_id),
                    "vector": embedding.embedding,
                    "payload": {
                        "derived_object_id": embedding.derived_object_id,
                        "artifact_id": embedding.artifact_id,
                        "derived_object_type": embedding.derived_object_type.as_str(),
                        "embedding_provider": embedding.provider_name,
                        "embedding_model": embedding.model_name,
                        "content_text_hash": embedding.content_text_hash,
                    }
                })
            })
            .collect::<Vec<_>>();

        let response = self
            .client
            .put(format!("{}/points?wait=true", self.collection_url()))
            .json(&json!({ "points": points }))
            .send()
            .map_err(|source| StorageError::VectorStoreRequest {
                operation: "upsert points",
                source: Box::new(source),
            })?;
        parse_empty_response(response, "upsert points")
    }
}

impl VectorSearchStore for QdrantVectorStore {
    fn search_by_embedding(
        &self,
        filters: &ObjectSearchFilters,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<VectorSearchHit>> {
        self.search_hits(query_embedding, limit, search_filter(filters))
    }

    fn find_related_by_embedding(
        &self,
        artifact_id: &str,
        derived_object_type: DerivedObjectType,
        query_embedding: &[f32],
        limit: usize,
    ) -> StorageResult<Vec<VectorSearchHit>> {
        let filter = json!({
            "must": [
                match_filter("derived_object_type", derived_object_type.as_str())
            ],
            "must_not": [
                match_filter("artifact_id", artifact_id)
            ]
        });
        self.search_hits(query_embedding, limit, Some(filter))
    }
}

fn search_filter(filters: &ObjectSearchFilters) -> Option<Value> {
    let mut must = Vec::new();
    if let Some(object_type) = filters.object_type {
        must.push(match_filter("derived_object_type", object_type.as_str()));
    }
    if let Some(artifact_id) = filters.artifact_id.as_deref() {
        must.push(match_filter("artifact_id", artifact_id));
    }
    if must.is_empty() {
        None
    } else {
        Some(json!({ "must": must }))
    }
}

fn match_filter(key: &str, value: &str) -> Value {
    json!({
        "key": key,
        "match": {
            "value": value
        }
    })
}

fn qdrant_point_id(derived_object_id: &str) -> String {
    let digest = Sha256::digest(derived_object_id.as_bytes());
    let hex = format!("{digest:x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn collection_dimensions(value: &Value) -> Option<usize> {
    value
        .get("result")?
        .get("config")?
        .get("params")?
        .get("vectors")?
        .get("size")?
        .as_u64()
        .and_then(|size| usize::try_from(size).ok())
}

fn parse_empty_response(response: Response, operation: &'static str) -> StorageResult<()> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(unexpected_status(operation, response)?)
    }
}

fn parse_json_response<T>(response: Response, operation: &'static str) -> StorageResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    if !response.status().is_success() {
        return Err(unexpected_status(operation, response)?);
    }
    let body = response
        .text()
        .map_err(|source| StorageError::VectorStoreRequest {
            operation,
            source: Box::new(source),
        })?;
    serde_json::from_str(&body).map_err(|source| StorageError::VectorStoreParseResponse {
        operation,
        detail: format!("{source}: {}", preview(&body)),
    })
}

fn unexpected_status(operation: &'static str, response: Response) -> StorageResult<StorageError> {
    let status = response.status().as_u16();
    let body = response
        .text()
        .map_err(|source| StorageError::VectorStoreRequest {
            operation,
            source: Box::new(source),
        })?;
    Ok(StorageError::VectorStoreUnexpectedStatus {
        operation,
        status,
        body_preview: preview(&body),
    })
}

fn preview(body: &str) -> String {
    const MAX_PREVIEW: usize = 240;
    let trimmed = body.trim();
    if trimmed.len() <= MAX_PREVIEW {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..MAX_PREVIEW])
    }
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    result: Vec<SearchPoint>,
}

#[derive(Debug, Deserialize)]
struct SearchPoint {
    score: f32,
    payload: Option<SearchPayload>,
}

#[derive(Debug, Deserialize)]
struct SearchPayload {
    derived_object_id: Option<String>,
}
