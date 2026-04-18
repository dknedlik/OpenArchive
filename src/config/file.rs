use serde::{Deserialize, Serialize};

// TOML file schema structs
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfigFile {
    pub(super) database: Option<DatabaseSection>,
    pub(super) object_store: Option<ObjectStoreSection>,
    pub(super) vector_store: Option<VectorStoreSection>,
    pub(super) enrichment: Option<EnrichmentSection>,
    pub(super) http: Option<HttpSection>,
    pub(super) embeddings: Option<EmbeddingsSection>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct DatabaseSection {
    pub(super) store: Option<String>,
    pub(super) path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct ObjectStoreSection {
    pub(super) store: Option<String>,
    pub(super) path: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct VectorStoreSection {
    pub(super) store: Option<String>,
    pub(super) url: Option<String>,
    pub(super) collection: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct EnrichmentSection {
    pub(super) provider: Option<String>,
    pub(super) model: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct EmbeddingsSection {
    pub(super) provider: Option<String>,
    pub(super) base_url: Option<String>,
    pub(super) api_key: Option<String>,
    pub(super) model: Option<String>,
    pub(super) dimensions: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct HttpSection {
    pub(super) bind_addr: Option<String>,
    pub(super) request_workers: Option<usize>,
    pub(super) enrichment_workers: Option<usize>,
}
