use std::sync::Arc;
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::{EmbeddingConfig, OpenAiEmbeddingConfig};
use crate::error::{EmbeddingError, EmbeddingResult};

pub const OPENAI_DEFAULT_EMBEDDING_MODEL: &str = "text-embedding-3-small";
pub const OPENAI_DEFAULT_EMBEDDING_DIMENSIONS: usize = 1536;

pub trait EmbeddingProvider: Send + Sync {
    fn provider_name(&self) -> &'static str;
    fn model_name(&self) -> &str;
    fn dimensions(&self) -> usize;
    fn embed_texts(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>>;
}

pub fn build_embedding_provider(
    config: &EmbeddingConfig,
) -> Result<Option<Arc<dyn EmbeddingProvider>>, String> {
    match config {
        EmbeddingConfig::Disabled => Ok(None),
        EmbeddingConfig::Stub(stub) => Ok(Some(Arc::new(StubEmbeddingProvider::new(
            stub.model.clone(),
            stub.dimensions,
        )))),
        EmbeddingConfig::OpenAi(openai) => Ok(Some(Arc::new(OpenAiEmbeddingProvider::new(
            openai.clone(),
        )?))),
    }
}

#[derive(Debug, Clone)]
pub struct StubEmbeddingProvider {
    model: String,
    dimensions: usize,
}

impl StubEmbeddingProvider {
    pub fn new(model: String, dimensions: usize) -> Self {
        Self { model, dimensions }
    }
}

impl EmbeddingProvider for StubEmbeddingProvider {
    fn provider_name(&self) -> &'static str {
        "stub"
    }

    fn model_name(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed_texts(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|text| stub_embedding_vector(text, self.dimensions))
            .collect())
    }
}

fn stub_embedding_vector(text: &str, dimensions: usize) -> Vec<f32> {
    let mut vector = vec![0.0f32; dimensions];
    if dimensions == 0 {
        return vector;
    }

    for (index, byte) in text.bytes().enumerate() {
        let slot = index % dimensions;
        let signed = (byte as i16 - 127) as f32 / 127.0;
        vector[slot] += signed;
    }

    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

#[derive(Debug)]
pub struct OpenAiEmbeddingProvider {
    client: Client,
    base_url: String,
    model: String,
    dimensions: usize,
}

impl OpenAiEmbeddingProvider {
    pub fn new(config: OpenAiEmbeddingConfig) -> Result<Self, String> {
        let mut default_headers = HeaderMap::new();
        default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let bearer = format!("Bearer {}", config.api_key);
        let auth_value = HeaderValue::from_str(&bearer)
            .map_err(|err| format!("invalid OpenAI embedding API key header: {err}"))?;
        default_headers.insert(AUTHORIZATION, auth_value);

        let client = Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(180))
            .build()
            .map_err(|err| err.to_string())?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            model: config.embedding_model,
            dimensions: config.embedding_dimensions,
        })
    }
}

impl EmbeddingProvider for OpenAiEmbeddingProvider {
    fn provider_name(&self) -> &'static str {
        "openai"
    }

    fn model_name(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed_texts(&self, texts: &[String]) -> EmbeddingResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let body = OpenAiEmbeddingsRequest {
            model: self.model.clone(),
            input: texts.to_vec(),
            dimensions: Some(self.dimensions),
            encoding_format: "float".to_string(),
        };
        let request_body =
            serde_json::to_vec(&body).map_err(|source| EmbeddingError::SerializeRequest {
                source: Box::new(source),
            })?;

        let response = self
            .client
            .post(format!("{}/embeddings", self.base_url))
            .body(request_body)
            .send()
            .map_err(|source| EmbeddingError::SendRequest {
                source: Box::new(source),
            })?;

        let status = response.status();
        let response_text = response
            .text()
            .map_err(|source| EmbeddingError::ReadResponse {
                source: Box::new(source),
            })?;
        if !status.is_success() {
            return Err(EmbeddingError::HttpStatus {
                status: status.as_u16(),
                body_preview: preview(&response_text),
            });
        }

        let parsed: OpenAiEmbeddingsResponse =
            serde_json::from_str(&response_text).map_err(|source| {
                EmbeddingError::ParseResponse {
                    source: Box::new(source),
                    body_preview: preview(&response_text),
                }
            })?;

        let mut vectors = Vec::with_capacity(parsed.data.len());
        for item in parsed.data {
            if item.embedding.len() != self.dimensions {
                return Err(EmbeddingError::UnexpectedDimensions {
                    expected: self.dimensions,
                    actual: item.embedding.len(),
                });
            }
            vectors.push(item.embedding);
        }
        Ok(vectors)
    }
}

fn preview(input: &str) -> String {
    const MAX_LEN: usize = 200;
    if input.len() <= MAX_LEN {
        input.to_string()
    } else {
        format!("{}...", &input[..MAX_LEN])
    }
}

#[derive(Debug, Serialize)]
struct OpenAiEmbeddingsRequest {
    model: String,
    input: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<usize>,
    encoding_format: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingsResponse {
    data: Vec<OpenAiEmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingData {
    embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_embedding_provider_returns_requested_dimensions() {
        let provider = StubEmbeddingProvider::new("stub-small".to_string(), 8);
        let vectors = provider
            .embed_texts(&["hello".to_string(), "world".to_string()])
            .expect("stub embeddings should succeed");

        assert_eq!(vectors.len(), 2);
        assert_eq!(vectors[0].len(), 8);
        assert_eq!(vectors[1].len(), 8);
    }
}
