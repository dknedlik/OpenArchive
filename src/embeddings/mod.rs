use std::path::PathBuf;
use std::sync::Mutex;

use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

use crate::config::{EmbeddingConfig, LocalEmbeddingModel};
use crate::error::EmbeddingError;

pub trait TextEmbedder: Send + Sync {
    fn embed_texts(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}

pub struct FastembedTextEmbedder {
    model: LocalEmbeddingModel,
    cache_dir: Option<PathBuf>,
    inner: Mutex<Option<TextEmbedding>>,
}

impl FastembedTextEmbedder {
    pub fn new(config: &EmbeddingConfig) -> Self {
        Self {
            model: config.model,
            cache_dir: config.cache_dir.clone(),
            inner: Mutex::new(None),
        }
    }

    fn get_or_init(&self) -> Result<std::sync::MutexGuard<'_, Option<TextEmbedding>>, EmbeddingError> {
        let mut guard = self.inner.lock().expect("embedding model mutex poisoned");
        if guard.is_none() {
            let mut options = InitOptions::new(self.model.as_fastembed_model());
            if let Some(cache_dir) = &self.cache_dir {
                options = options.with_cache_dir(cache_dir.clone());
            }
            let model = TextEmbedding::try_new(options).map_err(|err| EmbeddingError::Initialize {
                message: err.to_string(),
            })?;
            *guard = Some(model);
        }
        Ok(guard)
    }
}

impl TextEmbedder for FastembedTextEmbedder {
    fn embed_texts(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let input: Vec<&str> = texts.iter().map(String::as_str).collect();
        let mut guard = self.get_or_init()?;
        let model = guard.as_mut().expect("embedding model initialized");
        model
            .embed(input, None)
            .map_err(|err| EmbeddingError::Generate {
                message: err.to_string(),
            })
    }
}

impl LocalEmbeddingModel {
    pub fn as_fastembed_model(self) -> EmbeddingModel {
        match self {
            Self::BgeSmallEnV15 => EmbeddingModel::BGESmallENV15,
        }
    }
}
