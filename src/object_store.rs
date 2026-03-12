//! Object-store boundary for raw payloads and other large binary objects.
//!
//! Objects are stored under deterministic, content-addressed keys so relational
//! dedupe can safely point multiple imports at the same underlying bytes.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::StatusCode;
use reqwest::Url;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::config::{S3CompatibleObjectStoreConfig, S3UrlStyle};
use crate::error::{ObjectStoreError, ObjectStoreResult};

const S3_SIGNED_URL_TTL_SECS: u64 = 60 * 15;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewObject {
    pub object_id: String,
    pub namespace: String,
    pub mime_type: String,
    pub bytes: Vec<u8>,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub object_id: String,
    pub provider: String,
    pub storage_key: String,
    pub mime_type: String,
    pub size_bytes: i64,
    pub sha256: String,
}

pub trait ObjectStore: Send + Sync {
    fn put_object(&self, object: NewObject) -> ObjectStoreResult<StoredObject>;
    fn get_object_bytes(&self, object: &StoredObject) -> ObjectStoreResult<Vec<u8>>;
}

pub struct LocalFsObjectStore {
    root: PathBuf,
}

impl LocalFsObjectStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn object_path(&self, storage_key: &str) -> PathBuf {
        self.root.join(storage_key)
    }

    fn ensure_parent_dir(path: &Path) -> ObjectStoreResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| ObjectStoreError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    }
}

impl ObjectStore for LocalFsObjectStore {
    fn put_object(&self, object: NewObject) -> ObjectStoreResult<StoredObject> {
        let storage_key = content_addressed_storage_key(&object.namespace, &object.sha256);
        let path = self.object_path(&storage_key);
        Self::ensure_parent_dir(&path)?;

        fs::write(&path, &object.bytes).map_err(|source| ObjectStoreError::WriteObject {
            object_id: object.object_id.clone(),
            path: path.clone(),
            source,
        })?;

        Ok(StoredObject {
            object_id: object.object_id,
            provider: "local_fs".to_string(),
            storage_key,
            mime_type: object.mime_type,
            size_bytes: object.bytes.len() as i64,
            sha256: object.sha256,
        })
    }

    fn get_object_bytes(&self, object: &StoredObject) -> ObjectStoreResult<Vec<u8>> {
        let path = self.object_path(&object.storage_key);
        fs::read(&path).map_err(|source| ObjectStoreError::ReadObject {
            object_id: object.object_id.clone(),
            path,
            source,
        })
    }
}

pub struct S3CompatibleObjectStore {
    bucket: Bucket,
    credentials: Credentials,
    client: Client,
    key_prefix: Option<String>,
}

impl S3CompatibleObjectStore {
    pub fn new(config: S3CompatibleObjectStoreConfig) -> ObjectStoreResult<Self> {
        let endpoint = Url::parse(&config.endpoint).map_err(|source| ObjectStoreError::InvalidUrl {
            key: "OA_S3_ENDPOINT",
            value: config.endpoint.clone(),
            source,
        })?;
        let bucket = Bucket::new(
            endpoint,
            match config.url_style {
                S3UrlStyle::Path => UrlStyle::Path,
                S3UrlStyle::VirtualHost => UrlStyle::VirtualHost,
            },
            config.bucket,
            config.region,
        )
        .map_err(|source| ObjectStoreError::InvalidS3Config {
            message: source.to_string(),
        })?;

        Ok(Self {
            bucket,
            credentials: Credentials::new(config.access_key_id, config.secret_access_key),
            client: Client::builder()
                .build()
                .map_err(|source| ObjectStoreError::BuildHttpClient { source })?,
            key_prefix: config.key_prefix.map(normalize_prefix),
        })
    }

    fn prefixed_key(&self, storage_key: &str) -> String {
        match &self.key_prefix {
            Some(prefix) => format!("{prefix}{storage_key}"),
            None => storage_key.to_string(),
        }
    }

    fn upload_url(&self, storage_key: &str, mime_type: &str) -> Url {
        let mut action = self
            .bucket
            .put_object(Some(&self.credentials), storage_key);
        action.headers_mut().insert("content-type", mime_type);
        action.sign(Duration::from_secs(S3_SIGNED_URL_TTL_SECS))
    }

    fn download_url(&self, storage_key: &str) -> Url {
        self.bucket
            .get_object(Some(&self.credentials), storage_key)
            .sign(Duration::from_secs(S3_SIGNED_URL_TTL_SECS))
    }
}

impl ObjectStore for S3CompatibleObjectStore {
    fn put_object(&self, object: NewObject) -> ObjectStoreResult<StoredObject> {
        let storage_key = content_addressed_storage_key(&object.namespace, &object.sha256);
        let remote_key = self.prefixed_key(&storage_key);
        let url = self.upload_url(&remote_key, &object.mime_type);
        let response = self
            .client
            .put(url)
            .header(CONTENT_TYPE, object.mime_type.clone())
            .header(CONTENT_LENGTH, object.bytes.len())
            .body(object.bytes.clone())
            .send()
            .map_err(|source| ObjectStoreError::SendRequest {
                operation: "put_object",
                object_id: object.object_id.clone(),
                source,
            })?;

        if response.status() != StatusCode::OK {
            return Err(ObjectStoreError::UnexpectedStatus {
                operation: "put_object",
                object_id: object.object_id,
                status: response.status().as_u16(),
            });
        }

        Ok(StoredObject {
            object_id: object.object_id,
            provider: "s3".to_string(),
            storage_key,
            mime_type: object.mime_type,
            size_bytes: object.bytes.len() as i64,
            sha256: object.sha256,
        })
    }

    fn get_object_bytes(&self, object: &StoredObject) -> ObjectStoreResult<Vec<u8>> {
        let remote_key = self.prefixed_key(&object.storage_key);
        let response = self
            .client
            .get(self.download_url(&remote_key))
            .send()
            .map_err(|source| ObjectStoreError::SendRequest {
                operation: "get_object_bytes",
                object_id: object.object_id.clone(),
                source,
            })?;

        if response.status() != StatusCode::OK {
            return Err(ObjectStoreError::UnexpectedStatus {
                operation: "get_object_bytes",
                object_id: object.object_id.clone(),
                status: response.status().as_u16(),
            });
        }

        response
            .bytes()
            .map(|bytes| bytes.to_vec())
            .map_err(|source| ObjectStoreError::ReadResponseBody {
                operation: "get_object_bytes",
                object_id: object.object_id.clone(),
                source,
            })
    }
}

fn content_addressed_storage_key(namespace: &str, sha256: &str) -> String {
    let shard = &sha256[..2];
    format!("{namespace}/{shard}/{sha256}.bin")
}

fn normalize_prefix(prefix: String) -> String {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("{trimmed}/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_addressed_key_uses_sha256_not_object_id() {
        let key = content_addressed_storage_key(
            "import-payloads",
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        );
        assert_eq!(
            key,
            "import-payloads/ab/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.bin"
        );
    }

    #[test]
    fn normalize_prefix_adds_single_trailing_slash() {
        assert_eq!(normalize_prefix("archive".to_string()), "archive/");
        assert_eq!(normalize_prefix("/archive/".to_string()), "archive/");
        assert_eq!(normalize_prefix("///".to_string()), "");
    }

    #[test]
    fn local_fs_store_returns_content_addressed_storage_key() {
        let temp_root = std::env::temp_dir().join(format!(
            "open_archive_object_store_test_{}",
            std::process::id()
        ));
        let store = LocalFsObjectStore::new(temp_root.clone());
        let stored = store
            .put_object(NewObject {
                object_id: "payload-1".to_string(),
                namespace: "imports".to_string(),
                mime_type: "application/json".to_string(),
                bytes: b"hello".to_vec(),
                sha256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                    .to_string(),
            })
            .unwrap();

        assert_eq!(
            stored.storage_key,
            "imports/ab/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.bin"
        );

        let _ = fs::remove_dir_all(temp_root);
    }
}
