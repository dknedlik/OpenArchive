//! Object-store boundary for raw payloads and other large binary objects.
//!
//! Objects are stored under deterministic, content-addressed keys so relational
//! dedupe can safely point multiple imports at the same underlying bytes.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG};
use reqwest::StatusCode;
use reqwest::Url;
use rusty_s3::actions::CreateMultipartUpload;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::config::{S3CompatibleObjectStoreConfig, S3UrlStyle};
use crate::error::{ObjectStoreError, ObjectStoreResult};

const S3_SIGNED_URL_TTL_SECS: u64 = 60 * 15;
const MULTIPART_THRESHOLD: usize = 50 * 1024 * 1024; // 50 MB
const MULTIPART_PART_SIZE: usize = 10 * 1024 * 1024; // 10 MB

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectResult {
    pub stored_object: StoredObject,
    pub was_created: bool,
}

pub trait ObjectStore: Send + Sync {
    fn put_object(&self, object: NewObject) -> ObjectStoreResult<PutObjectResult>;
    fn get_object_bytes(&self, object: &StoredObject) -> ObjectStoreResult<Vec<u8>>;
    fn delete_object(&self, object: &StoredObject) -> ObjectStoreResult<()>;
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
                source: Box::new(source),
            })?;
        }
        Ok(())
    }
}

impl ObjectStore for LocalFsObjectStore {
    fn put_object(&self, object: NewObject) -> ObjectStoreResult<PutObjectResult> {
        let storage_key = content_addressed_storage_key(&object.namespace, &object.sha256);
        let path = self.object_path(&storage_key);
        let already_exists = path.exists();
        Self::ensure_parent_dir(&path)?;

        if !already_exists {
            fs::write(&path, &object.bytes).map_err(|source| ObjectStoreError::WriteObject {
                object_id: object.object_id.clone(),
                path: path.clone(),
                source: Box::new(source),
            })?;
        }

        Ok(PutObjectResult {
            stored_object: StoredObject {
                object_id: object.object_id,
                provider: "local_fs".to_string(),
                storage_key,
                mime_type: object.mime_type,
                size_bytes: object.bytes.len() as i64,
                sha256: object.sha256,
            },
            was_created: !already_exists,
        })
    }

    fn get_object_bytes(&self, object: &StoredObject) -> ObjectStoreResult<Vec<u8>> {
        let path = self.object_path(&object.storage_key);
        fs::read(&path).map_err(|source| ObjectStoreError::ReadObject {
            object_id: object.object_id.clone(),
            path,
            source: Box::new(source),
        })
    }

    fn delete_object(&self, object: &StoredObject) -> ObjectStoreResult<()> {
        let path = self.object_path(&object.storage_key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ObjectStoreError::DeleteObject {
                object_id: object.object_id.clone(),
                path,
                source: Box::new(source),
            }),
        }
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
        let endpoint =
            Url::parse(&config.endpoint).map_err(|source| ObjectStoreError::InvalidUrl {
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
            client: Client::builder().build().map_err(|source| {
                ObjectStoreError::BuildHttpClient {
                    source: Box::new(source),
                }
            })?,
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
        let mut action = self.bucket.put_object(Some(&self.credentials), storage_key);
        action.headers_mut().insert("content-type", mime_type);
        action.sign(Duration::from_secs(S3_SIGNED_URL_TTL_SECS))
    }

    fn download_url(&self, storage_key: &str) -> Url {
        self.bucket
            .get_object(Some(&self.credentials), storage_key)
            .sign(Duration::from_secs(S3_SIGNED_URL_TTL_SECS))
    }

    fn delete_url(&self, storage_key: &str) -> Url {
        self.bucket
            .delete_object(Some(&self.credentials), storage_key)
            .sign(Duration::from_secs(S3_SIGNED_URL_TTL_SECS))
    }

    fn head_url(&self, storage_key: &str) -> Url {
        self.bucket
            .head_object(Some(&self.credentials), storage_key)
            .sign(Duration::from_secs(S3_SIGNED_URL_TTL_SECS))
    }
}

impl S3CompatibleObjectStore {
    fn put_single(&self, remote_key: &str, object: &NewObject) -> ObjectStoreResult<()> {
        let response = self
            .client
            .put(self.upload_url(remote_key, &object.mime_type))
            .header(CONTENT_TYPE, object.mime_type.clone())
            .header(CONTENT_LENGTH, object.bytes.len())
            .body(object.bytes.clone())
            .send()
            .map_err(|source| ObjectStoreError::SendRequest {
                operation: "put_object",
                object_id: object.object_id.clone(),
                source: Box::new(source),
            })?;

        if response.status() != StatusCode::OK {
            return Err(ObjectStoreError::UnexpectedStatus {
                operation: "put_object",
                object_id: object.object_id.clone(),
                status: response.status().as_u16(),
            });
        }
        Ok(())
    }

    fn put_multipart(&self, remote_key: &str, object: &NewObject) -> ObjectStoreResult<()> {
        let ttl = Duration::from_secs(S3_SIGNED_URL_TTL_SECS);

        // 1. Initiate multipart upload
        let create_action = self
            .bucket
            .create_multipart_upload(Some(&self.credentials), remote_key);
        let create_url = create_action.sign(ttl);

        let create_resp = self.client.post(create_url).send().map_err(|source| {
            ObjectStoreError::SendRequest {
                operation: "create_multipart_upload",
                object_id: object.object_id.clone(),
                source: Box::new(source),
            }
        })?;

        if create_resp.status() != StatusCode::OK {
            return Err(ObjectStoreError::UnexpectedStatus {
                operation: "create_multipart_upload",
                object_id: object.object_id.clone(),
                status: create_resp.status().as_u16(),
            });
        }

        let create_body =
            create_resp
                .bytes()
                .map_err(|source| ObjectStoreError::ReadResponseBody {
                    operation: "create_multipart_upload",
                    object_id: object.object_id.clone(),
                    source: Box::new(source),
                })?;

        let parsed = CreateMultipartUpload::parse_response(&create_body).map_err(|e| {
            ObjectStoreError::ParseMultipartResponse {
                object_id: object.object_id.clone(),
                detail: e.to_string(),
            }
        })?;
        let upload_id = parsed.upload_id();

        // 2. Upload parts
        let chunks: Vec<&[u8]> = object.bytes.chunks(MULTIPART_PART_SIZE).collect();
        let total_parts = chunks.len();
        let mut etags: Vec<String> = Vec::with_capacity(total_parts);

        for (i, chunk) in chunks.iter().enumerate() {
            let part_number = (i + 1) as u16;

            log::info!(
                "multipart upload: part {}/{} for {}",
                part_number,
                total_parts,
                object.object_id,
            );

            let part_action = self.bucket.upload_part(
                Some(&self.credentials),
                remote_key,
                part_number,
                upload_id,
            );
            let part_url = part_action.sign(ttl);

            let part_resp = self
                .client
                .put(part_url)
                .header(CONTENT_LENGTH, chunk.len())
                .body(chunk.to_vec())
                .send();

            let part_resp = match part_resp {
                Ok(r) => r,
                Err(source) => {
                    let _ = self.abort_multipart(remote_key, upload_id, &object.object_id);
                    return Err(ObjectStoreError::SendRequest {
                        operation: "upload_part",
                        object_id: object.object_id.clone(),
                        source: Box::new(source),
                    });
                }
            };

            if part_resp.status() != StatusCode::OK {
                let status = part_resp.status().as_u16();
                let _ = self.abort_multipart(remote_key, upload_id, &object.object_id);
                return Err(ObjectStoreError::MultipartUploadFailed {
                    object_id: object.object_id.clone(),
                    part: part_number,
                    total_parts,
                    detail: format!("upload_part returned HTTP {status}"),
                });
            }

            let etag = part_resp
                .headers()
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_string());

            match etag {
                Some(tag) => etags.push(tag),
                None => {
                    let _ = self.abort_multipart(remote_key, upload_id, &object.object_id);
                    return Err(ObjectStoreError::MultipartUploadFailed {
                        object_id: object.object_id.clone(),
                        part: part_number,
                        total_parts,
                        detail: "upload_part response missing ETag header".to_string(),
                    });
                }
            }
        }

        // 3. Complete multipart upload
        let complete_action = self.bucket.complete_multipart_upload(
            Some(&self.credentials),
            remote_key,
            upload_id,
            etags.iter().map(|s| s.as_str()),
        );
        let complete_url = complete_action.sign(ttl);
        let complete_body = complete_action.body();

        let complete_resp = self
            .client
            .post(complete_url)
            .header(CONTENT_TYPE, "application/xml")
            .body(complete_body)
            .send();

        let complete_resp = match complete_resp {
            Ok(r) => r,
            Err(source) => {
                let _ = self.abort_multipart(remote_key, upload_id, &object.object_id);
                return Err(ObjectStoreError::SendRequest {
                    operation: "complete_multipart_upload",
                    object_id: object.object_id.clone(),
                    source: Box::new(source),
                });
            }
        };

        if complete_resp.status() != StatusCode::OK {
            let status = complete_resp.status().as_u16();
            let _ = self.abort_multipart(remote_key, upload_id, &object.object_id);
            return Err(ObjectStoreError::UnexpectedStatus {
                operation: "complete_multipart_upload",
                object_id: object.object_id.clone(),
                status,
            });
        }

        Ok(())
    }

    fn abort_multipart(
        &self,
        remote_key: &str,
        upload_id: &str,
        object_id: &str,
    ) -> ObjectStoreResult<()> {
        let ttl = Duration::from_secs(S3_SIGNED_URL_TTL_SECS);
        let abort_action =
            self.bucket
                .abort_multipart_upload(Some(&self.credentials), remote_key, upload_id);
        let abort_url = abort_action.sign(ttl);

        log::warn!(
            "aborting multipart upload for object {} (upload_id={})",
            object_id,
            upload_id,
        );

        let resp = self.client.delete(abort_url).send().map_err(|source| {
            ObjectStoreError::SendRequest {
                operation: "abort_multipart_upload",
                object_id: object_id.to_string(),
                source: Box::new(source),
            }
        })?;

        match resp.status() {
            StatusCode::OK | StatusCode::NO_CONTENT => Ok(()),
            status => Err(ObjectStoreError::UnexpectedStatus {
                operation: "abort_multipart_upload",
                object_id: object_id.to_string(),
                status: status.as_u16(),
            }),
        }
    }
}

impl ObjectStore for S3CompatibleObjectStore {
    fn put_object(&self, object: NewObject) -> ObjectStoreResult<PutObjectResult> {
        let storage_key = content_addressed_storage_key(&object.namespace, &object.sha256);
        let remote_key = self.prefixed_key(&storage_key);

        let head_response = self
            .client
            .head(self.head_url(&remote_key))
            .send()
            .map_err(|source| ObjectStoreError::SendRequest {
                operation: "head_object",
                object_id: object.object_id.clone(),
                source: Box::new(source),
            })?;

        let already_exists = match head_response.status() {
            StatusCode::OK => true,
            StatusCode::NOT_FOUND => false,
            status => {
                return Err(ObjectStoreError::UnexpectedStatus {
                    operation: "head_object",
                    object_id: object.object_id.clone(),
                    status: status.as_u16(),
                });
            }
        };

        if !already_exists {
            if object.bytes.len() >= MULTIPART_THRESHOLD {
                log::info!(
                    "object {} is {} bytes, using multipart upload",
                    object.object_id,
                    object.bytes.len(),
                );
                self.put_multipart(&remote_key, &object)?;
            } else {
                self.put_single(&remote_key, &object)?;
            }
        }

        Ok(PutObjectResult {
            stored_object: StoredObject {
                object_id: object.object_id,
                provider: "s3".to_string(),
                storage_key,
                mime_type: object.mime_type,
                size_bytes: object.bytes.len() as i64,
                sha256: object.sha256,
            },
            was_created: !already_exists,
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
                source: Box::new(source),
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
                source: Box::new(source),
            })
    }

    fn delete_object(&self, object: &StoredObject) -> ObjectStoreResult<()> {
        let remote_key = self.prefixed_key(&object.storage_key);
        let response = self
            .client
            .delete(self.delete_url(&remote_key))
            .send()
            .map_err(|source| ObjectStoreError::SendRequest {
                operation: "delete_object",
                object_id: object.object_id.clone(),
                source: Box::new(source),
            })?;

        match response.status() {
            StatusCode::NO_CONTENT | StatusCode::OK | StatusCode::NOT_FOUND => Ok(()),
            status => Err(ObjectStoreError::UnexpectedStatus {
                operation: "delete_object",
                object_id: object.object_id.clone(),
                status: status.as_u16(),
            }),
        }
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
            stored.stored_object.storage_key,
            "imports/ab/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789.bin"
        );
        assert!(stored.was_created);

        let _ = fs::remove_dir_all(temp_root);
    }
}
