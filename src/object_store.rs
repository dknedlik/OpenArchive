//! Object-store boundary for raw payloads and other large binary objects.
//!
//! Slice one uses a local filesystem-backed implementation. The current Oracle
//! relational path still stores payload bytes in its legacy schema, so the
//! object store is the source of truth at the application boundary while Oracle
//! remains a compatibility adapter underneath.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{ObjectStoreError, ObjectStoreResult};

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
        let storage_key = format!("{}/{}.bin", object.namespace, object.object_id);
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
