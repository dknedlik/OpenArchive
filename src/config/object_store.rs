use crate::error::{ConfigError, ConfigResult};
use std::path::PathBuf;

use super::env::{
    default_local_archive_root, optional_path_env, optional_trimmed_env, required_env,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    LocalFs(LocalFsObjectStoreConfig),
    S3Compatible(S3CompatibleObjectStoreConfig),
}

impl ObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let provider = std::env::var("OA_OBJECT_STORE").unwrap_or_else(|_| "local_fs".to_string());
        match provider.as_str() {
            "local_fs" => Ok(Self::LocalFs(LocalFsObjectStoreConfig::from_env()?)),
            "s3" => Ok(Self::S3Compatible(
                S3CompatibleObjectStoreConfig::from_env()?
            )),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_OBJECT_STORE",
                value: provider,
                expected: "local_fs, s3",
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFsObjectStoreConfig {
    pub root: PathBuf,
}

impl LocalFsObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        let root = optional_path_env("OA_OBJECT_STORE_ROOT")?
            .unwrap_or(default_local_archive_root()?.join("objects"));

        Ok(Self { root })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleObjectStoreConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub key_prefix: Option<String>,
    pub url_style: S3UrlStyle,
}

impl S3CompatibleObjectStoreConfig {
    pub fn from_env() -> ConfigResult<Self> {
        Ok(Self {
            endpoint: required_env("OA_S3_ENDPOINT")?,
            region: required_env("OA_S3_REGION")?,
            bucket: required_env("OA_S3_BUCKET")?,
            access_key_id: required_env("OA_S3_ACCESS_KEY_ID")?,
            secret_access_key: required_env("OA_S3_SECRET_ACCESS_KEY")?,
            key_prefix: optional_trimmed_env("OA_S3_KEY_PREFIX"),
            url_style: S3UrlStyle::from_env()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3UrlStyle {
    Path,
    VirtualHost,
}

impl S3UrlStyle {
    pub fn from_env() -> ConfigResult<Self> {
        let value = std::env::var("OA_S3_URL_STYLE").unwrap_or_else(|_| "path".to_string());
        match value.as_str() {
            "path" => Ok(Self::Path),
            "virtual_host" => Ok(Self::VirtualHost),
            _ => Err(ConfigError::InvalidEnumEnv {
                key: "OA_S3_URL_STYLE",
                value,
                expected: "path, virtual_host",
            }),
        }
    }
}
