#![deny(warnings)]

pub mod app;
pub mod bootstrap;
pub mod config;
pub mod db;
pub mod domain;
pub mod embedding;
pub mod enrichment_worker;
pub mod error;
pub mod extraction_chunking;
pub mod http;
pub mod import_service;
pub mod mcp;
pub mod migrations;
pub mod object_store;
pub mod parser;
pub mod postgres_db;
pub mod processor;
pub mod qdrant_sidecar;
pub mod secrets;
pub mod shutdown;
pub mod sqlite_db;
pub mod storage;
pub mod vector;

pub use domain::{ParticipantRole, SourceTimestamp, VisibilityStatus};
pub use error::{
    ConfigError, DbError, EmbeddingError, MigrationsError, ObjectStoreError, OpenArchiveError,
    ParserError, Result, SecretStoreError, StorageError, WorkerError, WorkerResult,
};
pub use import_service::looks_like_chatgpt_zip;
pub use secrets::{SecretBackend, SecretStore};
