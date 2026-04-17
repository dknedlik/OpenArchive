use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::types::Type;
use rusqlite::Connection;
use serde::de::DeserializeOwned;

use crate::config::SqliteConfig;
use crate::error::{DbError, StorageError, StorageResult};
use crate::sqlite_db;

pub(crate) mod artifact;
pub(crate) mod derived;
pub(crate) mod import;
pub(crate) mod job;
pub(crate) mod links;
pub(crate) mod retrieval;
pub(crate) mod review;
pub(crate) mod writeback;

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct SqliteImportWriteStore {
    pub(crate) config: SqliteConfig,
}

pub struct SqliteArtifactReadStore {
    pub(crate) config: SqliteConfig,
}

pub struct SqliteDerivedMetadataStore {
    pub(crate) config: SqliteConfig,
}

pub struct SqliteEnrichmentJobStore {
    pub(crate) config: SqliteConfig,
}

pub struct SqliteRetrievalReadStore {
    pub(crate) config: SqliteConfig,
}

pub struct SqliteOperatorStore {
    pub(crate) config: SqliteConfig,
}

pub struct SqliteWritebackStore {
    pub(crate) config: SqliteConfig,
}

impl SqliteImportWriteStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl SqliteArtifactReadStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl SqliteDerivedMetadataStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl SqliteEnrichmentJobStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl SqliteRetrievalReadStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl SqliteOperatorStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl SqliteWritebackStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

pub(super) fn open_connection(config: &SqliteConfig) -> StorageResult<Connection> {
    sqlite_db::connect(config).map_err(StorageError::from)
}

pub(super) fn db_err(config: &SqliteConfig, source: rusqlite::Error) -> StorageError {
    StorageError::Db(DbError::ConnectSqlite {
        path: config.path.display().to_string(),
        source: Box::new(source),
    })
}

pub(super) fn sqlite_count(value: i64, column: usize) -> rusqlite::Result<usize> {
    usize::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value))
}

pub(super) fn is_sqlite_contention_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database schema is locked")
        || message.contains("database is busy")
}

pub(super) fn is_sqlite_contention_error(error: &StorageError) -> bool {
    match error {
        StorageError::Db(DbError::ConnectSqlite { source, .. }) => {
            source.sqlite_error_code().is_some_and(|code| {
                matches!(
                    code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                )
            })
        }
        _ => is_sqlite_contention_error_message(&error.to_string()),
    }
}

pub(super) fn with_sqlite_write_retry<T, F>(
    config: &SqliteConfig,
    mut operation: F,
) -> StorageResult<T>
where
    F: FnMut() -> StorageResult<T>,
{
    let retry_budget = config.busy_timeout.max(Duration::from_secs(30));
    let deadline = Instant::now() + retry_budget;
    let mut backoff = Duration::from_millis(25);

    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if is_sqlite_contention_error(&error) && Instant::now() < deadline => {
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(250));
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn invalid_enum(column: &'static str, value: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid {column}: {value}"),
        )),
    )
}

pub(super) fn from_sqlite_timestamp(
    value: Option<String>,
) -> Option<crate::domain::SourceTimestamp> {
    value.and_then(|ts| crate::domain::SourceTimestamp::parse_rfc3339(&ts).ok())
}

pub(super) fn deserialize_json<T: DeserializeOwned>(json: &str, detail: &str) -> StorageResult<T> {
    serde_json::from_str(json).map_err(|err| StorageError::InvalidDerivationWrite {
        detail: format!("{detail}: {err}"),
    })
}

pub(super) fn next_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{counter}")
}

pub(super) fn parse_source_type(
    value: String,
) -> rusqlite::Result<crate::storage::types::SourceType> {
    use crate::storage::types::SourceType;
    SourceType::parse(&value).ok_or_else(|| invalid_enum("source_type", value))
}

pub(super) fn parse_enrichment_status(
    value: String,
) -> rusqlite::Result<crate::storage::types::EnrichmentStatus> {
    use crate::storage::types::EnrichmentStatus;
    EnrichmentStatus::parse(&value).ok_or_else(|| invalid_enum("enrichment_status", value))
}

pub(super) fn parse_job_status(value: String) -> rusqlite::Result<crate::storage::JobStatus> {
    crate::storage::JobStatus::parse(&value).ok_or_else(|| invalid_enum("job_status", value))
}

pub(super) fn parse_artifact_class(
    value: String,
) -> rusqlite::Result<crate::storage::types::ArtifactClass> {
    use crate::storage::types::ArtifactClass;
    ArtifactClass::parse(&value).ok_or_else(|| invalid_enum("artifact_class", value))
}

pub(super) fn parse_participant_role(
    value: String,
) -> rusqlite::Result<crate::domain::ParticipantRole> {
    crate::domain::ParticipantRole::parse(&value)
        .ok_or_else(|| invalid_enum("participant_role", value))
}

pub(super) fn parse_visibility_status(
    value: String,
) -> rusqlite::Result<crate::domain::VisibilityStatus> {
    crate::domain::VisibilityStatus::parse(&value)
        .ok_or_else(|| invalid_enum("visibility_status", value))
}

pub(super) fn parse_derived_object_type(
    value: String,
) -> rusqlite::Result<crate::storage::types::DerivedObjectType> {
    use crate::storage::types::DerivedObjectType;
    DerivedObjectType::parse(&value).ok_or_else(|| invalid_enum("derived_object_type", value))
}

pub(super) fn parse_job_type(value: String) -> rusqlite::Result<crate::storage::types::JobType> {
    use crate::storage::types::JobType;
    JobType::parse(&value).ok_or_else(|| invalid_enum("job_type", value))
}

pub(super) fn parse_enrichment_tier(
    value: String,
) -> rusqlite::Result<crate::storage::types::EnrichmentTier> {
    use crate::storage::types::EnrichmentTier;
    EnrichmentTier::parse(&value).ok_or_else(|| invalid_enum("enrichment_tier", value))
}

pub(super) fn parse_scope_type(
    value: String,
) -> rusqlite::Result<crate::storage::types::ScopeType> {
    use crate::storage::types::ScopeType;
    ScopeType::parse(&value).ok_or_else(|| invalid_enum("scope_type", value))
}

pub(super) fn parse_imported_note_property_kind(
    value: String,
) -> rusqlite::Result<crate::storage::types::ImportedNotePropertyValueKind> {
    use crate::storage::types::ImportedNotePropertyValueKind;
    ImportedNotePropertyValueKind::parse(&value).ok_or_else(|| invalid_enum("value_kind", value))
}

pub(super) fn parse_imported_note_tag_source(
    value: String,
) -> rusqlite::Result<crate::storage::types::ImportedNoteTagSourceKind> {
    use crate::storage::types::ImportedNoteTagSourceKind;
    ImportedNoteTagSourceKind::parse(&value).ok_or_else(|| invalid_enum("source_kind", value))
}

pub(super) fn parse_imported_note_link_kind(
    value: String,
) -> rusqlite::Result<crate::storage::types::ImportedNoteLinkKind> {
    use crate::storage::types::ImportedNoteLinkKind;
    ImportedNoteLinkKind::parse(&value).ok_or_else(|| invalid_enum("link_kind", value))
}

pub(super) fn parse_imported_note_target_kind(
    value: String,
) -> rusqlite::Result<crate::storage::types::ImportedNoteLinkTargetKind> {
    use crate::storage::types::ImportedNoteLinkTargetKind;
    ImportedNoteLinkTargetKind::parse(&value).ok_or_else(|| invalid_enum("target_kind", value))
}

pub(super) fn parse_imported_note_resolution_status(
    value: String,
) -> rusqlite::Result<crate::storage::types::ImportedNoteLinkResolutionStatus> {
    use crate::storage::types::ImportedNoteLinkResolutionStatus;
    ImportedNoteLinkResolutionStatus::parse(&value)
        .ok_or_else(|| invalid_enum("resolution_status", value))
}

pub(super) fn parse_artifact_link_type(
    value: String,
) -> rusqlite::Result<crate::storage::types::ArtifactLinkType> {
    use crate::storage::types::ArtifactLinkType;
    ArtifactLinkType::parse(&value).ok_or_else(|| invalid_enum("artifact_link_type", value))
}
