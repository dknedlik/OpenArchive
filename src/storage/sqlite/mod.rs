use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::types::Type;
use rusqlite::{
    params, params_from_iter, Connection, OptionalExtension, ToSql, TransactionBehavior,
};
use serde::de::DeserializeOwned;

use crate::config::SqliteConfig;
use crate::error::{DbError, StorageError, StorageResult};
use crate::sqlite_db;
use crate::storage::derivation_store::{
    DerivationWriteResult, DerivedMetadataWriteStore, WriteDerivationAttempt,
};
use crate::storage::enrichment_state_store::EnrichmentStateStore;
use crate::storage::job_store::EnrichmentJobLifecycleStore;
use crate::storage::retrieval_read_store::{
    ArchiveSearchCandidate, ArchiveSearchReadStore, ArtifactContextDerivedObject,
    ArtifactContextPackMaterial, ArtifactContextPackReadStore, ArtifactDetailDerivedObject,
    ArtifactDetailReadStore, ArtifactDetailRecord, ArtifactDetailSegment, ArtifactDetailView,
    CrossArtifactReadStore, DerivedObjectLookupStore, DerivedObjectSearchResult,
    DerivedObjectSearchStore, GraphRelatedEntry, ObjectSearchFilters, RelatedDerivedObject,
    RelatedDerivedObjectEmbeddingMatch, SearchCandidateKind, SearchFilters,
};
use crate::storage::review_read_store::{
    NewReviewDecision, ReviewCandidate, ReviewItemKind, ReviewQueueFilters, ReviewReadStore,
    ReviewWriteStore,
};
use crate::storage::types::{
    ArtifactClass, ArtifactExtractPayload, ArtifactExtractionResult, ArtifactIngestResult,
    ArtifactLinkRecord, ArtifactLinkType, ArtifactListFilters, ArtifactListItem, ClaimedJob,
    DerivedObjectType, EnrichmentStatus, EnrichmentTier, ImportedNoteAliasRecord,
    ImportedNoteLinkKind, ImportedNoteLinkRecord, ImportedNoteLinkResolutionStatus,
    ImportedNoteLinkTargetKind, ImportedNoteMetadata, ImportedNotePropertyRecord,
    ImportedNotePropertyValueKind, ImportedNoteTagRecord, ImportedNoteTagSourceKind, JobType,
    LoadedArtifactForEnrichment, LoadedArtifactRecord, LoadedParticipant, LoadedSegment,
    NewEnrichmentBatch, PersistedEnrichmentBatch, ReconciliationDecision, RetrievalIntent,
    RetrievedContextItem, RetryOutcome, ScopeType, SourceType, TimelineEntry, TimelineFilters,
};
use crate::storage::writeback_store::{
    NewAgentEntity, NewAgentMemory, NewArchiveLink, UpdateObjectStatus, WritebackStore,
};
use crate::storage::{
    ArchiveRetrievalStore, ArtifactReadStore, ImportWriteResult, ImportWriteStore,
    ImportedArtifact, NewImportedNoteAlias, NewImportedNoteLink, NewImportedNoteProperty,
    NewImportedNoteTag, WriteImportSet,
};
use crate::{ParticipantRole, SourceTimestamp, VisibilityStatus};

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct SqliteImportWriteStore {
    config: SqliteConfig,
}

pub struct SqliteArtifactReadStore {
    config: SqliteConfig,
}

pub struct SqliteDerivedMetadataStore {
    config: SqliteConfig,
}

pub struct SqliteEnrichmentJobStore {
    config: SqliteConfig,
}

pub struct SqliteRetrievalReadStore {
    config: SqliteConfig,
}

pub struct SqliteWritebackStore {
    config: SqliteConfig,
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

impl SqliteWritebackStore {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

fn open_connection(config: &SqliteConfig) -> StorageResult<Connection> {
    sqlite_db::connect(config).map_err(StorageError::from)
}

fn db_err(config: &SqliteConfig, source: rusqlite::Error) -> StorageError {
    StorageError::Db(DbError::ConnectSqlite {
        path: config.path.display().to_string(),
        source: Box::new(source),
    })
}

fn is_sqlite_contention_error_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database schema is locked")
        || message.contains("database is busy")
}

fn is_sqlite_contention_error(error: &StorageError) -> bool {
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

fn with_sqlite_write_retry<T, F>(config: &SqliteConfig, mut operation: F) -> StorageResult<T>
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

fn invalid_enum(column: &'static str, value: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid {column}: {value}"),
        )),
    )
}

fn from_sqlite_timestamp(value: Option<String>) -> Option<SourceTimestamp> {
    value.and_then(|ts| SourceTimestamp::parse_rfc3339(&ts).ok())
}

fn parse_source_type(value: String) -> rusqlite::Result<SourceType> {
    SourceType::parse(&value).ok_or_else(|| invalid_enum("source_type", value))
}

fn parse_enrichment_status(value: String) -> rusqlite::Result<EnrichmentStatus> {
    EnrichmentStatus::parse(&value).ok_or_else(|| invalid_enum("enrichment_status", value))
}

fn parse_artifact_class(value: String) -> rusqlite::Result<ArtifactClass> {
    ArtifactClass::parse(&value).ok_or_else(|| invalid_enum("artifact_class", value))
}

fn parse_participant_role(value: String) -> rusqlite::Result<ParticipantRole> {
    ParticipantRole::parse(&value).ok_or_else(|| invalid_enum("participant_role", value))
}

fn parse_visibility_status(value: String) -> rusqlite::Result<VisibilityStatus> {
    VisibilityStatus::parse(&value).ok_or_else(|| invalid_enum("visibility_status", value))
}

fn parse_derived_object_type(value: String) -> rusqlite::Result<DerivedObjectType> {
    DerivedObjectType::parse(&value).ok_or_else(|| invalid_enum("derived_object_type", value))
}

fn parse_job_type(value: String) -> rusqlite::Result<JobType> {
    JobType::parse(&value).ok_or_else(|| invalid_enum("job_type", value))
}

fn parse_enrichment_tier(value: String) -> rusqlite::Result<EnrichmentTier> {
    EnrichmentTier::parse(&value).ok_or_else(|| invalid_enum("enrichment_tier", value))
}

fn parse_scope_type(value: String) -> rusqlite::Result<ScopeType> {
    ScopeType::parse(&value).ok_or_else(|| invalid_enum("scope_type", value))
}

fn parse_imported_note_property_kind(
    value: String,
) -> rusqlite::Result<ImportedNotePropertyValueKind> {
    ImportedNotePropertyValueKind::parse(&value).ok_or_else(|| invalid_enum("value_kind", value))
}

fn parse_imported_note_tag_source(value: String) -> rusqlite::Result<ImportedNoteTagSourceKind> {
    ImportedNoteTagSourceKind::parse(&value).ok_or_else(|| invalid_enum("source_kind", value))
}

fn parse_imported_note_link_kind(value: String) -> rusqlite::Result<ImportedNoteLinkKind> {
    ImportedNoteLinkKind::parse(&value).ok_or_else(|| invalid_enum("link_kind", value))
}

fn parse_imported_note_target_kind(value: String) -> rusqlite::Result<ImportedNoteLinkTargetKind> {
    ImportedNoteLinkTargetKind::parse(&value).ok_or_else(|| invalid_enum("target_kind", value))
}

fn parse_imported_note_resolution_status(
    value: String,
) -> rusqlite::Result<ImportedNoteLinkResolutionStatus> {
    ImportedNoteLinkResolutionStatus::parse(&value)
        .ok_or_else(|| invalid_enum("resolution_status", value))
}

fn parse_artifact_link_type(value: String) -> rusqlite::Result<ArtifactLinkType> {
    ArtifactLinkType::parse(&value).ok_or_else(|| invalid_enum("artifact_link_type", value))
}

fn deserialize_json<T: DeserializeOwned>(json: &str, detail: &str) -> StorageResult<T> {
    serde_json::from_str(json).map_err(|err| StorageError::InvalidDerivationWrite {
        detail: format!("{detail}: {err}"),
    })
}

fn next_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{counter}")
}

fn insert_imported_note_property(
    tx: &rusqlite::Transaction<'_>,
    property: &NewImportedNoteProperty,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_property
         (artifact_note_property_id, artifact_id, property_key, value_kind, value_text, value_json, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            property.imported_note_property_id,
            property.artifact_id,
            property.property_key,
            property.value_kind.as_str(),
            property.value_text,
            property.value_json,
            property.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteProperty {
        artifact_id: property.artifact_id.clone(),
        property_key: property.property_key.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

fn insert_imported_note_tag(
    tx: &rusqlite::Transaction<'_>,
    tag: &NewImportedNoteTag,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_tag
         (artifact_note_tag_id, artifact_id, raw_tag, normalized_tag, tag_path, source_kind, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            tag.imported_note_tag_id,
            tag.artifact_id,
            tag.raw_tag,
            tag.normalized_tag,
            tag.tag_path,
            tag.source_kind.as_str(),
            tag.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteTag {
        artifact_id: tag.artifact_id.clone(),
        tag_value: tag.normalized_tag.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

fn insert_imported_note_alias(
    tx: &rusqlite::Transaction<'_>,
    alias: &NewImportedNoteAlias,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_alias
         (artifact_note_alias_id, artifact_id, alias_text, normalized_alias, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            alias.imported_note_alias_id,
            alias.artifact_id,
            alias.alias_text,
            alias.normalized_alias,
            alias.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteAlias {
        artifact_id: alias.artifact_id.clone(),
        alias_text: alias.alias_text.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

fn insert_imported_note_link(
    tx: &rusqlite::Transaction<'_>,
    link: &NewImportedNoteLink,
) -> StorageResult<()> {
    tx.execute(
        "INSERT INTO oa_artifact_note_link
         (artifact_note_link_id, artifact_id, source_segment_id, link_kind, target_kind, raw_target,
          normalized_target, display_text, target_path, target_heading, target_block, external_url,
          resolved_artifact_id, resolution_status, locator_json, sequence_no)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            link.imported_note_link_id,
            link.artifact_id,
            link.source_segment_id,
            link.link_kind.as_str(),
            link.target_kind.as_str(),
            link.raw_target,
            link.normalized_target,
            link.display_text,
            link.target_path,
            link.target_heading,
            link.target_block,
            link.external_url,
            link.resolved_artifact_id,
            link.resolution_status.as_str(),
            link.locator_json,
            link.sequence_no
        ],
    )
    .map_err(|source| StorageError::InsertImportedNoteLink {
        artifact_id: link.artifact_id.clone(),
        link_id: link.imported_note_link_id.clone(),
        source: Box::new(source),
    })?;
    Ok(())
}

fn load_note_links(
    connection: &Connection,
    sql: &str,
    params: &[&dyn ToSql],
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    let mut stmt =
        connection
            .prepare(sql)
            .map_err(|source| StorageError::InsertImportedNoteLink {
                artifact_id: "sqlite".to_string(),
                link_id: "prepare".to_string(),
                source: Box::new(source),
            })?;
    let rows = stmt
        .query_map(params, |row| {
            Ok(ImportedNoteLinkRecord {
                imported_note_link_id: row.get(0)?,
                source_segment_id: row.get(1)?,
                link_kind: parse_imported_note_link_kind(row.get(2)?)?,
                target_kind: parse_imported_note_target_kind(row.get(3)?)?,
                raw_target: row.get(4)?,
                normalized_target: row.get(5)?,
                display_text: row.get(6)?,
                target_path: row.get(7)?,
                target_heading: row.get(8)?,
                target_block: row.get(9)?,
                external_url: row.get(10)?,
                resolved_artifact_id: row.get(11)?,
                resolution_status: parse_imported_note_resolution_status(row.get(12)?)?,
                locator_json: row.get(13)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note links: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note links: {source}"),
        })
}

fn load_imported_note_metadata(
    connection: &Connection,
    artifact_id: &str,
    note_path: Option<String>,
) -> StorageResult<ImportedNoteMetadata> {
    let mut properties_stmt = connection
        .prepare(
            "SELECT property_key, value_kind, value_text, value_json
             FROM oa_artifact_note_property
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, artifact_note_property_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare imported note properties query: {source}"),
        })?;
    let properties = properties_stmt
        .query_map([artifact_id], |row| {
            Ok(ImportedNotePropertyRecord {
                property_key: row.get(0)?,
                value_kind: parse_imported_note_property_kind(row.get(1)?)?,
                value_text: row.get(2)?,
                value_json: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(3)?)
                    .map_err(|err| {
                        rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(err))
                    })?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note properties: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note properties: {source}"),
        })?;

    let mut tags_stmt = connection
        .prepare(
            "SELECT raw_tag, normalized_tag, tag_path, source_kind
             FROM oa_artifact_note_tag
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, artifact_note_tag_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare imported note tags query: {source}"),
        })?;
    let tags = tags_stmt
        .query_map([artifact_id], |row| {
            Ok(ImportedNoteTagRecord {
                raw_tag: row.get(0)?,
                normalized_tag: row.get(1)?,
                tag_path: row.get(2)?,
                source_kind: parse_imported_note_tag_source(row.get(3)?)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note tags: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note tags: {source}"),
        })?;

    let mut aliases_stmt = connection
        .prepare(
            "SELECT alias_text, normalized_alias
             FROM oa_artifact_note_alias
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, artifact_note_alias_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare imported note aliases query: {source}"),
        })?;
    let aliases = aliases_stmt
        .query_map([artifact_id], |row| {
            Ok(ImportedNoteAliasRecord {
                alias_text: row.get(0)?,
                normalized_alias: row.get(1)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load imported note aliases: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map imported note aliases: {source}"),
        })?;

    let outbound_links = load_note_links(
        connection,
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target,
                normalized_target, display_text, target_path, target_heading, target_block, external_url,
                resolved_artifact_id, resolution_status, locator_json
         FROM oa_artifact_note_link
         WHERE artifact_id = ?1
         ORDER BY sequence_no ASC, artifact_note_link_id ASC",
        &[&artifact_id],
    )?;

    Ok(ImportedNoteMetadata {
        note_path,
        properties,
        tags,
        aliases,
        outbound_links,
    })
}

fn load_inbound_note_links(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ImportedNoteLinkRecord>> {
    load_note_links(
        connection,
        "SELECT artifact_note_link_id, source_segment_id, link_kind, target_kind, raw_target,
                normalized_target, display_text, target_path, target_heading, target_block, external_url,
                resolved_artifact_id, resolution_status, locator_json
         FROM oa_artifact_note_link
         WHERE resolved_artifact_id = ?1
         ORDER BY sequence_no ASC, artifact_note_link_id ASC",
        &[&artifact_id],
    )
}

fn insert_artifact_link(
    tx: &rusqlite::Transaction<'_>,
    source_artifact_id: &str,
    target_artifact_id: &str,
    link_type: ArtifactLinkType,
    link_value: &str,
) -> StorageResult<()> {
    tx.execute(
        "INSERT OR IGNORE INTO oa_artifact_link
         (artifact_link_id, source_artifact_id, target_artifact_id, link_type, link_value)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            next_id("artlink"),
            source_artifact_id,
            target_artifact_id,
            link_type.as_str(),
            link_value
        ],
    )
    .map_err(|source| StorageError::InsertArtifactLink {
        source_artifact_id: source_artifact_id.to_string(),
        target_artifact_id: target_artifact_id.to_string(),
        link_type: link_type.as_str().to_string(),
        source: Box::new(source),
    })?;
    Ok(())
}

fn sync_structural_links_for_artifact(
    tx: &rusqlite::Transaction<'_>,
    artifact_id: &str,
) -> StorageResult<()> {
    tx.execute(
        "UPDATE oa_artifact_note_link
         SET resolved_artifact_id = (
                SELECT a.artifact_id
                FROM oa_artifact a
                WHERE a.source_type = 'obsidian_vault'
                  AND a.source_conversation_key = oa_artifact_note_link.target_path
                LIMIT 1
             ),
             resolution_status = 'resolved'
         WHERE resolved_artifact_id IS NULL
           AND resolution_status = 'unresolved'
           AND target_kind IN ('note', 'heading', 'block')
           AND artifact_id <> ?1
           AND EXISTS (
                SELECT 1
                FROM oa_artifact a
                WHERE a.source_type = 'obsidian_vault'
                  AND a.source_conversation_key = oa_artifact_note_link.target_path
           )",
        [artifact_id],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!("failed to resolve note links by path for {artifact_id}: {source}"),
    })?;

    tx.execute(
        "UPDATE oa_artifact_note_link
         SET resolved_artifact_id = (
                SELECT a.artifact_id
                FROM oa_artifact_note_alias aa
                JOIN oa_artifact a ON a.artifact_id = aa.artifact_id
                WHERE aa.normalized_alias = oa_artifact_note_link.normalized_target
                GROUP BY aa.normalized_alias
                HAVING COUNT(*) = 1
             ),
             resolution_status = 'resolved'
         WHERE resolved_artifact_id IS NULL
           AND resolution_status = 'unresolved'
           AND target_kind = 'note'
           AND artifact_id <> ?1",
        [artifact_id],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!("failed to resolve note links by alias for {artifact_id}: {source}"),
    })?;

    let mut wikilinks_stmt = tx
        .prepare(
            "SELECT DISTINCT artifact_id, resolved_artifact_id,
                    COALESCE(target_path, normalized_target, raw_target)
             FROM oa_artifact_note_link
             WHERE resolved_artifact_id IS NOT NULL
               AND artifact_id <> resolved_artifact_id
               AND (artifact_id = ?1 OR resolved_artifact_id = ?1)",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare wikilink sync query: {source}"),
        })?;
    let wikilinks = wikilinks_stmt
        .query_map([artifact_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load wikilink edges: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map wikilink edges: {source}"),
        })?;
    for (source_artifact_id, target_artifact_id, link_value) in wikilinks {
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::Wikilink,
            &link_value,
        )?;
    }

    let mut tag_stmt = tx
        .prepare(
            "SELECT DISTINCT t1.artifact_id, t2.artifact_id, t1.normalized_tag
             FROM oa_artifact_note_tag t1
             JOIN oa_artifact_note_tag t2
               ON t2.normalized_tag = t1.normalized_tag
              AND t2.artifact_id <> t1.artifact_id
             WHERE t1.artifact_id = ?1",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare shared-tag sync query: {source}"),
        })?;
    let tag_edges = tag_stmt
        .query_map([artifact_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load shared-tag edges: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map shared-tag edges: {source}"),
        })?;
    for (source_artifact_id, target_artifact_id, link_value) in tag_edges {
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::SharedTag,
            &link_value,
        )?;
    }

    let mut alias_stmt = tx
        .prepare(
            "SELECT DISTINCT a1.artifact_id, a2.artifact_id, a1.normalized_alias
             FROM oa_artifact_note_alias a1
             JOIN oa_artifact_note_alias a2
               ON a2.normalized_alias = a1.normalized_alias
              AND a2.artifact_id <> a1.artifact_id
             WHERE a1.artifact_id = ?1",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare alias sync query: {source}"),
        })?;
    let alias_edges = alias_stmt
        .query_map([artifact_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load alias edges: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map alias edges: {source}"),
        })?;
    for (source_artifact_id, target_artifact_id, link_value) in alias_edges {
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::Alias,
            &link_value,
        )?;
    }

    Ok(())
}

fn sync_reconcile_links_for_archive_links(
    tx: &rusqlite::Transaction<'_>,
    archive_links: &[NewArchiveLink],
) -> StorageResult<()> {
    for archive_link in archive_links {
        let row = tx
            .query_row(
                "SELECT src.artifact_id, tgt.artifact_id, src.derived_object_type,
                        COALESCE(NULLIF(src.title, ''), NULLIF(src.candidate_key, ''), src.derived_object_id)
                 FROM oa_derived_object src
                 JOIN oa_derived_object tgt ON tgt.derived_object_id = ?2
                 WHERE src.derived_object_id = ?1",
                params![archive_link.source_object_id, archive_link.target_object_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| StorageError::InvalidDerivationWrite {
                detail: format!("failed to map archive-link artifact edge: {source}"),
            })?;

        let Some((source_artifact_id, target_artifact_id, derived_object_type, label)) = row else {
            continue;
        };
        if source_artifact_id == target_artifact_id {
            continue;
        }
        let link_value = format!(
            "{}:{}:{}",
            archive_link.link_type, derived_object_type, label
        );
        insert_artifact_link(
            tx,
            &source_artifact_id,
            &target_artifact_id,
            ArtifactLinkType::ReconciledObject,
            &link_value,
        )?;
    }
    Ok(())
}

fn load_artifact_links(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactLinkRecord>> {
    let mut stmt = connection
        .prepare(
            "SELECT al.artifact_link_id,
                    al.source_artifact_id,
                    src.title,
                    src.source_conversation_key,
                    al.target_artifact_id,
                    tgt.title,
                    tgt.source_conversation_key,
                    al.link_type,
                    al.link_value
             FROM oa_artifact_link al
             JOIN oa_artifact src ON src.artifact_id = al.source_artifact_id
             JOIN oa_artifact tgt ON tgt.artifact_id = al.target_artifact_id
             WHERE al.source_artifact_id = ?1 OR al.target_artifact_id = ?1
             ORDER BY al.link_type ASC, al.link_value ASC, al.source_artifact_id ASC, al.target_artifact_id ASC, al.artifact_link_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare artifact links query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            Ok(ArtifactLinkRecord {
                artifact_link_id: row.get(0)?,
                source_artifact_id: row.get(1)?,
                source_title: row.get(2)?,
                source_note_path: row.get(3)?,
                target_artifact_id: row.get(4)?,
                target_title: row.get(5)?,
                target_note_path: row.get(6)?,
                link_type: parse_artifact_link_type(row.get(7)?)?,
                link_value: row.get(8)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact links: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map artifact links: {source}"),
        })
}

impl ImportWriteStore for SqliteImportWriteStore {
    fn write_import(&self, mut import_set: WriteImportSet) -> StorageResult<ImportWriteResult> {
        let mut connection = open_connection(&self.config)?;

        {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            let existing_object_id: Option<String> = tx
                .query_row(
                    "SELECT object_id FROM oa_object_ref WHERE sha256 = ?1 AND object_kind = 'import_payload'",
                    [&import_set.payload_object.sha256],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|source| db_err(&self.config, source))?;

            if let Some(existing_object_id) = existing_object_id {
                import_set.import.payload_object_id = existing_object_id;
            } else {
                tx.execute(
                    "INSERT INTO oa_object_ref
                     (object_id, object_kind, storage_provider, storage_key, mime_type, size_bytes, sha256)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        import_set.payload_object.object_id,
                        "import_payload",
                        import_set.payload_object.stored_object.provider,
                        import_set.payload_object.stored_object.storage_key,
                        import_set.payload_object.stored_object.mime_type,
                        import_set.payload_object.size_bytes,
                        import_set.payload_object.sha256
                    ],
                )
                .map_err(|source| StorageError::InsertPayload {
                    payload_id: import_set.payload_object.object_id.clone(),
                    source: Box::new(source),
                })?;
            }

            tx.execute(
                "INSERT INTO oa_import
                 (import_id, source_type, import_status, payload_object_id, source_filename, source_content_hash, conversation_count_detected)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    import_set.import.import_id,
                    import_set.import.source_type.as_str(),
                    import_set.import.import_status.as_str(),
                    import_set.import.payload_object_id,
                    import_set.import.source_filename,
                    import_set.import.source_content_hash,
                    import_set.import.conversation_count_detected
                ],
            )
            .map_err(|source| StorageError::InsertImport {
                import_id: import_set.import.import_id.clone(),
                source: Box::new(source),
            })?;
            tx.commit().map_err(|source| StorageError::InsertImport {
                import_id: import_set.import.import_id.clone(),
                source: Box::new(source),
            })?;
        }

        let import_id = import_set.import.import_id.clone();
        let mut artifacts = Vec::with_capacity(import_set.artifact_sets.len());
        let mut failed_artifact_ids = Vec::new();
        let mut segments_written = 0usize;
        let mut count_imported = 0i64;
        let mut count_failed = 0i64;
        let mut artifact_errors = Vec::new();
        let mut planned_to_actual_artifact_ids = std::collections::HashMap::new();
        let mut deferred_links = Vec::new();
        let mut created_artifact_ids = Vec::new();

        for artifact_set in import_set.artifact_sets {
            let planned_artifact_id = artifact_set.artifact.artifact_id.clone();
            let existing = connection
                .query_row(
                    "SELECT artifact_id, enrichment_status
                     FROM oa_artifact
                     WHERE source_type = ?1
                       AND content_hash_version = ?2
                       AND source_conversation_hash = ?3",
                    params![
                        artifact_set.artifact.source_type.as_str(),
                        artifact_set.artifact.content_hash_version,
                        artifact_set.artifact.source_conversation_hash
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            parse_enrichment_status(row.get(1)?)?,
                        ))
                    },
                )
                .optional()
                .map_err(|source| db_err(&self.config, source))?;
            if let Some((existing_artifact_id, enrichment_status)) = existing {
                planned_to_actual_artifact_ids
                    .insert(planned_artifact_id.clone(), existing_artifact_id.clone());
                artifacts.push(ImportedArtifact {
                    artifact_id: existing_artifact_id,
                    enrichment_status,
                    ingest_result: ArtifactIngestResult::AlreadyExists,
                });
                continue;
            }

            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            let phase_result = (|| -> StorageResult<usize> {
                tx.execute(
                    "INSERT INTO oa_artifact
                     (artifact_id, import_id, artifact_class, source_type, artifact_status, enrichment_status,
                      source_conversation_key, source_conversation_hash, title, created_at_source,
                      started_at, ended_at, primary_language, content_hash_version, content_facets_json,
                      normalization_version, error_message)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                    params![
                        artifact_set.artifact.artifact_id,
                        artifact_set.artifact.import_id,
                        artifact_set.artifact.artifact_class.as_str(),
                        artifact_set.artifact.source_type.as_str(),
                        artifact_set.artifact.artifact_status.as_str(),
                        artifact_set.artifact.enrichment_status.as_str(),
                        artifact_set.artifact.source_conversation_key,
                        artifact_set.artifact.source_conversation_hash,
                        artifact_set.artifact.title,
                        artifact_set
                            .artifact
                            .created_at_source
                            .as_ref()
                            .map(SourceTimestamp::as_str),
                        artifact_set
                            .artifact
                            .started_at
                            .as_ref()
                            .map(SourceTimestamp::as_str),
                        artifact_set
                            .artifact
                            .ended_at
                            .as_ref()
                            .map(SourceTimestamp::as_str),
                        artifact_set.artifact.primary_language,
                        artifact_set.artifact.content_hash_version,
                        artifact_set.artifact.content_facets_json,
                        artifact_set.artifact.normalization_version,
                        Option::<&str>::None
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;

                for participant in &artifact_set.participants {
                    tx.execute(
                        "INSERT INTO oa_conversation_participant
                         (participant_id, artifact_id, participant_role, display_name, provider_name, model_name, source_participant_key, sequence_no)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            participant.participant_id,
                            participant.artifact_id,
                            participant.participant_role.as_str(),
                            participant.display_name,
                            participant.provider_name,
                            participant.model_name,
                            participant.source_participant_key,
                            participant.sequence_no
                        ],
                    )
                    .map_err(|source| db_err(&self.config, source))?;
                }

                let mut seg_count = 0usize;
                for segment in &artifact_set.segments {
                    tx.execute(
                        "INSERT INTO oa_segment
                         (segment_id, artifact_id, participant_id, segment_type, source_segment_key, parent_segment_id,
                          sequence_no, created_at_source, text_content, text_content_hash, locator_json,
                          visibility_status, unsupported_content_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                        params![
                            segment.segment_id,
                            segment.artifact_id,
                            segment.participant_id,
                            segment.segment_type.as_str(),
                            segment.source_segment_key,
                            segment.parent_segment_id,
                            segment.sequence_no,
                            segment.created_at_source.as_ref().map(SourceTimestamp::as_str),
                            segment.text_content,
                            segment.text_content_hash,
                            segment.locator_json,
                            segment.visibility_status.as_str(),
                            segment.unsupported_content_json
                        ],
                    )
                    .map_err(|source| db_err(&self.config, source))?;
                    seg_count += 1;
                }

                for property in &artifact_set.imported_note_metadata.properties {
                    insert_imported_note_property(&tx, property)?;
                }
                for tag in &artifact_set.imported_note_metadata.tags {
                    insert_imported_note_tag(&tx, tag)?;
                }
                for alias in &artifact_set.imported_note_metadata.aliases {
                    insert_imported_note_alias(&tx, alias)?;
                }

                tx.execute(
                    "INSERT INTO oa_enrichment_job
                     (job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, job_status,
                      max_attempts, priority_no, required_capabilities, payload_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        artifact_set.job.job_id,
                        artifact_set.job.artifact_id,
                        artifact_set.job.job_type.as_str(),
                        artifact_set.job.enrichment_tier.as_str(),
                        artifact_set.job.spawned_by_job_id,
                        artifact_set.job.job_status.as_str(),
                        artifact_set.job.max_attempts,
                        artifact_set.job.priority_no,
                        serde_json::to_string(&artifact_set.job.required_capabilities)
                            .expect("required capabilities serializable"),
                        artifact_set.job.payload_json
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;

                Ok(seg_count)
            })();

            match phase_result {
                Ok(seg_count) => {
                    tx.commit().map_err(|source| db_err(&self.config, source))?;
                    planned_to_actual_artifact_ids
                        .insert(planned_artifact_id.clone(), planned_artifact_id.clone());
                    deferred_links.push((
                        planned_artifact_id.clone(),
                        artifact_set.imported_note_metadata.links,
                    ));
                    created_artifact_ids.push(planned_artifact_id.clone());
                    artifacts.push(ImportedArtifact {
                        artifact_id: planned_artifact_id.clone(),
                        enrichment_status: artifact_set.artifact.enrichment_status,
                        ingest_result: ArtifactIngestResult::Created,
                    });
                    segments_written += seg_count;
                    count_imported += 1;
                }
                Err(error) => {
                    failed_artifact_ids.push(planned_artifact_id.clone());
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {error:#}", planned_artifact_id));
                }
            }
        }

        for (artifact_id, links) in deferred_links {
            if links.is_empty() {
                continue;
            }
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            let result = (|| -> StorageResult<()> {
                for link in &links {
                    let resolved_artifact_id = link
                        .resolved_artifact_id
                        .as_ref()
                        .and_then(|planned_id| planned_to_actual_artifact_ids.get(planned_id))
                        .cloned();
                    let mut remapped = link.clone();
                    remapped.resolved_artifact_id = resolved_artifact_id;
                    insert_imported_note_link(&tx, &remapped)?;
                }
                Ok(())
            })();
            match result {
                Ok(()) => {
                    tx.commit().map_err(|source| db_err(&self.config, source))?;
                }
                Err(error) => {
                    count_failed += 1;
                    artifact_errors.push(format!("artifact {}: {error:#}", artifact_id));
                }
            }
        }

        for artifact_id in &created_artifact_ids {
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            match sync_structural_links_for_artifact(&tx, artifact_id) {
                Ok(()) => {
                    tx.commit().map_err(|source| db_err(&self.config, source))?;
                }
                Err(error) => {
                    count_failed += 1;
                    artifact_errors.push(format!(
                        "artifact {} structural links: {error:#}",
                        artifact_id
                    ));
                }
            }
        }

        let import_status = if count_failed == 0 {
            crate::storage::types::ImportStatus::Completed
        } else if count_imported == 0 {
            crate::storage::types::ImportStatus::Failed
        } else {
            crate::storage::types::ImportStatus::CompletedWithErrors
        };
        let error_message = if artifact_errors.is_empty() {
            None
        } else {
            Some(artifact_errors.join(" | "))
        };

        connection
            .execute(
                "UPDATE oa_import
                 SET conversation_count_imported = ?2,
                     conversation_count_failed = ?3,
                     import_status = ?4,
                     error_message = ?5,
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                 WHERE import_id = ?1",
                params![
                    import_id,
                    count_imported,
                    count_failed,
                    import_status.as_str(),
                    error_message
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;

        Ok(ImportWriteResult {
            import_id,
            import_status,
            artifacts,
            failed_artifact_ids,
            segments_written,
        })
    }
}

impl ArtifactReadStore for SqliteArtifactReadStore {
    fn list_artifacts(&self) -> StorageResult<Vec<ArtifactListItem>> {
        self.list_artifacts_filtered(&ArtifactListFilters::default(), 1000, 0)
    }

    fn list_artifacts_filtered(
        &self,
        filters: &ArtifactListFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<ArtifactListItem>> {
        let connection = open_connection(&self.config)?;
        let mut conditions = vec!["1=1".to_string()];
        let mut values: Vec<String> = Vec::new();
        if let Some(source_type) = filters.source_type {
            conditions.push("source_type = ?".to_string());
            values.push(source_type.as_str().to_string());
        }
        if let Some(status) = filters.enrichment_status {
            conditions.push("enrichment_status = ?".to_string());
            values.push(status.as_str().to_string());
        }
        if let Some(tag) = filters.tag.as_ref() {
            conditions.push(
                "EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = oa_artifact.artifact_id AND t.normalized_tag = ?)"
                    .to_string(),
            );
            values.push(tag.to_lowercase());
        }
        if let Some(alias) = filters.alias.as_ref() {
            conditions.push(
                "EXISTS (SELECT 1 FROM oa_artifact_note_alias a2 WHERE a2.artifact_id = oa_artifact.artifact_id AND a2.normalized_alias = ?)"
                    .to_string(),
            );
            values.push(alias.to_lowercase());
        }
        if let Some(path_prefix) = filters.path_prefix.as_ref() {
            conditions.push("lower(coalesce(source_conversation_key, '')) LIKE ?".to_string());
            values.push(format!("{}%", path_prefix.to_lowercase()));
        }
        if let Some(captured_after) = filters.captured_after.as_ref() {
            conditions.push("captured_at >= ?".to_string());
            values.push(captured_after.clone());
        }
        if let Some(captured_before) = filters.captured_before.as_ref() {
            conditions.push("captured_at < ?".to_string());
            values.push(captured_before.clone());
        }
        values.push(limit.to_string());
        values.push(offset.to_string());

        let sql = format!(
            "SELECT artifact_id, title, source_conversation_key, source_type, created_at_source, captured_at, enrichment_status
             FROM oa_artifact
             WHERE {}
             ORDER BY captured_at DESC, artifact_id ASC
             LIMIT ? OFFSET ?",
            conditions.join(" AND ")
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), |row| {
                Ok(ArtifactListItem {
                    artifact_id: row.get(0)?,
                    title: row.get(1)?,
                    note_path: row.get(2)?,
                    source_type: row.get(3)?,
                    created_at_source: row.get(4)?,
                    captured_at: row.get(5)?,
                    enrichment_status: parse_enrichment_status(row.get(6)?)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn get_timeline(
        &self,
        filters: &TimelineFilters,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<TimelineEntry>> {
        let connection = open_connection(&self.config)?;
        let mut conditions = vec!["1=1".to_string()];
        let mut values: Vec<String> = Vec::new();
        if let Some(keyword) = filters.keyword.as_ref() {
            conditions.push("lower(coalesce(a.title, '')) LIKE ?".to_string());
            values.push(format!("%{}%", keyword.to_lowercase()));
        }
        if let Some(source_type) = filters.source_type {
            conditions.push("a.source_type = ?".to_string());
            values.push(source_type.as_str().to_string());
        }
        if let Some(tag) = filters.tag.as_ref() {
            conditions.push(
                "EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = a.artifact_id AND t.normalized_tag = ?)"
                    .to_string(),
            );
            values.push(tag.to_lowercase());
        }
        if let Some(path_prefix) = filters.path_prefix.as_ref() {
            conditions.push("lower(coalesce(a.source_conversation_key, '')) LIKE ?".to_string());
            values.push(format!("{}%", path_prefix.to_lowercase()));
        }
        values.push(limit.to_string());
        values.push(offset.to_string());
        let sql = format!(
            "SELECT a.artifact_id, a.title, a.source_conversation_key, a.source_type,
                    a.created_at_source, a.captured_at, a.enrichment_status,
                    (SELECT d.body_text FROM oa_derived_object d
                     WHERE d.artifact_id = a.artifact_id
                       AND d.derived_object_type = 'summary'
                       AND d.object_status = 'active'
                     ORDER BY COALESCE(d.confidence_score, 0.0) DESC, d.derived_object_id ASC
                     LIMIT 1)
             FROM oa_artifact a
             WHERE {}
             ORDER BY COALESCE(a.created_at_source, a.captured_at) ASC, a.artifact_id ASC
             LIMIT ? OFFSET ?",
            conditions.join(" AND ")
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), |row| {
                Ok(TimelineEntry {
                    artifact_id: row.get(0)?,
                    title: row.get(1)?,
                    note_path: row.get(2)?,
                    source_type: row.get(3)?,
                    created_at_source: row.get(4)?,
                    captured_at: row.get(5)?,
                    enrichment_status: row.get(6)?,
                    summary_snippet: row.get(7)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn load_artifact_for_enrichment(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
        let connection = open_connection(&self.config)?;
        load_artifact_for_enrichment_record(&connection, artifact_id)
    }
}

fn load_artifact_for_enrichment_record(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Option<LoadedArtifactForEnrichment>> {
    let artifact = connection
        .query_row(
            "SELECT artifact_id, import_id, artifact_class, source_type, title, source_conversation_key
             FROM oa_artifact
             WHERE artifact_id = ?1",
            [artifact_id],
            |row| {
                Ok((
                    LoadedArtifactRecord {
                        artifact_id: row.get(0)?,
                        import_id: row.get(1)?,
                        artifact_class: parse_artifact_class(row.get(2)?)?,
                        source_type: parse_source_type(row.get(3)?)?,
                        title: row.get(4)?,
                    },
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact for enrichment {artifact_id}: {source}"),
        })?;
    let Some((artifact, note_path)) = artifact else {
        return Ok(None);
    };

    let mut participant_stmt = connection
        .prepare(
            "SELECT participant_id, participant_role, display_name, source_participant_key
             FROM oa_conversation_participant
             WHERE artifact_id = ?1
             ORDER BY sequence_no ASC, participant_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare participant load query: {source}"),
        })?;
    let participants = participant_stmt
        .query_map([artifact_id], |row| {
            Ok(LoadedParticipant {
                participant_id: row.get(0)?,
                participant_role: parse_participant_role(row.get(1)?)?,
                display_name: row.get(2)?,
                external_id: row.get(3)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load participants: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map participants: {source}"),
        })?;

    let mut segment_stmt = connection
        .prepare(
            "SELECT s.segment_id, s.participant_id, p.participant_role, s.sequence_no, s.text_content,
                    s.created_at_source, s.visibility_status
             FROM oa_segment s
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id
             WHERE s.artifact_id = ?1
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare segment load query: {source}"),
        })?;
    let segments = segment_stmt
        .query_map([artifact_id], |row| {
            Ok(LoadedSegment {
                segment_id: row.get(0)?,
                participant_id: row.get(1)?,
                participant_role: match row.get::<_, Option<String>>(2)? {
                    Some(value) => Some(parse_participant_role(value)?),
                    None => None,
                },
                sequence_no: row.get(3)?,
                text_content: row.get(4)?,
                created_at_source: from_sqlite_timestamp(row.get(5)?),
                visibility_status: parse_visibility_status(row.get(6)?)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load segments: {source}"),
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map segments: {source}"),
        })?;

    Ok(Some(LoadedArtifactForEnrichment {
        artifact,
        participants,
        segments,
        imported_note_metadata: load_imported_note_metadata(connection, artifact_id, note_path)?,
    }))
}

/// FTS5 query mode. `Plain` rewrites a bag-of-words query into an OR of quoted
/// tokens so that adding terms broadens results instead of eliminating them
/// (matching the spirit of Postgres `plainto_tsquery` rewritten with `|`).
/// `Operator` passes the user query straight through to FTS5 so power users can
/// write phrase queries (`"exact phrase"`), explicit operators (`AND`/`OR`/`NOT`),
/// `NEAR(...)`, prefix tokens (`foo*`), and column filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteSearchQueryMode {
    Plain,
    Operator,
}

fn detect_search_query_mode(query: &str) -> SqliteSearchQueryMode {
    let trimmed = query.trim();
    // Only route to Operator mode when the query looks intentionally written
    // for FTS5 syntax. Stray punctuation in a casual query (an unbalanced `"`
    // from a half-finished phrase, a lone `(`, etc.) would otherwise crash
    // FTS5 parsing — Plain mode escapes those characters safely.
    let quote_count = trimmed.chars().filter(|c| *c == '"').count();
    let open_paren_count = trimmed.chars().filter(|c| *c == '(').count();
    let close_paren_count = trimmed.chars().filter(|c| *c == ')').count();
    let has_quoted_phrase = quote_count >= 2
        && quote_count % 2 == 0
        && trimmed.starts_with('"')
        && trimmed.ends_with('"');
    let has_balanced_parens = open_paren_count > 0 && open_paren_count == close_paren_count;
    let has_prefix = trimmed.split_whitespace().any(|tok| {
        // Trailing `*` on a bare token is FTS5 prefix syntax. Ignore standalone
        // `*` and any token containing other punctuation we already escape.
        let bytes = tok.as_bytes();
        bytes.len() >= 2
            && bytes[bytes.len() - 1] == b'*'
            && bytes[..bytes.len() - 1]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    });
    let has_operator = trimmed
        .split_whitespace()
        .any(|tok| matches!(tok, "AND" | "OR" | "NOT" | "NEAR"));
    if has_quoted_phrase || has_balanced_parens || has_prefix || has_operator {
        SqliteSearchQueryMode::Operator
    } else {
        SqliteSearchQueryMode::Plain
    }
}

/// Build an FTS5 MATCH expression from a free-text query. Returns `None` when
/// the query reduces to no usable tokens.
///
/// In `Plain` mode each whitespace-separated token is wrapped in double quotes
/// (with embedded `"` escaped as `""`) and joined with ` OR `. Quoting disables
/// FTS5 operator parsing, so punctuation in user input is treated as a plain
/// phrase rather than a syntax error.
///
/// In `Operator` mode the query is forwarded verbatim.
fn build_fts5_match_expression(query: &str) -> Option<String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    match detect_search_query_mode(trimmed) {
        SqliteSearchQueryMode::Operator => Some(trimmed.to_string()),
        SqliteSearchQueryMode::Plain => {
            let tokens: Vec<String> = trimmed
                .split_whitespace()
                .filter(|tok| !tok.is_empty())
                .map(|tok| format!("\"{}\"", tok.replace('"', "\"\"")))
                .collect();
            if tokens.is_empty() {
                None
            } else {
                Some(tokens.join(" OR "))
            }
        }
    }
}

/// Convert FTS5 `bm25()` (smaller / more negative is better) into a non-negative
/// integer bonus to add on top of a `score_hint` base. Mirrors the
/// `base + GREATEST(0, FLOOR(rank * scale))` shape used by the Postgres path.
fn fts5_bm25_score_bonus(bm25: f64) -> i32 {
    let scaled = (-bm25) * 10.0;
    if !scaled.is_finite() || scaled <= 0.0 {
        0
    } else {
        scaled.round().min(i32::MAX as f64) as i32
    }
}

fn artifact_filter_conditions(
    filters: &SearchFilters,
    artifact_alias: &str,
    values: &mut Vec<String>,
) -> String {
    let mut conditions = Vec::new();
    if let Some(source_type) = filters.source_type {
        conditions.push(format!("{artifact_alias}.source_type = ?"));
        values.push(source_type.as_str().to_string());
    }
    if let Some(tag) = filters.tag.as_ref() {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM oa_artifact_note_tag t WHERE t.artifact_id = {artifact_alias}.artifact_id AND t.normalized_tag = ?)"
        ));
        values.push(tag.to_lowercase());
    }
    if let Some(alias) = filters.alias.as_ref() {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM oa_artifact_note_alias na WHERE na.artifact_id = {artifact_alias}.artifact_id AND na.normalized_alias = ?)"
        ));
        values.push(alias.to_lowercase());
    }
    if let Some(path_prefix) = filters.path_prefix.as_ref() {
        conditions.push(format!(
            "lower(coalesce({artifact_alias}.source_conversation_key, '')) LIKE ?"
        ));
        values.push(format!("{}%", path_prefix.to_lowercase()));
    }
    if conditions.is_empty() {
        "1=1".to_string()
    } else {
        conditions.join(" AND ")
    }
}

fn load_artifact_record(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Option<ArtifactDetailRecord>> {
    connection
        .query_row(
            "SELECT artifact_id, title, source_type, enrichment_status, source_conversation_key
             FROM oa_artifact
             WHERE artifact_id = ?1",
            [artifact_id],
            |row| {
                Ok(ArtifactDetailRecord {
                    artifact_id: row.get(0)?,
                    title: row.get(1)?,
                    source_type: parse_source_type(row.get(2)?)?,
                    enrichment_status: parse_enrichment_status(row.get(3)?)?,
                    note_path: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact record {artifact_id}: {source}"),
        })
}

fn load_artifact_segments(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactDetailSegment>> {
    let mut stmt = connection
        .prepare(
            "SELECT s.segment_id, s.participant_id, p.participant_role, s.sequence_no, s.text_content
             FROM oa_segment s
             LEFT JOIN oa_conversation_participant p ON p.participant_id = s.participant_id
             WHERE s.artifact_id = ?1
             ORDER BY s.sequence_no ASC, s.segment_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare artifact segments query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            let participant_id: Option<String> = row.get(1)?;
            Ok(ArtifactDetailSegment {
                segment_id: row.get(0)?,
                participant_id: participant_id.clone(),
                participant_role: row
                    .get::<_, Option<String>>(2)?
                    .map(parse_participant_role)
                    .transpose()?,
                sequence_no: row.get(3)?,
                text_content: row.get(4)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact segments: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map artifact segments: {source}"),
        })
}

fn load_artifact_detail_derived_objects(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactDetailDerivedObject>> {
    let mut stmt = connection
        .prepare(
            "SELECT derived_object_id, derived_object_type, title, body_text, confidence_score
             FROM oa_derived_object
             WHERE artifact_id = ?1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare artifact derived objects query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            Ok(ArtifactDetailDerivedObject {
                derived_object_id: row.get(0)?,
                derived_object_type: parse_derived_object_type(row.get(1)?)?,
                title: row.get(2)?,
                body_text: row.get(3)?,
                confidence_score: row.get(4)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load artifact derived objects: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map artifact derived objects: {source}"),
        })
}

fn load_artifact_context_derived_objects(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<Vec<ArtifactContextDerivedObject>> {
    let mut stmt = connection
        .prepare(
            "SELECT derived_object_id, derived_object_type, title, body_text, scope_id, scope_type, candidate_key
             FROM oa_derived_object
             WHERE artifact_id = ?1 AND object_status = 'active'
             ORDER BY derived_object_type ASC, derived_object_id ASC",
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to prepare context derived objects query: {source}"),
        })?;
    let rows = stmt
        .query_map([artifact_id], |row| {
            Ok(ArtifactContextDerivedObject {
                derived_object_id: row.get(0)?,
                derived_object_type: parse_derived_object_type(row.get(1)?)?,
                title: row.get(2)?,
                body_text: row.get(3)?,
                scope_id: row.get(4)?,
                scope_type: parse_scope_type(row.get(5)?)?,
                candidate_key: row.get(6)?,
            })
        })
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to load context derived objects: {source}"),
        })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to map context derived objects: {source}"),
        })
}

impl ArchiveRetrievalStore for SqliteRetrievalReadStore {
    fn retrieve_for_intents(
        &self,
        artifact_id: &str,
        intents: &[RetrievalIntent],
        limit_per_intent: usize,
    ) -> StorageResult<Vec<RetrievedContextItem>> {
        let connection = open_connection(&self.config)?;
        let mut items = Vec::new();
        for intent in intents {
            let query_text = intent.query_text.trim().to_lowercase();
            if query_text.is_empty() {
                continue;
            }
            let pattern = format!("%{query_text}%");
            let derived_type_filter = match intent.intent_type.as_str() {
                "memory_match" => Some("memory"),
                "relationship_lookup" => Some("relationship"),
                "topic_lookup" => None,
                "contradiction_check" => None,
                "entity_lookup" => None,
                _ => None,
            };
            let mut stmt = connection
                .prepare(
                    "SELECT derived_object_type, derived_object_id, artifact_id, title, body_text,
                            CASE
                                WHEN lower(coalesce(title, '')) LIKE ?2 THEN 'title'
                                ELSE 'body_text'
                            END
                     FROM oa_derived_object
                     WHERE artifact_id <> ?1
                       AND object_status = 'active'
                       AND (?3 IS NULL OR derived_object_type = ?3)
                       AND (
                            lower(coalesce(title, '')) LIKE ?2 OR
                            lower(coalesce(body_text, '')) LIKE ?2
                       )
                     ORDER BY confidence_score DESC, derived_object_id ASC
                     LIMIT ?4",
                )
                .map_err(|source| StorageError::InvalidDerivationWrite {
                    detail: format!("failed to prepare retrieval query: {source}"),
                })?;
            let rows = stmt
                .query_map(
                    params![
                        artifact_id,
                        pattern,
                        derived_type_filter,
                        limit_per_intent as i64
                    ],
                    |row| {
                        Ok(RetrievedContextItem {
                            item_type: row.get(0)?,
                            object_id: row.get(1)?,
                            artifact_id: row.get(2)?,
                            title: row.get(3)?,
                            body_text: row.get(4)?,
                            supporting_segment_ids: Vec::new(),
                            retrieval_reason: intent.question.clone(),
                            matched_fields: vec![row.get(5)?],
                            rank_score: 1000,
                        })
                    },
                )
                .map_err(|source| StorageError::InvalidDerivationWrite {
                    detail: format!("failed to load retrieval items: {source}"),
                })?;
            items.extend(rows.collect::<Result<Vec<_>, _>>().map_err(|source| {
                StorageError::InvalidDerivationWrite {
                    detail: format!("failed to map retrieval items: {source}"),
                }
            })?);
        }
        Ok(items)
    }
}

impl ArchiveSearchReadStore for SqliteRetrievalReadStore {
    fn search_candidates(
        &self,
        query_text: &str,
        limit: usize,
        filters: &SearchFilters,
    ) -> StorageResult<Vec<ArchiveSearchCandidate>> {
        let query_text = query_text.trim();
        if query_text.is_empty() {
            return Ok(Vec::new());
        }

        let connection = open_connection(&self.config)?;
        // Substring pattern is used by the lexical fallbacks for
        // tag / alias / note-path matches (those columns aren't FTS-indexed).
        let pattern = format!("%{}%", query_text.to_lowercase());
        let fts_match = build_fts5_match_expression(query_text);
        let mut candidates = Vec::new();

        if let Some(match_expr) = fts_match.as_ref() {
            let mut values: Vec<String> = vec![match_expr.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, a.title, bm25(oa_artifact_fts) AS rank_score
                 FROM oa_artifact_fts
                 JOIN oa_artifact a ON a.rowid = oa_artifact_fts.rowid
                 WHERE oa_artifact_fts MATCH ?1 AND {filter_sql}
                 ORDER BY rank_score ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    let bm25: f64 = row.get(2)?;
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(0)?,
                        match_kind: SearchCandidateKind::ArtifactTitle,
                        snippet: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        score_hint: 240 + fts5_bm25_score_bonus(bm25),
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            let mut values = vec![pattern.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, t.artifact_note_tag_id, t.raw_tag
                 FROM oa_artifact_note_tag t
                 JOIN oa_artifact a ON a.artifact_id = t.artifact_id
                 WHERE lower(t.raw_tag) LIKE ? AND {filter_sql}
                 ORDER BY t.sequence_no ASC, t.artifact_note_tag_id ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::ImportedNoteTag,
                        snippet: row.get(2)?,
                        score_hint: 380,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            let mut values = vec![pattern.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, na.artifact_note_alias_id, na.alias_text
                 FROM oa_artifact_note_alias na
                 JOIN oa_artifact a ON a.artifact_id = na.artifact_id
                 WHERE lower(na.alias_text) LIKE ? AND {filter_sql}
                 ORDER BY na.sequence_no ASC, na.artifact_note_alias_id ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::ImportedNoteAlias,
                        snippet: row.get(2)?,
                        score_hint: 340,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            let mut values = vec![pattern.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            values.push(limit.to_string());
            let sql = format!(
                "SELECT a.artifact_id, a.artifact_id, a.source_conversation_key
                 FROM oa_artifact a
                 WHERE lower(coalesce(a.source_conversation_key, '')) LIKE ? AND {filter_sql}
                 ORDER BY a.captured_at DESC, a.artifact_id ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::ImportedNotePath,
                        snippet: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        score_hint: 300,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if let Some(match_expr) = fts_match.as_ref() {
            let mut values: Vec<String> = vec![match_expr.clone()];
            let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
            if let Some(object_type) = filters.object_type {
                values.push(object_type.as_str().to_string());
            }
            values.push(limit.to_string());
            let object_filter = if filters.object_type.is_some() {
                "AND d.derived_object_type = ?"
            } else {
                ""
            };
            let sql = format!(
                "SELECT d.artifact_id, d.derived_object_id, d.derived_object_type,
                        COALESCE(d.title, d.body_text, ''),
                        bm25(oa_derived_object_fts) AS rank_score
                 FROM oa_derived_object_fts
                 JOIN oa_derived_object d ON d.rowid = oa_derived_object_fts.rowid
                 JOIN oa_artifact a ON a.artifact_id = d.artifact_id
                 WHERE oa_derived_object_fts MATCH ?1
                   AND d.object_status = 'active'
                   AND {filter_sql}
                   {object_filter}
                 ORDER BY rank_score ASC
                 LIMIT ?"
            );
            let mut stmt = connection
                .prepare(&sql)
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map(params_from_iter(values.iter()), |row| {
                    let bm25: f64 = row.get(4)?;
                    Ok(ArchiveSearchCandidate {
                        artifact_id: row.get(0)?,
                        match_record_id: row.get(1)?,
                        match_kind: SearchCandidateKind::DerivedObject {
                            derived_type: parse_derived_object_type(row.get(2)?)?,
                        },
                        snippet: row.get::<_, String>(3)?,
                        score_hint: 260 + fts5_bm25_score_bonus(bm25),
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if filters.object_type.is_none() {
            if let Some(match_expr) = fts_match.as_ref() {
                let mut values: Vec<String> = vec![match_expr.clone()];
                let filter_sql = artifact_filter_conditions(filters, "a", &mut values);
                values.push(limit.to_string());
                let sql = format!(
                    "SELECT s.artifact_id, s.segment_id,
                            substr(s.text_content, 1, 240),
                            bm25(oa_segment_fts) AS rank_score
                     FROM oa_segment_fts
                     JOIN oa_segment s ON s.rowid = oa_segment_fts.rowid
                     JOIN oa_artifact a ON a.artifact_id = s.artifact_id
                     WHERE oa_segment_fts MATCH ?1 AND {filter_sql}
                     ORDER BY rank_score ASC
                     LIMIT ?"
                );
                let mut stmt = connection
                    .prepare(&sql)
                    .map_err(|source| db_err(&self.config, source))?;
                let rows = stmt
                    .query_map(params_from_iter(values.iter()), |row| {
                        let bm25: f64 = row.get(3)?;
                        Ok(ArchiveSearchCandidate {
                            artifact_id: row.get(0)?,
                            match_record_id: row.get(1)?,
                            match_kind: SearchCandidateKind::SegmentExcerpt,
                            snippet: row.get(2)?,
                            score_hint: 200 + fts5_bm25_score_bonus(bm25),
                        })
                    })
                    .map_err(|source| db_err(&self.config, source))?;
                candidates.extend(
                    rows.collect::<Result<Vec<_>, _>>()
                        .map_err(|source| db_err(&self.config, source))?,
                );
            }
        }

        candidates.sort_by(|left, right| {
            right
                .score_hint
                .cmp(&left.score_hint)
                .then_with(|| left.artifact_id.cmp(&right.artifact_id))
                .then_with(|| left.match_record_id.cmp(&right.match_record_id))
        });
        candidates.dedup_by(|left, right| left.match_record_id == right.match_record_id);
        candidates.truncate(limit);
        Ok(candidates)
    }
}

impl ArtifactDetailReadStore for SqliteRetrievalReadStore {
    fn load_artifact_detail(&self, artifact_id: &str) -> StorageResult<Option<ArtifactDetailView>> {
        let connection = open_connection(&self.config)?;
        let Some(artifact) = load_artifact_record(&connection, artifact_id)? else {
            return Ok(None);
        };
        Ok(Some(ArtifactDetailView {
            imported_note_metadata: load_imported_note_metadata(
                &connection,
                artifact_id,
                artifact.note_path.clone(),
            )?,
            inbound_note_links: load_inbound_note_links(&connection, artifact_id)?,
            artifact_links: load_artifact_links(&connection, artifact_id)?,
            segments: load_artifact_segments(&connection, artifact_id)?,
            artifact,
            derived_objects: load_artifact_detail_derived_objects(&connection, artifact_id)?,
        }))
    }
}

impl ArtifactContextPackReadStore for SqliteRetrievalReadStore {
    fn load_artifact_context_pack_material(
        &self,
        artifact_id: &str,
    ) -> StorageResult<Option<ArtifactContextPackMaterial>> {
        let connection = open_connection(&self.config)?;
        let Some(artifact) = load_artifact_record(&connection, artifact_id)? else {
            return Ok(None);
        };
        Ok(Some(ArtifactContextPackMaterial {
            segments: load_artifact_segments(&connection, artifact_id)?,
            imported_note_metadata: load_imported_note_metadata(
                &connection,
                artifact_id,
                artifact.note_path.clone(),
            )?,
            inbound_note_links: load_inbound_note_links(&connection, artifact_id)?,
            artifact_links: load_artifact_links(&connection, artifact_id)?,
            derived_objects: load_artifact_context_derived_objects(&connection, artifact_id)?,
            artifact,
        }))
    }
}

impl DerivedObjectSearchStore for SqliteRetrievalReadStore {
    fn search_objects(
        &self,
        filters: &ObjectSearchFilters,
        limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        let connection = open_connection(&self.config)?;

        // If the caller provided a free-text query, rewrite it through the
        // shared FTS5 builder and rank with bm25. Otherwise fall back to a
        // plain confidence-ordered scan so callers that filter only by type /
        // candidate_key / artifact_id keep working.
        let normalized_query = filters
            .query
            .as_ref()
            .map(|q| q.trim())
            .filter(|q| !q.is_empty());
        let fts_match = normalized_query.and_then(build_fts5_match_expression);

        let mut values: Vec<String> = Vec::new();
        let mut conditions = vec!["d.object_status = 'active'".to_string()];

        let (from_clause, rank_select, order_clause) = if fts_match.is_some() {
            (
                "oa_derived_object_fts \
                 JOIN oa_derived_object d ON d.rowid = oa_derived_object_fts.rowid"
                    .to_string(),
                ", bm25(oa_derived_object_fts) AS rank_score".to_string(),
                "ORDER BY rank_score ASC".to_string(),
            )
        } else {
            (
                "oa_derived_object d".to_string(),
                ", NULL AS rank_score".to_string(),
                "ORDER BY COALESCE(d.confidence_score, 0.0) DESC, d.derived_object_id ASC"
                    .to_string(),
            )
        };

        if let Some(match_expr) = fts_match.as_ref() {
            conditions.push("oa_derived_object_fts MATCH ?".to_string());
            values.push(match_expr.clone());
        }
        if let Some(object_type) = filters.object_type {
            conditions.push("d.derived_object_type = ?".to_string());
            values.push(object_type.as_str().to_string());
        }
        if let Some(candidate_key) = filters.candidate_key.as_ref() {
            conditions.push("d.candidate_key = ?".to_string());
            values.push(candidate_key.clone());
        }
        if let Some(artifact_id) = filters.artifact_id.as_ref() {
            conditions.push("d.artifact_id = ?".to_string());
            values.push(artifact_id.clone());
        }
        values.push(limit.to_string());

        let sql = format!(
            "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type,
                    d.title, d.body_text, d.candidate_key, d.confidence_score
                    {rank_select}
             FROM {from_clause}
             WHERE {where_clause}
             {order_clause}
             LIMIT ?",
            rank_select = rank_select,
            from_clause = from_clause,
            where_clause = conditions.join(" AND "),
            order_clause = order_clause,
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), |row| {
                let bm25: Option<f64> = row.get(7)?;
                Ok(DerivedObjectSearchResult {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    candidate_key: row.get(5)?,
                    confidence_score: row.get(6)?,
                    // Surface a positive lexical relevance score (higher = better).
                    // `app::search::normalize_lexical_score` re-normalizes this
                    // before blending with semantic scores.
                    score: bm25.map(|value| (-value) as f32),
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn search_objects_by_embedding(
        &self,
        _filters: &ObjectSearchFilters,
        _query_embedding: &[f32],
        _limit: usize,
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        Err(StorageError::UnsupportedOperation {
            store: "sqlite",
            operation: "search_objects_by_embedding without external vector provider",
        })
    }

    fn get_related_objects(
        &self,
        derived_object_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<GraphRelatedEntry>> {
        let connection = open_connection(&self.config)?;
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        let mut archive_stmt = connection
            .prepare(
                "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type, d.title, d.body_text,
                        al.link_type, al.confidence_score, al.rationale
                 FROM oa_archive_link al
                 JOIN oa_derived_object d
                   ON d.derived_object_id = CASE
                        WHEN al.source_object_id = ?1 THEN al.target_object_id
                        ELSE al.source_object_id
                      END
                 WHERE (al.source_object_id = ?1 OR al.target_object_id = ?1)
                   AND d.object_status = 'active'
                 ORDER BY d.derived_object_id ASC
                 LIMIT ?2",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let archive_rows = archive_stmt
            .query_map(params![derived_object_id, limit as i64], |row| {
                Ok(GraphRelatedEntry {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    relation_kind: "archive_link".to_string(),
                    link_type: row.get(5)?,
                    confidence_score: row.get(6)?,
                    rationale: row.get(7)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        for row in archive_rows {
            let entry = row.map_err(|source| db_err(&self.config, source))?;
            if seen.insert(entry.derived_object_id.clone()) {
                results.push(entry);
            }
        }

        let remaining = limit.saturating_sub(results.len());
        if remaining > 0 {
            let mut candidate_stmt = connection
                .prepare(
                    "SELECT d.derived_object_id, d.artifact_id, d.derived_object_type, d.title, d.body_text
                     FROM oa_derived_object source
                     JOIN oa_derived_object d
                       ON d.candidate_key = source.candidate_key
                      AND d.derived_object_id <> source.derived_object_id
                     WHERE source.derived_object_id = ?1
                       AND COALESCE(source.candidate_key, '') <> ''
                       AND d.object_status = 'active'
                     ORDER BY d.derived_object_id ASC
                     LIMIT ?2",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let candidate_rows = candidate_stmt
                .query_map(params![derived_object_id, remaining as i64], |row| {
                    Ok(GraphRelatedEntry {
                        derived_object_id: row.get(0)?,
                        artifact_id: row.get(1)?,
                        derived_object_type: parse_derived_object_type(row.get(2)?)?,
                        title: row.get(3)?,
                        body_text: row.get(4)?,
                        relation_kind: "shared_candidate_key".to_string(),
                        link_type: None,
                        confidence_score: None,
                        rationale: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            for row in candidate_rows {
                let entry = row.map_err(|source| db_err(&self.config, source))?;
                if seen.insert(entry.derived_object_id.clone()) {
                    results.push(entry);
                    if results.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }
}

impl DerivedObjectLookupStore for SqliteRetrievalReadStore {
    fn load_active_objects_by_ids(
        &self,
        derived_object_ids: &[String],
    ) -> StorageResult<Vec<DerivedObjectSearchResult>> {
        if derived_object_ids.is_empty() {
            return Ok(Vec::new());
        }
        let connection = open_connection(&self.config)?;
        let placeholders = std::iter::repeat_n("?", derived_object_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT derived_object_id, artifact_id, derived_object_type, title, body_text, candidate_key, confidence_score
             FROM oa_derived_object
             WHERE object_status = 'active'
               AND derived_object_id IN ({placeholders})
             ORDER BY derived_object_id ASC"
        );
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(derived_object_ids.iter()), |row| {
                Ok(DerivedObjectSearchResult {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    candidate_key: row.get(5)?,
                    confidence_score: row.get(6)?,
                    score: None,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }
}

impl CrossArtifactReadStore for SqliteRetrievalReadStore {
    fn find_related_by_candidate_keys(
        &self,
        artifact_id: &str,
        candidate_keys: &[String],
        limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>> {
        if candidate_keys.is_empty() {
            return Ok(Vec::new());
        }
        let connection = open_connection(&self.config)?;
        let placeholders = std::iter::repeat_n("?", candidate_keys.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT derived_object_id, artifact_id, derived_object_type, title, body_text, candidate_key, confidence_score
             FROM oa_derived_object
             WHERE object_status = 'active'
               AND artifact_id <> ?
               AND candidate_key IN ({placeholders})
             ORDER BY candidate_key ASC, derived_object_id ASC
             LIMIT ?"
        );
        let mut params = Vec::with_capacity(candidate_keys.len() + 2);
        params.push(artifact_id.to_string());
        params.extend(candidate_keys.iter().cloned());
        params.push(limit.to_string());
        let mut stmt = connection
            .prepare(&sql)
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map(params_from_iter(params.iter()), |row| {
                Ok(RelatedDerivedObject {
                    derived_object_id: row.get(0)?,
                    artifact_id: row.get(1)?,
                    derived_object_type: parse_derived_object_type(row.get(2)?)?,
                    title: row.get(3)?,
                    body_text: row.get(4)?,
                    candidate_key: row.get(5)?,
                    confidence_score: row.get(6)?,
                })
            })
            .map_err(|source| db_err(&self.config, source))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_err(&self.config, source))
    }

    fn find_related_by_embedding(
        &self,
        _artifact_id: &str,
        _derived_object_type: DerivedObjectType,
        _query_embedding: &[f32],
        _limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>> {
        Err(StorageError::UnsupportedOperation {
            store: "sqlite",
            operation: "find_related_by_embedding without external vector provider",
        })
    }
}

impl ReviewReadStore for SqliteRetrievalReadStore {
    fn list_review_candidates(
        &self,
        filters: &ReviewQueueFilters,
        limit: usize,
    ) -> StorageResult<Vec<ReviewCandidate>> {
        let connection = open_connection(&self.config)?;
        let include_all = filters.kinds.as_ref().is_none_or(Vec::is_empty);
        let include_kind = |kind: ReviewItemKind| {
            include_all
                || filters
                    .kinds
                    .as_ref()
                    .is_some_and(|kinds| kinds.contains(&kind))
        };

        let mut candidates = Vec::new();

        if include_kind(ReviewItemKind::ArtifactNeedsAttention) {
            let mut stmt = connection.prepare(
                "SELECT artifact_id, source_type, captured_at, title, enrichment_status
                 FROM oa_artifact
                 WHERE enrichment_status IN ('partial', 'failed')
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:artifact_needs_attention:' || oa_artifact.artifact_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                 ORDER BY captured_at DESC, artifact_id ASC
                 LIMIT ?1",
            ).map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::ArtifactNeedsAttention,
                        artifact_id: row.get(0)?,
                        derived_object_id: None,
                        source_type: parse_source_type(row.get(1)?)?,
                        captured_at: row.get(2)?,
                        title: row.get(3)?,
                        body_text: None,
                        derived_object_type: None,
                        candidate_key: None,
                        enrichment_status: Some(parse_enrichment_status(row.get(4)?)?),
                        confidence_score: None,
                        related_artifact_count: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if include_kind(ReviewItemKind::ArtifactMissingSummary) {
            let mut stmt = connection.prepare(
                "SELECT a.artifact_id, a.source_type, a.captured_at, a.title, a.enrichment_status
                 FROM oa_artifact a
                 WHERE a.enrichment_status IN ('completed', 'partial')
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:artifact_missing_summary:' || a.artifact_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_derived_object d
                        WHERE d.artifact_id = a.artifact_id
                          AND d.derived_object_type = 'summary'
                          AND d.object_status = 'active'
                   )
                 ORDER BY a.captured_at DESC, a.artifact_id ASC
                 LIMIT ?1",
            ).map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::ArtifactMissingSummary,
                        artifact_id: row.get(0)?,
                        derived_object_id: None,
                        source_type: parse_source_type(row.get(1)?)?,
                        captured_at: row.get(2)?,
                        title: row.get(3)?,
                        body_text: None,
                        derived_object_type: None,
                        candidate_key: None,
                        enrichment_status: Some(parse_enrichment_status(row.get(4)?)?),
                        confidence_score: None,
                        related_artifact_count: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if include_kind(ReviewItemKind::ObjectLowConfidence) {
            let mut stmt = connection
                .prepare(
                    "SELECT d.derived_object_id, d.artifact_id, a.source_type, a.captured_at,
                        d.derived_object_type, d.title, d.body_text, d.confidence_score
                 FROM oa_derived_object d
                 JOIN oa_artifact a ON a.artifact_id = d.artifact_id
                 WHERE d.object_status = 'active'
                   AND d.confidence_score IS NOT NULL
                   AND d.confidence_score < 0.60
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:object_low_confidence:' || d.derived_object_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                 ORDER BY d.confidence_score ASC, a.captured_at DESC, d.derived_object_id ASC
                 LIMIT ?1",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::ObjectLowConfidence,
                        artifact_id: row.get(1)?,
                        derived_object_id: Some(row.get(0)?),
                        source_type: parse_source_type(row.get(2)?)?,
                        captured_at: row.get(3)?,
                        title: row.get(5)?,
                        body_text: row.get(6)?,
                        derived_object_type: Some(parse_derived_object_type(row.get(4)?)?),
                        candidate_key: None,
                        enrichment_status: None,
                        confidence_score: row.get(7)?,
                        related_artifact_count: None,
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        if include_kind(ReviewItemKind::CandidateKeyCollision) {
            let mut stmt = connection.prepare(
                "WITH collided_keys AS (
                     SELECT candidate_key, COUNT(DISTINCT artifact_id) AS artifact_count
                     FROM oa_derived_object
                     WHERE object_status = 'active'
                       AND candidate_key IS NOT NULL
                       AND candidate_key <> ''
                     GROUP BY candidate_key
                     HAVING COUNT(DISTINCT artifact_id) > 1
                 )
                 SELECT d.derived_object_id, d.artifact_id, a.source_type, a.captured_at,
                        d.derived_object_type, d.title, d.body_text, d.candidate_key, ck.artifact_count
                 FROM collided_keys ck
                 JOIN oa_derived_object d ON d.candidate_key = ck.candidate_key
                 JOIN oa_artifact a ON a.artifact_id = d.artifact_id
                 WHERE d.object_status = 'active'
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_review_decision rd
                        WHERE rd.item_id = 'review:candidate_key_collision:' || d.derived_object_id
                          AND rd.decision_status IN ('dismissed', 'resolved')
                   )
                 ORDER BY ck.artifact_count DESC, a.captured_at DESC, d.derived_object_id ASC
                 LIMIT ?1",
            ).map_err(|source| db_err(&self.config, source))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    Ok(ReviewCandidate {
                        kind: ReviewItemKind::CandidateKeyCollision,
                        artifact_id: row.get(1)?,
                        derived_object_id: Some(row.get(0)?),
                        source_type: parse_source_type(row.get(2)?)?,
                        captured_at: row.get(3)?,
                        title: row.get(5)?,
                        body_text: row.get(6)?,
                        derived_object_type: Some(parse_derived_object_type(row.get(4)?)?),
                        candidate_key: row.get(7)?,
                        enrichment_status: None,
                        confidence_score: None,
                        related_artifact_count: Some(row.get::<_, i64>(8)? as usize),
                    })
                })
                .map_err(|source| db_err(&self.config, source))?;
            candidates.extend(
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|source| db_err(&self.config, source))?,
            );
        }

        Ok(candidates)
    }
}

impl ReviewWriteStore for SqliteRetrievalReadStore {
    fn record_review_decision(&self, decision: &NewReviewDecision) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        connection
            .execute(
                "INSERT INTO oa_review_decision
                 (review_decision_id, item_id, item_kind, artifact_id, derived_object_id,
                  decision_status, note_text, decided_by)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    decision.review_decision_id,
                    decision.item_id,
                    decision.item_kind.as_str(),
                    decision.artifact_id,
                    decision.derived_object_id,
                    decision.decision_status.as_str(),
                    decision.note_text,
                    decision.decided_by
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn retry_artifact_enrichment(&self, artifact_id: &str) -> StorageResult<String> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;

        let row = tx
            .query_row(
                "SELECT artifact_id, import_id, source_type
                 FROM oa_artifact
                 WHERE artifact_id = ?1",
                [artifact_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        parse_source_type(row.get(2)?)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?;
        let Some((artifact_id_value, import_id, source_type)) = row else {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("artifact {artifact_id} not found for retry"),
            });
        };

        let active_job_count: i64 = tx
            .query_row(
                "SELECT COUNT(*)
                 FROM oa_enrichment_job
                 WHERE artifact_id = ?1
                   AND job_status IN ('pending', 'running', 'retryable')",
                [&artifact_id_value],
                |row| row.get(0),
            )
            .map_err(|source| db_err(&self.config, source))?;
        if active_job_count > 0 {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!("artifact {artifact_id} already has active enrichment work"),
            });
        }

        let job_id = next_id("job");
        let payload_json = ArtifactExtractPayload::new_v1(
            &artifact_id_value,
            &import_id,
            source_type,
            None,
            Vec::new(),
            Vec::new(),
        )
        .to_json();
        tx.execute(
            "INSERT INTO oa_enrichment_job
             (job_id, artifact_id, job_type, enrichment_tier, job_status, max_attempts,
              priority_no, required_capabilities, payload_json)
             VALUES (?1, ?2, 'artifact_extract', 'default', 'pending', 3, 100, '[\"text\"]', ?3)",
            params![job_id, artifact_id_value, payload_json],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(job_id)
    }
}

#[derive(Default)]
struct ArtifactEnrichmentSnapshot {
    pending_jobs: i64,
    running_jobs: i64,
    retryable_jobs: i64,
    completed_jobs: i64,
    failed_jobs: i64,
    extraction_results: i64,
    reconciliation_decisions: i64,
    completed_derivation_runs: i64,
}

fn derive_artifact_enrichment_status(snapshot: ArtifactEnrichmentSnapshot) -> EnrichmentStatus {
    let has_running = snapshot.running_jobs > 0 || snapshot.retryable_jobs > 0;
    let has_pending = snapshot.pending_jobs > 0;
    let has_failed = snapshot.failed_jobs > 0;
    let has_completed_jobs = snapshot.completed_jobs > 0;
    let has_durable_outputs = snapshot.extraction_results > 0
        || snapshot.reconciliation_decisions > 0
        || snapshot.completed_derivation_runs > 0;
    let is_fully_derived = snapshot.completed_derivation_runs > 0;

    if is_fully_derived && !has_running && !has_pending {
        return EnrichmentStatus::Completed;
    }
    if has_running {
        return if has_durable_outputs {
            EnrichmentStatus::Partial
        } else {
            EnrichmentStatus::Running
        };
    }
    if has_pending {
        return if has_durable_outputs {
            EnrichmentStatus::Partial
        } else if has_completed_jobs {
            EnrichmentStatus::Running
        } else {
            EnrichmentStatus::Pending
        };
    }
    if has_failed {
        return if has_durable_outputs {
            EnrichmentStatus::Partial
        } else {
            EnrichmentStatus::Failed
        };
    }
    if has_durable_outputs {
        return EnrichmentStatus::Partial;
    }
    if has_completed_jobs {
        return EnrichmentStatus::Running;
    }
    EnrichmentStatus::Pending
}

fn recompute_artifact_enrichment_status_sqlite(
    connection: &Connection,
    artifact_id: &str,
) -> StorageResult<EnrichmentStatus> {
    let snapshot = connection
        .query_row(
            "SELECT
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'pending'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'running'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'retryable'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'completed'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_enrichment_job WHERE artifact_id = ?1 AND job_status = 'failed'), 0),
                COALESCE((SELECT COUNT(*) FROM oa_artifact_extraction_result WHERE artifact_id = ?1), 0),
                COALESCE((SELECT COUNT(*) FROM oa_reconciliation_decision WHERE artifact_id = ?1), 0),
                COALESCE((SELECT COUNT(*) FROM oa_derivation_run WHERE artifact_id = ?1 AND run_status = 'completed'), 0)",
            [artifact_id],
            |row| {
                Ok(ArtifactEnrichmentSnapshot {
                    pending_jobs: row.get(0)?,
                    running_jobs: row.get(1)?,
                    retryable_jobs: row.get(2)?,
                    completed_jobs: row.get(3)?,
                    failed_jobs: row.get(4)?,
                    extraction_results: row.get(5)?,
                    reconciliation_decisions: row.get(6)?,
                    completed_derivation_runs: row.get(7)?,
                })
            },
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to compute enrichment snapshot for {artifact_id}: {source}"),
        })?;
    let next_status = derive_artifact_enrichment_status(snapshot);
    connection
        .execute(
            "UPDATE oa_artifact SET enrichment_status = ?2 WHERE artifact_id = ?1",
            params![artifact_id, next_status.as_str()],
        )
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to update enrichment status for {artifact_id}: {source}"),
        })?;
    Ok(next_status)
}

fn load_claimed_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClaimedJob> {
    let job_id: String = row.get(0)?;
    let required_capabilities_json: String = row.get(7)?;
    let required_capabilities = serde_json::from_str(&required_capabilities_json)
        .map_err(|err| rusqlite::Error::FromSqlConversionFailure(7, Type::Text, Box::new(err)))?;
    Ok(ClaimedJob {
        job_id,
        artifact_id: row.get(1)?,
        job_type: parse_job_type(row.get(2)?)?,
        enrichment_tier: parse_enrichment_tier(row.get(3)?)?,
        spawned_by_job_id: row.get(4)?,
        attempt_count: row.get(5)?,
        max_attempts: row.get(6)?,
        required_capabilities,
        payload_json: row.get(8)?,
    })
}

fn claim_job_with_sql(
    tx: &rusqlite::Transaction<'_>,
    worker_id: &str,
    sql: &str,
    params: &[&dyn ToSql],
) -> StorageResult<Option<ClaimedJob>> {
    let job = tx
        .query_row(sql, params, load_claimed_job_from_row)
        .optional()
        .map_err(|source| StorageError::InvalidDerivationWrite {
            detail: format!("failed to claim job candidate: {source}"),
        })?;
    let Some(job) = job else {
        return Ok(None);
    };

    tx.execute(
        "UPDATE oa_enrichment_job
         SET job_status = 'running',
             claimed_by = ?2,
             claimed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
             attempt_count = attempt_count + 1
         WHERE job_id = ?1",
        params![job.job_id, worker_id],
    )
    .map_err(|source| StorageError::InvalidDerivationWrite {
        detail: format!("failed to update claimed job {}: {source}", job.job_id),
    })?;
    recompute_artifact_enrichment_status_sqlite(tx, &job.artifact_id)?;

    Ok(Some(ClaimedJob {
        attempt_count: job.attempt_count + 1,
        ..job
    }))
}

impl EnrichmentJobLifecycleStore for SqliteEnrichmentJobStore {
    fn enqueue_jobs(&self, jobs: &[crate::storage::types::NewEnrichmentJob]) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        for job in jobs {
            tx.execute(
                "INSERT INTO oa_enrichment_job
                 (job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id, job_status,
                  max_attempts, priority_no, required_capabilities, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    job.job_id,
                    job.artifact_id,
                    job.job_type.as_str(),
                    job.enrichment_tier.as_str(),
                    job.spawned_by_job_id,
                    job.job_status.as_str(),
                    job.max_attempts,
                    job.priority_no,
                    serde_json::to_string(&job.required_capabilities)
                        .expect("required capabilities serializable"),
                    job.payload_json
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
            recompute_artifact_enrichment_status_sqlite(&tx, &job.artifact_id)?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn claim_next_job(&self, worker_id: &str) -> StorageResult<Option<ClaimedJob>> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let claimed = claim_job_with_sql(
            &tx,
            worker_id,
            "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                    attempt_count, max_attempts, required_capabilities, payload_json
             FROM oa_enrichment_job
             WHERE job_status IN ('pending', 'retryable')
               AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
             ORDER BY priority_no ASC, available_at ASC, job_id ASC
             LIMIT 1",
            &[],
        )?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(claimed)
    }

    fn claim_matching_jobs(
        &self,
        worker_id: &str,
        template_job: &ClaimedJob,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let required_capabilities = serde_json::to_string(&template_job.required_capabilities)
            .expect("required capabilities serializable");
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let next = claim_job_with_sql(
                &tx,
                worker_id,
                "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                        attempt_count, max_attempts, required_capabilities, payload_json
                 FROM oa_enrichment_job
                 WHERE job_status IN ('pending', 'retryable')
                   AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                   AND job_type = ?1
                   AND enrichment_tier = ?2
                   AND required_capabilities = ?3
                 ORDER BY priority_no ASC, available_at ASC, job_id ASC
                 LIMIT 1",
                &[
                    &template_job.job_type.as_str(),
                    &template_job.enrichment_tier.as_str(),
                    &required_capabilities,
                ],
            )?;
            let Some(next) = next else {
                break;
            };
            claimed.push(next);
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(claimed)
    }

    fn claim_jobs_by_type(
        &self,
        worker_id: &str,
        job_type: JobType,
        enrichment_tier: Option<EnrichmentTier>,
        limit: usize,
    ) -> StorageResult<Vec<ClaimedJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let next = if let Some(tier) = enrichment_tier {
                claim_job_with_sql(
                    &tx,
                    worker_id,
                    "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                            attempt_count, max_attempts, required_capabilities, payload_json
                     FROM oa_enrichment_job
                     WHERE job_status IN ('pending', 'retryable')
                       AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       AND job_type = ?1
                       AND enrichment_tier = ?2
                     ORDER BY priority_no ASC, available_at ASC, job_id ASC
                     LIMIT 1",
                    &[&job_type.as_str(), &tier.as_str()],
                )?
            } else {
                claim_job_with_sql(
                    &tx,
                    worker_id,
                    "SELECT job_id, artifact_id, job_type, enrichment_tier, spawned_by_job_id,
                            attempt_count, max_attempts, required_capabilities, payload_json
                     FROM oa_enrichment_job
                     WHERE job_status IN ('pending', 'retryable')
                       AND available_at <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
                       AND job_type = ?1
                     ORDER BY priority_no ASC, available_at ASC, job_id ASC
                     LIMIT 1",
                    &[&job_type.as_str()],
                )?
            };
            let Some(next) = next else {
                break;
            };
            claimed.push(next);
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(claimed)
    }

    fn complete_job(&self, worker_id: &str, job_id: &str) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_id: String = tx
            .query_row(
                "SELECT artifact_id
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        tx.execute(
            "UPDATE oa_enrichment_job
             SET job_status = 'completed',
                 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                 claimed_by = NULL,
                 claimed_at = NULL
             WHERE job_id = ?1 AND claimed_by = ?2",
            params![job_id, worker_id],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn fail_job(&self, worker_id: &str, job_id: &str, error_message: &str) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_id: String = tx
            .query_row(
                "SELECT artifact_id
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        tx.execute(
            "UPDATE oa_enrichment_job
             SET job_status = 'failed',
                 last_error_message = ?3,
                 claimed_by = NULL,
                 claimed_at = NULL
             WHERE job_id = ?1 AND claimed_by = ?2",
            params![job_id, worker_id, error_message],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn mark_job_retryable(
        &self,
        worker_id: &str,
        job_id: &str,
        error_message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<RetryOutcome> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let row = tx
            .query_row(
                "SELECT artifact_id, attempt_count, max_attempts
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        let (artifact_id, attempt_count, max_attempts) = row;
        let outcome = if attempt_count >= max_attempts {
            tx.execute(
                "UPDATE oa_enrichment_job
                 SET job_status = 'failed',
                     last_error_message = ?3,
                     claimed_by = NULL,
                     claimed_at = NULL
                 WHERE job_id = ?1 AND claimed_by = ?2",
                params![job_id, worker_id, error_message],
            )
            .map_err(|source| db_err(&self.config, source))?;
            RetryOutcome::RetriesExhausted
        } else {
            tx.execute(
                "UPDATE oa_enrichment_job
                 SET job_status = 'retryable',
                     last_error_message = ?3,
                     available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?4),
                     claimed_by = NULL,
                     claimed_at = NULL
                 WHERE job_id = ?1 AND claimed_by = ?2",
                params![
                    job_id,
                    worker_id,
                    error_message,
                    format!("+{retry_after_seconds} seconds")
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
            RetryOutcome::Retried
        };
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(outcome)
    }

    fn reschedule_running_job(
        &self,
        worker_id: &str,
        job_id: &str,
        message: &str,
        retry_after_seconds: i64,
    ) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_id: String = tx
            .query_row(
                "SELECT artifact_id
                 FROM oa_enrichment_job
                 WHERE job_id = ?1 AND job_status = 'running' AND claimed_by = ?2",
                params![job_id, worker_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?
            .ok_or_else(|| StorageError::JobNotClaimed {
                job_id: job_id.to_string(),
                worker_id: worker_id.to_string(),
            })?;
        tx.execute(
            "UPDATE oa_enrichment_job
             SET job_status = 'retryable',
                 last_error_message = ?3,
                 available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now', ?4),
                 claimed_by = NULL,
                 claimed_at = NULL
             WHERE job_id = ?1 AND claimed_by = ?2",
            params![
                job_id,
                worker_id,
                message,
                format!("+{retry_after_seconds} seconds")
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn record_batch_submission(
        &self,
        batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "INSERT INTO oa_enrichment_batch
             (provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                batch.provider_batch_id,
                batch.provider_name,
                batch.stage_name,
                batch.phase_name,
                batch.owner_worker_id,
                batch.context_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        for (index, job) in jobs.iter().enumerate() {
            tx.execute(
                "INSERT INTO oa_enrichment_batch_job (provider_batch_id, job_id, job_order)
                 VALUES (?1, ?2, ?3)",
                params![batch.provider_batch_id, job.job_id, index as i64],
            )
            .map_err(|source| db_err(&self.config, source))?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn transition_batch_submission(
        &self,
        completed_provider_batch_id: &str,
        next_batch: &NewEnrichmentBatch,
        jobs: &[ClaimedJob],
    ) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "UPDATE oa_enrichment_batch
             SET batch_status = 'completed',
                 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE provider_batch_id = ?1",
            [completed_provider_batch_id],
        )
        .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "INSERT INTO oa_enrichment_batch
             (provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                next_batch.provider_batch_id,
                next_batch.provider_name,
                next_batch.stage_name,
                next_batch.phase_name,
                next_batch.owner_worker_id,
                next_batch.context_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        for (index, job) in jobs.iter().enumerate() {
            tx.execute(
                "INSERT INTO oa_enrichment_batch_job (provider_batch_id, job_id, job_order)
                 VALUES (?1, ?2, ?3)",
                params![next_batch.provider_batch_id, job.job_id, index as i64],
            )
            .map_err(|source| db_err(&self.config, source))?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn complete_batch(&self, provider_batch_id: &str) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        connection
            .execute(
                "UPDATE oa_enrichment_batch
                 SET batch_status = 'completed',
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                 WHERE provider_batch_id = ?1",
                [provider_batch_id],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn fail_batch_record(&self, provider_batch_id: &str, error_message: &str) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        connection
            .execute(
                "UPDATE oa_enrichment_batch
                 SET batch_status = 'failed',
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                     last_error_message = ?2
                 WHERE provider_batch_id = ?1",
                params![provider_batch_id, error_message],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn load_running_batches(
        &self,
        stage_name: &str,
    ) -> StorageResult<Vec<PersistedEnrichmentBatch>> {
        let connection = open_connection(&self.config)?;
        let mut batch_stmt = connection
            .prepare(
                "SELECT provider_batch_id, provider_name, stage_name, phase_name, owner_worker_id, context_json
                 FROM oa_enrichment_batch
                 WHERE stage_name = ?1 AND batch_status = 'running'
                 ORDER BY submitted_at ASC, provider_batch_id ASC",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let batch_rows = batch_stmt
            .query_map([stage_name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })
            .map_err(|source| db_err(&self.config, source))?;

        let mut batches = Vec::new();
        for batch_row in batch_rows {
            let (
                provider_batch_id,
                provider_name,
                stage_name,
                phase_name,
                owner_worker_id,
                context_json,
            ) = batch_row.map_err(|source| db_err(&self.config, source))?;
            let mut job_stmt = connection
                .prepare(
                    "SELECT j.job_id, j.artifact_id, j.job_type, j.enrichment_tier, j.spawned_by_job_id,
                            j.attempt_count, j.max_attempts, j.required_capabilities, j.payload_json
                     FROM oa_enrichment_batch_job bj
                     JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                     WHERE bj.provider_batch_id = ?1
                     ORDER BY bj.job_order ASC",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let jobs = job_stmt
                .query_map([&provider_batch_id], load_claimed_job_from_row)
                .map_err(|source| db_err(&self.config, source))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| db_err(&self.config, source))?;
            batches.push(PersistedEnrichmentBatch {
                provider_batch_id,
                provider_name,
                stage_name,
                phase_name,
                owner_worker_id,
                context_json,
                jobs,
            });
        }
        Ok(batches)
    }

    fn reconcile_stale_running_batches(&self, stage_name: &str) -> StorageResult<usize> {
        let connection = open_connection(&self.config)?;
        let rows = connection
            .execute(
                "UPDATE oa_enrichment_batch
                 SET batch_status = 'failed',
                     completed_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                     last_error_message = COALESCE(last_error_message, 'reconciled stale running batch')
                 WHERE stage_name = ?1
                   AND batch_status = 'running'
                   AND NOT EXISTS (
                        SELECT 1
                        FROM oa_enrichment_batch_job bj
                        JOIN oa_enrichment_job j ON j.job_id = bj.job_id
                        WHERE bj.provider_batch_id = oa_enrichment_batch.provider_batch_id
                          AND j.job_status = 'running'
                   )",
                [stage_name],
            )
            .map_err(|source| db_err(&self.config, source))?;
        Ok(rows)
    }

    fn reconcile_stale_running_jobs(&self, stage_name: &str) -> StorageResult<usize> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let artifact_ids = {
            let mut artifact_stmt = tx
                .prepare(
                    "SELECT DISTINCT j.artifact_id
                     FROM oa_enrichment_job j
                     JOIN oa_enrichment_batch_job bj ON bj.job_id = j.job_id
                     JOIN oa_enrichment_batch b ON b.provider_batch_id = bj.provider_batch_id
                     WHERE b.stage_name = ?1
                       AND b.batch_status <> 'running'
                       AND j.job_status = 'running'",
                )
                .map_err(|source| db_err(&self.config, source))?;
            let artifact_ids = artifact_stmt
                .query_map([stage_name], |row| row.get::<_, String>(0))
                .map_err(|source| db_err(&self.config, source))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| db_err(&self.config, source))?;
            artifact_ids
        };
        let updated = tx
            .execute(
                "UPDATE oa_enrichment_job
                 SET job_status = 'retryable',
                     last_error_message = COALESCE(last_error_message, 'reconciled stale running job'),
                     available_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
                     claimed_by = NULL,
                     claimed_at = NULL
                 WHERE job_id IN (
                     SELECT j.job_id
                     FROM oa_enrichment_job j
                     JOIN oa_enrichment_batch_job bj ON bj.job_id = j.job_id
                     JOIN oa_enrichment_batch b ON b.provider_batch_id = bj.provider_batch_id
                     WHERE b.stage_name = ?1
                       AND b.batch_status <> 'running'
                       AND j.job_status = 'running'
                 )",
                [stage_name],
            )
            .map_err(|source| db_err(&self.config, source))?;
        for artifact_id in artifact_ids {
            recompute_artifact_enrichment_status_sqlite(&tx, &artifact_id)?;
        }
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(updated)
    }
}

impl EnrichmentStateStore for SqliteDerivedMetadataStore {
    fn save_extraction_result(&self, result: &ArtifactExtractionResult) -> StorageResult<()> {
        with_sqlite_write_retry(&self.config, || {
            let connection = open_connection(&self.config)?;
            connection
                .execute(
                    "INSERT INTO oa_artifact_extraction_result
                     (extraction_result_id, artifact_id, job_id, pipeline_name, pipeline_version, result_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(extraction_result_id) DO UPDATE SET
                         result_json = excluded.result_json,
                         pipeline_name = excluded.pipeline_name,
                         pipeline_version = excluded.pipeline_version",
                    params![
                        result.extraction_result_id,
                        result.artifact_id,
                        result.job_id,
                        result.pipeline_name,
                        result.pipeline_version,
                        serde_json::to_string(result).expect("extraction result serializable")
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
            Ok(())
        })
    }

    fn load_extraction_result(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Option<ArtifactExtractionResult>> {
        let connection = open_connection(&self.config)?;
        let json: Option<String> = connection
            .query_row(
                "SELECT result_json FROM oa_artifact_extraction_result WHERE extraction_result_id = ?1",
                [extraction_result_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|source| db_err(&self.config, source))?;
        json.map(|json| deserialize_json(&json, "failed to deserialize extraction result"))
            .transpose()
    }

    fn save_reconciliation_decisions(
        &self,
        decisions: &[ReconciliationDecision],
    ) -> StorageResult<()> {
        with_sqlite_write_retry(&self.config, || {
            let mut connection = open_connection(&self.config)?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;
            for decision in decisions {
                tx.execute(
                    "INSERT INTO oa_reconciliation_decision
                     (reconciliation_decision_id, artifact_id, job_id, extraction_result_id, pipeline_name,
                      pipeline_version, decision_kind, target_kind, target_key, matched_object_id, rationale, decision_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                     ON CONFLICT(reconciliation_decision_id) DO UPDATE SET
                        decision_json = excluded.decision_json,
                        rationale = excluded.rationale",
                    params![
                        decision.reconciliation_decision_id,
                        decision.artifact_id,
                        decision.job_id,
                        decision.extraction_result_id,
                        decision.pipeline_name,
                        decision.pipeline_version,
                        serde_json::to_string(&decision.decision_kind).expect("decision kind serializable"),
                        decision.target_kind,
                        decision.target_key,
                        decision.matched_object_id,
                        decision.rationale,
                        serde_json::to_string(decision).expect("decision serializable")
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
            }
            tx.commit().map_err(|source| db_err(&self.config, source))?;
            Ok(())
        })
    }

    fn load_reconciliation_decisions(
        &self,
        extraction_result_id: &str,
    ) -> StorageResult<Vec<ReconciliationDecision>> {
        let connection = open_connection(&self.config)?;
        let mut stmt = connection
            .prepare(
                "SELECT decision_json
                 FROM oa_reconciliation_decision
                 WHERE extraction_result_id = ?1
                 ORDER BY reconciliation_decision_id",
            )
            .map_err(|source| db_err(&self.config, source))?;
        let rows = stmt
            .query_map([extraction_result_id], |row| row.get::<_, String>(0))
            .map_err(|source| db_err(&self.config, source))?;
        let mut decisions = Vec::new();
        for row in rows {
            decisions.push(deserialize_json(
                &row.map_err(|source| db_err(&self.config, source))?,
                "failed to deserialize reconciliation decision",
            )?);
        }
        Ok(decisions)
    }
}

impl DerivedMetadataWriteStore for SqliteDerivedMetadataStore {
    fn write_derivation_attempt(
        &self,
        attempt: WriteDerivationAttempt,
    ) -> StorageResult<DerivationWriteResult> {
        with_sqlite_write_retry(&self.config, || {
            let mut connection = open_connection(&self.config)?;
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|source| db_err(&self.config, source))?;

            let artifact_exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM oa_artifact WHERE artifact_id = ?1)",
                    [&attempt.run.artifact_id],
                    |row| row.get(0),
                )
                .map_err(|source| db_err(&self.config, source))?;
            if !artifact_exists {
                return Err(StorageError::InvalidDerivationWrite {
                    detail: format!("artifact {} does not exist", attempt.run.artifact_id),
                });
            }

            tx.execute(
                "UPDATE oa_derived_object SET object_status = 'superseded'
                 WHERE artifact_id = ?1 AND object_status = 'active'",
                [&attempt.run.artifact_id],
            )
            .map_err(|source| db_err(&self.config, source))?;

            tx.execute(
                "INSERT INTO oa_derivation_run
                 (derivation_run_id, artifact_id, job_id, run_type, pipeline_name, pipeline_version,
                  provider_name, model_name, prompt_version, run_status, input_scope_type, input_scope_json,
                  started_at, completed_at, error_message)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    attempt.run.derivation_run_id,
                    attempt.run.artifact_id,
                    attempt.run.job_id,
                    attempt.run.run_type.as_str(),
                    attempt.run.pipeline_name,
                    attempt.run.pipeline_version,
                    attempt.run.provider_name,
                    attempt.run.model_name,
                    attempt.run.prompt_version,
                    attempt.run.run_status.as_str(),
                    attempt.run.input_scope_type.as_str(),
                    attempt.run.input_scope_json,
                    attempt.run.started_at.as_str(),
                    attempt.run.completed_at.as_ref().map(SourceTimestamp::as_str),
                    attempt.run.error_message
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;

            let mut derived_object_ids = Vec::with_capacity(attempt.objects.len());
            for object_write in &attempt.objects {
                let object = &object_write.object;
                tx.execute(
                    "INSERT INTO oa_derived_object
                     (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind,
                      object_status, confidence_score, confidence_label, scope_type, scope_id, title,
                      body_text, object_json, supersedes_derived_object_id)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        object.derived_object_id,
                        object.artifact_id,
                        object.derivation_run_id,
                        object.payload.derived_object_type().as_str(),
                        object.origin_kind.as_str(),
                        object.object_status.as_str(),
                        object.confidence_score,
                        object.confidence_label,
                        object.scope_type.as_str(),
                        object.scope_id,
                        object.payload.title(),
                        object.payload.body_text(),
                        object.payload.object_json(),
                        object.supersedes_derived_object_id
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
                derived_object_ids.push(object.derived_object_id.clone());
            }

            for link in &attempt.archive_links {
                tx.execute(
                    "INSERT INTO oa_archive_link
                     (archive_link_id, source_object_id, target_object_id, link_type, confidence_score, rationale, origin_kind, contributed_by)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'inferred', ?7)
                     ON CONFLICT(source_object_id, target_object_id, link_type) DO UPDATE SET
                        confidence_score = COALESCE(excluded.confidence_score, oa_archive_link.confidence_score),
                        rationale = COALESCE(excluded.rationale, oa_archive_link.rationale),
                        contributed_by = COALESCE(excluded.contributed_by, oa_archive_link.contributed_by)",
                    params![
                        link.archive_link_id,
                        link.source_object_id,
                        link.target_object_id,
                        link.link_type,
                        link.confidence_score,
                        link.rationale,
                        link.contributed_by
                    ],
                )
                .map_err(|source| db_err(&self.config, source))?;
            }
            sync_reconcile_links_for_archive_links(&tx, &attempt.archive_links)?;
            tx.commit().map_err(|source| db_err(&self.config, source))?;

            Ok(DerivationWriteResult {
                derivation_run_id: attempt.run.derivation_run_id.clone(),
                derived_object_ids,
            })
        })
    }
}

impl WritebackStore for SqliteWritebackStore {
    fn store_agent_memory(&self, memory: &NewAgentMemory) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let derivation_run_id = format!("agentrun-{}", memory.derived_object_id);
        tx.execute(
            "INSERT INTO oa_derivation_run
             (derivation_run_id, artifact_id, run_type, pipeline_name, pipeline_version, provider_name,
              run_status, input_scope_type, input_scope_json, started_at, completed_at)
             VALUES (?1, ?2, 'agent_contributed', 'agent_writeback', '1.0', ?3,
                     'completed', 'artifact', '{}', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            params![derivation_run_id, memory.artifact_id, memory.contributed_by],
        )
        .map_err(|source| db_err(&self.config, source))?;
        let object_json = serde_json::json!({
            "memory_type": memory.memory_type,
            "candidate_key": memory.candidate_key.as_deref().unwrap_or(""),
            "memory_scope": "artifact",
            "memory_scope_value": memory.artifact_id,
        })
        .to_string();
        tx.execute(
            "INSERT INTO oa_derived_object
             (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status,
              scope_type, scope_id, title, body_text, object_json)
             VALUES (?1, ?2, ?3, 'memory', 'agent_contributed', 'active', 'artifact', ?4, ?5, ?6, ?7)",
            params![
                memory.derived_object_id,
                memory.artifact_id,
                derivation_run_id,
                memory.artifact_id,
                memory.title,
                memory.body_text,
                object_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn store_archive_link(&self, link: &NewArchiveLink) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        tx.execute(
            "INSERT INTO oa_archive_link
             (archive_link_id, source_object_id, target_object_id, link_type, confidence_score, rationale, origin_kind, contributed_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'agent_contributed', ?7)
             ON CONFLICT(source_object_id, target_object_id, link_type) DO UPDATE SET
                 confidence_score = COALESCE(excluded.confidence_score, oa_archive_link.confidence_score),
                 rationale = COALESCE(excluded.rationale, oa_archive_link.rationale),
                 contributed_by = COALESCE(excluded.contributed_by, oa_archive_link.contributed_by)",
            params![
                link.archive_link_id,
                link.source_object_id,
                link.target_object_id,
                link.link_type,
                link.confidence_score,
                link.rationale,
                link.contributed_by
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        sync_reconcile_links_for_archive_links(&tx, std::slice::from_ref(link))?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }

    fn update_object_status(&self, update: &UpdateObjectStatus) -> StorageResult<()> {
        let connection = open_connection(&self.config)?;
        let rows = connection
            .execute(
                "UPDATE oa_derived_object
                 SET object_status = ?2,
                     supersedes_derived_object_id = ?3
                 WHERE derived_object_id = ?1
                   AND object_status = 'active'",
                params![
                    update.derived_object_id,
                    update.new_status.as_str(),
                    update.replacement_object_id
                ],
            )
            .map_err(|source| db_err(&self.config, source))?;
        if rows == 0 {
            return Err(StorageError::InvalidDerivationWrite {
                detail: format!(
                    "no active derived object found with id {} to update",
                    update.derived_object_id
                ),
            });
        }
        Ok(())
    }

    fn store_agent_entity(&self, entity: &NewAgentEntity) -> StorageResult<()> {
        let mut connection = open_connection(&self.config)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| db_err(&self.config, source))?;
        let derivation_run_id = format!("agentrun-{}", entity.derived_object_id);
        tx.execute(
            "INSERT INTO oa_derivation_run
             (derivation_run_id, artifact_id, run_type, pipeline_name, pipeline_version, provider_name,
              run_status, input_scope_type, input_scope_json, started_at, completed_at)
             VALUES (?1, ?2, 'agent_contributed', 'agent_writeback', '1.0', ?3,
                     'completed', 'artifact', '{}', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            params![derivation_run_id, entity.artifact_id, entity.contributed_by],
        )
        .map_err(|source| db_err(&self.config, source))?;
        let object_json = serde_json::json!({
            "entity_type": entity.entity_type,
            "candidate_key": entity.candidate_key.as_deref().unwrap_or(""),
        })
        .to_string();
        tx.execute(
            "INSERT INTO oa_derived_object
             (derived_object_id, artifact_id, derivation_run_id, derived_object_type, origin_kind, object_status,
              scope_type, scope_id, title, body_text, object_json)
             VALUES (?1, ?2, ?3, 'entity', 'agent_contributed', 'active', 'artifact', ?4, ?5, ?6, ?7)",
            params![
                entity.derived_object_id,
                entity.artifact_id,
                derivation_run_id,
                entity.artifact_id,
                entity.title,
                entity.body_text,
                object_json
            ],
        )
        .map_err(|source| db_err(&self.config, source))?;
        tx.commit().map_err(|source| db_err(&self.config, source))?;
        Ok(())
    }
}

#[cfg(test)]
mod fts5_helper_tests {
    use super::{
        build_fts5_match_expression, detect_search_query_mode, fts5_bm25_score_bonus,
        SqliteSearchQueryMode,
    };

    #[test]
    fn plain_query_becomes_or_of_quoted_tokens() {
        assert_eq!(
            build_fts5_match_expression("dressage horse training").as_deref(),
            Some("\"dressage\" OR \"horse\" OR \"training\"")
        );
    }

    #[test]
    fn single_token_plain_query_is_quoted() {
        assert_eq!(
            build_fts5_match_expression("dressage").as_deref(),
            Some("\"dressage\"")
        );
    }

    #[test]
    fn extra_whitespace_is_collapsed() {
        assert_eq!(
            build_fts5_match_expression("  blood   pressure  ").as_deref(),
            Some("\"blood\" OR \"pressure\"")
        );
    }

    #[test]
    fn empty_query_yields_none() {
        assert!(build_fts5_match_expression("").is_none());
        assert!(build_fts5_match_expression("   ").is_none());
    }

    #[test]
    fn embedded_double_quote_is_escaped() {
        // FTS5 escapes a literal `"` inside a quoted token by doubling it.
        assert_eq!(
            build_fts5_match_expression("foo\"bar").as_deref(),
            Some("\"foo\"\"bar\"")
        );
    }

    #[test]
    fn operator_query_passes_through_verbatim() {
        let raw = "memory AND retrieval NOT cache";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
        assert_eq!(build_fts5_match_expression(raw).as_deref(), Some(raw));
    }

    #[test]
    fn quoted_phrase_query_passes_through_verbatim() {
        let raw = "\"enrichment pipeline\"";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
        assert_eq!(build_fts5_match_expression(raw).as_deref(), Some(raw));
    }

    #[test]
    fn mixed_natural_language_with_quoted_phrase_stays_plain() {
        let raw = "Ahab's \"Strike Through the Mask\" Speech";
        assert_eq!(detect_search_query_mode(raw), SqliteSearchQueryMode::Plain);
        assert_eq!(
            build_fts5_match_expression(raw).as_deref(),
            Some("\"Ahab's\" OR \"\"\"Strike\" OR \"Through\" OR \"the\" OR \"Mask\"\"\" OR \"Speech\"")
        );
    }

    #[test]
    fn parenthesized_query_is_operator_mode() {
        let raw = "(memory OR summary) AND retrieval";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
    }

    #[test]
    fn prefix_query_is_operator_mode() {
        let raw = "dressag*";
        assert_eq!(
            detect_search_query_mode(raw),
            SqliteSearchQueryMode::Operator
        );
        assert_eq!(build_fts5_match_expression(raw).as_deref(), Some(raw));
    }

    #[test]
    fn lowercase_and_or_not_are_treated_as_plain_tokens() {
        // FTS5 only treats uppercase AND/OR/NOT as operators, so a lowercase
        // "and" should be quoted like any other plain token. This matches the
        // intent of "adding a term broadens results".
        assert_eq!(
            build_fts5_match_expression("dressage and horse").as_deref(),
            Some("\"dressage\" OR \"and\" OR \"horse\"")
        );
    }

    #[test]
    fn bm25_bonus_rewards_better_matches_with_larger_values() {
        // bm25() returns smaller (more negative) values for better matches.
        let weak = fts5_bm25_score_bonus(-0.1);
        let strong = fts5_bm25_score_bonus(-2.5);
        assert!(strong > weak, "strong={strong} should exceed weak={weak}");
    }

    #[test]
    fn bm25_bonus_clamps_non_matches_to_zero() {
        assert_eq!(fts5_bm25_score_bonus(0.0), 0);
        assert_eq!(fts5_bm25_score_bonus(1.5), 0);
        assert_eq!(fts5_bm25_score_bonus(f64::NAN), 0);
        assert_eq!(fts5_bm25_score_bonus(f64::INFINITY), 0);
    }
}
