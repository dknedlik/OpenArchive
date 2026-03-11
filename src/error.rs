use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, OpenArchiveError>;
pub type ConfigResult<T> = std::result::Result<T, ConfigError>;
pub type DbResult<T> = std::result::Result<T, DbError>;
pub type MigrationsResult<T> = std::result::Result<T, MigrationsError>;
pub type StorageResult<T> = std::result::Result<T, StorageError>;
pub type ParserResult<T> = std::result::Result<T, ParserError>;

#[derive(Debug, Error)]
pub enum OpenArchiveError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Db(#[from] DbError),

    #[error(transparent)]
    Migrations(#[from] MigrationsError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Parser(#[from] ParserError),

    #[error("internal invariant violated: {0}")]
    Invariant(String),
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{key} is required")]
    MissingEnv { key: &'static str },

    #[error("{key} is required when WALLET_DIR is not provided")]
    MissingEnvWithDependency { key: &'static str },

    #[error("invalid {key} value {value:?}; expected positive integer")]
    InvalidPositiveIntegerEnv { key: &'static str, value: String },

    #[error("set DB_PASSWORD or one of DB_DEV_PASSWORD / DB_PROD_PASSWORD / DB_ADMIN_PASSWORD")]
    MissingPassword,
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("invalid {key} value {value:?}; expected integer")]
    InvalidIntegerEnv { key: &'static str, value: String },

    #[error("invalid {key} value {value:?}; expected integer milliseconds")]
    InvalidDurationEnv { key: &'static str, value: String },

    #[error("failed to configure Oracle pool ping interval")]
    ConfigurePoolPingInterval {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to create Oracle pool for alias {tns_alias}")]
    CreatePool {
        tns_alias: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to acquire Oracle connection for alias {tns_alias}")]
    AcquireConnection {
        tns_alias: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to set Oracle call timeout")]
    SetCallTimeout {
        #[source]
        source: oracle::Error,
    },
}

#[derive(Debug, Error)]
pub enum MigrationsError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("database is not up to date")]
    DatabaseNotUpToDate,

    #[error("migration checksum mismatch for version {version} (file {filename})")]
    ChecksumMismatch { version: String, filename: String },

    #[error("failed to ensure oa_schema_migration exists")]
    EnsureSchemaMigrationTable {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to commit oa_schema_migration bootstrap")]
    CommitSchemaMigrationBootstrap {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to reset open_archive schema objects")]
    ResetSchemaObjects {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to load applied migrations")]
    LoadAppliedMigrations {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to read migration history row")]
    ReadMigrationHistoryRow {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to read migration version")]
    ReadMigrationVersion {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to read migration checksum")]
    ReadMigrationChecksum {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to read migration directory {path}")]
    ReadMigrationsDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read migration entry")]
    ReadMigrationEntry {
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read migration {path}")]
    ReadMigrationFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("invalid migration filename: {path}")]
    InvalidMigrationFilename { path: PathBuf },

    #[error("migration file must end in .sql: {filename}")]
    MigrationFileMissingSqlSuffix { filename: String },

    #[error("migration file must match VNNN__name.sql: {filename}")]
    MigrationFileInvalidPattern { filename: String },

    #[error("migration file must start with VNNN: {filename}")]
    MigrationFileInvalidVersionPrefix { filename: String },

    #[error("unterminated string literal in migration SQL")]
    UnterminatedStringLiteral,

    #[error("statement failed in migration {filename}: {statement_preview}")]
    ExecuteMigrationStatement {
        filename: String,
        statement_preview: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to record migration {filename}")]
    RecordMigration {
        filename: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to commit migration {filename}")]
    CommitMigration {
        filename: String,
        #[source]
        source: oracle::Error,
    },
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("failed to insert import payload {payload_id}")]
    InsertPayload {
        payload_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert import {import_id}")]
    InsertImport {
        import_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to update import counts for {import_id}")]
    UpdateImportCounts {
        import_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to finalize import {import_id}")]
    FinalizeImport {
        import_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to complete import {import_id}")]
    CompleteImport {
        import_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert artifact {artifact_id}")]
    InsertArtifact {
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to list artifacts")]
    ListArtifacts {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert participant {participant_id} for artifact {artifact_id}")]
    InsertParticipant {
        participant_id: String,
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert segment {segment_id} for artifact {artifact_id}")]
    InsertSegment {
        segment_id: String,
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert derivation run {derivation_run_id} for artifact {artifact_id}")]
    InsertDerivationRun {
        derivation_run_id: String,
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert derived object {derived_object_id} for artifact {artifact_id}")]
    InsertDerivedObject {
        derived_object_id: String,
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error(
        "failed to insert evidence link {evidence_link_id} for derived object {derived_object_id}"
    )]
    InsertEvidenceLink {
        evidence_link_id: String,
        derived_object_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("invalid derivation write: {detail}")]
    InvalidDerivationWrite { detail: String },

    #[error("failed to validate artifact ownership for {artifact_id}")]
    ValidateArtifactOwnership {
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to validate segment ownership for {segment_id}")]
    ValidateSegmentOwnership {
        segment_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to insert enrichment job {job_id} for artifact {artifact_id}")]
    InsertJob {
        job_id: String,
        artifact_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to claim next enrichment job")]
    ClaimJob {
        #[source]
        source: oracle::Error,
    },

    #[error("failed to update enrichment job {job_id} status")]
    UpdateJobStatus {
        job_id: String,
        #[source]
        source: oracle::Error,
    },

    #[error("invalid job_type '{job_type}' for job {job_id}")]
    InvalidJobType { job_id: String, job_type: String },

    #[error("failed to commit {operation}")]
    Commit {
        operation: &'static str,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to rollback after {operation}")]
    Rollback {
        operation: String,
        #[source]
        source: oracle::Error,
    },

    #[error("failed to recover finalization for import {import_id}")]
    RecoverImportFinalization {
        import_id: String,
        #[source]
        source: Box<StorageError>,
    },

    #[error("invalid enrichment_status '{value}' for artifact {artifact_id}")]
    InvalidEnrichmentStatus { artifact_id: String, value: String },
}

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("invalid JSON: {detail}")]
    InvalidJson { detail: String },

    #[error("export contains no conversations")]
    EmptyExport,

    #[error("conversation {conversation_id} has an empty mapping")]
    EmptyConversation { conversation_id: String },

    #[error("conversation {conversation_id} has no root node")]
    NoRoot { conversation_id: String },

    #[error("conversation {conversation_id} references missing current_node {node_id}")]
    MissingCurrentNode {
        conversation_id: String,
        node_id: String,
    },

    #[error("conversation {conversation_id}: node {node_id} references a missing parent")]
    BrokenParentLink {
        conversation_id: String,
        node_id: String,
    },

    #[error(
        "conversation {conversation_id}: node {parent_id} references missing child {child_id}"
    )]
    MissingChild {
        conversation_id: String,
        parent_id: String,
        child_id: String,
    },

    #[error("conversation {conversation_id} has a cyclic node graph")]
    CyclicTree { conversation_id: String },
}

pub fn preview_sql_statement(statement: &str) -> String {
    const MAX_PREVIEW_LEN: usize = 120;

    let normalized = statement.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.len() <= MAX_PREVIEW_LEN {
        normalized
    } else {
        format!("{}...", &normalized[..MAX_PREVIEW_LEN])
    }
}
