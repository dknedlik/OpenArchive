use std::error::Error as StdError;
use std::path::PathBuf;
use thiserror::Error;

// Library code stays on typed errors so binaries and future transport adapters
// can decide presentation and retry behavior at the boundary.
pub type Result<T> = std::result::Result<T, OpenArchiveError>;
pub type ConfigResult<T> = std::result::Result<T, ConfigError>;
pub type DbResult<T> = std::result::Result<T, DbError>;
pub type MigrationsResult<T> = std::result::Result<T, MigrationsError>;
pub type ObjectStoreResult<T> = std::result::Result<T, ObjectStoreError>;
pub type StorageResult<T> = std::result::Result<T, StorageError>;
pub type ParserResult<T> = std::result::Result<T, ParserError>;
pub type EmbeddingResult<T> = std::result::Result<T, EmbeddingError>;
pub type WorkerResult<T> = std::result::Result<T, WorkerError>;

#[derive(Debug, Error)]
pub enum OpenArchiveError {
    #[error(transparent)]
    Config(ConfigError),

    #[error(transparent)]
    Db(Box<DbError>),

    #[error(transparent)]
    Migrations(Box<MigrationsError>),

    #[error(transparent)]
    ObjectStore(Box<ObjectStoreError>),

    #[error(transparent)]
    Storage(Box<StorageError>),

    #[error(transparent)]
    Parser(ParserError),

    #[error(transparent)]
    Embedding(Box<EmbeddingError>),

    #[error("internal invariant violated: {0}")]
    Invariant(String),
}

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("{key} is required when OA_OBJECT_STORE_ROOT is not provided")]
    MissingEnvWithDependency { key: &'static str },

    #[error("failed to create object-store directory {path}")]
    CreateDir {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },

    #[error("failed to write object {object_id} to {path}")]
    WriteObject {
        object_id: String,
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },

    #[error("failed to read object {object_id} from {path}")]
    ReadObject {
        object_id: String,
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },

    #[error("failed to delete object {object_id} at {path}")]
    DeleteObject {
        object_id: String,
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },

    #[error("invalid {key} URL {value:?}")]
    InvalidUrl {
        key: &'static str,
        value: String,
        #[source]
        source: url::ParseError,
    },

    #[error("invalid S3 object-store configuration: {message}")]
    InvalidS3Config { message: String },

    #[error("failed to build object-store HTTP client")]
    BuildHttpClient {
        #[source]
        source: Box<reqwest::Error>,
    },

    #[error("failed to send {operation} request for object {object_id}: {source}")]
    SendRequest {
        operation: &'static str,
        object_id: String,
        #[source]
        source: Box<reqwest::Error>,
    },

    #[error("{operation} for object {object_id} returned unexpected HTTP status {status}")]
    UnexpectedStatus {
        operation: &'static str,
        object_id: String,
        status: u16,
    },

    #[error("failed to read response body for {operation} on object {object_id}")]
    ReadResponseBody {
        operation: &'static str,
        object_id: String,
        #[source]
        source: Box<reqwest::Error>,
    },

    #[error("failed to parse multipart upload response for object {object_id}: {detail}")]
    ParseMultipartResponse { object_id: String, detail: String },

    #[error(
        "multipart upload failed for object {object_id} at part {part}/{total_parts}: {detail}"
    )]
    MultipartUploadFailed {
        object_id: String,
        part: u16,
        total_parts: usize,
        detail: String,
    },
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{key} is required")]
    MissingEnv { key: &'static str },

    #[error("{key} is required")]
    MissingEnvWithDependency { key: &'static str },

    #[error("invalid {key} value {value:?}; expected one of: {expected}")]
    InvalidEnumEnv {
        key: &'static str,
        value: String,
        expected: &'static str,
    },

    #[error("invalid {key} value {value:?}; expected positive integer")]
    InvalidPositiveIntegerEnv { key: &'static str, value: String },

    #[error("invalid object-store configuration: {message}")]
    InvalidObjectStoreConfig { message: String },

    #[error("invalid inference configuration: {message}")]
    InvalidInferenceConfig { message: String },

    #[error("invalid embedding configuration: {message}")]
    InvalidEmbeddingConfig { message: String },

    #[error("invalid vector store configuration: {message}")]
    InvalidVectorStoreConfig { message: String },
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("failed to configure Oracle pool ping interval")]
    ConfigurePoolPingInterval {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to create Oracle pool for connect string {connect_string}")]
    CreatePool {
        connect_string: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to acquire Oracle connection for connect string {connect_string}")]
    AcquireConnection {
        connect_string: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to set Oracle call timeout")]
    SetCallTimeout {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to use Postgres via {connection_string}: {source}")]
    ConnectPostgres {
        connection_string: String,
        #[source]
        source: Box<postgres::Error>,
    },

    #[error("failed to use SQLite database at {path}: {source}")]
    ConnectSqlite {
        path: String,
        #[source]
        source: Box<rusqlite::Error>,
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
        source: Box<oracle::Error>,
    },

    #[error("failed to commit oa_schema_migration bootstrap")]
    CommitSchemaMigrationBootstrap {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to reset open_archive schema objects")]
    ResetSchemaObjects {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to load applied migrations")]
    LoadAppliedMigrations {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to read migration history row")]
    ReadMigrationHistoryRow {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to read migration version")]
    ReadMigrationVersion {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to read migration checksum")]
    ReadMigrationChecksum {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to read migration directory {path}")]
    ReadMigrationsDir {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
    },

    #[error("failed to read migration entry")]
    ReadMigrationEntry {
        #[source]
        source: Box<std::io::Error>,
    },

    #[error("failed to read migration {path}")]
    ReadMigrationFile {
        path: PathBuf,
        #[source]
        source: Box<std::io::Error>,
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
        source: Box<oracle::Error>,
    },

    #[error("failed to record migration {filename}")]
    RecordMigration {
        filename: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to commit migration {filename}")]
    CommitMigration {
        filename: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to ensure SQLite migration table")]
    EnsureSqliteSchemaMigrationTable {
        #[source]
        source: Box<rusqlite::Error>,
    },

    #[error("failed to reset SQLite schema objects")]
    ResetSqliteSchemaObjects {
        #[source]
        source: Box<rusqlite::Error>,
    },

    #[error("failed to load applied SQLite migrations")]
    LoadSqliteAppliedMigrations {
        #[source]
        source: Box<rusqlite::Error>,
    },

    #[error("failed to apply SQLite migration {filename}: {statement_preview}")]
    ExecuteSqliteMigrationStatement {
        filename: String,
        statement_preview: String,
        #[source]
        source: Box<rusqlite::Error>,
    },

    #[error("failed to record SQLite migration {filename}")]
    RecordSqliteMigration {
        filename: String,
        #[source]
        source: Box<rusqlite::Error>,
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
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("failed to insert import {import_id}")]
    InsertImport {
        import_id: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("failed to update import counts for {import_id}")]
    UpdateImportCounts {
        import_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to finalize import {import_id}")]
    FinalizeImport {
        import_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to complete import {import_id}")]
    CompleteImport {
        import_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to insert artifact {artifact_id}")]
    InsertArtifact {
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to list artifacts")]
    ListArtifacts {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to insert participant {participant_id} for artifact {artifact_id}")]
    InsertParticipant {
        participant_id: String,
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to insert segment {segment_id} for artifact {artifact_id}")]
    InsertSegment {
        segment_id: String,
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to insert imported note property {property_key} for artifact {artifact_id}")]
    InsertImportedNoteProperty {
        artifact_id: String,
        property_key: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("failed to insert imported note tag {tag_value} for artifact {artifact_id}")]
    InsertImportedNoteTag {
        artifact_id: String,
        tag_value: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("failed to insert imported note alias for artifact {artifact_id}: {alias_text}")]
    InsertImportedNoteAlias {
        artifact_id: String,
        alias_text: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("failed to insert imported note link {link_id} for artifact {artifact_id}")]
    InsertImportedNoteLink {
        artifact_id: String,
        link_id: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error(
        "failed to insert artifact link from {source_artifact_id} to {target_artifact_id} ({link_type})"
    )]
    InsertArtifactLink {
        source_artifact_id: String,
        target_artifact_id: String,
        link_type: String,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("failed to insert derivation run {derivation_run_id} for artifact {artifact_id}")]
    InsertDerivationRun {
        derivation_run_id: String,
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to insert derived object {derived_object_id} for artifact {artifact_id}")]
    InsertDerivedObject {
        derived_object_id: String,
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to update derived object status for artifact {artifact_id}")]
    UpdateDerivedObjectStatus {
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("invalid derivation write: {detail}")]
    InvalidDerivationWrite { detail: String },

    #[error("failed to validate artifact ownership for {artifact_id}")]
    ValidateArtifactOwnership {
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to validate segment ownership for {segment_id}")]
    ValidateSegmentOwnership {
        segment_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to insert enrichment job {job_id} for artifact {artifact_id}")]
    InsertJob {
        job_id: String,
        artifact_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to claim next enrichment job")]
    ClaimJob {
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to update enrichment job {job_id} status")]
    UpdateJobStatus {
        job_id: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("invalid job_type '{job_type}' for job {job_id}")]
    InvalidJobType { job_id: String, job_type: String },

    #[error("invalid source_type '{value}' for artifact {artifact_id}")]
    InvalidSourceType { artifact_id: String, value: String },

    #[error("invalid artifact_class '{value}' for artifact {artifact_id}")]
    InvalidArtifactClass { artifact_id: String, value: String },

    #[error("invalid participant_role '{value}' for participant {participant_id}")]
    InvalidParticipantRole {
        participant_id: String,
        value: String,
    },

    #[error("invalid visibility_status '{value}' for segment {segment_id}")]
    InvalidVisibilityStatus { segment_id: String, value: String },

    #[error("invalid enrichment_tier '{value}' for job {job_id}")]
    InvalidEnrichmentTier { job_id: String, value: String },

    #[error("invalid required_capabilities for job {job_id}: {detail}")]
    InvalidJobCapabilities { job_id: String, detail: String },

    #[error("worker {worker_id} does not own running job {job_id}")]
    JobNotClaimed { job_id: String, worker_id: String },

    #[error("failed to commit {operation}")]
    Commit {
        operation: &'static str,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to rollback after {operation}")]
    Rollback {
        operation: String,
        #[source]
        source: Box<oracle::Error>,
    },

    #[error("failed to recover finalization for import {import_id}")]
    RecoverImportFinalization {
        import_id: String,
        #[source]
        source: Box<StorageError>,
    },

    #[error("invalid enrichment_status '{value}' for artifact {artifact_id}")]
    InvalidEnrichmentStatus { artifact_id: String, value: String },

    #[error("invalid derived_object_type '{value}' while loading context for {artifact_id}")]
    InvalidDerivedObjectType { artifact_id: String, value: String },

    #[error("invalid artifact_link_type '{value}' while loading context for {artifact_id}")]
    InvalidArtifactLinkType { artifact_id: String, value: String },

    #[error("invalid scope_type '{value}' while loading context for {artifact_id}")]
    InvalidScopeType { artifact_id: String, value: String },

    #[error(
        "failed to read confidence_score for derived object {derived_object_id} while loading artifact {artifact_id}"
    )]
    ReadDerivedObjectConfidenceScore {
        artifact_id: String,
        derived_object_id: String,
        #[source]
        source: Box<postgres::Error>,
    },

    #[error("{store} storage does not support {operation}")]
    UnsupportedOperation {
        store: &'static str,
        operation: &'static str,
    },

    #[error("failed to send vector-store {operation} request")]
    VectorStoreRequest {
        operation: &'static str,
        #[source]
        source: Box<reqwest::Error>,
    },

    #[error("vector-store {operation} returned unexpected HTTP status {status}: {body_preview}")]
    VectorStoreUnexpectedStatus {
        operation: &'static str,
        status: u16,
        body_preview: String,
    },

    #[error("failed to parse vector-store {operation} response: {detail}")]
    VectorStoreParseResponse {
        operation: &'static str,
        detail: String,
    },
}

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("invalid JSON: {detail}")]
    InvalidJson { detail: String },

    #[error("invalid Obsidian vault: {detail}")]
    InvalidObsidianVault { detail: String },

    #[error("unsupported payload: {detail}")]
    UnsupportedPayload { detail: String },

    #[error("invalid frontmatter in note {note_path}: {detail}")]
    InvalidFrontmatter { note_path: String, detail: String },

    #[error("export contains no conversations")]
    EmptyExport,

    #[error("document contains no usable text")]
    EmptyDocument,

    #[error("vault contains no Markdown notes")]
    EmptyVault,

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

#[derive(Debug, Error)]
pub enum EmbeddingError {
    #[error("failed to serialize embedding request")]
    SerializeRequest {
        #[source]
        source: Box<serde_json::Error>,
    },

    #[error("failed to send embedding request")]
    SendRequest {
        #[source]
        source: Box<reqwest::Error>,
    },

    #[error("failed to read embedding response")]
    ReadResponse {
        #[source]
        source: Box<reqwest::Error>,
    },

    #[error("embedding provider returned unexpected HTTP status {status}")]
    HttpStatus { status: u16, body_preview: String },

    #[error("failed to parse embedding response")]
    ParseResponse {
        #[source]
        source: Box<serde_json::Error>,
        body_preview: String,
    },

    #[error("embedding response returned dimension {actual}; expected {expected}")]
    UnexpectedDimensions { expected: usize, actual: usize },
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Processor(#[from] crate::processor::ProcessorError),

    #[error("batch execution mode requires {stage} batch processor support")]
    MissingBatchProcessor { stage: &'static str },

    #[error("batch execution mode requires {stage} batch submitter support")]
    MissingBatchSubmitter { stage: &'static str },

    #[error("failed to spawn {worker_kind} thread")]
    SpawnThread {
        worker_kind: String,
        #[source]
        source: Box<std::io::Error>,
    },
}

impl From<ConfigError> for OpenArchiveError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<DbError> for OpenArchiveError {
    fn from(value: DbError) -> Self {
        Self::Db(Box::new(value))
    }
}

impl From<MigrationsError> for OpenArchiveError {
    fn from(value: MigrationsError) -> Self {
        Self::Migrations(Box::new(value))
    }
}

impl From<ObjectStoreError> for OpenArchiveError {
    fn from(value: ObjectStoreError) -> Self {
        Self::ObjectStore(Box::new(value))
    }
}

impl From<StorageError> for OpenArchiveError {
    fn from(value: StorageError) -> Self {
        Self::Storage(Box::new(value))
    }
}

impl From<ParserError> for OpenArchiveError {
    fn from(value: ParserError) -> Self {
        Self::Parser(value)
    }
}

impl From<EmbeddingError> for OpenArchiveError {
    fn from(value: EmbeddingError) -> Self {
        Self::Embedding(Box::new(value))
    }
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
