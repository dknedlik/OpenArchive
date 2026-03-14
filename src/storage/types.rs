/// Domain-shaped storage write structs and storage-owned enums for the slice-one
/// write path.
///
/// These types are domain-shaped. They do not expose Oracle types or SQL concerns.
///
/// Nullability follows the live DDL in V001. Fields that are NOT NULL in the
/// schema are required (`String`, `i64`, etc.); nullable columns become
/// `Option<_>`. Source timestamps use `Option<SourceTimestamp>` so RFC3339
/// format and timezone are validated at parse time rather than at DB binding.
/// Server-assigned timestamps (created_at, captured_at, etc.) are omitted —
/// the DB sets them via DEFAULT SYSTIMESTAMP.
use crate::domain::{ParticipantRole, SourceTimestamp, VisibilityStatus};
use crate::object_store::StoredObject;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    ChatGptExport,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::ChatGptExport => "chatgpt_export",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "chatgpt_export" => Some(Self::ChatGptExport),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadFormat {
    ChatGptExportZip,
    ChatGptExportJson,
    ChatGptExportCanonicalJson,
}

impl PayloadFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            PayloadFormat::ChatGptExportZip => "chatgpt_export_zip",
            PayloadFormat::ChatGptExportJson => "chatgpt_export_json",
            PayloadFormat::ChatGptExportCanonicalJson => "chatgpt_export_canonical_json",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportStatus {
    Pending,
    Parsing,
    Completed,
    CompletedWithErrors,
    Failed,
}

impl ImportStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ImportStatus::Pending => "pending",
            ImportStatus::Parsing => "parsing",
            ImportStatus::Completed => "completed",
            ImportStatus::CompletedWithErrors => "completed_with_errors",
            ImportStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactClass {
    Conversation,
}

impl ArtifactClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactClass::Conversation => "conversation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStatus {
    Captured,
    Normalized,
    Failed,
}

impl ArtifactStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactStatus::Captured => "captured",
            ArtifactStatus::Normalized => "normalized",
            ArtifactStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentStatus {
    Pending,
    Running,
    Completed,
    Partial,
    Failed,
}

impl EnrichmentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EnrichmentStatus::Pending => "pending",
            EnrichmentStatus::Running => "running",
            EnrichmentStatus::Completed => "completed",
            EnrichmentStatus::Partial => "partial",
            EnrichmentStatus::Failed => "failed",
        }
    }
}

impl serde::Serialize for EnrichmentStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl EnrichmentStatus {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "partial" => Some(Self::Partial),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactIngestResult {
    Created,
    AlreadyExists,
    Failed,
}

impl ArtifactIngestResult {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactIngestResult::Created => "created",
            ArtifactIngestResult::AlreadyExists => "already_exists",
            ArtifactIngestResult::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentType {
    Message,
    MessageWindow,
}

impl SegmentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SegmentType::Message => "message",
            SegmentType::MessageWindow => "message_window",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    ArtifactEnrichment,
}

impl JobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobType::ArtifactEnrichment => "artifact_enrichment",
        }
    }
}

impl JobType {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "artifact_enrichment" => Some(Self::ArtifactEnrichment),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Partial,
    Failed,
    Retryable,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Partial => "partial",
            JobStatus::Failed => "failed",
            JobStatus::Retryable => "retryable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentTier {
    Standard,
    Quality,
}

impl EnrichmentTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            EnrichmentTier::Standard => "standard",
            EnrichmentTier::Quality => "quality",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "standard" => Some(Self::Standard),
            "quality" => Some(Self::Quality),
            _ => None,
        }
    }
}

impl JobStatus {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "partial" => Some(Self::Partial),
            "failed" => Some(Self::Failed),
            "retryable" => Some(Self::Retryable),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivationRunType {
    SummaryExtraction,
    ClassificationExtraction,
    MemoryExtraction,
    SummaryReduction,
    MemoryReduction,
    ContextPackAssembly,
}

impl DerivationRunType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DerivationRunType::SummaryExtraction => "summary_extraction",
            DerivationRunType::ClassificationExtraction => "classification_extraction",
            DerivationRunType::MemoryExtraction => "memory_extraction",
            DerivationRunType::SummaryReduction => "summary_reduction",
            DerivationRunType::MemoryReduction => "memory_reduction",
            DerivationRunType::ContextPackAssembly => "context_pack_assembly",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivationRunStatus {
    Running,
    Completed,
    Failed,
}

impl DerivationRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            DerivationRunStatus::Running => "running",
            DerivationRunStatus::Completed => "completed",
            DerivationRunStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputScopeType {
    Artifact,
    SegmentWindow,
    ArtifactReduce,
}

impl InputScopeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InputScopeType::Artifact => "artifact",
            InputScopeType::SegmentWindow => "segment_window",
            InputScopeType::ArtifactReduce => "artifact_reduce",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedObjectType {
    Summary,
    Classification,
    Memory,
}

impl DerivedObjectType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DerivedObjectType::Summary => "summary",
            DerivedObjectType::Classification => "classification",
            DerivedObjectType::Memory => "memory",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginKind {
    Explicit,
    Deterministic,
    Inferred,
    UserConfirmed,
}

impl OriginKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            OriginKind::Explicit => "explicit",
            OriginKind::Deterministic => "deterministic",
            OriginKind::Inferred => "inferred",
            OriginKind::UserConfirmed => "user_confirmed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStatus {
    Active,
    Superseded,
    Failed,
}

impl ObjectStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectStatus::Active => "active",
            ObjectStatus::Superseded => "superseded",
            ObjectStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeType {
    Artifact,
    Segment,
}

impl ScopeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScopeType::Artifact => "artifact",
            ScopeType::Segment => "segment",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceRole {
    PrimarySupport,
    SecondarySupport,
    ReductionInput,
}

impl EvidenceRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            EvidenceRole::PrimarySupport => "primary_support",
            EvidenceRole::SecondarySupport => "secondary_support",
            EvidenceRole::ReductionInput => "reduction_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportStrength {
    Strong,
    Medium,
    Weak,
}

impl SupportStrength {
    pub fn as_str(&self) -> &'static str {
        match self {
            SupportStrength::Strong => "strong",
            SupportStrength::Medium => "medium",
            SupportStrength::Weak => "weak",
        }
    }
}

// ---------------------------------------------------------------------------
// "New" structs — data needed to create one row, minus server-set fields
// ---------------------------------------------------------------------------

/// Data required to create the relational metadata row for one copied import
/// payload object.
#[derive(Debug)]
pub struct NewImportObjectRef {
    pub object_id: String,
    pub payload_format: PayloadFormat,
    pub mime_type: String,
    /// Transitional field used only by the legacy Oracle adapter until all
    /// relational providers read from the object store directly.
    pub copied_bytes: Vec<u8>,
    pub size_bytes: i64,
    /// SHA-256 hex digest. Unique constraint enforces payload-level deduplication.
    pub sha256: String,
    /// Object-store metadata for the copied raw payload. Oracle still writes the
    /// bytes inline today, but the application boundary now treats this as the
    /// durable payload reference.
    pub stored_object: StoredObject,
}

/// Data required to create one oa_import row.
#[derive(Debug)]
pub struct NewImport {
    pub import_id: String,
    pub source_type: SourceType,
    pub import_status: ImportStatus,
    /// FK to the payload object-reference row. Must be inserted before the
    /// import row.
    pub payload_object_id: String,
    /// Original filename from the upload, nullable in DDL.
    pub source_filename: Option<String>,
    /// SHA-256 hex digest of the raw source bytes. NOT NULL in DDL.
    pub source_content_hash: String,
    /// Best-effort conversation count at intake time. DDL DEFAULT 0 NOT NULL.
    pub conversation_count_detected: i32,
}

/// Data required to create one oa_artifact row.
#[derive(Debug)]
pub struct NewArtifact {
    pub artifact_id: String,
    /// FK to oa_import
    pub import_id: String,
    pub artifact_class: ArtifactClass,
    pub source_type: SourceType,
    pub artifact_status: ArtifactStatus,
    pub enrichment_status: EnrichmentStatus,
    /// Source-native conversation identifier, nullable in DDL.
    pub source_conversation_key: Option<String>,
    /// Application-computed per-conversation hash. NOT NULL + unique constraint.
    pub source_conversation_hash: String,
    pub title: Option<String>,
    /// Timestamp from the source export, nullable in DDL.
    pub created_at_source: Option<SourceTimestamp>,
    /// Nullable in DDL.
    pub started_at: Option<SourceTimestamp>,
    /// Nullable in DDL. Must be >= started_at when both are present (DDL CHECK).
    pub ended_at: Option<SourceTimestamp>,
    pub primary_language: Option<String>,
    /// Identifies the hash algorithm and normalization basis. NOT NULL.
    pub content_hash_version: String,
    /// JSON array, e.g. `["messages","text","participants","timestamps"]`. NOT NULL in DDL.
    pub content_facets_json: String,
    pub normalization_version: String,
}

/// Data required to create one oa_conversation_participant row.
#[derive(Debug)]
pub struct NewParticipant {
    pub participant_id: String,
    /// FK to oa_artifact
    pub artifact_id: String,
    pub participant_role: ParticipantRole,
    pub display_name: Option<String>,
    pub provider_name: Option<String>,
    pub model_name: Option<String>,
    pub source_participant_key: Option<String>,
    /// Zero-based ordering within the artifact. Unique per (artifact_id, sequence_no).
    pub sequence_no: i32,
}

/// Data required to create one oa_segment row.
///
/// For slice-one `message` segments `text_content` and `text_content_hash` are
/// always provided. The DDL allows them to be NULL for non-message types but
/// enforces `text_content IS NOT NULL` for `message` via CHECK constraint.
#[derive(Debug)]
pub struct NewSegment {
    pub segment_id: String,
    /// FK to oa_artifact
    pub artifact_id: String,
    /// FK to oa_conversation_participant, nullable in DDL.
    pub participant_id: Option<String>,
    pub segment_type: SegmentType,
    pub source_segment_key: Option<String>,
    /// Self-FK, nullable in DDL.
    pub parent_segment_id: Option<String>,
    /// Zero-based ordering. Unique per (artifact_id, sequence_no, segment_type).
    pub sequence_no: i32,
    /// Timestamp from the source export, nullable in DDL.
    pub created_at_source: Option<SourceTimestamp>,
    pub text_content: String,
    pub text_content_hash: String,
    /// JSON locator, nullable in DDL; must be valid JSON when present (DDL CHECK).
    pub locator_json: Option<String>,
    pub visibility_status: VisibilityStatus,
    /// JSON blob recording skipped non-text content. Must be valid JSON when present.
    pub unsupported_content_json: Option<String>,
}

/// Data required to create one oa_enrichment_job row.
#[derive(Debug)]
pub struct NewEnrichmentJob {
    pub job_id: String,
    /// FK to oa_artifact
    pub artifact_id: String,
    pub job_type: JobType,
    pub enrichment_tier: EnrichmentTier,
    pub spawned_by_job_id: Option<String>,
    pub job_status: JobStatus,
    /// DDL DEFAULT 3. Must be > 0 (DDL CHECK).
    pub max_attempts: i32,
    /// DDL DEFAULT 100. Must be >= 0 (DDL CHECK).
    pub priority_no: i32,
    /// Required model capabilities for this job, stored as a JSON array.
    pub required_capabilities: Vec<String>,
    /// Self-contained JSON payload for future out-of-process execution. NOT NULL in DDL.
    pub payload_json: String,
}

/// Data required to create one oa_derivation_run row.
#[derive(Debug)]
pub struct NewDerivationRun {
    pub derivation_run_id: String,
    pub artifact_id: String,
    pub job_id: Option<String>,
    pub run_type: DerivationRunType,
    pub pipeline_name: String,
    pub pipeline_version: String,
    pub provider_name: Option<String>,
    pub model_name: Option<String>,
    pub prompt_version: Option<String>,
    pub run_status: DerivationRunStatus,
    pub input_scope_type: InputScopeType,
    pub input_scope_json: String,
    pub started_at: SourceTimestamp,
    pub completed_at: Option<SourceTimestamp>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SummaryObjectJson {
    pub summary_kind: Option<String>,
    pub summary_version: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClassificationObjectJson {
    pub classification_type: String,
    pub classification_value: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct MemoryObjectJson {
    pub memory_type: String,
    pub memory_scope: ScopeType,
    pub memory_scope_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivedObjectPayload {
    Summary {
        title: Option<String>,
        body_text: String,
        object_json: Option<SummaryObjectJson>,
    },
    Classification {
        title: Option<String>,
        body_text: Option<String>,
        object_json: ClassificationObjectJson,
    },
    Memory {
        title: Option<String>,
        body_text: String,
        object_json: MemoryObjectJson,
    },
}

impl DerivedObjectPayload {
    pub fn derived_object_type(&self) -> DerivedObjectType {
        match self {
            DerivedObjectPayload::Summary { .. } => DerivedObjectType::Summary,
            DerivedObjectPayload::Classification { .. } => DerivedObjectType::Classification,
            DerivedObjectPayload::Memory { .. } => DerivedObjectType::Memory,
        }
    }

    pub fn title(&self) -> Option<&str> {
        match self {
            DerivedObjectPayload::Summary { title, .. }
            | DerivedObjectPayload::Classification { title, .. }
            | DerivedObjectPayload::Memory { title, .. } => title.as_deref(),
        }
    }

    pub fn body_text(&self) -> Option<&str> {
        match self {
            DerivedObjectPayload::Summary { body_text, .. }
            | DerivedObjectPayload::Memory { body_text, .. } => Some(body_text.as_str()),
            DerivedObjectPayload::Classification { body_text, .. } => body_text.as_deref(),
        }
    }

    pub fn object_json(&self) -> Option<String> {
        match self {
            DerivedObjectPayload::Summary { object_json, .. } => object_json
                .as_ref()
                .map(|value| serde_json::to_string(value).expect("summary payload serializable")),
            DerivedObjectPayload::Classification { object_json, .. } => Some(
                serde_json::to_string(object_json).expect("classification payload serializable"),
            ),
            DerivedObjectPayload::Memory { object_json, .. } => {
                Some(serde_json::to_string(object_json).expect("memory payload serializable"))
            }
        }
    }
}

/// Data required to create one oa_derived_object row.
#[derive(Debug, Clone, PartialEq)]
pub struct NewDerivedObject {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derivation_run_id: String,
    pub origin_kind: OriginKind,
    pub object_status: ObjectStatus,
    pub confidence_score: Option<f64>,
    pub confidence_label: Option<String>,
    pub scope_type: ScopeType,
    pub scope_id: String,
    pub supersedes_derived_object_id: Option<String>,
    pub payload: DerivedObjectPayload,
}

/// Data required to create one oa_evidence_link row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewEvidenceLink {
    pub evidence_link_id: String,
    pub derived_object_id: String,
    pub segment_id: String,
    pub evidence_role: EvidenceRole,
    pub evidence_rank: i64,
    pub support_strength: SupportStrength,
}

// ---------------------------------------------------------------------------
// Job payload contract
// ---------------------------------------------------------------------------

/// Documented contract for the `artifact_enrichment` job payload.
///
/// This is the exact shape stored in `oa_enrichment_job.payload_json`. The
/// payload is self-contained: a future out-of-process worker can execute the
/// job using only this payload, without hidden importer state.
///
/// ## Fields
///
/// * `schema_version` — `"1"` for slice one. Bump when the shape changes.
/// * `artifact_id` — the artifact to enrich (FK to `oa_artifact`).
/// * `import_id` — the import that created this artifact (for lineage).
/// * `source_type` — e.g. `"chatgpt_export"`. Tells the worker which
///   enrichment pipeline to apply.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArtifactEnrichmentPayload {
    pub schema_version: String,
    pub artifact_id: String,
    pub import_id: String,
    pub source_type: String,
}

impl ArtifactEnrichmentPayload {
    /// Build a slice-one payload with `schema_version = "1"`.
    pub fn new_v1(artifact_id: &str, import_id: &str, source_type: SourceType) -> Self {
        Self {
            schema_version: "1".to_string(),
            artifact_id: artifact_id.to_string(),
            import_id: import_id.to_string(),
            source_type: source_type.as_str().to_string(),
        }
    }

    /// Serialize to a JSON string suitable for `oa_enrichment_job.payload_json`.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ArtifactEnrichmentPayload is always serializable")
    }

    /// Deserialize from the `payload_json` column.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Worker-facing read types
// ---------------------------------------------------------------------------

/// A job that has been successfully claimed by a worker.
///
/// Returned by `claim_next_job`. Contains everything the worker needs to
/// execute the job without querying additional tables.
#[derive(Debug)]
pub struct ClaimedJob {
    pub job_id: String,
    pub artifact_id: String,
    pub job_type: JobType,
    pub enrichment_tier: EnrichmentTier,
    pub spawned_by_job_id: Option<String>,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub required_capabilities: Vec<String>,
    pub payload_json: String,
}

/// Outcome of a `mark_job_retryable` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Job was marked retryable and will be re-claimable after the backoff delay.
    Retried,
    /// `attempt_count >= max_attempts`: job was terminally failed instead.
    RetriesExhausted,
}

/// Narrow read-model row for `GET /artifacts`.
#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct ArtifactListItem {
    pub artifact_id: String,
    pub title: Option<String>,
    pub source_type: String,
    pub created_at_source: Option<String>,
    pub captured_at: String,
    pub enrichment_status: EnrichmentStatus,
}

/// Worker-facing artifact metadata assembled from canonical relational rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedArtifactRecord {
    pub artifact_id: String,
    pub import_id: String,
    pub source_type: SourceType,
    pub title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedParticipant {
    pub participant_id: String,
    pub participant_role: ParticipantRole,
    pub display_name: Option<String>,
    pub external_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedSegment {
    pub segment_id: String,
    pub participant_id: Option<String>,
    pub participant_role: Option<ParticipantRole>,
    pub sequence_no: i32,
    pub text_content: String,
    pub created_at_source: Option<SourceTimestamp>,
    pub visibility_status: VisibilityStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedArtifactForEnrichment {
    pub artifact: LoadedArtifactRecord,
    pub participants: Vec<LoadedParticipant>,
    pub segments: Vec<LoadedSegment>,
}
