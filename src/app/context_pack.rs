use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{OpenArchiveError, Result};
use crate::storage::{
    ArtifactContextPackReadStore, CrossArtifactReadStore, DerivedObjectType, EnrichmentStatus,
    EvidenceRole, ImportedNoteLinkRecord, ImportedNoteMetadata, ScopeType, SourceType,
    SupportStrength,
};

/// Maximum characters to include in an evidence text excerpt.
const EXCERPT_MAX_CHARS: usize = 200;

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ContextPackRequest {
    pub artifact_id: String,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPackReadiness {
    /// Enrichment has not produced any usable output yet.
    NotReady,
    /// Some derived objects exist but the minimum useful set (at least one
    /// summary) is missing or enrichment itself is incomplete.
    Partial,
    /// Enrichment completed and the minimum useful object set is present.
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EvidenceExcerpt {
    pub segment_id: String,
    pub evidence_role: EvidenceRole,
    pub support_strength: SupportStrength,
    pub rank: i32,
    pub text_excerpt: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ContextDerivedEntry {
    pub derived_object_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub scope_type: ScopeType,
    pub evidence: Vec<EvidenceExcerpt>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suppressed_variant_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OmissionReason {
    /// Segment exists but no derived object references it via evidence links.
    NoEvidenceLinks,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PackOmission {
    pub reason: OmissionReason,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PackProvenance {
    pub total_segments: usize,
    pub referenced_segments: usize,
    /// True when the artifact's enrichment has not yet started or is still
    /// running.  This is a pack-level signal — not derived from row counts.
    pub enrichment_incomplete: bool,
    pub omissions: Vec<PackOmission>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct RelatedObjectEntry {
    pub derived_object_id: String,
    pub artifact_id: String,
    pub derived_object_type: DerivedObjectType,
    pub title: Option<String>,
    pub body_text: Option<String>,
    pub candidate_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ContextPackResponse {
    pub artifact_id: String,
    pub title: Option<String>,
    pub note_path: Option<String>,
    pub source_type: SourceType,
    pub imported_note_metadata: ImportedNoteMetadata,
    pub inbound_note_links: Vec<ImportedNoteLinkRecord>,
    pub readiness: ContextPackReadiness,
    pub summaries: Vec<ContextDerivedEntry>,
    pub classifications: Vec<ContextDerivedEntry>,
    pub memories: Vec<ContextDerivedEntry>,
    pub relationships: Vec<ContextDerivedEntry>,
    pub entities: Vec<ContextDerivedEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub related_objects: Vec<RelatedObjectEntry>,
    pub segment_count: usize,
    pub provenance: PackProvenance,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

pub struct ContextPackService {
    read_store: Arc<dyn ArtifactContextPackReadStore + Send + Sync>,
    cross_artifact_store: Option<Arc<dyn CrossArtifactReadStore + Send + Sync>>,
}

impl ContextPackService {
    pub fn new(read_store: Arc<dyn ArtifactContextPackReadStore + Send + Sync>) -> Self {
        Self {
            read_store,
            cross_artifact_store: None,
        }
    }

    pub fn with_cross_artifact_store(
        read_store: Arc<dyn ArtifactContextPackReadStore + Send + Sync>,
        cross_artifact_store: Arc<dyn CrossArtifactReadStore + Send + Sync>,
    ) -> Self {
        Self {
            read_store,
            cross_artifact_store: Some(cross_artifact_store),
        }
    }

    pub fn assemble(&self, request: ContextPackRequest) -> Result<Option<ContextPackResponse>> {
        let material = self
            .read_store
            .load_artifact_context_pack_material(&request.artifact_id)
            .map_err(OpenArchiveError::from)?;

        let material = match material {
            Some(m) => m,
            None => return Ok(None),
        };

        // Segment text lookup for evidence excerpts.
        let segment_text: HashMap<&str, &str> = material
            .segments
            .iter()
            .map(|s| (s.segment_id.as_str(), s.text_content.as_str()))
            .collect();

        // Group evidence links by derived_object_id.
        let mut evidence_by_object: HashMap<&str, Vec<EvidenceExcerpt>> = HashMap::new();
        let mut referenced_segment_ids: HashSet<&str> = HashSet::new();

        for link in &material.evidence_links {
            referenced_segment_ids.insert(link.segment_id.as_str());

            let text = segment_text.get(link.segment_id.as_str()).unwrap_or(&"");
            let text_excerpt = truncate_to_chars(text, EXCERPT_MAX_CHARS).to_string();

            evidence_by_object
                .entry(link.derived_object_id.as_str())
                .or_default()
                .push(EvidenceExcerpt {
                    segment_id: link.segment_id.clone(),
                    evidence_role: link.evidence_role,
                    support_strength: link.support_strength,
                    rank: link.evidence_rank,
                    text_excerpt,
                });
        }

        // Sort each evidence group by rank.
        for excerpts in evidence_by_object.values_mut() {
            excerpts.sort_by_key(|e| e.rank);
        }

        // Bucket derived objects by type.
        let mut summaries = Vec::new();
        let mut classifications = Vec::new();
        let mut memories = Vec::new();
        let mut relationships = Vec::new();
        let mut entities = Vec::new();

        for obj in &material.derived_objects {
            let evidence = evidence_by_object
                .remove(obj.derived_object_id.as_str())
                .unwrap_or_default();

            let entry = ContextDerivedEntry {
                derived_object_id: obj.derived_object_id.clone(),
                derived_object_type: obj.derived_object_type,
                title: obj.title.clone(),
                body_text: obj.body_text.clone(),
                scope_type: obj.scope_type,
                evidence,
                suppressed_variant_ids: Vec::new(),
            };

            match obj.derived_object_type {
                DerivedObjectType::Summary => summaries.push(entry),
                DerivedObjectType::Classification => classifications.push(entry),
                DerivedObjectType::Memory => memories.push(entry),
                DerivedObjectType::Relationship => relationships.push(entry),
                DerivedObjectType::Entity => entities.push(entry),
            }
        }

        memories = self.deduplicate_memories(memories);

        // Readiness.
        let has_summary = !summaries.is_empty();
        let has_any_objects = has_summary
            || !classifications.is_empty()
            || !memories.is_empty()
            || !relationships.is_empty()
            || !entities.is_empty();

        let readiness = compute_readiness(
            material.artifact.enrichment_status,
            has_summary,
            has_any_objects,
        );

        // Provenance.
        let total_segments = material.segments.len();
        let referenced_segments = referenced_segment_ids.len();
        let unreferenced = total_segments.saturating_sub(referenced_segments);

        let enrichment_incomplete = matches!(
            material.artifact.enrichment_status,
            EnrichmentStatus::Pending | EnrichmentStatus::Running
        );

        let mut omissions = Vec::new();
        if unreferenced > 0 && !enrichment_incomplete {
            omissions.push(PackOmission {
                reason: OmissionReason::NoEvidenceLinks,
                count: unreferenced,
            });
        }

        let provenance = PackProvenance {
            total_segments,
            referenced_segments,
            enrichment_incomplete,
            omissions,
        };

        // Cross-artifact related objects lookup.
        let mut related_objects = Vec::new();
        if let Some(cross_store) = &self.cross_artifact_store {
            let candidate_keys: Vec<String> = material
                .derived_objects
                .iter()
                .filter_map(|obj| obj.candidate_key.clone())
                .filter(|k| !k.is_empty())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            if !candidate_keys.is_empty() {
                match cross_store.find_related_by_candidate_keys(
                    &request.artifact_id,
                    &candidate_keys,
                    30,
                ) {
                    Ok(related) => {
                        related_objects = related
                            .into_iter()
                            .map(|r| RelatedObjectEntry {
                                derived_object_id: r.derived_object_id,
                                artifact_id: r.artifact_id,
                                derived_object_type: r.derived_object_type,
                                title: r.title,
                                body_text: r.body_text,
                                candidate_key: r.candidate_key,
                            })
                            .collect();
                    }
                    Err(err) => return Err(OpenArchiveError::from(err)),
                }
            }
        }

        Ok(Some(ContextPackResponse {
            artifact_id: material.artifact.artifact_id.clone(),
            title: material.artifact.title.clone(),
            note_path: material.artifact.note_path.clone(),
            source_type: material.artifact.source_type,
            imported_note_metadata: material.imported_note_metadata,
            inbound_note_links: material.inbound_note_links,
            readiness,
            summaries,
            classifications,
            memories,
            relationships,
            entities,
            related_objects,
            segment_count: total_segments,
            provenance,
        }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn compute_readiness(
    enrichment_status: EnrichmentStatus,
    has_summary: bool,
    has_any_objects: bool,
) -> ContextPackReadiness {
    match enrichment_status {
        EnrichmentStatus::Pending | EnrichmentStatus::Running => ContextPackReadiness::NotReady,
        EnrichmentStatus::Completed => {
            if has_summary {
                ContextPackReadiness::Ready
            } else if has_any_objects {
                ContextPackReadiness::Partial
            } else {
                ContextPackReadiness::NotReady
            }
        }
        EnrichmentStatus::Partial => {
            if has_any_objects {
                ContextPackReadiness::Partial
            } else {
                ContextPackReadiness::NotReady
            }
        }
        EnrichmentStatus::Failed => {
            if has_any_objects {
                ContextPackReadiness::Partial
            } else {
                ContextPackReadiness::NotReady
            }
        }
    }
}

/// Truncate a string to at most `max` characters (not bytes).
fn truncate_to_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((byte_offset, _)) => &s[..byte_offset],
        None => s,
    }
}

impl ContextPackService {
    fn deduplicate_memories(&self, memories: Vec<ContextDerivedEntry>) -> Vec<ContextDerivedEntry> {
        cluster_exact_memory_duplicates(memories)
    }
}

fn cluster_exact_memory_duplicates(memories: Vec<ContextDerivedEntry>) -> Vec<ContextDerivedEntry> {
    let mut deduped = Vec::<ContextDerivedEntry>::new();
    for memory in memories {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| exact_memory_duplicate(existing, &memory))
        {
            merge_context_memory(existing, &memory);
        } else {
            deduped.push(memory);
        }
    }
    deduped
}

fn exact_memory_duplicate(left: &ContextDerivedEntry, right: &ContextDerivedEntry) -> bool {
    if left.scope_type != right.scope_type {
        return false;
    }
    let left_title = normalize_memory_text(left.title.as_deref().unwrap_or_default());
    let right_title = normalize_memory_text(right.title.as_deref().unwrap_or_default());
    let left_body = normalize_memory_text(left.body_text.as_deref().unwrap_or_default());
    let right_body = normalize_memory_text(right.body_text.as_deref().unwrap_or_default());
    (!left_body.is_empty() && left_body == right_body)
        || (!left_title.is_empty() && left_title == right_title && left_body == right_body)
}

fn merge_context_memory(target: &mut ContextDerivedEntry, incoming: &ContextDerivedEntry) {
    if incoming.body_text.as_deref().unwrap_or_default().len()
        > target.body_text.as_deref().unwrap_or_default().len()
    {
        target.body_text = incoming.body_text.clone();
    }
    if incoming.title.as_deref().unwrap_or_default().len()
        > target.title.as_deref().unwrap_or_default().len()
    {
        target.title = incoming.title.clone();
    }
    for evidence in &incoming.evidence {
        if !target.evidence.iter().any(|existing| {
            existing.segment_id == evidence.segment_id && existing.rank == evidence.rank
        }) {
            target.evidence.push(evidence.clone());
        }
    }
    target.evidence.sort_by_key(|e| e.rank);
    if incoming.derived_object_id != target.derived_object_id
        && !target
            .suppressed_variant_ids
            .contains(&incoming.derived_object_id)
    {
        target
            .suppressed_variant_ids
            .push(incoming.derived_object_id.clone());
    }
    for suppressed in &incoming.suppressed_variant_ids {
        if !target.suppressed_variant_ids.contains(suppressed) {
            target.suppressed_variant_ids.push(suppressed.clone());
        }
    }
}

fn normalize_memory_text(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_space = true;
    for ch in value.chars().flat_map(char::to_lowercase) {
        let ch = if ch.is_ascii_alphanumeric() || ch.is_whitespace() {
            ch
        } else {
            ' '
        };
        if ch.is_whitespace() {
            if !previous_space {
                normalized.push(' ');
                previous_space = true;
            }
        } else {
            normalized.push(ch);
            previous_space = false;
        }
    }
    normalized.trim().to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageResult;
    use crate::storage::{
        ArtifactContextDerivedObject, ArtifactContextEvidenceLink, ArtifactContextPackMaterial,
        ArtifactContextPackReadStore, ArtifactDetailRecord, ArtifactDetailSegment,
    };

    struct MockPackReadStore {
        material: Option<ArtifactContextPackMaterial>,
    }

    impl ArtifactContextPackReadStore for MockPackReadStore {
        fn load_artifact_context_pack_material(
            &self,
            _artifact_id: &str,
        ) -> StorageResult<Option<ArtifactContextPackMaterial>> {
            Ok(self.material.clone())
        }
    }

    fn make_artifact(status: EnrichmentStatus) -> ArtifactDetailRecord {
        ArtifactDetailRecord {
            artifact_id: "art-1".to_string(),
            title: Some("Test conversation".to_string()),
            source_type: SourceType::ChatGptExport,
            enrichment_status: status,
            note_path: None,
        }
    }

    fn make_segment(id: &str, text: &str) -> ArtifactDetailSegment {
        ArtifactDetailSegment {
            segment_id: id.to_string(),
            participant_id: None,
            participant_role: None,
            sequence_no: 1,
            text_content: text.to_string(),
        }
    }

    fn make_derived_object(id: &str, obj_type: DerivedObjectType) -> ArtifactContextDerivedObject {
        ArtifactContextDerivedObject {
            derived_object_id: id.to_string(),
            derived_object_type: obj_type,
            title: Some(format!("{id} title")),
            body_text: Some(format!("{id} body")),
            scope_id: "art-1".to_string(),
            scope_type: ScopeType::Artifact,
            candidate_key: None,
        }
    }

    fn make_evidence_link(
        id: &str,
        object_id: &str,
        segment_id: &str,
        rank: i32,
    ) -> ArtifactContextEvidenceLink {
        ArtifactContextEvidenceLink {
            evidence_link_id: id.to_string(),
            derived_object_id: object_id.to_string(),
            segment_id: segment_id.to_string(),
            evidence_role: EvidenceRole::PrimarySupport,
            support_strength: SupportStrength::Strong,
            evidence_rank: rank,
        }
    }

    fn service_with(material: Option<ArtifactContextPackMaterial>) -> ContextPackService {
        ContextPackService::new(Arc::new(MockPackReadStore { material }))
    }

    fn request() -> ContextPackRequest {
        ContextPackRequest {
            artifact_id: "art-1".to_string(),
        }
    }

    #[test]
    fn not_found_returns_none() {
        let svc = service_with(None);
        let result = svc.assemble(request()).expect("should succeed");
        assert!(result.is_none());
    }

    #[test]
    fn not_ready_when_enrichment_pending() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Pending),
            segments: vec![make_segment("seg-1", "hello")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::NotReady);
        assert!(pack.provenance.enrichment_incomplete);
        // P1: unreferenced segments should NOT be reported as NoEvidenceLinks
        // when enrichment hasn't produced evidence yet.
        assert!(pack.provenance.omissions.is_empty());
    }

    #[test]
    fn pending_enrichment_does_not_attribute_gaps_to_no_evidence_links() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Running),
            segments: vec![
                make_segment("seg-1", "first"),
                make_segment("seg-2", "second"),
                make_segment("seg-3", "third"),
            ],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert!(pack.provenance.enrichment_incomplete);
        assert_eq!(pack.provenance.total_segments, 3);
        assert_eq!(pack.provenance.referenced_segments, 0);
        // All 3 segments are unreferenced, but omissions should be empty
        // because the gap is due to enrichment not having run, not missing links.
        assert!(pack.provenance.omissions.is_empty());
    }

    #[test]
    fn not_ready_when_completed_but_no_derived_objects() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![make_segment("seg-1", "hello")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::NotReady);
        assert!(!pack.provenance.enrichment_incomplete);
    }

    #[test]
    fn partial_when_completed_with_classification_but_no_summary() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![make_segment("seg-1", "hello")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![make_derived_object(
                "cls-1",
                DerivedObjectType::Classification,
            )],
            evidence_links: vec![make_evidence_link("el-1", "cls-1", "seg-1", 1)],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::Partial);
        assert_eq!(pack.classifications.len(), 1);
        assert!(pack.summaries.is_empty());
    }

    #[test]
    fn partial_when_enrichment_partial_with_summary() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Partial),
            segments: vec![make_segment("seg-1", "hello")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![make_derived_object("sum-1", DerivedObjectType::Summary)],
            evidence_links: vec![make_evidence_link("el-1", "sum-1", "seg-1", 1)],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::Partial);
        assert_eq!(pack.summaries.len(), 1);
    }

    #[test]
    fn ready_when_completed_with_summary_and_evidence() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![
                make_segment("seg-1", "first segment text"),
                make_segment("seg-2", "second segment text"),
            ],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![
                make_derived_object("sum-1", DerivedObjectType::Summary),
                make_derived_object("mem-1", DerivedObjectType::Memory),
            ],
            evidence_links: vec![
                make_evidence_link("el-1", "sum-1", "seg-1", 1),
                make_evidence_link("el-2", "mem-1", "seg-2", 1),
            ],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::Ready);
        assert_eq!(pack.summaries.len(), 1);
        assert_eq!(pack.memories.len(), 1);
        assert_eq!(pack.summaries[0].evidence.len(), 1);
        assert_eq!(
            pack.summaries[0].evidence[0].text_excerpt,
            "first segment text"
        );
    }

    #[test]
    fn evidence_excerpts_are_truncated_by_char_count() {
        // Use multi-byte characters to verify we count chars, not bytes.
        let long_text = "\u{00e9}".repeat(300); // 'é' is 2 bytes in UTF-8
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![make_segment("seg-1", &long_text)],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![make_derived_object("sum-1", DerivedObjectType::Summary)],
            evidence_links: vec![make_evidence_link("el-1", "sum-1", "seg-1", 1)],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        let excerpt = &pack.summaries[0].evidence[0].text_excerpt;
        assert_eq!(excerpt.chars().count(), EXCERPT_MAX_CHARS);
        // Byte length should be 2x char count for 2-byte chars.
        assert_eq!(excerpt.len(), EXCERPT_MAX_CHARS * 2);
    }

    #[test]
    fn provenance_counts_unreferenced_segments() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![
                make_segment("seg-1", "referenced"),
                make_segment("seg-2", "also referenced"),
                make_segment("seg-3", "unreferenced"),
                make_segment("seg-4", "also unreferenced"),
            ],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![make_derived_object("sum-1", DerivedObjectType::Summary)],
            evidence_links: vec![
                make_evidence_link("el-1", "sum-1", "seg-1", 1),
                make_evidence_link("el-2", "sum-1", "seg-2", 2),
            ],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.provenance.total_segments, 4);
        assert_eq!(pack.provenance.referenced_segments, 2);
        assert_eq!(pack.provenance.omissions.len(), 1);
        assert_eq!(
            pack.provenance.omissions[0].reason,
            OmissionReason::NoEvidenceLinks
        );
        assert_eq!(pack.provenance.omissions[0].count, 2);
    }

    #[test]
    fn type_grouping_is_correct() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![make_segment("seg-1", "text")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![
                make_derived_object("sum-1", DerivedObjectType::Summary),
                make_derived_object("cls-1", DerivedObjectType::Classification),
                make_derived_object("mem-1", DerivedObjectType::Memory),
                make_derived_object("rel-1", DerivedObjectType::Relationship),
            ],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.summaries.len(), 1);
        assert_eq!(pack.summaries[0].derived_object_id, "sum-1");
        assert_eq!(pack.classifications.len(), 1);
        assert_eq!(pack.classifications[0].derived_object_id, "cls-1");
        assert_eq!(pack.memories.len(), 1);
        assert_eq!(pack.memories[0].derived_object_id, "mem-1");
        assert_eq!(pack.relationships.len(), 1);
        assert_eq!(pack.relationships[0].derived_object_id, "rel-1");
    }

    #[test]
    fn failed_enrichment_with_objects_is_partial() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Failed),
            segments: vec![make_segment("seg-1", "text")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![make_derived_object("mem-1", DerivedObjectType::Memory)],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::Partial);
    }

    #[test]
    fn failed_enrichment_without_objects_is_not_ready() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Failed),
            segments: vec![make_segment("seg-1", "text")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.readiness, ContextPackReadiness::NotReady);
    }

    #[test]
    fn evidence_sorted_by_rank() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: make_artifact(EnrichmentStatus::Completed),
            segments: vec![
                make_segment("seg-1", "first"),
                make_segment("seg-2", "second"),
            ],
            imported_note_metadata: crate::storage::ImportedNoteMetadata::default(),
            inbound_note_links: Vec::new(),
            derived_objects: vec![make_derived_object("sum-1", DerivedObjectType::Summary)],
            evidence_links: vec![
                make_evidence_link("el-2", "sum-1", "seg-2", 2),
                make_evidence_link("el-1", "sum-1", "seg-1", 1),
            ],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        let evidence = &pack.summaries[0].evidence;
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence[0].rank, 1);
        assert_eq!(evidence[0].segment_id, "seg-1");
        assert_eq!(evidence[1].rank, 2);
        assert_eq!(evidence[1].segment_id, "seg-2");
    }

    #[test]
    fn carries_imported_note_metadata_and_links() {
        let svc = service_with(Some(ArtifactContextPackMaterial {
            artifact: ArtifactDetailRecord {
                artifact_id: "art-1".to_string(),
                title: Some("Inbox".to_string()),
                source_type: SourceType::ObsidianVault,
                enrichment_status: EnrichmentStatus::Completed,
                note_path: Some("Inbox.md".to_string()),
            },
            segments: vec![make_segment("seg-1", "hello")],
            imported_note_metadata: crate::storage::ImportedNoteMetadata {
                note_path: Some("Inbox.md".to_string()),
                properties: vec![],
                tags: vec![crate::storage::ImportedNoteTagRecord {
                    raw_tag: "#inbox".to_string(),
                    normalized_tag: "inbox".to_string(),
                    tag_path: "inbox".to_string(),
                    source_kind: crate::storage::ImportedNoteTagSourceKind::Inline,
                }],
                aliases: vec![crate::storage::ImportedNoteAliasRecord {
                    alias_text: "OA Inbox".to_string(),
                    normalized_alias: "oa inbox".to_string(),
                }],
                outbound_links: vec![crate::storage::ImportedNoteLinkRecord {
                    imported_note_link_id: "out-1".to_string(),
                    source_segment_id: Some("seg-1".to_string()),
                    link_kind: crate::storage::ImportedNoteLinkKind::Link,
                    target_kind: crate::storage::ImportedNoteLinkTargetKind::Note,
                    raw_target: "Projects/Acme".to_string(),
                    normalized_target: Some("projects/acme".to_string()),
                    display_text: Some("Acme".to_string()),
                    target_path: Some("Projects/Acme.md".to_string()),
                    target_heading: None,
                    target_block: None,
                    external_url: None,
                    resolved_artifact_id: Some("art-2".to_string()),
                    resolution_status: crate::storage::ImportedNoteLinkResolutionStatus::Resolved,
                    locator_json: None,
                }],
            },
            inbound_note_links: vec![crate::storage::ImportedNoteLinkRecord {
                imported_note_link_id: "in-1".to_string(),
                source_segment_id: None,
                link_kind: crate::storage::ImportedNoteLinkKind::Link,
                target_kind: crate::storage::ImportedNoteLinkTargetKind::Note,
                raw_target: "Inbox".to_string(),
                normalized_target: Some("inbox".to_string()),
                display_text: Some("Inbox".to_string()),
                target_path: Some("Inbox.md".to_string()),
                target_heading: None,
                target_block: None,
                external_url: None,
                resolved_artifact_id: Some("art-1".to_string()),
                resolution_status: crate::storage::ImportedNoteLinkResolutionStatus::Resolved,
                locator_json: None,
            }],
            derived_objects: vec![],
            evidence_links: vec![],
        }));

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.note_path.as_deref(), Some("Inbox.md"));
        assert_eq!(pack.imported_note_metadata.tags.len(), 1);
        assert_eq!(pack.imported_note_metadata.aliases.len(), 1);
        assert_eq!(pack.imported_note_metadata.outbound_links.len(), 1);
        assert_eq!(pack.inbound_note_links.len(), 1);
    }
}
