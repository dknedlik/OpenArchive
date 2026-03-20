use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::embeddings::TextEmbedder;
use crate::error::{OpenArchiveError, Result};
use crate::storage::{
    ArtifactContextPackReadStore, DerivedObjectType, EnrichmentStatus, EvidenceRole, ScopeType,
    SourceType, SupportStrength,
};
use log::warn;

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
pub struct ContextPackResponse {
    pub artifact_id: String,
    pub title: Option<String>,
    pub source_type: SourceType,
    pub readiness: ContextPackReadiness,
    pub summaries: Vec<ContextDerivedEntry>,
    pub classifications: Vec<ContextDerivedEntry>,
    pub memories: Vec<ContextDerivedEntry>,
    pub relationships: Vec<ContextDerivedEntry>,
    pub segment_count: usize,
    pub provenance: PackProvenance,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

pub struct ContextPackService {
    read_store: Arc<dyn ArtifactContextPackReadStore + Send + Sync>,
    embedder: Option<Arc<dyn TextEmbedder>>,
}

impl ContextPackService {
    pub fn new(
        read_store: Arc<dyn ArtifactContextPackReadStore + Send + Sync>,
        embedder: Option<Arc<dyn TextEmbedder>>,
    ) -> Self {
        Self {
            read_store,
            embedder,
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
            }
        }

        memories = self.deduplicate_memories(memories);

        // Readiness.
        let has_summary = !summaries.is_empty();
        let has_any_objects = has_summary
            || !classifications.is_empty()
            || !memories.is_empty()
            || !relationships.is_empty();

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

        Ok(Some(ContextPackResponse {
            artifact_id: material.artifact.artifact_id.clone(),
            title: material.artifact.title.clone(),
            source_type: material.artifact.source_type,
            readiness,
            summaries,
            classifications,
            memories,
            relationships,
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
        if memories.len() <= 1 {
            return memories;
        }

        let exact_groups = cluster_exact_memory_duplicates(memories);
        let Some(embedder) = &self.embedder else {
            return exact_groups;
        };

        match cluster_memory_entries_with_embeddings(embedder.as_ref(), exact_groups.clone()) {
            Ok(clustered) => clustered,
            Err(err) => {
                warn!("context-pack memory embedding dedupe failed: {err}");
                exact_groups
            }
        }
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

fn cluster_memory_entries_with_embeddings(
    embedder: &dyn TextEmbedder,
    memories: Vec<ContextDerivedEntry>,
) -> Result<Vec<ContextDerivedEntry>> {
    if memories.len() <= 1 {
        return Ok(memories);
    }

    let embedding_inputs: Vec<String> = memories
        .iter()
        .map(memory_embedding_text)
        .collect();
    let vectors = embedder
        .embed_texts(&embedding_inputs)
        .map_err(OpenArchiveError::from)?;
    if vectors.len() != memories.len() {
        return Err(OpenArchiveError::Invariant(
            "embedding service returned mismatched vector count".to_string(),
        ));
    }

    let mut uf = UnionFind::new(memories.len());
    for i in 0..memories.len() {
        for j in (i + 1)..memories.len() {
            if memories[i].scope_type != memories[j].scope_type {
                continue;
            }
            let similarity = cosine_similarity(&vectors[i], &vectors[j]);
            if should_cluster_memory_pair(&memories[i], &memories[j], similarity) {
                uf.union(i, j);
            }
        }
    }

    let mut clusters: HashMap<usize, Vec<usize>> = HashMap::new();
    for index in 0..memories.len() {
        clusters.entry(uf.find(index)).or_default().push(index);
    }

    let mut clustered = Vec::new();
    for indices in clusters.into_values() {
        let mut members: Vec<ContextDerivedEntry> =
            indices.into_iter().map(|index| memories[index].clone()).collect();
        let representative_index = select_representative_index(&members);
        let mut representative = members.remove(representative_index);
        for member in members {
            merge_context_memory(&mut representative, &member);
        }
        clustered.push(representative);
    }

    Ok(clustered)
}

fn memory_embedding_text(entry: &ContextDerivedEntry) -> String {
    match (&entry.body_text, &entry.title) {
        (Some(body), Some(title)) if !title.trim().is_empty() => format!("{body}\n\n{title}"),
        (Some(body), _) => body.clone(),
        (None, Some(title)) => title.clone(),
        (None, None) => String::new(),
    }
}

fn should_cluster_memory_pair(
    left: &ContextDerivedEntry,
    right: &ContextDerivedEntry,
    similarity: f32,
) -> bool {
    if similarity >= 0.90 {
        return true;
    }
    if similarity < 0.80 {
        return false;
    }

    memory_overlap_score(left, right) >= 0.65
}

fn memory_overlap_score(left: &ContextDerivedEntry, right: &ContextDerivedEntry) -> f32 {
    let left_body = left.body_text.as_deref().unwrap_or_default();
    let right_body = right.body_text.as_deref().unwrap_or_default();
    if !left_body.trim().is_empty() && !right_body.trim().is_empty() {
        return lexical_overlap(left_body, right_body);
    }

    lexical_overlap(
        left.title.as_deref().unwrap_or_default(),
        right.title.as_deref().unwrap_or_default(),
    )
}

fn lexical_overlap(left: &str, right: &str) -> f32 {
    let left_tokens: HashSet<String> = normalize_memory_text(left)
        .split_whitespace()
        .map(|token| token.to_string())
        .collect();
    let right_tokens: HashSet<String> = normalize_memory_text(right)
        .split_whitespace()
        .map(|token| token.to_string())
        .collect();
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }
    let overlap = left_tokens.intersection(&right_tokens).count() as f32;
    let smaller = left_tokens.len().min(right_tokens.len()) as f32;
    overlap / smaller
}

fn select_representative_index(entries: &[ContextDerivedEntry]) -> usize {
    entries
        .iter()
        .enumerate()
        .max_by_key(|(_, entry)| {
            let evidence_count = entry.evidence.len();
            let body_len = entry.body_text.as_deref().unwrap_or_default().len();
            let title_len = entry.title.as_deref().unwrap_or_default().len();
            (evidence_count, body_len, title_len)
        })
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn merge_context_memory(target: &mut ContextDerivedEntry, incoming: &ContextDerivedEntry) {
    if incoming
        .body_text
        .as_deref()
        .unwrap_or_default()
        .len()
        > target.body_text.as_deref().unwrap_or_default().len()
    {
        target.body_text = incoming.body_text.clone();
    }
    if incoming
        .title
        .as_deref()
        .unwrap_or_default()
        .len()
        > target.title.as_deref().unwrap_or_default().len()
    {
        target.title = incoming.title.clone();
    }
    for evidence in &incoming.evidence {
        if !target
            .evidence
            .iter()
            .any(|existing| existing.segment_id == evidence.segment_id && existing.rank == evidence.rank)
        {
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

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut left_norm = 0.0f32;
    let mut right_norm = 0.0f32;
    for (l, r) in left.iter().zip(right.iter()) {
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return 0.0;
    }
    dot / (left_norm.sqrt() * right_norm.sqrt())
}

struct UnionFind {
    parents: Vec<usize>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parents: (0..size).collect(),
        }
    }

    fn find(&mut self, index: usize) -> usize {
        if self.parents[index] != index {
            let root = self.find(self.parents[index]);
            self.parents[index] = root;
        }
        self.parents[index]
    }

    fn union(&mut self, left: usize, right: usize) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root != right_root {
            self.parents[right_root] = left_root;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embeddings::TextEmbedder;
    use crate::error::StorageResult;
    use crate::storage::{
        ArtifactContextDerivedObject, ArtifactContextEvidenceLink, ArtifactContextPackMaterial,
        ArtifactContextPackReadStore, ArtifactDetailRecord, ArtifactDetailSegment,
    };
    use crate::error::EmbeddingError;

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
        ContextPackService::new(Arc::new(MockPackReadStore { material }), None)
    }

    struct MockEmbedder {
        vectors: Vec<Vec<f32>>,
    }

    impl TextEmbedder for MockEmbedder {
        fn embed_texts(&self, _texts: &[String]) -> std::result::Result<Vec<Vec<f32>>, EmbeddingError> {
            Ok(self.vectors.clone())
        }
    }

    fn service_with_embedder(
        material: Option<ArtifactContextPackMaterial>,
        vectors: Vec<Vec<f32>>,
    ) -> ContextPackService {
        ContextPackService::new(
            Arc::new(MockPackReadStore { material }),
            Some(Arc::new(MockEmbedder { vectors })),
        )
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
    fn context_pack_memory_dedup_suppresses_embedding_cluster_variants() {
        let svc = service_with_embedder(
            Some(ArtifactContextPackMaterial {
                artifact: make_artifact(EnrichmentStatus::Completed),
                segments: vec![
                    make_segment("seg-1", "first"),
                    make_segment("seg-2", "second"),
                ],
                derived_objects: vec![
                    make_derived_object("mem-1", DerivedObjectType::Memory),
                    make_derived_object("mem-2", DerivedObjectType::Memory),
                    make_derived_object("mem-3", DerivedObjectType::Memory),
                ],
                evidence_links: vec![
                    make_evidence_link("ev-1", "mem-1", "seg-1", 1),
                    make_evidence_link("ev-2", "mem-2", "seg-1", 1),
                    make_evidence_link("ev-3", "mem-3", "seg-2", 1),
                ],
            }),
            vec![
                vec![1.0, 0.0],
                vec![0.95, 0.05],
                vec![0.0, 1.0],
            ],
        );

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.memories.len(), 2);
        assert!(pack
            .memories
            .iter()
            .any(|memory| memory.suppressed_variant_ids.len() == 1));
    }

    #[test]
    fn context_pack_memory_dedup_keeps_related_but_distinct_memories_when_overlap_is_weak() {
        let svc = service_with_embedder(
            Some(ArtifactContextPackMaterial {
                artifact: make_artifact(EnrichmentStatus::Completed),
                segments: vec![
                    make_segment("seg-1", "first"),
                    make_segment("seg-2", "second"),
                ],
                derived_objects: vec![
                    ArtifactContextDerivedObject {
                        derived_object_id: "mem-a".to_string(),
                        derived_object_type: DerivedObjectType::Memory,
                        title: Some("Breakfast Fish Rotation".to_string()),
                        body_text: Some(
                            "The user rotates breakfast fish options such as kippers, mackerel, and lox."
                                .to_string(),
                        ),
                        scope_id: "art-1".to_string(),
                        scope_type: ScopeType::Artifact,
                    },
                    ArtifactContextDerivedObject {
                        derived_object_id: "mem-b".to_string(),
                        derived_object_type: DerivedObjectType::Memory,
                        title: Some("Tinned fish breakfast rotation for cost savings".to_string()),
                        body_text: Some(
                            "The user is using tinned kippers and mackerel in breakfast specifically to reduce costs compared with daily lox."
                                .to_string(),
                        ),
                        scope_id: "art-1".to_string(),
                        scope_type: ScopeType::Artifact,
                    },
                ],
                evidence_links: vec![
                    make_evidence_link("ev-a", "mem-a", "seg-1", 1),
                    make_evidence_link("ev-b", "mem-b", "seg-2", 1),
                ],
            }),
            vec![
                vec![1.0, 0.0],
                vec![0.85, 0.5267827],
            ],
        );

        let pack = svc.assemble(request()).unwrap().unwrap();
        assert_eq!(pack.memories.len(), 2);
        assert!(pack
            .memories
            .iter()
            .all(|memory| memory.suppressed_variant_ids.is_empty()));
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
}
