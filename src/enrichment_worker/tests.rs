use super::reconcile::{build_derivation_attempt, prepare_reconciliation_work};
use crate::embedding::StubEmbeddingProvider;
use crate::error::StorageResult;
use crate::processor::{MemoryOutput, ReconciliationProcessorInput, SummaryOutput};
use crate::storage::{
    ArtifactExtractionResult, ClaimedJob, CrossArtifactReadStore, DerivedObjectType,
    EnrichmentTier, ExtractedMemory, JobType, ReconciliationDecision, ReconciliationDecisionKind,
    RelatedDerivedObject, RelatedDerivedObjectEmbeddingMatch, ScopeType, SourceType,
};

struct MockCrossArtifactReadStore {
    exact_matches: Vec<RelatedDerivedObject>,
    embedding_matches: Vec<RelatedDerivedObjectEmbeddingMatch>,
}

impl CrossArtifactReadStore for MockCrossArtifactReadStore {
    fn find_related_by_candidate_keys(
        &self,
        _artifact_id: &str,
        _candidate_keys: &[String],
        _limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObject>> {
        Ok(self.exact_matches.clone())
    }

    fn find_related_by_embedding(
        &self,
        _artifact_id: &str,
        _derived_object_type: DerivedObjectType,
        _query_embedding: &[f32],
        _limit: usize,
    ) -> StorageResult<Vec<RelatedDerivedObjectEmbeddingMatch>> {
        Ok(self.embedding_matches.clone())
    }
}

fn sample_reconciliation_input() -> ReconciliationProcessorInput {
    ReconciliationProcessorInput {
        artifact_id: "artifact-1".to_string(),
        source_type: SourceType::TextFile,
        summary: SummaryOutput {
            title: Some("Summary".to_string()),
            body_text: "Summary body".to_string(),
        },
        memories: vec![MemoryOutput {
            candidate_key: "memory-key".to_string(),
            title: Some("Memory".to_string()),
            body_text: "Memory body".to_string(),
            memory_type: "project_fact".to_string(),
            memory_scope: ScopeType::Artifact,
            memory_scope_value: "artifact-1".to_string(),
        }],
        entities: Vec::new(),
        relationships: Vec::new(),
        retrieval_results_json: "[]".to_string(),
    }
}

#[test]
fn prepare_reconciliation_work_attaches_exact_candidate_key_matches() {
    let input = sample_reconciliation_input();
    let cross_store = MockCrossArtifactReadStore {
        exact_matches: vec![RelatedDerivedObject {
            derived_object_id: "dobj-9".to_string(),
            artifact_id: "artifact-2".to_string(),
            derived_object_type: DerivedObjectType::Memory,
            title: Some("Existing".to_string()),
            body_text: Some("Existing body".to_string()),
            candidate_key: Some("memory-key".to_string()),
            confidence_score: None,
        }],
        embedding_matches: Vec::new(),
    };

    let prepared = prepare_reconciliation_work(&input, Some(&cross_store), None)
        .expect("prepare work should succeed");

    assert_eq!(prepared.deterministic_outputs.len(), 1);
    assert!(prepared.ambiguous_input.is_none());
    let output = &prepared.deterministic_outputs[0];
    assert_eq!(
        output.decision_kind,
        ReconciliationDecisionKind::AttachToExisting
    );
    assert_eq!(output.target_key, "memory-key");
    assert_eq!(output.matched_object_id.as_deref(), Some("dobj-9"));
}

#[test]
fn prepare_reconciliation_work_limits_model_fallback_to_ambiguous_embedding_matches() {
    let input = sample_reconciliation_input();
    let cross_store = MockCrossArtifactReadStore {
        exact_matches: Vec::new(),
        embedding_matches: vec![RelatedDerivedObjectEmbeddingMatch {
            derived_object_id: "dobj-7".to_string(),
            artifact_id: "artifact-2".to_string(),
            derived_object_type: DerivedObjectType::Memory,
            title: Some("Possible match".to_string()),
            body_text: Some("Possible body".to_string()),
            candidate_key: Some("other-key".to_string()),
            confidence_score: None,
            similarity_score: 0.82,
        }],
    };
    let embedding_provider = StubEmbeddingProvider::new("stub".to_string(), 8);

    let prepared =
        prepare_reconciliation_work(&input, Some(&cross_store), Some(&embedding_provider))
            .expect("prepare work should succeed");

    assert!(prepared.deterministic_outputs.is_empty());
    let ambiguous_input = prepared
        .ambiguous_input
        .expect("ambiguous candidate should remain for model fallback");
    assert_eq!(ambiguous_input.memories.len(), 1);
    assert!(ambiguous_input.entities.is_empty());
    assert!(ambiguous_input.relationships.is_empty());
    assert!(ambiguous_input
        .retrieval_results_json
        .contains("\"object_id\": \"dobj-7\""));
}

fn sample_claimed_job() -> ClaimedJob {
    ClaimedJob {
        job_id: "job-1".to_string(),
        artifact_id: "artifact-1".to_string(),
        job_type: JobType::ArtifactReconcile,
        enrichment_tier: EnrichmentTier::Default,
        spawned_by_job_id: None,
        attempt_count: 1,
        max_attempts: 3,
        required_capabilities: vec!["text".to_string()],
        payload_json: "{}".to_string(),
    }
}

fn sample_extraction_result() -> ArtifactExtractionResult {
    ArtifactExtractionResult {
        extraction_result_id: "extract-1".to_string(),
        artifact_id: "artifact-1".to_string(),
        job_id: "job-1".to_string(),
        pipeline_name: "artifact_extraction".to_string(),
        pipeline_version: "v1".to_string(),
        summary_title: Some("Summary".to_string()),
        summary_body_text: "Summary body".to_string(),
        classifications: Vec::new(),
        memories: vec![ExtractedMemory {
            title: Some("Memory".to_string()),
            body_text: "Memory body".to_string(),
            memory_type: "project_fact".to_string(),
            memory_scope: ScopeType::Artifact,
            memory_scope_value: "artifact-1".to_string(),
            candidate_key: "memory-key".to_string(),
        }],
        entities: Vec::new(),
        relationships: Vec::new(),
        status: "completed".to_string(),
        error_message: None,
    }
}

#[test]
fn build_derivation_attempt_persists_attached_candidates_and_links_them_directly() {
    let attempt = build_derivation_attempt(
        &sample_claimed_job(),
        "artifact-1",
        &sample_extraction_result(),
        &[ReconciliationDecision {
            reconciliation_decision_id: "reconcile-1".to_string(),
            artifact_id: "artifact-1".to_string(),
            job_id: "job-1".to_string(),
            extraction_result_id: "extract-1".to_string(),
            pipeline_name: "artifact_reconciliation".to_string(),
            pipeline_version: "v1".to_string(),
            decision_kind: ReconciliationDecisionKind::AttachToExisting,
            target_kind: "memory".to_string(),
            target_key: "memory-key".to_string(),
            matched_object_id: Some("existing-1".to_string()),
            rationale: "same underlying memory".to_string(),
            status: "completed".to_string(),
            error_message: None,
        }],
    );

    assert_eq!(attempt.objects.len(), 2);
    let memory_object = attempt
        .objects
        .iter()
        .find(|object| object.object.payload.derived_object_type() == DerivedObjectType::Memory)
        .expect("attached memory should still be persisted");
    assert_eq!(attempt.archive_links.len(), 1);
    assert_eq!(
        attempt.archive_links[0].source_object_id,
        memory_object.object.derived_object_id
    );
    assert_eq!(attempt.archive_links[0].target_object_id, "existing-1");
    assert_eq!(attempt.archive_links[0].link_type, "same_as");
}
