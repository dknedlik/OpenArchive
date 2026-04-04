use crate::storage::types::{
    EnrichmentTier, ReconciliationDecisionKind, RetrievalIntent, ScopeType,
};

use super::*;

#[derive(Debug, Default)]
pub struct StubProcessor;

impl ArtifactProcessor for StubProcessor {
    fn process(
        &self,
        input: &ArtifactProcessorInput,
    ) -> Result<ArtifactProcessorOutput, ProcessorError> {
        let first_segment_id = input
            .segments
            .first()
            .map(|segment| segment.segment_id.clone())
            .ok_or_else(|| ProcessorError::InvalidInput {
                detail: format!("artifact {} has no segments to enrich", input.artifact_id),
            })?;
        let evidence_segment_ids = vec![first_segment_id];
        let segment_count = input.segments.len();
        let participant_count = input.participants.len();
        let title = input
            .title
            .clone()
            .unwrap_or_else(|| format!("Artifact {}", input.artifact_id));

        Ok(ArtifactProcessorOutput {
            pipeline_name: "stub_enrichment".to_string(),
            pipeline_version: "v1".to_string(),
            provider_name: Some("stub".to_string()),
            model_name: Some("stub".to_string()),
            prompt_version: Some("stub-v1".to_string()),
            usage: None,
            summary: SummaryOutput {
                title: Some(format!("Stub summary for {title}")),
                body_text: format!(
                    "Stub summary for artifact {} with {} segments and {} participants.",
                    input.artifact_id, segment_count, participant_count
                ),
                evidence_segment_ids: evidence_segment_ids.clone(),
            },
            classifications: vec![ClassificationOutput {
                title: Some("Source type".to_string()),
                body_text: Some(format!(
                    "Stub classification for {} based on {} segments.",
                    input.source_type.as_str(),
                    segment_count
                )),
                classification_type: "topic".to_string(),
                classification_value: "stub_enrichment".to_string(),
                evidence_segment_ids: evidence_segment_ids.clone(),
            }],
            memories: vec![MemoryOutput {
                candidate_key: memory_candidate_key_from_fields(
                    "project_fact",
                    ScopeType::Artifact,
                    &input.artifact_id,
                    Some("Artifact memory"),
                    &format!(
                        "Stub memory for artifact {} derived from {} segments.",
                        input.artifact_id, segment_count
                    ),
                ),
                title: Some("Artifact memory".to_string()),
                body_text: format!(
                    "Stub memory for artifact {} derived from {} segments.",
                    input.artifact_id, segment_count
                ),
                memory_type: "project_fact".to_string(),
                memory_scope: ScopeType::Artifact,
                memory_scope_value: input.artifact_id.clone(),
                evidence_segment_ids,
            }],
            entities: input
                .participants
                .iter()
                .enumerate()
                .map(|(index, participant)| EntityOutput {
                    entity_key: participant
                        .display_name
                        .clone()
                        .unwrap_or_else(|| format!("participant_{index}"))
                        .to_ascii_lowercase()
                        .replace(' ', "_"),
                    display_name: participant
                        .display_name
                        .clone()
                        .unwrap_or_else(|| format!("Participant {index}")),
                    entity_type: "participant".to_string(),
                    evidence_segment_ids: vec![input.segments[0].segment_id.clone()],
                })
                .collect(),
            relationships: if input.participants.len() >= 2 {
                vec![RelationshipOutput {
                    relationship_type: "participant_interaction".to_string(),
                    subject_key: input
                        .participants
                        .first()
                        .and_then(|p| p.display_name.clone())
                        .unwrap_or_else(|| "participant_0".to_string())
                        .to_ascii_lowercase()
                        .replace(' ', "_"),
                    object_key: input
                        .participants
                        .get(1)
                        .and_then(|p| p.display_name.clone())
                        .unwrap_or_else(|| "participant_1".to_string())
                        .to_ascii_lowercase()
                        .replace(' ', "_"),
                    title: Some("Participant interaction".to_string()),
                    body_text: "The artifact captures an interaction between two participants."
                        .to_string(),
                    confidence_label: "medium".to_string(),
                    evidence_segment_ids: vec![input.segments[0].segment_id.clone()],
                }]
            } else {
                Vec::new()
            },
            retrieval_intents: vec![RetrievalIntent {
                intent_id: "intent-stub-1".to_string(),
                question: "Find prior archive context related to the artifact memory.".to_string(),
                query_text: title.clone(),
                intent_type: "memory_match".to_string(),
                evidence_segment_ids: vec![input.segments[0].segment_id.clone()],
            }],
            importance_score: 1,
        })
    }
}

#[derive(Debug, Default)]
pub struct StubReconciliationProcessor;

impl ReconciliationProcessor for StubReconciliationProcessor {
    fn reconcile(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        Ok(input
            .memories
            .iter()
            .map(|memory| ReconciliationDecisionOutput {
                decision_kind: ReconciliationDecisionKind::CreateNew,
                target_kind: "memory".to_string(),
                target_key: memory.candidate_key.clone(),
                matched_object_id: None,
                rationale: "Stub reconciliation defaults to create_new.".to_string(),
                evidence_segment_ids: memory.evidence_segment_ids.clone(),
            })
            .chain(
                input
                    .entities
                    .iter()
                    .map(|entity| ReconciliationDecisionOutput {
                        decision_kind: ReconciliationDecisionKind::CreateNew,
                        target_kind: "entity".to_string(),
                        target_key: entity.entity_key.clone(),
                        matched_object_id: None,
                        rationale: "Stub reconciliation defaults to create_new.".to_string(),
                        evidence_segment_ids: entity.evidence_segment_ids.clone(),
                    }),
            )
            .chain(
                input
                    .relationships
                    .iter()
                    .map(|relationship| ReconciliationDecisionOutput {
                        decision_kind: ReconciliationDecisionKind::CreateNew,
                        target_kind: "relationship".to_string(),
                        target_key: format!(
                            "{}:{}:{}",
                            relationship.relationship_type,
                            relationship.subject_key,
                            relationship.object_key
                        ),
                        matched_object_id: None,
                        rationale: "Stub reconciliation defaults to create_new.".to_string(),
                        evidence_segment_ids: relationship.evidence_segment_ids.clone(),
                    }),
            )
            .collect())
    }
}

#[derive(Debug, Default)]
pub struct StubProcessorFactory;

impl ArtifactProcessorFactory for StubProcessorFactory {
    fn build(&self, _tier: EnrichmentTier) -> Result<Box<dyn ArtifactProcessor>, ProcessorError> {
        Ok(Box::new(StubProcessor))
    }

    fn build_reconciliation_processor(
        &self,
        _tier: EnrichmentTier,
    ) -> Result<Box<dyn ReconciliationProcessor>, ProcessorError> {
        Ok(Box::new(StubReconciliationProcessor))
    }
}
