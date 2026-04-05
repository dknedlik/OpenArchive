use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::storage::types::{ReconciliationDecisionKind, ScopeType};

use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct ModelReconciliationOutput {
    pub(crate) decisions: Vec<ModelReconciliationDecision>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ModelReconciliationDecision {
    pub(crate) decision_kind: ReconciliationDecisionKind,
    pub(crate) target_kind: String,
    pub(crate) target_key: String,
    pub(crate) matched_object_id: Option<String>,
    pub(crate) rationale: String,
}

impl ModelReconciliationOutput {
    fn normalize_ungrounded_existing_decisions(mut self) -> Self {
        for decision in &mut self.decisions {
            if requires_matched_object_id(decision.decision_kind)
                && decision
                    .matched_object_id
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                // Reconciliation is only allowed to merge when it can point at a
                // concrete existing object. Otherwise preserve the extracted
                // candidate by treating it as create_new.
                decision.decision_kind = ReconciliationDecisionKind::CreateNew;
                decision.matched_object_id = None;
            }
        }
        self
    }

    pub(crate) fn validate_against(
        &self,
        input: &ReconciliationProcessorInput,
    ) -> Result<(), ProcessorError> {
        let valid_targets: HashSet<String> = input
            .memories
            .iter()
            .map(|memory| memory.candidate_key.clone())
            .chain(
                input
                    .entities
                    .iter()
                    .map(|entity| entity.entity_key.clone()),
            )
            .chain(input.relationships.iter().map(|relationship| {
                format!(
                    "{}:{}:{}",
                    relationship.relationship_type,
                    relationship.subject_key,
                    relationship.object_key
                )
            }))
            .collect();
        let target_kinds: HashMap<String, &'static str> = input
            .memories
            .iter()
            .map(|memory| (memory.candidate_key.clone(), "memory"))
            .chain(
                input
                    .entities
                    .iter()
                    .map(|entity| (entity.entity_key.clone(), "entity")),
            )
            .chain(input.relationships.iter().map(|relationship| {
                (
                    format!(
                        "{}:{}:{}",
                        relationship.relationship_type,
                        relationship.subject_key,
                        relationship.object_key
                    ),
                    "relationship",
                )
            }))
            .collect();
        let mut seen_targets = HashSet::new();

        for (index, decision) in self.decisions.iter().enumerate() {
            validate_text_field(
                &format!("decisions[{index}].target_kind"),
                &decision.target_kind,
            )?;
            validate_text_field(
                &format!("decisions[{index}].target_key"),
                &decision.target_key,
            )?;
            validate_text_field(
                &format!("decisions[{index}].rationale"),
                &decision.rationale,
            )?;
            if requires_matched_object_id(decision.decision_kind)
                && decision
                    .matched_object_id
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default()
                    .is_empty()
            {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].matched_object_id is required for {:?}",
                        decision.decision_kind
                    ),
                });
            }
            if !valid_targets.contains(&decision.target_key) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].target_key {:?} does not match a candidate",
                        decision.target_key
                    ),
                });
            }
            let expected_target_kind =
                target_kinds
                    .get(&decision.target_key)
                    .copied()
                    .ok_or_else(|| ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "decisions[{index}].target_key {:?} does not match a candidate",
                            decision.target_key
                        ),
                    })?;
            if decision.target_kind != expected_target_kind {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].target_kind {:?} does not match candidate kind {:?} for target_key {:?}",
                        decision.target_kind, expected_target_kind, decision.target_key
                    ),
                });
            }
            if !seen_targets.insert(decision.target_key.clone()) {
                return Err(ProcessorError::InvalidModelOutput {
                    detail: format!(
                        "decisions[{index}].target_key {:?} is duplicated",
                        decision.target_key
                    ),
                });
            }
        }

        if seen_targets != valid_targets {
            return Err(ProcessorError::InvalidModelOutput {
                detail: "reconciliation output must provide exactly one decision for each candidate memory, entity, or relationship"
                    .to_string(),
            });
        }

        Ok(())
    }

    pub(crate) fn into_validated_outputs(
        self,
        input: &ReconciliationProcessorInput,
    ) -> Result<Vec<ReconciliationDecisionOutput>, ProcessorError> {
        let normalized = self.normalize_ungrounded_existing_decisions();
        normalized.validate_against(input)?;
        Ok(normalized.into_outputs())
    }

    pub(crate) fn into_outputs(self) -> Vec<ReconciliationDecisionOutput> {
        self.decisions
            .into_iter()
            .map(|decision| ReconciliationDecisionOutput {
                decision_kind: decision.decision_kind,
                target_kind: decision.target_kind,
                target_key: decision.target_key,
                matched_object_id: decision.matched_object_id,
                rationale: decision.rationale,
            })
            .collect()
    }
}

fn requires_matched_object_id(kind: ReconciliationDecisionKind) -> bool {
    matches!(
        kind,
        ReconciliationDecisionKind::AttachToExisting
            | ReconciliationDecisionKind::StrengthenExisting
            | ReconciliationDecisionKind::SupersedeExisting
            | ReconciliationDecisionKind::ContradictsExisting
    )
}

pub(crate) fn validate_input(input: &ArtifactProcessorInput) -> Result<(), ProcessorError> {
    if input.segments.is_empty() {
        return Err(ProcessorError::InvalidInput {
            detail: format!("artifact {} has no segments to enrich", input.artifact_id),
        });
    }

    Ok(())
}

fn validate_text_field(field: &str, value: &str) -> Result<(), ProcessorError> {
    if value.trim().is_empty() {
        return Err(ProcessorError::InvalidModelOutput {
            detail: format!("{field} must not be empty"),
        });
    }

    Ok(())
}

fn hex_prefix(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub fn memory_candidate_key_from_fields(
    memory_type: &str,
    memory_scope: ScopeType,
    memory_scope_value: &str,
    title: Option<&str>,
    body_text: &str,
) -> String {
    let canonical = format!(
        "{}|{}|{}|{}|{}",
        normalize_candidate_key_text(memory_type),
        memory_scope.as_str(),
        normalize_candidate_key_text(memory_scope_value),
        normalize_candidate_key_text(title.unwrap_or_default()),
        normalize_candidate_key_text(body_text),
    );
    let digest = Sha256::digest(canonical.as_bytes());
    format!("mem:{}", hex_prefix(&digest[..16]))
}
