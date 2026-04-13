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
    fn normalize_decisions(mut self, allowed_match_ids: &HashMap<String, HashSet<String>>) -> Self {
        for decision in &mut self.decisions {
            if !requires_matched_object_id(decision.decision_kind) {
                continue;
            }

            let matched_object_id = decision
                .matched_object_id
                .as_deref()
                .map(str::trim)
                .unwrap_or_default();
            let is_allowed = allowed_match_ids
                .get(&decision.target_key)
                .is_some_and(|ids| ids.contains(matched_object_id));

            if matched_object_id.is_empty() || !is_allowed {
                // Reconciliation is only allowed to merge when it can point at a
                // concrete existing object that was actually offered as a match
                // option for this target. Otherwise preserve the extracted
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
        allowed_match_ids: &HashMap<String, HashSet<String>>,
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
            if requires_matched_object_id(decision.decision_kind) {
                let matched_object_id = decision
                    .matched_object_id
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or_default();
                let is_allowed = allowed_match_ids
                    .get(&decision.target_key)
                    .is_some_and(|ids| ids.contains(matched_object_id));
                if !is_allowed {
                    return Err(ProcessorError::InvalidModelOutput {
                        detail: format!(
                            "decisions[{index}].matched_object_id {:?} was not offered for target_key {:?}",
                            matched_object_id, decision.target_key
                        ),
                    });
                }
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
        let allowed_match_ids = allowed_match_ids_by_target(input)?;
        let normalized = self.normalize_decisions(&allowed_match_ids);
        normalized.validate_against(input, &allowed_match_ids)?;
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

#[derive(Debug, Deserialize)]
struct ReconciliationMatchOption {
    object_id: String,
}

#[derive(Debug, Deserialize)]
struct ReconciliationMatchOptionCase {
    target_key: String,
    #[serde(default)]
    embedding_matches: Vec<ReconciliationMatchOption>,
}

#[derive(Debug, Deserialize)]
struct LegacyReconciliationMatchOptionEnvelope {
    #[serde(default)]
    objects: Vec<ReconciliationMatchOption>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ReconciliationMatchOptionEnvelope {
    Cases(Vec<ReconciliationMatchOptionCase>),
    Legacy(LegacyReconciliationMatchOptionEnvelope),
}

fn allowed_match_ids_by_target(
    input: &ReconciliationProcessorInput,
) -> Result<HashMap<String, HashSet<String>>, ProcessorError> {
    let valid_targets: Vec<String> = input
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
                relationship.relationship_type, relationship.subject_key, relationship.object_key
            )
        }))
        .collect();

    let trimmed = input.retrieval_results_json.trim();
    if trimmed.is_empty() {
        return Ok(HashMap::new());
    }

    let parsed: ReconciliationMatchOptionEnvelope =
        serde_json::from_str(trimmed).map_err(|source| ProcessorError::InvalidInput {
            detail: format!("invalid reconciliation candidate_match_options JSON: {source}"),
        })?;

    let map = match parsed {
        ReconciliationMatchOptionEnvelope::Cases(cases) => cases
            .into_iter()
            .map(|case| {
                (
                    case.target_key,
                    case.embedding_matches
                        .into_iter()
                        .map(|option| option.object_id)
                        .collect::<HashSet<_>>(),
                )
            })
            .collect(),
        ReconciliationMatchOptionEnvelope::Legacy(legacy) => {
            let shared_ids = legacy
                .objects
                .into_iter()
                .map(|option| option.object_id)
                .collect::<HashSet<_>>();
            valid_targets
                .into_iter()
                .map(|target_key| (target_key, shared_ids.clone()))
                .collect()
        }
    };

    Ok(map)
}

fn requires_matched_object_id(kind: ReconciliationDecisionKind) -> bool {
    matches!(
        kind,
        ReconciliationDecisionKind::AttachToExisting
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
